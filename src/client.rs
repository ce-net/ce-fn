//! [`FnClient`] — the serverless control plane over a local CE node.
//!
//! `FnClient` composes existing CE primitives into Cloud-Run / Cloud-Functions semantics:
//!
//! * **deploy** — pick an atlas-ranked host ([`crate::placement`]) and place the handler with
//!   `mesh-deploy` (container) or `mesh_deploy_wasm` (WASM); the bid flows through the normal
//!   job/credit escrow, so billing is the existing economy.
//! * **invoke** — send an [`InvokeRequest`] to the host over the authenticated `AppRequest`/reply
//!   primitive (`CeClient::request`) on [`INVOKE_TOPIC`]; HTTP-style request/response.
//! * **trigger** — subscribe to a pubsub topic and invoke the function once per published event.
//! * **kill** — stop the deployed cell with `mesh-kill`.
//!
//! State (which function lives on which host) is kept in a [`Registry`]; deploy records it, the
//! other verbs read it. No new node endpoints — everything is `ce-rs` over the local node.

use anyhow::{Result, anyhow, bail};
use ce_rs::{Amount, CeClient};

use crate::function::{Deployment, Function, Handler, Registry};
use crate::placement::{self, Candidate, Requirements};
use crate::protocol::{INVOKE_TOPIC, InvokeRequest, InvokeResponse, TriggerEvent};

/// Default per-invocation request timeout (ms) — generous, since a cold container pull + run can
/// take a while. Callers can override via [`FnClient::invoke_with`].
pub const DEFAULT_INVOKE_TIMEOUT_MS: u64 = 600_000;

/// The ce-fn control plane bound to one local CE node and an in-memory registry view.
pub struct FnClient {
    ce: CeClient,
    registry: Registry,
}

impl FnClient {
    /// Build a control plane over `ce` with a starting `registry`.
    pub fn new(ce: CeClient, registry: Registry) -> Self {
        FnClient { ce, registry }
    }

    /// Borrow the registry (e.g. to persist it after a mutation).
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The underlying CE client (for callers needing raw access, e.g. uploading a WASM module).
    pub fn ce(&self) -> &CeClient {
        &self.ce
    }

    /// Translate a function's resource needs + handler kind into placement [`Requirements`].
    fn requirements(f: &Function) -> Requirements {
        match &f.handler {
            Handler::Container { .. } => {
                Requirements::for_container(f.cpu_cores, f.mem_mb, f.select.clone())
            }
            Handler::Wasm { .. } => Requirements::for_wasm(f.cpu_cores, f.mem_mb, f.select.clone()),
        }
    }

    /// Choose the single best host for `f` from the live atlas, ranked by on-chain delivered work.
    /// Returns the chosen [`Candidate`] or an error if no host qualifies. This is the atlas-guided,
    /// swarm-style placement step, factored out so it can be inspected before deploying.
    pub async fn select_host(&self, f: &Function) -> Result<Candidate> {
        let atlas = self.ce.atlas().await?;
        let req = Self::requirements(f);

        // Gather reputation only for the pool that already meets the requirements, to avoid an
        // O(atlas) history fan-out. `placement::best` re-filters, so build a name->rep map first.
        let pool = placement::candidates(atlas.clone(), &req);
        if pool.is_empty() {
            bail!(
                "no host in the atlas satisfies the function's requirements \
                 (need {} cores / {} MiB{}{})",
                f.cpu_cores,
                f.mem_mb,
                if req.require_docker { " + docker" } else { " + wasm" },
                tag_note(&f.select),
            );
        }
        let mut reps = std::collections::HashMap::new();
        for h in &pool {
            let rep = self.ce.history(&h.node_id).await.map(|r| r.delivered_work()).unwrap_or(0);
            reps.insert(h.node_id.clone(), rep);
        }
        placement::best(atlas, &req, |id| reps.get(id).copied().unwrap_or(0))
            .ok_or_else(|| anyhow!("no qualifying host (atlas changed during selection)"))
    }

    /// Deploy `f`: pick a host, place the handler over the mesh (billing flows through the job
    /// bid), record the deployment in the registry, and return it. `grant` is an optional
    /// capability token authorizing the deploy on the chosen host.
    pub async fn deploy(&mut self, f: Function, grant: Option<&str>) -> Result<Deployment> {
        Function::validate_name(&f.name)?;
        let chosen = self.select_host(&f).await?;
        self.deploy_on(f, &chosen.host.node_id, grant).await
    }

    /// Deploy `f` on a specific `host` node id (skips atlas selection — useful for pinning a
    /// function or for tests). Records and returns the deployment.
    pub async fn deploy_on(
        &mut self,
        f: Function,
        host: &str,
        grant: Option<&str>,
    ) -> Result<Deployment> {
        Function::validate_name(&f.name)?;
        let job_id = match &f.handler {
            Handler::Container { image, cmd } => {
                let spec = ce_rs::BidSpec {
                    image: image.clone(),
                    cmd: cmd.clone(),
                    cpu_cores: f.cpu_cores,
                    mem_mb: f.mem_mb as u64,
                    duration_secs: f.duration_secs,
                    bid: f.bid,
                };
                self.ce.mesh_deploy(host, &spec, grant).await?
            }
            Handler::Wasm { module_hash, entry } => {
                let dep = self
                    .ce
                    .mesh_deploy_wasm(
                        host,
                        module_hash,
                        entry,
                        f.cpu_cores,
                        f.mem_mb as u64,
                        f.duration_secs,
                        f.bid,
                        grant,
                        &[],
                    )
                    .await?;
                dep.job_id
            }
        };

        let deployment = Deployment {
            function: f,
            host: host.to_string(),
            job_id,
            deployed_at: now_secs(),
        };
        self.registry.insert(deployment.clone());
        Ok(deployment)
    }

    /// Invoke a deployed function by name with `payload`, returning the handler's output bytes.
    /// Sends an [`InvokeRequest`] over the mesh to the host running the function and waits for its
    /// [`InvokeResponse`]. Errors if the function is unknown or the handler reports failure.
    pub async fn invoke(&self, name: &str, payload: &[u8]) -> Result<Vec<u8>> {
        self.invoke_with(name, payload, None, DEFAULT_INVOKE_TIMEOUT_MS).await
    }

    /// Invoke with an explicit capability token and timeout.
    pub async fn invoke_with(
        &self,
        name: &str,
        payload: &[u8],
        caps: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Vec<u8>> {
        let d = self
            .registry
            .get(name)
            .ok_or_else(|| anyhow!("no deployed function named '{name}' (deploy it first)"))?;
        let mut req = InvokeRequest::new(name, payload);
        if let Some(c) = caps {
            req = req.with_caps(c);
        }
        let reply = self.ce.request(&d.host, INVOKE_TOPIC, &req.encode(), timeout_ms).await?;
        let resp = InvokeResponse::decode(&reply)?;
        if !resp.ok {
            bail!("function '{name}' failed: {}", resp.error.unwrap_or_else(|| "remote error".into()));
        }
        resp.output()
    }

    /// Stop a deployed function: kill its cell on the host and forget it. Returns the removed
    /// deployment. `grant` authorizes the kill on the host.
    pub async fn kill(&mut self, name: &str, grant: Option<&str>) -> Result<Deployment> {
        let d = self
            .registry
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("no deployed function named '{name}'"))?;
        self.ce.mesh_kill(&d.host, &d.job_id, grant).await?;
        self.registry
            .remove(name)
            .ok_or_else(|| anyhow!("function '{name}' vanished from registry"))
    }

    /// Bind a deployed function to a pubsub `topic`: subscribe, then invoke the function once for
    /// every event published on the topic, forwarding the event's data as the invocation payload.
    /// Runs until `shutdown` resolves (e.g. ctrl-c). For each event, `on_result` is called with the
    /// invocation outcome so callers can log it. Best-effort, at-most-once (the inbox ring is
    /// bounded) — matching CE pubsub delivery semantics.
    pub async fn run_trigger<F>(
        &self,
        name: &str,
        topic: &str,
        caps: Option<&str>,
        mut on_result: F,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<()>
    where
        F: FnMut(&TriggerEvent, &Result<Vec<u8>>),
    {
        // Confirm the function exists before subscribing.
        if self.registry.get(name).is_none() {
            bail!("no deployed function named '{name}' (deploy it first)");
        }
        self.ce.subscribe(topic).await?;

        let mut seen = SeenWindow::new(8192);
        tokio::pin!(shutdown);
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = tick.tick() => {
                    let messages = self.ce.messages().await.unwrap_or_default();
                    for m in messages {
                        if m.topic != topic {
                            continue;
                        }
                        let fp = fingerprint(&m.from, &m.topic, &m.payload_hex, m.received_at);
                        if !seen.insert(fp) {
                            continue; // already handled
                        }
                        let bytes = m.payload().unwrap_or_default();
                        let event = TriggerEvent::decode_lenient(topic, &bytes);
                        let data = event.data().unwrap_or_default();
                        let outcome = self.invoke_with(name, &data, caps, DEFAULT_INVOKE_TIMEOUT_MS).await;
                        on_result(&event, &outcome);
                    }
                }
            }
        }
        Ok(())
    }
}

/// Convenience: per-invocation bid as whole credits.
pub fn bid_credits(n: u64) -> Amount {
    Amount::from_credits(n)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn tag_note(select: &[String]) -> String {
    if select.is_empty() {
        String::new()
    } else {
        format!(" + tags [{}]", select.join(","))
    }
}

/// A 64-bit fingerprint of a received message, used to de-dup the bounded inbox ring across polls.
fn fingerprint(from: &str, topic: &str, payload_hex: &str, received_at: u64) -> u64 {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(from.as_bytes());
    h.update([0]);
    h.update(topic.as_bytes());
    h.update([0]);
    h.update(payload_hex.as_bytes());
    h.update(received_at.to_le_bytes());
    let d = h.finalize();
    u64::from_le_bytes(d[..8].try_into().unwrap_or([0; 8]))
}

/// A bounded sliding set of recently-seen fingerprints (FIFO eviction). Mirrors ce-coord's pump
/// de-dup so a bounded inbox ring does not double-fire a trigger.
struct SeenWindow {
    cap: usize,
    set: std::collections::HashSet<u64>,
    order: std::collections::VecDeque<u64>,
}

impl SeenWindow {
    fn new(cap: usize) -> Self {
        SeenWindow {
            cap,
            set: std::collections::HashSet::with_capacity(cap),
            order: std::collections::VecDeque::with_capacity(cap),
        }
    }

    /// Insert `fp`; returns true if it was new (should be processed), false if already seen.
    fn insert(&mut self, fp: u64) -> bool {
        if self.set.contains(&fp) {
            return false;
        }
        if self.order.len() >= self.cap
            && let Some(old) = self.order.pop_front()
        {
            self.set.remove(&old);
        }
        self.set.insert(fp);
        self.order.push_back(fp);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seen_window_dedups_and_evicts() {
        let mut w = SeenWindow::new(2);
        assert!(w.insert(1)); // new
        assert!(!w.insert(1)); // dup
        assert!(w.insert(2)); // new
        assert!(w.insert(3)); // evicts 1
        assert!(w.insert(1)); // 1 was evicted → new again
    }

    #[test]
    fn fingerprint_is_stable_and_distinct() {
        let a = fingerprint("node", "topic", "aa", 10);
        let b = fingerprint("node", "topic", "aa", 10);
        assert_eq!(a, b, "same inputs → same fingerprint");
        let c = fingerprint("node", "topic", "aa", 11);
        assert_ne!(a, c, "different received_at → different fingerprint");
        let d = fingerprint("node2", "topic", "aa", 10);
        assert_ne!(a, d, "different sender → different fingerprint");
    }

    #[test]
    fn bid_credits_helper() {
        assert_eq!(bid_credits(3).base(), Amount::from_credits(3).base());
    }

    #[test]
    fn requirements_match_handler_kind() {
        let container = Function {
            name: "c".into(),
            handler: Handler::Container { image: "alpine".into(), cmd: vec![] },
            cpu_cores: 2,
            mem_mb: 256,
            duration_secs: 60,
            bid: Amount::from_credits(1),
            select: vec!["gpu".into()],
        };
        let r = FnClient::requirements(&container);
        assert!(r.require_docker);
        assert!(r.select.contains(&"gpu".to_string()));

        let wasm = Function {
            name: "w".into(),
            handler: Handler::Wasm { module_hash: "00".repeat(32), entry: "_start".into() },
            cpu_cores: 1,
            mem_mb: 64,
            duration_secs: 30,
            bid: Amount::from_credits(1),
            select: vec![],
        };
        let r = FnClient::requirements(&wasm);
        assert!(!r.require_docker);
        assert!(r.select.contains(&"wasm".to_string()));
    }
}
