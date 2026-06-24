//! [`FnClient`] — the serverless control plane over a local CE node.
//!
//! `FnClient` composes existing CE primitives into Cloud-Run / Cloud-Functions semantics:
//!
//! * **deploy** — pick the top atlas-ranked hosts ([`crate::placement`]) and place `replicas`
//!   handler cells with `mesh-deploy` (container) or `mesh_deploy_wasm` (WASM), with fallback to the
//!   next-ranked candidate when a host fails between the atlas read and the deploy. The bid flows
//!   through the normal job/credit escrow, so billing is the existing economy.
//! * **invoke** — send an [`InvokeRequest`] to a live replica over the authenticated `AppRequest`/
//!   reply primitive (`CeClient::request`) on [`INVOKE_TOPIC`]; HTTP-style request/response with
//!   bounded retry across replicas and recorded invocation accounting.
//! * **trigger** — subscribe to a pubsub topic and invoke the function once per published event,
//!   driven by the reliable SSE message stream with a sliding de-dup window.
//! * **kill** — stop every deployed cell with `mesh-kill`.
//!
//! State (which function lives on which hosts) is kept in a [`Registry`]; deploy records it, the
//! other verbs read it. No new node endpoints — everything is `ce-rs` over the local node.

use anyhow::{Result, anyhow, bail};
use ce_rs::{Amount, AppMessage, CeClient};
use std::collections::HashMap;
use std::time::Duration;

use crate::function::{Deployment, Function, Handler, Registry, Replica, validate_hex64};
use crate::placement::{self, Candidate, Requirements};
use crate::protocol::{INVOKE_TOPIC, InvokeRequest, InvokeResponse, TriggerEvent};

/// Default per-invocation request timeout (ms) — generous, since a cold container pull + run can
/// take a while. Callers can override via [`FnClient::invoke_with`].
pub const DEFAULT_INVOKE_TIMEOUT_MS: u64 = 600_000;

/// Number of attempts an invocation makes across the deployment's live replicas before failing.
pub const DEFAULT_INVOKE_ATTEMPTS: u32 = 3;

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

    /// Mutable access to the registry (e.g. to persist recorded stats).
    pub fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
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
    pub async fn select_host(&self, f: &Function) -> Result<Candidate> {
        self.select_hosts(f, 1).await?.into_iter().next().ok_or_else(|| {
            anyhow!("no qualifying host (atlas changed during selection)")
        })
    }

    /// Choose up to `count` ranked candidate hosts for `f`. Reputation is fetched in parallel for the
    /// already-filtered pool only (bounded fan-out), each lookup guarded by a timeout so one slow
    /// node cannot stall placement. A history-fetch error leaves that host at zero reputation but
    /// still placeable (it just ranks lower) — a transient blip never excludes a host outright.
    pub async fn select_hosts(&self, f: &Function, count: usize) -> Result<Vec<Candidate>> {
        let atlas = self.ce.atlas().await?;
        let req = Self::requirements(f);

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

        // Fan out the per-host history lookups concurrently with a per-lookup timeout.
        let mut futs = Vec::with_capacity(pool.len());
        for h in &pool {
            let id = h.node_id.clone();
            let ce = &self.ce;
            futs.push(async move {
                let rep = match tokio::time::timeout(Duration::from_secs(5), ce.history(&id)).await {
                    Ok(Ok(r)) => r.delivered_work(),
                    _ => 0,
                };
                (id, rep)
            });
        }
        let reps: HashMap<String, u64> = futures_util::future::join_all(futs).await.into_iter().collect();

        Ok(placement::top(atlas, &req, count.max(1), |id| {
            reps.get(id).copied().unwrap_or(0)
        }))
    }

    /// Deploy `f`: place `f.replicas` handler cells across the top atlas-ranked hosts (billing flows
    /// through the job bids), record the deployment in the registry, and return it. `grant` is an
    /// optional capability token authorizing the deploy on the chosen hosts. Falls back to lower-
    /// ranked candidates when a chosen host fails (handles the atlas TOCTOU) and surfaces a clear
    /// error only if it cannot place at least one replica.
    pub async fn deploy(&mut self, f: Function, grant: Option<&str>) -> Result<Deployment> {
        f.validate()?;
        let want = f.replicas as usize;
        // Over-fetch candidates so we have fallbacks beyond the replica count.
        let candidates = self.select_hosts(&f, want + 3).await?;
        if candidates.is_empty() {
            bail!("no qualifying host for '{}'", f.name);
        }

        let mut placed: Vec<Replica> = Vec::new();
        let mut last_err: Option<anyhow::Error> = None;
        for c in &candidates {
            if placed.len() >= want {
                break;
            }
            // Skip a host we already placed a replica on (one replica per host).
            if placed.iter().any(|r| r.host == c.host.node_id) {
                continue;
            }
            match self.place_one(&f, &c.host.node_id, grant).await {
                Ok(job_id) => placed.push(Replica { host: c.host.node_id.clone(), job_id }),
                Err(e) => {
                    tracing::warn!(host = %short(&c.host.node_id), error = %e, "replica placement failed, trying next host");
                    last_err = Some(e);
                }
            }
        }

        if placed.is_empty() {
            return Err(last_err
                .unwrap_or_else(|| anyhow!("could not place any replica of '{}'", f.name)));
        }

        let deployment = Deployment {
            function: f,
            replicas: placed,
            revision: 1,
            deployed_at: now_secs(),
            stats: Default::default(),
        };
        self.registry.insert(deployment.clone());
        // insert may have bumped the revision; reflect that back to the caller.
        let stored = self.registry.get(&deployment.function.name).cloned().unwrap_or(deployment);
        Ok(stored)
    }

    /// Deploy `f` on a specific `host` node id (skips atlas selection — useful for pinning a
    /// function or for tests). Records and returns the deployment with a single replica.
    pub async fn deploy_on(
        &mut self,
        f: Function,
        host: &str,
        grant: Option<&str>,
    ) -> Result<Deployment> {
        f.validate()?;
        validate_hex64(host, "host node id")?;
        let job_id = self.place_one(&f, host, grant).await?;
        let deployment = Deployment {
            function: f,
            replicas: vec![Replica { host: host.to_string(), job_id }],
            revision: 1,
            deployed_at: now_secs(),
            stats: Default::default(),
        };
        self.registry.insert(deployment.clone());
        let stored = self.registry.get(&deployment.function.name).cloned().unwrap_or(deployment);
        Ok(stored)
    }

    /// Place a single replica cell of `f` on `host` and return its job id.
    async fn place_one(&self, f: &Function, host: &str, grant: Option<&str>) -> Result<String> {
        validate_hex64(host, "host node id")?;
        match &f.handler {
            Handler::Container { image, cmd } => {
                let spec = ce_rs::BidSpec {
                    image: image.clone(),
                    cmd: cmd.clone(),
                    cpu_cores: f.cpu_cores,
                    mem_mb: f.mem_mb as u64,
                    duration_secs: f.duration_secs,
                    bid: f.bid,
                };
                self.ce.mesh_deploy(host, &spec, grant).await
            }
            Handler::Wasm { module_hash, entry } => {
                validate_hex64(module_hash, "wasm module hash")?;
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
                Ok(dep.job_id)
            }
        }
    }

    /// Invoke a deployed function by name with `payload`, returning the handler's output bytes.
    /// Sends an [`InvokeRequest`] over the mesh to a live replica and waits for its
    /// [`InvokeResponse`], retrying across replicas on transport failure. Records invocation
    /// accounting in the registry. Errors if the function is unknown or every replica fails.
    pub async fn invoke(&mut self, name: &str, payload: &[u8]) -> Result<Vec<u8>> {
        self.invoke_with(name, payload, None, DEFAULT_INVOKE_TIMEOUT_MS).await
    }

    /// Invoke with an explicit capability token and timeout, load-balancing/retrying across the
    /// deployment's replicas (up to [`DEFAULT_INVOKE_ATTEMPTS`] attempts).
    pub async fn invoke_with(
        &mut self,
        name: &str,
        payload: &[u8],
        caps: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Vec<u8>> {
        // Resolve the replica set (cloned so we don't hold a borrow across the await/stat update).
        let replicas: Vec<Replica> = self
            .registry
            .get(name)
            .ok_or_else(|| anyhow!("no deployed function named '{name}' (deploy it first)"))?
            .replicas
            .clone();
        if replicas.is_empty() {
            bail!("function '{name}' has no live replicas");
        }

        let mut req = InvokeRequest::new(name, payload);
        if let Some(c) = caps {
            req = req.with_caps(c);
        }
        let body = req.encode()?;

        let start = std::time::Instant::now();
        let attempts = (DEFAULT_INVOKE_ATTEMPTS as usize).min(replicas.len().max(1)).max(1);
        let mut last_err: Option<anyhow::Error> = None;

        // Spread starting replica by call count so successive invokes round-robin across hosts.
        let base = self.registry.get(name).map(|d| d.stats.invocations as usize).unwrap_or(0);
        for i in 0..attempts {
            let r = &replicas[(base + i) % replicas.len()];
            match self.ce.request(&r.host, INVOKE_TOPIC, &body, timeout_ms).await {
                Ok(reply) => {
                    let outcome = Self::decode_invoke(name, &reply);
                    let ok = outcome.is_ok();
                    let latency = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
                    self.registry.record_invocation(name, ok, latency, now_secs());
                    return outcome;
                }
                Err(e) => {
                    tracing::warn!(function = name, host = %short(&r.host), attempt = i + 1, error = %e, "invoke attempt failed");
                    last_err = Some(e);
                    // Brief backoff before the next replica.
                    if i + 1 < attempts {
                        tokio::time::sleep(Duration::from_millis(100 * (i as u64 + 1))).await;
                    }
                }
            }
        }
        let latency = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
        self.registry.record_invocation(name, false, latency, now_secs());
        Err(last_err.unwrap_or_else(|| anyhow!("function '{name}' invocation failed")))
    }

    /// Decode and interpret an invoke reply, turning a handler failure into an error.
    fn decode_invoke(name: &str, reply: &[u8]) -> Result<Vec<u8>> {
        let resp = InvokeResponse::decode(reply)?;
        if !resp.ok {
            bail!(
                "function '{name}' failed: {}",
                resp.error.unwrap_or_else(|| "remote error".into())
            );
        }
        resp.output()
    }

    /// Stop a deployed function: kill every cell on its hosts and forget it. Returns the removed
    /// deployment. `grant` authorizes the kills on the hosts. Best-effort across replicas: a replica
    /// whose kill fails is logged, but the registry entry is still removed so it does not leak — use
    /// [`forget`](Self::forget) only when you intend to drop a record without killing.
    pub async fn kill(&mut self, name: &str, grant: Option<&str>) -> Result<Deployment> {
        let d = self
            .registry
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("no deployed function named '{name}'"))?;
        let mut errs = Vec::new();
        for r in &d.replicas {
            if let Err(e) = self.ce.mesh_kill(&r.host, &r.job_id, grant).await {
                tracing::warn!(host = %short(&r.host), job = %short(&r.job_id), error = %e, "mesh_kill failed");
                errs.push(format!("{}: {e}", short(&r.host)));
            }
        }
        let removed = self
            .registry
            .remove(name)
            .ok_or_else(|| anyhow!("function '{name}' vanished from registry"))?;
        if !errs.is_empty() {
            // The cells may still be running on those hosts; surface it but keep the registry clean.
            tracing::warn!(function = name, "kill: {} replica(s) could not be stopped: {}", errs.len(), errs.join("; "));
        }
        Ok(removed)
    }

    /// Drop a deployment record from the registry **without** killing its cells. For records whose
    /// host is unreachable (so `kill` cannot reach it) — the cell expires on its own duration/heartbeat
    /// timeout. Returns the removed record.
    pub fn forget(&mut self, name: &str) -> Result<Deployment> {
        self.registry
            .remove(name)
            .ok_or_else(|| anyhow!("no deployed function named '{name}'"))
    }

    /// Bind a deployed function to a pubsub `topic`: subscribe, then invoke the function once for
    /// every event published on the topic, forwarding the event's data as the invocation payload.
    /// Runs until `shutdown` resolves (e.g. ctrl-c). For each event, `on_result` is called with the
    /// invocation outcome so callers can log it.
    ///
    /// Delivery is driven by the node's **SSE message stream** (`messages_stream`), not the bounded
    /// inbox poll, so events are not silently dropped by ring GC; a sliding [`SeenWindow`] de-dups
    /// redeliveries. Each invocation already retries across replicas. Semantics are at-most-once at
    /// the trigger boundary (the stream is best-effort); durable at-least-once would layer a ce-coord
    /// log (the ce-pubsub product), out of scope here. On a stream error the loop reconnects with
    /// bounded backoff rather than exiting.
    pub async fn run_trigger<F>(
        &mut self,
        name: &str,
        topic: &str,
        caps: Option<&str>,
        mut on_result: F,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<()>
    where
        F: FnMut(&TriggerEvent, &Result<Vec<u8>>),
    {
        use futures_util::StreamExt as _;

        if self.registry.get(name).is_none() {
            bail!("no deployed function named '{name}' (deploy it first)");
        }
        self.ce.subscribe(topic).await?;

        // The message stream borrows the client for its whole lifetime; use a cloned client so the
        // stream does not conflict with the `&mut self` needed to invoke (which records stats).
        let stream_ce = self.ce.clone();

        let mut seen = SeenWindow::new(8192);
        let caps_owned = caps.map(|c| c.to_string());
        tokio::pin!(shutdown);

        let mut backoff_ms = 250u64;
        loop {
            // (Re)open the message stream. On failure, back off and retry unless shutting down.
            let stream = match stream_ce.messages_stream().await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(error = %e, "messages_stream open failed; backing off");
                    tokio::select! {
                        _ = &mut shutdown => break,
                        _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                    }
                    backoff_ms = (backoff_ms * 2).min(10_000);
                    continue;
                }
            };
            backoff_ms = 250;
            tokio::pin!(stream);

            loop {
                tokio::select! {
                    _ = &mut shutdown => return Ok(()),
                    item = stream.next() => {
                        match item {
                            Some(Ok(m)) => {
                                if let Some((event, outcome)) = self
                                    .handle_trigger_message(name, topic, caps_owned.as_deref(), &mut seen, &m)
                                    .await
                                {
                                    on_result(&event, &outcome);
                                }
                            }
                            Some(Err(e)) => {
                                tracing::warn!(error = %e, "message stream error; reconnecting");
                                break; // reopen the stream
                            }
                            None => {
                                tracing::debug!("message stream ended; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Process one streamed message for the trigger loop. Returns `Some((event, outcome))` if the
    /// message was a new event on `topic` that produced an invocation; `None` if filtered/deduped.
    async fn handle_trigger_message(
        &mut self,
        name: &str,
        topic: &str,
        caps: Option<&str>,
        seen: &mut SeenWindow,
        m: &AppMessage,
    ) -> Option<(TriggerEvent, Result<Vec<u8>>)> {
        if m.topic != topic {
            return None;
        }
        let fp = fingerprint(&m.from, &m.topic, &m.payload_hex, m.received_at);
        if !seen.insert(fp) {
            return None; // already handled
        }
        let bytes = match m.payload() {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "trigger event with bad payload hex; skipping");
                return None;
            }
        };
        let event = TriggerEvent::decode_lenient(topic, &bytes);
        let data = event.data().unwrap_or_default();
        let outcome = self.invoke_with(name, &data, caps, DEFAULT_INVOKE_TIMEOUT_MS).await;
        Some((event, outcome))
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

fn short(s: &str) -> &str {
    if s.len() > 12 { &s[..12] } else { s }
}

fn tag_note(select: &[String]) -> String {
    if select.is_empty() {
        String::new()
    } else {
        format!(" + tags [{}]", select.join(","))
    }
}

/// A 64-bit fingerprint of a received message, used to de-dup redeliveries on the trigger stream.
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
/// de-dup so redelivered events do not double-fire a trigger.
struct SeenWindow {
    cap: usize,
    set: std::collections::HashSet<u64>,
    order: std::collections::VecDeque<u64>,
}

impl SeenWindow {
    fn new(cap: usize) -> Self {
        SeenWindow {
            cap: cap.max(1),
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

    fn f1(name: &str, handler: Handler, select: Vec<String>) -> Function {
        Function {
            name: name.into(),
            handler,
            cpu_cores: 2,
            mem_mb: 256,
            duration_secs: 60,
            bid: Amount::from_credits(1),
            select,
            env: vec![],
            secrets: vec![],
            replicas: 1,
        }
    }

    #[test]
    fn requirements_match_handler_kind() {
        let container = f1("c", Handler::Container { image: "alpine".into(), cmd: vec![] }, vec!["gpu".into()]);
        let r = FnClient::requirements(&container);
        assert!(r.require_docker);
        assert!(r.select.contains(&"gpu".to_string()));

        let wasm = f1("w", Handler::Wasm { module_hash: "00".repeat(32), entry: "_start".into() }, vec![]);
        let r = FnClient::requirements(&wasm);
        assert!(!r.require_docker);
        assert!(r.select.contains(&"wasm".to_string()));
    }

    #[test]
    fn decode_invoke_surfaces_handler_failure() {
        let ok = InvokeResponse::success(b"out").encode().unwrap();
        assert_eq!(FnClient::decode_invoke("f", &ok).unwrap(), b"out");
        let bad = InvokeResponse::failure("nope").encode().unwrap();
        assert!(FnClient::decode_invoke("f", &bad).is_err());
    }
}
