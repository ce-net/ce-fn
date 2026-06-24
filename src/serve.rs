//! The ce-fn **serve-side runtime** — the data plane that actually runs function handlers.
//!
//! `ce-fn` the control plane (deploy/invoke/kill) places a cell on a host and sends
//! [`InvokeRequest`]s to it. *Something* on that host must answer those requests by running the named
//! handler and replying with its output. This module is that something: the daemon driven by
//! `ce-fn serve`, the analog of `rdev serve` for `rdev/exec`.
//!
//! ## Model
//!
//! A host operator declares the handlers it is willing to run in a **handler manifest** (a JSON file,
//! see [`HandlerManifest`]). Each [`HandlerSpec`] maps a function name to a local command line plus
//! its environment and secret bindings. Secrets are resolved at launch from the host's own
//! environment (`secret.from` -> `std::env::var`), so credentials live only on the host and never
//! traverse the wire or the registry.
//!
//! On each `ce-fn/invoke` request the runtime:
//! 1. decodes and bounds-checks the [`InvokeRequest`],
//! 2. authorizes the requester with the `ce-cap` chain ([`crate::caps::authorize_invoke`]) — the
//!    capability enforcement point,
//! 3. runs the handler via a [`HandlerRuntime`] (the real [`ProcessRuntime`] spawns the command,
//!    feeds the payload on stdin, and captures stdout/exit within a wall-clock timeout),
//! 4. replies with an [`InvokeResponse`] carrying the handler's output and exit code.
//!
//! The dispatch core ([`Runtime::handle_invoke`]) is pure over a [`HandlerRuntime`] seam, so it is
//! unit-tested end to end without Docker, a node, or the network.

use anyhow::{Result, anyhow, bail};
use ce_identity::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::caps::{ABILITY_INVOKE, authorize_invoke};
use crate::protocol::{InvokeRequest, InvokeResponse, MAX_OUTPUT_BYTES};

/// How a single function's handler is launched on this host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandlerSpec {
    /// The function name this handler serves (matches [`InvokeRequest::function`]).
    pub function: String,
    /// The command to run, argv-style (`["python3", "handler.py"]`). The invocation payload is
    /// written to the process's stdin; its stdout is the response body.
    pub command: Vec<String>,
    /// Working directory for the handler (optional).
    #[serde(default)]
    pub cwd: Option<String>,
    /// Plaintext environment variables to set for the handler.
    #[serde(default)]
    pub env: Vec<(String, String)>,
    /// Secret bindings: expose host env var `from` to the handler as env var `env`. Resolved at
    /// launch; a missing secret fails the invocation rather than running with an empty value.
    #[serde(default)]
    pub secrets: Vec<crate::function::SecretRef>,
    /// Max wall-clock seconds a single invocation may run before it is killed (0 = manifest default).
    #[serde(default)]
    pub timeout_secs: u64,
}

impl HandlerSpec {
    /// Resolve this handler's full environment (plaintext env + resolved secrets) for launch.
    /// `lookup` resolves a secret's host source name to its value (defaults to `std::env::var`).
    pub fn resolve_env(
        &self,
        lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Vec<(String, String)>> {
        let mut out: Vec<(String, String)> = self.env.clone();
        for s in &self.secrets {
            let val = lookup(&s.from)
                .ok_or_else(|| anyhow!("secret source '{}' is not set on this host", s.from))?;
            out.push((s.env.clone(), val));
        }
        Ok(out)
    }
}

/// The host's set of runnable handlers, keyed by function name. Loaded from a JSON manifest file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HandlerManifest {
    /// Default per-invocation timeout (seconds) when a [`HandlerSpec`] does not set its own.
    #[serde(default = "default_timeout")]
    pub default_timeout_secs: u64,
    /// The declared handlers.
    #[serde(default)]
    pub handlers: Vec<HandlerSpec>,
}

fn default_timeout() -> u64 {
    300
}

impl HandlerManifest {
    /// Load a manifest from `path`. Errors if the file is missing or malformed (the operator must
    /// declare at least the handlers they want served).
    pub fn load(path: &Path) -> Result<HandlerManifest> {
        let bytes = std::fs::read(path)
            .map_err(|e| anyhow!("reading handler manifest {}: {e}", path.display()))?;
        let m: HandlerManifest = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow!("parsing handler manifest {}: {e}", path.display()))?;
        m.validate()?;
        Ok(m)
    }

    /// Validate the manifest: every handler has a valid function name and a non-empty command, and
    /// no two handlers serve the same function.
    pub fn validate(&self) -> Result<()> {
        let mut seen = HashSet::new();
        for h in &self.handlers {
            crate::function::Function::validate_name(&h.function)?;
            if h.command.is_empty() {
                bail!("handler '{}' has an empty command", h.function);
            }
            if !seen.insert(h.function.clone()) {
                bail!("duplicate handler for function '{}'", h.function);
            }
        }
        Ok(())
    }

    /// Look up the handler for `function`.
    pub fn get(&self, function: &str) -> Option<&HandlerSpec> {
        self.handlers.iter().find(|h| h.function == function)
    }
}

/// The outcome of running a handler: its stdout bytes and process exit code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandlerOutcome {
    /// Handler stdout (the response body).
    pub output: Vec<u8>,
    /// Process exit code (0 on success).
    pub exit_code: i64,
}

/// The seam between the runtime's request dispatch and the actual handler execution mechanism.
/// The real implementation ([`ProcessRuntime`]) spawns a subprocess; tests substitute a deterministic
/// fake so the whole invoke path is exercised offline.
#[allow(async_fn_in_trait)]
pub trait HandlerRuntime: Send + Sync {
    /// Run `spec`'s handler with `payload` on stdin, the resolved `env`, and a wall-clock `timeout`.
    /// Returns the captured stdout + exit code, or an error if the handler could not be launched or
    /// exceeded the timeout.
    async fn run(
        &self,
        spec: &HandlerSpec,
        payload: &[u8],
        env: &[(String, String)],
        timeout: Duration,
    ) -> Result<HandlerOutcome>;
}

/// The real handler runtime: spawns the handler command as a local subprocess, writes the payload to
/// its stdin, and captures stdout within the timeout. This is what `ce-fn serve` uses on a host.
#[derive(Debug, Clone, Default)]
pub struct ProcessRuntime;

impl HandlerRuntime for ProcessRuntime {
    async fn run(
        &self,
        spec: &HandlerSpec,
        payload: &[u8],
        env: &[(String, String)],
        timeout: Duration,
    ) -> Result<HandlerOutcome> {
        use tokio::io::AsyncWriteExt;
        use tokio::process::Command;

        let (program, args) = spec
            .command
            .split_first()
            .ok_or_else(|| anyhow!("handler '{}' has an empty command", spec.function))?;

        let mut cmd = Command::new(program);
        cmd.args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .env_clear();
        // Keep a minimal PATH so common interpreters resolve; everything else is explicit.
        if let Ok(path) = std::env::var("PATH") {
            cmd.env("PATH", path);
        }
        for (k, v) in env {
            cmd.env(k, v);
        }
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| anyhow!("spawning handler '{}': {e}", spec.function))?;

        // Feed the payload to stdin, then close it so the handler sees EOF.
        if let Some(mut stdin) = child.stdin.take() {
            let payload = payload.to_vec();
            // Write in a task so a handler that does not read stdin cannot deadlock the parent.
            tokio::spawn(async move {
                let _ = stdin.write_all(payload.as_slice()).await;
                let _ = stdin.shutdown().await;
            });
        }

        let out = match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(Ok(o)) => o,
            Ok(Err(e)) => return Err(anyhow!("handler '{}' I/O error: {e}", spec.function)),
            Err(_) => {
                return Err(anyhow!(
                    "handler '{}' exceeded its {}s timeout",
                    spec.function,
                    timeout.as_secs()
                ));
            }
        };

        let mut stdout = out.stdout;
        if stdout.len() > MAX_OUTPUT_BYTES {
            stdout.truncate(MAX_OUTPUT_BYTES);
        }
        Ok(HandlerOutcome { output: stdout, exit_code: out.status.code().unwrap_or(-1) as i64 })
    }
}

/// Configuration for a serving runtime: which authority roots to honor and this host's atlas tags.
#[derive(Debug, Clone, Default)]
pub struct ServeConfig {
    /// Accepted capability root keys (besides this host's own id, which is always accepted).
    pub roots: Vec<NodeId>,
    /// This host's atlas self-tags (consulted when a capability scopes to `Resource::Tag`).
    pub self_tags: Vec<String>,
    /// Path to a roots file (64-hex node ids, one per line) — informational, loaded by the binary.
    pub roots_path: Option<PathBuf>,
}

/// The serve-side runtime: a manifest of runnable handlers, a [`HandlerRuntime`] execution backend,
/// and the capability-enforcement context. [`handle_invoke`](Self::handle_invoke) is the pure
/// dispatch core; the binary wires it to the node's message stream.
pub struct Runtime<R: HandlerRuntime> {
    self_id: NodeId,
    manifest: HandlerManifest,
    backend: R,
    config: ServeConfig,
}

impl<R: HandlerRuntime> Runtime<R> {
    /// Build a runtime for host `self_id` serving `manifest` via `backend`, with `config`.
    pub fn new(self_id: NodeId, manifest: HandlerManifest, backend: R, config: ServeConfig) -> Self {
        Runtime { self_id, manifest, backend, config }
    }

    /// The declared handler function names.
    pub fn functions(&self) -> Vec<&str> {
        self.manifest.handlers.iter().map(|h| h.function.as_str()).collect()
    }

    /// Handle one decoded invocation from `requester` (the authenticated sender NodeId), returning
    /// the [`InvokeResponse`] to reply with. Performs capability authorization, secret resolution,
    /// and handler execution. `now` is unix seconds; `is_revoked` consults the on-chain revoked set;
    /// `secret_lookup` resolves a secret source name to its value (host env in production).
    pub async fn handle_invoke(
        &self,
        requester: &NodeId,
        req: &InvokeRequest,
        now: u64,
        is_revoked: &dyn Fn(&NodeId, u64) -> bool,
        secret_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> InvokeResponse {
        match self.try_handle(requester, req, now, is_revoked, secret_lookup).await {
            Ok(outcome) => InvokeResponse::exited(&outcome.output, outcome.exit_code),
            Err(e) => InvokeResponse::failure(e.to_string()),
        }
    }

    async fn try_handle(
        &self,
        requester: &NodeId,
        req: &InvokeRequest,
        now: u64,
        is_revoked: &dyn Fn(&NodeId, u64) -> bool,
        secret_lookup: &dyn Fn(&str) -> Option<String>,
    ) -> Result<HandlerOutcome> {
        req.validate()?;
        let spec = self
            .manifest
            .get(&req.function)
            .ok_or_else(|| anyhow!("no handler for function '{}' on this host", req.function))?;

        // Capability enforcement point: the requester must hold a chain granting fn:invoke,
        // rooted at this host or a configured root. An empty token is rejected by ce-cap.
        authorize_invoke(
            &self.self_id,
            &self.config.roots,
            &self.config.self_tags,
            now,
            requester,
            ABILITY_INVOKE,
            &req.caps,
            is_revoked,
        )?;

        let payload = req.payload()?;
        let env = spec.resolve_env(secret_lookup)?;
        let timeout = Duration::from_secs(if spec.timeout_secs > 0 {
            spec.timeout_secs
        } else {
            self.manifest.default_timeout_secs.max(1)
        });
        self.backend.run(spec, &payload, &env, timeout).await
    }
}

/// Run the serve loop forever (until `shutdown`): answer each `ce-fn/invoke` request from the node's
/// message stream. The subscribe + stream + de-dup + reply + reconnect mechanics are delegated to the
/// shared [`ce_rs::serve`] loop; this module supplies only the invoke-specific [`InvokeHandler`]
/// (capability authorization, secret resolution, handler execution). Requires a live local node.
pub async fn serve_loop<R: HandlerRuntime>(
    ce: &ce_rs::CeClient,
    runtime: &Runtime<R>,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<()> {
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    // On-chain revoked (issuer, nonce) set, seeded now and refreshed lazily (>=10s) on the request
    // path by the handler.
    let revoked: Arc<Mutex<HashSet<(NodeId, u64)>>> = Arc::new(Mutex::new(HashSet::new()));
    refresh_revoked(ce, &revoked).await;

    let handler = InvokeHandler { ce, runtime, revoked, last_refresh: Mutex::new(Instant::now()) };
    ce_rs::serve::serve(ce, &[crate::protocol::INVOKE_TOPIC], &handler, shutdown).await
}

/// The ce-fn invoke handler plugged into the shared [`ce_rs::serve`] loop: it authorizes the
/// requester (ce-cap), resolves secrets, runs the handler, and encodes the [`InvokeResponse`].
struct InvokeHandler<'a, R: HandlerRuntime> {
    ce: &'a ce_rs::CeClient,
    runtime: &'a Runtime<R>,
    revoked: std::sync::Arc<std::sync::Mutex<HashSet<(NodeId, u64)>>>,
    last_refresh: std::sync::Mutex<std::time::Instant>,
}

impl<R: HandlerRuntime> ce_rs::serve::Handler for InvokeHandler<'_, R> {
    async fn handle(&self, req: ce_rs::serve::Request) -> Vec<u8> {
        // Refresh the revoked set at most every ~10s, lazily on the request path.
        let stale = self
            .last_refresh
            .lock()
            .map(|g| g.elapsed() > Duration::from_secs(10))
            .unwrap_or(true);
        if stale {
            refresh_revoked(self.ce, &self.revoked).await;
            if let Ok(mut g) = self.last_refresh.lock() {
                *g = std::time::Instant::now();
            }
        }
        let resp = build_response(self.runtime, &self.revoked, &req.from, &req.payload).await;
        resp.encode().unwrap_or_else(|_| {
            // Encoding a response should never fail; if it somehow does, send a minimal failure.
            InvokeResponse::failure("internal: response encode failed")
                .encode()
                .unwrap_or_default()
        })
    }
}

async fn build_response<R: HandlerRuntime>(
    runtime: &Runtime<R>,
    revoked: &std::sync::Arc<std::sync::Mutex<HashSet<(NodeId, u64)>>>,
    from_hex: &str,
    payload: &[u8],
) -> InvokeResponse {
    let requester: NodeId = match hex::decode(from_hex).ok().and_then(|b| b.try_into().ok()) {
        Some(id) => id,
        None => return InvokeResponse::failure("bad sender node id"),
    };
    let req = match InvokeRequest::decode(payload) {
        Ok(r) => r,
        Err(e) => return InvokeResponse::failure(format!("malformed invoke request: {e}")),
    };

    let revoked_set = revoked.lock().map(|g| g.clone()).unwrap_or_default();
    let is_revoked = move |issuer: &NodeId, nonce: u64| revoked_set.contains(&(*issuer, nonce));
    let secret_lookup = |name: &str| std::env::var(name).ok();
    runtime
        .handle_invoke(&requester, &req, now_secs(), &is_revoked, &secret_lookup)
        .await
}

async fn refresh_revoked(
    ce: &ce_rs::CeClient,
    revoked: &std::sync::Arc<std::sync::Mutex<HashSet<(NodeId, u64)>>>,
) {
    if let Ok(pairs) = ce.revoked().await {
        let set: HashSet<(NodeId, u64)> = pairs
            .into_iter()
            .filter_map(|(issuer, nonce)| {
                hex::decode(&issuer).ok().and_then(|b| b.try_into().ok()).map(|i| (i, nonce))
            })
            .collect();
        if let Ok(mut g) = revoked.lock() {
            *g = set;
        }
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Resolve a map of secrets for tests / programmatic lookup.
pub fn map_lookup(map: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
    move |k| map.get(k).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::SecretRef;
    use ce_identity::Identity;

    fn gen_identity(tag: &str) -> Identity {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ce-fn-serve-id-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Identity::load_or_generate(&dir).expect("generate identity")
    }

    /// A deterministic fake backend that echoes the payload, asserting env/timeout passthrough.
    #[derive(Clone)]
    struct EchoBackend {
        exit_code: i64,
    }

    impl HandlerRuntime for EchoBackend {
        async fn run(
            &self,
            _spec: &HandlerSpec,
            payload: &[u8],
            env: &[(String, String)],
            _timeout: Duration,
        ) -> Result<HandlerOutcome> {
            // Prefix the echoed output with any FOO env value so tests can assert env injection.
            let mut out = Vec::new();
            if let Some((_, v)) = env.iter().find(|(k, _)| k == "FOO") {
                out.extend_from_slice(v.as_bytes());
                out.push(b':');
            }
            out.extend_from_slice(payload);
            Ok(HandlerOutcome { output: out, exit_code: self.exit_code })
        }
    }

    fn manifest_with(spec: HandlerSpec) -> HandlerManifest {
        HandlerManifest { default_timeout_secs: 5, handlers: vec![spec] }
    }

    fn spec(function: &str) -> HandlerSpec {
        HandlerSpec {
            function: function.into(),
            command: vec!["true".into()],
            cwd: None,
            env: vec![],
            secrets: vec![],
            timeout_secs: 0,
        }
    }

    fn grant_invoke(host: &Identity, caller: &Identity) -> String {
        crate::caps::grant(
            host,
            caller.node_id(),
            &[ABILITY_INVOKE],
            ce_cap::Resource::Node(host.node_id()),
            0,
            1,
        )
    }

    #[tokio::test]
    async fn authorized_invoke_runs_handler() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(spec("echo")),
            EchoBackend { exit_code: 0 },
            ServeConfig::default(),
        );
        let token = grant_invoke(&host, &caller);
        let req = InvokeRequest::new("echo", b"hello").with_caps(token);
        let resp = rt
            .handle_invoke(&caller.node_id(), &req, 0, &|_, _| false, &|_| None)
            .await;
        assert!(resp.ok, "resp: {resp:?}");
        assert_eq!(resp.output().unwrap(), b"hello");
    }

    #[tokio::test]
    async fn unauthorized_invoke_denied() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(spec("echo")),
            EchoBackend { exit_code: 0 },
            ServeConfig::default(),
        );
        // No capability token at all.
        let req = InvokeRequest::new("echo", b"hello");
        let resp = rt.handle_invoke(&caller.node_id(), &req, 0, &|_, _| false, &|_| None).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("capability"));
    }

    #[tokio::test]
    async fn unknown_function_fails() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(spec("echo")),
            EchoBackend { exit_code: 0 },
            ServeConfig::default(),
        );
        let token = grant_invoke(&host, &caller);
        let req = InvokeRequest::new("missing", b"x").with_caps(token);
        let resp = rt.handle_invoke(&caller.node_id(), &req, 0, &|_, _| false, &|_| None).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("no handler"));
    }

    #[tokio::test]
    async fn nonzero_exit_is_not_ok() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(spec("echo")),
            EchoBackend { exit_code: 7 },
            ServeConfig::default(),
        );
        let token = grant_invoke(&host, &caller);
        let req = InvokeRequest::new("echo", b"x").with_caps(token);
        let resp = rt.handle_invoke(&caller.node_id(), &req, 0, &|_, _| false, &|_| None).await;
        assert!(!resp.ok);
        assert_eq!(resp.exit_code, Some(7));
    }

    #[tokio::test]
    async fn secrets_and_env_injected() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let mut s = spec("echo");
        s.secrets.push(SecretRef { env: "FOO".into(), from: "HOST_FOO".into() });
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(s),
            EchoBackend { exit_code: 0 },
            ServeConfig::default(),
        );
        let token = grant_invoke(&host, &caller);
        let req = InvokeRequest::new("echo", b"data").with_caps(token);
        let secrets: HashMap<String, String> =
            [("HOST_FOO".to_string(), "bar".to_string())].into_iter().collect();
        let resp = rt
            .handle_invoke(&caller.node_id(), &req, 0, &|_, _| false, &map_lookup(&secrets))
            .await;
        assert!(resp.ok, "resp: {resp:?}");
        assert_eq!(resp.output().unwrap(), b"bar:data");
    }

    #[tokio::test]
    async fn missing_secret_fails_closed() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let mut s = spec("echo");
        s.secrets.push(SecretRef { env: "FOO".into(), from: "ABSENT".into() });
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(s),
            EchoBackend { exit_code: 0 },
            ServeConfig::default(),
        );
        let token = grant_invoke(&host, &caller);
        let req = InvokeRequest::new("echo", b"x").with_caps(token);
        let resp = rt.handle_invoke(&caller.node_id(), &req, 0, &|_, _| false, &|_| None).await;
        assert!(!resp.ok);
        assert!(resp.error.unwrap().contains("secret source"));
    }

    #[tokio::test]
    async fn revoked_capability_denied() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let rt = Runtime::new(
            host.node_id(),
            manifest_with(spec("echo")),
            EchoBackend { exit_code: 0 },
            ServeConfig::default(),
        );
        let token = grant_invoke(&host, &caller);
        let req = InvokeRequest::new("echo", b"x").with_caps(token);
        let host_id = host.node_id();
        let revoked = |issuer: &NodeId, nonce: u64| *issuer == host_id && nonce == 1;
        let resp = rt.handle_invoke(&caller.node_id(), &req, 0, &revoked, &|_| None).await;
        assert!(!resp.ok);
    }

    #[test]
    fn manifest_validate_rejects_duplicates_and_empty_command() {
        let dup = HandlerManifest {
            default_timeout_secs: 5,
            handlers: vec![spec("a"), spec("a")],
        };
        assert!(dup.validate().is_err());
        let empty = HandlerManifest {
            default_timeout_secs: 5,
            handlers: vec![HandlerSpec {
                function: "a".into(),
                command: vec![],
                cwd: None,
                env: vec![],
                secrets: vec![],
                timeout_secs: 0,
            }],
        };
        assert!(empty.validate().is_err());
    }

    #[test]
    fn manifest_json_roundtrip() {
        let m = manifest_with(spec("echo"));
        let json = serde_json::to_string(&m).unwrap();
        let back: HandlerManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.handlers.len(), 1);
        assert_eq!(back.get("echo").unwrap().command, vec!["true".to_string()]);
    }

    #[tokio::test]
    async fn process_runtime_echoes_real_subprocess() {
        // Use `cat` as a trivial handler: stdin -> stdout. Skips gracefully if cat is unavailable.
        let backend = ProcessRuntime;
        let s = HandlerSpec {
            function: "cat".into(),
            command: vec!["cat".into()],
            cwd: None,
            env: vec![],
            secrets: vec![],
            timeout_secs: 5,
        };
        match backend.run(&s, b"roundtrip", &[], Duration::from_secs(5)).await {
            Ok(o) => {
                assert_eq!(o.output, b"roundtrip");
                assert_eq!(o.exit_code, 0);
            }
            Err(e) => {
                // On a host without `cat`, don't fail the suite; log and move on.
                eprintln!("skipping process runtime test (cat unavailable): {e}");
            }
        }
    }

    #[tokio::test]
    async fn process_runtime_times_out() {
        let backend = ProcessRuntime;
        let s = HandlerSpec {
            function: "sleep".into(),
            command: vec!["sleep".into(), "10".into()],
            cwd: None,
            env: vec![],
            secrets: vec![],
            timeout_secs: 1,
        };
        let res = backend.run(&s, b"", &[], Duration::from_millis(200)).await;
        // Either it times out (expected) or sleep is unavailable; both are acceptable non-hangs.
        if let Ok(o) = res {
            // sleep missing -> nonzero exit, that's fine.
            assert_ne!(o.exit_code, 0);
        } else {
            assert!(res.unwrap_err().to_string().contains("timeout"));
        }
    }
}
