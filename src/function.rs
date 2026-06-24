//! Function specs and the deployment registry.
//!
//! A [`Function`] is the deployable definition (name, handler, resources, per-invocation bid). A
//! [`Deployment`] is the record of a function placed on a concrete host — it carries the host node
//! id and the CE job id so later `invoke`/`kill`/`on` calls can find it. The [`Registry`] persists
//! deployments to a JSON file so the CLI is stateful across invocations (like `gcloud functions`
//! remembering your deployed functions).

use anyhow::{Result, anyhow, bail};
use ce_rs::Amount;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Current on-disk registry schema version. Bumped when the persisted shape changes; [`Registry::load`]
/// migrates older versions forward.
pub const REGISTRY_SCHEMA_VERSION: u32 = 1;

/// Maximum number of environment variables a function may declare (DoS bound on the spec).
pub const MAX_ENV_VARS: usize = 256;
/// Maximum byte length of a single env var name or value.
pub const MAX_ENV_LEN: usize = 32 * 1024;
/// Upper bound on replica count, so a typo cannot trigger thousands of deploys.
pub const MAX_REPLICAS: u32 = 1024;

/// The kind of handler a function runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Handler {
    /// A container image pulled and run per invocation (Cloud Run / Functions, container runtime).
    Container {
        /// Docker image reference (e.g. `myorg/resize:latest`).
        image: String,
        /// Command override; empty = the image entrypoint.
        #[serde(default)]
        cmd: Vec<String>,
    },
    /// A WASM module referenced by its content hash (uploaded to the blob store first).
    Wasm {
        /// 64-hex sha256 of the module (the blob hash returned by `put_blob`).
        module_hash: String,
        /// Exported entry point to call (e.g. `_start` or a named export).
        entry: String,
    },
}

impl Handler {
    /// Is this a WASM handler?
    pub fn is_wasm(&self) -> bool {
        matches!(self, Handler::Wasm { .. })
    }
}

/// A reference to a secret value the runtime mounts into the handler's environment. The secret
/// itself is **not** stored in the registry (it is resolved at launch from the named source), so a
/// committed registry never leaks credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    /// Environment variable name to expose the resolved secret as inside the handler.
    pub env: String,
    /// The secret's logical name in the host's secret source (resolved by the runtime).
    pub from: String,
}

/// A deployable serverless function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Function {
    /// Unique, human-chosen name for this function (the handle used by `invoke`/`on`/`kill`).
    pub name: String,
    /// What runs when the function is invoked.
    pub handler: Handler,
    /// CPU cores the handler needs.
    pub cpu_cores: u32,
    /// Memory in MiB the handler needs.
    pub mem_mb: u32,
    /// Wall-clock seconds a single invocation may run before the host reclaims it.
    pub duration_secs: u64,
    /// Maximum credits committed per deploy/invocation (the job bid, base units).
    pub bid: Amount,
    /// Extra host capability self-tags required for placement (e.g. `gpu`). `docker`/`wasm` are
    /// implied by the handler kind and need not be listed.
    #[serde(default)]
    pub select: Vec<String>,
    /// Plaintext environment variables injected into the handler (config, not secrets).
    #[serde(default)]
    pub env: Vec<(String, String)>,
    /// Secret references resolved at launch and exposed as env vars (never persisted in the clear).
    #[serde(default)]
    pub secrets: Vec<SecretRef>,
    /// Desired number of replica cells placed across the top-ranked hosts (>= 1). Multiple replicas
    /// give horizontal scale-out and host redundancy; `invoke` load-balances across the live set.
    #[serde(default = "one")]
    pub replicas: u32,
}

fn one() -> u32 {
    1
}

impl Function {
    /// Validate the function name: 1–64 chars, lowercase `a-z`/`0-9`/hyphen/underscore, not
    /// leading/trailing hyphen. Keeps names safe as registry keys and topic segments.
    pub fn validate_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > 64 {
            bail!("function name must be 1–64 characters");
        }
        let ok = name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_');
        if !ok {
            bail!("function name may contain only a-z, 0-9, '-', '_'");
        }
        if name.starts_with('-') || name.ends_with('-') {
            bail!("function name may not start or end with '-'");
        }
        Ok(())
    }

    /// Validate an environment variable name: non-empty, no `=` or NUL, bounded length. POSIX-ish.
    fn validate_env_name(name: &str) -> Result<()> {
        if name.is_empty() || name.len() > MAX_ENV_LEN {
            bail!("env var name must be 1..={MAX_ENV_LEN} bytes");
        }
        if name.contains('=') || name.contains('\0') {
            bail!("env var name '{name}' may not contain '=' or NUL");
        }
        Ok(())
    }

    /// Fully validate a function spec before deploy: name, handler hashes, resources, env, replicas.
    /// Every externally-supplied field is bounded here so a malformed spec is rejected client-side
    /// with a clear message rather than surfacing as an opaque downstream error.
    pub fn validate(&self) -> Result<()> {
        Self::validate_name(&self.name)?;
        self.handler.validate()?;
        if self.cpu_cores == 0 {
            bail!("cpu_cores must be >= 1");
        }
        if self.mem_mb == 0 {
            bail!("mem_mb must be >= 1");
        }
        if self.duration_secs == 0 {
            bail!("duration_secs must be >= 1");
        }
        if self.replicas == 0 || self.replicas > MAX_REPLICAS {
            bail!("replicas must be 1..={MAX_REPLICAS}");
        }
        if self.env.len() + self.secrets.len() > MAX_ENV_VARS {
            bail!("too many env vars / secrets (max {MAX_ENV_VARS})");
        }
        for (k, v) in &self.env {
            Self::validate_env_name(k)?;
            if v.len() > MAX_ENV_LEN {
                bail!("env var '{k}' value exceeds {MAX_ENV_LEN} bytes");
            }
        }
        for s in &self.secrets {
            Self::validate_env_name(&s.env)?;
            if s.from.is_empty() {
                bail!("secret for '{}' has an empty source name", s.env);
            }
        }
        Ok(())
    }
}

/// Validate that `s` is exactly 64 lowercase/uppercase hex chars (a 32-byte node id or sha256).
pub fn validate_hex64(s: &str, what: &str) -> Result<()> {
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("{what} must be 64 hex characters (got {} chars)", s.len());
    }
    Ok(())
}

impl Handler {
    /// Validate handler-specific invariants (the WASM module hash is a 64-hex sha256; image/entry
    /// non-empty).
    pub fn validate(&self) -> Result<()> {
        match self {
            Handler::Container { image, .. } => {
                if image.trim().is_empty() {
                    bail!("container image must not be empty");
                }
                if image.len() > 512 {
                    bail!("container image reference too long");
                }
                Ok(())
            }
            Handler::Wasm { module_hash, entry } => {
                validate_hex64(module_hash, "wasm module hash")?;
                if entry.trim().is_empty() {
                    bail!("wasm entry export must not be empty");
                }
                Ok(())
            }
        }
    }
}

/// A single placed replica cell of a function on one host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replica {
    /// The host node id (64-hex) this replica was placed on.
    pub host: String,
    /// The CE job id assigned by the host (used to kill it later).
    pub job_id: String,
}

/// Mutable invocation accounting for a deployment — the lightweight observability substrate.
/// Updated client-side on each [`crate::FnClient::invoke`] so `ls`/`describe` can show call volume,
/// error rate, and last-call time without a separate metrics service.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentStats {
    /// Total invocations attempted against this deployment.
    #[serde(default)]
    pub invocations: u64,
    /// Invocations that returned an error (transport, denial, or non-zero handler exit).
    #[serde(default)]
    pub errors: u64,
    /// Unix seconds of the most recent invocation (0 = never invoked).
    #[serde(default)]
    pub last_invoked: u64,
    /// Most recent observed round-trip latency in milliseconds (0 = never).
    #[serde(default)]
    pub last_latency_ms: u64,
}

/// A record of a function deployed on one or more hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    /// The function definition that was deployed.
    pub function: Function,
    /// The placed replica cells (>= 1). The first replica's host is the primary.
    pub replicas: Vec<Replica>,
    /// Monotonic revision number (incremented each time the function is re-deployed in place).
    #[serde(default = "one_u64")]
    pub revision: u64,
    /// Unix seconds the deployment was created.
    pub deployed_at: u64,
    /// Invocation accounting.
    #[serde(default)]
    pub stats: DeploymentStats,
}

fn one_u64() -> u64 {
    1
}

impl Deployment {
    /// The primary host node id (first replica). Empty only for a malformed record.
    pub fn host(&self) -> &str {
        self.replicas.first().map(|r| r.host.as_str()).unwrap_or("")
    }

    /// The primary job id (first replica).
    pub fn job_id(&self) -> &str {
        self.replicas.first().map(|r| r.job_id.as_str()).unwrap_or("")
    }

    /// Number of placed replicas.
    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }
}

/// A persisted set of deployments, keyed by function name. Stateful across CLI invocations.
///
/// On disk the registry is a versioned, atomically-written JSON document; concurrent CLI processes
/// coordinate via an advisory lock file taken around a load-modify-save (see [`Registry::with_lock`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    /// Schema version of the persisted document, for forward migration.
    #[serde(default)]
    schema: u32,
    #[serde(default)]
    deployments: BTreeMap<String, Deployment>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry { schema: REGISTRY_SCHEMA_VERSION, deployments: BTreeMap::new() }
    }
}

impl Registry {
    /// The default registry path: `$CE_FN_REGISTRY` if set, else `<config dir>/ce-fn/registry.json`.
    pub fn default_path() -> PathBuf {
        if let Ok(p) = std::env::var("CE_FN_REGISTRY")
            && !p.trim().is_empty()
        {
            return PathBuf::from(p);
        }
        if let Some(dirs) = directories_config_dir() {
            return dirs.join("ce-fn").join("registry.json");
        }
        PathBuf::from("ce-fn-registry.json")
    }

    /// Load a registry from `path`, returning an empty one if the file does not exist. Newer-than-
    /// supported schemas are rejected (a downgrade would silently drop fields); older ones migrate.
    pub fn load(path: &Path) -> Result<Registry> {
        match std::fs::read(path) {
            Ok(bytes) => {
                let mut reg: Registry = serde_json::from_slice(&bytes)
                    .map_err(|e| anyhow!("corrupt registry at {}: {e}", path.display()))?;
                if reg.schema > REGISTRY_SCHEMA_VERSION {
                    bail!(
                        "registry at {} is schema v{} but this ce-fn supports up to v{} — upgrade ce-fn",
                        path.display(),
                        reg.schema,
                        REGISTRY_SCHEMA_VERSION
                    );
                }
                reg.migrate();
                Ok(reg)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Registry::default()),
            Err(e) => Err(anyhow!("reading registry {}: {e}", path.display())),
        }
    }

    /// Migrate an older-schema registry forward in memory. Currently a no-op beyond stamping the
    /// version (serde `#[serde(default)]` fills new fields); the hook exists for future changes.
    fn migrate(&mut self) {
        self.schema = REGISTRY_SCHEMA_VERSION;
    }

    /// Atomically write the registry to `path`: serialize to a sibling temp file, fsync it, then
    /// rename over the target. A crash mid-write leaves the previous registry intact rather than a
    /// truncated/corrupt file. Parent directories are created as needed.
    pub fn save(&self, path: &Path) -> Result<()> {
        let mut doc = self.clone();
        doc.schema = REGISTRY_SCHEMA_VERSION;
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow!("creating {}: {e}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(&doc)?;
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| anyhow!("creating temp {}: {e}", tmp.display()))?;
            f.write_all(&bytes).map_err(|e| anyhow!("writing temp {}: {e}", tmp.display()))?;
            f.sync_all().map_err(|e| anyhow!("fsync temp {}: {e}", tmp.display()))?;
        }
        std::fs::rename(&tmp, path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            anyhow!("renaming {} -> {}: {e}", tmp.display(), path.display())
        })?;
        Ok(())
    }

    /// Run `f` under an advisory lock on `path` so two concurrent ce-fn processes cannot lose each
    /// other's updates across a load-modify-save. The lock is a sibling `<path>.lock` file held for
    /// the duration of `f`; it is created exclusively and removed afterward. `f` receives a freshly
    /// loaded registry and returns the (possibly mutated) registry plus its own result value; the
    /// registry is saved before the lock is released. Blocks (with bounded backoff) until the lock
    /// is acquired or a timeout elapses.
    pub fn with_lock<T>(
        path: &Path,
        f: impl FnOnce(&mut Registry) -> Result<T>,
    ) -> Result<T> {
        let lock = Lock::acquire(path)?;
        let mut reg = Registry::load(path)?;
        let out = f(&mut reg)?;
        reg.save(path)?;
        drop(lock);
        Ok(out)
    }

    /// Record (or replace) a deployment. If replacing, the revision is bumped and stats carried over.
    pub fn insert(&mut self, mut d: Deployment) {
        if let Some(prev) = self.deployments.get(&d.function.name) {
            d.revision = prev.revision.saturating_add(1);
            d.stats = prev.stats.clone();
        }
        self.deployments.insert(d.function.name.clone(), d);
    }

    /// Look up a deployment by function name.
    pub fn get(&self, name: &str) -> Option<&Deployment> {
        self.deployments.get(name)
    }

    /// Mutable access to a deployment (for updating stats).
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Deployment> {
        self.deployments.get_mut(name)
    }

    /// Remove a deployment by name; returns it if present.
    pub fn remove(&mut self, name: &str) -> Option<Deployment> {
        self.deployments.remove(name)
    }

    /// All deployments, sorted by function name.
    pub fn list(&self) -> Vec<&Deployment> {
        self.deployments.values().collect()
    }

    /// Record the outcome of an invocation against `name` (count, error, latency, last time).
    pub fn record_invocation(&mut self, name: &str, ok: bool, latency_ms: u64, at: u64) {
        if let Some(d) = self.deployments.get_mut(name) {
            d.stats.invocations = d.stats.invocations.saturating_add(1);
            if !ok {
                d.stats.errors = d.stats.errors.saturating_add(1);
            }
            d.stats.last_invoked = at;
            d.stats.last_latency_ms = latency_ms;
        }
    }
}

/// A best-effort advisory lock backed by exclusive creation of a `<path>.lock` file. Removed on
/// drop. Stale locks (older than [`LOCK_STALE_SECS`]) are reclaimed so a crashed process does not
/// wedge the registry forever.
struct Lock {
    path: PathBuf,
}

const LOCK_STALE_SECS: u64 = 30;

impl Lock {
    fn acquire(target: &Path) -> Result<Lock> {
        let path = lock_path(target);
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            let _ = std::fs::create_dir_all(parent);
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(LOCK_STALE_SECS / 2 + 1);
        loop {
            match std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut f) => {
                    let _ = write!(f, "{}", std::process::id());
                    return Ok(Lock { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Reclaim a stale lock left by a crashed process.
                    if let Ok(meta) = std::fs::metadata(&path)
                        && let Ok(modified) = meta.modified()
                        && let Ok(age) = modified.elapsed()
                        && age.as_secs() > LOCK_STALE_SECS
                    {
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    if std::time::Instant::now() >= deadline {
                        bail!(
                            "another ce-fn process holds the registry lock at {} — retry shortly",
                            path.display()
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => return Err(anyhow!("acquiring registry lock {}: {e}", path.display())),
            }
        }
    }
}

impl Drop for Lock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn lock_path(target: &Path) -> PathBuf {
    let mut name = target.file_name().map(|n| n.to_os_string()).unwrap_or_default();
    name.push(".lock");
    target.with_file_name(name)
}

/// Resolve a per-user config directory. Honors `XDG_CONFIG_HOME` first (explicit override on any
/// platform), then falls back to the platform-native config dir via the `directories` crate. This
/// is cross-platform: on Windows `$HOME` is typically unset (the relevant vars are `USERPROFILE`
/// / `%APPDATA%`), so a hardcoded `$HOME/.config` would yield `None` there. `ProjectDirs` resolves
/// `%APPDATA%\ce\ce-fn\config` on Windows, `~/Library/Application Support/...` on macOS, and the
/// XDG default on Linux.
fn directories_config_dir() -> Option<PathBuf> {
    if let Ok(home) = std::env::var("XDG_CONFIG_HOME")
        && !home.trim().is_empty()
    {
        return Some(PathBuf::from(home));
    }
    directories::ProjectDirs::from("net", "ce", "ce-fn").map(|p| p.config_dir().to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_fn(name: &str) -> Function {
        Function {
            name: name.to_string(),
            handler: Handler::Container { image: "alpine:latest".into(), cmd: vec!["echo".into(), "hi".into()] },
            cpu_cores: 1,
            mem_mb: 128,
            duration_secs: 60,
            bid: Amount::from_credits(1),
            select: vec![],
            env: vec![],
            secrets: vec![],
            replicas: 1,
        }
    }

    fn sample_dep(name: &str, host: &str, job: &str) -> Deployment {
        Deployment {
            function: sample_fn(name),
            replicas: vec![Replica { host: host.to_string(), job_id: job.to_string() }],
            revision: 1,
            deployed_at: 1,
            stats: DeploymentStats::default(),
        }
    }

    #[test]
    fn name_validation() {
        assert!(Function::validate_name("resize").is_ok());
        assert!(Function::validate_name("resize-thumb_2").is_ok());
        assert!(Function::validate_name("").is_err());
        assert!(Function::validate_name("-bad").is_err());
        assert!(Function::validate_name("bad-").is_err());
        assert!(Function::validate_name("Bad").is_err());
        assert!(Function::validate_name("has space").is_err());
        assert!(Function::validate_name(&"x".repeat(65)).is_err());
    }

    #[test]
    fn handler_wasm_flag() {
        assert!(!sample_fn("a").handler.is_wasm());
        let w = Handler::Wasm { module_hash: "ab".repeat(32), entry: "_start".into() };
        assert!(w.is_wasm());
    }

    #[test]
    fn function_json_roundtrip() {
        let f = sample_fn("resize");
        let json = serde_json::to_string(&f).unwrap();
        let back: Function = serde_json::from_str(&json).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn registry_insert_get_remove() {
        let mut r = Registry::default();
        assert!(r.get("resize").is_none());
        r.insert(sample_dep("resize", &"ab".repeat(32), &"cd".repeat(32)));
        assert_eq!(r.get("resize").unwrap().host(), "ab".repeat(32));
        assert_eq!(r.list().len(), 1);
        let removed = r.remove("resize").unwrap();
        assert_eq!(removed.function.name, "resize");
        assert!(r.get("resize").is_none());
    }

    #[test]
    fn registry_persist_roundtrip() {
        let dir = std::env::temp_dir().join(format!("ce-fn-test-{}", std::process::id()));
        let path = dir.join("registry.json");
        let _ = std::fs::remove_file(&path);

        let mut r = Registry::default();
        r.insert(sample_dep("a", &"11".repeat(32), &"22".repeat(32)));
        r.save(&path).unwrap();

        let loaded = Registry::load(&path).unwrap();
        assert_eq!(loaded.get("a").unwrap().job_id(), "22".repeat(32));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registry_load_missing_is_empty() {
        let path = std::env::temp_dir().join("ce-fn-definitely-missing-xyz.json");
        let _ = std::fs::remove_file(&path);
        let r = Registry::load(&path).unwrap();
        assert!(r.list().is_empty());
    }

    #[test]
    fn registry_save_is_atomic_no_temp_left() {
        let dir = std::env::temp_dir().join(format!("ce-fn-atomic-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("registry.json");
        let mut r = Registry::default();
        r.insert(sample_dep("a", &"33".repeat(32), &"44".repeat(32)));
        r.save(&path).unwrap();
        // No leftover temp files in the directory.
        let leftover: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
            .collect();
        assert!(leftover.is_empty(), "atomic save left a temp file");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registry_insert_bumps_revision_and_carries_stats() {
        let mut r = Registry::default();
        r.insert(sample_dep("a", &"11".repeat(32), &"22".repeat(32)));
        r.record_invocation("a", true, 5, 100);
        assert_eq!(r.get("a").unwrap().revision, 1);
        assert_eq!(r.get("a").unwrap().stats.invocations, 1);
        // Re-deploy: revision bumps, stats carry over.
        r.insert(sample_dep("a", &"99".repeat(32), &"88".repeat(32)));
        assert_eq!(r.get("a").unwrap().revision, 2);
        assert_eq!(r.get("a").unwrap().stats.invocations, 1);
        assert_eq!(r.get("a").unwrap().host(), "99".repeat(32));
    }

    #[test]
    fn registry_rejects_newer_schema() {
        let dir = std::env::temp_dir().join(format!("ce-fn-schema-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.json");
        std::fs::write(&path, br#"{"schema":9999,"deployments":{}}"#).unwrap();
        assert!(Registry::load(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registry_with_lock_serializes_mutation() {
        let dir = std::env::temp_dir().join(format!("ce-fn-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("registry.json");
        Registry::with_lock(&path, |r| {
            r.insert(sample_dep("a", &"11".repeat(32), &"22".repeat(32)));
            Ok(())
        })
        .unwrap();
        // Lock file is released after the closure.
        assert!(!lock_path(&path).exists());
        let loaded = Registry::load(&path).unwrap();
        assert!(loaded.get("a").is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn function_validate_catches_bad_specs() {
        let mut f = sample_fn("ok");
        assert!(f.validate().is_ok());
        f.replicas = 0;
        assert!(f.validate().is_err());
        f.replicas = MAX_REPLICAS + 1;
        assert!(f.validate().is_err());
        let mut f = sample_fn("ok");
        f.env.push(("BAD=NAME".into(), "v".into()));
        assert!(f.validate().is_err());
        let mut f = sample_fn("ok");
        f.cpu_cores = 0;
        assert!(f.validate().is_err());
    }

    #[test]
    fn wasm_handler_validates_module_hash() {
        let good = Handler::Wasm { module_hash: "ab".repeat(32), entry: "_start".into() };
        assert!(good.validate().is_ok());
        let bad = Handler::Wasm { module_hash: "xyz".into(), entry: "_start".into() };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn validate_hex64_works() {
        assert!(validate_hex64(&"a".repeat(64), "x").is_ok());
        assert!(validate_hex64(&"a".repeat(63), "x").is_err());
        assert!(validate_hex64(&"g".repeat(64), "x").is_err());
    }
}
