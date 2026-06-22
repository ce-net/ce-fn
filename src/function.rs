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
use std::path::{Path, PathBuf};

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
}

/// A record of a function deployed on a concrete host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    /// The function definition that was deployed.
    pub function: Function,
    /// The host node id (64-hex) the function was placed on.
    pub host: String,
    /// The CE job id assigned by the host (used to kill it later).
    pub job_id: String,
    /// Unix seconds the deployment was created.
    pub deployed_at: u64,
}

/// A persisted set of deployments, keyed by function name. Stateful across CLI invocations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Registry {
    #[serde(default)]
    deployments: BTreeMap<String, Deployment>,
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

    /// Load a registry from `path`, returning an empty one if the file does not exist.
    pub fn load(path: &Path) -> Result<Registry> {
        match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| anyhow!("corrupt registry at {}: {e}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Registry::default()),
            Err(e) => Err(anyhow!("reading registry {}: {e}", path.display())),
        }
    }

    /// Write the registry to `path`, creating parent directories as needed.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| anyhow!("creating {}: {e}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, bytes).map_err(|e| anyhow!("writing {}: {e}", path.display()))?;
        Ok(())
    }

    /// Record (or replace) a deployment.
    pub fn insert(&mut self, d: Deployment) {
        self.deployments.insert(d.function.name.clone(), d);
    }

    /// Look up a deployment by function name.
    pub fn get(&self, name: &str) -> Option<&Deployment> {
        self.deployments.get(name)
    }

    /// Remove a deployment by name; returns it if present.
    pub fn remove(&mut self, name: &str) -> Option<Deployment> {
        self.deployments.remove(name)
    }

    /// All deployments, sorted by function name.
    pub fn list(&self) -> Vec<&Deployment> {
        self.deployments.values().collect()
    }
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
        r.insert(Deployment {
            function: sample_fn("resize"),
            host: "ab".repeat(32),
            job_id: "cd".repeat(32),
            deployed_at: 100,
        });
        assert_eq!(r.get("resize").unwrap().host, "ab".repeat(32));
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
        r.insert(Deployment {
            function: sample_fn("a"),
            host: "11".repeat(32),
            job_id: "22".repeat(32),
            deployed_at: 1,
        });
        r.save(&path).unwrap();

        let loaded = Registry::load(&path).unwrap();
        assert_eq!(loaded.get("a").unwrap().job_id, "22".repeat(32));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registry_load_missing_is_empty() {
        let path = std::env::temp_dir().join("ce-fn-definitely-missing-xyz.json");
        let _ = std::fs::remove_file(&path);
        let r = Registry::load(&path).unwrap();
        assert!(r.list().is_empty());
    }
}
