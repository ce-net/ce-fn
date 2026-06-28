//! ce-fn — serverless functions on the CE mesh.
//!
//! Deploy a container or WASM handler, invoke it on atlas-ranked hosts over the mesh, bind it to
//! pubsub topics as event triggers, and run the host-side runtime that executes handlers. A thin CLI
//! over the [`ce_fn`] library; all client state lives in a local JSON registry so
//! deploy/invoke/on/kill are stateful across runs.

use anyhow::{Result, anyhow, bail};
use ce_fn::caps::{ABILITY_DEPLOY, ABILITY_INVOKE, grant};
use ce_fn::serve::{HandlerManifest, ProcessRuntime, Runtime, ServeConfig, serve_loop};
use ce_fn::{Function, FnClient, Handler, Registry, SecretRef, TriggerEvent};
use ce_rs::{Amount, CeClient};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ce-fn", about = "Serverless functions over CE jobs + atlas", version)]
struct Cli {
    /// CE node HTTP API URL.
    #[arg(long, default_value = "http://127.0.0.1:8844", global = true)]
    node: String,
    /// Registry file path (where deployed functions are recorded).
    #[arg(long, global = true)]
    registry: Option<PathBuf>,
    /// Capability token authorizing deploy/invoke/kill on the target host(s).
    #[arg(long, global = true)]
    cap: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Deploy a function: pick the top atlas-ranked hosts and place the handler.
    ///
    /// Container: ce-fn deploy resize --image myorg/resize:latest -- /bin/resize
    /// WASM:      ce-fn deploy thumb  --wasm <module-hash> --entry _start
    Deploy {
        /// Unique function name (the handle for invoke/on/kill).
        name: String,
        /// Container image to run (mutually exclusive with --wasm).
        #[arg(long)]
        image: Option<String>,
        /// WASM module content hash (mutually exclusive with --image; upload it first).
        #[arg(long)]
        wasm: Option<String>,
        /// WASM entry export to call (with --wasm).
        #[arg(long, default_value = "_start")]
        entry: String,
        /// CPU cores the handler needs.
        #[arg(long, default_value = "1")]
        cpu: u32,
        /// Memory (MiB) the handler needs.
        #[arg(long, default_value = "256")]
        mem: u32,
        /// Max wall-clock seconds per invocation.
        #[arg(long, default_value = "120")]
        duration: u64,
        /// Per-invocation bid in whole credits.
        #[arg(long, default_value = "1")]
        bid: u64,
        /// Number of replica cells to place across distinct hosts.
        #[arg(long, default_value = "1")]
        replicas: u32,
        /// Require hosts advertising this capability self-tag (repeatable, e.g. --select gpu).
        #[arg(long)]
        select: Vec<String>,
        /// Set an environment variable for the handler: --env KEY=VALUE (repeatable).
        #[arg(long, value_name = "KEY=VALUE")]
        env: Vec<String>,
        /// Bind a host secret to an env var: --secret ENV=HOST_SOURCE (repeatable). The value is
        /// resolved on the host at launch and never stored in the registry.
        #[arg(long, value_name = "ENV=SOURCE")]
        secret: Vec<String>,
        /// Pin to a specific host node id instead of atlas selection (forces replicas=1).
        #[arg(long)]
        host: Option<String>,
        /// Container command override (after `--`).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Invoke a deployed function with a payload and print the response.
    ///
    /// Payload comes from --data, or stdin if omitted. Output is written raw to stdout.
    Invoke {
        /// Function name.
        name: String,
        /// Inline payload string (UTF-8). If absent, read payload from stdin.
        #[arg(long)]
        data: Option<String>,
    },
    /// Bind a function to a pubsub topic: invoke it once per published event (runs until ctrl-c).
    On {
        /// The pubsub topic to watch.
        topic: String,
        /// The function to invoke on each event.
        name: String,
    },
    /// Stop a deployed function and remove it from the registry.
    Kill {
        /// Function name.
        name: String,
    },
    /// Drop a deployment record WITHOUT killing its cells (for unreachable hosts; cells expire).
    Forget {
        /// Function name.
        name: String,
    },
    /// List deployed functions.
    Ls,
    /// Show detailed status for one deployed function (replicas, revision, invocation stats).
    Describe {
        /// Function name.
        name: String,
    },
    /// Show invocation accounting (call count, error rate, last call) for deployed functions.
    Logs {
        /// Optional function name; omit to show all.
        name: Option<String>,
    },
    /// List candidate hosts from the atlas (those able to run functions).
    Hosts {
        /// Only show hosts advertising this self-tag.
        #[arg(long)]
        select: Option<String>,
    },
    /// Run the host-side runtime: answer ce-fn/invoke requests by executing declared handlers.
    ///
    /// Requires a handler manifest (JSON) declaring the function -> command mappings this host
    /// will serve. See docs/serve-protocol.md and examples/handlers.json.
    Serve {
        /// Path to the handler manifest JSON file.
        #[arg(long)]
        manifest: PathBuf,
        /// Path to an accepted-roots file (64-hex node ids, one per line). Optional.
        #[arg(long)]
        roots: Option<PathBuf>,
        /// This host's atlas self-tags (repeatable) — consulted for tag-scoped capabilities.
        #[arg(long)]
        tag: Vec<String>,
    },
    /// Mint a capability token authorizing another node to deploy/invoke on this node.
    Grant {
        /// Audience node id (64-hex) receiving the capability.
        audience: String,
        /// Abilities to grant (comma-separated): deploy, invoke. Default: invoke.
        #[arg(long, default_value = "invoke")]
        can: String,
        /// Expiry in seconds from now (0 = never).
        #[arg(long, default_value = "0")]
        expires: u64,
        /// Path to an identity directory containing `node.key`.
        #[arg(long)]
        key: PathBuf,
        /// Capability nonce (issuer-unique; names it for revocation).
        #[arg(long, default_value = "1")]
        nonce: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let ce = CeClient::new(cli.node.clone());
    let reg_path = cli.registry.clone().unwrap_or_else(Registry::default_path);
    let cap = cli.cap.clone();

    match cli.cmd {
        Cmd::Deploy {
            name, image, wasm, entry, cpu, mem, duration, bid, replicas, select, env, secret, host,
            command,
        } => {
            let handler = build_handler(&image, &wasm, &entry, command)?;
            let env = parse_env(&env)?;
            let secrets = parse_secrets(&secret)?;
            let replicas = if host.is_some() { 1 } else { replicas.max(1) };
            let f = Function {
                name: name.clone(),
                handler,
                cpu_cores: cpu,
                mem_mb: mem,
                duration_secs: duration,
                bid: Amount::from_credits(bid),
                select,
                env,
                secrets,
                replicas,
            };
            f.validate()?;

            // Mutate the registry under an advisory lock so concurrent deploys do not clobber.
            let registry = Registry::load(&reg_path)?;
            let mut fns = FnClient::new(ce, registry);
            let dep = match &host {
                Some(h) => fns.deploy_on(f, h, cap.as_deref()).await?,
                None => fns.deploy(f, cap.as_deref()).await?,
            };
            fns.registry().save(&reg_path)?;
            println!("deployed '{}' (rev {})", dep.function.name, dep.revision);
            for r in &dep.replicas {
                println!("  replica  {}  job {}", short(&r.host), short(&r.job_id));
            }
            println!("  bid      {} credits / invocation", dep.function.bid.credits());
            println!("invoke it with: ce-fn invoke {} --data '...'", name);
        }

        Cmd::Invoke { name, data } => {
            let payload = match data {
                Some(s) => s.into_bytes(),
                None => read_stdin()?,
            };
            let registry = Registry::load(&reg_path)?;
            let mut fns = FnClient::new(ce, registry);
            let out =
                fns.invoke_with(&name, &payload, cap.as_deref(), ce_fn::DEFAULT_INVOKE_TIMEOUT_MS).await;
            // Persist updated invocation stats regardless of outcome.
            let _ = fns.registry().save(&reg_path);
            let out = out?;
            use std::io::Write;
            std::io::stdout().write_all(&out)?;
        }

        Cmd::On { topic, name } => {
            let registry = Registry::load(&reg_path)?;
            let mut fns = FnClient::new(ce, registry);
            println!("binding '{name}' to topic '{topic}' (ctrl-c to stop)...");
            let on_result = |ev: &TriggerEvent, outcome: &Result<Vec<u8>>| match outcome {
                Ok(out) => println!("  event on '{}' -> {} bytes", ev.topic, out.len()),
                Err(e) => println!("  event on '{}' -> error: {e}", ev.topic),
            };
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            fns.run_trigger(&name, &topic, cap.as_deref(), on_result, shutdown).await?;
            let _ = fns.registry().save(&reg_path);
            println!("trigger stopped.");
        }

        Cmd::Kill { name } => {
            let registry = Registry::load(&reg_path)?;
            let mut fns = FnClient::new(ce, registry);
            let dep = fns.kill(&name, cap.as_deref()).await?;
            fns.registry().save(&reg_path)?;
            println!("killed '{}' ({} replica(s))", name, dep.replica_count());
        }

        Cmd::Forget { name } => {
            Registry::with_lock(&reg_path, |r| {
                r.remove(&name).ok_or_else(|| anyhow!("no deployed function named '{name}'"))?;
                Ok(())
            })?;
            println!("forgot '{name}' (its cells will expire on their own).");
        }

        Cmd::Ls => {
            let registry = Registry::load(&reg_path)?;
            let deps = registry.list();
            if deps.is_empty() {
                println!("no deployed functions (registry: {})", reg_path.display());
                return Ok(());
            }
            println!(
                "{:<20}  {:<9}  {:<12}  {:>4}  {:>4}  {:>5}  HOST",
                "NAME", "KIND", "RESOURCES", "REPL", "REV", "BID"
            );
            for d in deps {
                let kind = if d.function.handler.is_wasm() { "wasm" } else { "container" };
                let res = format!("{}c/{}M", d.function.cpu_cores, d.function.mem_mb);
                println!(
                    "{:<20}  {:<9}  {:<12}  {:>4}  {:>4}  {:>5}  {}",
                    d.function.name,
                    kind,
                    res,
                    d.replica_count(),
                    d.revision,
                    d.function.bid.credits(),
                    short(d.host()),
                );
            }
        }

        Cmd::Describe { name } => {
            let registry = Registry::load(&reg_path)?;
            let d = registry
                .get(&name)
                .ok_or_else(|| anyhow!("no deployed function named '{name}'"))?;
            let kind = if d.function.handler.is_wasm() { "wasm" } else { "container" };
            println!("function   {}", d.function.name);
            println!("kind       {kind}");
            match &d.function.handler {
                Handler::Container { image, cmd } => {
                    println!("image      {image}");
                    if !cmd.is_empty() {
                        println!("command    {}", cmd.join(" "));
                    }
                }
                Handler::Wasm { module_hash, entry } => {
                    println!("module     {module_hash}");
                    println!("entry      {entry}");
                }
            }
            println!("resources  {} cores / {} MiB / {}s", d.function.cpu_cores, d.function.mem_mb, d.function.duration_secs);
            println!("bid        {} credits", d.function.bid.credits());
            println!("revision   {}", d.revision);
            if !d.function.env.is_empty() {
                println!("env        {}", d.function.env.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>().join(","));
            }
            if !d.function.secrets.is_empty() {
                println!("secrets    {}", d.function.secrets.iter().map(|s| s.env.as_str()).collect::<Vec<_>>().join(","));
            }
            println!("replicas   {}", d.replica_count());
            for (i, r) in d.replicas.iter().enumerate() {
                println!("  [{i}] host {}  job {}", short(&r.host), short(&r.job_id));
            }
            println!(
                "stats      {} invocations, {} errors, last {}ms",
                d.stats.invocations, d.stats.errors, d.stats.last_latency_ms
            );
        }

        Cmd::Logs { name } => {
            let registry = Registry::load(&reg_path)?;
            let deps: Vec<_> = match &name {
                Some(n) => registry.get(n).into_iter().collect(),
                None => registry.list(),
            };
            if deps.is_empty() {
                println!("no matching deployments.");
                return Ok(());
            }
            println!("{:<20}  {:>10}  {:>8}  {:>9}  {:>12}", "NAME", "INVOKES", "ERRORS", "LAST(ms)", "LAST(unix)");
            for d in deps {
                println!(
                    "{:<20}  {:>10}  {:>8}  {:>9}  {:>12}",
                    d.function.name,
                    d.stats.invocations,
                    d.stats.errors,
                    d.stats.last_latency_ms,
                    d.stats.last_invoked,
                );
            }
        }

        Cmd::Hosts { select } => {
            let atlas = ce.atlas().await?;
            let hosts: Vec<_> = atlas
                .into_iter()
                .filter(|h| h.has_tag("docker") || h.has_tag("wasm"))
                .filter(|h| select.as_ref().is_none_or(|t| h.has_tag(t)))
                .collect();
            if hosts.is_empty() {
                println!("no candidate hosts (need a node advertising 'docker' or 'wasm').");
                return Ok(());
            }
            println!("{:<66}  {:>4}  {:>8}  {:>4}  TAGS", "NODE", "CPU", "MEM(MB)", "JOBS");
            for h in &hosts {
                println!(
                    "{:<66}  {:>4}  {:>8}  {:>4}  {}",
                    h.node_id, h.cpu_cores, h.mem_mb, h.running_jobs, h.tags.join(",")
                );
            }
        }

        Cmd::Serve { manifest, roots, tag } => {
            let manifest = HandlerManifest::load(&manifest)?;
            let status = ce.status().await?;
            let self_id: ce_identity::NodeId = hex::decode(&status.node_id)
                .ok()
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| anyhow!("node returned a bad node id"))?;
            let root_keys = match &roots {
                Some(p) => load_roots(p)?,
                None => vec![],
            };
            let config = ServeConfig { roots: root_keys, self_tags: tag, roots_path: roots };
            let runtime = Runtime::new(self_id, manifest, ProcessRuntime, config);
            println!(
                "ce-fn serving as {} — handlers: [{}]",
                short(&status.node_id),
                runtime.functions().join(", ")
            );
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            serve_loop(&ce, &runtime, shutdown).await?;
            println!("serve stopped.");
        }

        Cmd::Grant { audience, can, expires, key, nonce } => {
            let issuer = load_identity(&key)?;
            let aud = parse_node_id(&audience)?;
            let abilities = parse_abilities(&can)?;
            let not_after = if expires == 0 { 0 } else { now_secs() + expires };
            let token = grant(&issuer, aud, &abilities, ce_iam_core::Resource::Any, not_after, nonce);
            println!("{token}");
        }
    }
    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("CE_FN_LOG").unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = fmt().with_env_filter(filter).with_writer(std::io::stderr).try_init();
}

/// Build a [`Handler`] from the `--image`/`--wasm` flags, rejecting ambiguous/empty combinations.
fn build_handler(
    image: &Option<String>,
    wasm: &Option<String>,
    entry: &str,
    command: Vec<String>,
) -> Result<Handler> {
    match (image, wasm) {
        (Some(_), Some(_)) => bail!("specify either --image or --wasm, not both"),
        (None, None) => bail!("specify a handler: --image <img> or --wasm <module-hash>"),
        (Some(img), None) => Ok(Handler::Container { image: img.clone(), cmd: command }),
        (None, Some(hash)) => {
            if !command.is_empty() {
                bail!("a WASM function takes no command override (it calls --entry)");
            }
            Ok(Handler::Wasm { module_hash: hash.clone(), entry: entry.to_string() })
        }
    }
}

/// Parse `KEY=VALUE` env pairs.
fn parse_env(items: &[String]) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for item in items {
        let (k, v) = item
            .split_once('=')
            .ok_or_else(|| anyhow!("--env must be KEY=VALUE (got '{item}')"))?;
        if k.is_empty() {
            bail!("--env key must not be empty (got '{item}')");
        }
        out.push((k.to_string(), v.to_string()));
    }
    Ok(out)
}

/// Parse `ENV=SOURCE` secret bindings.
fn parse_secrets(items: &[String]) -> Result<Vec<SecretRef>> {
    let mut out = Vec::new();
    for item in items {
        let (env, from) = item
            .split_once('=')
            .ok_or_else(|| anyhow!("--secret must be ENV=SOURCE (got '{item}')"))?;
        if env.is_empty() || from.is_empty() {
            bail!("--secret must be ENV=SOURCE with both sides non-empty (got '{item}')");
        }
        out.push(SecretRef { env: env.to_string(), from: from.to_string() });
    }
    Ok(out)
}

/// Map the comma-separated `--can` list to ce-fn abilities.
fn parse_abilities(can: &str) -> Result<Vec<&'static str>> {
    let mut out = Vec::new();
    for part in can.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match part {
            "deploy" => out.push(ABILITY_DEPLOY),
            "invoke" => out.push(ABILITY_INVOKE),
            other => bail!("unknown ability '{other}' (use: deploy, invoke)"),
        }
    }
    if out.is_empty() {
        bail!("grant at least one ability (deploy, invoke)");
    }
    Ok(out)
}

fn parse_node_id(hex_str: &str) -> Result<ce_identity::NodeId> {
    let bytes = hex::decode(hex_str.trim()).map_err(|_| anyhow!("node id is not hex"))?;
    let arr: [u8; 32] =
        bytes.try_into().map_err(|_| anyhow!("node id must be 32 bytes (64 hex chars)"))?;
    Ok(arr)
}

/// Load accepted capability root keys (64-hex node ids, one per line, `#` comments).
fn load_roots(path: &std::path::Path) -> Result<Vec<ce_identity::NodeId>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("reading roots file {}: {e}", path.display()))?;
    Ok(text
        .lines()
        .map(|l| l.split('#').next().unwrap_or("").trim())
        .filter(|l| !l.is_empty())
        .filter_map(|h| hex::decode(h).ok().and_then(|b| b.try_into().ok()))
        .collect())
}

/// Load an Ed25519 identity from a directory containing `node.key`.
fn load_identity(path: &std::path::Path) -> Result<ce_identity::Identity> {
    ce_identity::Identity::load_or_generate(path)
        .map_err(|e| anyhow!("loading identity from {}: {e}", path.display()))
}

fn read_stdin() -> Result<Vec<u8>> {
    use std::io::Read;
    let mut buf = Vec::new();
    std::io::stdin().read_to_end(&mut buf)?;
    Ok(buf)
}

fn short(s: &str) -> &str {
    if s.len() > 12 { &s[..12] } else { s }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_pairs() {
        let got = parse_env(&["A=1".into(), "B=x=y".into()]).unwrap();
        assert_eq!(got, vec![("A".into(), "1".into()), ("B".into(), "x=y".into())]);
        assert!(parse_env(&["noeq".into()]).is_err());
        assert!(parse_env(&["=v".into()]).is_err());
    }

    #[test]
    fn parse_secret_bindings() {
        let got = parse_secrets(&["TOK=HOST_TOK".into()]).unwrap();
        assert_eq!(got[0].env, "TOK");
        assert_eq!(got[0].from, "HOST_TOK");
        assert!(parse_secrets(&["bad".into()]).is_err());
        assert!(parse_secrets(&["A=".into()]).is_err());
    }

    #[test]
    fn build_handler_rejects_ambiguous() {
        assert!(build_handler(&Some("a".into()), &Some("b".into()), "_start", vec![]).is_err());
        assert!(build_handler(&None, &None, "_start", vec![]).is_err());
        assert!(build_handler(&None, &Some("h".into()), "_start", vec!["x".into()]).is_err());
        assert!(build_handler(&Some("img".into()), &None, "_start", vec!["c".into()]).is_ok());
    }

    #[test]
    fn parse_abilities_maps_and_rejects() {
        assert_eq!(parse_abilities("invoke").unwrap(), vec![ABILITY_INVOKE]);
        assert_eq!(parse_abilities("deploy,invoke").unwrap(), vec![ABILITY_DEPLOY, ABILITY_INVOKE]);
        assert!(parse_abilities("bogus").is_err());
        assert!(parse_abilities("").is_err());
    }

    #[test]
    fn parse_node_id_validates() {
        assert!(parse_node_id(&"a".repeat(64)).is_ok());
        assert!(parse_node_id(&"a".repeat(63)).is_err());
        assert!(parse_node_id("nothex").is_err());
    }
}
