//! End-to-end-ish tests of the ce-fn public API that need no running node: placement selection,
//! the registry lifecycle, and the invoke/trigger wire round-trips a host and client agree on.

use ce_fn::placement::{Requirements, best, candidates, rank, top};
use ce_fn::{
    Amount, Deployment, Function, Handler, InvokeRequest, InvokeResponse, Registry, Replica,
    TriggerEvent,
};
use ce_rs::AtlasEntry;

fn host(id: &str, cpu: u32, mem: u32, jobs: u32, tags: &[&str]) -> AtlasEntry {
    AtlasEntry {
        node_id: id.to_string(),
        cpu_cores: cpu,
        mem_mb: mem,
        running_jobs: jobs,
        last_seen_secs: 0,
        tags: tags.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn placement_picks_most_proven_capable_host() {
    let atlas = vec![
        host("nodocker", 8, 8192, 0, &["linux"]),       // can't run containers
        host("weak-docker", 8, 8192, 0, &["docker"]),   // capable, low reputation
        host("strong-docker", 8, 8192, 3, &["docker"]), // capable, high reputation
        host("tiny-docker", 1, 256, 0, &["docker"]),    // capable tag but too small
    ];
    let req = Requirements::for_container(4, 4096, vec![]);

    // Only the two adequately-sized docker hosts qualify.
    let pool = candidates(atlas.clone(), &req);
    let mut names: Vec<&str> = pool.iter().map(|h| h.node_id.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["strong-docker", "weak-docker"]);

    // Reputation lookup favors strong-docker even though it has more running jobs.
    let rep = |id: &str| if id == "strong-docker" { 100 } else { 1 };
    let chosen = best(atlas, &req, rep).expect("a host qualifies");
    assert_eq!(chosen.host.node_id, "strong-docker");
    assert_eq!(chosen.delivered_work, 100);
}

#[test]
fn ranking_is_deterministic() {
    let hosts = vec![
        host("b", 4, 1024, 0, &["docker"]),
        host("a", 4, 1024, 0, &["docker"]),
        host("c", 4, 1024, 0, &["docker"]),
    ];
    // All equal reputation + load → stable tiebreak by node id.
    let ranked = rank(hosts, 10, |_| 0);
    let order: Vec<&str> = ranked.iter().map(|c| c.host.node_id.as_str()).collect();
    assert_eq!(order, vec!["a", "b", "c"]);
}

#[test]
fn registry_lifecycle_persists() {
    let dir = std::env::temp_dir().join(format!("ce-fn-flow-{}", std::process::id()));
    let path = dir.join("registry.json");
    let _ = std::fs::remove_dir_all(&dir);

    let mut reg = Registry::load(&path).unwrap();
    assert!(reg.list().is_empty());

    let f = Function {
        name: "resize".into(),
        handler: Handler::Container { image: "alpine:latest".into(), cmd: vec!["echo".into()] },
        cpu_cores: 1,
        mem_mb: 128,
        duration_secs: 60,
        bid: Amount::from_credits(2),
        select: vec![],
        env: vec![("LOG".into(), "info".into())],
        secrets: vec![],
        replicas: 1,
    };
    reg.insert(Deployment {
        function: f.clone(),
        replicas: vec![Replica { host: "ab".repeat(32), job_id: "cd".repeat(32) }],
        revision: 1,
        deployed_at: 1234,
        stats: Default::default(),
    });
    reg.save(&path).unwrap();

    // Reload in a fresh registry and confirm the deployment survived intact.
    let reloaded = Registry::load(&path).unwrap();
    let d = reloaded.get("resize").expect("deployment persisted");
    assert_eq!(d.function, f);
    assert_eq!(d.host(), "ab".repeat(32));
    assert_eq!(d.function.bid.credits(), "2");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn placement_top_n_for_replicas() {
    let atlas = vec![
        host("a", 4, 1024, 0, &["docker"]),
        host("b", 4, 1024, 0, &["docker"]),
        host("c", 4, 1024, 0, &["docker"]),
        host("nodocker", 4, 1024, 0, &["linux"]),
    ];
    let req = Requirements::for_container(1, 128, vec![]);
    let rep = |id: &str| match id {
        "a" => 30,
        "b" => 20,
        "c" => 10,
        _ => 0,
    };
    let chosen = top(atlas, &req, 2, rep);
    let names: Vec<&str> = chosen.iter().map(|c| c.host.node_id.as_str()).collect();
    assert_eq!(names, vec!["a", "b"]);
}

#[test]
fn invoke_wire_contract_between_caller_and_host() {
    // The caller encodes a request; a host decodes it, runs, and encodes a response the caller
    // decodes — the exact contract the AppRequest/reply carries.
    let req_bytes = InvokeRequest::new("resize", b"PNGDATA")
        .with_content_type("image/png")
        .encode()
        .unwrap();

    // host side
    let req = InvokeRequest::decode(&req_bytes).unwrap();
    assert_eq!(req.function, "resize");
    assert_eq!(req.payload().unwrap(), b"PNGDATA");
    let resp_bytes = InvokeResponse::success(b"THUMB").encode().unwrap();

    // caller side
    let resp = InvokeResponse::decode(&resp_bytes).unwrap();
    assert!(resp.ok);
    assert_eq!(resp.output().unwrap(), b"THUMB");
}

#[test]
fn trigger_event_carries_data_to_function() {
    // A producer publishes an event; the trigger loop decodes it (leniently) and forwards data.
    let published = TriggerEvent::new("ce-storage/uploads", b"cid-of-uploaded-object").encode().unwrap();
    let event = TriggerEvent::decode_lenient("ce-storage/uploads", &published);
    assert_eq!(event.topic, "ce-storage/uploads");
    assert_eq!(event.data().unwrap(), b"cid-of-uploaded-object");
}

// ----- serve-side runtime: full caller -> runtime -> caller round-trip (no node needed) -----

use ce_fn::serve::{
    HandlerManifest, HandlerOutcome, HandlerRuntime, HandlerSpec, Runtime, ServeConfig,
};
use ce_fn::caps::{ABILITY_INVOKE, grant};
use ce_identity::{Identity, NodeId};
use std::time::Duration;

fn gen_identity(tag: &str) -> Identity {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("ce-fn-flow-id-{}-{tag}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    Identity::load_or_generate(&dir).expect("generate identity")
}

/// A fake backend that uppercases the payload — stands in for a real handler so the whole serve
/// dispatch path runs offline.
#[derive(Clone)]
struct UpperBackend;

impl HandlerRuntime for UpperBackend {
    async fn run(
        &self,
        _spec: &HandlerSpec,
        payload: &[u8],
        _env: &[(String, String)],
        _timeout: Duration,
    ) -> anyhow::Result<HandlerOutcome> {
        let out = payload.to_ascii_uppercase();
        Ok(HandlerOutcome { output: out, exit_code: 0 })
    }
}

fn manifest(function: &str) -> HandlerManifest {
    HandlerManifest {
        default_timeout_secs: 5,
        handlers: vec![HandlerSpec {
            function: function.into(),
            command: vec!["true".into()],
            cwd: None,
            env: vec![],
            secrets: vec![],
            timeout_secs: 0,
        }],
    }
}

#[tokio::test]
async fn end_to_end_invoke_through_runtime() {
    // This is the product's reason for existing: a caller encodes an InvokeRequest, the serve-side
    // runtime authorizes + runs the handler, and the caller decodes the InvokeResponse — exercising
    // the exact bytes that ride an AppRequest/reply, minus the network.
    let host = gen_identity("host");
    let caller = gen_identity("caller");

    let runtime = Runtime::new(
        host.node_id(),
        manifest("echo"),
        UpperBackend,
        ServeConfig::default(),
    );

    // Caller mints/holds a capability and builds the request.
    let token = grant(
        &host,
        caller.node_id(),
        &[ABILITY_INVOKE],
        ce_cap::Resource::Node(host.node_id()),
        0,
        1,
    );
    let req = InvokeRequest::new("echo", b"hello world").with_caps(token);
    let wire = req.encode().unwrap();

    // ---- the request crosses the (simulated) mesh ----
    let decoded = InvokeRequest::decode(&wire).unwrap();

    // Host-side runtime processes it.
    let no_revoke = |_: &NodeId, _: u64| false;
    let resp = runtime
        .handle_invoke(&caller.node_id(), &decoded, 0, &no_revoke, &|_| None)
        .await;
    let reply = resp.encode().unwrap();

    // ---- the reply crosses back ----
    let back = InvokeResponse::decode(&reply).unwrap();
    assert!(back.ok, "round-trip should succeed: {back:?}");
    assert_eq!(back.output().unwrap(), b"HELLO WORLD");
}

#[tokio::test]
async fn end_to_end_unauthorized_is_rejected_by_runtime() {
    let host = gen_identity("host");
    let stranger = gen_identity("stranger");
    let runtime = Runtime::new(host.node_id(), manifest("echo"), UpperBackend, ServeConfig::default());

    // Stranger presents no capability.
    let req = InvokeRequest::new("echo", b"x");
    let resp = runtime
        .handle_invoke(&stranger.node_id(), &req, 0, &|_, _| false, &|_| None)
        .await;
    assert!(!resp.ok);
    assert!(resp.error.unwrap().contains("capability"));
}
