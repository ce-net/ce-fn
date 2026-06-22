//! End-to-end-ish tests of the ce-fn public API that need no running node: placement selection,
//! the registry lifecycle, and the invoke/trigger wire round-trips a host and client agree on.

use ce_fn::placement::{Requirements, best, candidates, rank};
use ce_fn::{
    Amount, Deployment, Function, Handler, InvokeRequest, InvokeResponse, Registry, TriggerEvent,
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
    };
    reg.insert(Deployment {
        function: f.clone(),
        host: "ab".repeat(32),
        job_id: "cd".repeat(32),
        deployed_at: 1234,
    });
    reg.save(&path).unwrap();

    // Reload in a fresh registry and confirm the deployment survived intact.
    let reloaded = Registry::load(&path).unwrap();
    let d = reloaded.get("resize").expect("deployment persisted");
    assert_eq!(d.function, f);
    assert_eq!(d.host, "ab".repeat(32));
    assert_eq!(d.function.bid.credits(), "2");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn invoke_wire_contract_between_caller_and_host() {
    // The caller encodes a request; a host decodes it, runs, and encodes a response the caller
    // decodes — the exact contract the AppRequest/reply carries.
    let req_bytes = InvokeRequest::new("resize", b"PNGDATA")
        .with_content_type("image/png")
        .encode();

    // host side
    let req = InvokeRequest::decode(&req_bytes).unwrap();
    assert_eq!(req.function, "resize");
    assert_eq!(req.payload().unwrap(), b"PNGDATA");
    let resp_bytes = InvokeResponse::success(b"THUMB").encode();

    // caller side
    let resp = InvokeResponse::decode(&resp_bytes).unwrap();
    assert!(resp.ok);
    assert_eq!(resp.output().unwrap(), b"THUMB");
}

#[test]
fn trigger_event_carries_data_to_function() {
    // A producer publishes an event; the trigger loop decodes it (leniently) and forwards data.
    let published = TriggerEvent::new("ce-storage/uploads", b"cid-of-uploaded-object").encode();
    let event = TriggerEvent::decode_lenient("ce-storage/uploads", &published);
    assert_eq!(event.topic, "ce-storage/uploads");
    assert_eq!(event.data().unwrap(), b"cid-of-uploaded-object");
}
