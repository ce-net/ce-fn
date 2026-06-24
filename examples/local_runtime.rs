//! Runnable example: drive the ce-fn serve-side runtime end to end in one process, with no node and
//! no network. It builds a capability, encodes an InvokeRequest exactly as a caller would, hands it
//! to the runtime (which authorizes it and runs a real subprocess handler), and decodes the reply.
//!
//! Run with:  cargo run --example local_runtime
//!
//! This is the same dispatch path `ce-fn serve` uses on a host; only the message transport (the
//! node's AppRequest/reply) is elided here.

use ce_cap::Resource;
use ce_fn::caps::{ABILITY_INVOKE, grant};
use ce_fn::serve::{HandlerManifest, HandlerSpec, ProcessRuntime, Runtime, ServeConfig};
use ce_fn::{InvokeRequest, InvokeResponse};
use ce_identity::{Identity, NodeId};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Two identities: the host running the function, and a caller authorized to invoke it.
    let tmp = std::env::temp_dir().join(format!("ce-fn-example-{}", std::process::id()));
    let host = Identity::load_or_generate(&tmp.join("host"))?;
    let caller = Identity::load_or_generate(&tmp.join("caller"))?;

    // The host declares one handler: `upper` runs `tr a-z A-Z` (uppercases stdin).
    let manifest = HandlerManifest {
        default_timeout_secs: 10,
        handlers: vec![HandlerSpec {
            function: "upper".into(),
            command: vec!["tr".into(), "a-z".into(), "A-Z".into()],
            cwd: None,
            env: vec![],
            secrets: vec![],
            timeout_secs: 5,
        }],
    };

    let runtime = Runtime::new(host.node_id(), manifest, ProcessRuntime, ServeConfig::default());

    // The host self-issues a capability letting the caller invoke functions on it.
    let token = grant(
        &host,
        caller.node_id(),
        &[ABILITY_INVOKE],
        Resource::Node(host.node_id()),
        0,
        1,
    );

    // The caller builds and encodes the request (these are the exact bytes that would ride an
    // AppRequest over the mesh).
    let req = InvokeRequest::new("upper", b"hello from ce-fn").with_caps(token);
    let wire = req.encode()?;

    // ... transport elided ...

    // The host-side runtime authorizes and runs the handler, producing the reply bytes.
    let decoded = InvokeRequest::decode(&wire)?;
    let no_revoke = |_: &NodeId, _: u64| false;
    let resp = runtime
        .handle_invoke(&caller.node_id(), &decoded, now(), &no_revoke, &|_| None)
        .await;
    let reply = resp.encode()?;

    // The caller decodes the reply.
    let back = InvokeResponse::decode(&reply)?;
    if back.ok {
        println!("output: {}", String::from_utf8_lossy(&back.output()?));
    } else {
        println!("error: {}", back.error.unwrap_or_default());
    }

    let _ = std::fs::remove_dir_all(&tmp);
    Ok(())
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
