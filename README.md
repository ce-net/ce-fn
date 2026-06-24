# ce-fn — serverless functions over CE

`ce-fn` is the **Cloud Functions / Cloud Run** layer of the "CE Cloud" portfolio: deploy a
container or WASM **handler**, run a host-side **runtime** that executes it, then **invoke** it on
atlas-ranked mesh hosts and/or **trigger** it from pubsub events — billed through the normal
job/credit escrow.

It is an **app-tier crate**. It composes existing CE primitives via [`ce-rs`](../ce-rs) (the HTTP
SDK) and [`ce-cap`](../ce/crates/ce-cap) (the capability verifier); it adds **no node endpoints**.

> **This crate ships both halves.** Earlier versions shipped only the control plane (deploy/invoke
> client) with no daemon to answer invocations. ce-fn now also ships the **serve-side runtime**
> (`ce-fn serve`) that runs handlers and replies to `ce-fn/invoke` requests — so deploy → invoke →
> trigger works end to end. The execution backend is a local subprocess (`ProcessRuntime`) with a
> pluggable [`HandlerRuntime`] seam for Docker-exec / WASM backends.

## What it composes (no reinvention)

| ce-fn concern | CE primitive (via ce-rs / ce-cap) |
|---|---|
| place a handler on a host | `mesh-deploy` / `mesh_deploy_wasm` (jobs) |
| pick the host(s) | `/atlas` capacity + `/history` reputation (`placement`, swarm-style ranking) |
| invoke (HTTP-style) | `AppRequest`/reply (`CeClient::request`) on topic `ce-fn/invoke` |
| run the handler | `ce-fn serve` runtime (subprocess / pluggable backend) |
| event triggers | mesh pubsub (`subscribe` + `messages_stream`) |
| billing | the job bid → credit escrow (Cloud-Run-style; see *Billing*) |
| authorize deploy/invoke | `ce-cap` chains, abilities `fn:deploy` / `fn:invoke` |
| state (which fn → which hosts) | a local, versioned, atomically-written JSON registry |

## Model

A **function** is a deployable definition: a name, a handler (container image **or** WASM module
hash + entry), resource needs (cpu/mem/duration), a per-deploy bid, optional env vars and secrets,
a replica count, and optional host self-tags (`gpu`, ...).

```
deploy ──► select top-N hosts (atlas + reputation) ──► mesh-deploy ×replicas (bid → escrow) ──► registry
invoke ──► request(replica, "ce-fn/invoke", InvokeRequest) ──► runtime runs handler ──► InvokeResponse
on     ──► subscribe(topic) ──► per event: invoke(function, event.data)   (SSE stream, deduped)
serve  ──► answer ce-fn/invoke: authorize (ce-cap) → run handler → reply
kill   ──► mesh-kill(host, job) ×replicas ──► forget
```

Placement mirrors `swarm`: filter the atlas to hosts that can run the workload (advertise `docker`
for containers / `wasm` for WASM, plus required tags and capacity), then rank survivors by **on-chain
delivered work** (most-proven first), tie-broken by least-loaded then node id. It is pure
(`placement::rank`/`top`) so it is unit- and property-tested. Deploy places `replicas` cells across
the top distinct hosts and **falls back to the next-ranked candidate** when a host fails between the
atlas read and the deploy (the placement TOCTOU). `invoke` load-balances and retries across the live
replicas.

Invocation is **HTTP-shaped** but rides CE's authenticated `AppRequest`/reply — the same primitive
`swarm` uses for `rdev/exec`. The sender's NodeId is verified by the node for free; the serve runtime
authorizes with a `ce-cap` chain (`fn:invoke`) before running the handler. See
[docs/serve-protocol.md](docs/serve-protocol.md) for the normative wire contract and
[docs/architecture.md](docs/architecture.md) for the full design and lifecycle.

## CLI

```bash
# Deploy a container function across the 2 best atlas-ranked hosts, with config + a secret:
ce-fn deploy resize --image myorg/resize:latest --cpu 1 --mem 256 --bid 1 --replicas 2 \
  --env LOG=info --secret API_TOKEN=CE_FN_RESIZE_TOKEN -- /bin/resize

# Deploy a WASM function (upload the module to the blob store first, pass its hash):
ce-fn deploy thumb --wasm <module-hash> --entry _start --mem 64

# Invoke it (payload from --data or stdin; raw output to stdout):
ce-fn invoke resize --data '<image bytes>'
cat photo.png | ce-fn invoke resize > thumb.png

# Bind it to a pubsub topic — one invocation per event (e.g. ce-storage upload notifications):
ce-fn on ce-storage/uploads resize

# Run the host-side runtime that actually executes handlers (declare them in a manifest):
ce-fn serve --manifest examples/handlers.json --tag docker

# Manage / observe:
ce-fn ls                       # deployed functions (replicas, revision, bid)
ce-fn describe resize          # full spec + replica + invocation stats
ce-fn logs                     # invocation count / errors / latency for all functions
ce-fn hosts --select gpu       # candidate hosts from the atlas
ce-fn kill resize              # stop every replica + forget
ce-fn forget resize            # drop the record WITHOUT killing (unreachable host; cell expires)

# Authorize another node to invoke (mint a ce-cap token; --key = an identity dir with node.key):
ce-fn grant <audience-node-id> --can invoke --expires 86400 --key ~/.local/share/ce/identity
# the audience then passes it:  ce-fn --cap <token> invoke resize --data ...
```

Global flags: `--node <url>` (default `http://127.0.0.1:8844`), `--registry <path>`
(default `$CE_FN_REGISTRY` or `<config>/ce-fn/registry.json`), `--cap <token>`. Set `CE_FN_LOG`
(e.g. `CE_FN_LOG=info`) for tracing output.

## Library

```rust
use ce_fn::{FnClient, Function, Handler, Registry, bid_credits};
use ce_rs::CeClient;

# async fn demo() -> anyhow::Result<()> {
let registry = Registry::load(&Registry::default_path())?;
let mut fns = FnClient::new(CeClient::local(), registry);

let f = Function {
    name: "resize".into(),
    handler: Handler::Container { image: "myorg/resize:latest".into(), cmd: vec![] },
    cpu_cores: 1, mem_mb: 256, duration_secs: 120,
    bid: bid_credits(1), select: vec![],
    env: vec![("LOG".into(), "info".into())], secrets: vec![], replicas: 2,
};
let dep = fns.deploy(f, None).await?;            // top-N placement + fallback + bill via job bids
fns.registry().save(&Registry::default_path())?;

let out = fns.invoke("resize", b"<image bytes>").await?;  // retried/load-balanced across replicas
# Ok(()) }
```

The serve-side runtime is reusable as a library too — implement [`HandlerRuntime`] to plug a custom
execution backend into `serve::Runtime`/`serve_loop`. See `examples/local_runtime.rs` for a full
offline round-trip and `examples/handlers.json` + `examples/handler.py` for a manifest and handler.

Modules: `function` (specs + registry), `placement` (pure host selection), `protocol`
(invoke/trigger wire types), `caps` (`ce-cap` helpers), `client` (`FnClient`), `serve` (runtime).

## Billing

**This is Cloud-Run-style, not pay-per-call.** `deploy` submits a CE **job bid** (`mesh-deploy`) per
replica; each bid is locked in credit escrow and settled to the host through the normal
`JobBid`/`JobSettle` flow, and the cell is held via heartbeats. `invoke` does **not** debit credits
per call — it is a request/reply on an already-running cell. Per-invocation micropayments can be
layered with payment channels (`ce-rs` `channel_open`/`sign_receipt`), left to the caller since
channel policy is app-specific. ce-fn records per-invocation accounting (count/errors/latency) locally
for observability (`ce-fn logs`/`describe`), not as a meter.

## Status & limits

- Builds, unit-, integration-, doc-, and property-tests green (`cargo test`); `cargo clippy` clean.
  Tests are pure/offline (placement, registry, protocol, capability authorization, and the full
  serve-side dispatch path against a fake backend) — they need no running node.
- **Data plane:** the `ce-fn serve` runtime executes handlers as local subprocesses. A Docker-exec or
  WASM-instantiation backend is a documented extension point (`HandlerRuntime`), not yet shipped.
- **Triggers:** at-most-once at the trigger boundary (the pubsub stream is best-effort), driven by the
  reliable SSE message stream and de-duped over a sliding window. Each invocation retries across
  replicas. Durable at-least-once would layer a `ce-coord` log (the `ce-pubsub` product), out of scope.
- **Deferred** (documented in [docs/architecture.md](docs/architecture.md)): per-invocation on-chain
  metering, HTTP-ingress / cron triggers, weighted traffic-split canary across revisions.

## License

MIT. Author: Leif Rydenfalk <ledamecrydenfalk@gmail.com>.

[`HandlerRuntime`]: https://docs.rs/ce-fn
