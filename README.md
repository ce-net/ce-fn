# ce-fn — serverless functions over CE

`ce-fn` is the **Cloud Functions / Cloud Run** layer of the "CE Cloud" portfolio: deploy a
container or WASM **handler**, then **invoke** it on atlas-ranked mesh hosts and/or **trigger** it
from pubsub events — billed per deploy through the normal job/credit escrow.

It is an **app-tier crate**. It composes existing CE primitives via [`ce-rs`](../ce-rs) (the HTTP
SDK) and [`ce-cap`](../ce/crates/ce-cap) (the capability verifier); it adds **no node endpoints**.
This is the design stub `3.4 ce-fn` from `PLAN/12-google-infra-portfolio.md`, built.

## What it composes (no reinvention)

| ce-fn concern | CE primitive (via ce-rs / ce-cap) |
|---|---|
| place a handler on a host | `mesh-deploy` / `mesh_deploy_wasm` (jobs) |
| pick the host | `/atlas` capacity + `/history` reputation (`placement`, swarm-style ranking) |
| invoke (HTTP-style) | `AppRequest`/reply (`CeClient::request`) on topic `ce-fn/invoke` |
| event triggers | mesh pubsub (`subscribe` + `messages`) |
| billing | the job bid → credit escrow (no separate metering) |
| authorize deploy/invoke | `ce-cap` chains, abilities `fn:deploy` / `fn:invoke` |
| state (which fn → which host) | a local JSON registry |

Object storage (`ce-pin`/blobs), mutable state/DB (`ce-coord` Merged+Snapshot), and the event bus
(`ce-pubsub`) are the sibling products a function reads from and writes to — they are not
re-implemented here; a handler uses `ce-rs` (`put_object`/`get_object`) or those crates directly.

## Model

A **function** is a deployable definition: a name, a handler (container image **or** WASM module
hash + entry), resource needs (cpu/mem/duration), a per-invocation bid in credits, and optional
host self-tags (`gpu`, ...).

```
deploy ──► select_host (atlas + reputation) ──► mesh-deploy (bid → escrow) ──► registry
invoke ──► request(host, "ce-fn/invoke", InvokeRequest) ──► InvokeResponse
on     ──► subscribe(topic) ──► per event: invoke(function, event.data)
kill   ──► mesh-kill(host, job) ──► forget
```

Placement mirrors `swarm`: filter the atlas to hosts that can actually run the workload (advertise
`docker` for containers / `wasm` for WASM, plus required tags and enough capacity), then rank the
survivors by **on-chain delivered work** (most-proven first), tie-broken by least-loaded. It is a
pure function (`placement::rank`) so it is unit-tested and reproducible.

Invocation is **HTTP-shaped** but rides CE's authenticated `AppRequest`/reply — the same primitive
`swarm` uses for `rdev/exec`. The sender's NodeId is verified by the node for free; the function
runtime authorizes with a `ce-cap` chain (`caps::authorize_invoke`) before running the handler.

## CLI

```bash
# Deploy a container function on the best atlas-ranked host:
ce-fn deploy resize --image myorg/resize:latest --cpu 1 --mem 256 --bid 1 -- /bin/resize

# Deploy a WASM function (upload the module to the blob store first, pass its hash):
ce-fn deploy thumb --wasm <module-hash> --entry _start --mem 64

# Invoke it (payload from --data or stdin; raw output to stdout):
ce-fn invoke resize --data '<image bytes>'
cat photo.png | ce-fn invoke resize > thumb.png

# Bind it to a pubsub topic — one invocation per event (e.g. ce-storage upload notifications):
ce-fn on ce-storage/uploads resize

# Manage:
ce-fn ls                       # deployed functions
ce-fn hosts --select gpu       # candidate hosts from the atlas
ce-fn kill resize              # stop + forget

# Authorize another node to invoke (mint a ce-cap token; --key = an identity dir with node.key):
ce-fn grant <audience-node-id> --can invoke --expires 86400 --key ~/.local/share/ce/identity
# the audience then passes it:  ce-fn --cap <token> invoke resize --data ...
```

Global flags: `--node <url>` (default `http://127.0.0.1:8844`), `--registry <path>`
(default `$CE_FN_REGISTRY` or `<config>/ce-fn/registry.json`), `--cap <token>`.

## Library

```rust
use ce_fn::{FnClient, Function, Handler, Registry, bid_credits};
use ce_rs::CeClient;

let registry = Registry::load(&Registry::default_path())?;
let mut fns = FnClient::new(CeClient::local(), registry);

let f = Function {
    name: "resize".into(),
    handler: Handler::Container { image: "myorg/resize:latest".into(), cmd: vec![] },
    cpu_cores: 1, mem_mb: 256, duration_secs: 120,
    bid: bid_credits(1), select: vec![],
};
let dep = fns.deploy(f, None).await?;         // atlas-ranked placement + bill via job bid
fns.registry().save(&Registry::default_path())?;

let out = fns.invoke("resize", b"<image bytes>").await?;  // HTTP-style over the mesh
```

Modules: `function` (specs + registry), `placement` (pure host selection), `protocol`
(invoke/trigger wire types), `caps` (`ce-cap` helpers), `client` (`FnClient`).

## Billing

There is no separate meter. `deploy` submits a CE **job bid** (`mesh-deploy`) for the handler; the
bid is locked in credit escrow and settled to the host through the normal `JobBid`/`JobSettle`
flow. A long-running function holds its cell via heartbeats (the node's job manager); per-invocation
micropayments can be layered with payment channels (`ce-rs` `channel_open`/`sign_receipt`) — left
to the caller, since the channel policy is app-specific.

## Status & limits

- Compiles and unit-tests green (`cargo build && cargo test`). Tests are pure/offline (placement,
  registry, protocol, capability authorization) — they need no running node.
- **Runtime side:** the function host needs a small runtime that answers `ce-fn/invoke`
  `AppRequest`s by running the named handler (the container/WASM the cell launched) and replying
  with its output. That serve-side daemon is the sibling deliverable (mirrors `rdev serve` for
  `rdev/exec`); `ce-fn` ships the client/control plane and the wire protocol it speaks.
- Trigger delivery is best-effort / at-most-once (CE's inbox ring is bounded), de-duped over a
  sliding window — matching CE pubsub semantics. Durable at-least-once would layer a `ce-coord`
  log (the `ce-pubsub` product), out of scope here.

## License

MIT. Author: Leif Rydenfalk <ledamecrydenfalk@gmail.com>.
