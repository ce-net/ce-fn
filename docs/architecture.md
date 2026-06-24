# ce-fn architecture

ce-fn is the **Cloud Functions / Cloud Run** layer of the CE Cloud portfolio. It is an **app-tier**
crate: it composes existing CE primitives via the `ce-rs` SDK and `ce-cap` capability verifier, and
adds **no node endpoints**. It has two halves, both shipped in this crate:

```
  ┌──────────────────────────┐         ce-fn/invoke (AppRequest)         ┌────────────────────────┐
  │  control plane           │  ───────────────────────────────────────▶ │  serve-side runtime    │
  │  (FnClient + CLI)        │                                            │  (Runtime + serve_loop)│
  │  deploy / invoke / on /  │ ◀─────────────────────────────────────── │  ce-fn serve           │
  │  kill / logs / describe  │            InvokeResponse (reply)          │  runs the handler      │
  └──────────────────────────┘                                            └────────────────────────┘
        │  mesh-deploy / mesh-kill (jobs)                                       │  subprocess /
        │  atlas + history (placement)                                          │  (pluggable backend)
        ▼                                                                       ▼
                         CE node (ce-rs SDK over the local HTTP API)
```

## Modules

| module | role |
|---|---|
| `function` | `Function`/`Handler`/`SecretRef` specs, validation, and the versioned, atomic, lock-coordinated `Registry`. |
| `placement` | pure atlas-filter + reputation rank (`candidates`/`rank`/`best`/`top`). No network. |
| `protocol` | `InvokeRequest`/`InvokeResponse`/`TriggerEvent` wire types with size bounds and validation. |
| `caps` | `ce-cap` grant/authorize wrappers (`fn:deploy` / `fn:invoke`). |
| `client` | `FnClient` control plane: deploy (multi-replica + fallback), invoke (retry + accounting), trigger (SSE stream), kill, forget. |
| `serve` | the data plane: `Runtime` (capability-enforced dispatch), `serve_loop`, `ProcessRuntime`, the `HandlerRuntime` seam. |

## Deploy → escrow → heartbeat → settle lifecycle

1. **placement** — `FnClient::select_hosts` reads the `/atlas`, filters to hosts that can run the
   workload (`docker` for containers / `wasm` for WASM, plus required tags and capacity), and ranks
   the survivors by on-chain delivered work (`/history`), fetched concurrently with a per-host
   timeout. Ranking is pure and deterministic.
2. **deploy** — `FnClient::deploy` places `replicas` cells across the top-ranked distinct hosts via
   `mesh-deploy` (container) or `mesh_deploy_wasm` (WASM). If a chosen host fails between the atlas
   read and the deploy (the documented placement TOCTOU), it falls back to the next-ranked candidate;
   it only errors if it cannot place at least one replica. Each cell's bid is locked in **credit
   escrow** through the normal `JobBid` flow.
3. **billing** — this is **Cloud-Run-style, not pay-per-call**. A function holds its cell(s) via the
   node's job manager (`Heartbeat` txs every 30s); the bid settles to the host through
   `JobBid`/`JobSettle`. `invoke` does **not** debit credits per call — it is a request/reply on an
   already-running cell. Per-invocation micropayments can be layered with payment channels (`ce-rs`
   `channel_open`/`sign_receipt`), left to the caller because channel policy is app-specific. The
   client records per-invocation accounting (count/errors/latency) locally for observability, not for
   billing.
4. **kill** — `FnClient::kill` stops every replica cell with `mesh-kill` and removes the registry
   entry. `forget` drops the record without killing (for unreachable hosts; the cell expires on its
   own duration/heartbeat timeout).

## Why CLI-driven, not a daemon (control plane)

The control plane is stateless across CLI invocations: all state is the JSON `Registry`. The
long-running process in this product is the **serve-side runtime** (the data plane), which must run
on each host. The control-plane verbs are one-shot, like `gcloud functions`. `ce-fn on` is the one
long-running control-plane verb (a trigger binding) and runs until ctrl-c.

## Registry robustness

- **Atomic writes**: serialize to a sibling temp file, `fsync`, then `rename` over the target — a
  crash mid-write leaves the previous registry intact.
- **Advisory lock**: `Registry::with_lock` takes a `<path>.lock` file around a load-modify-save so
  two concurrent `ce-fn` processes cannot lose each other's updates. Stale locks (from a crashed
  process) are reclaimed after 30s.
- **Schema versioning**: the document carries a `schema` version; an older schema migrates forward on
  load, a newer one is rejected (a downgrade would silently drop fields).

## Concurrency & resource safety

- All external inputs are validated and size-bounded (`Function::validate`, `InvokeRequest::validate`,
  manifest validation, hex-64 host/module checks).
- The invoke/serve paths cap payload (10 MiB), output (10 MiB), error strings (16 KiB), and the whole
  envelope, on both encode and decode.
- The serve runtime feeds stdin in a separate task so a handler that ignores stdin cannot deadlock the
  parent, and enforces a per-handler wall-clock timeout.

## Deferred (real slices shipped, full scope documented)

- **Per-invocation billing**: documented as Cloud-Run-style; payment-channel wiring is left to the
  caller. Invocation accounting is recorded but not metered on-chain.
- **WASM/Docker exec backends in the runtime**: the reference `ProcessRuntime` runs subprocess
  handlers; the `HandlerRuntime` trait is the seam for a Docker-exec or WASM-instantiation backend.
- **HTTP-ingress and cron triggers**: only the pubsub-topic trigger (`on`) ships; CloudEvents mapping
  for richer producers is a documented extension point on `TriggerEvent`.
- **Traffic-split canary across revisions**: the registry versions deployments (revision counter);
  weighted traffic splitting is not implemented.
