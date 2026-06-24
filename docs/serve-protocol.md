# ce-fn serve protocol (normative)

This is the wire contract between the **ce-fn control plane** (`deploy`/`invoke`/`on`) and a
**host-side runtime** (`ce-fn serve`, or any compatible daemon) that actually executes function
handlers. It is the analog of `rdev/exec` for `rdev serve`: invocation is HTTP-shaped but rides CE's
authenticated `AppRequest`/reply primitive, not a node RPC. No new node endpoints are introduced.

## Topic

All invocations use the single topic:

```
ce-fn/invoke
```

The function name is carried **in the request body**, not the topic, so one runtime endpoint hosts
many functions. A caller sends an `AppRequest` to the host running the function on this topic; the
runtime replies with an `InvokeResponse`.

## Request — `InvokeRequest`

JSON, hex-for-bytes. Sent as the `AppRequest` payload.

| field | type | meaning |
|---|---|---|
| `function` | string | the function to run; MUST be a valid ce-fn name (`a-z0-9-_`, 1–64, no leading/trailing `-`). |
| `caps` | string (hex) | a `ce-cap` capability chain authorizing the caller; empty = none (denied by a runtime that requires authorization). |
| `payload_hex` | string (hex) | the invocation payload (opaque bytes). MUST decode to ≤ `MAX_PAYLOAD_BYTES` (10 MiB). |
| `content_type` | string? | optional informational hint; the handler decides how to use it. |

A runtime MUST reject a request whose envelope exceeds `MAX_ENVELOPE_BYTES` **before** parsing it,
and whose decoded payload exceeds `MAX_PAYLOAD_BYTES`. The client enforces the same bounds on encode,
so neither side can be forced to buffer an unbounded request (DoS/OOM closure).

## Response — `InvokeResponse`

JSON, hex-for-bytes. Returned via `CeClient::reply(reply_token, ...)`.

| field | type | meaning |
|---|---|---|
| `ok` | bool | true iff the handler ran and exited 0. |
| `exit_code` | i64? | the handler's process exit code; absent if it never started. |
| `output_hex` | string (hex) | the handler's stdout (response body), ≤ `MAX_OUTPUT_BYTES` (10 MiB); truncated with a flag in `error` if larger. |
| `error` | string? | diagnostic/stderr text on failure (≤ `MAX_ERROR_BYTES`), or a truncation note. |

`exit_code` semantics: `0` = success; non-zero = the handler ran but failed (its stdout is still
returned in `output_hex` so a caller can inspect partial output); absent = the handler never launched
(authorization denied, unknown function, spawn error) and `error` explains why.

## Authorization (the enforcement point)

Before running any handler, the runtime MUST authorize the **authenticated sender NodeId** (`from`
on the `AppMessage`, verified by the node for free) against the presented `caps` chain using the
`ce-cap` verifier with action `fn:invoke`:

```
authorize(self_id, accepted_roots, self_tags, now, requester, "fn:invoke", chain, is_revoked)
```

- The chain MUST root at the host's own key or a configured accepted root.
- Each link's signature, temporal caveats (`not_after`), resource scope, and revocation are checked.
- `is_revoked` MUST consult the node's on-chain revoked set (`GET /capabilities/revoked`), refreshed
  periodically (the reference runtime refreshes every ~10s) so a revoked-but-unexpired token is
  denied without a restart.

A denied request yields `ok=false` with an `error` containing `capability`. An empty `caps` is denied
(`ce-cap` rejects an empty chain).

## Handler execution model (reference runtime)

The reference `ce-fn serve` runtime runs each declared handler as a local subprocess:

1. spawn the handler's `command` (argv) with a cleared environment except `PATH`,
2. inject the function's declared `env` and resolved `secrets` (see below),
3. write the request payload to the handler's **stdin**, then close it (EOF),
4. capture **stdout** as the response body, within a per-handler wall-clock timeout,
5. report the process exit code.

A handler that exceeds its timeout is killed and the invocation fails with a timeout error. Output
beyond `MAX_OUTPUT_BYTES` is truncated.

### Handler manifest

A host declares which functions it serves in a JSON manifest (`--manifest`):

```json
{
  "default_timeout_secs": 60,
  "handlers": [
    { "function": "echo", "command": ["/bin/cat"], "timeout_secs": 10 },
    { "function": "greet", "command": ["python3", "handler.py"],
      "env": [["GREETING", "hello"]],
      "secrets": [{ "env": "API_TOKEN", "from": "CE_FN_API_TOKEN" }] }
  ]
}
```

### Secrets

`secrets[].from` names a **host-local source** (an environment variable on the serving host) that is
resolved at launch and exposed to the handler as `secrets[].env`. Secret **values never traverse the
wire and are never stored in the client registry** — only the binding (`env` ← `from`) is. A missing
secret source fails the invocation closed (the handler does not run with an empty credential).

## Delivery & idempotency

Invocation is request/reply over `AppRequest`, so each call is a single round-trip the caller drives.
The client retries across the deployment's replicas on transport failure (bounded attempts with
backoff). The runtime de-dups by `reply_token` so a redelivered request is answered once. Trigger
delivery (the `on` verb) is at-most-once at the trigger boundary (the pubsub stream is best-effort);
durable at-least-once would layer a ce-coord log (the ce-pubsub product), out of scope here.

## Building a compatible runtime

Any daemon that (a) subscribes to `ce-fn/invoke`, (b) authorizes with `ce-cap` as above, (c) runs the
named handler, and (d) replies with a well-formed `InvokeResponse` is compatible. The reference
implementation lives in `src/serve.rs` (`Runtime`, `serve_loop`, `ProcessRuntime`) and is reusable as
a library: implement the `HandlerRuntime` trait to plug in a different execution backend (e.g. a
Docker `exec` into an already-running cell, or a WASM instantiation) without changing dispatch,
bounds, or authorization.
