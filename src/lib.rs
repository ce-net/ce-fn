//! # ce-fn — serverless functions over CE
//!
//! `ce-fn` is the Cloud Functions / Cloud Run layer of the "CE Cloud" portfolio: deploy a
//! container or WASM **handler**, then invoke it on **atlas-ranked** mesh hosts and/or trigger it
//! from **pubsub** events — paid per deploy through the normal job/credit escrow. It is an
//! **app-tier** crate built entirely on existing CE primitives via [`ce_rs`] and [`ce_cap`]; it
//! adds **no node endpoints**.
//!
//! ## What it composes (no reinvention)
//!
//! | ce-fn concern | CE primitive (via ce-rs / ce-cap) |
//! |---|---|
//! | place a handler on a host | `mesh-deploy` / `mesh_deploy_wasm` (jobs) |
//! | pick the host | `/atlas` capacity + `/history` reputation ([`placement`], swarm-style ranking) |
//! | invoke (HTTP-style) | `AppRequest`/reply (`CeClient::request`) on [`protocol::INVOKE_TOPIC`] |
//! | event triggers | mesh pubsub (`subscribe` + `messages`) |
//! | billing | the job bid → credit escrow (no separate metering) |
//! | authorize deploy/invoke | `ce-cap` chains, abilities [`caps::ABILITY_DEPLOY`] / [`caps::ABILITY_INVOKE`] |
//! | state (which fn → which host) | a local JSON [`function::Registry`] |
//!
//! ## Quick start (library)
//!
//! ```no_run
//! use ce_fn::{FnClient, Function, Handler, Registry, bid_credits};
//! use ce_rs::CeClient;
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let registry = Registry::load(&Registry::default_path())?;
//! let mut fns = FnClient::new(CeClient::local(), registry);
//!
//! // Deploy a container function on the best atlas-ranked host.
//! let f = Function {
//!     name: "resize".into(),
//!     handler: Handler::Container { image: "myorg/resize:latest".into(), cmd: vec![] },
//!     cpu_cores: 1, mem_mb: 256, duration_secs: 120,
//!     bid: bid_credits(1), select: vec![],
//! };
//! let dep = fns.deploy(f, None).await?;
//! fns.registry().save(&Registry::default_path())?;
//!
//! // Invoke it (HTTP-style request/response over the mesh).
//! let out = fns.invoke("resize", b"<image bytes>").await?;
//! println!("{} bytes back from {}", out.len(), dep.host);
//! # Ok(()) }
//! ```
//!
//! See `src/bin/ce-fn.rs` for the CLI (`ce-fn deploy|invoke|on|kill|ls|hosts|grant`).

pub mod caps;
pub mod client;
pub mod function;
pub mod placement;
pub mod protocol;

pub use client::{DEFAULT_INVOKE_TIMEOUT_MS, FnClient, bid_credits};
pub use function::{Deployment, Function, Handler, Registry};
pub use placement::{Candidate, Requirements};
pub use protocol::{INVOKE_TOPIC, InvokeRequest, InvokeResponse, TriggerEvent};

// Re-export the money type so callers need not also depend on ce-rs just for `Amount`.
pub use ce_rs::Amount;
