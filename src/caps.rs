//! Capability helpers for ce-fn — thin wrappers over `ce-cap`.
//!
//! ce-fn defines two opaque abilities on the shared `ce-cap` chain primitive:
//! `fn:deploy` (may place a function on a host) and `fn:invoke` (may call a deployed function).
//! These are plain strings (ce-cap assigns abilities no meaning); the function runtime is the
//! enforcement point and calls [`authorize_invoke`] before running a handler.
//!
//! Issuance is the zero-config personal case from `ce-cap`: a host self-delegates to a caller, who
//! presents the resulting chain on invoke. Org/fleet roots work identically — point the runtime at
//! a configured root key instead of self.

use anyhow::{Result, anyhow};
use ce_iam_core::{Caveats, Resource, SignedCapability, authorize, decode_chain, encode_chain};
use ce_identity::{Identity, NodeId};

/// Ability: may deploy/place a function.
pub const ABILITY_DEPLOY: &str = "fn:deploy";
/// Ability: may invoke a deployed function.
pub const ABILITY_INVOKE: &str = "fn:invoke";

/// Mint a capability token (hex chain) authorizing `audience` to perform `abilities` on the nodes
/// selected by `resource`, signed by `issuer` (root delegation: `parent = None`). `not_after` is a
/// unix-seconds expiry (0 = never). Returns the portable hex token to hand to the audience.
pub fn grant(
    issuer: &Identity,
    audience: NodeId,
    abilities: &[&str],
    resource: Resource,
    not_after: u64,
    nonce: u64,
) -> String {
    let caveats = Caveats { not_after, ..Default::default() };
    let cap = SignedCapability::issue(
        issuer,
        audience,
        abilities.iter().map(|s| s.to_string()).collect(),
        resource,
        caveats,
        nonce,
        None,
    );
    encode_chain(&[cap])
}

/// Verify that `requester` may perform `action` on this node, given a presented capability `token`
/// (hex chain) and the set of `accepted_roots` (the node's own id is always implicitly accepted).
/// `now` is unix seconds; `self_tags` are this node's atlas self-tags. `is_revoked` consults the
/// on-chain revocation set (pass a closure over `CeClient::revoked`).
///
/// Returns `Ok(())` if allowed, else an error whose message is safe to surface.
#[allow(clippy::too_many_arguments)]
pub fn authorize_invoke(
    self_id: &NodeId,
    accepted_roots: &[NodeId],
    self_tags: &[String],
    now: u64,
    requester: &NodeId,
    action: &str,
    token: &str,
    is_revoked: &(dyn Fn(&NodeId, u64) -> bool + Sync),
) -> Result<()> {
    let chain = decode_chain(token).map_err(|e| anyhow!("bad capability token: {e}"))?;
    authorize(self_id, accepted_roots, self_tags, now, requester, action, &chain, is_revoked)
        .map_err(|reason| anyhow!("capability denied: {reason}"))
}

/// Decode a hex token to a chain (for inspection / re-delegation).
pub fn decode(token: &str) -> Result<Vec<SignedCapability>> {
    decode_chain(token).map_err(|e| anyhow!("bad capability token: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ce_identity::Identity;

    /// Generate a fresh identity in a unique temp directory (ce-identity has no in-memory
    /// constructor; `load_or_generate` writes a key file the first time).
    fn gen_identity(tag: &str) -> Identity {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ce-fn-id-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Identity::load_or_generate(&dir).expect("generate identity")
    }

    #[test]
    fn grant_and_authorize_invoke_roundtrip() {
        // host self-delegates fn:invoke to a caller, then authorizes the caller's request
        let host = gen_identity("host");
        let caller = gen_identity("caller");

        let token = grant(
            &host,
            caller.node_id(),
            &[ABILITY_INVOKE],
            Resource::Node(host.node_id()),
            0,
            1,
        );

        let no_revoke = |_: &NodeId, _: u64| false;
        let res = authorize_invoke(
            &host.node_id(),
            &[], // own id is implicitly an accepted root
            &[],
            0,
            &caller.node_id(),
            ABILITY_INVOKE,
            &token,
            &no_revoke,
        );
        assert!(res.is_ok(), "self-delegated invoke must be authorized: {res:?}");
    }

    #[test]
    fn wrong_ability_denied() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        // granted deploy, but tries to invoke
        let token = grant(&host, caller.node_id(), &[ABILITY_DEPLOY], Resource::Any, 0, 7);
        let res = authorize_invoke(
            &host.node_id(),
            &[],
            &[],
            0,
            &caller.node_id(),
            ABILITY_INVOKE,
            &token,
            &|_, _| false,
        );
        assert!(res.is_err(), "invoke must be denied when only deploy was granted");
    }

    #[test]
    fn revoked_chain_denied() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let token = grant(&host, caller.node_id(), &[ABILITY_INVOKE], Resource::Any, 0, 42);
        // revoke (issuer=host, nonce=42)
        let revoked = |issuer: &NodeId, nonce: u64| *issuer == host.node_id() && nonce == 42;
        let res = authorize_invoke(
            &host.node_id(),
            &[],
            &[],
            0,
            &caller.node_id(),
            ABILITY_INVOKE,
            &token,
            &revoked,
        );
        assert!(res.is_err(), "revoked capability must be denied");
    }

    #[test]
    fn bad_token_errors() {
        let host = gen_identity("host");
        let caller = gen_identity("caller");
        let res = authorize_invoke(
            &host.node_id(),
            &[],
            &[],
            0,
            &caller.node_id(),
            ABILITY_INVOKE,
            "not-hex!!",
            &|_, _| false,
        );
        assert!(res.is_err());
    }
}
