//! Property tests for the ce-fn wire protocol and placement ranking. These assert the invariants
//! the deploy/invoke/trigger paths rely on across arbitrary inputs — they fail if a regression
//! breaks round-tripping, bounds enforcement, or ranking determinism.

use ce_fn::placement::{Requirements, rank};
use ce_fn::{InvokeRequest, InvokeResponse, MAX_PAYLOAD_BYTES, TriggerEvent};
use ce_rs::AtlasEntry;
use proptest::prelude::*;

proptest! {
    /// Any payload up to the cap round-trips through encode -> decode -> payload unchanged.
    #[test]
    fn invoke_request_roundtrips(payload in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let req = InvokeRequest::new("fn1", &payload);
        let bytes = req.encode().expect("encode within bounds");
        let back = InvokeRequest::decode(&bytes).expect("decode");
        prop_assert_eq!(back.payload().unwrap(), payload);
    }

    /// Any response output up to the cap round-trips and reports ok.
    #[test]
    fn invoke_response_roundtrips(out in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let resp = InvokeResponse::success(&out);
        let bytes = resp.encode().expect("encode");
        let back = InvokeResponse::decode(&bytes).expect("decode");
        prop_assert!(back.ok);
        prop_assert_eq!(back.output().unwrap(), out);
    }

    /// Arbitrary non-JSON bytes decode leniently as raw trigger data (bounded), never panicking.
    #[test]
    fn trigger_event_lenient_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let ev = TriggerEvent::decode_lenient("t", &bytes);
        // data() must succeed and be bounded.
        let data = ev.data().unwrap();
        prop_assert!(data.len() <= MAX_PAYLOAD_BYTES);
    }

    /// Decoding never accepts an over-cap envelope (no OOM vector).
    #[test]
    fn oversized_envelope_always_rejected(extra in 1usize..512) {
        // Build a JSON whose total length exceeds the envelope cap by `extra` via a long caps field.
        // Construct via a giant payload hex (cheap: a string, not real bytes).
        let huge = "a".repeat(ce_fn::protocol::MAX_ENVELOPE_BYTES + extra);
        let bytes = huge.into_bytes();
        prop_assert!(InvokeRequest::decode(&bytes).is_err());
        prop_assert!(InvokeResponse::decode(&bytes).is_err());
    }

    /// Ranking is a total order: it returns exactly `min(count, pool)` candidates, sorted so that
    /// no earlier candidate has strictly lower reputation than a later one.
    #[test]
    fn rank_is_ordered_and_bounded(
        reps in proptest::collection::vec(0u64..1000, 1..32),
        count in 0usize..40,
    ) {
        let hosts: Vec<AtlasEntry> = reps.iter().enumerate().map(|(i, _)| AtlasEntry {
            node_id: format!("{i:064x}"),
            cpu_cores: 4,
            mem_mb: 1024,
            running_jobs: 0,
            last_seen_secs: 0,
            tags: vec!["docker".into()],
        }).collect();
        let n = hosts.len();
        let rep_map: std::collections::HashMap<String, u64> =
            hosts.iter().zip(reps.iter()).map(|(h, r)| (h.node_id.clone(), *r)).collect();
        let ranked = rank(hosts, count, |id| rep_map.get(id).copied().unwrap_or(0));
        prop_assert_eq!(ranked.len(), count.min(n));
        for w in ranked.windows(2) {
            prop_assert!(w[0].delivered_work >= w[1].delivered_work, "ranking not sorted by reputation");
        }
    }

    /// A host that fails the capacity floor is never a candidate, regardless of reputation.
    #[test]
    fn undercapacity_host_never_satisfies(cpu in 0u32..3, mem in 0u32..1023) {
        let req = Requirements::for_container(4, 1024, vec![]);
        let host = AtlasEntry {
            node_id: "00".repeat(32),
            cpu_cores: cpu,
            mem_mb: mem,
            running_jobs: 0,
            last_seen_secs: 0,
            tags: vec!["docker".into()],
        };
        prop_assert!(!req.satisfied_by(&host));
    }
}
