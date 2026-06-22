//! Host selection for function placement — atlas-guided, reputation-ranked.
//!
//! This mirrors swarm's placement policy ([`swarm`'s `select_hosts`]): start from the capacity
//! atlas, keep only hosts that can actually run the workload (advertise `docker`, plus any
//! caller-required self-tags and enough capacity), then rank the survivors by proven on-chain
//! delivered work (most-proven first) so a function lands on the host most likely to honor it.
//!
//! Everything here is **pure** (no network) so it is unit-tested directly; the [`crate::FnClient`]
//! fetches the atlas + per-host history and feeds them in.

use ce_rs::AtlasEntry;

/// What a host must satisfy to be a placement candidate for a function.
#[derive(Debug, Clone, Default)]
pub struct Requirements {
    /// Required CPU cores (host must advertise at least this many).
    pub cpu_cores: u32,
    /// Required memory in MiB.
    pub mem_mb: u32,
    /// Extra capability self-tags the host must advertise (e.g. `gpu`, `wasm`). `docker` is
    /// always required for container functions and need not be listed here; for WASM functions
    /// pass `select_tag = Some("wasm")` and clear the implicit docker requirement via
    /// [`Requirements::for_wasm`].
    pub select: Vec<String>,
    /// Whether a `docker` self-tag is required (true for container functions, false for WASM).
    pub require_docker: bool,
}

impl Requirements {
    /// Requirements for a container (Docker) function.
    pub fn for_container(cpu_cores: u32, mem_mb: u32, select: Vec<String>) -> Self {
        Requirements { cpu_cores, mem_mb, select, require_docker: true }
    }

    /// Requirements for a WASM function — no Docker needed; the host must advertise `wasm`.
    pub fn for_wasm(cpu_cores: u32, mem_mb: u32, mut select: Vec<String>) -> Self {
        if !select.iter().any(|t| t == "wasm") {
            select.push("wasm".to_string());
        }
        Requirements { cpu_cores, mem_mb, select, require_docker: false }
    }

    /// Does `host` meet every requirement?
    pub fn satisfied_by(&self, host: &AtlasEntry) -> bool {
        if self.require_docker && !host.has_tag("docker") {
            return false;
        }
        if host.cpu_cores < self.cpu_cores {
            return false;
        }
        if host.mem_mb < self.mem_mb {
            return false;
        }
        self.select.iter().all(|t| host.has_tag(t))
    }
}

/// A scored placement candidate: a host plus its proven delivered-work reputation.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub host: AtlasEntry,
    /// On-chain delivered work (settled jobs + heartbeats) — higher is more proven.
    pub delivered_work: u64,
}

/// Filter `atlas` down to hosts that satisfy `req`. Pure.
pub fn candidates(atlas: Vec<AtlasEntry>, req: &Requirements) -> Vec<AtlasEntry> {
    atlas.into_iter().filter(|h| req.satisfied_by(h)).collect()
}

/// Rank `hosts` by reputation (most-proven first), breaking ties by fewest running jobs (least
/// loaded first) then by node id (stable, deterministic). `reputation(node_id) -> delivered_work`
/// is supplied by the caller (it comes from `GET /history/:node_id`). Truncates to `count`.
///
/// Pure and deterministic so placement is reproducible and unit-testable.
pub fn rank(
    hosts: Vec<AtlasEntry>,
    count: usize,
    reputation: impl Fn(&str) -> u64,
) -> Vec<Candidate> {
    let mut scored: Vec<Candidate> = hosts
        .into_iter()
        .map(|h| {
            let delivered_work = reputation(&h.node_id);
            Candidate { host: h, delivered_work }
        })
        .collect();
    scored.sort_by(|a, b| {
        b.delivered_work
            .cmp(&a.delivered_work)
            .then(a.host.running_jobs.cmp(&b.host.running_jobs))
            .then(a.host.node_id.cmp(&b.host.node_id))
    });
    scored.truncate(count);
    scored
}

/// Pick the single best host for a function: filter by `req`, rank, take the top one.
/// Returns `None` if no host qualifies. Pure.
pub fn best(
    atlas: Vec<AtlasEntry>,
    req: &Requirements,
    reputation: impl Fn(&str) -> u64,
) -> Option<Candidate> {
    let pool = candidates(atlas, req);
    rank(pool, 1, reputation).into_iter().next()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn container_requires_docker() {
        let req = Requirements::for_container(1, 128, vec![]);
        assert!(req.satisfied_by(&host("a", 4, 1024, 0, &["docker"])));
        assert!(!req.satisfied_by(&host("b", 4, 1024, 0, &["linux"])));
    }

    #[test]
    fn wasm_requires_wasm_tag_not_docker() {
        let req = Requirements::for_wasm(1, 64, vec![]);
        assert!(req.satisfied_by(&host("a", 2, 256, 0, &["wasm"])));
        // docker alone is not enough for a wasm function
        assert!(!req.satisfied_by(&host("b", 2, 256, 0, &["docker"])));
    }

    #[test]
    fn capacity_floor_enforced() {
        let req = Requirements::for_container(4, 2048, vec![]);
        assert!(!req.satisfied_by(&host("small", 2, 4096, 0, &["docker"]))); // too few cores
        assert!(!req.satisfied_by(&host("lowmem", 8, 1024, 0, &["docker"]))); // too little mem
        assert!(req.satisfied_by(&host("ok", 4, 2048, 0, &["docker"])));
    }

    #[test]
    fn extra_select_tags_required() {
        let req = Requirements::for_container(1, 64, vec!["gpu".into()]);
        assert!(req.satisfied_by(&host("a", 4, 1024, 0, &["docker", "gpu"])));
        assert!(!req.satisfied_by(&host("b", 4, 1024, 0, &["docker"])));
    }

    #[test]
    fn candidates_filters_pool() {
        let atlas = vec![
            host("a", 4, 1024, 0, &["docker", "gpu"]),
            host("b", 4, 1024, 0, &["linux"]),
            host("c", 4, 1024, 0, &["docker"]),
        ];
        let req = Requirements::for_container(1, 128, vec![]);
        let got = candidates(atlas, &req);
        assert_eq!(got.iter().map(|h| h.node_id.as_str()).collect::<Vec<_>>(), vec!["a", "c"]);
    }

    #[test]
    fn rank_most_proven_first() {
        let hosts = vec![
            host("low", 4, 1024, 0, &["docker"]),
            host("high", 4, 1024, 0, &["docker"]),
        ];
        let rep = |id: &str| if id == "high" { 100 } else { 1 };
        let ranked = rank(hosts, 10, rep);
        assert_eq!(ranked[0].host.node_id, "high");
        assert_eq!(ranked[0].delivered_work, 100);
        assert_eq!(ranked[1].host.node_id, "low");
    }

    #[test]
    fn rank_ties_broken_by_load_then_id() {
        let hosts = vec![
            host("busy", 4, 1024, 5, &["docker"]),
            host("idle", 4, 1024, 0, &["docker"]),
        ];
        // equal reputation → least loaded first
        let ranked = rank(hosts, 10, |_| 7);
        assert_eq!(ranked[0].host.node_id, "idle");
        assert_eq!(ranked[1].host.node_id, "busy");
    }

    #[test]
    fn rank_truncates_to_count() {
        let hosts = vec![
            host("a", 4, 1024, 0, &["docker"]),
            host("b", 4, 1024, 0, &["docker"]),
            host("c", 4, 1024, 0, &["docker"]),
        ];
        let ranked = rank(hosts, 2, |_| 0);
        assert_eq!(ranked.len(), 2);
    }

    #[test]
    fn best_picks_top_qualified() {
        let atlas = vec![
            host("nodocker", 4, 1024, 0, &["linux"]),
            host("weak", 4, 1024, 0, &["docker"]),
            host("strong", 4, 1024, 0, &["docker"]),
        ];
        let req = Requirements::for_container(1, 128, vec![]);
        let rep = |id: &str| if id == "strong" { 50 } else { 0 };
        let chosen = best(atlas, &req, rep).expect("a host qualifies");
        assert_eq!(chosen.host.node_id, "strong");
    }

    #[test]
    fn best_none_when_nothing_qualifies() {
        let atlas = vec![host("a", 1, 64, 0, &["linux"])];
        let req = Requirements::for_container(1, 64, vec![]);
        assert!(best(atlas, &req, |_| 0).is_none());
    }
}
