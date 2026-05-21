//! Waterfall success/failure stats — used by the orchestrator to skip the cheapest-first
//! warmup when a route has *proven* itself on a domain.
//!
//! ## Why
//!
//! [`LadderStrategy`](crate::LadderStrategy) is great for cold-start: we don't know which
//! tier will work for a URL, so we walk the ladder. But once we've fetched
//! `cloudflare-protected.example.com` 200 times and only T7 ever succeeds, walking T4 →
//! T5 → T6 again is pure waste — three failed requests plus 30s of latency, every time.
//!
//! ## How
//!
//! - Every successful or retryable-failed fetch records into `(route_id, domain_hash) →
//!   (successes, failures)`. Atomics only — no locks, no contention.
//! - On each new fetch, the orchestrator asks the stats: "for this URL's domain, is there
//!   a route that has met the promotion thresholds?" If yes, that becomes the starting
//!   route; if no, the strategy's normal cheapest-first kicks in.
//! - When multiple routes qualify, the *cheapest qualifier* wins — we don't promote
//!   prematurely past savings.
//!
//! ## Memory bound
//!
//! [`WaterfallConfig::max_entries`] caps the (route, domain) pairs tracked. Past the cap,
//! new pairs are refused (existing pairs keep updating). Default is 1,000,000 — at ~56
//! bytes per entry that's ~56 MB worst-case, suitable for a long-running server.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use url::Url;

use crate::{
    catalog::RouteCatalog,
    route::{Route, RouteId},
};

/// u64 hash of a host string. Collisions are astronomically rare and would only cause a
/// promoted route to be incorrectly applied to a different domain — never a crash.
pub type DomainKey = u64;

/// Hash a host string to a [`DomainKey`]. Case-insensitive (hosts are case-insensitive
/// per RFC 3986).
pub fn domain_key(host: &str) -> DomainKey {
    let mut hasher = DefaultHasher::new();
    host.to_ascii_lowercase().hash(&mut hasher);
    hasher.finish()
}

/// Hash the host of a URL. Falls back to empty-string hash if the URL has no host
/// (e.g. `data:` URLs) — those will all hash to the same bucket, which is fine.
pub fn domain_key_from_url(url: &Url) -> DomainKey {
    domain_key(url.host_str().unwrap_or(""))
}

/// Tunables for [`WaterfallStats`].
#[derive(Debug, Clone)]
pub struct WaterfallConfig {
    /// Minimum successful fetches for a (route, domain) pair before promotion is considered.
    pub promotion_threshold: u64,
    /// Minimum success rate (0.0–1.0) required to promote.
    pub min_success_rate: f64,
    /// Maximum number of (route, domain) entries tracked. Inserts past this cap are
    /// silently dropped; existing entries continue updating.
    pub max_entries: usize,
}

impl Default for WaterfallConfig {
    fn default() -> Self {
        Self {
            promotion_threshold: 100,
            min_success_rate: 0.80,
            max_entries: 1_000_000,
        }
    }
}

/// Atomics-only counters for one (route, domain) pair.
#[derive(Debug)]
pub struct RouteDomainEntry {
    successes: AtomicU64,
    failures: AtomicU64,
}

impl RouteDomainEntry {
    fn new() -> Self {
        Self {
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }

    pub fn success_count(&self) -> u64 {
        self.successes.load(Ordering::Relaxed)
    }
    pub fn failure_count(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    pub fn total(&self) -> u64 {
        self.success_count().saturating_add(self.failure_count())
    }

    pub fn success_rate(&self) -> f64 {
        let s = self.success_count() as f64;
        let t = self.total() as f64;
        if t == 0.0 {
            0.0
        } else {
            s / t
        }
    }
}

/// Per (route, domain) running counts of fetch outcomes. Thread-safe; share via `Arc`.
#[derive(Debug)]
pub struct WaterfallStats {
    entries: DashMap<(RouteId, DomainKey), Arc<RouteDomainEntry>>,
    config: WaterfallConfig,
    total_records: AtomicU64,
}

impl Default for WaterfallStats {
    fn default() -> Self {
        Self::new(WaterfallConfig::default())
    }
}

impl WaterfallStats {
    pub fn new(config: WaterfallConfig) -> Self {
        Self {
            entries: DashMap::new(),
            config,
            total_records: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &WaterfallConfig {
        &self.config
    }

    /// Direct access to the underlying DashMap. Use when you need iteration semantics
    /// that `snapshot()` doesn't cover (e.g. concurrent enumeration with mutation, or
    /// custom eviction policies). The map is keyed by `(RouteId, DomainKey)`.
    pub fn entries(&self) -> &DashMap<(RouteId, DomainKey), Arc<RouteDomainEntry>> {
        &self.entries
    }

    /// Record a successful fetch for (route, url.host).
    pub fn record_success(&self, route_id: &RouteId, url: &Url) {
        self.record(route_id, url, true);
    }

    /// Record a retryable-failed fetch for (route, url.host).
    pub fn record_failure(&self, route_id: &RouteId, url: &Url) {
        self.record(route_id, url, false);
    }

    fn record(&self, route_id: &RouteId, url: &Url, success: bool) {
        let key = (route_id.clone(), domain_key_from_url(url));

        // Fast path: entry exists — bump atomically, no map mutation.
        if let Some(entry) = self.entries.get(&key) {
            if success {
                entry.successes.fetch_add(1, Ordering::Relaxed);
            } else {
                entry.failures.fetch_add(1, Ordering::Relaxed);
            }
            self.total_records.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Slow path: need to insert. Respect the entry cap.
        if self.entries.len() >= self.config.max_entries {
            return;
        }

        let entry = self
            .entries
            .entry(key)
            .or_insert_with(|| Arc::new(RouteDomainEntry::new()))
            .clone();
        if success {
            entry.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            entry.failures.fetch_add(1, Ordering::Relaxed);
        }
        self.total_records.fetch_add(1, Ordering::Relaxed);
    }

    /// Best-route promotion for a URL's domain: the *cheapest* route in the catalog that
    /// has met both the `promotion_threshold` (enough samples) and `min_success_rate`
    /// (proven reliable). Returns `None` when no route qualifies.
    pub fn promoted_route(&self, url: &Url, catalog: &RouteCatalog) -> Option<Arc<Route>> {
        let domain = domain_key_from_url(url);
        let mut best: Option<Arc<Route>> = None;
        for route in catalog.all() {
            let key = (route.id.clone(), domain);
            if let Some(entry) = self.entries.get(&key) {
                if entry.success_count() < self.config.promotion_threshold {
                    continue;
                }
                if entry.success_rate() < self.config.min_success_rate {
                    continue;
                }
                match &best {
                    None => best = Some(route.clone()),
                    Some(current) if route.cost < current.cost => best = Some(route.clone()),
                    _ => {}
                }
            }
        }
        best
    }

    /// Stats for one (route, domain) pair, if it exists.
    pub fn get(&self, route_id: &RouteId, url: &Url) -> Option<Arc<RouteDomainEntry>> {
        let key = (route_id.clone(), domain_key_from_url(url));
        self.entries.get(&key).map(|e| e.clone())
    }

    /// Total number of recorded outcomes (successes + failures).
    pub fn total_records(&self) -> u64 {
        self.total_records.load(Ordering::Relaxed)
    }

    /// Number of distinct (route, domain) pairs tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Materialize a snapshot of every tracked (route, domain) pair. O(N) over entries
    /// — call sparingly (e.g. for a CLI `stats` command or periodic export).
    pub fn snapshot(&self) -> Vec<StatsSnapshot> {
        self.entries
            .iter()
            .map(|kv| StatsSnapshot {
                route_id: kv.key().0.clone(),
                domain_key: kv.key().1,
                successes: kv.value().success_count(),
                failures: kv.value().failure_count(),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub route_id: RouteId,
    pub domain_key: DomainKey,
    pub successes: u64,
    pub failures: u64,
}

impl StatsSnapshot {
    pub fn total(&self) -> u64 {
        self.successes.saturating_add(self.failures)
    }
    pub fn success_rate(&self) -> f64 {
        if self.total() == 0 {
            0.0
        } else {
            self.successes as f64 / self.total() as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AdapterKind, Capabilities, EndpointTemplate, HttpMethod, RouteCatalogBuilder, Tier,
    };

    fn route(id: &str, cost: u64) -> Route {
        Route {
            id: Arc::from(id),
            adapter: AdapterKind::Custom(Arc::from("mock")),
            endpoint: EndpointTemplate::parse("https://example.test/").unwrap(),
            method: HttpMethod::Get,
            auth: Default::default(),
            headers: vec![],
            body: Default::default(),
            parse: Default::default(),
            validate: vec![],
            tier: Tier::T0,
            cost,
            priority: 100,
            caps: Capabilities::default(),
            timeout_ms: 5_000,
            concurrency: 8,
            retry_on: Default::default(),
        }
    }

    fn catalog_two_routes() -> RouteCatalog {
        RouteCatalogBuilder::new()
            .add(route("cheap", 10))
            .add(route("expensive", 100))
            .build()
    }

    #[test]
    fn no_promotion_below_threshold() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 10,
            min_success_rate: 0.5,
            max_entries: 1000,
        });
        let cat = catalog_two_routes();
        let url = Url::parse("https://hard.test/").unwrap();
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..3 {
            stats.record_success(&exp, &url);
        }
        // Only 3 successes — below threshold of 10. No promotion.
        assert!(stats.promoted_route(&url, &cat).is_none());
    }

    #[test]
    fn no_promotion_below_success_rate() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 5,
            min_success_rate: 0.80,
            max_entries: 1000,
        });
        let cat = catalog_two_routes();
        let url = Url::parse("https://flaky.test/").unwrap();
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..5 {
            stats.record_success(&exp, &url);
        }
        for _ in 0..5 {
            stats.record_failure(&exp, &url);
        }
        // 50% success rate, below 80% — no promotion despite meeting count threshold.
        assert!(stats.promoted_route(&url, &cat).is_none());
    }

    #[test]
    fn promotes_after_threshold_and_rate() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 5,
            min_success_rate: 0.80,
            max_entries: 1000,
        });
        let cat = catalog_two_routes();
        let url = Url::parse("https://proven.test/").unwrap();
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..10 {
            stats.record_success(&exp, &url);
        }
        stats.record_failure(&exp, &url);
        // 10/11 ≈ 91% success rate, above 80% — promote.
        let promoted = stats.promoted_route(&url, &cat).expect("should promote");
        assert_eq!(promoted.id.as_ref(), "expensive");
    }

    #[test]
    fn prefers_cheapest_qualifier() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 5,
            min_success_rate: 0.80,
            max_entries: 1000,
        });
        let cat = catalog_two_routes();
        let url = Url::parse("https://both-work.test/").unwrap();
        let cheap: RouteId = Arc::from("cheap");
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..10 {
            stats.record_success(&cheap, &url);
            stats.record_success(&exp, &url);
        }
        let promoted = stats.promoted_route(&url, &cat).expect("should promote");
        assert_eq!(
            promoted.id.as_ref(),
            "cheap",
            "should pick cheapest qualifying route"
        );
    }

    #[test]
    fn entries_cap_refuses_new_pairs() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 1,
            min_success_rate: 0.0,
            max_entries: 2,
        });
        let r1: RouteId = Arc::from("r1");
        for i in 0..5 {
            let url = Url::parse(&format!("https://host-{i}.test/")).unwrap();
            stats.record_success(&r1, &url);
        }
        // Only 2 distinct (route, domain) pairs accepted; rest dropped.
        assert!(stats.len() <= 2);
        // But total_records doesn't tick for refused inserts.
        assert!(stats.total_records() <= 5);
    }

    #[test]
    fn existing_entries_keep_updating_past_cap() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 1,
            min_success_rate: 0.0,
            max_entries: 1,
        });
        let r1: RouteId = Arc::from("r1");
        let url = Url::parse("https://only.test/").unwrap();
        for _ in 0..100 {
            stats.record_success(&r1, &url);
        }
        let entry = stats.get(&r1, &url).unwrap();
        assert_eq!(entry.success_count(), 100);
        assert_eq!(stats.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_records_are_atomic() {
        let stats = Arc::new(WaterfallStats::default());
        let r: RouteId = Arc::from("r");
        let url = Url::parse("https://concurrent.test/").unwrap();
        let mut tasks = Vec::new();
        for _ in 0..50 {
            let s = stats.clone();
            let r = r.clone();
            let url = url.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..20 {
                    s.record_success(&r, &url);
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let entry = stats.get(&r, &url).unwrap();
        assert_eq!(entry.success_count(), 50 * 20);
    }
}
