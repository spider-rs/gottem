//! Waterfall success/failure stats — used by the orchestrator to skip the cheapest-first
//! warmup when a route has *proven* itself on a domain.
//!
//! ## Why
//!
//! [`LadderStrategy`](crate::LadderStrategy) is great for cold-start: we don't know which
//! tier will work for a URL, so we walk the ladder. Once we've fetched
//! `cloudflare-protected.example.com` 200 times and learned which routes actually work,
//! walking T0 → T4 → T5 → T6 again is pure waste — every failed tier costs time and money.
//!
//! ## What the qualifier scores on
//!
//! - **Cost** — cheaper is better (within the cohort of qualifying routes)
//! - **Speed** — lower EMA latency is better
//! - **Reliability** — higher success rate is better
//! - **Confidence** — more samples is better (log-scaled so 10× more data isn't 10× weight)
//!
//! Weights are configurable via [`ScoreWeights`]. The default leans toward
//! reliability + cost; latency is a secondary signal. Routes that haven't crossed
//! [`WaterfallConfig::promotion_threshold`] successes OR [`WaterfallConfig::min_success_rate`]
//! are excluded entirely — scoring runs only on the proven cohort.
//!
//! ## Memory bound (no memleaks)
//!
//! Three layers of protection:
//!
//! 1. **`max_entries` cap** — DashMap is hard-capped (default 1M ~ 64 MB).
//! 2. **Stale-entry auto-eviction on insert** — when at cap, the inserter scans a small
//!    sample (default 16 entries) and evicts the *oldest* before adding. O(1) amortized.
//! 3. **`evict_older_than(Duration)`** — user-callable; long-running processes should
//!    invoke this periodically (e.g. every hour) to drop entries unused for a configurable
//!    grace period.
//!
//! No Arc cycles: WaterfallStats owns its entries; entries hold only atomic primitives.
//! Dropping the stats drops every entry. Verified by [`tests::no_leak_on_drop`].

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use url::Url;

use crate::{
    catalog::RouteCatalog,
    route::{Route, RouteId},
};

/// u64 hash of a host string. Collisions are astronomically rare and would only cause a
/// promoted route to be incorrectly applied to a different domain — never a crash.
pub type DomainKey = u64;

pub fn domain_key(host: &str) -> DomainKey {
    let mut hasher = DefaultHasher::new();
    host.to_ascii_lowercase().hash(&mut hasher);
    hasher.finish()
}

pub fn domain_key_from_url(url: &Url) -> DomainKey {
    domain_key(url.host_str().unwrap_or(""))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Per-factor weights for the promotion scorer. Each in [0.0, 1.0]; they don't need to
/// sum to 1.0 but doing so keeps the final score in [0.0, 1.0] for easy reasoning.
#[derive(Debug, Clone)]
pub struct ScoreWeights {
    /// How much cost matters. Cheaper route → higher score component.
    pub cost: f64,
    /// How much latency matters. Faster route → higher score component.
    pub speed: f64,
    /// How much success rate matters.
    pub reliability: f64,
    /// How much sample size matters. Log-scaled — 10k samples doesn't 100×-dominate 100.
    pub confidence: f64,
}

impl Default for ScoreWeights {
    fn default() -> Self {
        Self {
            cost: 0.30,
            speed: 0.20,
            reliability: 0.35,
            confidence: 0.15,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WaterfallConfig {
    /// Minimum successful fetches for a (route, domain) pair before promotion is considered.
    pub promotion_threshold: u64,
    /// Minimum success rate (0.0–1.0) required to qualify.
    pub min_success_rate: f64,
    /// Maximum number of (route, domain) entries tracked.
    pub max_entries: usize,
    /// Entries older than this on insert are eligible for auto-eviction to make room.
    pub stale_after: Duration,
    /// Sample size for the at-capacity stale-entry probe. Probabilistic LRU.
    pub eviction_sample_size: usize,
    /// Scoring weights for the qualifier.
    pub weights: ScoreWeights,
}

impl Default for WaterfallConfig {
    fn default() -> Self {
        Self {
            promotion_threshold: 100,
            min_success_rate: 0.80,
            max_entries: 1_000_000,
            stale_after: Duration::from_secs(60 * 60 * 24 * 7), // 7 days
            eviction_sample_size: 16,
            weights: ScoreWeights::default(),
        }
    }
}

/// Atomics-only counters for one (route, domain) pair.
#[derive(Debug)]
pub struct RouteDomainEntry {
    successes: AtomicU64,
    failures: AtomicU64,
    /// Exponential moving average of successful-fetch latency, in milliseconds.
    ema_latency_ms: AtomicU64,
    /// Unix-epoch ms of the most recent update — used for stale-entry eviction.
    last_seen_ms: AtomicU64,
}

impl RouteDomainEntry {
    fn new() -> Self {
        Self {
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            ema_latency_ms: AtomicU64::new(0),
            last_seen_ms: AtomicU64::new(now_ms()),
        }
    }

    /// Build an entry from persisted counters — used to seed warm state on
    /// boot via [`WaterfallStats::seed_entry`].
    pub fn from_parts(
        successes: u64,
        failures: u64,
        ema_latency_ms: u64,
        last_seen_ms: u64,
    ) -> Self {
        Self {
            successes: AtomicU64::new(successes),
            failures: AtomicU64::new(failures),
            ema_latency_ms: AtomicU64::new(ema_latency_ms),
            last_seen_ms: AtomicU64::new(last_seen_ms),
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
        let t = self.total() as f64;
        if t == 0.0 {
            0.0
        } else {
            self.success_count() as f64 / t
        }
    }
    pub fn ema_latency_ms(&self) -> u64 {
        self.ema_latency_ms.load(Ordering::Relaxed)
    }
    pub fn last_seen_ms(&self) -> u64 {
        self.last_seen_ms.load(Ordering::Relaxed)
    }

    fn record_success(&self, latency_ms: u64) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.touch();
        self.update_ema_latency(latency_ms);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.touch();
    }

    fn touch(&self) {
        self.last_seen_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// EMA with alpha = 0.2 (same shape as spider's HedgeTracker). Lock-free CAS loop —
    /// concurrent updates from different worker threads always merge without loss.
    fn update_ema_latency(&self, sample_ms: u64) {
        let _ = self
            .ema_latency_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                let next = if old == 0 {
                    sample_ms
                } else {
                    (old * 4 + sample_ms) / 5
                };
                Some(next)
            });
    }
}

#[derive(Debug)]
pub struct WaterfallStats {
    entries: DashMap<(RouteId, DomainKey), Arc<RouteDomainEntry>>,
    config: WaterfallConfig,
    total_records: AtomicU64,
    /// Total entries evicted ever — useful for ops dashboards verifying memory bound.
    evictions: AtomicU64,
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
            evictions: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &WaterfallConfig {
        &self.config
    }

    /// Direct access to the underlying DashMap.
    pub fn entries(&self) -> &DashMap<(RouteId, DomainKey), Arc<RouteDomainEntry>> {
        &self.entries
    }

    /// Seed a `(route, domain)` entry from persisted counters — used to warm
    /// the waterfall on startup so routing intelligence survives a restart.
    ///
    /// A no-op if the pair already has a live entry (a running record always
    /// wins over stale persisted state) or if the `max_entries` cap is full.
    pub fn seed_entry(
        &self,
        route_id: RouteId,
        domain: DomainKey,
        successes: u64,
        failures: u64,
        ema_latency_ms: u64,
        last_seen_ms: u64,
    ) {
        if self.entries.len() >= self.config.max_entries {
            return;
        }
        self.entries.entry((route_id, domain)).or_insert_with(|| {
            Arc::new(RouteDomainEntry::from_parts(
                successes,
                failures,
                ema_latency_ms,
                last_seen_ms,
            ))
        });
    }

    /// Record a successful fetch with its observed latency.
    pub fn record_success(&self, route_id: &RouteId, url: &Url, latency_ms: u64) {
        if let Some(entry) = self.get_or_insert(route_id, url) {
            entry.record_success(latency_ms);
            self.total_records.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a retryable-failed fetch. Latency is omitted because failed fetches don't
    /// contribute meaningful latency data — they're cut short by errors.
    pub fn record_failure(&self, route_id: &RouteId, url: &Url) {
        if let Some(entry) = self.get_or_insert(route_id, url) {
            entry.record_failure();
            self.total_records.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn get_or_insert(&self, route_id: &RouteId, url: &Url) -> Option<Arc<RouteDomainEntry>> {
        let key = (route_id.clone(), domain_key_from_url(url));

        // Fast path: entry exists.
        if let Some(entry) = self.entries.get(&key) {
            return Some(entry.clone());
        }

        // Slow path: at capacity? Try to evict a stale neighbor first.
        if self.entries.len() >= self.config.max_entries {
            self.try_evict_one_stale();
            if self.entries.len() >= self.config.max_entries {
                // No room and nothing stale — refuse the insert. This is the "bounded
                // behavior past the cap" promise; we never grow the map unbounded.
                return None;
            }
        }

        Some(
            self.entries
                .entry(key)
                .or_insert_with(|| Arc::new(RouteDomainEntry::new()))
                .clone(),
        )
    }

    /// Probabilistic LRU: sample N entries, evict the oldest one that crossed the
    /// `stale_after` threshold. Returns true if anything was evicted.
    fn try_evict_one_stale(&self) -> bool {
        let cutoff = now_ms().saturating_sub(self.config.stale_after.as_millis() as u64);
        let sample = self.config.eviction_sample_size.max(1);
        let mut oldest: Option<((RouteId, DomainKey), u64)> = None;

        for kv in self.entries.iter().take(sample) {
            let ts = kv.value().last_seen_ms();
            if ts <= cutoff {
                match &oldest {
                    None => oldest = Some((kv.key().clone(), ts)),
                    Some((_, prev_ts)) if ts < *prev_ts => oldest = Some((kv.key().clone(), ts)),
                    _ => {}
                }
            }
        }

        if let Some((key, _)) = oldest {
            self.entries.remove(&key);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Drop every entry whose `last_seen_ms` is older than `now - max_age`. Returns the
    /// number of entries evicted. Safe to call concurrently with fetches; tasks accessing
    /// an evicted entry just get None and re-insert.
    pub fn evict_older_than(&self, max_age: Duration) -> u64 {
        let cutoff = now_ms().saturating_sub(max_age.as_millis() as u64);
        let to_remove: Vec<(RouteId, DomainKey)> = self
            .entries
            .iter()
            .filter(|kv| kv.value().last_seen_ms() <= cutoff)
            .map(|kv| kv.key().clone())
            .collect();
        let n = to_remove.len() as u64;
        for k in to_remove {
            self.entries.remove(&k);
        }
        self.evictions.fetch_add(n, Ordering::Relaxed);
        n
    }

    /// Best-route promotion for a URL's domain: scores every qualifying route against
    /// `WaterfallConfig::weights` and returns the winner. Returns `None` when no route
    /// has met the qualification gates (threshold + success rate).
    ///
    /// Scoring per qualifier (each factor in [0.0, 1.0]):
    /// - **cost_factor** = `1 - cost / max_cost_in_cohort`
    /// - **speed_factor** = `1 - ema_latency / max_latency_in_cohort` (1.0 if no latency data)
    /// - **reliability_factor** = `success_rate`
    /// - **confidence_factor** = `ln(samples) / ln(max_samples_in_cohort)`
    ///
    /// Final score = weighted sum. Highest wins; ties broken by lower cost.
    pub fn promoted_route(&self, url: &Url, catalog: &RouteCatalog) -> Option<Arc<Route>> {
        let domain = domain_key_from_url(url);
        let mut cohort: Vec<(Arc<Route>, Arc<RouteDomainEntry>)> = Vec::new();
        for route in catalog.all() {
            let key = (route.id.clone(), domain);
            if let Some(entry) = self.entries.get(&key) {
                if entry.success_count() < self.config.promotion_threshold {
                    continue;
                }
                if entry.success_rate() < self.config.min_success_rate {
                    continue;
                }
                cohort.push((route.clone(), entry.clone()));
            }
        }
        if cohort.is_empty() {
            return None;
        }

        // Normalize against cohort maximums.
        let max_cost = cohort.iter().map(|(r, _)| r.cost).max().unwrap_or(1).max(1);
        let max_latency = cohort
            .iter()
            .map(|(_, e)| e.ema_latency_ms())
            .max()
            .unwrap_or(1)
            .max(1);
        let max_samples = cohort
            .iter()
            .map(|(_, e)| e.total())
            .max()
            .unwrap_or(1)
            .max(1);

        let w = &self.config.weights;
        let mut best: Option<(Arc<Route>, f64)> = None;
        for (route, entry) in &cohort {
            let cost_factor = 1.0 - (route.cost as f64 / max_cost as f64);
            let latency = entry.ema_latency_ms();
            // If no latency yet (only failures recorded? unlikely past threshold), treat
            // as average so we don't unfairly penalize.
            let speed_factor = if latency == 0 {
                0.5
            } else {
                1.0 - (latency as f64 / max_latency as f64)
            };
            let reliability_factor = entry.success_rate();
            let confidence_factor = if max_samples <= 1 {
                1.0
            } else {
                (entry.total() as f64).ln().max(0.0) / (max_samples as f64).ln().max(1e-9)
            };
            let score = w.cost * cost_factor
                + w.speed * speed_factor
                + w.reliability * reliability_factor
                + w.confidence * confidence_factor;
            match &best {
                None => best = Some((route.clone(), score)),
                Some((current, current_score)) => {
                    if score > *current_score
                        || (score == *current_score && route.cost < current.cost)
                    {
                        best = Some((route.clone(), score));
                    }
                }
            }
        }
        best.map(|(r, _)| r)
    }

    pub fn get(&self, route_id: &RouteId, url: &Url) -> Option<Arc<RouteDomainEntry>> {
        let key = (route_id.clone(), domain_key_from_url(url));
        self.entries.get(&key).map(|e| e.clone())
    }

    pub fn total_records(&self) -> u64 {
        self.total_records.load(Ordering::Relaxed)
    }
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn snapshot(&self) -> Vec<StatsSnapshot> {
        self.entries
            .iter()
            .map(|kv| StatsSnapshot {
                route_id: kv.key().0.clone(),
                domain_key: kv.key().1,
                successes: kv.value().success_count(),
                failures: kv.value().failure_count(),
                ema_latency_ms: kv.value().ema_latency_ms(),
                last_seen_ms: kv.value().last_seen_ms(),
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
    pub ema_latency_ms: u64,
    pub last_seen_ms: u64,
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
            cost_extract: None,
        }
    }

    fn cat_two() -> RouteCatalog {
        RouteCatalogBuilder::new()
            .add(route("cheap", 10))
            .add(route("expensive", 100))
            .build()
    }

    fn cat_three() -> RouteCatalog {
        RouteCatalogBuilder::new()
            .add(route("cheap_slow", 10))
            .add(route("mid_fast", 50))
            .add(route("expensive_proven", 100))
            .build()
    }

    fn permissive() -> WaterfallConfig {
        WaterfallConfig {
            promotion_threshold: 5,
            min_success_rate: 0.80,
            max_entries: 1000,
            stale_after: Duration::from_secs(3600),
            eviction_sample_size: 16,
            weights: ScoreWeights::default(),
        }
    }

    #[test]
    fn no_promotion_below_threshold() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 10,
            ..permissive()
        });
        let cat = cat_two();
        let url = Url::parse("https://hard.test/").unwrap();
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..3 {
            stats.record_success(&exp, &url, 1000);
        }
        assert!(stats.promoted_route(&url, &cat).is_none());
    }

    #[test]
    fn no_promotion_below_success_rate() {
        let stats = WaterfallStats::new(permissive());
        let cat = cat_two();
        let url = Url::parse("https://flaky.test/").unwrap();
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..5 {
            stats.record_success(&exp, &url, 1000);
        }
        for _ in 0..5 {
            stats.record_failure(&exp, &url);
        }
        assert!(stats.promoted_route(&url, &cat).is_none());
    }

    #[test]
    fn scorer_picks_cheapest_when_speed_and_reliability_equal() {
        let stats = WaterfallStats::new(permissive());
        let cat = cat_two();
        let url = Url::parse("https://both-work.test/").unwrap();
        let cheap: RouteId = Arc::from("cheap");
        let exp: RouteId = Arc::from("expensive");
        for _ in 0..10 {
            stats.record_success(&cheap, &url, 500);
            stats.record_success(&exp, &url, 500);
        }
        let promoted = stats.promoted_route(&url, &cat).expect("promote");
        assert_eq!(promoted.id.as_ref(), "cheap");
    }

    #[test]
    fn scorer_picks_faster_when_cost_equal() {
        // Two routes with same cost; the faster one should win.
        let cat = RouteCatalogBuilder::new()
            .add(route("a", 50))
            .add(route("b", 50))
            .build();
        let stats = WaterfallStats::new(permissive());
        let url = Url::parse("https://speed.test/").unwrap();
        let a: RouteId = Arc::from("a");
        let b: RouteId = Arc::from("b");
        // a is slow (3s), b is fast (200ms) — same success counts.
        for _ in 0..10 {
            stats.record_success(&a, &url, 3000);
            stats.record_success(&b, &url, 200);
        }
        let promoted = stats.promoted_route(&url, &cat).expect("promote");
        assert_eq!(
            promoted.id.as_ref(),
            "b",
            "faster route should win equal-cost tie"
        );
    }

    #[test]
    fn scorer_promotes_high_confidence_route() {
        // expensive_proven has 10000 samples at 99% rate; mid_fast has barely-qualifying
        // 5 samples at 100% rate. The high-confidence route should win even though it's
        // more expensive.
        let cat = cat_three();
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 5,
            min_success_rate: 0.80,
            max_entries: 1000,
            stale_after: Duration::from_secs(3600),
            eviction_sample_size: 16,
            // Lean heavily on confidence for this test.
            weights: ScoreWeights {
                cost: 0.10,
                speed: 0.10,
                reliability: 0.20,
                confidence: 0.60,
            },
        });
        let url = Url::parse("https://confidence.test/").unwrap();
        let mid_fast: RouteId = Arc::from("mid_fast");
        let proven: RouteId = Arc::from("expensive_proven");
        for _ in 0..5 {
            stats.record_success(&mid_fast, &url, 100);
        }
        for _ in 0..9900 {
            stats.record_success(&proven, &url, 500);
        }
        for _ in 0..100 {
            stats.record_failure(&proven, &url);
        }
        let promoted = stats.promoted_route(&url, &cat).expect("promote");
        assert_eq!(
            promoted.id.as_ref(),
            "expensive_proven",
            "10k-sample 99% route should beat 5-sample 100% route on confidence"
        );
    }

    #[test]
    fn entries_cap_with_no_stale_refuses_new_inserts() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 1,
            min_success_rate: 0.0,
            max_entries: 2,
            stale_after: Duration::from_secs(3600),
            eviction_sample_size: 16,
            weights: ScoreWeights::default(),
        });
        let r1: RouteId = Arc::from("r1");
        for i in 0..5 {
            let url = Url::parse(&format!("https://host-{i}.test/")).unwrap();
            stats.record_success(&r1, &url, 100);
        }
        assert!(stats.len() <= 2);
    }

    #[test]
    fn evict_older_than_drops_stale_entries() {
        let stats = WaterfallStats::new(permissive());
        let r1: RouteId = Arc::from("r1");
        let url = Url::parse("https://a.test/").unwrap();
        stats.record_success(&r1, &url, 100);
        assert_eq!(stats.len(), 1);

        // Backdate last_seen to look ancient.
        if let Some(entry) = stats.get(&r1, &url) {
            entry.last_seen_ms.store(0, Ordering::Relaxed);
        }

        let evicted = stats.evict_older_than(Duration::from_secs(1));
        assert_eq!(evicted, 1);
        assert_eq!(stats.len(), 0);
        assert_eq!(stats.evictions(), 1);
    }

    #[test]
    fn auto_evict_makes_room_at_capacity() {
        let stats = WaterfallStats::new(WaterfallConfig {
            promotion_threshold: 1,
            min_success_rate: 0.0,
            max_entries: 1,
            stale_after: Duration::from_millis(0), // everything is "stale" immediately
            eviction_sample_size: 16,
            weights: ScoreWeights::default(),
        });
        let r1: RouteId = Arc::from("r1");
        // First insert.
        stats.record_success(&r1, &Url::parse("https://a.test/").unwrap(), 100);
        assert_eq!(stats.len(), 1);

        // At-capacity insert for a different domain. With stale_after=0, the existing
        // entry is immediately eligible — auto-eviction should make room.
        stats.record_success(&r1, &Url::parse("https://b.test/").unwrap(), 100);
        assert_eq!(stats.len(), 1);
        assert!(stats.evictions() >= 1);
    }

    #[test]
    fn no_leak_on_drop() {
        // Verify dropping the WaterfallStats releases all Arcs in its entries.
        let stats = WaterfallStats::default();
        let r1: RouteId = Arc::from("r1");
        let url = Url::parse("https://leak.test/").unwrap();
        stats.record_success(&r1, &url, 100);
        let entry_arc = stats.get(&r1, &url).expect("entry exists");
        // Two Arcs: one in the DashMap, one we just cloned out.
        assert_eq!(Arc::strong_count(&entry_arc), 2);
        drop(stats);
        // After dropping the stats, our local Arc is the only owner.
        assert_eq!(Arc::strong_count(&entry_arc), 1);
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
                for i in 0..20 {
                    s.record_success(&r, &url, 100 + i as u64);
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let entry = stats.get(&r, &url).unwrap();
        assert_eq!(entry.success_count(), 50 * 20);
        // EMA should be in the recorded range (100..=120ms).
        let ema = entry.ema_latency_ms();
        assert!((100..=120).contains(&ema), "ema out of range: {ema}");
    }
}
