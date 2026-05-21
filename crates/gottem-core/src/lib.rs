//! gottem-core: universal scraper foundation, powered by [`spider`].
//!
//! gottem re-exports spider's [`RetryStrategy`](spider::retry_strategy::RetryStrategy) trait
//! family, [`HedgeTracker`], [`HedgeConfig`], [`AntiBotTech`] detection, and
//! [`RequestProxy`] — and adds a *cross-vendor* layer on top:
//!
//! - [`Route`] describes a vendor endpoint as **data** (no per-vendor code path).
//! - [`AdapterKind`] is a small finite set of protocols
//!   ([`DirectHttp`](AdapterKind::DirectHttp),
//!   [`HttpJson`](AdapterKind::HttpJson),
//!   [`HttpJsonlStream`](AdapterKind::HttpJsonlStream),
//!   [`ChromeCdp`](AdapterKind::ChromeCdp),
//!   [`SpiderLocal`](AdapterKind::SpiderLocal),
//!   [`Custom`](AdapterKind::Custom)).
//! - [`RouteCatalog`] is a frozen registry of routes loadable from TOML.
//! - [`Orchestrator`] drives requests against the catalog with cheap/race/hedge modes.
//! - [`RetryDirective`] additively wraps [`spider::retry_strategy::RetryDirective`] with
//!   `route: Option<Arc<Route>>`. Existing spider
//!   [`RetryStrategy`](spider::retry_strategy::RetryStrategy) impls plug in unchanged
//!   via [`SpiderStrategyAdapter`] — `route` stays `None` so behavior is identical.
//!
//! # Concurrency invariants
//!
//! - Catalog is `Arc<RouteCatalog>`, built once, frozen. Read-only lookups, no lock.
//! - Per-route concurrency cap is a `tokio::sync::Semaphore` (cancel-safe).
//! - Stats (EMA, error counts) are spider's atomics-only [`HedgeTracker`].
//! - Circuit breakers use only atomics ([`CircuitBreaker`]).
//! - Race / hedge use [`CancelToken`] + `tokio::select!`; losers' adapter calls are
//!   cancelled on drop, propagating through `reqwest::Response`'s drop.
//! - No `.await` is ever held across a `std::sync::Mutex` (CI-enforced by
//!   `clippy::await_holding_lock = "deny"`).

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

pub mod adapter;
pub mod budget;
pub mod cancel;
pub mod capabilities;
pub mod catalog;
pub mod circuit;
pub mod error;
pub mod orchestrator;
pub mod request;
pub mod response;
pub mod retry;
pub mod route;
pub mod templating;
pub mod tier;
pub mod validator;
pub mod waterfall;

// Re-exports from spider so downstream crates only depend on gottem-core for the trait surface.
pub use spider;
pub use spider::configuration::RequestProxy;
pub use spider::page::AntiBotTech;
pub use spider::utils::hedge::{HedgeConfig, HedgeTracker};

// Lock-free map re-exported so callers can iterate / extend waterfall stats without
// taking their own dashmap dep.
pub use dashmap;
pub use dashmap::DashMap;

pub use adapter::{Adapter, AdapterContext, AdapterRegistry};
pub use budget::Budget;
pub use cancel::CancelToken;
pub use capabilities::Capabilities;
pub use catalog::{RouteCatalog, RouteCatalogBuilder};
pub use circuit::CircuitBreaker;
pub use error::FetchError;
pub use orchestrator::{Mode, Orchestrator};
pub use request::{HttpMethod, ScrapeRequest};
pub use response::ScrapeResponse;
pub use retry::{
    AttemptOutcome, LadderStrategy, RetryDirective, RetryStrategy, SpiderStrategyAdapter,
};
pub use route::{
    AdapterKind, AuthSpec, BodyTemplate, CostExtract, EndpointTemplate, ResponseParse,
    RetryClassifier, Route, RouteId,
};
pub use tier::Tier;
pub use validator::Validator;
pub use waterfall::{
    domain_key, domain_key_from_url, DomainKey, RouteDomainEntry, ScoreWeights, StatsSnapshot,
    WaterfallConfig, WaterfallStats,
};
