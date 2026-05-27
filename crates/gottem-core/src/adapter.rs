use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures_core::Stream;

use crate::{
    cancel::CancelToken,
    crawl::{CrawlRequest, PageEntry},
    error::FetchError,
    request::ScrapeRequest,
    response::ScrapeResponse,
    route::{AdapterKind, Route},
};

/// Per-attempt context handed to adapters. Lets adapters know which retry this is and
/// when the attempt started (for tail-latency measurement).
#[derive(Debug)]
pub struct AdapterContext {
    pub attempt: u32,
    pub started: Instant,
}

impl AdapterContext {
    pub fn new(attempt: u32) -> Self {
        Self {
            attempt,
            started: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.started.elapsed()
    }
}

/// Implementing this trait wires a protocol family into the gottem orchestrator.
///
/// Each adapter is registered for exactly one [`AdapterKind`]. Implementations
/// **must** respect `cancel` — typically via `tokio::select!` against
/// [`CancelToken::cancelled`] — so race winners can promptly free losers.
#[async_trait]
pub trait Adapter: Send + Sync + 'static {
    fn kind(&self) -> AdapterKind;

    async fn execute(
        &self,
        route: &Route,
        req: &ScrapeRequest,
        ctx: &AdapterContext,
        cancel: &CancelToken,
    ) -> Result<ScrapeResponse, FetchError>;
}

/// Registry mapping each [`AdapterKind`] to a concrete [`Adapter`] implementation.
/// Built once at startup, frozen after that — reads are lock-free hash lookups.
#[derive(Default)]
pub struct AdapterRegistry {
    by_kind: HashMap<AdapterKind, Arc<dyn Adapter>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, adapter: Arc<dyn Adapter>) {
        let k = adapter.kind();
        self.by_kind.insert(k, adapter);
    }

    pub fn get(&self, kind: &AdapterKind) -> Option<Arc<dyn Adapter>> {
        self.by_kind.get(kind).cloned()
    }

    pub fn len(&self) -> usize {
        self.by_kind.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_kind.is_empty()
    }
}

impl fmt::Debug for AdapterRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterRegistry")
            .field("kinds", &self.by_kind.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Convenience alias for the stream a [`CrawlAdapter`] returns.
pub type PageEntryStream =
    Pin<Box<dyn Stream<Item = Result<PageEntry, FetchError>> + Send + 'static>>;

/// Implementing this trait wires a crawl protocol family into the orchestrator.
///
/// Unlike [`Adapter`], a `CrawlAdapter` returns a **stream** of [`PageEntry`]
/// values — one yield per page discovered/fetched. The stream is driven lazily
/// by the consumer; pages flow through, are read, and are dropped without ever
/// being accumulated. Cancellation propagates through dropping the stream or
/// via the [`CancelToken`] the adapter received at construction.
///
/// Two implementations ship with gottem:
///
/// - `HttpJsonlStreamManyAdapter` — Spider `/crawl`, JSONL over HTTP.
/// - `SpiderLocalCrawlAdapter` — local BFS reusing the scrape orchestrator
///   for per-URL fetching, with spider's `Website` doing tracking and
///   `Page::links` doing extraction.
#[async_trait]
pub trait CrawlAdapter: Send + Sync + 'static {
    fn kind(&self) -> AdapterKind;

    /// Begin the crawl. The returned stream is pinned and `Send` so it can
    /// be parked across tasks. Errors yielded as `Err` items don't terminate
    /// the stream — the crawl continues past per-page failures.
    async fn execute(
        &self,
        route: &Route,
        req: &CrawlRequest,
        cancel: &CancelToken,
    ) -> Result<PageEntryStream, FetchError>;
}

/// Registry mapping each crawl-capable [`AdapterKind`] to its implementation.
/// Mirrors [`AdapterRegistry`]; sibling registry rather than a single typed
/// map because the adapter trait shapes differ (one Response vs. a Stream).
#[derive(Default)]
pub struct CrawlAdapterRegistry {
    by_kind: HashMap<AdapterKind, Arc<dyn CrawlAdapter>>,
}

impl CrawlAdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, adapter: Arc<dyn CrawlAdapter>) {
        let k = adapter.kind();
        self.by_kind.insert(k, adapter);
    }

    pub fn get(&self, kind: &AdapterKind) -> Option<Arc<dyn CrawlAdapter>> {
        self.by_kind.get(kind).cloned()
    }

    pub fn len(&self) -> usize {
        self.by_kind.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_kind.is_empty()
    }
}

impl fmt::Debug for CrawlAdapterRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrawlAdapterRegistry")
            .field("kinds", &self.by_kind.keys().collect::<Vec<_>>())
            .finish()
    }
}
