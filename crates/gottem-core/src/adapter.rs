use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::{
    cancel::CancelToken,
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
        Self { attempt, started: Instant::now() }
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
    pub fn new() -> Self { Self::default() }

    pub fn register(&mut self, adapter: Arc<dyn Adapter>) {
        let k = adapter.kind();
        self.by_kind.insert(k, adapter);
    }

    pub fn get(&self, kind: &AdapterKind) -> Option<Arc<dyn Adapter>> {
        self.by_kind.get(kind).cloned()
    }

    pub fn len(&self) -> usize { self.by_kind.len() }
    pub fn is_empty(&self) -> bool { self.by_kind.is_empty() }
}

impl fmt::Debug for AdapterRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterRegistry")
            .field("kinds", &self.by_kind.keys().collect::<Vec<_>>())
            .finish()
    }
}
