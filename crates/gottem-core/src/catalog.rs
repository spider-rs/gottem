use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    error::FetchError,
    route::{Route, RouteId},
    tier::Tier,
};

/// Immutable, frozen registry of routes. Lookups are lock-free hash reads.
#[derive(Debug)]
pub struct RouteCatalog {
    by_id: HashMap<RouteId, Arc<Route>>,
    by_tier: HashMap<Tier, Vec<Arc<Route>>>,
}

impl RouteCatalog {
    pub fn builder() -> RouteCatalogBuilder {
        RouteCatalogBuilder::default()
    }

    pub fn get(&self, id: &str) -> Option<Arc<Route>> {
        self.by_id.get(id).cloned()
    }

    pub fn at_tier(&self, tier: Tier) -> &[Arc<Route>] {
        self.by_tier.get(&tier).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn all(&self) -> impl Iterator<Item = &Arc<Route>> + '_ {
        self.by_id.values()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}

#[derive(Default)]
pub struct RouteCatalogBuilder {
    routes: Vec<Arc<Route>>,
}

impl RouteCatalogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    // Intentionally named `add` for readable builder chains; the name shadow with
    // `std::ops::Add::add` is harmless because this method takes a `Route`, not Self.
    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, route: Route) -> Self {
        self.routes.push(Arc::new(route));
        self
    }

    pub fn add_arc(mut self, route: Arc<Route>) -> Self {
        self.routes.push(route);
        self
    }

    /// Parse a TOML document with one or more `[[route]]` tables.
    pub fn add_toml(mut self, toml_str: &str) -> Result<Self, FetchError> {
        #[derive(serde::Deserialize)]
        struct Doc {
            #[serde(default)]
            route: Vec<Route>,
        }
        let doc: Doc =
            toml::from_str(toml_str).map_err(|e| FetchError::Config(format!("toml parse: {e}")))?;
        for r in doc.route {
            self.routes.push(Arc::new(r));
        }
        Ok(self)
    }

    pub fn build(self) -> RouteCatalog {
        let mut by_id = HashMap::new();
        let mut by_tier: HashMap<Tier, Vec<Arc<Route>>> = HashMap::new();
        for r in self.routes {
            by_tier.entry(r.tier).or_default().push(r.clone());
            by_id.insert(r.id.clone(), r);
        }
        // Within a tier: lowest cost first, then by priority (Spider's are 0 — wins
        // ties), then by id for a fully deterministic ordering.
        for v in by_tier.values_mut() {
            v.sort_by(|a, b| {
                a.cost
                    .cmp(&b.cost)
                    .then_with(|| a.priority.cmp(&b.priority))
                    .then_with(|| a.id.cmp(&b.id))
            });
        }
        RouteCatalog { by_id, by_tier }
    }
}
