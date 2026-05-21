use std::sync::Arc;

use url::Url;

use crate::{
    capabilities::Capabilities, catalog::RouteCatalog, error::FetchError, route::Route, tier::Tier,
};

/// Outcome of a completed (or failed) attempt, handed to a [`RetryStrategy`] so it can
/// decide what to do next. All borrowing fields are reborrowed from values that live for
/// the duration of the strategy call — strategy impls must not store these references.
pub struct AttemptOutcome<'a> {
    pub attempt: u32,
    pub last_route: Option<&'a Route>,
    pub last_status: Option<u16>,
    pub last_html_length: usize,
    pub last_error: Option<&'a FetchError>,
    pub url: &'a Url,
    pub anti_bot: Option<&'a spider::page::AntiBotTech>,
    pub waf_check: bool,
}

/// gottem's [`RetryDirective`] additively wraps [`spider::retry_strategy::RetryDirective`]
/// with an optional `route` override. When `route` is `Some`, the next attempt is dispatched
/// through that route's adapter. When `None`, behavior is identical to using spider directly.
#[derive(Debug, Default)]
pub struct RetryDirective {
    pub inner: spider::retry_strategy::RetryDirective,
    pub route: Option<Arc<Route>>,
}

impl RetryDirective {
    pub fn stop() -> Self {
        Self {
            inner: spider::retry_strategy::RetryDirective::stop(),
            route: None,
        }
    }

    pub fn continue_default() -> Self {
        Self::default()
    }

    pub fn with_route(mut self, route: Arc<Route>) -> Self {
        self.route = Some(route);
        self
    }

    pub fn should_retry(&self) -> bool {
        self.inner.should_retry
    }
}

/// gottem's retry strategy. Trait shape mirrors [`spider::retry_strategy::RetryStrategy`]
/// but takes a [`AttemptOutcome`] from gottem (which can carry a route reference) and returns
/// a gottem [`RetryDirective`].
pub trait RetryStrategy: Send + Sync + 'static {
    fn max_retries(&self) -> u32;

    /// Initial route before any attempt has been made. Default is `None` — the orchestrator
    /// then expects the caller to supply a starting route. [`LadderStrategy`] returns the
    /// cheapest route at `tier_min` from its catalog.
    fn initial(&self) -> Option<Arc<Route>> {
        None
    }

    /// Called after a failed attempt to decide what to do next. Mirrors
    /// [`spider::retry_strategy::RetryStrategy::on_retry`].
    fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective;
}

/// Bridge from any [`spider::retry_strategy::RetryStrategy`] into a gottem [`RetryStrategy`].
/// The `route` override is always `None`, so behavior is identical to using spider directly.
pub struct SpiderStrategyAdapter<S: spider::retry_strategy::RetryStrategy>(pub S);

impl<S: spider::retry_strategy::RetryStrategy> RetryStrategy for SpiderStrategyAdapter<S> {
    fn max_retries(&self) -> u32 {
        self.0.max_retries()
    }

    fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective {
        let no_tech = spider::page::AntiBotTech::default();
        let err_str = outcome.last_error.map(|e| e.to_string());
        let url_str = outcome.url.as_str();
        let profile = outcome.last_route.map(|r| r.id.as_ref());
        let spider_out = spider::retry_strategy::AttemptOutcome {
            attempt: outcome.attempt,
            status_code: http::StatusCode::from_u16(outcome.last_status.unwrap_or(500))
                .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR),
            should_retry: true,
            content_truncated: false,
            waf_check: outcome.waf_check,
            anti_bot_tech: outcome.anti_bot.unwrap_or(&no_tech),
            proxy_configured: false,
            url: url_str,
            profile_key: profile,
            html_length: outcome.last_html_length,
            bytes_transferred: None,
            error_status: err_str.as_deref(),
            final_redirect_destination: None,
        };
        let inner = spider::retry_strategy::RetryStrategy::on_retry(&self.0, &spider_out);
        RetryDirective { inner, route: None }
    }
}

/// Built-in cheap-to-expensive ladder strategy. On each retry, walks one tier higher in
/// the catalog and picks the cheapest route at that tier whose [`Capabilities`] satisfy
/// `required_caps`.
pub struct LadderStrategy {
    catalog: Arc<RouteCatalog>,
    tier_min: Tier,
    tier_max: Tier,
    required_caps: Capabilities,
    max_retries: u32,
}

impl LadderStrategy {
    pub fn new(
        catalog: Arc<RouteCatalog>,
        tier_min: Tier,
        tier_max: Tier,
        required_caps: Capabilities,
        max_retries: u32,
    ) -> Self {
        Self {
            catalog,
            tier_min,
            tier_max,
            required_caps,
            max_retries,
        }
    }

    fn route_at_or_above(&self, from: Tier, exclude: Option<&str>) -> Option<Arc<Route>> {
        let max_u = u8::from(self.tier_max);
        for t in u8::from(from)..=max_u {
            let tier = match Tier::from_u8(t) {
                Ok(t) => t,
                Err(_) => break,
            };
            for route in self.catalog.at_tier(tier) {
                let not_excluded = exclude.map_or(true, |id| id != route.id.as_ref());
                if not_excluded && self.required_caps.satisfied_by(&route.caps) {
                    return Some(route.clone());
                }
            }
        }
        None
    }
}

impl RetryStrategy for LadderStrategy {
    fn max_retries(&self) -> u32 {
        self.max_retries
    }

    fn initial(&self) -> Option<Arc<Route>> {
        self.route_at_or_above(self.tier_min, None)
    }

    fn on_retry(&self, outcome: &AttemptOutcome<'_>) -> RetryDirective {
        let bump_from = match outcome.last_route.map(|r| r.tier) {
            Some(t) => t.next().unwrap_or(t),
            None => self.tier_min,
        };
        let exclude = outcome.last_route.map(|r| r.id.as_ref());
        match self.route_at_or_above(bump_from, exclude) {
            Some(route) => RetryDirective::default().with_route(route),
            None => RetryDirective::stop(),
        }
    }
}
