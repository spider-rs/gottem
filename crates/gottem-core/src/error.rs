use std::time::Duration;
use thiserror::Error;

use crate::{capabilities::Capabilities, route::RouteId, tier::Tier};

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("network error: {0}")]
    Network(String),

    #[error("timeout after {0:?}")]
    Timeout(Duration),

    #[error("upstream status {0}")]
    Status(u16),

    #[error("validation failed: {0}")]
    Validation(String),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("auth missing or invalid: {0}")]
    Auth(String),

    #[error("cancelled")]
    Cancelled,

    #[error("budget exceeded: spent {spent} milli-cents, limit {limit}")]
    BudgetExceeded { spent: u64, limit: u64 },

    #[error("no route available for tier {tier:?} caps {caps:?}")]
    NoRoute { tier: Tier, caps: Capabilities },

    #[error("circuit open for route {0}")]
    CircuitOpen(RouteId),

    #[error("all routes exhausted")]
    Exhausted,

    #[error("unknown adapter: {0}")]
    UnknownAdapter(String),

    #[error("config error: {0}")]
    Config(String),
}

impl FetchError {
    /// Whether this error type warrants another attempt (possibly on a different route).
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Network(_)
            | Self::Timeout(_)
            | Self::Validation(_)
            | Self::Parse(_)
            | Self::CircuitOpen(_) => true,
            Self::Status(s) => *s == 408 || *s == 425 || *s == 429 || (*s >= 500 && *s < 600),
            Self::Auth(_)
            | Self::Cancelled
            | Self::BudgetExceeded { .. }
            | Self::NoRoute { .. }
            | Self::Exhausted
            | Self::UnknownAdapter(_)
            | Self::Config(_) => false,
        }
    }

    /// Whether this error should bump to a HIGHER tier rather than retry on the same one.
    pub fn should_escalate(&self) -> bool {
        matches!(
            self,
            Self::Status(_) | Self::Validation(_) | Self::CircuitOpen(_)
        )
    }
}
