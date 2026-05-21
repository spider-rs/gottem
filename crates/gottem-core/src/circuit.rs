use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CLOSED: u8 = 0;
const OPEN: u8 = 1;
const HALF_OPEN: u8 = 2;

/// Atomics-only circuit breaker per route. After `threshold` consecutive failures the
/// circuit opens; after `cooldown` it transitions to half-open and admits one trial.
/// Trial success closes; trial failure re-opens with a fresh cooldown.
#[derive(Debug)]
pub struct CircuitBreaker {
    state: AtomicU8,
    consecutive_failures: AtomicU64,
    opened_at_ms: AtomicU64,
    threshold: u64,
    cooldown_ms: u64,
}

impl CircuitBreaker {
    pub fn new(threshold: u64, cooldown: Duration) -> Self {
        Self {
            state: AtomicU8::new(CLOSED),
            consecutive_failures: AtomicU64::new(0),
            opened_at_ms: AtomicU64::new(0),
            threshold,
            cooldown_ms: cooldown.as_millis() as u64,
        }
    }

    /// Whether a request should be admitted right now.
    pub fn allow(&self) -> bool {
        match self.state.load(Ordering::Acquire) {
            CLOSED => true,
            OPEN => {
                let opened = self.opened_at_ms.load(Ordering::Relaxed);
                if unix_ms_now().saturating_sub(opened) >= self.cooldown_ms {
                    // Transition Open -> HalfOpen exactly once (whichever caller wins the CAS).
                    let _ = self.state.compare_exchange(
                        OPEN,
                        HALF_OPEN,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    true
                } else {
                    false
                }
            }
            HALF_OPEN => true,
            _ => true,
        }
    }

    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.state.store(CLOSED, Ordering::Release);
    }

    pub fn record_failure(&self) {
        let n = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if n >= self.threshold || self.state.load(Ordering::Acquire) == HALF_OPEN {
            self.opened_at_ms.store(unix_ms_now(), Ordering::Relaxed);
            self.state.store(OPEN, Ordering::Release);
        }
    }

    pub fn state_name(&self) -> &'static str {
        match self.state.load(Ordering::Relaxed) {
            CLOSED => "closed",
            OPEN => "open",
            HALF_OPEN => "half_open",
            _ => "?",
        }
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_threshold() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(10));
        assert!(cb.allow());
        cb.record_failure();
        cb.record_failure();
        assert!(cb.allow());
        cb.record_failure();
        assert!(!cb.allow());
    }

    #[test]
    fn closes_on_success_after_half_open() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(5));
        cb.record_failure();
        assert!(!cb.allow());
        std::thread::sleep(Duration::from_millis(10));
        // Should transition to half-open and admit.
        assert!(cb.allow());
        cb.record_success();
        assert_eq!(cb.state_name(), "closed");
    }
}
