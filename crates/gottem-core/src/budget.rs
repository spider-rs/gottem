use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::FetchError;

/// Atomic cost accumulator with a hard ceiling. Costs are in **milli-cents**
/// (10 = $0.001, 1000 = $0.10). Lock-free; safe to share across tasks via [`std::sync::Arc`].
#[derive(Debug)]
pub struct Budget {
    spent: AtomicU64,
    limit: u64,
}

impl Budget {
    pub fn new(limit_milli_cents: u64) -> Self {
        Self {
            spent: AtomicU64::new(0),
            limit: limit_milli_cents,
        }
    }

    pub fn unlimited() -> Self {
        Self::new(u64::MAX)
    }

    /// Atomically reserve `cost` milli-cents. Returns `BudgetExceeded` if the new total
    /// would exceed the limit. Uses a CAS loop so concurrent spends never over-spend.
    pub fn try_spend(&self, cost: u64) -> Result<(), FetchError> {
        let mut current = self.spent.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(cost);
            if next > self.limit {
                return Err(FetchError::BudgetExceeded {
                    spent: current,
                    limit: self.limit,
                });
            }
            match self.spent.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    pub fn spent(&self) -> u64 {
        self.spent.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.spent())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn rejects_overspend() {
        let b = Budget::new(100);
        assert!(b.try_spend(40).is_ok());
        assert!(b.try_spend(40).is_ok());
        assert!(b.try_spend(40).is_err());
        assert_eq!(b.spent(), 80);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_spends_dont_overspend() {
        let b = Arc::new(Budget::new(1000));
        let mut tasks = Vec::new();
        for _ in 0..100 {
            let b = b.clone();
            tasks.push(tokio::spawn(async move {
                let _ = b.try_spend(20);
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        assert!(b.spent() <= 1000);
    }
}
