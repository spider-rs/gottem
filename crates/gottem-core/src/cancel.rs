use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Lightweight cancellation token: cancellable from any task, observable via [`Self::cancelled`].
///
/// Designed for race winners to abort losers and for outer callers to abort the whole
/// orchestrator. Backed by a single [`AtomicBool`] and a [`Notify`]; cloning is cheap and
/// shares cancellation state.
#[derive(Clone, Debug, Default)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Idempotent; notifies all awaiters exactly once.
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Future that resolves once cancellation is requested. Safe to await even if already
    /// cancelled (returns immediately). Safe to await from multiple tasks concurrently.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.inner.notify.notified();
        // Re-check after registering the waiter — closes the cancel-before-notify race.
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancel_propagates() {
        let t = CancelToken::new();
        let t2 = t.clone();
        let h = tokio::spawn(async move {
            t2.cancelled().await;
        });
        assert!(!t.is_cancelled());
        t.cancel();
        h.await.unwrap();
        assert!(t.is_cancelled());
    }

    #[tokio::test]
    async fn cancel_before_await_returns_immediately() {
        let t = CancelToken::new();
        t.cancel();
        t.cancelled().await;
    }
}
