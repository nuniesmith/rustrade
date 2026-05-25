//! The [`TradingService`] trait — every supervised unit must implement this.
//!
//! This is a thin rename of `janus-core`'s `JanusService` trait. The design
//! and rationale are preserved verbatim; see that crate's service.rs doc
//! comments for the full rationale.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Restart policy for a [`TradingService`] under supervisor control.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RestartPolicy {
    /// Always restart on exit, regardless of success/failure.
    Always,
    /// Only restart if `run()` returned `Err`.
    #[default]
    OnFailure,
    /// Never restart. One-shot.
    Never,
}

impl std::fmt::Display for RestartPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "always"),
            Self::OnFailure => write!(f, "on_failure"),
            Self::Never => write!(f, "never"),
        }
    }
}

/// A long-running task managed by the [`crate::Supervisor`].
///
/// # Cancellation contract
///
/// Implementations **must** select on `cancel.cancelled()` in their main loop.
/// A service that doesn't respond to cancellation will hang the whole
/// shutdown process until the supervisor's shutdown timeout fires.
///
/// ```ignore
/// async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
///     loop {
///         tokio::select! {
///             _ = cancel.cancelled() => break,
///             result = self.do_work() => result?,
///         }
///     }
///     Ok(())
/// }
/// ```
///
/// # Interior mutability
///
/// `run` takes `&self`, so services wrapped in `Arc` work naturally. Mutable
/// state (counters, connection handles, etc.) should use atomics, `Mutex`,
/// or `RwLock`. This is required anyway by the `Send + Sync + 'static` bound.
#[async_trait]
pub trait TradingService: Send + Sync + 'static {
    /// Unique service name for logs, metrics, and supervisor identification.
    fn name(&self) -> &str;

    /// When should the supervisor restart this service on exit?
    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    /// Main execution loop. Must honour the cancellation token.
    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct DummyService {
        name: String,
        run_count: AtomicU32,
    }

    impl DummyService {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                run_count: AtomicU32::new(0),
            }
        }

        fn run_count(&self) -> u32 {
            self.run_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl TradingService for DummyService {
        fn name(&self) -> &str {
            &self.name
        }

        async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
            self.run_count.fetch_add(1, Ordering::SeqCst);
            cancel.cancelled().await;
            Ok(())
        }
    }

    #[test]
    fn test_default_restart_policy() {
        let svc = DummyService::new("test");
        assert_eq!(svc.restart_policy(), RestartPolicy::OnFailure);
    }

    #[test]
    fn test_restart_policy_display() {
        assert_eq!(RestartPolicy::Always.to_string(), "always");
        assert_eq!(RestartPolicy::OnFailure.to_string(), "on_failure");
        assert_eq!(RestartPolicy::Never.to_string(), "never");
    }

    #[tokio::test]
    async fn test_dummy_service_runs_and_cancels() {
        let svc = DummyService::new("test-svc");
        let token = CancellationToken::new();

        let token_clone = token.clone();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            token_clone.cancel();
        });

        let result = svc.run(token).await;
        assert!(result.is_ok());
        assert_eq!(svc.run_count(), 1);
        handle.await.unwrap();
    }

    #[test]
    fn test_service_name() {
        let svc = DummyService::new("market-data");
        assert_eq!(svc.name(), "market-data");
    }
}
