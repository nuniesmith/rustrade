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
