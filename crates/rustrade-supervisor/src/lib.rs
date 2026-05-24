//! # rustrade-supervisor
//!
//! Structured service lifecycle management for async trading bots.
//!
//! Every long-running task in a rustrade bot — market feeds, heartbeats,
//! candle pollers, the brain itself — implements [`TradingService`] and is
//! spawned through a [`Supervisor`]. The supervisor:
//!
//! - Tracks running tasks without accumulating their results (uses
//!   `TaskTracker` rather than `JoinSet`).
//! - Propagates graceful shutdown via a root `CancellationToken` that
//!   branches to each service.
//! - Restarts failed services with exponential backoff and per-service
//!   circuit breakers.
//! - Surfaces lifecycle state (starting / running / restarting / terminated)
//!   and metrics (restarts, active services, uptime) for observability.
//!
//! # Why this design
//!
//! The naive `tokio::spawn` approach leaks tasks that silently die, makes
//! graceful shutdown a nightmare, and has no concept of "this service failed
//! 20 times in a row — stop retrying." This module replaces that pattern
//! with a small supervision tree that makes failure modes explicit.
//!
//! # Port status
//!
//! > **This is a skeleton.** The real implementation should be lifted almost
//! > verbatim from `janus-main/lib/janus-core/src/supervisor/`:
//! >
//! > - `service.rs`      → this crate's `service.rs`   (rename trait)
//! > - `backoff.rs`      → this crate's `backoff.rs`   (no changes)
//! > - `lifecycle.rs`    → this crate's `lifecycle.rs` (no changes)
//! > - `mod.rs`          → this crate's `supervisor.rs` (rename struct)
//! >
//! > Changes to make during the port:
//! > 1. Rename `JanusService` → [`TradingService`].
//! > 2. Rename `JanusSupervisor` → [`Supervisor`].
//! > 3. Gate the Prometheus integration behind the `prometheus` feature
//! >    flag. The atomic counters in `SupervisorMetrics` stay as-is; the
//! >    `crate::metrics::metrics()` calls move behind `#[cfg(feature = "prometheus")]`.
//! > 4. Drop the `janus-core::metrics` module dependency entirely — the
//! >    host binary registers its own Prometheus collectors if it wants them.
//! > 5. Keep all the chaos tests. They're the best proof the supervisor works.

pub mod backoff;
pub mod lifecycle;
pub mod service;
pub mod supervisor;

pub use backoff::{BackoffAction, BackoffConfig, BackoffState};
pub use lifecycle::{ServiceLifecycle, ServicePhase, TerminationReason};
pub use service::{RestartPolicy, TradingService};
pub use supervisor::{Supervisor, SupervisorConfig, SupervisorMetrics};
