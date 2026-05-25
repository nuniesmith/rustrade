#![warn(missing_docs)]

//! # rustrade-supervisor
//!
//! Structured service lifecycle management for async trading bots.
//!
//! Every long-running task in a rustrade bot — market feeds, candle
//! pollers, heartbeats, brains — implements [`TradingService`] and is
//! spawned through a [`Supervisor`] that:
//!
//! - Tracks running tasks without accumulating their results (uses
//!   `TaskTracker` rather than `JoinSet`).
//! - Propagates graceful shutdown via a root `CancellationToken` that
//!   branches to each service.
//! - Restarts failed services with exponential backoff plus full jitter
//!   and per-service circuit breakers.
//! - Surfaces lifecycle state (Starting / Running / BackingOff / Stopping
//!   / Terminated) and metrics (restarts, active services, uptime) for
//!   observability.
//!
//! # Quickstart
//!
//! ```rust,ignore
//! use rustrade_supervisor::{Supervisor, SupervisorConfig, TradingService};
//!
//! let supervisor = Supervisor::new(SupervisorConfig::default());
//! supervisor.spawn_service(Box::new(my_market_feed));
//! supervisor.spawn_service(Box::new(my_risk_engine));
//!
//! // Blocks until Ctrl-C / SIGTERM, then orchestrates graceful shutdown.
//! supervisor.run_until_shutdown().await?;
//! ```
//!
//! # Observability
//!
//! Atomic counters in [`SupervisorMetrics`] are the in-process source of
//! truth and are always available. Enable the `prometheus` feature to
//! mirror them into a crate-local `prometheus::Registry` accessible via
//! the `prometheus::registry` accessor in this crate's `prometheus`
//! submodule. The host service serves `.gather()` from that registry in
//! its `/metrics` handler — rustrade does not own an HTTP layer of its
//! own.

pub mod backoff;
pub mod lifecycle;
#[cfg(feature = "prometheus")]
pub mod prometheus;
pub mod service;
pub mod supervisor;

pub use backoff::{BackoffAction, BackoffConfig, BackoffState};
pub use lifecycle::{
    ServiceLifecycle, ServiceLifecycleSnapshot, ServicePhase, TerminationReason, TransitionError,
};
pub use service::{RestartPolicy, TradingService};
pub use supervisor::{
    MetricsSnapshot, SpawnOptions, Supervisor, SupervisorConfig, SupervisorMetrics,
};
