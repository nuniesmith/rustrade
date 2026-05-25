#![warn(missing_docs)]

//! # rustrade
//!
//! Open-source trading bot framework — the facade crate downstream
//! services depend on. Re-exports the core types, the supervisor, and the
//! risk primitives, and adds the [`Bot`] builder that wires them into a
//! single supervised runtime.
//!
//! # Quickstart
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use rustrade::{Bot, BotConfig};
//!
//! let bot = Bot::new(
//!     BotConfig::builder().name("my-bot").symbol("BTCUSDT").build()?,
//!     Arc::new(MyExchangeAdapter::new()),
//!     vec![Arc::new(MyBrain::new())],
//! )?;
//! let handle = bot.handle();
//! // host can shutdown via handle.shutdown() at any time
//! bot.run_until_shutdown().await
//! ```
//!
//! # What this crate adds on top of the sub-crates
//!
//! - [`Bot`] / [`BotConfig`] / [`BotConfigBuilder`] — the embedding entry
//!   point. Wraps a [`Supervisor`] and the framework-side services.
//! - [`BotHandle`] / [`BotHealth`] — cheap cloneable handle for host
//!   services to query state and trigger shutdown without holding the
//!   `Bot` itself.
//! - [`Bot::with_market_source`] / [`Bot::with_fill_source`] /
//!   [`Bot::with_external_cancel`] — optional wirings for adapters and
//!   host-driven shutdown.
//! - [`logging::init_tracing`] — opinionated default tracing subscriber.
//!   Skippable; downstream services with their own subscriber don't use it.
//!
//! # Tokio runtime requirements
//!
//! `Bot::run_until_shutdown` must be awaited inside a **multi-thread
//! tokio runtime context**. Internally the supervisor calls
//! `tokio::spawn` for every service, and the framework expects the host's
//! shutdown to be cooperative (services watch a `CancellationToken`).
//! The simplest valid host is:
//!
//! ```rust,ignore
//! #[tokio::main(flavor = "multi_thread")]
//! async fn main() -> anyhow::Result<()> {
//!     // ... build bot ...
//!     bot.run_until_shutdown().await
//! }
//! ```
//!
//! Embedding into an existing runtime is supported — just spawn the bot
//! as a task and hold a [`BotHandle`] for control. See
//! `examples/embed-in-service/`.
//!
//! # Resource expectations
//!
//! - **Memory per active symbol:** a few hundred bytes for the position
//!   cache entry plus the per-symbol `SymbolRisk` (a `SessionPnl` and a
//!   `CircuitBreaker` whose ring buffer is bounded by `loss_limit`).
//! - **Channel buffers:** `market_bus_capacity` + `signal_bus_capacity`
//!   slots per bus, each holding a clone of `MarketDataEvent` / `Signal`.
//!   Backed by `tokio::sync::broadcast`, which has **drop-oldest**
//!   semantics: slow subscribers see `RecvError::Lagged(n)` and miss
//!   the oldest dropped events.
//! - **Expected shutdown time:** ≤ `shutdown_timeout` (default 30 s).
//!   Well-behaved services drain in milliseconds; the timeout is the
//!   worst-case ceiling, not the typical case.
//! - **Restart-after-crash latency:** bounded by the supervisor's
//!   `rustrade_supervisor::BackoffConfig`. Defaults: 100 ms base delay,
//!   60 s cap, 10 retries within a 10-minute window before the circuit
//!   breaker trips and the service terminates.
//!
//! # Module status
//!
//! | Module       | Surface                                                    |
//! | ------------ | ---------------------------------------------------------- |
//! | `bot`        | `Bot`, `BotConfig`, `BotConfigBuilder`, `RiskConfig`       |
//! | `handle`     | `BotHandle`, `BotHealth`, `record_trade_outcome`           |
//! | `execution`  | `ExecutionService` with full risk-gate pipeline            |
//! | `services`   | `MarketFeedService`, `FillRoutingService`                  |
//! | `logging`    | `init_tracing`                                             |

pub mod bot;
pub mod execution;
pub mod handle;
pub mod logging;
pub(crate) mod risk_state;
pub mod services;

pub use bot::{Bot, BotConfig, BotConfigBuilder, RiskConfig};
pub use handle::{BotHandle, BotHealth, BrainHealthSnapshot};
pub use services::{FillRoutingService, MarketFeedService};

// Re-exports from sub-crates so downstream depends on `rustrade` only.
pub use rustrade_core::{
    Brain, BrainHealth, Candle, Capability, Decision, Error, EventSource, Exchange, ExchangeClient,
    Fill, FillSource, MarketDataBus, MarketDataEvent, MarketSource, Order, OrderKind, OrderStatus,
    Position, Price, Result, Side, Signal, SignalBus, SignalType, SizeHint, StopAttachment,
    StopKind, Symbol, Tick, Volume,
};
pub use rustrade_risk::{
    CircuitBreaker, CircuitBreakerConfig, Clock, ManualClock, PositionSizer, SessionPnl,
    SessionPnlConfig, SizingConfig, SystemClock,
};
pub use rustrade_supervisor::{
    BackoffConfig, RestartPolicy, ServiceLifecycleSnapshot, ServicePhase, SpawnOptions, Supervisor,
    SupervisorConfig, TerminationReason, TradingService,
};
