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
//! - [`logging::init_tracing`] — opinionated default tracing subscriber.
//!   Skippable; downstream services with their own subscriber don't use it.
//!
//! # Module status
//!
//! | Module      | Phase 2a                                              |
//! | ----------- | ----------------------------------------------------- |
//! | `bot`       | `Bot`, `BotConfig`, `BotConfigBuilder` — complete     |
//! | `handle`    | `BotHandle`, `BotHealth` — complete                   |
//! | `execution` | `ExecutionService` routes events → brains             |
//! | `logging`   | `init_tracing` complete                               |
//!
//! Risk gating, fill routing, candle polling, and the pluggable
//! `MetricsSink` are Phase 2b/2c — see the workspace `TODO.md`.

pub mod bot;
pub mod execution;
pub mod handle;
pub mod logging;

pub use bot::{Bot, BotConfig, BotConfigBuilder};
pub use handle::{BotHandle, BotHealth};

// Re-exports from sub-crates so downstream depends on `rustrade` only.
pub use rustrade_core::{
    Brain, BrainHealth, Candle, Capability, Decision, Error, Exchange, ExchangeClient, Fill,
    MarketDataBus, MarketDataEvent, MarketSource, Order, OrderKind, OrderStatus, Position, Price,
    Result, Side, Signal, SignalBus, SignalType, SizeHint, StopAttachment, StopKind, Symbol, Tick,
    Volume,
};
pub use rustrade_risk::{
    CircuitBreaker, CircuitBreakerConfig, Clock, ManualClock, PositionSizer, SessionPnl,
    SessionPnlConfig, SizingConfig, SystemClock,
};
pub use rustrade_supervisor::{
    BackoffConfig, RestartPolicy, ServiceLifecycleSnapshot, ServicePhase, SpawnOptions, Supervisor,
    SupervisorConfig, TerminationReason, TradingService,
};
