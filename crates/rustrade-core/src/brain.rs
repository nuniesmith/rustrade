//! The [`Brain`] trait — rustrade's central abstraction.
//!
//! A `Brain` is the strategic layer of a trading bot: it consumes market
//! events and outputs [`Decision`]s. Everything else in rustrade (supervisor,
//! exchange client, risk layer, execution) is plumbing around this one trait.
//!
//! # Why a single trait?
//!
//! Trading bots come in many flavours — indicator-based, ML-based,
//! neuromorphic, hybrid. The common contract is: "given market state, tell
//! me what to do." Encoding that contract as one narrow trait means:
//!
//! - A rule-based `SarBrain` and a 10-million-parameter `NeuromorphicBrain`
//!   are interchangeable to the rest of the framework.
//! - Backtesting and live trading share the same brain implementation.
//! - You can run multiple brains in parallel (e.g. A/B or ensemble) by
//!   composing them in an outer `Brain` impl.
//!
//! # What `Brain` does NOT do
//!
//! A `Brain` does **not**:
//! - Place orders directly — it returns a [`Decision`]; the execution
//!   layer decides whether to act.
//! - Manage positions — `on_position_change` is informational only.
//! - Do risk sizing — it may suggest size via [`SizeHint`], but the risk
//!   layer has the final say.
//! - Own the indicator state externally — that's a brain-internal concern.
//!   Two different brains can maintain entirely different indicator stacks.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::market::MarketDataEvent;
use crate::signal::SignalType;
use crate::types::{Fill, Position, Price, Volume};

/// How large the brain wants the next order to be. The risk layer can honour,
/// scale down, or reject this hint.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum SizeHint {
    /// Use a fraction of available margin (0.0..=1.0).
    MarginFraction(f64),
    /// Target a specific notional in quote currency.
    NotionalUsd(f64),
    /// Target a specific number of contracts or base units.
    Quantity(Volume),
    /// Defer to the risk layer's default sizing entirely.
    #[default]
    Default,
}

/// A brain's decision on a single market event.
///
/// `signal` is always present; the other fields are hints and metadata that
/// the execution and risk layers may or may not use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub signal: SignalType,
    /// Confidence in [0.0, 1.0].
    pub confidence: f64,
    /// Optional suggested size.
    #[serde(default)]
    pub size_hint: SizeHint,
    /// Optional suggested stop-loss price.
    pub stop_price: Option<Price>,
    /// Optional suggested take-profit price.
    pub take_profit_price: Option<Price>,
    /// Free-form brain metadata, used for logging and post-trade analysis.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl Decision {
    /// Convenience: "no action".
    pub fn hold() -> Self {
        Self {
            signal: SignalType::Hold,
            confidence: 0.0,
            size_hint: SizeHint::Default,
            stop_price: None,
            take_profit_price: None,
            metadata: serde_json::Value::Null,
        }
    }

    pub fn buy(confidence: f64) -> Self {
        Self {
            signal: SignalType::Buy,
            confidence,
            ..Self::hold()
        }
    }

    pub fn sell(confidence: f64) -> Self {
        Self {
            signal: SignalType::Sell,
            confidence,
            ..Self::hold()
        }
    }

    pub fn close() -> Self {
        Self {
            signal: SignalType::Close,
            confidence: 1.0,
            ..Self::hold()
        }
    }

    pub fn with_stop(mut self, price: Price) -> Self {
        self.stop_price = Some(price);
        self
    }

    pub fn with_take_profit(mut self, price: Price) -> Self {
        self.take_profit_price = Some(price);
        self
    }

    pub fn with_size_hint(mut self, hint: SizeHint) -> Self {
        self.size_hint = hint;
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Reported health of a [`Brain`]. Surfaces to the supervisor's health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrainHealth {
    /// Is the brain healthy enough to continue trading?
    pub healthy: bool,
    /// Number of events processed since startup.
    pub events_processed: u64,
    /// Number of decisions emitted that were not `Hold`.
    pub non_hold_decisions: u64,
    /// Free-form status fields for the `/health` JSON response.
    #[serde(default)]
    pub details: serde_json::Value,
}

impl BrainHealth {
    pub fn ok() -> Self {
        Self {
            healthy: true,
            ..Default::default()
        }
    }

    pub fn unhealthy(reason: impl Into<String>) -> Self {
        Self {
            healthy: false,
            details: serde_json::json!({ "reason": reason.into() }),
            ..Default::default()
        }
    }
}

/// The strategic layer of a trading bot.
///
/// Implementors receive market events and the current position state, and
/// return a decision on each event. See the module-level docs for the
/// design rationale.
///
/// # Threading & mutability
///
/// Methods take `&self` so implementors can be shared across tasks via `Arc`.
/// Use interior mutability (`Mutex`, `RwLock`, atomics) for any state that
/// needs to be updated across calls. This mirrors the pattern in
/// [`rustrade-supervisor::TradingService`].
///
/// # Object safety
///
/// `Brain` is object-safe. You can store brains as `Box<dyn Brain>` or
/// `Arc<dyn Brain>` and swap between implementations at runtime.
#[async_trait]
pub trait Brain: Send + Sync + 'static {
    /// Human-readable identifier used in logs and metrics.
    fn name(&self) -> &str;

    /// Core decision point — called on every market event for any symbol
    /// this brain cares about.
    ///
    /// `position` is the exchange-reported position for the event's symbol
    /// at the time this call is made. May be [`Position::FLAT`].
    ///
    /// Return [`Decision::hold`] for "do nothing" — this is always safe.
    async fn on_event(&self, event: &MarketDataEvent, position: &Position) -> Result<Decision>;

    /// Called after the exchange confirms a fill. Informational only —
    /// returning an error does not unwind the fill.
    ///
    /// Default implementation is a no-op.
    async fn on_fill(&self, _fill: &Fill) -> Result<()> {
        Ok(())
    }

    /// Called whenever the exchange reports a position change from any
    /// source (our fills, external actions, liquidations, funding).
    /// Informational only.
    ///
    /// Default implementation is a no-op.
    async fn on_position_change(&self, _symbol: &str, _position: &Position) -> Result<()> {
        Ok(())
    }

    /// Report current brain health for the supervisor's `/health` endpoint.
    ///
    /// Default implementation returns "healthy" — override to surface
    /// indicator warm-up state, model staleness, memory pressure, etc.
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}
