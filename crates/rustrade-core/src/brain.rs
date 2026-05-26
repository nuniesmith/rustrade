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
use crate::market::{MarketDataEvent, Symbol};
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
///
/// # Example
///
/// ```
/// use rustrade_core::{Decision, Price, SizeHint};
///
/// // The four constructor shapes the framework uses internally.
/// let _hold = Decision::hold();
/// let _close = Decision::close();
/// let _buy = Decision::buy(0.8);
/// let _sell = Decision::sell(0.6);
///
/// // Chain stop / take-profit / size hints / metadata.
/// let decision = Decision::buy(0.9)
///     .with_stop(Price(95.0))
///     .with_take_profit(Price(110.0))
///     .with_size_hint(SizeHint::MarginFraction(0.5))
///     .with_metadata(serde_json::json!({"reason": "ema-cross"}));
///
/// assert_eq!(decision.stop_price, Some(Price(95.0)));
/// assert_eq!(decision.take_profit_price, Some(Price(110.0)));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    /// What the brain decided to do (Buy / Sell / Hold / Close).
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

    /// Convenience: open or flip a long with the given confidence.
    pub fn buy(confidence: f64) -> Self {
        Self {
            signal: SignalType::Buy,
            confidence,
            ..Self::hold()
        }
    }

    /// Convenience: open or flip a short with the given confidence.
    pub fn sell(confidence: f64) -> Self {
        Self {
            signal: SignalType::Sell,
            confidence,
            ..Self::hold()
        }
    }

    /// Convenience: close the current position without reversing.
    pub fn close() -> Self {
        Self {
            signal: SignalType::Close,
            confidence: 1.0,
            ..Self::hold()
        }
    }

    /// Suggest a stop-loss price.
    pub fn with_stop(mut self, price: Price) -> Self {
        self.stop_price = Some(price);
        self
    }

    /// Suggest a take-profit price.
    pub fn with_take_profit(mut self, price: Price) -> Self {
        self.take_profit_price = Some(price);
        self
    }

    /// Override the default size hint.
    pub fn with_size_hint(mut self, hint: SizeHint) -> Self {
        self.size_hint = hint;
        self
    }

    /// Attach free-form metadata for logging / post-hoc analysis.
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
    /// A healthy default with zero counters.
    pub fn ok() -> Self {
        Self {
            healthy: true,
            ..Default::default()
        }
    }

    /// An unhealthy state with a single `reason` field in `details`.
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
/// `rustrade-supervisor::TradingService`.
///
/// # Object safety
///
/// `Brain` is object-safe. You can store brains as `Box<dyn Brain>` or
/// `Arc<dyn Brain>` and swap between implementations at runtime.
///
/// # Example
///
/// A minimal brain that goes long when the close is above a fixed
/// threshold and flat otherwise. Note the `Mutex<State>` pattern for
/// any cross-call state.
///
/// ```
/// use std::sync::Mutex;
/// use async_trait::async_trait;
/// use rustrade_core::{
///     Brain, BrainHealth, Decision, MarketDataEvent, Position, Result,
/// };
///
/// struct ThresholdBrain {
///     threshold: f64,
///     state: Mutex<usize>, // events seen
/// }
///
/// #[async_trait]
/// impl Brain for ThresholdBrain {
///     fn name(&self) -> &str { "threshold" }
///
///     async fn on_event(
///         &self,
///         event: &MarketDataEvent,
///         position: &Position,
///     ) -> Result<Decision> {
///         *self.state.lock().unwrap() += 1;
///         let close = match event {
///             MarketDataEvent::Candle { candle, .. } => candle.close,
///             _ => return Ok(Decision::hold()),
///         };
///         if close > self.threshold && position.qty <= 0.0 {
///             Ok(Decision::buy(1.0))
///         } else if close <= self.threshold && position.qty > 0.0 {
///             Ok(Decision::close())
///         } else {
///             Ok(Decision::hold())
///         }
///     }
///
///     async fn health(&self) -> BrainHealth { BrainHealth::ok() }
/// }
/// ```
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
    async fn on_position_change(&self, _symbol: &Symbol, _position: &Position) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Price;

    #[test]
    fn decision_hold() {
        let d = Decision::hold();
        assert!(matches!(d.signal, SignalType::Hold));
        assert_eq!(d.confidence, 0.0);
        assert!(d.stop_price.is_none());
        assert!(d.take_profit_price.is_none());
        assert!(matches!(d.size_hint, SizeHint::Default));
    }

    #[test]
    fn decision_buy_sell_close() {
        let b = Decision::buy(0.75);
        assert!(matches!(b.signal, SignalType::Buy));
        assert_eq!(b.confidence, 0.75);

        let s = Decision::sell(0.5);
        assert!(matches!(s.signal, SignalType::Sell));

        let c = Decision::close();
        assert!(matches!(c.signal, SignalType::Close));
        assert_eq!(c.confidence, 1.0);
    }

    #[test]
    fn decision_builders_compose() {
        let d = Decision::buy(0.9)
            .with_stop(Price(95.0))
            .with_take_profit(Price(110.0))
            .with_size_hint(SizeHint::MarginFraction(0.25))
            .with_metadata(serde_json::json!({"reason": "ema-cross"}));

        assert_eq!(d.stop_price, Some(Price(95.0)));
        assert_eq!(d.take_profit_price, Some(Price(110.0)));
        assert!(matches!(d.size_hint, SizeHint::MarginFraction(f) if (f - 0.25).abs() < 1e-9));
        assert_eq!(d.metadata["reason"], "ema-cross");
    }

    #[test]
    fn decision_serde_roundtrip() {
        let d = Decision::sell(0.6).with_stop(Price(120.0));
        let json = serde_json::to_string(&d).unwrap();
        let back: Decision = serde_json::from_str(&json).unwrap();
        assert!(matches!(back.signal, SignalType::Sell));
        assert_eq!(back.confidence, 0.6);
        assert_eq!(back.stop_price, Some(Price(120.0)));
    }

    #[test]
    fn brain_health_ok_is_healthy() {
        let h = BrainHealth::ok();
        assert!(h.healthy);
        assert_eq!(h.events_processed, 0);
        assert_eq!(h.non_hold_decisions, 0);
    }

    #[test]
    fn brain_health_unhealthy_captures_reason() {
        let h = BrainHealth::unhealthy("warm-up incomplete");
        assert!(!h.healthy);
        assert_eq!(h.details["reason"], "warm-up incomplete");
    }
}
