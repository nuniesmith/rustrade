//! Domain types using the newtype pattern to prevent unit-confusion bugs.
//!
//! Every quantity that has units (price, volume, notional) gets its own
//! wrapper type. This costs a few lines of code now and saves hours of
//! debugging later.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::market::{Side, Symbol};

// ── Scalar wrappers ──────────────────────────────────────────────────────────

/// Price in the quote currency (e.g. USD / USDT).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Price(pub f64);

impl Price {
    /// Zero price.
    pub const ZERO: Self = Self(0.0);

    /// Construct a `Price` from an `f64`.
    #[inline]
    pub const fn new(v: f64) -> Self {
        Self(v)
    }
    /// Unwrap to the inner `f64`.
    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Volume / quantity in base-asset units (e.g. BTC, ETH) or in contracts.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Volume(pub f64);

impl Volume {
    /// Zero volume.
    pub const ZERO: Self = Self(0.0);

    /// Construct a `Volume` from an `f64`.
    #[inline]
    pub const fn new(v: f64) -> Self {
        Self(v)
    }
    /// Unwrap to the inner `f64`.
    #[inline]
    pub const fn value(self) -> f64 {
        self.0
    }
}

impl fmt::Display for Volume {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── Market data ──────────────────────────────────────────────────────────────

/// A single trade tick or best-bid/best-ask snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    /// Exchange symbol the tick is for.
    pub symbol: Symbol,
    /// Time the tick was observed at the exchange.
    pub timestamp: DateTime<Utc>,
    /// Best bid price.
    pub bid: Price,
    /// Best ask price.
    pub ask: Price,
    /// Size resting at the best bid.
    pub bid_size: Volume,
    /// Size resting at the best ask.
    pub ask_size: Volume,
    /// Most recent trade price, if the feed reports it.
    pub last_price: Option<Price>,
    /// Most recent trade size, if the feed reports it.
    pub last_size: Option<Volume>,
}

impl Tick {
    /// Midpoint of bid and ask.
    pub fn mid_price(&self) -> Price {
        Price((self.bid.0 + self.ask.0) / 2.0)
    }

    /// Ask minus bid.
    pub fn spread(&self) -> Price {
        Price(self.ask.0 - self.bid.0)
    }
}

/// OHLCV candle — the atomic unit of batched market data.
///
/// `time` is the open time of the candle in milliseconds since the UNIX epoch.
/// Stored as `i64` (not `f64`) to avoid precision loss at millisecond granularity.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    /// Candle open time in milliseconds since the UNIX epoch.
    pub time: i64,
    /// Open price.
    pub open: f64,
    /// High price during the bar.
    pub high: f64,
    /// Low price during the bar.
    pub low: f64,
    /// Close price.
    pub close: f64,
    /// Traded volume in base-asset units (or contracts) during the bar.
    pub volume: f64,
}

// ── Orders and fills ─────────────────────────────────────────────────────────

/// Order kind (market vs limit and their time-in-force variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    /// Market order — fill immediately at the best available price.
    Market,
    /// Standard limit order — rest on the book until filled or cancelled.
    Limit,
    /// Post-only limit (rejected if it would cross the book as taker).
    PostOnly,
    /// Immediate-or-cancel — fill what you can now, cancel the rest.
    Ioc,
    /// Fill-or-kill — fill completely at the given price or cancel entirely.
    Fok,
}

/// What kind of stop attachment an [`Order`] carries.
///
/// Opaque to the framework — the adapter is responsible for translating
/// these into native exchange semantics (e.g. KuCoin futures stops vs
/// Binance OCO orders vs Bybit conditional orders). Adapters that don't
/// support a given variant should reject the order with a clear error
/// rather than silently ignoring the attachment.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StopKind {
    /// Trigger a market order at `trigger_price`.
    StopMarket,
    /// Trigger a limit order at `trigger_price`, posted at `limit_price`.
    StopLimit {
        /// Limit price posted when the stop is triggered.
        limit_price: Price,
    },
    /// Take-profit market order at `trigger_price`.
    TakeProfit,
    /// Trailing stop with the given trail distance in quote currency.
    TrailingStop {
        /// Distance in quote currency the price must retrace before triggering.
        trail_distance: Price,
    },
}

/// Stop-order attachment on an [`Order`].
///
/// See [`StopKind`] for variants. Use
/// [`ExchangeClient::supports`](crate::ExchangeClient::supports) with
/// [`Capability::StopOrders`](crate::Capability::StopOrders) to check
/// adapter capability before constructing one.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StopAttachment {
    /// Price at which the exchange triggers the stop.
    pub trigger_price: Price,
    /// What kind of stop to fire on trigger.
    pub kind: StopKind,
}

impl StopAttachment {
    /// Convenience: stop-market at `trigger_price`.
    pub fn stop_market(trigger_price: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::StopMarket,
        }
    }

    /// Convenience: stop-limit triggered at `trigger_price`, posted at `limit_price`.
    pub fn stop_limit(trigger_price: Price, limit_price: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::StopLimit { limit_price },
        }
    }

    /// Convenience: take-profit market order at `trigger_price`.
    pub fn take_profit(trigger_price: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::TakeProfit,
        }
    }

    /// Convenience: trailing stop with the given trail distance.
    pub fn trailing(trigger_price: Price, trail_distance: Price) -> Self {
        Self {
            trigger_price,
            kind: StopKind::TrailingStop { trail_distance },
        }
    }
}

/// A request to enter, exit, or reduce a position.
///
/// This is the framework-level abstraction; concrete exchange adapters translate
/// it into exchange-specific payloads. The `client_id` is optional but strongly
/// recommended — it lets the framework reconcile fills back to this order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Symbol the order is for.
    pub symbol: Symbol,
    /// Side of the trade (buy or sell).
    pub side: Side,
    /// Market / limit / time-in-force variant.
    pub kind: OrderKind,
    /// Quantity in base-asset units or contracts.
    pub size: Volume,
    /// Limit price for non-market orders.
    pub limit_price: Option<Price>,
    /// Set to `true` for exit orders that must never increase the position.
    pub reduce_only: bool,
    /// Optional client-supplied id. Exchanges that support it will echo it back
    /// on fills, making reconciliation trivial.
    pub client_id: Option<String>,
    /// Optional stop-order attachment. Adapters that don't advertise
    /// [`Capability::StopOrders`](crate::Capability::StopOrders) must
    /// reject orders carrying this field.
    #[serde(default)]
    pub stop: Option<StopAttachment>,
}

impl Order {
    /// Construct a market order.
    pub fn market(symbol: impl Into<Symbol>, side: Side, size: Volume) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            kind: OrderKind::Market,
            size,
            limit_price: None,
            reduce_only: false,
            client_id: None,
            stop: None,
        }
    }

    /// Construct a standard limit order resting at `price`.
    pub fn limit(symbol: impl Into<Symbol>, side: Side, size: Volume, price: Price) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            kind: OrderKind::Limit,
            size,
            limit_price: Some(price),
            reduce_only: false,
            client_id: None,
            stop: None,
        }
    }

    /// Set or clear the `reduce_only` flag.
    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    /// Set the optional client id used to reconcile fills back to this order.
    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    /// Attach a stop. Adapter must advertise
    /// [`Capability::StopOrders`](crate::Capability::StopOrders).
    pub fn with_stop(mut self, stop: StopAttachment) -> Self {
        self.stop = Some(stop);
        self
    }
}

/// A trade fill reported by the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    /// Symbol this fill is for.
    pub symbol: Symbol,
    /// Exchange-assigned order id this fill belongs to.
    pub order_id: String,
    /// Optional client id echoed back by the exchange.
    pub client_id: Option<String>,
    /// Side of the trade.
    pub side: Side,
    /// Fill price.
    pub price: Price,
    /// Filled quantity in base-asset units or contracts.
    pub size: Volume,
    /// Fee charged for this fill, in `fee_currency`.
    pub fee: f64,
    /// Currency the `fee` is denominated in (e.g. `"USDT"`).
    pub fee_currency: String,
    /// Time the fill occurred at the exchange.
    pub timestamp: DateTime<Utc>,
}

// ── Position ─────────────────────────────────────────────────────────────────

/// Current exchange-reported position for a single symbol.
///
/// `qty` is signed: positive = long, negative = short, zero = flat.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct Position {
    /// Signed position size — positive = long, negative = short.
    pub qty: f64,
    /// Average entry price (weighted by qty if multiple opens), or
    /// `None` for a flat position.
    pub entry_price: Option<f64>,
    /// Unrealised PnL at the most recent mark price, in quote currency.
    pub unrealised_pnl: f64,
}

impl Position {
    /// A flat (zero-size) position with no entry price.
    pub const FLAT: Self = Self {
        qty: 0.0,
        entry_price: None,
        unrealised_pnl: 0.0,
    };

    /// `true` when `qty == 0`.
    #[inline]
    pub fn is_flat(&self) -> bool {
        self.qty == 0.0
    }

    /// `true` when `qty > 0`.
    #[inline]
    pub fn is_long(&self) -> bool {
        self.qty > 0.0
    }

    /// `true` when `qty < 0`.
    #[inline]
    pub fn is_short(&self) -> bool {
        self.qty < 0.0
    }

    /// Side needed to fully close this position (None if flat).
    pub fn close_side(&self) -> Option<Side> {
        if self.qty > 0.0 {
            Some(Side::Sell)
        } else if self.qty < 0.0 {
            Some(Side::Buy)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn tick(bid: f64, ask: f64) -> Tick {
        Tick {
            symbol: Symbol::new("BTCUSDT"),
            timestamp: Utc.timestamp_opt(0, 0).unwrap(),
            bid: Price(bid),
            ask: Price(ask),
            bid_size: Volume(1.0),
            ask_size: Volume(1.0),
            last_price: None,
            last_size: None,
        }
    }

    #[test]
    fn tick_mid_price_and_spread() {
        let t = tick(100.0, 102.0);
        assert!((t.mid_price().value() - 101.0).abs() < 1e-9);
        assert!((t.spread().value() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn tick_mid_handles_zero_spread() {
        let t = tick(100.0, 100.0);
        assert_eq!(t.mid_price().value(), 100.0);
        assert_eq!(t.spread().value(), 0.0);
    }

    #[test]
    fn position_close_side_long() {
        let p = Position {
            qty: 5.0,
            ..Position::FLAT
        };
        assert!(p.is_long());
        assert!(!p.is_short());
        assert!(!p.is_flat());
        assert_eq!(p.close_side(), Some(Side::Sell));
    }

    #[test]
    fn position_close_side_short() {
        let p = Position {
            qty: -5.0,
            ..Position::FLAT
        };
        assert!(p.is_short());
        assert!(!p.is_long());
        assert_eq!(p.close_side(), Some(Side::Buy));
    }

    #[test]
    fn position_close_side_flat() {
        assert_eq!(Position::FLAT.close_side(), None);
        assert!(Position::FLAT.is_flat());
    }

    #[test]
    fn order_market_builder() {
        let o = Order::market("BTCUSDT", Side::Buy, Volume(1.0));
        assert_eq!(o.symbol, Symbol::new("BTCUSDT"));
        assert_eq!(o.side, Side::Buy);
        assert_eq!(o.kind, OrderKind::Market);
        assert!(o.limit_price.is_none());
        assert!(!o.reduce_only);
        assert!(o.client_id.is_none());
        assert!(o.stop.is_none());
    }

    #[test]
    fn order_limit_builder() {
        let o = Order::limit("BTCUSDT", Side::Sell, Volume(2.0), Price(50_000.0));
        assert_eq!(o.kind, OrderKind::Limit);
        assert_eq!(o.limit_price, Some(Price(50_000.0)));
    }

    #[test]
    fn order_with_reduce_only() {
        let o = Order::market("X", Side::Buy, Volume(1.0)).with_reduce_only(true);
        assert!(o.reduce_only);
    }

    #[test]
    fn order_with_client_id() {
        let o = Order::market("X", Side::Buy, Volume(1.0)).with_client_id("abc-123");
        assert_eq!(o.client_id.as_deref(), Some("abc-123"));
    }

    #[test]
    fn order_with_stop_market() {
        let o = Order::market("X", Side::Sell, Volume(1.0))
            .with_reduce_only(true)
            .with_stop(StopAttachment::stop_market(Price(95.0)));
        assert!(o.reduce_only);
        let stop = o.stop.unwrap();
        assert_eq!(stop.trigger_price, Price(95.0));
        assert!(matches!(stop.kind, StopKind::StopMarket));
    }

    #[test]
    fn order_with_stop_limit() {
        let s = StopAttachment::stop_limit(Price(95.0), Price(94.5));
        assert_eq!(s.trigger_price, Price(95.0));
        match s.kind {
            StopKind::StopLimit { limit_price } => assert_eq!(limit_price, Price(94.5)),
            other => panic!("unexpected stop kind: {other:?}"),
        }
    }

    #[test]
    fn stop_attachment_take_profit() {
        let s = StopAttachment::take_profit(Price(110.0));
        assert!(matches!(s.kind, StopKind::TakeProfit));
    }

    #[test]
    fn stop_attachment_trailing() {
        let s = StopAttachment::trailing(Price(100.0), Price(2.5));
        match s.kind {
            StopKind::TrailingStop { trail_distance } => {
                assert_eq!(trail_distance, Price(2.5));
            }
            other => panic!("unexpected stop kind: {other:?}"),
        }
    }

    #[test]
    fn order_serde_roundtrip_without_stop() {
        let o = Order::market("BTCUSDT", Side::Buy, Volume(1.5));
        let json = serde_json::to_string(&o).unwrap();
        let back: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(back.symbol, o.symbol);
        assert_eq!(back.side, o.side);
        assert_eq!(back.size, o.size);
        assert!(back.stop.is_none());
    }

    #[test]
    fn order_serde_roundtrip_with_stop() {
        let o = Order::market("X", Side::Sell, Volume(1.0))
            .with_stop(StopAttachment::trailing(Price(100.0), Price(2.5)));
        let json = serde_json::to_string(&o).unwrap();
        let back: Order = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stop, o.stop);
    }
}
