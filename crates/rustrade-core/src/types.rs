//! Domain types using the newtype pattern to prevent unit-confusion bugs.
//!
//! Every quantity that has units (price, volume, notional) gets its own
//! wrapper type. This costs a few lines of code now and saves hours of
//! debugging later.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::market::Side;

// ── Scalar wrappers ──────────────────────────────────────────────────────────

/// Price in the quote currency (e.g. USD / USDT).
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Price(pub f64);

impl Price {
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub const fn new(v: f64) -> Self {
        Self(v)
    }
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
    pub const ZERO: Self = Self(0.0);

    #[inline]
    pub const fn new(v: f64) -> Self {
        Self(v)
    }
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
    pub symbol: String,
    pub timestamp: DateTime<Utc>,
    pub bid: Price,
    pub ask: Price,
    pub bid_size: Volume,
    pub ask_size: Volume,
    pub last_price: Option<Price>,
    pub last_size: Option<Volume>,
}

impl Tick {
    pub fn mid_price(&self) -> Price {
        Price((self.bid.0 + self.ask.0) / 2.0)
    }

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
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

// ── Orders and fills ─────────────────────────────────────────────────────────

/// Order kind (market vs limit and their time-in-force variants).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderKind {
    Market,
    Limit,
    /// Post-only limit (rejected if it would cross the book as taker).
    PostOnly,
    /// Immediate-or-cancel — fill what you can now, cancel the rest.
    Ioc,
    /// Fill-or-kill — fill completely at the given price or cancel entirely.
    Fok,
}

/// A request to enter, exit, or reduce a position.
///
/// This is the framework-level abstraction; concrete exchange adapters translate
/// it into exchange-specific payloads. The `client_id` is optional but strongly
/// recommended — it lets the framework reconcile fills back to this order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub symbol: String,
    pub side: Side,
    pub kind: OrderKind,
    pub size: Volume,
    /// Limit price for non-market orders.
    pub limit_price: Option<Price>,
    /// Set to `true` for exit orders that must never increase the position.
    pub reduce_only: bool,
    /// Optional client-supplied id. Exchanges that support it will echo it back
    /// on fills, making reconciliation trivial.
    pub client_id: Option<String>,
}

impl Order {
    pub fn market(symbol: impl Into<String>, side: Side, size: Volume) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            kind: OrderKind::Market,
            size,
            limit_price: None,
            reduce_only: false,
            client_id: None,
        }
    }

    pub fn limit(symbol: impl Into<String>, side: Side, size: Volume, price: Price) -> Self {
        Self {
            symbol: symbol.into(),
            side,
            kind: OrderKind::Limit,
            size,
            limit_price: Some(price),
            reduce_only: false,
            client_id: None,
        }
    }

    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    pub fn with_client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }
}

/// A trade fill reported by the exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub symbol: String,
    pub order_id: String,
    pub client_id: Option<String>,
    pub side: Side,
    pub price: Price,
    pub size: Volume,
    pub fee: f64,
    pub fee_currency: String,
    pub timestamp: DateTime<Utc>,
}

// ── Position ─────────────────────────────────────────────────────────────────

/// Current exchange-reported position for a single symbol.
///
/// `qty` is signed: positive = long, negative = short, zero = flat.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub struct Position {
    pub qty: f64,
    pub entry_price: Option<f64>,
    pub unrealised_pnl: f64,
}

impl Position {
    pub const FLAT: Self = Self {
        qty: 0.0,
        entry_price: None,
        unrealised_pnl: 0.0,
    };

    #[inline]
    pub fn is_flat(&self) -> bool {
        self.qty == 0.0
    }

    #[inline]
    pub fn is_long(&self) -> bool {
        self.qty > 0.0
    }

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
