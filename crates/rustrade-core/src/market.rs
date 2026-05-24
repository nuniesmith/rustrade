//! Exchange-agnostic market data primitives.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{Candle, Tick};

/// Side of a trade or order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// The opposite side — used to construct closing orders.
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// Identifies an exchange-symbol pair.
///
/// `Symbol` is intentionally a bare `String` wrapper rather than a lookup
/// into an exchange-specific enum, because symbols are exchange-specific
/// strings (e.g. `"XBTUSDTM"` on KuCoin, `"BTCUSDT"` on Binance) and the
/// framework shouldn't pretend otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol(pub String);

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Opaque identifier for which exchange produced this data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Exchange(pub String);

impl From<&str> for Exchange {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// A normalized market-data event that can come from any exchange.
///
/// Exchange adapters parse their native formats into this enum so the
/// downstream brain and risk layers are exchange-agnostic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketDataEvent {
    /// Best-bid/best-ask update.
    Ticker {
        exchange: Exchange,
        symbol: Symbol,
        tick: Tick,
    },
    /// A completed candle (fully closed bar).
    Candle {
        exchange: Exchange,
        symbol: Symbol,
        candle: Candle,
    },
    /// An individual trade print.
    Trade {
        exchange: Exchange,
        symbol: Symbol,
        side: Side,
        price: f64,
        size: f64,
        timestamp: DateTime<Utc>,
    },
}

impl MarketDataEvent {
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Ticker { symbol, .. }
            | Self::Candle { symbol, .. }
            | Self::Trade { symbol, .. } => symbol,
        }
    }

    pub fn exchange(&self) -> &Exchange {
        match self {
            Self::Ticker { exchange, .. }
            | Self::Candle { exchange, .. }
            | Self::Trade { exchange, .. } => exchange,
        }
    }
}
