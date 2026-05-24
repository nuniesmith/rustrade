//! Trading signals — the output of a [`crate::Brain`].

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A trading signal direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalType {
    /// Enter long (or flip from short to long).
    Buy,
    /// Enter short (or flip from long to short).
    Sell,
    /// No new action. Existing position may or may not be held depending on
    /// other gates (stops, max-hold, etc.).
    Hold,
    /// Close the existing position without reversing.
    Close,
}

impl std::fmt::Display for SignalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buy => write!(f, "BUY"),
            Self::Sell => write!(f, "SELL"),
            Self::Hold => write!(f, "HOLD"),
            Self::Close => write!(f, "CLOSE"),
        }
    }
}

/// A richer signal carrying confidence, source, and arbitrary metadata for logging.
///
/// The framework's execution layer doesn't interpret `metadata` — it's there so
/// a brain can record its rationale for post-hoc analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    pub id: String,
    pub symbol: String,
    pub kind: SignalType,
    /// Confidence in [0.0, 1.0]. A brain producing a `Buy` with confidence
    /// 0.2 is saying "I'm barely sure about this" — the risk layer can choose
    /// to size down or reject.
    pub confidence: f64,
    pub timestamp: DateTime<Utc>,
    pub source: String,
    /// Free-form. Use `serde_json::json!({...})` to populate.
    #[serde(default)]
    pub metadata: serde_json::Value,
}
