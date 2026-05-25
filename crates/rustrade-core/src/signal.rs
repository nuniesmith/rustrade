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
    /// Unique identifier for this signal (typically `{brain_name}-{counter}`).
    pub id: String,
    /// Symbol the signal is for, as a free-form string.
    pub symbol: String,
    /// Buy / Sell / Hold / Close.
    pub kind: SignalType,
    /// Confidence in [0.0, 1.0]. A brain producing a `Buy` with confidence
    /// 0.2 is saying "I'm barely sure about this" — the risk layer can choose
    /// to size down or reject.
    pub confidence: f64,
    /// Time the brain emitted this signal.
    pub timestamp: DateTime<Utc>,
    /// Brain name that emitted the signal — useful for routing in
    /// multi-brain bots.
    pub source: String,
    /// Free-form. Use `serde_json::json!({...})` to populate.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_type_display() {
        assert_eq!(SignalType::Buy.to_string(), "BUY");
        assert_eq!(SignalType::Sell.to_string(), "SELL");
        assert_eq!(SignalType::Hold.to_string(), "HOLD");
        assert_eq!(SignalType::Close.to_string(), "CLOSE");
    }

    #[test]
    fn signal_serde_roundtrip() {
        let s = Signal {
            id: "sig-1".into(),
            symbol: "BTCUSDT".into(),
            kind: SignalType::Buy,
            confidence: 0.8,
            timestamp: chrono::Utc::now(),
            source: "ema-cross".into(),
            metadata: serde_json::json!({"fast": 9, "slow": 21}),
        };
        let json = serde_json::to_string(&s).unwrap();
        let back: Signal = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, s.id);
        assert!(matches!(back.kind, SignalType::Buy));
        assert_eq!(back.metadata["fast"], 9);
    }
}
