//! Candle series loaders for backtests.
//!
//! Phase 4b ships a single loader: a CSV reader with a fixed column
//! layout. Parquet, JSON-lines, and exchange-native dumps are
//! out-of-scope for v0.1 — they're easy enough for users to write
//! against [`crate::Backtest::with_candles`].
//!
//! # CSV format
//!
//! ```text
//! time,open,high,low,close,volume
//! 1700000000000,42000.0,42100.0,41900.0,42050.0,123.4
//! 1700000060000,42050.0,42200.0,42030.0,42180.0,98.7
//! ...
//! ```
//!
//! - `time` is milliseconds since the UNIX epoch (`i64`).
//! - The header row is required and column order is fixed.
//! - Empty rows and rows beginning with `#` are skipped.
//!
//! # Sort order
//!
//! Candles are returned in the order they appear in the file. The
//! [`Backtest`](crate::Backtest) engine assumes chronological order
//! (oldest first); use [`sort_chronological`] if your source is
//! newest-first.

use std::path::Path;

use rustrade_core::Candle;
use serde::Deserialize;

use crate::error::{Error, Result};

#[derive(Debug, Deserialize)]
struct CandleRow {
    time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

impl From<CandleRow> for Candle {
    fn from(r: CandleRow) -> Self {
        Self {
            time: r.time,
            open: r.open,
            high: r.high,
            low: r.low,
            close: r.close,
            volume: r.volume,
        }
    }
}

/// Load candles from a CSV file. See the module docs for the expected
/// format.
pub fn load_csv<P: AsRef<Path>>(path: P) -> Result<Vec<Candle>> {
    let mut rdr = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .flexible(false)
        .from_path(path.as_ref())
        .map_err(|e| Error::Config(format!("failed to open CSV: {e}")))?;

    let mut out = Vec::new();
    for (idx, row) in rdr.deserialize::<CandleRow>().enumerate() {
        let row = row.map_err(|e| {
            Error::Config(format!(
                "CSV row {} parse error: {e}",
                idx + 2 // +1 for 1-based, +1 for header
            ))
        })?;
        let candle: Candle = row.into();
        crate::engine::validate_candle(&candle)
            .map_err(|why| Error::Data(format!("CSV row {}: {why}", idx + 2)))?;
        out.push(candle);
    }
    Ok(out)
}

/// Load candles from a CSV string (in-memory). Mostly useful for tests.
pub fn load_csv_str(s: &str) -> Result<Vec<Candle>> {
    let mut rdr = csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .flexible(false)
        .from_reader(s.as_bytes());

    let mut out = Vec::new();
    for (idx, row) in rdr.deserialize::<CandleRow>().enumerate() {
        let row =
            row.map_err(|e| Error::Config(format!("CSV row {} parse error: {e}", idx + 2)))?;
        let candle: Candle = row.into();
        crate::engine::validate_candle(&candle)
            .map_err(|why| Error::Data(format!("CSV row {}: {why}", idx + 2)))?;
        out.push(candle);
    }
    Ok(out)
}

/// Sort candles by `time` ascending. Stable — preserves order for
/// candles with identical timestamps (rare, but exchange ticks
/// sometimes coincide).
pub fn sort_chronological(mut candles: Vec<Candle>) -> Vec<Candle> {
    candles.sort_by_key(|c| c.time);
    candles
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_csv_str_basic() {
        let csv = "\
time,open,high,low,close,volume
1000,1.0,2.0,0.5,1.5,10.0
2000,1.5,2.5,1.0,2.0,12.0
3000,2.0,3.0,1.8,2.8,8.0
";
        let candles = load_csv_str(csv).unwrap();
        assert_eq!(candles.len(), 3);
        assert_eq!(candles[0].time, 1000);
        assert_eq!(candles[0].open, 1.0);
        assert_eq!(candles[2].close, 2.8);
    }

    #[test]
    fn load_csv_str_skips_comments_and_blanks() {
        let csv = "\
time,open,high,low,close,volume
# top comment
1000,1.0,2.0,0.5,1.5,10.0
# mid comment
2000,1.5,2.5,1.0,2.0,12.0
";
        let candles = load_csv_str(csv).unwrap();
        assert_eq!(candles.len(), 2);
    }

    #[test]
    fn load_csv_str_rejects_malformed_row() {
        let csv = "\
time,open,high,low,close,volume
1000,not-a-number,2.0,0.5,1.5,10.0
";
        let err = load_csv_str(csv).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn load_csv_str_rejects_non_positive_price() {
        // Structurally valid f64, but a zero/negative price is unusable.
        let csv = "\
time,open,high,low,close,volume
1000,1.0,2.0,0.5,0.0,10.0
";
        let err = load_csv_str(csv).unwrap_err();
        assert!(matches!(err, Error::Data(_)), "got {err:?}");
    }

    #[test]
    fn load_csv_str_rejects_non_finite_price() {
        // f64::from_str parses "inf"/"NaN"; validation must reject them.
        let csv = "\
time,open,high,low,close,volume
1000,1.0,2.0,0.5,inf,10.0
";
        let err = load_csv_str(csv).unwrap_err();
        assert!(matches!(err, Error::Data(_)), "got {err:?}");
    }

    #[test]
    fn sort_chronological_reorders_descending_input() {
        let candles = vec![
            Candle {
                time: 3000,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
            },
            Candle {
                time: 1000,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
            },
            Candle {
                time: 2000,
                open: 0.0,
                high: 0.0,
                low: 0.0,
                close: 0.0,
                volume: 0.0,
            },
        ];
        let sorted = sort_chronological(candles);
        assert_eq!(sorted[0].time, 1000);
        assert_eq!(sorted[1].time, 2000);
        assert_eq!(sorted[2].time, 3000);
    }
}
