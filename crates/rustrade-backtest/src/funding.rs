//! Perpetual-futures funding cashflows for the replay engine.
//!
//! Perp exchanges settle funding at fixed timestamps (KuCoin and Binance
//! USDT perps every 8 hours, on the 00:00 / 08:00 / 16:00 UTC grid). At
//! each settlement an open position pays or receives `rate × notional`:
//! a **positive rate means longs pay shorts** (a long's cashflow is
//! negative, a short collects), a negative rate mirrors. A strategy that
//! holds across settlements — the platform's funding-capture edge most of
//! all — has this cashflow as a first-order PnL term, so a backtest that
//! omits it overstates (or understates) the edge.
//!
//! [`FundingModel`] plugs the settlement schedule into
//! [`BacktestConfig`](crate::BacktestConfig) the same way
//! [`FeeModel`](crate::FeeModel) / [`SlippageModel`](crate::SlippageModel)
//! do. It defaults to [`FundingModel::None`] — existing backtests are
//! bit-for-bit unchanged unless a model is configured.
//!
//! # Settlement semantics (parity with the live paper ledger)
//!
//! The live paper bot accrues funding over a hold as the settlements in
//! `(entry, exit]` — strictly after entry, up to and including exit — each
//! paying the position `−direction × rate × notional`. The replay engine
//! applies the same window per candle: settlements in
//! `(previous_candle_time, candle_time]` are booked against the position
//! *as it stood entering the candle*, at the last-known mark. A position
//! opened exactly on a settlement timestamp does not pay that settlement;
//! a position closed exactly on one does.

use serde::{Deserialize, Serialize};

/// Eight hours in milliseconds — the funding interval of KuCoin and
/// Binance USDT perps (settling on the 00:00 / 08:00 / 16:00 UTC grid,
/// which is exactly the epoch-aligned 8 h grid).
pub const EIGHT_HOURS_MS: i64 = 8 * 3_600_000;

/// Pluggable perp funding-rate schedule. Defaults to [`Self::None`].
///
/// Rates are fractions of notional per settlement (`0.0001` = 1 bp), the
/// same convention exchanges publish. Timestamps are epoch milliseconds —
/// the same unit as [`Candle::time`](rustrade_core::Candle).
///
/// # Example
///
/// ```
/// use rustrade_backtest::{FundingModel, EIGHT_HOURS_MS};
///
/// // Historical series: (settlement timestamp ms, rate).
/// let series = FundingModel::Series(vec![
///     (EIGHT_HOURS_MS, 0.0001),
///     (2 * EIGHT_HOURS_MS, -0.0002),
/// ]);
/// assert_eq!(series.settlements_between(0, 2 * EIGHT_HOURS_MS).len(), 2);
///
/// // Constant fallback when no series exists: 1 bp every 8 h.
/// let constant = FundingModel::Constant { rate: 0.0001, interval_ms: EIGHT_HOURS_MS };
/// let s = constant.settlements_between(0, 24 * 3_600_000);
/// assert_eq!(s, vec![
///     (EIGHT_HOURS_MS, 0.0001),
///     (2 * EIGHT_HOURS_MS, 0.0001),
///     (3 * EIGHT_HOURS_MS, 0.0001),
/// ]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum FundingModel {
    /// No funding cashflows — spot backtests and every pre-existing
    /// consumer. The default.
    #[default]
    None,
    /// Constant rate applied at every multiple of `interval_ms` on the
    /// epoch-aligned grid — the fallback when no historical series
    /// exists. `interval_ms = EIGHT_HOURS_MS` reproduces the KuCoin /
    /// Binance 8 h UTC settlement grid.
    Constant {
        /// Rate per settlement as a fraction of notional (`0.0001` = 1 bp).
        /// Positive = longs pay, shorts receive.
        rate: f64,
        /// Milliseconds between settlements. Must be `> 0`.
        interval_ms: i64,
    },
    /// Historical settlement series: `(timestamp_ms, rate)` pairs.
    /// [`BacktestConfigBuilder::build`](crate::BacktestConfigBuilder::build)
    /// sorts the series by timestamp and rejects non-finite rates and
    /// duplicate timestamps (a duplicate would double-book a settlement).
    Series(Vec<(i64, f64)>),
}

impl FundingModel {
    /// The settlements in the half-open window `(after, until]`, ascending
    /// by timestamp. This is the live paper ledger's accrual window: a
    /// settlement exactly at `after` is excluded, one exactly at `until`
    /// is included.
    ///
    /// For [`Self::Series`] the series is assumed ascending by timestamp
    /// (the config builder guarantees it); results on an unsorted series
    /// are unspecified.
    pub fn settlements_between(&self, after: i64, until: i64) -> Vec<(i64, f64)> {
        if until <= after {
            return Vec::new();
        }
        match self {
            Self::None => Vec::new(),
            Self::Constant { rate, interval_ms } => {
                let interval = *interval_ms;
                if interval <= 0 {
                    return Vec::new();
                }
                // First grid point strictly greater than `after`.
                // `div_euclid` keeps the grid epoch-aligned for negative
                // timestamps too.
                let mut t = (after.div_euclid(interval) + 1) * interval;
                let mut out = Vec::new();
                while t <= until {
                    out.push((t, *rate));
                    t += interval;
                }
                out
            }
            Self::Series(points) => {
                let start = points.partition_point(|(t, _)| *t <= after);
                let end = points.partition_point(|(t, _)| *t <= until);
                points[start..end].to_vec()
            }
        }
    }

    /// Validate (and normalise) the model for use in a config.
    ///
    /// Rejects non-finite rates and non-positive intervals; sorts a
    /// [`Self::Series`] ascending by timestamp and rejects duplicate
    /// timestamps. Returns the normalised model.
    pub(crate) fn validated(mut self) -> std::result::Result<Self, String> {
        match &mut self {
            Self::None => {}
            Self::Constant { rate, interval_ms } => {
                if !rate.is_finite() {
                    return Err(format!("funding rate must be finite, got {rate}"));
                }
                if *interval_ms <= 0 {
                    return Err(format!(
                        "funding interval_ms must be > 0, got {interval_ms}"
                    ));
                }
            }
            Self::Series(points) => {
                for (t, rate) in points.iter() {
                    if !rate.is_finite() {
                        return Err(format!("funding rate at t={t} must be finite, got {rate}"));
                    }
                }
                points.sort_by_key(|(t, _)| *t);
                if points.windows(2).any(|w| w[0].0 == w[1].0) {
                    return Err(
                        "funding series has duplicate settlement timestamps (would double-book)"
                            .into(),
                    );
                }
            }
        }
        Ok(self)
    }

    /// `true` unless the model is [`Self::None`].
    pub(crate) fn is_enabled(&self) -> bool {
        !matches!(self, Self::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_yields_no_settlements() {
        assert!(
            FundingModel::None
                .settlements_between(0, i64::MAX)
                .is_empty()
        );
    }

    #[test]
    fn constant_grid_is_entry_exclusive_exit_inclusive() {
        let m = FundingModel::Constant {
            rate: 0.0001,
            interval_ms: 100,
        };
        // A settlement exactly at `after` is excluded…
        assert_eq!(m.settlements_between(100, 250), vec![(200, 0.0001)]);
        // …and one exactly at `until` is included.
        assert_eq!(
            m.settlements_between(50, 200),
            vec![(100, 0.0001), (200, 0.0001)]
        );
        // Empty / inverted windows yield nothing.
        assert!(m.settlements_between(100, 100).is_empty());
        assert!(m.settlements_between(200, 100).is_empty());
    }

    #[test]
    fn constant_grid_stays_epoch_aligned_across_zero() {
        let m = FundingModel::Constant {
            rate: 0.5,
            interval_ms: 100,
        };
        assert_eq!(
            m.settlements_between(-150, 100),
            vec![(-100, 0.5), (0, 0.5), (100, 0.5)]
        );
    }

    #[test]
    fn series_window_matches_constant_window_semantics() {
        let m = FundingModel::Series(vec![(100, 0.1), (200, 0.2), (300, 0.3)]);
        assert_eq!(
            m.settlements_between(100, 300),
            vec![(200, 0.2), (300, 0.3)]
        );
        assert!(m.settlements_between(300, 1_000).is_empty());
    }

    #[test]
    fn validated_sorts_series_and_rejects_duplicates() {
        let m = FundingModel::Series(vec![(300, 0.3), (100, 0.1)])
            .validated()
            .unwrap();
        assert_eq!(m, FundingModel::Series(vec![(100, 0.1), (300, 0.3)]));

        assert!(
            FundingModel::Series(vec![(100, 0.1), (100, 0.2)])
                .validated()
                .is_err()
        );
    }

    #[test]
    fn validated_rejects_bad_constant() {
        assert!(
            FundingModel::Constant {
                rate: f64::NAN,
                interval_ms: 100
            }
            .validated()
            .is_err()
        );
        assert!(
            FundingModel::Constant {
                rate: 0.0001,
                interval_ms: 0
            }
            .validated()
            .is_err()
        );
    }

    #[test]
    fn validated_rejects_non_finite_series_rate() {
        assert!(
            FundingModel::Series(vec![(100, f64::INFINITY)])
                .validated()
                .is_err()
        );
    }
}
