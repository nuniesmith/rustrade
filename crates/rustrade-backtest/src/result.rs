//! Backtest result — aggregated metrics + the full trade ledger.

use serde::{Deserialize, Serialize};

use crate::metrics::{Outcome, TradeOutcome};

/// Final outcome of a [`crate::Backtest::run`] call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    /// Symbol the backtest was configured for. For multi-symbol
    /// backtests this is a comma-separated list in config order.
    pub symbol: String,
    /// Initial cash balance.
    pub initial_cash: f64,
    /// Final cash balance (= initial + net realised PnL).
    pub final_cash: f64,
    /// Total realised PnL net of fees.
    pub net_pnl: f64,
    /// Sum of fees charged across every fill.
    pub total_fees: f64,
    /// Number of candles fed to the brain.
    pub candles_processed: usize,
    /// Number of non-`Hold` decisions emitted by the brain.
    pub signals_emitted: usize,
    /// Number of orders the engine placed (may be `< signals_emitted`
    /// if the sizer returned 0 for some signals).
    pub orders_filled: usize,
    /// Per-trade outcomes, in chronological order.
    pub trades: Vec<TradeOutcome>,
    /// Maximum peak-to-trough drawdown of equity (cash) over the run,
    /// in quote currency. Always `<= 0`.
    pub max_drawdown: f64,
    /// Portfolio equity at each sample point. The first element is
    /// [`Self::initial_cash`]; one additional sample is appended per
    /// candle in the merged event stream.
    pub equity_curve: Vec<f64>,
    /// Per-period simple returns derived from [`Self::equity_curve`].
    /// Length is `equity_curve.len() - 1` for any non-empty run.
    pub period_returns: Vec<f64>,
    /// Per-period risk-free rate used by [`Self::sharpe_ratio`] and
    /// [`Self::sortino_ratio`]. See [`crate::BacktestConfig::risk_free_rate`].
    pub risk_free_rate: f64,
    /// Annualisation factor for the Sharpe and Sortino ratios. See
    /// [`crate::BacktestConfig::periods_per_year`].
    pub periods_per_year: u32,
}

impl BacktestResult {
    /// Total return as a percentage of initial cash.
    pub fn total_return_pct(&self) -> f64 {
        if self.initial_cash == 0.0 {
            0.0
        } else {
            (self.net_pnl / self.initial_cash) * 100.0
        }
    }

    /// Count of trades with net PnL > 0.
    pub fn wins(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Win)
            .count()
    }

    /// Count of trades with net PnL < 0.
    pub fn losses(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Loss)
            .count()
    }

    /// Count of trades with net PnL == 0.
    pub fn breakevens(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Breakeven)
            .count()
    }

    /// Win rate over decided trades (excludes breakevens), in `[0, 1]`.
    pub fn win_rate(&self) -> f64 {
        let decided = self.wins() + self.losses();
        if decided == 0 {
            0.0
        } else {
            self.wins() as f64 / decided as f64
        }
    }

    /// Sum of winning trades' net PnL / sum of losing trades' net PnL
    /// (positive). `None` if there are no losing trades.
    pub fn profit_factor(&self) -> Option<f64> {
        let wins: f64 = self
            .trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Win)
            .map(|t| t.net_pnl())
            .sum();
        let losses: f64 = self
            .trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Loss)
            .map(|t| t.net_pnl().abs())
            .sum();
        if losses == 0.0 {
            None
        } else {
            Some(wins / losses)
        }
    }

    /// Annualised Sharpe ratio of the per-period returns.
    ///
    /// Computed as `√P · (mean(rᵢ - rf) / stddev(rᵢ))` where `rᵢ` is
    /// each entry in [`Self::period_returns`], `rf` is
    /// [`Self::risk_free_rate`], `stddev` is the sample standard
    /// deviation (`N - 1` denominator), and `P` is
    /// [`Self::periods_per_year`].
    ///
    /// Returns `None` when there are fewer than two return samples, or
    /// when the sample stddev is zero (a perfectly flat equity curve —
    /// Sharpe is undefined).
    pub fn sharpe_ratio(&self) -> Option<f64> {
        let r = &self.period_returns;
        if r.len() < 2 {
            return None;
        }
        let rf = self.risk_free_rate;
        let excess: Vec<f64> = r.iter().map(|x| x - rf).collect();
        let n = excess.len() as f64;
        let mean = excess.iter().sum::<f64>() / n;
        let variance = excess.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
        let stddev = variance.sqrt();
        if stddev == 0.0 || !stddev.is_finite() {
            return None;
        }
        let scale = (self.periods_per_year as f64).sqrt();
        Some(scale * mean / stddev)
    }

    /// Annualised Sortino ratio of the per-period returns.
    ///
    /// Same shape as Sharpe but only penalises downside deviation —
    /// returns below `rf` contribute to the denominator, returns above
    /// `rf` don't. Specifically `√P · mean(rᵢ - rf) / downside_dev`
    /// where `downside_dev = √(Σ min(rᵢ - rf, 0)² / N)`. Returns
    /// `None` if no returns are below `rf` (no downside to measure) or
    /// fewer than two samples exist.
    pub fn sortino_ratio(&self) -> Option<f64> {
        let r = &self.period_returns;
        if r.len() < 2 {
            return None;
        }
        let rf = self.risk_free_rate;
        let excess: Vec<f64> = r.iter().map(|x| x - rf).collect();
        let n = excess.len() as f64;
        let mean = excess.iter().sum::<f64>() / n;
        let downside_var = excess
            .iter()
            .map(|x| if *x < 0.0 { x.powi(2) } else { 0.0 })
            .sum::<f64>()
            / n;
        let downside_dev = downside_var.sqrt();
        if downside_dev == 0.0 || !downside_dev.is_finite() {
            return None;
        }
        let scale = (self.periods_per_year as f64).sqrt();
        Some(scale * mean / downside_dev)
    }

    /// Pretty-printed multi-line summary suitable for logging.
    pub fn summary(&self) -> String {
        let pf = self
            .profit_factor()
            .map(|p| format!("{p:.3}"))
            .unwrap_or_else(|| "∞ (no losses)".into());
        let sharpe = self
            .sharpe_ratio()
            .map(|s| format!("{s:.3}"))
            .unwrap_or_else(|| "n/a".into());
        let sortino = self
            .sortino_ratio()
            .map(|s| format!("{s:.3}"))
            .unwrap_or_else(|| "n/a".into());
        format!(
            "Backtest [{}]\n\
             ├ candles_processed: {}\n\
             ├ signals / orders : {} / {}\n\
             ├ trades           : {} (W {} / L {} / BE {})\n\
             ├ win_rate         : {:.2}%\n\
             ├ profit_factor    : {pf}\n\
             ├ sharpe / sortino : {sharpe} / {sortino}\n\
             ├ total_return     : {:.4}%\n\
             ├ net_pnl          : {:.4}\n\
             ├ total_fees       : {:.4}\n\
             ├ max_drawdown     : {:.4}\n\
             └ final_cash       : {:.4}",
            self.symbol,
            self.candles_processed,
            self.signals_emitted,
            self.orders_filled,
            self.trades.len(),
            self.wins(),
            self.losses(),
            self.breakevens(),
            self.win_rate() * 100.0,
            self.total_return_pct(),
            self.net_pnl,
            self.total_fees,
            self.max_drawdown,
            self.final_cash,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline_result(period_returns: Vec<f64>) -> BacktestResult {
        let mut equity = vec![10_000.0];
        let mut prev = 10_000.0;
        for r in &period_returns {
            prev *= 1.0 + r;
            equity.push(prev);
        }
        BacktestResult {
            symbol: "X".into(),
            initial_cash: 10_000.0,
            final_cash: prev,
            net_pnl: prev - 10_000.0,
            total_fees: 0.0,
            candles_processed: period_returns.len(),
            signals_emitted: 0,
            orders_filled: 0,
            trades: Vec::new(),
            max_drawdown: 0.0,
            equity_curve: equity,
            period_returns,
            risk_free_rate: 0.0,
            periods_per_year: 252,
        }
    }

    #[test]
    fn sharpe_none_with_one_sample() {
        let r = baseline_result(vec![0.01]);
        assert!(r.sharpe_ratio().is_none());
    }

    #[test]
    fn sharpe_none_with_zero_variance() {
        // All returns identically 0.0 → exact zero stddev (no FP noise).
        let r = baseline_result(vec![0.0; 20]);
        assert!(r.sharpe_ratio().is_none());
    }

    #[test]
    fn sharpe_positive_on_uptrend_with_some_noise() {
        // Mostly-positive returns with a couple negative blips.
        let r = baseline_result(vec![
            0.01, -0.002, 0.012, -0.001, 0.015, 0.008, -0.003, 0.011,
        ]);
        let s = r.sharpe_ratio().unwrap();
        assert!(s > 0.0, "expected positive sharpe, got {s}");
        // Annualised by sqrt(252) — so the scale factor is sensible.
        assert!(s.is_finite());
    }

    #[test]
    fn sortino_only_penalises_downside() {
        // Same returns as sharpe test — sortino should be at least as
        // high as sharpe because it ignores upside variance.
        let r = baseline_result(vec![
            0.01, -0.002, 0.012, -0.001, 0.015, 0.008, -0.003, 0.011,
        ]);
        let sharpe = r.sharpe_ratio().unwrap();
        let sortino = r.sortino_ratio().unwrap();
        assert!(
            sortino >= sharpe - 1e-9,
            "sortino={sortino} sharpe={sharpe}"
        );
    }

    #[test]
    fn sortino_none_when_no_downside() {
        let r = baseline_result(vec![0.01, 0.005, 0.02, 0.001, 0.015]);
        assert!(r.sortino_ratio().is_none());
    }
}
