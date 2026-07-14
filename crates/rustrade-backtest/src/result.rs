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
    /// Total realised PnL net of fees and (when a
    /// [`FundingModel`](crate::FundingModel) is configured) funding
    /// cashflows — the per-trade nets plus funding settled on positions
    /// still open at the end of the run.
    pub net_pnl: f64,
    /// Sum of fees charged across every fill.
    pub total_fees: f64,
    /// Total perp funding *collected* across the run, in quote currency
    /// (`>= 0`). Includes settlements on positions still open at the end.
    /// Always `0.0` with [`FundingModel::None`](crate::FundingModel::None)
    /// (the default). `#[serde(default)]` so previously serialized
    /// results still deserialize.
    #[serde(default)]
    pub funding_received: f64,
    /// Total perp funding *paid* across the run, in quote currency,
    /// stored as a positive number. Always `0.0` with funding off.
    #[serde(default)]
    pub funding_paid: f64,
    /// Number of candles fed to the brain.
    pub candles_processed: usize,
    /// Number of non-`Hold` decisions emitted by the brain.
    pub signals_emitted: usize,
    /// Number of orders the engine placed (may be `< signals_emitted`
    /// if the sizer returned 0 for some signals).
    pub orders_filled: usize,
    /// Number of non-`Hold` decisions blocked by a risk gate (the
    /// session-PnL halt or the circuit breaker — see
    /// [`crate::BacktestConfig::session_pnl`] /
    /// [`crate::BacktestConfig::circuit_breaker`]). Always `0` when no
    /// gate is configured.
    #[serde(default)]
    pub orders_blocked: usize,
    /// Per-trade outcomes, in chronological order.
    pub trades: Vec<TradeOutcome>,
    /// Maximum peak-to-trough drawdown of equity (cash) over the run,
    /// in quote currency. Always `<= 0`.
    pub max_drawdown: f64,
    /// Portfolio equity at each sample point. The first element is
    /// [`Self::initial_cash`]; one additional sample is appended per
    /// candle in the merged event stream.
    pub equity_curve: Vec<f64>,
    /// Candle timestamp (epoch ms — the same unit as
    /// [`Candle::time`](rustrade_core::Candle)) of each
    /// [`Self::equity_curve`] sample. The seed sample borrows the first
    /// candle's timestamp. Empty for empty runs and for results
    /// serialized before this field shipped (`#[serde(default)]`) — see
    /// [`Self::equity_points`].
    #[serde(default)]
    pub equity_times: Vec<i64>,
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

/// One timestamped sample of the portfolio equity curve — the export
/// shape for plotting / persisting a run (see
/// [`BacktestResult::equity_points`]).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EquityPoint {
    /// Candle timestamp of the sample, epoch milliseconds (the same unit
    /// as [`Candle::time`](rustrade_core::Candle)).
    pub time: i64,
    /// Portfolio equity (cash + unrealised PnL) at the sample, in quote
    /// currency.
    pub equity: f64,
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

    /// Net funding cashflow over the run:
    /// [`Self::funding_received`] − [`Self::funding_paid`]. Positive =
    /// the run collected more funding than it paid. `0.0` with funding
    /// modelling off.
    pub fn net_funding(&self) -> f64 {
        self.funding_received - self.funding_paid
    }

    /// The equity curve as timestamped points, zipping
    /// [`Self::equity_times`] with [`Self::equity_curve`]. Empty for
    /// empty runs and for results deserialized from versions that
    /// predate [`Self::equity_times`] (the timestamps are unknowable
    /// there — fall back to [`Self::equity_curve`] indices).
    pub fn equity_points(&self) -> Vec<EquityPoint> {
        self.equity_times
            .iter()
            .zip(self.equity_curve.iter())
            .map(|(&time, &equity)| EquityPoint { time, equity })
            .collect()
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

    /// Mean net PnL per trade across **all** trades (wins, losses and
    /// breakevens) — the classic per-trade expectancy. `None` when the
    /// run closed no trades.
    pub fn expectancy(&self) -> Option<f64> {
        if self.trades.is_empty() {
            return None;
        }
        let total: f64 = self.trades.iter().map(TradeOutcome::net_pnl).sum();
        Some(total / self.trades.len() as f64)
    }

    /// Mean net PnL of winning trades (`> 0`). `None` when there are no
    /// winning trades.
    pub fn avg_win(&self) -> Option<f64> {
        self.mean_net_where(Outcome::Win)
    }

    /// Mean net PnL of losing trades — a **negative** number (the sign
    /// is kept so `expectancy ≈ win_rate·avg_win + loss_rate·avg_loss`
    /// holds without juggling signs). `None` when there are no losing
    /// trades.
    pub fn avg_loss(&self) -> Option<f64> {
        self.mean_net_where(Outcome::Loss)
    }

    fn mean_net_where(&self, outcome: Outcome) -> Option<f64> {
        let mut total = 0.0;
        let mut n = 0usize;
        for t in self.trades.iter().filter(|t| t.outcome() == outcome) {
            total += t.net_pnl();
            n += 1;
        }
        if n == 0 { None } else { Some(total / n as f64) }
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
        let fmt_opt = |v: Option<f64>| v.map(|x| format!("{x:.4}")).unwrap_or_else(|| "n/a".into());
        let expectancy = fmt_opt(self.expectancy());
        let avg_win = fmt_opt(self.avg_win());
        let avg_loss = fmt_opt(self.avg_loss());
        format!(
            "Backtest [{}]\n\
             ├ candles_processed: {}\n\
             ├ signals / orders : {} / {} ({} risk-blocked)\n\
             ├ trades           : {} (W {} / L {} / BE {})\n\
             ├ win_rate         : {:.2}%\n\
             ├ expectancy       : {expectancy}\n\
             ├ avg win / loss   : {avg_win} / {avg_loss}\n\
             ├ profit_factor    : {pf}\n\
             ├ sharpe / sortino : {sharpe} / {sortino}\n\
             ├ total_return     : {:.4}%\n\
             ├ net_pnl          : {:.4}\n\
             ├ total_fees       : {:.4}\n\
             ├ funding recv/paid: {:.4} / {:.4} (net {:.4})\n\
             ├ max_drawdown     : {:.4}\n\
             └ final_cash       : {:.4}",
            self.symbol,
            self.candles_processed,
            self.signals_emitted,
            self.orders_filled,
            self.orders_blocked,
            self.trades.len(),
            self.wins(),
            self.losses(),
            self.breakevens(),
            self.win_rate() * 100.0,
            self.total_return_pct(),
            self.net_pnl,
            self.total_fees,
            self.funding_received,
            self.funding_paid,
            self.net_funding(),
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
            funding_received: 0.0,
            funding_paid: 0.0,
            candles_processed: period_returns.len(),
            signals_emitted: 0,
            orders_filled: 0,
            orders_blocked: 0,
            trades: Vec::new(),
            max_drawdown: 0.0,
            equity_curve: equity,
            equity_times: Vec::new(),
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

    // ── Trade-stat math (expectancy / avg win / avg loss) ──────────────

    /// A trade whose net PnL is exactly `net` (gross = net, no fee, no
    /// funding).
    fn trade(net: f64) -> TradeOutcome {
        TradeOutcome {
            symbol: "X".into(),
            close_side: rustrade_core::Side::Sell,
            qty: 1.0,
            entry_price: 100.0,
            exit_price: 100.0 + net,
            gross_pnl: net,
            fee: 0.0,
            funding: 0.0,
            closed_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn trade_stats_on_known_trade_set() {
        let mut r = baseline_result(vec![0.0; 4]);
        r.trades = vec![trade(10.0), trade(20.0), trade(-5.0), trade(0.0)];

        assert_eq!(r.wins(), 2);
        assert_eq!(r.losses(), 1);
        assert_eq!(r.breakevens(), 1);
        assert!((r.avg_win().unwrap() - 15.0).abs() < 1e-12);
        assert!((r.avg_loss().unwrap() - (-5.0)).abs() < 1e-12);
        // Expectancy over ALL 4 trades: (10 + 20 - 5 + 0) / 4 = 6.25.
        assert!((r.expectancy().unwrap() - 6.25).abs() < 1e-12);
        // Decomposition identity: N·E = W·avg_win + L·avg_loss.
        let n = r.trades.len() as f64;
        let lhs = n * r.expectancy().unwrap();
        let rhs =
            r.wins() as f64 * r.avg_win().unwrap() + r.losses() as f64 * r.avg_loss().unwrap();
        assert!((lhs - rhs).abs() < 1e-12);
    }

    #[test]
    fn trade_stats_none_without_matching_trades() {
        let r = baseline_result(vec![0.0; 2]);
        assert!(r.expectancy().is_none());
        assert!(r.avg_win().is_none());
        assert!(r.avg_loss().is_none());

        let mut winners_only = baseline_result(vec![0.0; 2]);
        winners_only.trades = vec![trade(3.0)];
        assert!(winners_only.avg_win().is_some());
        assert!(winners_only.avg_loss().is_none());
        assert!((winners_only.expectancy().unwrap() - 3.0).abs() < 1e-12);
    }

    #[test]
    fn trade_net_includes_funding() {
        // gross 5, fee 1, funding +2 → net 6; funding flips a loser into
        // a winner when it covers the deficit.
        let mut t = trade(5.0);
        t.fee = 1.0;
        t.funding = 2.0;
        assert!((t.net_pnl() - 6.0).abs() < 1e-12);

        let mut r = baseline_result(vec![0.0; 1]);
        r.trades = vec![t];
        assert_eq!(r.wins(), 1);
        assert!((r.expectancy().unwrap() - 6.0).abs() < 1e-12);
    }

    // ── Equity-curve export ─────────────────────────────────────────────

    #[test]
    fn equity_points_zip_times_with_curve() {
        let mut r = baseline_result(vec![0.01, -0.01]);
        // baseline_result builds a 3-sample curve; timestamp them.
        r.equity_times = vec![1_000, 1_000, 2_000];
        let points = r.equity_points();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].time, 1_000);
        assert_eq!(points[2].time, 2_000);
        assert_eq!(points[1].equity, r.equity_curve[1]);
    }

    #[test]
    fn equity_points_empty_without_timestamps() {
        // Results deserialized from pre-`equity_times` versions have an
        // empty times vec — the export degrades to empty, not garbage.
        let r = baseline_result(vec![0.01, -0.01]);
        assert!(r.equity_times.is_empty());
        assert!(r.equity_points().is_empty());
    }

    #[test]
    fn net_funding_is_received_minus_paid() {
        let mut r = baseline_result(vec![0.0]);
        r.funding_received = 3.5;
        r.funding_paid = 1.25;
        assert!((r.net_funding() - 2.25).abs() < 1e-12);
    }
}
