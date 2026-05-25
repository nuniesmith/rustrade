//! Backtest result — aggregated metrics + the full trade ledger.

use serde::{Deserialize, Serialize};

use crate::metrics::{Outcome, TradeOutcome};

/// Final outcome of a [`crate::Backtest::run`] call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    /// Symbol the backtest was configured for.
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
}

impl BacktestResult {
    pub fn total_return_pct(&self) -> f64 {
        if self.initial_cash == 0.0 {
            0.0
        } else {
            (self.net_pnl / self.initial_cash) * 100.0
        }
    }

    pub fn wins(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Win)
            .count()
    }

    pub fn losses(&self) -> usize {
        self.trades
            .iter()
            .filter(|t| t.outcome() == Outcome::Loss)
            .count()
    }

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

    /// Pretty-printed multi-line summary suitable for logging.
    pub fn summary(&self) -> String {
        let pf = self
            .profit_factor()
            .map(|p| format!("{p:.3}"))
            .unwrap_or_else(|| "∞ (no losses)".into());
        format!(
            "Backtest [{}]\n\
             ├ candles_processed: {}\n\
             ├ signals / orders : {} / {}\n\
             ├ trades           : {} (W {} / L {} / BE {})\n\
             ├ win_rate         : {:.2}%\n\
             ├ profit_factor    : {pf}\n\
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
