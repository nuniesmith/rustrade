//! Property-based invariants for the replay engine.
//!
//! The position-accounting math (weighted-average entry, partial closes,
//! flips, fee apportioning) is exactly where subtle bugs hide. Rather
//! than enumerate cases by hand, these tests drive the engine with
//! randomly-generated candle series and decision streams and assert the
//! invariants that must hold for *any* input:
//!
//! 1. No `NaN`/`inf` ever leaks into the result (equity curve, returns,
//!    PnL, drawdown).
//! 2. Aggregation is consistent: `net_pnl == Σ trade.net_pnl`,
//!    `total_fees == Σ trade.fee`, `final_cash == initial + net_pnl`.
//! 3. Shapes line up: one equity sample per candle (plus the seed), one
//!    return per candle.
//! 4. Drawdown is never positive.
//! 5. The run is deterministic — identical input yields a bit-identical
//!    equity curve and PnL across two runs.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use proptest::prelude::*;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, SlippageModel};
use rustrade_core::{Brain, BrainHealth, Candle, Decision, MarketDataEvent, Position, Result};
use rustrade_risk::SizingConfig;

/// Brain that replays a fixed opcode stream: one decision per `on_event`
/// call, cycling if the engine asks for more than were generated.
/// 0 = Hold, 1 = Buy, 2 = Sell, 3 = Close.
struct ScriptedBrain {
    opcodes: Vec<u8>,
    idx: Mutex<usize>,
}

#[async_trait]
impl Brain for ScriptedBrain {
    fn name(&self) -> &str {
        "scripted"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> Result<Decision> {
        let mut i = self.idx.lock().unwrap();
        let op = self.opcodes[*i % self.opcodes.len()];
        *i += 1;
        Ok(match op {
            1 => Decision::buy(1.0),
            2 => Decision::sell(1.0),
            3 => Decision::close(),
            _ => Decision::hold(),
        })
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

fn build_candles(prices: &[f64]) -> Vec<Candle> {
    prices
        .iter()
        .enumerate()
        .map(|(i, &p)| Candle {
            time: i as i64 * 60_000,
            open: p,
            high: p,
            low: p,
            close: p,
            volume: 1.0,
        })
        .collect()
}

fn cfg() -> BacktestConfig {
    BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(1_000_000.0)
        .sizing(SizingConfig {
            margin_per_trade: 1_000.0,
            leverage: 1,
            max_contracts: 100,
        })
        // Non-zero fees + slippage so those code paths are exercised too.
        .fees(FeeModel::Flat(0.0005))
        .slippage(SlippageModel::FixedBps(2.0))
        .build()
        .unwrap()
}

async fn run_once(prices: Vec<f64>, opcodes: Vec<u8>) -> rustrade_backtest::BacktestResult {
    let brain = Arc::new(ScriptedBrain {
        opcodes,
        idx: Mutex::new(0),
    });
    Backtest::new(cfg(), brain)
        .with_candles(build_candles(&prices))
        .run()
        .await
        .unwrap()
}

proptest! {
    // 64 cases keeps the suite fast while still covering a wide spread of
    // price paths and decision sequences.
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn engine_invariants_hold_for_random_runs(
        seq in prop::collection::vec((1.0f64..10_000.0, 0u8..4u8), 1..60)
    ) {
        let prices: Vec<f64> = seq.iter().map(|(p, _)| *p).collect();
        let opcodes: Vec<u8> = seq.iter().map(|(_, o)| *o).collect();
        let n = prices.len();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let r = rt.block_on(run_once(prices.clone(), opcodes.clone()));

        // (1) No NaN/inf anywhere.
        prop_assert!(r.net_pnl.is_finite(), "net_pnl not finite: {}", r.net_pnl);
        prop_assert!(r.total_fees.is_finite());
        prop_assert!(r.final_cash.is_finite());
        prop_assert!(r.max_drawdown.is_finite());
        prop_assert!(r.equity_curve.iter().all(|e| e.is_finite()));
        prop_assert!(r.period_returns.iter().all(|x| x.is_finite()));

        // (2) Aggregation consistency.
        let sum_net: f64 = r.trades.iter().map(|t| t.net_pnl()).sum();
        let sum_fee: f64 = r.trades.iter().map(|t| t.fee).sum();
        prop_assert!((r.net_pnl - sum_net).abs() < 1e-6, "net_pnl {} vs Σtrades {}", r.net_pnl, sum_net);
        prop_assert!((r.total_fees - sum_fee).abs() < 1e-6);
        prop_assert!((r.final_cash - (r.initial_cash + r.net_pnl)).abs() < 1e-6);

        // (3) Shapes.
        prop_assert_eq!(r.candles_processed, n);
        prop_assert_eq!(r.equity_curve.len(), n + 1);
        prop_assert_eq!(r.period_returns.len(), n);

        // (4) Drawdown never positive.
        prop_assert!(r.max_drawdown <= 1e-9, "drawdown positive: {}", r.max_drawdown);

        // (5) Determinism: a second identical run is bit-for-bit equal.
        let r2 = rt.block_on(run_once(prices, opcodes));
        prop_assert_eq!(&r.equity_curve, &r2.equity_curve);
        prop_assert_eq!(r.net_pnl.to_bits(), r2.net_pnl.to_bits());
        prop_assert_eq!(r.total_fees.to_bits(), r2.total_fees.to_bits());
    }
}
