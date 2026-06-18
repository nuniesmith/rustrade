//! Backtest simulation of reduce-only OCO brackets — the engine's counterpart
//! of the live `ExecutionService::place_brackets` SL (stop-market) + TP legs.
//!
//! A `Brain` that emits a single `Decision::buy().with_stop().with_take_profit()`
//! and then holds forever can only ever exit via the simulated bracket. These
//! tests pin down that:
//!   * the take-profit leg fills when a later candle's high crosses it,
//!   * the stop-loss leg fills when a later candle's low crosses it,
//!   * when one bar spans BOTH legs the stop fills first (pessimistic), and
//!   * a position whose range never reaches either level stays open.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, SlippageModel};
use rustrade_core::{
    Brain, Candle, Decision, MarketDataEvent, Position, Price, Result as CoreResult,
};
use rustrade_risk::SizingConfig;

// ── Brain: enter once with a fixed bracket, then hold ───────────────────────

struct BracketBrain {
    stop: f64,
    take_profit: f64,
    entered: Mutex<bool>,
}
impl BracketBrain {
    fn new(stop: f64, take_profit: f64) -> Arc<Self> {
        Arc::new(Self { stop, take_profit, entered: Mutex::new(false) })
    }
}
#[async_trait]
impl Brain for BracketBrain {
    fn name(&self) -> &str {
        "bracket-once"
    }
    async fn on_event(&self, event: &MarketDataEvent, position: &Position) -> CoreResult<Decision> {
        if !matches!(event, MarketDataEvent::Candle { .. }) || !position.is_flat() {
            return Ok(Decision::hold());
        }
        let mut entered = self.entered.lock().unwrap();
        if *entered {
            return Ok(Decision::hold()); // never re-enter after the first exit
        }
        *entered = true;
        Ok(Decision::buy(0.9)
            .with_stop(Price(self.stop))
            .with_take_profit(Price(self.take_profit)))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn c(i: i64, open: f64, high: f64, low: f64, close: f64) -> Candle {
    Candle { time: i * 60_000, open, high, low, close, volume: 1.0 }
}

/// Build the backtest: price ~100 with margin 1000 / 1× / cv 1.0 → 10 contracts
/// per entry, zero fees + zero slippage so exit prices land exactly on the legs.
async fn run(stop: f64, tp: f64, candles: Vec<Candle>) -> rustrade_backtest::BacktestResult {
    Backtest::new(
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig { margin_per_trade: 1_000.0, leverage: 1, max_contracts: 100 })
            .fees(FeeModel::Zero)
            .slippage(SlippageModel::Zero)
            .build()
            .unwrap(),
        BracketBrain::new(stop, tp),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn take_profit_leg_fills_when_high_crosses() {
    // Enter at 100 (c0 close); c1 high 104 crosses the 103 take-profit.
    let r = run(98.0, 103.0, vec![
        c(0, 100.0, 100.0, 100.0, 100.0),
        c(1, 100.0, 104.0, 100.0, 102.0),
        c(2, 102.0, 102.0, 101.0, 101.0),
    ])
    .await;

    assert_eq!(r.trades.len(), 1, "exactly one closed trade");
    assert_eq!(r.orders_filled, 2, "one entry + one bracket exit");
    let t = &r.trades[0];
    assert!((t.entry_price - 100.0).abs() < 1e-9);
    assert!((t.exit_price - 103.0).abs() < 1e-9, "TP fill at the leg price");
    assert!(t.gross_pnl > 0.0, "take-profit is a win, got {}", t.gross_pnl);
    // 10 contracts × (103 - 100) = +30, zero fees.
    assert!((r.net_pnl - 30.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_leg_fills_when_low_crosses() {
    // Enter at 100; c1 low 97 crosses the 98 stop.
    let r = run(98.0, 103.0, vec![
        c(0, 100.0, 100.0, 100.0, 100.0),
        c(1, 100.0, 100.0, 97.0, 98.0),
    ])
    .await;

    assert_eq!(r.trades.len(), 1);
    assert_eq!(r.orders_filled, 2);
    let t = &r.trades[0];
    assert!((t.exit_price - 98.0).abs() < 1e-9, "stop fill at the leg price");
    assert!(t.gross_pnl < 0.0, "stop-loss is a loss, got {}", t.gross_pnl);
    assert!((r.net_pnl + 20.0).abs() < 1e-9, "net_pnl={}", r.net_pnl); // 10 × (98-100)
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_fills_first_when_one_bar_spans_both_legs() {
    // c1 low 96 crosses the stop AND high 105 crosses the TP — pessimistic:
    // the stop fills first, so this is booked as a loss at 98.
    let r = run(98.0, 103.0, vec![
        c(0, 100.0, 100.0, 100.0, 100.0),
        c(1, 100.0, 105.0, 96.0, 100.0),
    ])
    .await;

    assert_eq!(r.trades.len(), 1);
    let t = &r.trades[0];
    assert!((t.exit_price - 98.0).abs() < 1e-9, "spanning bar resolves to the STOP");
    assert!(t.gross_pnl < 0.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn position_stays_open_when_neither_leg_is_touched() {
    // Range stays strictly between stop (98) and TP (103) → no bracket fill.
    let r = run(98.0, 103.0, vec![
        c(0, 100.0, 100.0, 100.0, 100.0),
        c(1, 100.0, 102.0, 99.0, 101.0),
        c(2, 101.0, 102.5, 99.5, 100.0),
    ])
    .await;

    assert!(r.trades.is_empty(), "no exit → no closed trade");
    assert_eq!(r.orders_filled, 1, "only the entry filled");
}
