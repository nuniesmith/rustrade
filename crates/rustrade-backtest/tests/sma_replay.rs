//! End-to-end backtest of an SMA-crossover `Brain` against a synthetic
//! sinusoidal candle series. Demonstrates the brain-identical guarantee:
//! the same `impl Brain` works in `rustrade-backtest` exactly as it does
//! in the live `rustrade::Bot`.
//!
//! Mirrors `examples/sma-cross-bot/` but runs the brain offline through
//! the deterministic engine — no broadcast bus, no async services, no
//! supervisor.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, SlippageModel};
use rustrade_core::{Brain, Candle, Decision, MarketDataEvent, Position, Result as CoreResult};
use rustrade_risk::SizingConfig;

// ── Brain ──────────────────────────────────────────────────────────────

const FAST: usize = 5;
const SLOW: usize = 20;

struct SmaCrossBrain {
    state: Mutex<SmaState>,
}
#[derive(Default)]
struct SmaState {
    closes: Vec<f64>,
    last_fast_above: Option<bool>,
}
impl SmaCrossBrain {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SmaState::default()),
        })
    }
}

fn sma(window: &[f64], n: usize) -> Option<f64> {
    if window.len() < n {
        return None;
    }
    Some(window[window.len() - n..].iter().sum::<f64>() / n as f64)
}

#[async_trait]
impl Brain for SmaCrossBrain {
    fn name(&self) -> &str {
        "sma-cross"
    }
    async fn on_event(&self, event: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
        let close = match event {
            MarketDataEvent::Candle { candle, .. } => candle.close,
            _ => return Ok(Decision::hold()),
        };
        let mut st = self.state.lock().unwrap();
        st.closes.push(close);
        if st.closes.len() > SLOW * 2 {
            let drop = st.closes.len() - SLOW * 2;
            st.closes.drain(0..drop);
        }
        let (Some(fast), Some(slow)) = (sma(&st.closes, FAST), sma(&st.closes, SLOW)) else {
            return Ok(Decision::hold());
        };
        let fast_above = fast > slow;
        let decision = match st.last_fast_above {
            Some(prev) if prev != fast_above => {
                if fast_above {
                    Decision::buy(0.9)
                } else {
                    Decision::sell(0.9)
                }
            }
            _ => Decision::hold(),
        };
        st.last_fast_above = Some(fast_above);
        Ok(decision)
    }
}

// ── Series ─────────────────────────────────────────────────────────────

fn sinusoidal_candles(n: usize) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let close = 100.0 + 10.0 * (i as f64 * 0.15).sin();
            Candle {
                time: i as i64 * 60_000,
                open: close,
                high: close,
                low: close,
                close,
                volume: 1.0,
            }
        })
        .collect()
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn sma_cross_replay_produces_trades_on_sinusoid() {
    let result = Backtest::new(
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .slippage(SlippageModel::Zero)
            .fees(FeeModel::Zero)
            .build()
            .unwrap(),
        SmaCrossBrain::new(),
    )
    .with_candles(sinusoidal_candles(200))
    .run()
    .await
    .unwrap();

    // Brain emits buy on every up-cross, sell on every down-cross. The
    // 200-candle sinusoid produces several full cycles → at least one
    // closed round-trip.
    assert!(
        result.signals_emitted >= 4,
        "expected >= 4 brain signals, got {}",
        result.signals_emitted
    );
    assert!(
        !result.trades.is_empty(),
        "expected at least one closed trade"
    );
    // With zero fees and zero slippage, total_fees must be 0.
    assert!((result.total_fees - 0.0).abs() < 1e-12);
    // Equity high-water mark logic: max_drawdown is never positive.
    assert!(result.max_drawdown <= 0.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn deterministic_replay_same_brain_same_series() {
    let candles = sinusoidal_candles(200);
    let make_cfg = || {
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .fees(FeeModel::Flat(0.0005))
            .slippage(SlippageModel::FixedBps(2.0))
            .build()
            .unwrap()
    };

    let r1 = Backtest::new(make_cfg(), SmaCrossBrain::new())
        .with_candles(candles.clone())
        .run()
        .await
        .unwrap();
    let r2 = Backtest::new(make_cfg(), SmaCrossBrain::new())
        .with_candles(candles)
        .run()
        .await
        .unwrap();

    assert_eq!(r1.signals_emitted, r2.signals_emitted);
    assert_eq!(r1.orders_filled, r2.orders_filled);
    assert_eq!(r1.trades.len(), r2.trades.len());
    assert!((r1.net_pnl - r2.net_pnl).abs() < 1e-9);
    assert!((r1.total_fees - r2.total_fees).abs() < 1e-9);
    assert!((r1.max_drawdown - r2.max_drawdown).abs() < 1e-9);
}

#[tokio::test(flavor = "multi_thread")]
async fn slippage_reduces_pnl_vs_zero_baseline() {
    let candles = sinusoidal_candles(200);
    let base_cfg = |slip: SlippageModel| {
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .slippage(slip)
            .fees(FeeModel::Zero)
            .build()
            .unwrap()
    };

    let baseline = Backtest::new(base_cfg(SlippageModel::Zero), SmaCrossBrain::new())
        .with_candles(candles.clone())
        .run()
        .await
        .unwrap();
    let with_slip = Backtest::new(
        base_cfg(SlippageModel::FixedBps(10.0)),
        SmaCrossBrain::new(),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap();

    // Same brain, same series, same fees (zero), only slippage differs.
    // Slippage is symmetric adverse → with-slippage PnL ≤ baseline PnL.
    assert!(
        with_slip.net_pnl <= baseline.net_pnl + 1e-9,
        "slippage should not improve realised PnL: baseline={}, with_slip={}",
        baseline.net_pnl,
        with_slip.net_pnl
    );
    assert_eq!(with_slip.trades.len(), baseline.trades.len());
}
