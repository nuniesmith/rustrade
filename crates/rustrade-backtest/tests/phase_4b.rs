//! Phase 4b integration tests: CSV loader → engine, multi-symbol
//! replay, Sharpe / Sortino on real synthetic price paths.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use rustrade_backtest::{
    Backtest, BacktestConfig, FeeModel, SlippageModel, load_csv_str, sort_chronological,
};
use rustrade_core::{
    Brain, BrainHealth, Candle, Decision, MarketDataEvent, Position, Result as CoreResult,
};
use rustrade_risk::SizingConfig;

// ── Helpers ────────────────────────────────────────────────────────────

/// Brain that goes long when price is rising and short when falling.
/// Decision is per-candle and doesn't pyramid — only flips when the
/// direction reverses.
struct DirectionalBrain {
    state: Mutex<Option<f64>>,
}
impl DirectionalBrain {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(None),
        })
    }
}
#[async_trait]
impl Brain for DirectionalBrain {
    fn name(&self) -> &str {
        "directional"
    }
    async fn on_event(&self, event: &MarketDataEvent, p: &Position) -> CoreResult<Decision> {
        let close = match event {
            MarketDataEvent::Candle { candle, .. } => candle.close,
            _ => return Ok(Decision::hold()),
        };
        let mut st = self.state.lock().unwrap();
        let prev = *st;
        *st = Some(close);
        let Some(prev) = prev else {
            return Ok(Decision::hold());
        };
        if close > prev && p.qty <= 0.0 {
            Ok(Decision::buy(1.0))
        } else if close < prev && p.qty >= 0.0 {
            Ok(Decision::sell(1.0))
        } else {
            Ok(Decision::hold())
        }
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

/// Brain that buys on AAA and sells on BBB once, then holds forever.
/// Used to exercise the multi-symbol routing without flipping.
struct OneShotBrain {
    fired: Mutex<std::collections::HashSet<String>>,
}
impl OneShotBrain {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            fired: Mutex::new(std::collections::HashSet::new()),
        })
    }
}
#[async_trait]
impl Brain for OneShotBrain {
    fn name(&self) -> &str {
        "oneshot"
    }
    async fn on_event(&self, event: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
        let sym = event.symbol().as_str().to_string();
        let mut fired = self.fired.lock().unwrap();
        if fired.contains(&sym) {
            return Ok(Decision::hold());
        }
        fired.insert(sym.clone());
        Ok(match sym.as_str() {
            "AAA" => Decision::buy(1.0),
            "BBB" => Decision::sell(1.0),
            _ => Decision::hold(),
        })
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

fn ramp(start: f64, step: f64, n: usize, t0: i64) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let p = start + step * i as f64;
            Candle {
                time: t0 + i as i64 * 60_000,
                open: p,
                high: p,
                low: p,
                close: p,
                volume: 1.0,
            }
        })
        .collect()
}

// ── Tests ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn csv_loader_feeds_backtest_engine_end_to_end() {
    let csv = "\
time,open,high,low,close,volume
60000,100.0,100.0,100.0,100.0,1.0
120000,101.0,101.0,101.0,101.0,1.0
180000,102.0,102.0,102.0,102.0,1.0
240000,103.0,103.0,103.0,103.0,1.0
300000,104.0,104.0,104.0,104.0,1.0
";
    let candles = load_csv_str(csv).unwrap();
    assert_eq!(candles.len(), 5);

    let result = Backtest::new(
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .fees(FeeModel::Zero)
            .slippage(SlippageModel::Zero)
            .build()
            .unwrap(),
        DirectionalBrain::new(),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap();

    assert_eq!(result.candles_processed, 5);
    // Monotone uptrend → at least one buy signal once a prev exists.
    assert!(result.signals_emitted >= 1);
    assert!(result.equity_curve.len() == 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn sort_chronological_then_run_is_deterministic() {
    let mut candles = ramp(100.0, 1.0, 10, 0);
    candles.reverse(); // newest first → engine needs chronological order
    let candles = sort_chronological(candles);
    assert_eq!(candles[0].time, 0);
    assert_eq!(candles[9].time, 9 * 60_000);

    let result = Backtest::new(
        BacktestConfig::builder().symbol("X").build().unwrap(),
        DirectionalBrain::new(),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap();
    assert_eq!(result.candles_processed, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_symbol_backtest_keeps_per_symbol_state() {
    // Buy AAA once, sell BBB once. After the run we should have two open
    // (opposite-direction) positions on the two symbols — neither closed.
    let result = Backtest::new(
        BacktestConfig::builder()
            .symbols(["AAA", "BBB"])
            .initial_cash(100_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 5_000.0,
                leverage: 1,
                max_contracts: 100,
            })
            .fees(FeeModel::Zero)
            .slippage(SlippageModel::Zero)
            .build()
            .unwrap(),
        OneShotBrain::new(),
    )
    .with_symbol_candles("AAA", ramp(100.0, 0.5, 20, 0))
    .with_symbol_candles("BBB", ramp(200.0, -1.0, 20, 0))
    .run()
    .await
    .unwrap();

    assert_eq!(result.candles_processed, 40);
    assert_eq!(result.orders_filled, 2); // one per symbol
    assert_eq!(result.trades.len(), 0); // never closed
    assert_eq!(result.symbol, "AAA,BBB");
    // Equity curve always seeds initial + one sample per candle.
    assert_eq!(result.equity_curve.len(), 41);
}

#[tokio::test(flavor = "multi_thread")]
async fn merged_stream_is_chronological_across_symbols() {
    // AAA candles at t = 0, 60s, 120s ... ; BBB at t = 30s, 90s, 150s ...
    // Brain just records every event's (time, symbol) pair so we can
    // assert chronological merging.
    let seen = Arc::new(Mutex::new(Vec::<(i64, String)>::new()));
    struct RecordBrain {
        seen: Arc<Mutex<Vec<(i64, String)>>>,
    }
    #[async_trait]
    impl Brain for RecordBrain {
        fn name(&self) -> &str {
            "record"
        }
        async fn on_event(&self, event: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
            if let MarketDataEvent::Candle { candle, symbol, .. } = event {
                self.seen
                    .lock()
                    .unwrap()
                    .push((candle.time, symbol.as_str().to_string()));
            }
            Ok(Decision::hold())
        }
        async fn health(&self) -> BrainHealth {
            BrainHealth::ok()
        }
    }
    let brain = Arc::new(RecordBrain {
        seen: Arc::clone(&seen),
    });

    let aaa: Vec<Candle> = (0..4)
        .map(|i| Candle {
            time: i as i64 * 60_000,
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 0.0,
        })
        .collect();
    let bbb: Vec<Candle> = (0..4)
        .map(|i| Candle {
            time: 30_000 + i as i64 * 60_000,
            open: 200.0,
            high: 200.0,
            low: 200.0,
            close: 200.0,
            volume: 0.0,
        })
        .collect();

    let _ = Backtest::new(
        BacktestConfig::builder()
            .symbols(["AAA", "BBB"])
            .build()
            .unwrap(),
        brain,
    )
    .with_symbol_candles("AAA", aaa)
    .with_symbol_candles("BBB", bbb)
    .run()
    .await
    .unwrap();

    let seen = seen.lock().unwrap();
    let times: Vec<i64> = seen.iter().map(|(t, _)| *t).collect();
    // Must be sorted ascending.
    for w in times.windows(2) {
        assert!(w[0] <= w[1], "out of order: {} > {}", w[0], w[1]);
    }
    // Should be 8 total events alternating AAA / BBB by 30s offset.
    assert_eq!(seen.len(), 8);
    assert_eq!(seen[0].1, "AAA");
    assert_eq!(seen[1].1, "BBB");
    assert_eq!(seen[2].1, "AAA");
}

#[tokio::test(flavor = "multi_thread")]
async fn sharpe_and_sortino_finite_on_real_replay() {
    // Synthetic noisy uptrend so the directional brain produces a mix of
    // wins and losses — gives Sharpe and Sortino a real distribution to
    // chew on.
    let mut candles = Vec::new();
    let mut price = 100.0;
    for i in 0..200 {
        let drift = 0.05;
        let shock = ((i as f64 * 0.37).sin() * 1.5) + ((i as f64 * 0.11).cos() * 0.8);
        price += drift + shock;
        candles.push(Candle {
            time: i as i64 * 60_000,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 1.0,
        });
    }

    let result = Backtest::new(
        BacktestConfig::builder()
            .symbol("X")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 500.0,
                leverage: 1,
                max_contracts: 50,
            })
            .fees(FeeModel::Zero)
            .slippage(SlippageModel::Zero)
            .periods_per_year(252 * 24 * 60) // per-minute
            .build()
            .unwrap(),
        DirectionalBrain::new(),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap();

    let sharpe = result.sharpe_ratio().expect("sharpe should be defined");
    let sortino = result.sortino_ratio().expect("sortino should be defined");
    assert!(sharpe.is_finite(), "sharpe NaN/inf: {sharpe}");
    assert!(sortino.is_finite(), "sortino NaN/inf: {sortino}");
    // Equity curve has 1 + N samples; period returns has N.
    assert_eq!(result.equity_curve.len(), 201);
    assert_eq!(result.period_returns.len(), 200);
}
