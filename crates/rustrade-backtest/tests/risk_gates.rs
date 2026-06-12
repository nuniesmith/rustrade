//! Risk-gate parity in the backtest engine.
//!
//! The live `ExecutionService` gates every non-`Hold` decision behind the
//! per-symbol session-PnL halt and circuit breaker. These tests pin that
//! the replay engine reproduces that behaviour when the same configs are
//! threaded through `BacktestConfig` — driven by candle time, so halts,
//! rollovers, and cooldowns happen in replay time deterministically.

use std::sync::Arc;

use async_trait::async_trait;
use rustrade_backtest::{Backtest, BacktestConfig};
use rustrade_core::{
    Brain, BrainHealth, Candle, Decision, MarketDataEvent, Position, Result as CoreResult,
};
use rustrade_risk::{CircuitBreakerConfig, SessionPnlConfig, SizingConfig};

/// Buys when flat, closes when long — one round trip every two candles.
/// On a falling series every round trip realises a loss.
struct ChurnBrain;
#[async_trait]
impl Brain for ChurnBrain {
    fn name(&self) -> &str {
        "churn"
    }
    async fn on_event(&self, _e: &MarketDataEvent, p: &Position) -> CoreResult<Decision> {
        if p.qty == 0.0 {
            Ok(Decision::buy(1.0))
        } else {
            Ok(Decision::close())
        }
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

/// A falling series: one candle per `step_ms`, price dropping 1.0 each
/// candle from `start`. Flat OHLC so fills land exactly on the close.
fn down_series(n: usize, start: f64, t0_ms: i64, step_ms: i64) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let p = start - i as f64;
            Candle {
                time: t0_ms + i as i64 * step_ms,
                open: p,
                high: p,
                low: p,
                close: p,
                volume: 1.0,
            }
        })
        .collect()
}

fn base_cfg() -> rustrade_backtest::BacktestConfigBuilder {
    BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(100_000.0)
        // 10 contracts per entry at price ~100 → each losing round trip
        // realises ≈ -10 (price falls 1.0 between entry and close).
        .sizing(SizingConfig {
            margin_per_trade: 1_000.0,
            leverage: 1,
            max_contracts: 100,
        })
        .fees(rustrade_backtest::FeeModel::Zero)
}

#[tokio::test]
async fn session_halt_stops_trading_at_the_loss_limit() {
    // Each round trip loses ~10. With loss_limit -25 the third loss
    // (net -30) halts the session; every later decision is blocked.
    let gated = Backtest::new(
        base_cfg()
            .session_pnl(SessionPnlConfig { loss_limit: -25.0 })
            .build()
            .unwrap(),
        Arc::new(ChurnBrain),
    )
    .with_candles(down_series(20, 100.0, 0, 60_000))
    .run()
    .await
    .unwrap();

    assert_eq!(
        gated.trades.len(),
        3,
        "the 3rd loss crosses -25 and halts: {:#?}",
        gated.trades
    );
    assert!(
        gated.orders_blocked > 0,
        "post-halt decisions must be counted as blocked"
    );
    // 3 round trips × ~-10 each; nothing after the halt.
    assert!(
        gated.net_pnl <= -25.0 && gated.net_pnl > -45.0,
        "loss must stop near the cap, got {}",
        gated.net_pnl
    );

    // Without the gate the same brain keeps churning losses all the way.
    let ungated = Backtest::new(base_cfg().build().unwrap(), Arc::new(ChurnBrain))
        .with_candles(down_series(20, 100.0, 0, 60_000))
        .run()
        .await
        .unwrap();
    assert!(ungated.trades.len() > gated.trades.len());
    assert_eq!(
        ungated.orders_blocked, 0,
        "no gates configured → never blocked"
    );
    assert!(
        ungated.net_pnl < gated.net_pnl,
        "the gate must cap the bleed"
    );
}

#[tokio::test]
async fn session_halt_rolls_over_at_utc_midnight_in_replay_time() {
    // Day 1 (just before 00:00 UTC): two losing round trips (-20) halt a
    // -15 session. Day 2 candles must trade again — the rollover happens
    // at the *candle* timestamps, not wall time.
    const DAY_MS: i64 = 86_400_000;
    let mut candles = down_series(6, 100.0, DAY_MS - 6 * 60_000, 60_000); // day 1
    candles.extend(down_series(6, 94.0, DAY_MS + 60_000, 60_000)); // day 2

    let result = Backtest::new(
        base_cfg()
            .session_pnl(SessionPnlConfig { loss_limit: -15.0 })
            .build()
            .unwrap(),
        Arc::new(ChurnBrain),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap();

    // Day 1: trips 1+2 complete (-20 ≤ -15 halts on the 2nd), the rest of
    // day 1 is blocked. Day 2: a fresh session — trading resumes and at
    // least one more round trip completes.
    assert!(
        result.trades.len() >= 3,
        "day-2 candles must trade after the UTC rollover: {:#?}",
        result.trades
    );
    assert!(
        result.orders_blocked > 0,
        "day-1 tail must have been blocked"
    );
    let last_close = result.trades.last().unwrap().closed_at.timestamp_millis();
    assert!(
        last_close >= DAY_MS,
        "the last trade must come from day 2 (got t={last_close})"
    );
}

#[tokio::test]
async fn circuit_breaker_trips_then_resumes_after_cooldown() {
    // Breaker: 2 losses in the window → trip, 1h cooldown. The churn
    // brain loses twice (4 candles), is blocked while tripped, and the
    // gap in the series past the cooldown lets it trade again.
    let mut candles = down_series(8, 100.0, 0, 60_000); // losses + blocked tail
    // Resume 2h later (cooldown expired in replay time).
    candles.extend(down_series(4, 90.0, 2 * 3_600_000, 60_000));

    let result = Backtest::new(
        base_cfg()
            .circuit_breaker(CircuitBreakerConfig {
                loss_limit: 2,
                window_secs: 86_400,
                cooldown_secs: 3_600,
            })
            .build()
            .unwrap(),
        Arc::new(ChurnBrain),
    )
    .with_candles(candles)
    .run()
    .await
    .unwrap();

    assert!(
        result.orders_blocked > 0,
        "decisions while tripped must be blocked"
    );
    assert!(
        result.trades.len() > 2,
        "trading must resume after the cooldown: {:#?}",
        result.trades
    );
    let last_close = result.trades.last().unwrap().closed_at.timestamp_millis();
    assert!(
        last_close >= 2 * 3_600_000,
        "the last trade must come from the post-cooldown window (got t={last_close})"
    );
}

#[tokio::test]
async fn gates_block_close_decisions_too_matching_live() {
    // Live gates run before build_order, so a halted session blocks Close
    // as well. Halt while still holding a position (via a partial close
    // whose realised loss crosses the cap): the remaining position stays
    // open and every later Close decision counts as blocked.
    struct PartialThenClose;
    #[async_trait]
    impl Brain for PartialThenClose {
        fn name(&self) -> &str {
            "partial-then-close"
        }
        async fn on_event(&self, e: &MarketDataEvent, p: &Position) -> CoreResult<Decision> {
            let MarketDataEvent::Candle { candle, .. } = e else {
                return Ok(Decision::hold());
            };
            match candle.time / 60_000 {
                // Open 10 long at 100.
                0 => Ok(Decision::buy(1.0)),
                // Partially close 5 at 99 → realised -5 ≤ -5 halts, but
                // 5 contracts are still open.
                1 => Ok(
                    Decision::sell(1.0).with_size_hint(rustrade_core::SizeHint::Quantity(
                        rustrade_core::Volume(5.0),
                    )),
                ),
                // From here every Close attempt must be blocked.
                _ if p.qty != 0.0 => Ok(Decision::close()),
                _ => Ok(Decision::hold()),
            }
        }
        async fn health(&self) -> BrainHealth {
            BrainHealth::ok()
        }
    }

    let result = Backtest::new(
        base_cfg()
            .session_pnl(SessionPnlConfig { loss_limit: -5.0 })
            .build()
            .unwrap(),
        Arc::new(PartialThenClose),
    )
    .with_candles(down_series(8, 100.0, 0, 60_000))
    .run()
    .await
    .unwrap();

    // The partial close is the only realised trade; the halt then blocks
    // every Close attempt on the still-open 5 contracts (candles 2..7).
    assert_eq!(result.trades.len(), 1, "{:#?}", result.trades);
    assert_eq!(result.trades[0].qty, 5.0);
    assert!(
        result.orders_blocked >= 6,
        "every post-halt Close must be blocked, got {}",
        result.orders_blocked
    );
}

#[tokio::test]
async fn no_gates_configured_is_unchanged_and_never_blocks() {
    let result = Backtest::new(base_cfg().build().unwrap(), Arc::new(ChurnBrain))
        .with_candles(down_series(10, 100.0, 0, 60_000))
        .run()
        .await
        .unwrap();
    assert_eq!(result.orders_blocked, 0);
    assert_eq!(result.trades.len(), 5, "5 uninterrupted round trips");
}
