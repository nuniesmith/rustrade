//! Perp funding-model integration tests.
//!
//! Pins down the four sign cases (long/short × positive/negative rate),
//! the settlement-window boundaries (entry-exclusive, exit-inclusive —
//! matching the live paper ledger's `(entry, exit]` accrual), the
//! historical-series vs constant-grid fallback, trade attribution of
//! accrued funding, the timestamped equity-curve export, and — most
//! importantly for existing consumers — that results with funding OFF
//! are bit-identical to a config that never mentions funding.

use std::sync::Arc;

use async_trait::async_trait;
use rustrade_backtest::{
    Backtest, BacktestConfig, BacktestResult, EIGHT_HOURS_MS, Error, FeeModel, FundingModel,
    SlippageModel,
};
use rustrade_core::{
    Brain, Candle, Decision, MarketDataEvent, Position, Result as CoreResult, SizeHint, Volume,
};
use rustrade_risk::SizingConfig;

const HOUR_MS: i64 = 3_600_000;

// ── Brains ──────────────────────────────────────────────────────────────────

/// Opens once (long or short) on the candle at `open_at`, optionally closes
/// on the candle at `close_at`, and holds otherwise. Never re-enters.
struct OpenCloseBrain {
    long: bool,
    open_at: i64,
    close_at: Option<i64>,
}
#[async_trait]
impl Brain for OpenCloseBrain {
    fn name(&self) -> &str {
        "open-close"
    }
    async fn on_event(&self, event: &MarketDataEvent, position: &Position) -> CoreResult<Decision> {
        let MarketDataEvent::Candle { candle, .. } = event else {
            return Ok(Decision::hold());
        };
        if position.is_flat() {
            if candle.time == self.open_at {
                return Ok(if self.long {
                    Decision::buy(1.0)
                } else {
                    Decision::sell(1.0)
                });
            }
        } else if Some(candle.time) == self.close_at {
            return Ok(Decision::close());
        }
        Ok(Decision::hold())
    }
}

/// Opens long at `open_at`, scales out `partial_qty` contracts at
/// `partial_at`, and closes the rest at `close_at`. Drives the pro-rata
/// funding-attribution branch that only partial closes exercise.
struct ScaleOutBrain {
    open_at: i64,
    partial_at: i64,
    partial_qty: f64,
    close_at: i64,
}
#[async_trait]
impl Brain for ScaleOutBrain {
    fn name(&self) -> &str {
        "scale-out"
    }
    async fn on_event(&self, event: &MarketDataEvent, position: &Position) -> CoreResult<Decision> {
        let MarketDataEvent::Candle { candle, .. } = event else {
            return Ok(Decision::hold());
        };
        if position.is_flat() {
            if candle.time == self.open_at {
                return Ok(Decision::buy(1.0));
            }
        } else if candle.time == self.partial_at {
            return Ok(
                Decision::sell(1.0).with_size_hint(SizeHint::Quantity(Volume(self.partial_qty)))
            );
        } else if candle.time == self.close_at {
            return Ok(Decision::close());
        }
        Ok(Decision::hold())
    }
}

/// Buys on every candle — the pyramiding brain the engine's determinism
/// test uses; here it drives the no-funding regression comparison.
struct BuyEveryCandle;
#[async_trait]
impl Brain for BuyEveryCandle {
    fn name(&self) -> &str {
        "buy-every-candle"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
        Ok(Decision::buy(1.0))
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// `n` hourly candles at a flat price of 100 — margin 1000 / 1× / cv 1.0
/// sizes entries to exactly 10 contracts, so position notional is exactly
/// 1000 and a 1 bp settlement moves exactly 0.1 quote units.
fn hourly_flat(n: usize) -> Vec<Candle> {
    (0..n)
        .map(|i| Candle {
            time: i as i64 * HOUR_MS,
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 1.0,
        })
        .collect()
}

fn ramp(n: usize, start: f64, step: f64) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let p = start + step * i as f64;
            Candle {
                time: i as i64 * 60_000,
                open: p,
                high: p,
                low: p,
                close: p,
                volume: 1.0,
            }
        })
        .collect()
}

fn config(funding: FundingModel) -> BacktestConfig {
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
        .funding(funding)
        .build()
        .unwrap()
}

/// Open at t=0, hold for 25 hourly candles (0 → 24 h). With an 8 h
/// constant grid the hold crosses exactly three settlements (8 h, 16 h,
/// 24 h — the last exactly on the final candle).
async fn run_hold(long: bool, funding: FundingModel) -> BacktestResult {
    Backtest::new(
        config(funding),
        Arc::new(OpenCloseBrain {
            long,
            open_at: 0,
            close_at: None,
        }),
    )
    .with_candles(hourly_flat(25))
    .run()
    .await
    .unwrap()
}

fn assert_close(actual: f64, expected: f64, what: &str) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "{what}: expected {expected}, got {actual}"
    );
}

// ── Sign conventions: long/short × positive/negative rate ──────────────────

const ONE_BP: f64 = 0.0001;

#[tokio::test]
async fn long_pays_positive_funding() {
    let r = run_hold(
        true,
        FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        },
    )
    .await;
    // 3 settlements × 1 bp × 1000 notional = 0.3 paid.
    assert_close(r.funding_paid, 0.3, "funding_paid");
    assert_close(r.funding_received, 0.0, "funding_received");
    assert_close(r.net_funding(), -0.3, "net_funding");
    // Flat price + zero fees → the funding IS the whole PnL, and it
    // lands in the equity curve and final cash.
    assert_close(r.net_pnl, -0.3, "net_pnl");
    assert_close(r.final_cash, 9_999.7, "final_cash");
    assert_close(*r.equity_curve.last().unwrap(), 9_999.7, "final equity");
    // First settlement lands in the sample after candle 8 (t = 8 h).
    assert_close(r.equity_curve[9], 9_999.9, "equity after first settlement");
    assert_close(
        r.equity_curve[8],
        10_000.0,
        "equity before first settlement",
    );
}

#[tokio::test]
async fn short_receives_positive_funding() {
    let r = run_hold(
        false,
        FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        },
    )
    .await;
    assert_close(r.funding_received, 0.3, "funding_received");
    assert_close(r.funding_paid, 0.0, "funding_paid");
    assert_close(r.net_pnl, 0.3, "net_pnl");
    assert_close(r.final_cash, 10_000.3, "final_cash");
}

#[tokio::test]
async fn long_receives_negative_funding() {
    let r = run_hold(
        true,
        FundingModel::Constant {
            rate: -ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        },
    )
    .await;
    assert_close(r.funding_received, 0.3, "funding_received");
    assert_close(r.funding_paid, 0.0, "funding_paid");
    assert_close(r.net_pnl, 0.3, "net_pnl");
}

#[tokio::test]
async fn short_pays_negative_funding() {
    let r = run_hold(
        false,
        FundingModel::Constant {
            rate: -ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        },
    )
    .await;
    assert_close(r.funding_paid, 0.3, "funding_paid");
    assert_close(r.funding_received, 0.0, "funding_received");
    assert_close(r.net_pnl, -0.3, "net_pnl");
}

// ── Settlement-window boundaries ────────────────────────────────────────────

#[tokio::test]
async fn position_opened_mid_interval_closed_on_settlement_pays_it() {
    // Open at t = 1 h (mid-way through the 0→8 h funding interval), close
    // at t = 8 h — exactly a settlement timestamp. The `(entry, exit]`
    // window charges that settlement: it fires against the position as it
    // stood entering the close candle, before the close fill.
    let r = Backtest::new(
        config(FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        }),
        Arc::new(OpenCloseBrain {
            long: true,
            open_at: HOUR_MS,
            close_at: Some(8 * HOUR_MS),
        }),
    )
    .with_candles(hourly_flat(12))
    .run()
    .await
    .unwrap();

    // Exactly one settlement — not zero (exit-inclusive) and not two.
    assert_close(r.funding_paid, 0.1, "funding_paid");
    assert_eq!(r.trades.len(), 1);
    // The close attributes the accrual to the trade: flat price + zero
    // fees → trade net is pure funding.
    assert_close(r.trades[0].funding, -0.1, "trade funding");
    assert_close(r.trades[0].net_pnl(), -0.1, "trade net");
    assert_close(r.net_pnl, -0.1, "net_pnl");
    // Nothing accrues after the close (candles run on to t = 11 h).
    assert_close(r.funding_received, 0.0, "funding_received");
}

#[tokio::test]
async fn position_opened_exactly_on_settlement_does_not_pay_it() {
    // Open at t = 8 h — exactly a settlement timestamp. The window is
    // entry-exclusive: that settlement fires against a still-flat book,
    // and the hold ends (t = 15 h) before the next one at 16 h.
    let r = Backtest::new(
        config(FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        }),
        Arc::new(OpenCloseBrain {
            long: true,
            open_at: 8 * HOUR_MS,
            close_at: None,
        }),
    )
    .with_candles(hourly_flat(16))
    .run()
    .await
    .unwrap();

    assert_eq!(r.orders_filled, 1, "the entry must actually fill");
    assert_close(r.funding_paid, 0.0, "funding_paid");
    assert_close(r.funding_received, 0.0, "funding_received");
    assert_close(r.net_pnl, 0.0, "net_pnl");
}

#[tokio::test]
async fn accrual_attribution_stops_at_close() {
    // Hold from t = 0 across the 8 h and 16 h settlements, close at 16 h,
    // then let the series run to 24 h — the third grid point must NOT
    // charge the closed book, and the trade carries exactly two
    // settlements' worth of funding.
    let r = Backtest::new(
        config(FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        }),
        Arc::new(OpenCloseBrain {
            long: true,
            open_at: 0,
            close_at: Some(16 * HOUR_MS),
        }),
    )
    .with_candles(hourly_flat(25))
    .run()
    .await
    .unwrap();

    assert_close(r.funding_paid, 0.2, "funding_paid");
    assert_eq!(r.trades.len(), 1);
    assert_close(r.trades[0].funding, -0.2, "trade funding");
    // Everything funding-related is attributed — headline PnL equals the
    // single trade's net.
    assert_close(r.net_pnl, r.trades[0].net_pnl(), "net_pnl == trade net");
}

// ── Partial-close pro-rata funding attribution ──────────────────────────────

#[tokio::test]
async fn partial_close_splits_accrued_funding_pro_rata() {
    // Long 10 at t=0. First settlement (8 h) charges the full book:
    //   1 bp × 10 × 100 = 0.1 accrued.
    // Scale out 4 at t=9 h: the close takes 4/10 of the accrual (0.04)
    // and leaves 0.06 on the still-open 6.
    // Second settlement (16 h) charges the 6 remaining:
    //   1 bp × 6 × 100 = 0.06 → accrual is now 0.12.
    // Close the last 6 at t=17 h: it takes the whole 0.12.
    let r = Backtest::new(
        config(FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        }),
        Arc::new(ScaleOutBrain {
            open_at: 0,
            partial_at: 9 * HOUR_MS,
            partial_qty: 4.0,
            close_at: 17 * HOUR_MS,
        }),
    )
    .with_candles(hourly_flat(18))
    .run()
    .await
    .unwrap();

    assert_eq!(r.trades.len(), 2, "one partial close + one final close");
    // Partial close: 4 contracts, pro-rata 4/10 of the 0.1 accrued so far.
    assert_close(r.trades[0].qty, 4.0, "partial qty");
    assert_close(r.trades[0].funding, -0.04, "partial-close funding share");
    // Final close: the remaining 6 contracts carry the leftover 0.06 from
    // the first settlement plus the whole 0.06 from the second.
    assert_close(r.trades[1].qty, 6.0, "final qty");
    assert_close(r.trades[1].funding, -0.12, "final-close funding share");
    // Every quote unit of funding is attributed exactly once: the two
    // trades' shares sum to the total paid, no dust and no double-count.
    assert_close(r.funding_paid, 0.16, "total funding_paid");
    assert_close(
        r.trades[0].funding + r.trades[1].funding,
        -r.funding_paid,
        "attributed funding == booked funding",
    );
    // Flat price + zero fees → headline PnL is pure funding.
    assert_close(r.net_pnl, -0.16, "net_pnl");
}

// ── Mark sourcing: funding uses the last mark, not the entry price ──────────

#[tokio::test]
async fn funding_notional_uses_last_mark_not_entry_price() {
    // 1-minute candles ramping 100, 101, 102, … so the mark diverges from
    // the entry price. Settlement every 8 minutes; a single settlement at
    // t=8 min books against the *previous* candle's close (t=7 min → 107),
    // not the entry (100). Entry-price sourcing would yield 0.1; last-mark
    // sourcing yields 0.107, and only the latter is correct.
    let r = Backtest::new(
        config(FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: 8 * 60_000,
        }),
        Arc::new(OpenCloseBrain {
            long: true,
            open_at: 0,
            close_at: None,
        }),
    )
    .with_candles(ramp(10, 100.0, 1.0))
    .run()
    .await
    .unwrap();

    // 1 bp × 10 contracts × 107 (mark at the candle before settlement).
    assert_close(r.funding_paid, 0.107, "funding_paid at last mark");
    assert_close(r.funding_received, 0.0, "funding_received");
}

// ── Post-build struct-literal assignment can't smuggle an unsorted series ────

#[tokio::test]
async fn unsorted_series_assigned_after_build_is_sorted_at_run_time() {
    // `BacktestConfig.funding` is a pub field, so a series can be assigned
    // after the builder's validation. An unsorted series would make
    // `settlements_between`'s binary search drop or double-book
    // settlements; `run()` re-validates so the result matches the sorted
    // equivalent (three 1 bp settlements = 0.3 paid).
    let mut cfg = config(FundingModel::None);
    cfg.funding = FundingModel::Series(vec![
        (3 * EIGHT_HOURS_MS, ONE_BP),
        (EIGHT_HOURS_MS, ONE_BP),
        (2 * EIGHT_HOURS_MS, ONE_BP),
    ]);
    let r = Backtest::new(
        cfg,
        Arc::new(OpenCloseBrain {
            long: true,
            open_at: 0,
            close_at: None,
        }),
    )
    .with_candles(hourly_flat(25))
    .run()
    .await
    .unwrap();
    assert_close(r.funding_paid, 0.3, "unsorted series booked all three");
}

#[tokio::test]
async fn duplicate_series_timestamps_assigned_after_build_fail_loud() {
    // A post-build series with a duplicate settlement timestamp would
    // double-book; `run()`'s re-validation rejects it as a config error
    // rather than producing plausible-but-wrong funding PnL.
    let mut cfg = config(FundingModel::None);
    cfg.funding = FundingModel::Series(vec![(EIGHT_HOURS_MS, ONE_BP), (EIGHT_HOURS_MS, ONE_BP)]);
    let err = Backtest::new(
        cfg,
        Arc::new(OpenCloseBrain {
            long: true,
            open_at: 0,
            close_at: None,
        }),
    )
    .with_candles(hourly_flat(25))
    .run()
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "got {err:?}");
}

// ── Series vs constant fallback ─────────────────────────────────────────────

#[tokio::test]
async fn series_matching_the_grid_reproduces_constant_model() {
    let constant = run_hold(
        true,
        FundingModel::Constant {
            rate: ONE_BP,
            interval_ms: EIGHT_HOURS_MS,
        },
    )
    .await;
    let series = run_hold(
        true,
        FundingModel::Series(vec![
            (EIGHT_HOURS_MS, ONE_BP),
            (2 * EIGHT_HOURS_MS, ONE_BP),
            (3 * EIGHT_HOURS_MS, ONE_BP),
        ]),
    )
    .await;
    assert_eq!(constant.equity_curve, series.equity_curve);
    assert_eq!(
        constant.funding_paid.to_bits(),
        series.funding_paid.to_bits()
    );
    assert_eq!(constant.net_pnl.to_bits(), series.net_pnl.to_bits());
}

#[tokio::test]
async fn series_with_mixed_rates_sums_signed_cashflows() {
    // Long through +1 bp (pays 0.1), −2 bp (collects 0.2), +0.5 bp
    // (pays 0.05): received 0.2, paid 0.15, net +0.05.
    let r = run_hold(
        true,
        FundingModel::Series(vec![
            (EIGHT_HOURS_MS, ONE_BP),
            (2 * EIGHT_HOURS_MS, -2.0 * ONE_BP),
            (3 * EIGHT_HOURS_MS, 0.5 * ONE_BP),
        ]),
    )
    .await;
    assert_close(r.funding_received, 0.2, "funding_received");
    assert_close(r.funding_paid, 0.15, "funding_paid");
    assert_close(r.net_funding(), 0.05, "net_funding");
    assert_close(r.net_pnl, 0.05, "net_pnl");
}

// ── Default-off regression ──────────────────────────────────────────────────

#[tokio::test]
async fn funding_off_is_bit_identical_to_a_config_that_never_mentions_it() {
    let base_cfg = || {
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 100,
            })
    };
    // (a) never mentions funding — every pre-existing consumer's config.
    let untouched = Backtest::new(base_cfg().build().unwrap(), Arc::new(BuyEveryCandle))
        .with_candles(ramp(30, 100.13, 0.37))
        .run()
        .await
        .unwrap();
    // (b) explicitly off.
    let explicit_off = Backtest::new(
        base_cfg().funding(FundingModel::None).build().unwrap(),
        Arc::new(BuyEveryCandle),
    )
    .with_candles(ramp(30, 100.13, 0.37))
    .run()
    .await
    .unwrap();

    assert_eq!(untouched.equity_curve, explicit_off.equity_curve);
    assert_eq!(untouched.period_returns, explicit_off.period_returns);
    assert_eq!(untouched.net_pnl.to_bits(), explicit_off.net_pnl.to_bits());
    assert_eq!(
        untouched.max_drawdown.to_bits(),
        explicit_off.max_drawdown.to_bits()
    );
    assert_eq!(untouched.trades, explicit_off.trades);
    // Funding metrics report exact zeros when the model is off.
    assert_eq!(untouched.funding_received.to_bits(), 0.0_f64.to_bits());
    assert_eq!(untouched.funding_paid.to_bits(), 0.0_f64.to_bits());
    assert!(untouched.trades.iter().all(|t| t.funding == 0.0));

    // Sanity that this regression test has teeth: the same run with a
    // funding model ON must differ.
    let on = Backtest::new(
        base_cfg()
            .funding(FundingModel::Constant {
                rate: ONE_BP,
                interval_ms: 8 * 60_000, // every 8 minutes on 1 m candles
            })
            .build()
            .unwrap(),
        Arc::new(BuyEveryCandle),
    )
    .with_candles(ramp(30, 100.13, 0.37))
    .run()
    .await
    .unwrap();
    assert!(on.funding_paid > 0.0);
    assert_ne!(untouched.equity_curve, on.equity_curve);
}

// ── Timestamped equity export ───────────────────────────────────────────────

#[tokio::test]
async fn equity_curve_export_is_timestamped_per_candle() {
    let r = run_hold(true, FundingModel::None).await;
    assert_eq!(r.equity_times.len(), r.equity_curve.len());
    // Seed sample borrows the first candle's timestamp; then one sample
    // per candle at that candle's time.
    assert_eq!(r.equity_times[0], 0);
    assert_eq!(r.equity_times[1], 0);
    assert_eq!(r.equity_times[2], HOUR_MS);
    assert_eq!(*r.equity_times.last().unwrap(), 24 * HOUR_MS);

    let points = r.equity_points();
    assert_eq!(points.len(), r.equity_curve.len());
    assert_eq!(points[2].time, HOUR_MS);
    assert_eq!(points[2].equity, r.equity_curve[2]);
}
