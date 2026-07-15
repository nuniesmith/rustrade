//! Honest resting limit/stop fills — `FillModel::Resting`.
//!
//! The legacy engine fills everything on the decision candle: markets at
//! the close, limits only if that same candle crosses them (a lookahead —
//! the candle's range printed before the brain decided), and drops
//! anything else. `FillModel::Resting` opts in to honest semantics:
//!
//!   * orders reach the book at the decision candle's CLOSE, so the
//!     decision candle can never fill a resting order (no lookahead);
//!   * a resting limit fills when a later candle crosses its level — at
//!     the limit price, or at that candle's open when it gaps through;
//!   * a stop triggers on cross and fills at the level or WORSE (gap →
//!     open), while a take-profit (a resting limit) fills at the level
//!     or BETTER (gap → open);
//!   * standalone stop-only / TP-only protections are honoured (legacy
//!     requires both bracket legs);
//!   * same-candle ambiguity resolves conservatively — the stop leg
//!     before the TP leg, and on the candle a resting entry fills its
//!     attached stop may fire but its TP must wait for the next candle;
//!   * resting fills are makers (maker fee rate, no slippage);
//!   * the default (`FillModel::TakerAtClose`) is bit-identical to the
//!     legacy behaviour — regression-pinned below.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, FillModel, SlippageModel};
use rustrade_core::{
    Brain, BrainHealth, Candle, Decision, MarketDataEvent, OrderKind, Position, Price,
    Result as CoreResult,
};
use rustrade_risk::SizingConfig;

// ── Scripted brain ──────────────────────────────────────────────────────────

/// Emits one scripted decision per candle index; `Hold` for anything
/// beyond the script.
struct ScriptBrain {
    script: Vec<Decision>,
    seen: AtomicUsize,
}

impl ScriptBrain {
    fn new(script: Vec<Decision>) -> Arc<Self> {
        Arc::new(Self {
            script,
            seen: AtomicUsize::new(0),
        })
    }
}

#[async_trait]
impl Brain for ScriptBrain {
    fn name(&self) -> &str {
        "script"
    }
    async fn on_event(&self, _e: &MarketDataEvent, _p: &Position) -> CoreResult<Decision> {
        let i = self.seen.fetch_add(1, Ordering::Relaxed);
        Ok(self.script.get(i).cloned().unwrap_or_else(Decision::hold))
    }
    async fn health(&self) -> BrainHealth {
        BrainHealth::ok()
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn ohlc(i: i64, open: f64, high: f64, low: f64, close: f64) -> Candle {
    Candle {
        time: i * 60_000,
        open,
        high,
        low,
        close,
        volume: 1.0,
    }
}

/// margin 1000 / 1× / cv 1.0 at price ~100 → 10 contracts per entry;
/// zero fees + zero slippage so fills land exactly on the conventions.
fn cfg(fill_model: FillModel) -> BacktestConfig {
    BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(10_000.0)
        .sizing(SizingConfig {
            margin_per_trade: 1_000.0,
            leverage: 1,
            max_contracts: 1_000,
        })
        .fees(FeeModel::Zero)
        .slippage(SlippageModel::Zero)
        .fill_model(fill_model)
        .build()
        .unwrap()
}

async fn run(
    fill_model: FillModel,
    script: Vec<Decision>,
    candles: Vec<Candle>,
) -> rustrade_backtest::BacktestResult {
    Backtest::new(cfg(fill_model), ScriptBrain::new(script))
        .with_candles(candles)
        .run()
        .await
        .unwrap()
}

fn buy_limit(price: f64) -> Decision {
    Decision::buy(1.0).with_limit_price(Price(price))
}

fn sell_limit(price: f64) -> Decision {
    Decision::sell(1.0).with_limit_price(Price(price))
}

// ── Resting limit entries ───────────────────────────────────────────────────

#[tokio::test]
async fn resting_buy_limit_fills_when_later_candle_crosses_from_above() {
    // Buy limit 95 placed at c0's close (100). c1 trades down through 95
    // → fill at the limit. Close at c2's close (100).
    let r = run(
        FillModel::Resting,
        vec![buy_limit(95.0), Decision::hold(), Decision::close()],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 100.0, 94.0, 98.0),
            ohlc(2, 98.0, 100.0, 98.0, 100.0),
        ],
    )
    .await;
    assert_eq!(r.orders_filled, 2, "resting entry + market close");
    assert_eq!(r.trades.len(), 1);
    let t = &r.trades[0];
    assert!((t.entry_price - 95.0).abs() < 1e-9, "entry at the limit");
    assert!((t.exit_price - 100.0).abs() < 1e-9);
    // 10 contracts × (100 − 95) = +50.
    assert!((r.net_pnl - 50.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

#[tokio::test]
async fn resting_sell_limit_fills_when_later_candle_crosses_from_below() {
    // Sell limit 105 placed at c0's close (100). c1 trades up through 105
    // → short entry at the limit. Close at c2's close (100).
    let r = run(
        FillModel::Resting,
        vec![sell_limit(105.0), Decision::hold(), Decision::close()],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 106.0, 100.0, 102.0),
            ohlc(2, 102.0, 102.0, 99.0, 100.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    let t = &r.trades[0];
    assert!((t.entry_price - 105.0).abs() < 1e-9, "entry at the limit");
    assert!((t.exit_price - 100.0).abs() < 1e-9);
    // Short 10 × (105 − 100) = +50.
    assert!((r.net_pnl - 50.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

#[tokio::test]
async fn no_lookahead_decision_candle_range_cannot_fill_resting_limit() {
    // The DECISION candle's low (94) is below the 95 limit, but that range
    // printed before the order existed — the order reaches the book at the
    // close. No later candle crosses → never fills. (Legacy fills this.)
    let candles = vec![
        ohlc(0, 100.0, 101.0, 94.0, 100.0),
        ohlc(1, 100.0, 101.0, 99.0, 100.0),
    ];
    let honest = run(FillModel::Resting, vec![buy_limit(95.0)], candles.clone()).await;
    assert_eq!(honest.signals_emitted, 1);
    assert_eq!(honest.orders_filled, 0, "no fill without a later cross");

    // Contrast: legacy same-candle lookahead fill.
    let legacy = run(FillModel::TakerAtClose, vec![buy_limit(95.0)], candles).await;
    assert_eq!(
        legacy.orders_filled, 1,
        "legacy fills on the decision candle"
    );
}

#[tokio::test]
async fn resting_buy_limit_gap_through_fills_at_open() {
    // Buy limit 95; c1 gaps down and OPENS at 90 — the 95 level never
    // traded, so the fill is the open (the first price the market offered).
    let r = run(
        FillModel::Resting,
        vec![buy_limit(95.0), Decision::hold(), Decision::close()],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 90.0, 92.0, 89.0, 91.0),
            ohlc(2, 91.0, 91.0, 90.0, 91.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    assert!(
        (r.trades[0].entry_price - 90.0).abs() < 1e-9,
        "gap-through fills at the open, not the level: {}",
        r.trades[0].entry_price
    );
}

#[tokio::test]
async fn resting_sell_limit_gap_through_fills_at_open() {
    // Sell limit 105; c1 gaps up and opens at 110 → fill at the open.
    let r = run(
        FillModel::Resting,
        vec![sell_limit(105.0), Decision::hold(), Decision::close()],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 110.0, 112.0, 108.0, 110.0),
            ohlc(2, 110.0, 110.0, 109.0, 110.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    assert!(
        (r.trades[0].entry_price - 110.0).abs() < 1e-9,
        "gap-through fills at the open: {}",
        r.trades[0].entry_price
    );
}

#[tokio::test]
async fn untouched_resting_limit_never_fills() {
    // Buy limit 95 with every later candle staying above 96 → GTC order
    // rests to the end of the run without filling.
    let r = run(
        FillModel::Resting,
        vec![buy_limit(95.0)],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 101.0, 96.5, 100.0),
            ohlc(2, 100.0, 102.0, 97.0, 101.0),
            ohlc(3, 101.0, 103.0, 96.1, 102.0),
        ],
    )
    .await;
    assert_eq!(r.signals_emitted, 1);
    assert_eq!(r.orders_filled, 0);
    assert!(r.trades.is_empty());
    assert_eq!(r.net_pnl, 0.0);
}

#[tokio::test]
async fn marketable_limit_fills_immediately_at_close_as_taker() {
    // Buy limit 105 ≥ close 103 → crosses immediately at the decision
    // close (103), NOT at the limit and NOT at the legacy open (100).
    let r = run(
        FillModel::Resting,
        vec![buy_limit(105.0), Decision::close()],
        vec![
            ohlc(0, 100.0, 106.0, 100.0, 103.0),
            ohlc(1, 103.0, 103.0, 103.0, 103.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    assert!(
        (r.trades[0].entry_price - 103.0).abs() < 1e-9,
        "marketable limit crosses at the decision close: {}",
        r.trades[0].entry_price
    );
}

#[tokio::test]
async fn new_decision_cancels_resting_entry() {
    // Buy limit 95 placed at c0; c1's Close (flat position) cancels the
    // working order; c2 dips through 95 — nothing may fill.
    let r = run(
        FillModel::Resting,
        vec![buy_limit(95.0), Decision::close(), Decision::hold()],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 101.0, 99.0, 100.0),
            ohlc(2, 100.0, 100.0, 90.0, 95.0),
        ],
    )
    .await;
    assert_eq!(r.orders_filled, 0, "cancelled order must not fill");
    assert!(r.trades.is_empty());
}

// ── Post-only / IOC / FOK under the resting model ───────────────────────────

#[tokio::test]
async fn post_only_rests_and_fills_as_maker() {
    // Non-marketable post-only (95 < close 100) rests and fills on c1's
    // cross at the limit.
    let entry = Decision::buy(1.0)
        .with_limit_price(Price(95.0))
        .with_order_kind(OrderKind::PostOnly);
    let r = run(
        FillModel::Resting,
        vec![entry, Decision::close()],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 100.0, 94.0, 98.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    assert!((r.trades[0].entry_price - 95.0).abs() < 1e-9);
}

#[tokio::test]
async fn marketable_post_only_is_rejected() {
    // Post-only at 105 ≥ close 103 would cross as taker → rejected, never
    // rests, never fills.
    let entry = Decision::buy(1.0)
        .with_limit_price(Price(105.0))
        .with_order_kind(OrderKind::PostOnly);
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 106.0, 100.0, 103.0),
            ohlc(1, 103.0, 108.0, 100.0, 104.0),
        ],
    )
    .await;
    assert_eq!(r.orders_filled, 0);
}

#[tokio::test]
async fn non_marketable_ioc_is_cancelled_not_rested() {
    // IOC buy limit 95 < close 100: nothing to fill now → cancelled. The
    // later dip through 95 must not fill it.
    let entry = Decision::buy(1.0)
        .with_limit_price(Price(95.0))
        .with_order_kind(OrderKind::Ioc);
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 100.0, 90.0, 95.0),
        ],
    )
    .await;
    assert_eq!(r.orders_filled, 0, "IOC never rests");
}

// ── Standalone stops (long and short) ───────────────────────────────────────

#[tokio::test]
async fn standalone_stop_triggers_on_long() {
    // Market long at 100 with ONLY a stop (no TP) — honoured in resting
    // mode. c1 trades down through 95 → stop-out at the level.
    let entry = Decision::buy(1.0).with_stop(Price(95.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 100.0, 94.0, 96.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1, "stop-only protection must fire");
    let t = &r.trades[0];
    assert!((t.entry_price - 100.0).abs() < 1e-9);
    assert!((t.exit_price - 95.0).abs() < 1e-9, "stop fill at the level");
    assert!(t.gross_pnl < 0.0);
    // 10 × (95 − 100) = −50.
    assert!((r.net_pnl + 50.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

#[tokio::test]
async fn standalone_stop_triggers_on_short() {
    // Market short at 100 with only a stop above. c1 trades up through
    // 105 → buy-stop closes the short at the level.
    let entry = Decision::sell(1.0).with_stop(Price(105.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 106.0, 100.0, 104.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    let t = &r.trades[0];
    assert!(
        (t.exit_price - 105.0).abs() < 1e-9,
        "stop fill at the level"
    );
    assert!(t.gross_pnl < 0.0);
    assert!((r.net_pnl + 50.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

#[tokio::test]
async fn stop_gap_through_fills_at_open_worse_than_level() {
    // Long with stop 95; c1 gaps down and opens at 90 — the stop-market
    // triggers into a market that never traded 95, so the fill is the
    // open: WORSE than the level. No optimistic level fills.
    let entry = Decision::buy(1.0).with_stop(Price(95.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 90.0, 92.0, 89.0, 91.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    assert!(
        (r.trades[0].exit_price - 90.0).abs() < 1e-9,
        "gapped stop fills at the open: {}",
        r.trades[0].exit_price
    );
    // 10 × (90 − 100) = −100 — the honest loss, not the −50 a fixed-level
    // fill would report.
    assert!((r.net_pnl + 100.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

#[tokio::test]
async fn standalone_take_profit_fills_and_gaps_at_open_in_strategy_favour() {
    // Long at 100 with only a TP at 105 (a resting sell limit). c1 gaps
    // UP and opens at 110 → the resting limit fills at the open — price
    // improvement, mirroring a real book.
    let entry = Decision::buy(1.0).with_take_profit(Price(105.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 110.0, 112.0, 108.0, 111.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1, "TP-only protection must fire");
    assert!(
        (r.trades[0].exit_price - 110.0).abs() < 1e-9,
        "gapped TP fills at the open: {}",
        r.trades[0].exit_price
    );
    assert!((r.net_pnl - 100.0).abs() < 1e-9, "net_pnl={}", r.net_pnl);
}

// ── Conservative same-candle resolution ─────────────────────────────────────

#[tokio::test]
async fn same_candle_entry_and_stop_resolve_to_immediate_stop_out() {
    // Resting buy limit 100 with stop 95. c1's range [94, 104] touches
    // BOTH levels; the OHLC path is unknown, so the engine assumes the
    // worse outcome: entry at 100, stopped out at 95 on the same candle.
    let entry = buy_limit(100.0).with_stop(Price(95.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 103.0, 104.0, 102.0, 103.0),
            ohlc(1, 103.0, 104.0, 94.0, 103.0),
        ],
    )
    .await;
    assert_eq!(r.orders_filled, 2, "entry fill + same-candle stop-out");
    assert_eq!(r.trades.len(), 1);
    let t = &r.trades[0];
    assert!((t.entry_price - 100.0).abs() < 1e-9);
    assert!((t.exit_price - 95.0).abs() < 1e-9);
    assert!(t.gross_pnl < 0.0, "conservative: booked as a loss");
}

#[tokio::test]
async fn same_candle_entry_and_take_profit_waits_for_next_candle() {
    // Resting buy limit 100 with TP 105. c1's range [99, 106] touches
    // both — but the TP gets no benefit of the doubt (its level may have
    // printed before the entry): the position stays open through c1 and
    // the TP fills on c2's cross instead.
    let entry = buy_limit(100.0).with_take_profit(Price(105.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 103.0, 104.0, 102.0, 103.0),
            ohlc(1, 103.0, 106.0, 99.0, 103.0),
            ohlc(2, 103.0, 106.0, 103.0, 104.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    let t = &r.trades[0];
    assert!((t.entry_price - 100.0).abs() < 1e-9);
    assert!(
        (t.exit_price - 105.0).abs() < 1e-9,
        "TP fills on c2, at the level"
    );
    // Fill count: 1 entry + 1 TP = 2; had the TP (wrongly) filled on c1
    // the numbers would be identical — so also pin the exit timestamp.
    assert_eq!(r.orders_filled, 2);
    assert_eq!(
        t.closed_at.timestamp_millis(),
        2 * 60_000,
        "exit must land on c2, not the entry candle"
    );
}

#[tokio::test]
async fn stop_still_fills_before_tp_when_one_bar_spans_both() {
    // Full bracket on an open long: one bar spans stop AND TP → the stop
    // fills first (pessimistic), same convention as legacy.
    let entry = Decision::buy(1.0)
        .with_stop(Price(95.0))
        .with_take_profit(Price(105.0));
    let r = run(
        FillModel::Resting,
        vec![entry],
        vec![
            ohlc(0, 100.0, 101.0, 99.0, 100.0),
            ohlc(1, 100.0, 106.0, 94.0, 100.0),
        ],
    )
    .await;
    assert_eq!(r.trades.len(), 1);
    assert!(
        (r.trades[0].exit_price - 95.0).abs() < 1e-9,
        "spanning bar resolves to the STOP"
    );
    assert!(r.trades[0].gross_pnl < 0.0);
}

// ── Maker vs taker fees on resting fills ────────────────────────────────────

#[tokio::test]
async fn resting_entry_fill_charges_the_maker_rate() {
    // MakerTaker with a deliberately huge taker rate: if the resting fill
    // were (wrongly) charged as taker the equity impact would be 50×
    // larger. Entry: 10 contracts at 95 → maker fee = 950 × 0.001 = 0.95.
    // Final candle closes at 100 → unrealised +50.
    let entry = buy_limit(95.0);
    let r = Backtest::new(
        BacktestConfig::builder()
            .symbol("BTCUSDT")
            .initial_cash(10_000.0)
            .sizing(SizingConfig {
                margin_per_trade: 1_000.0,
                leverage: 1,
                max_contracts: 1_000,
            })
            .fees(FeeModel::MakerTaker {
                maker: 0.001,
                taker: 0.05,
            })
            .slippage(SlippageModel::Zero)
            .fill_model(FillModel::Resting)
            .build()
            .unwrap(),
        ScriptBrain::new(vec![entry]),
    )
    .with_candles(vec![
        ohlc(0, 100.0, 101.0, 99.0, 100.0),
        ohlc(1, 100.0, 100.0, 94.0, 100.0),
    ])
    .run()
    .await
    .unwrap();

    let final_equity = *r.equity_curve.last().unwrap();
    let expected = 10_000.0 - 0.95 + 50.0;
    assert!(
        (final_equity - expected).abs() < 1e-9,
        "expected equity {expected} (maker fee), got {final_equity}"
    );
}

// ── Legacy regression: the default is bit-identical ─────────────────────────

/// A scenario touching every legacy code path the resting model changes:
/// a same-candle lookahead limit fill, a full OCO bracket whose stop is
/// gapped through (legacy fills at the fixed level), a standalone stop
/// that legacy silently drops, and a market close.
fn legacy_scenario() -> (Vec<Decision>, Vec<Candle>) {
    let script = vec![
        // c0: lookahead limit — legacy fills at 95 on the decision candle.
        buy_limit(95.0),
        // c1: close it at the candle close.
        Decision::close(),
        // c2: bracketed long entry at the close.
        Decision::buy(1.0)
            .with_stop(Price(95.0))
            .with_take_profit(Price(110.0)),
        // c3: (bracket candle — gap through the stop, legacy fills at 95)
        Decision::hold(),
        // c4: stop-only long — legacy registers NO protection.
        Decision::buy(1.0).with_stop(Price(98.0)),
        // c5: crosses 98, but legacy has no standalone stop → still open.
        Decision::hold(),
    ];
    let candles = vec![
        ohlc(0, 100.0, 101.0, 94.0, 100.0),
        ohlc(1, 100.0, 101.0, 99.0, 100.0),
        ohlc(2, 100.0, 101.0, 99.0, 100.0),
        ohlc(3, 90.0, 92.0, 89.0, 91.0),
        ohlc(4, 100.0, 101.0, 99.0, 100.0),
        ohlc(5, 100.0, 100.0, 97.0, 99.0),
    ];
    (script, candles)
}

#[tokio::test]
async fn legacy_default_behaviour_is_bit_identical() {
    // Run the scenario with the DEFAULT config (no .fill_model call) and
    // with an explicit TakerAtClose — every observable must be bit-equal,
    // and the absolute numbers pin the legacy conventions so any change
    // to the default path fails loudly here.
    let (script, candles) = legacy_scenario();

    let default_cfg = BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(10_000.0)
        .sizing(SizingConfig {
            margin_per_trade: 1_000.0,
            leverage: 1,
            max_contracts: 1_000,
        })
        .fees(FeeModel::Zero)
        .slippage(SlippageModel::Zero)
        .build()
        .unwrap();
    let r_default = Backtest::new(default_cfg, ScriptBrain::new(script.clone()))
        .with_candles(candles.clone())
        .run()
        .await
        .unwrap();
    let r_explicit = run(FillModel::TakerAtClose, script, candles).await;

    // Bit-identical across the two spellings of the default.
    assert_eq!(r_default.equity_curve, r_explicit.equity_curve);
    assert_eq!(r_default.net_pnl.to_bits(), r_explicit.net_pnl.to_bits());
    assert_eq!(r_default.orders_filled, r_explicit.orders_filled);
    assert_eq!(r_default.trades.len(), r_explicit.trades.len());

    // Pin the legacy conventions in absolute numbers:
    // c0 lookahead limit entry at 95 (10 contracts), closed c1 at 100 → +50.
    // c2 entry at 100, c3 stop GAPPED through but fills at the LEVEL 95 → −50.
    // c4 stop-only entry at 100 → no protection registered → stays open.
    assert_eq!(r_default.trades.len(), 2);
    assert!((r_default.trades[0].entry_price - 95.0).abs() < 1e-9);
    assert!((r_default.trades[0].exit_price - 100.0).abs() < 1e-9);
    assert!((r_default.trades[1].entry_price - 100.0).abs() < 1e-9);
    assert!(
        (r_default.trades[1].exit_price - 95.0).abs() < 1e-9,
        "legacy bracket ignores the gap and fills at the level: {}",
        r_default.trades[1].exit_price
    );
    // Fills: c0 limit entry, c1 close, c2 entry, c3 bracket stop, c4 entry.
    assert_eq!(r_default.orders_filled, 5);
    assert!((r_default.net_pnl - 0.0).abs() < 1e-9, "+50 − 50 = 0");
}
