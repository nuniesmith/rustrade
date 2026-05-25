# Backtesting

`rustrade-backtest` is a deterministic replay engine that consumes the
same `Brain` trait as live trading. Write your strategy once, validate
offline against a canned candle series, ship it live with the same
code.

## 1. Brain-identical guarantee

The whole point of the engine: any `impl rustrade::Brain` that runs
through the live `Bot` runs in the backtest unchanged. No
"backtest-mode" branches, no duplicate decision logic, no
maintenance debt.

The tests in `crates/rustrade-backtest/tests/sma_replay.rs` exercise
exactly the same `SmaCrossBrain` shape as `examples/sma-cross-bot/`
uses live.

## 2. Quickstart

```rust,ignore
use std::sync::Arc;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, SlippageModel};

let result = Backtest::new(
    BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(10_000.0)
        .slippage(SlippageModel::FixedBps(5.0))
        .fees(FeeModel::Flat(0.0005))
        .build()?,
    Arc::new(MySmaCrossBrain::new()),
)
.with_candles(load_candles())
.run()
.await?;

println!("{}", result.summary());
```

## 3. Configuration

```rust,ignore
BacktestConfig::builder()
    .symbol("BTCUSDT")
    .initial_cash(10_000.0)
    .sizing(SizingConfig {
        margin_per_trade: 100.0,
        leverage: 1,
        max_contracts: 10,
    })
    .slippage(SlippageModel::FixedBps(5.0))
    .fees(FeeModel::MakerTaker { maker: 0.0002, taker: 0.0005 })
    .contract_value(1.0)
    .build()?;
```

- **`sizing`** uses the same `SizingConfig` struct as the live
  framework; configuring it identically in both paths is the
  recommended approach.
- **`slippage`** options today: `Zero`, `FixedBps(bps)`. Book-walk
  slippage waits for Phase 4b.
- **`fees`** options today: `Zero`, `Flat(rate)`, `MakerTaker`.
  Phase 4a treats every order as taker.
- **`contract_value`** is the per-symbol contract multiplier (`1.0`
  for spot). The engine is single-symbol today; multi-symbol
  backtests are Phase 4b.

All fields except `symbol` have sensible defaults — `build()` validates
and returns `Error::Config` on any constraint violation.

## 4. Result interpretation

```rust,ignore
let result: BacktestResult = backtest.run().await?;

result.total_return_pct();   // (net_pnl / initial_cash) * 100
result.win_rate();           // (0.0..=1.0), excludes break-evens
result.profit_factor();      // Σ winning_pnl / Σ |losing_pnl|, None if no losses
result.max_drawdown;         // peak-to-trough, in quote currency (≤ 0)
result.trades.len();         // total closed trades
result.wins() / .losses() / .breakevens();

// Multi-line stat block, suitable for logging:
println!("{}", result.summary());
```

`result.trades` is the full per-trade ledger — entry / exit prices,
quantities, gross PnL, fee paid, timestamp. Useful for:

- Custom metric calculation (the engine ships the basics; everything
  else is one iterator chain away)
- Sanity-check plots
- Regression assertions in test suites

## 5. Determinism

Same `(Brain, candles, config)` → same `BacktestResult`. Every run.

The engine is single-threaded, has no random number source, and
processes candles in order. This means:

- A failing CI test reproduces locally.
- You can pin down a strategy's order count for regression testing —
  see `sma_replay.rs::deterministic_replay_same_brain_same_series`.
- Comparing two strategies on the same series is honest; differences
  are entirely the brains' doing.

If you need stochastic effects (slippage with variance, exchange
"latency", etc.), seed them on the brain or in a wrapper — keep the
engine deterministic.

## 6. The position state machine

The engine tracks one open position per symbol. Fills modify it as
follows:

- A fill in the same direction as the open position **adds** to it,
  with a weighted-average entry price.
- A fill in the opposite direction **reduces** the position. The
  closed quantity emits a `TradeOutcome` with realised PnL.
- A fill larger than the current opposing position **flips** the
  position. The engine splits it: one `TradeOutcome` for the closed
  portion at the original entry, then a fresh open at the fill price
  for the leftover.
- Fees are apportioned proportionally between closing and opening
  portions of a flip fill.

This is broadly equivalent to FIFO accounting with average-cost
entries. If your brain wants LIFO or per-tax-lot accounting, that's
the brain's concern — the engine just reports realised outcomes as
they happen.

## 7. Slippage model details

`SlippageModel::FixedBps(bps)` applies symmetric adverse slippage:

| Side  | Fill price                                     |
| ----- | ---------------------------------------------- |
| Buy   | `reference * (1 + bps / 10_000)` (higher)      |
| Sell  | `reference * (1 - bps / 10_000)` (lower)       |

The reference is the candle's `close` — the engine fills "at the
close" by default. This matters: backtests that fill at the next
candle's `open` (and avoid lookahead concerns from intra-bar trading)
are not yet supported. Phase 4b will add a fill-at-next-open mode.

## 8. CI-friendly determinism test

```rust,ignore
#[tokio::test(flavor = "multi_thread")]
async fn strategy_is_deterministic() {
    let candles = load_canonical_series();
    let make_cfg = || BacktestConfig::builder()
        .symbol("BTCUSDT")
        .sizing(...)
        .fees(FeeModel::Flat(0.0005))
        .build().unwrap();

    let r1 = Backtest::new(make_cfg(), MyBrain::new())
        .with_candles(candles.clone()).run().await.unwrap();
    let r2 = Backtest::new(make_cfg(), MyBrain::new())
        .with_candles(candles).run().await.unwrap();

    assert_eq!(r1.signals_emitted, r2.signals_emitted);
    assert_eq!(r1.trades.len(), r2.trades.len());
    assert!((r1.net_pnl - r2.net_pnl).abs() < 1e-9);
}
```

A failing version of this test catches "brain has an iteration order
problem" (e.g. iterating a `HashMap`) before it shows up in production
as a flapping signal stream.

## 9. What the engine does NOT do

Defer to a downstream tool, the brain, or a future phase:

- **Loading data.** Today candles are `Vec<Candle>`. CSV / Parquet
  loaders are Phase 4b.
- **Sharpe / Sortino.** Need a risk-free rate input and an equity
  sampling cadence. Phase 4b.
- **Walk-forward optimisation.** Use the engine as the inner loop;
  the outer loop is your responsibility.
- **Order-book reconstruction.** Slippage is parameter-based.
  Book-walk slippage is Phase 4b.

## Next steps

- Runnable reference:
  [`crates/rustrade-backtest/tests/sma_replay.rs`](../crates/rustrade-backtest/tests/sma_replay.rs)
- [Writing a Brain](./writing-a-brain.md) — the same brain runs in
  both the live `Bot` and the backtest engine
- [API docs](https://docs.rs/rustrade-backtest) — full surface
