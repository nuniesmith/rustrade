# rustrade-backtest

Deterministic backtest engine for [rustrade](../../README.md) `Brain`s.
The same `Brain` trait used by `rustrade` for live trading drives the
backtest — no special "backtest-mode" code paths in the strategy, no
duplicate decision logic to keep in sync.

## What's in this crate

| Type           | Purpose                                                          |
| -------------- | ---------------------------------------------------------------- |
| `Backtest`     | The replay loop — feeds candles to a `Brain` and accumulates fills |
| `BacktestConfig` | Symbol, sizing config, slippage model, fee model, initial cash |
| `SlippageModel` | `Zero`, `FixedBps`. Applied between the brain's signal and the simulated fill price |
| `FeeModel`     | `Zero`, `Flat`, `MakerTaker`. Applied to every simulated fill   |
| `BacktestResult` | Final stats: total return %, win rate, max drawdown, # trades |

## Quickstart

```rust,ignore
use std::sync::Arc;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, SlippageModel};
use rustrade_core::{Candle, Symbol};

let candles: Vec<Candle> = load_candles_somehow();
let result = Backtest::new(
    BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(10_000.0)
        .slippage(SlippageModel::FixedBps(5.0))
        .fees(FeeModel::Flat(0.001))
        .build()?,
    Arc::new(MySmaCrossBrain::new()),
)
.with_candles(candles)
.run()
.await?;

println!("{}", result.summary());
```

## Brain parity

Any `impl rustrade_core::Brain` that runs through the live `rustrade::Bot`
runs in this engine unchanged. See `tests/brain_parity.rs` for the
regression test that pins this down: the same brain emits the same
sequence of decisions for the same candle series in both code paths.

## Status

Phase 4a — minimum viable engine. CSV / Parquet candle loaders,
book-walk slippage, tiered fees, and Sharpe / Sortino metrics land in
Phase 4b. See the workspace [`TODO.md`](../../TODO.md).

## Licence

MIT.
