# rustrade-backtest

Deterministic backtest engine for [rustrade](../../README.md) `Brain`s.
The same `Brain` trait used by `rustrade` for live trading drives the
backtest — no special "backtest-mode" code paths in the strategy, no
duplicate decision logic to keep in sync.

## What's in this crate

| Type             | Purpose                                                                                |
| ---------------- | -------------------------------------------------------------------------------------- |
| `Backtest`       | The replay loop — feeds candles to a `Brain` and accumulates fills                     |
| `BacktestConfig` | Symbols, sizing config, slippage / fee models, initial cash, Sharpe annualisation      |
| `SlippageModel`  | `Zero`, `FixedBps`. Applied between the brain's signal and the simulated fill price    |
| `FeeModel`       | `Zero`, `Flat`, `MakerTaker`. Applied to every simulated fill                          |
| `FundingModel`   | `None` (default), `Constant { rate, interval_ms }`, `Series`. Perp funding cashflows against open positions at settlement timestamps |
| `load_csv` / `load_csv_str` | CSV → `Vec<Candle>` with a fixed `time,open,high,low,close,volume` layout   |
| `sort_chronological` | Stable ascending-time sort for loaders that hand you newest-first              |
| `BacktestResult` | Final stats: return %, win rate, expectancy, avg win/loss, max drawdown, timestamped equity curve, Sharpe, Sortino, funding totals, # trades |

## Quickstart

```rust,ignore
use std::sync::Arc;
use rustrade_backtest::{Backtest, BacktestConfig, FeeModel, SlippageModel, load_csv};

let candles = load_csv("data/btcusdt-1m.csv")?;
let result = Backtest::new(
    BacktestConfig::builder()
        .symbol("BTCUSDT")
        .initial_cash(10_000.0)
        .slippage(SlippageModel::FixedBps(5.0))
        .fees(FeeModel::Flat(0.001))
        .periods_per_year(252 * 24 * 60) // per-minute Sharpe
        .build()?,
    Arc::new(MySmaCrossBrain::new()),
)
.with_candles(candles)
.run()
.await?;

println!("{}", result.summary());
println!("sharpe : {:?}", result.sharpe_ratio());
println!("sortino: {:?}", result.sortino_ratio());
```

### Multi-symbol replay

For portfolio strategies, attach a candle series per symbol. The engine
merges all series chronologically before replay and maintains
independent `Position` state per symbol against a single shared cash
balance.

```rust,ignore
let result = Backtest::new(
    BacktestConfig::builder()
        .symbols(["BTCUSDT", "ETHUSDT"])
        .initial_cash(100_000.0)
        .build()?,
    Arc::new(MyPortfolioBrain::new()),
)
.with_symbol_candles("BTCUSDT", load_csv("data/btc.csv")?)
.with_symbol_candles("ETHUSDT", load_csv("data/eth.csv")?)
.run()
.await?;
```

### Perp funding

For perpetual-futures strategies — funding-capture edges above all —
attach a `FundingModel` so settlements hit the ledger the way they hit
the exchange account. Positive rate → longs pay, shorts receive;
settlements accrue onto each closed trade's `TradeOutcome::funding` and
total up in `BacktestResult::{funding_received, funding_paid}`. Off by
default: results without a model are bit-identical to previous releases.

```rust,ignore
use rustrade_backtest::{FundingModel, EIGHT_HOURS_MS};

// Historical series when you have one…
.funding(FundingModel::Series(settled_rates)) // Vec<(timestamp_ms, rate)>
// …or a constant rate + interval fallback (1 bp every 8 h, the
// KuCoin/Binance grid).
.funding(FundingModel::Constant { rate: 0.0001, interval_ms: EIGHT_HOURS_MS })
```

## Brain parity

Any `impl rustrade_core::Brain` that runs through the live `rustrade::Bot`
runs in this engine unchanged. See `tests/sma_replay.rs` for the
regression test that pins this down: the same brain emits the same
sequence of decisions for the same candle series in both code paths.

## Status

Phase 4b — adds CSV candle loader, Sharpe / Sortino, and multi-symbol
replay. Parquet loaders and book-walk slippage remain deferred. See the
workspace [`TODO.md`](../../TODO.md).

## Licence

MIT.
