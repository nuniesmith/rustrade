# rustrade

Open-source trading bot framework in Rust. Provides the scaffolding —
service lifecycle, supervision, risk primitives, buses, traits — that every
trading bot rewrites from scratch. Plug in your own exchange adapter,
indicator stack, and strategy (`Brain`) and you get a production-ready bot.

> **Status: 0.3.0, published on crates.io.** All five crates are complete
> and tested — core, supervisor, risk, backtest, and the facade. CI is green
> on Linux + macOS across MSRV (1.94.1) and stable, with ~91% line coverage.
>
> The facade is published as **`rustrade-framework`** (the bare `rustrade`
> name on crates.io belongs to an unrelated project) but is still imported
> as `rustrade` — add `rustrade-framework = "0.3"` to your `Cargo.toml` and
> write `use rustrade::{Bot, BotConfig};` as usual.

---

## Design in one diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         YOUR SERVICE                            │
│  (kucoin-bot, binance-bot, janus-bin, …)                        │
│                                                                 │
│  fn main() {                                                    │
│    let exchange = Arc::new(KucoinExchangeAdapter::new(..));     │
│    let brain    = Arc::new(MySarBrain::new(..));                │
│    Bot::new(config, exchange, vec![brain])                      │
│       .run_until_shutdown().await                               │
│  }                                                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                       rustrade (facade)                         │
│    Bot builder, logging setup, ergonomic re-exports             │
└──────┬──────────────┬──────────────┬──────────────┬─────────────┘
       │              │              │              │
┌──────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐ ┌─────▼──────┐
│   -core     │ │ -supervisor│ │   -risk    │ │  -backtest │
│             │ │            │ │            │ │            │
│ Types,      │ │ Service    │ │ Position   │ │ Temporal   │
│ Brain,      │ │ lifecycle, │ │ sizer,     │ │ fortress   │
│ Buses,      │ │ backoff,   │ │ breaker,   │ │ replay     │
│ Traits      │ │ circuit    │ │ session    │ │ engine     │
│             │ │ breaker,   │ │ PnL        │ │            │
│             │ │ prometheus │ │            │ │            │
└─────────────┘ └────────────┘ └────────────┘ └────────────┘
       ▲                                              ▲
       │                                              │
       │  (your brain consumes these external crates) │
       │                                              │
┌──────┴──────┐   ┌──────────────┐   ┌────────────────┘
│  exchange-  │   │ indicators-  │   │
│  apiws      │   │ ta           │   │
│ (published) │   │ (published)  │   │
└─────────────┘   └──────────────┘   │
                                     │
                  ┌──────────────────┘
                  │
            ┌─────▼──────┐
            │   janus    │   (private — your brain IP)
            │ neuromorph │
            │ strategies │
            └────────────┘
```

---

## Crates

### `rustrade-core` ✅ complete

Zero-runtime type layer. Defines:

- **Domain types** — `Price`, `Volume`, `Candle`, `Tick`, `Order`, `Fill`, `Position`
- **Market primitives** — `Side`, `Symbol`, `Exchange`, `MarketDataEvent`
- **The `Brain` trait** — the single abstraction every strategy implements
- **`Decision` + `SizeHint`** — intent-vs-execution separation
- **Trait contracts** — `ExchangeClient`, `MarketSource`, `FillSource`, `EventSource`
- **Broadcast buses** — `MarketDataBus`, `SignalBus`

No I/O. No tokio runtime state. No optional features. Every other rustrade
crate depends on this; this one depends on nothing internal.

### `rustrade-supervisor` ✅ complete

Structured service lifecycle. Every long-running task in your bot (WS feed,
candle poller, heartbeat, brain) implements `TradingService` and is spawned
through a `Supervisor` that handles:

- Graceful shutdown via `CancellationToken` propagation
- Exponential-backoff restart (full jitter) with per-service circuit breakers
- Service lifecycle state machine (Starting → Running → BackingOff → Stopping → Terminated)
- Optional Prometheus metrics (feature-gated, crate-local registry)

Full restart loop, lifecycle state machine, and chaos-test suite are in
place (56 unit tests including three chaos tests).

### `rustrade-risk` ✅ complete, 29 passing tests

Generic trading risk primitives. Nothing strategy- or exchange-specific.

- **`CircuitBreaker`** — sliding-window loss breaker. Trips when N losses
  occur within a rolling window; stays open for the configured cooldown
  regardless of intervening wins. Ported from the kucoin Apr 2026 patch.
- **`SessionPnl`** — realised PnL tracker with drawdown cap and automatic
  00:00 UTC rollover. Classifies trades as W/L/B on **net** (after fees),
  so fee-flipped trades count correctly.
- **`PositionSizer`** — notional-based sizing from margin × leverage ÷
  (price × contract_value). Includes `max_contracts` cap and bailout-on-zero
  guard for all degenerate inputs.

### `rustrade-backtest` ✅ complete

Deterministic replay engine driven by the **same** `Brain` trait the live
bot uses — no backtest-specific strategy code. Ships:

- Single-threaded synchronous replay over a `Vec<Candle>` (or multi-symbol
  series merged chronologically)
- Pluggable `SlippageModel` (`Zero`, `FixedBps`) and `FeeModel` (`Zero`,
  `Flat`, `MakerTaker`)
- CSV candle loader (`load_csv` / `load_csv_str` / `sort_chronological`)
- Performance metrics: total return, win rate, profit factor, max drawdown,
  Sharpe / Sortino, plus the full per-trade ledger and equity curve

### `rustrade` ✅ complete

Top-level facade crate — the one most users depend on directly. It pulls in
and re-exports the others and adds the `Bot` builder:

```rust
use rustrade::{Bot, BotConfig};

let config = BotConfig::builder()
    .name("my-bot")
    .symbols(["BTCUSDT", "ETHUSDT"])
    .build()?;

Bot::new(config, exchange, brains)
    .run_until_shutdown()
    .await
```

`Bot` owns a `Supervisor`, your `ExchangeClient`, one or more `Brain`s, and
the in-process buses. Optional services (market feed, fill routing, candle
polling) attach via `with_market_source` / `with_fill_source` /
`with_candle_poller`. A cloneable `BotHandle` exposes shutdown, health, and
signal subscription for the host.

---

## Testing

On rustc 1.94+ with edition 2024:

```
cargo test --workspace --all-features
```

The full suite is ~160 unit + integration + doc tests across the five
crates and the four examples. The same five commands CI runs (`fmt`,
`clippy`, `test`, `doc`, `cargo-deny`) are documented in
[`CONTRIBUTING.md`](./CONTRIBUTING.md).

---

## What's next

The framework is published on crates.io (0.3.0) and is feature-complete for
embedded, multi-asset use. Shipped since 0.1.0 (see [`CHANGELOG.md`](./CHANGELOG.md)):
crates.io publish, a `StateStore` trait with a durable `JsonFileStore` (risk
state survives restarts), `Decision` stops + order kinds, order lifecycle
tracking, SL/TP bracket (OCO) orders, multi-brain arbitration, and an
account-level + per-asset-class risk layer. Remaining candidates, roughly in
priority order (see [`TODO.md`](./TODO.md) for the full list):

1. **Backtest depth:** Parquet loader, book-walk slippage (needs order-book
   replay), expectancy / avg-win-loss metrics, and mirroring live limit-fill
   semantics in the simulated exchange.
2. **A reference exchange adapter** in its own crate, demonstrating the
   `ExchangeClient` / `MarketSource` / `FillSource` contracts end-to-end.

Explicit non-goals for now: an HTTP/gRPC control plane, a built-in indicator
library, and bundled exchange adapters — those live in downstream crates.

---

## Getting started

- **First-time users:** [`docs/quickstart.md`](./docs/quickstart.md) —
  build a working bot in 50 lines.
- **Writing strategies:** [`docs/writing-a-brain.md`](./docs/writing-a-brain.md)
  — the `Brain` trait, state, position handling, the canonical
  `Mutex<State>` pattern.
- **Writing exchange adapters:**
  [`docs/writing-an-exchange-adapter.md`](./docs/writing-an-exchange-adapter.md)
  — `ExchangeClient`, `MarketSource`, `FillSource`, the `Capability`
  enum.
- **Embedding into a host service:**
  [`docs/embedding.md`](./docs/embedding.md) — `BotHandle`, external
  cancellation, signal subscription, runtime + resource expectations.
- **Backtesting:** [`docs/backtesting.md`](./docs/backtesting.md) —
  deterministic replay, slippage / fee models, the brain-identical
  guarantee.
- **Reference embeddings:**
  [`examples/noop-bot`](./examples/noop-bot),
  [`examples/sma-cross-bot`](./examples/sma-cross-bot),
  [`examples/multi-brain-bot`](./examples/multi-brain-bot),
  [`examples/embed-in-service`](./examples/embed-in-service).
- **API docs:** `cargo doc --workspace --no-deps --open`.

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) — toolchain, the five
required commands, branch + commit conventions, versioning policy.

## License

MIT.

