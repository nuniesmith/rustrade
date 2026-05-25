# Your first rustrade bot in 50 lines

This walks through building the smallest possible working bot — a brain
that does nothing, a mock exchange that pretends to fill orders, a 10-
second runtime. The same shape scales to a production bot; the only
things that change are the `Brain` and `ExchangeClient` impls.

The full code is in [`examples/noop-bot/`](../examples/noop-bot) — you
can run it with `cargo run -p noop-bot`. Below is an annotated walk-
through.

## 1. `Cargo.toml`

```toml
[package]
name = "my-bot"
version = "0.1.0"
edition = "2024"

[dependencies]
rustrade    = "0.1"   # the facade — re-exports everything you need
async-trait = "0.1"
anyhow      = "1"
tokio       = { version = "1", features = ["rt-multi-thread", "macros"] }
```

Downstream services depend on a single `rustrade` crate. No need to
pick `rustrade-core` / `rustrade-supervisor` / `rustrade-risk`
individually — they're all re-exported.

## 2. Implement a `Brain`

A `Brain` is the strategic layer. It receives market events, sees the
current position, and returns a `Decision`. `Decision::hold` is always
safe.

```rust
use async_trait::async_trait;
use rustrade::{Brain, Decision, MarketDataEvent, Position, Result};

struct NoopBrain;

#[async_trait]
impl Brain for NoopBrain {
    fn name(&self) -> &str {
        "noop"
    }

    async fn on_event(
        &self,
        _event: &MarketDataEvent,
        _position: &Position,
    ) -> Result<Decision> {
        Ok(Decision::hold())
    }
}
```

Real brains use interior mutability for indicator state — see
[`examples/sma-cross-bot/`](../examples/sma-cross-bot) for a working
SMA-crossover.

## 3. Implement an `ExchangeClient`

Every framework operation that talks to the exchange — order placement,
cancellation, position lookup — goes through this trait. Adapters for
KuCoin, Binance, etc. live in their own crates; we'll stub it here.

```rust
use async_trait::async_trait;
use rustrade::{ExchangeClient, Order, Position, Result, Symbol};

struct MockExchange;

#[async_trait]
impl ExchangeClient for MockExchange {
    fn name(&self) -> &str { "mock" }
    async fn place_order(&self, _o: &Order)           -> Result<String> { Ok("ok".into()) }
    async fn cancel_all (&self, _s: &Symbol)          -> Result<usize>  { Ok(0) }
    async fn close_position(&self, _s: &Symbol, _p: &Position) -> Result<String> { Ok("close".into()) }
    async fn get_position(&self, _s: &Symbol)         -> Result<Position> { Ok(Position::FLAT) }
    async fn get_balance (&self, _ccy: &str)          -> Result<f64> { Ok(0.0) }
}
```

`ExchangeClient` is small on purpose — the framework only needs what's
necessary to trade. Native features like stop orders, sub-accounts, or
funding history live on the adapter's concrete type.

## 4. Wire the `Bot`

```rust
use std::sync::Arc;
use std::time::Duration;
use rustrade::{Bot, BotConfig};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    let bot = Bot::new(
        BotConfig::builder()
            .name("my-first-bot")
            .symbol("BTCUSDT")
            .without_signal_handler()  // host drives shutdown
            .shutdown_timeout(Duration::from_secs(5))
            .build()?,
        Arc::new(MockExchange),
        vec![Arc::new(NoopBrain)],
    )?;

    let handle = bot.handle();
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    tokio::time::sleep(Duration::from_secs(10)).await;
    handle.shutdown();
    bot_task.await??;
    Ok(())
}
```

Five things to notice:

1. **`BotConfig::builder()`** validates every field on `.build()`.
   Empty symbol list, zero shutdown timeout, NaN risk parameters — all
   return `Error::Config` rather than panicking.
2. **`.without_signal_handler()`** tells the bot the host owns Ctrl-C.
   For binaries that don't have their own signal handling, remove this
   line and the bot installs its own.
3. **`bot.handle()`** returns a `Clone`-able `BotHandle`. Hold on to one
   before `run_until_shutdown` — once that future is awaited, the bot
   value is consumed.
4. **`run_until_shutdown` returns after drain.** When `handle.shutdown()`
   is called, the supervisor cancels every service, waits for them to
   exit (up to `shutdown_timeout`), and returns.
5. **Multi-thread tokio runtime is required.** `tokio::spawn` is used
   internally; a current-thread runtime works but loses per-service
   parallelism.

## 5. Run it

```sh
cargo run -p my-bot
```

You'll see structured `tracing` output for the framework's lifecycle
events, finishing with `rustrade Bot exited`. No orders were placed —
`NoopBrain` returns `Decision::hold()` for every event.

## Next steps

| You want to…                                            | See                                          |
| ------------------------------------------------------- | -------------------------------------------- |
| Run a real strategy against canned data                 | [`examples/sma-cross-bot/`](../examples/sma-cross-bot) |
| Run two brains in one bot                               | [`examples/multi-brain-bot/`](../examples/multi-brain-bot) |
| Embed the bot into a larger host service                | [`examples/embed-in-service/`](../examples/embed-in-service) |
| Replay candles offline through the same brain interface | [`rustrade-backtest`](../crates/rustrade-backtest) |
| Implement an exchange adapter                           | [`rustrade-core::ExchangeClient` rustdocs](https://docs.rs/rustrade) |
| Tune the risk gates                                     | `BotConfigBuilder::session_pnl_config` / `circuit_breaker_config` / `sizing_config` |
| Subscribe to brain signals from the host                | `BotHandle::subscribe_signals` |
