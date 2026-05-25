# Embedding rustrade in your service

`rustrade` is an embedded library — there's no daemon, no IPC, no
network layer. Your host service depends on the `rustrade` crate and
hosts the bot inside its own tokio runtime. This guide shows how to
tie the bot's lifecycle to a larger system.

## 1. The shape

```rust,ignore
use std::sync::Arc;
use rustrade::{Bot, BotConfig};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Host owns its own shutdown.
    let host_shutdown = CancellationToken::new();

    // Build the bot, tying it to the host's shutdown.
    let bot = Bot::new(
        BotConfig::builder()
            .name("my-service-bot")
            .symbol("BTCUSDT")
            .without_signal_handler()  // host owns Ctrl-C
            .build()?,
        Arc::new(my_exchange_adapter()),
        vec![Arc::new(my_brain())],
    )?
    .with_external_cancel(host_shutdown.clone());

    let handle = bot.handle();

    // Spawn the bot. The host keeps the handle for control.
    let bot_task = tokio::spawn(async move { bot.run_until_shutdown().await });

    // Host's own work — HTTP server, message consumer, scheduler, etc.
    // When it's done (or sees a SIGTERM), it cancels its own token,
    // and the bot drains automatically.
    run_host_service(handle.clone(), host_shutdown).await?;

    bot_task.await??;
    Ok(())
}
```

`examples/embed-in-service/` is a runnable, end-to-end version of this
shape.

## 2. The `BotHandle` contract

`BotHandle` is `Clone` and cheap (Arc-wrapped state). Hold one wherever
your host needs to observe or steer the bot.

| Method                                  | Use                                                |
| --------------------------------------- | -------------------------------------------------- |
| `shutdown()`                            | Fire-and-forget cancellation. Idempotent.          |
| `is_shutting_down() -> bool`            | Non-blocking check.                                |
| `await_shutdown()`                      | Resolves when shutdown is triggered (not when fully drained). |
| `health() -> BotHealth`                 | Aggregate snapshot of every service + every brain. |
| `subscribe_signals() -> Receiver<Signal>` | Stream brain decisions for dashboards / metrics. |
| `record_trade_outcome(symbol, gross, fee)` | Feed realised PnL into the risk gates.          |
| `position(symbol)` / `set_position(...)` | Read or override the cached position.            |

Three things to know:

1. **`await_shutdown` resolves on trigger, not drain.** If your host
   needs to know the bot is fully done, await the `JoinHandle` from the
   `tokio::spawn(bot.run_until_shutdown())` call instead.
2. **Handle outlives the bot.** Once `run_until_shutdown` returns, the
   handle remains usable — `health()` will reflect the terminated
   state, `shutdown()` is a no-op. Useful for post-mortem reporting.
3. **Multiple clones are fine.** Pass them through your service tree
   freely; the underlying `Arc`-wrapped state is shared.

## 3. External cancellation

`Bot::with_external_cancel(token)` installs an internal task that
mirrors the external token into the bot's supervisor token. The host's
shutdown sequence triggers the bot's drain without a linker task on
the host side.

The reverse is **not** wired: calling `handle.shutdown()` does NOT
cancel the external token. The bot is the dependent here.

For services that already model shutdown as "broadcast a signal to
N subsystems", point the bot at the same token:

```rust,ignore
let bot = bot.with_external_cancel(host.shutdown_token());
// ...
host.shutdown();  // cancels every subsystem, including the bot
```

## 4. Publishing market data

The bot owns a `MarketDataBus`. The host (or an adapter) publishes
events to it; the framework's `ExecutionService` subscribes
automatically:

```rust,ignore
let bus = bot.market_data_bus().clone();
tokio::spawn(async move {
    while let Some(event) = my_websocket.next().await {
        bus.publish(event);
    }
});
```

Or wire an adapter with `Bot::with_market_source` and let the framework
supervise it.

## 5. Subscribing to signals

```rust,ignore
let mut signals = handle.subscribe_signals();
let shutdown = host_shutdown.clone();
tokio::spawn(async move {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            r = signals.recv() => match r {
                Ok(sig) => forward_to_dashboard(sig).await,
                Err(_)  => break,  // channel closed
            }
        }
    }
});
```

**Important:** Hold the subscriber's own shutdown signal — `BotHandle`
keeps a `Sender` clone alive, so `signals.recv()` never returns
`Err(Closed)` even after the bot exits. Naïve `while let Ok(...)`
loops hang forever otherwise.

## 6. Feeding the risk gates

The bot's risk gates (`SessionPnl`, `CircuitBreaker`) need to see
realised trade outcomes to do their job. The framework will auto-feed
them once `FillRoutingService` gains entry-price-aware PnL accounting;
until then the host calls:

```rust,ignore
handle.record_trade_outcome(&Symbol::from("BTCUSDT"), gross_pnl, fee).await;
```

Typically this happens in the host's own fill-handling code, right
after computing realised PnL from the entry and exit prices.

## 7. Runtime requirements

- **Multi-thread tokio runtime.** The supervisor uses `tokio::spawn`
  for every service. Current-thread runtimes work but lose
  parallelism.
- **Active runtime context.** `Bot::run_until_shutdown` is `async`;
  invoke it from a task or `#[tokio::main]`. Don't `block_on` it from
  inside another runtime.
- **No `Send` on `Bot`.** Spawning the bot consumes it; observers go
  via `BotHandle`.

## 8. Resource expectations

- **Memory per active symbol:** a few hundred bytes.
- **Channel buffers:** `market_bus_capacity` + `signal_bus_capacity`
  slots, drop-oldest semantics.
- **Shutdown time:** ≤ `shutdown_timeout` (default 30 s); typical < 1 s.
- **Restart-after-crash:** bounded by `BackoffConfig`; defaults 100 ms
  base, 60 s cap, 10-retry circuit breaker over a 10-minute window.

## Next steps

- Runnable reference: [`examples/embed-in-service/`](../examples/embed-in-service)
- [Backtesting](./backtesting.md) — same brain, same `Bot`, offline replay
- [API docs](https://docs.rs/rustrade) — full `BotHandle` surface
