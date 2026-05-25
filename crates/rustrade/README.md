# rustrade

The facade crate of the [rustrade](../../README.md) trading bot framework.
This is the crate downstream services should depend on — it re-exports the
core types, the supervisor, and the risk primitives, and adds the
[`Bot`](./src/bot.rs) builder that wires them together into a single
embedded runtime.

## Quickstart

```rust,ignore
use std::sync::Arc;
use rustrade::{Bot, BotConfig};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustrade::logging::init_tracing();

    let exchange = Arc::new(my_exchange_adapter::Client::new(...));
    let brain    = Arc::new(my_strategy::Brain::new(...));

    let bot = Bot::new(
        BotConfig::builder()
            .name("kucoin-sar-bot")
            .symbol("XBTUSDTM")
            .build()?,
        exchange,
        vec![brain],
    )?;

    let handle = bot.handle();

    // Tie the bot's lifetime to the host service's shutdown.
    let host_shutdown = my_host_service::shutdown_token();
    tokio::spawn(async move {
        host_shutdown.cancelled().await;
        handle.shutdown();
    });

    bot.run_until_shutdown().await
}
```

## Status

**Phase 2a — minimum viable facade.** `Bot`, `BotConfig`,
`BotHandle`, `BotHealth`, `logging::init_tracing`, and one wired
`ExecutionService` that routes market-data events to brains. Risk gating,
fill routing, candle polling, and pluggable metrics land in Phase 2b/2c —
see the workspace [`TODO.md`](../../TODO.md).

## Licence

MIT.
