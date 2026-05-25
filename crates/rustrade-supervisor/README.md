# rustrade-supervisor

Structured service lifecycle for async trading bots. Every long-running
task in a [`rustrade`](../../README.md) bot — market feeds, candle
pollers, heartbeats, brains — implements [`TradingService`] and is
spawned through a [`Supervisor`] that handles graceful shutdown, restart
on failure with exponential backoff, and per-service circuit breakers.

## Why this exists

The naive `tokio::spawn` pattern leaks tasks that silently die, makes
graceful shutdown a coordination nightmare, and has no notion of "this
service has failed 20 times in a row — stop retrying." This crate
replaces that pattern with a small supervision tree that makes failure
modes explicit and observable.

## API at a glance

```rust,ignore
use rustrade_supervisor::{Supervisor, SupervisorConfig, TradingService};

let supervisor = Supervisor::new(SupervisorConfig::default());
supervisor.spawn_service(Box::new(my_service));
supervisor.run_until_shutdown().await?;
```

`TradingService` implementors must honour the `CancellationToken` passed
to `run` — services that don't will hang the shutdown sequence until the
supervisor's drain timeout fires.

## Features

| Feature      | Default | Purpose                                                  |
| ------------ | ------- | -------------------------------------------------------- |
| `prometheus` | off     | Mirror `SupervisorMetrics` atomics into a local registry |

The host service is responsible for exposing the registry (HTTP, push
gateway, etc.) — this crate does not own a server.

## Status

**Skeleton.** The full restart logic, backoff, and lifecycle state
machine are placeholders pending the port from `janus-core/supervisor/`.
See Phase 1 in the workspace [`TODO.md`](../../TODO.md) for the porting
checklist.

## Licence

MIT.
