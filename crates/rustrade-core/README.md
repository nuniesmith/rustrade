# rustrade-core

Type-layer foundation for the [`rustrade`](../../README.md) trading bot
framework. Defines the domain types, trait contracts, and broadcast buses
that every other rustrade crate (and every downstream service depending
on `rustrade`) builds on.

This crate is deliberately dependency-light:

- No I/O — no HTTP, no Redis, no exchange clients
- No async runtime state — only tokio's `broadcast` primitive
- No optional features — everything compiles unconditionally

Downstream crates layer the runtime concerns on top:
[`rustrade-supervisor`](../rustrade-supervisor/) for service lifecycle,
[`rustrade-risk`](../rustrade-risk/) for risk primitives,
[`rustrade-backtest`](../rustrade-backtest/) for replay (planned),
and the facade [`rustrade`](../rustrade/) crate for end users (planned).

## What lives here

| Module      | Contents                                                                  |
| ----------- | ------------------------------------------------------------------------- |
| `brain`     | [`Brain`] trait, [`Decision`], [`SizeHint`], [`BrainHealth`]              |
| `bus`       | [`MarketDataBus`], [`SignalBus`] — broadcast channels for events/signals  |
| `error`     | Framework-wide [`Error`] enum and [`Result`] alias                        |
| `exchange`  | [`ExchangeClient`], [`MarketSource`], [`FillSource`], [`EventSource`]     |
| `market`    | [`Side`], [`Symbol`], [`Exchange`], [`MarketDataEvent`]                   |
| `signal`    | [`Signal`], [`SignalType`]                                                |
| `types`     | [`Price`], [`Volume`], [`Tick`], [`Candle`], [`Order`], [`Fill`], [`Position`] |

## Design rules

1. Numeric quantities use newtype wrappers (`Price`, `Volume`) to prevent
   unit-confusion bugs at compile time.
2. Traits are narrow — implementors should not have to stub methods they
   don't care about.
3. Traits are object-safe where possible. `Arc<dyn Brain>` and
   `Arc<dyn ExchangeClient>` must keep working.

## Status

See the workspace [`TODO.md`](../../TODO.md) for the Phase 1 trait-surface
lockdown items (symbol newtype consistency, leverage / stop-orders /
contract-multipliers design decisions, missing unit tests).

## Licence

MIT.
