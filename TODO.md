# TODO

Living checklist for `rustrade`. Pairs with [`NEXT_STEPS.md`](./NEXT_STEPS.md)
(original porting plan), [`README.md`](./README.md) (design overview), and
[`CHANGELOG.md`](./CHANGELOG.md) (what actually shipped, per release). When in
doubt, those explain *why* and *what shipped*; this one tracks *what's left*.

Scope reminder (revisited 2026-05):

- **Consumption model:** embedded library. Downstream services depend on the
  `rustrade` crate and call `Bot::new(...).run_until_shutdown().await`. No
  HTTP/gRPC control plane in the core framework (a host can wrap one around
  `BotHandle`).
- **Exchanges:** rustrade ships zero *real* exchange adapters — the framework
  defines `ExchangeClient` / `MarketSource` / `FillSource` / `CandleSource`
  traits; concrete adapters live in downstream crates (e.g. `exchange-apiws`).
  A **simulated/paper** reference adapter is in scope (see 0.3) because it
  proves the trait surface without binding to a venue.
- **Persistence:** in-memory in 0.1. **Promoted to the 0.2 headline** — a
  `StateStore` trait so session PnL / breaker state survive restarts.

---

## Status snapshot

`main` is at **0.2.1** (`workspace.package.version`, all internal path-deps,
and `Cargo.lock` agree; facade publishes as `rustrade-framework`). ~200 tests
(unit + integration + doc + proptest + fuzz + 3 chaos) pass on stable;
`clippy -D warnings` clean; `cargo doc --no-deps` clean. CI runs Linux + macOS
on MSRV (1.94.1) + stable.

> ⚠️ **Docs lag the code.** `README.md:8-11` says "0.2.0"; `CHANGELOG.md` has no
> `0.2.0`/`0.2.1` section and doesn't mention the three latest shipped features
> (per-symbol risk, SL+TP brackets, multi-brain arbitration); there's no git
> tag. **Do:** add the `0.2.0`/`0.2.1` CHANGELOG sections from git history, tag
> `v0.2.1`, and fix the README status line.

| Crate                  | State    | Tests                    |
| ---------------------- | -------- | ------------------------ |
| `rustrade-core`        | shipped  | 30 unit + 10 doc         |
| `rustrade-supervisor`  | shipped  | 56 unit + 3 doc          |
| `rustrade-risk`        | shipped  | 29 unit + 5 doc          |
| `rustrade-backtest`    | shipped  | 38 unit + 13 integ + doc |
| `rustrade` (facade)    | shipped  | 13 unit + 19 integ       |
| examples (4)           | shipped  | run end-to-end           |

---

## 0.3+ — Multi-asset, risk-aware, brain-driven trading  ◀ NET-NEW

> Driven by the cross-repo goal: a (janus) brain trading **multiple asset
> classes under explicit risk rules** through this framework. See
> `fks-full/docs/MULTI_ASSET_BRAIN_ROADMAP.md`. These items are **not** covered
> by the 0.2–0.5 milestones below — the per-symbol risk tier and trait surface
> are solid, but the portfolio/asset-class layer and a real exchange adapter are
> greenfield. Sequenced by leverage.

### A — A real exchange adapter (the #1 gap; pairs with 0.3a sim adapter)
- [ ] **`exchange-apiws` → `ExchangeClient` adapter.** Today the only impls are
      `StubExchange`/`NoopExchange`/`MockExchange` — nothing places a real order.
      Build the bridge (likely a separate crate, `rustrade-exchange-apiws`, so
      the core stays venue-free) over exchange-apiws's signed clients
      (KuCoin `rest/orders`, `BybitPrivateClient`). Impl `MarketSource`/
      `FillSource`/`CandleSource` too. Advertise `Capability` truthfully.
- [x] **Instrument metadata on the trait.** `InstrumentSpec` (contract value,
      tick size, lot size, min notional, **asset class**) returned by
      `ExchangeClient::instrument_spec`; the execution service sizes from it,
      enforces min-notional, and snaps limit prices to the tick. *(Shipped — see
      "Shipped since 0.2".)* Remaining nicety: richer `Symbol` parsing.

### B — Portfolio / account-level risk (entirely absent today)
> Every `SessionPnl` / `CircuitBreaker` is **per-symbol**. Trading more than one
> symbol needs account-wide controls.
- [x] **`PortfolioRisk`** in `rustrade-risk`: account-wide daily-loss halt,
      **gross-exposure cap**, **max concurrent positions** — checked in the
      execution pre-trade gate. *(Shipped.)* Remaining: net-exposure + an explicit
      buying-power budget (needs a cached account balance).
- [ ] **Per-asset-class `RiskConfig` presets** (crypto-perp / spot / FX / futures):
      different leverage/stop/size rules per class, keyed off `InstrumentSpec`
      (now that `AssetClass` exists). *(Track 2.3 — next.)*
- [x] **Wire `SessionPnl::tick()` / `CircuitBreaker::tick()` into a periodic
      sweep.** `RiskSweepService` ticks per-symbol + portfolio risk on a cadence
      so the daily halt rolls over at UTC midnight in a live run. *(Shipped.)*

### C — Durable risk state (trait exists, no real impl)
- [ ] **`JsonFileStore` (or sqlite) `StateStore`.** The trait + `Bot::with_state_store`
      + restore-on-boot are wired, but the only impl is in-memory, so risk state
      does **not** survive restart out of the box. (Overlaps 0.2a.)

### D — Backtest fidelity for live order types (so a risk brain backtests as it trades)
- [ ] **Apply the risk gates in backtest.** The engine ignores
      `SessionPnl`/`CircuitBreaker` today, so a backtest won't reproduce live
      gating. Thread `RiskConfig` through `BacktestConfig` + engine.
- [ ] **Honour limit/stop fills** (today taker-at-close) + **funding model** for
      perps + **portfolio-level metrics** (expectancy, avg win/loss, per-asset).
      (Overlaps 0.4a/0.4b — called out here because it's load-bearing for
      multi-asset risk validation.)

### E — Ensemble support (for a multi-model brain)
- [ ] **Multi-brain-per-symbol netting/arbitration.** Today: startup rejection
      (`owned_symbols`) or unguarded coexistence (`None`). An ensemble that
      genuinely shares a symbol has no netting layer — two `None` brains can place
      opposing orders. A `BrainComposer` / netting service would close this.

---

## Shipped in 0.1.0 (Phases 0–6)

Condensed — full detail is in `CHANGELOG.md` and the PR history (#1–#23).

- **Phase 0** — workspace hygiene: `.gitignore`, `rust-toolchain.toml`,
  `CHANGELOG.md`, `CONTRIBUTING.md`, per-crate READMEs, `.editorconfig`,
  `rustfmt.toml`, `clippy.toml`.
- **Phase 1** — supervisor port (backoff + lifecycle + restart policies +
  3 chaos tests), `rustrade-core` trait lockdown (`Symbol` newtype,
  `StopAttachment`, `Capability`, `contract_value`), risk-crate polish
  (`Clock`/`ManualClock`, proptest sizer).
- **Phase 2** — the `rustrade` facade: `Bot`/`BotConfig`/`BotHandle`,
  risk-gated `ExecutionService`, `MarketFeedService` / `FillRoutingService` /
  `CandlePollerService`, `MetricsSink` + `CandleSource`, auto-PnL feeding,
  external `CancellationToken`, signal subscription.
- **Phase 3** — four examples (`noop`, `sma-cross`, `multi-brain`,
  `embed-in-service`) doubling as integration tests.
- **Phase 4** — `rustrade-backtest`: deterministic replay on the same `Brain`
  trait, slippage/fee models, CSV loader, Sharpe/Sortino, multi-symbol.
- **Phase 5** — service-integration ergonomics: config validation, channel
  capacities, documented resource SLAs.
- **Phase 6** — docs (quickstart + 4 tutorials), CI (fmt/clippy/test/doc/deny),
  dependabot, `# Example` rustdoc on every public item, `cargo-llvm-cov`
  coverage in PR comments.
- **Post-0.1 hardening (unreleased)** — virtual-time tests, brain-panic
  isolation, CSV fuzzing, multi-symbol determinism fix, NaN/inf candle guards.
  *(Not yet versioned — see 0.1.1 below.)*

The four original open design decisions (leverage, stop orders, contract
multipliers, parameter overrides) are all resolved in code — see the
**Resolved design decisions** section near the bottom.

---

## Shipped since 0.2 (unreleased)

- **Account-level `PortfolioRisk`** (`rustrade-risk`): an account-wide
  daily-loss halt (latching, with 00:00 UTC rollover), a max-concurrent-positions
  cap, and a gross-exposure cap. Wired as a third pre-trade gate in the
  execution service (entries only); account net PnL is derived from the
  per-symbol session PnLs, so there's a single source of truth. Configured via
  `BotConfig::portfolio` (`BotConfigBuilder::portfolio_config`); defaults to
  all-off so existing bots are unaffected.
- **`RiskSweepService`**: a supervised periodic sweep that `tick()`s every
  symbol's `SessionPnl`/`CircuitBreaker` and the `PortfolioRisk` during a live
  run — previously `tick()` only ran on restart, so a long-running bot never
  rolled its daily-loss halt over at UTC midnight.
- **`InstrumentSpec` + `AssetClass`** (`rustrade-core`): instrument metadata on
  `ExchangeClient` (`instrument_spec(&Symbol)`) — contract value, price tick,
  quantity lot, min notional, and asset class — generalising the single
  `contract_value` hook. The execution service now sizes from the spec, enforces
  the min-notional, and snaps limit prices to the tick (all no-ops under the
  permissive default, so existing adapters are unaffected). The foundation for
  class-aware rules.

> These land the framework side of the FKS multi-asset risk roadmap (Track 2.1,
> 2.2, 2.4). Bots consume them once a new `rustrade-framework` is published.

---

# Roadmap

Milestones below are **dependency order, not a calendar**. The active focus is
0.2 (live-trading hardening); the rest is captured so nothing is lost.

## 0.1.1 — cut the patch release (½ day)

The `[Unreleased]` block in `CHANGELOG.md:10` already holds real, shipped
hardening (resilience tiers). Stamp it so downstream git-dep users can pin it.

- [ ] Promote `CHANGELOG.md` `[Unreleased]` → `[0.1.1]` with the date; open a
      fresh `[Unreleased]`.
- [ ] Bump `workspace.package.version` to `0.1.1` (`Cargo.toml:54`) and the
      internal path-dep versions (`Cargo.toml:93-97`).
- [ ] Tag `v0.1.1`; update the README status line (`README.md:8-11`) test/
      coverage counts to match.
- [ ] Document the MSRV-install caveat seen in CI/containers (the `1.94.1`
      toolchain can fail a component re-sync; `stable` is the fallback) in
      `CONTRIBUTING.md`.

## 0.2 — Live-trading hardening  ◀ NEXT UP

The gap between "passes tests" and "safe to run real money." Each track is
independently reviewable. Together they close the production holes and unblock
porting a real strategy (e.g. the kucoin SAR bot) on top of the facade.

### 0.2a — Persistence (`StateStore`)

A crash mid-session currently resets `SessionPnl` + `CircuitBreaker`, so the
daily drawdown cap and loss-streak breaker are **forgotten on restart** —
the single biggest production risk hole.

- [ ] `StateStore` trait in `rustrade-core` (async, object-safe): `load`,
      `save`, `flush`. Keyed by `(bot_name, symbol)`. Versioned, serde-backed
      snapshots so the schema can evolve.
- [ ] Snapshot/restore for `SessionPnl` (`crates/rustrade-risk/src/session_pnl.rs`)
      and `CircuitBreaker` (`crates/rustrade-risk/src/circuit_breaker.rs`):
      `to_snapshot()` / `from_snapshot()` that round-trip realised PnL, the
      sliding loss window, cooldown deadline, and the UTC session date.
- [ ] `InMemoryStore` (default, current behaviour) + one durable impl —
      `JsonFileStore` first (no new heavy deps); sqlite behind a feature flag
      as a follow-up.
- [ ] Wire into `Bot`: `Bot::with_state_store(Arc<dyn StateStore>)`. Restore
      per-symbol risk on `run_until_shutdown` startup (alongside the existing
      `prefetch_positions`); persist on every `record_close` and on graceful
      shutdown. Define the **stale-snapshot policy** (e.g. ignore a snapshot
      whose session date != today so a day-old breaker doesn't wrongly halt).
- [ ] Integration test: boot → trip breaker → drop bot → re-boot with the same
      store → breaker is still tripped and session PnL is preserved.

### 0.2b — Rich order intents (stops, limits, TIF)

`Decision` already carries `stop_price` / `take_profit_price`
(`crates/rustrade-core/src/brain.rs:88-91`) but `ExecutionService::build_order`
(`crates/rustrade/src/execution.rs:188-240`) **ignores them** and always emits
a bare `Order::market(...)` (`:233`). Protective stops are half-wired; limit
entries can't be expressed at all.

- [ ] Honour `decision.stop_price` / `take_profit_price`: attach a
      `StopAttachment` (`crates/rustrade-core/src/types.rs:168`) to the entry
      order, or place a paired reduce-only protective order, when the adapter
      advertises `Capability::StopOrders` (`exchange.rs:25`). When it does not,
      log + skip the attachment (never silently drop without a trace).
- [ ] Express **order kind + limit price** from a brain. Add an
      `OrderKind`/`limit_price` (or a small `OrderIntent`) to `Decision` so a
      brain can request `Limit` / `PostOnly` / `Ioc` / `Fok`
      (`OrderKind` already exists, `types.rs:129`). Execution builds the right
      `Order` and sizes it consistently with the market path.
- [ ] Decide market-vs-limit default and document it; keep `Decision::buy/sell`
      defaulting to market so existing brains are unaffected.
- [ ] Mirror the new order kinds in the backtest fill model (links to 0.4a) so
      a stop/limit brain backtests the same way it trades live.

### 0.2c — Order lifecycle & reconciliation

`place_order` returns an id that is then forgotten
(`crates/rustrade/src/execution.rs:163-184`). No resting-order tracking, no
cancel-on-timeout, no reconnect reconciliation — fine for market-only, required
once 0.2b lands limit orders.

- [ ] Extend `ExchangeClient` (`crates/rustrade-core/src/exchange.rs:104`):
      `get_open_orders(&Symbol) -> Vec<OpenOrder>` and
      `cancel_order(&Symbol, order_id)`, both with conservative defaults so
      existing adapters keep compiling.
- [ ] `OrderTracker` in the facade: remember submitted client_ids, expose them
      on `BotHealth`, and cancel unfilled limit orders after a configurable
      TTL.
- [ ] Reconnect reconciliation: on `MarketFeedService` / `FillRoutingService`
      restart, reconcile tracked orders + cached position against the exchange
      so a missed fill during a gap doesn't desync risk state.

### 0.2d — Per-symbol risk + multi-brain safety

- [ ] Per-symbol `RiskConfig` overrides. Today `build_risk_state`
      (`crates/rustrade/src/risk_state.rs:58`) clones one config across every
      symbol; let `BotConfig` carry per-symbol overrides (different vol → different
      drawdown cap / breaker thresholds) layered over a default.
- [ ] Multi-brain-per-symbol arbitration. Each brain gets its own
      `ExecutionService` (`execution.rs:50`); two brains trading the same symbol
      can place opposing orders and fight over one position. Either add a netting/
      arbitration layer keyed by symbol, or add a startup guard that rejects
      overlapping `(brain, symbol)` ownership — and document the chosen model.
- [ ] Document the position-cache staleness window (`risk_state.rs:48-54`):
      between fills, execution reads a possibly-stale position; add an optional
      periodic refresh for adapters without a private fill feed.

## 0.3 — Reference adapter + crates.io publish

Make the framework provable end-to-end and installable.

### 0.3a — Simulated / paper-trading reference adapter

Every end-to-end path today is exercised only through mocks. A sim adapter
proves the trait surface, gives new users a runnable starting point, and
enables paper trading — all without binding to a real venue.

- [ ] `SimulatedExchange` implementing `ExchangeClient` + `FillSource`:
      in-memory matching against the live `MarketDataBus` (fill market orders
      at the next tick, rest limit orders until crossed), configurable
      slippage/fees reusing the `rustrade-backtest` models, synthetic position
      + balance accounting.
- [ ] Honour `Capability` truthfully (advertise `StopOrders`, `ReduceOnly`,
      `PrivateFeed`) so it exercises the 0.2b/0.2c paths.
- [ ] `examples/paper-trading-bot/` wiring a real brain to the sim adapter for
      a self-contained, fills-and-all demo (the current examples stop at
      mocks).

### 0.3b — Release engineering

Deferred from Phase 6c — design against a real publish target now.

- [ ] `cargo publish` driver: publish in dependency order (core → supervisor →
      risk → backtest → rustrade), gated on a clean tag.
- [ ] `cargo-semver-checks` in CI to catch accidental breaking changes to the
      public ABI before they ship.
- [ ] Release workflow (`.github/workflows/release.yml`): on tag, verify,
      publish, attach `CHANGELOG` notes.
- [ ] Update README install instructions from git-dep to `cargo add rustrade`
      once live.

## 0.4 — Backtest & research depth

Pull the deferred Phase 4 items forward and add a research loop.

### 0.4a — Fill realism

- [ ] Limit-order fills in the engine. `engine.rs:183-188` fills every order as
      a taker at candle close; model resting limit fills (filled when the
      candle's high/low crosses the limit) so 0.2b limit brains backtest
      faithfully.
- [ ] Funding-rate model for perpetuals — periodic funding cashflows on open
      positions, configurable schedule. Materially affects perp PnL and is
      modelled nowhere today (live or backtest).
- [ ] Book-walk slippage (`SlippageModel`) — needs an order-book replay, not
      just candles; gate behind an optional book-data input.
- [ ] `BacktestBus` type that makes the zero-lookahead invariant structural
      (the engine guarantees it by construction today; a type would prevent
      future regressions).

### 0.4b — Research ergonomics

- [ ] Parquet candle loader alongside `load_csv` (`crates/rustrade-backtest/src/loaders.rs`).
- [ ] Walk-forward / parameter-sweep harness: run a brain factory across a grid
      or date windows, collect `BacktestResult`s, aggregate. Enables Optuna-style
      tuning the design already anticipates.
- [ ] Expectancy / avg-win / avg-loss + portfolio-level metrics on
      `BacktestResult` (`crates/rustrade-backtest/src/result.rs`) — derivable
      from the ledger today; promote to first-class.
- [ ] Trade-ledger + equity-curve export (CSV/JSON) and an optional HTML/plot
      report for eyeballing a run.

## 0.5 — Observability & ops

`MetricsSink` (`crates/rustrade-core/src/metrics.rs`) exists; the supervisor
has feature-gated Prometheus. Close the loop to a real backend.

- [ ] `rustrade-prometheus` crate (or facade feature): a `MetricsSink` impl
      that registers/serves the counters/histograms the framework already
      emits (`rustrade_fills_routed_total`, `rustrade_candles_published_total`,
      `rustrade_realised_pnl_quote`, …).
- [ ] `examples/observable-bot/` exporting metrics to a scrape endpoint +
      surfacing `BotHealth` over HTTP (a host-owned `/health`).
- [ ] Optional OpenTelemetry tracing layer alongside `logging::init_tracing`.
- [ ] `criterion` benchmarks for hot paths: backtest throughput
      (candles/sec) and `MarketDataBus` fan-out, to catch perf regressions.
- [ ] Coverage threshold gate in CI (warn/fail under N% line coverage).
- [ ] `cargo-audit` weekly scheduled job (currently subsumed by `cargo-deny`'s
      advisory check; split out if a separate report pipeline is wanted).

## Backlog / 1.0+

Bigger bets, deliberately deferred. Listed so contributors don't build them by
accident — revisit when there's a concrete pull.

- [ ] HTTP/gRPC control plane around `BotHandle` (start/stop/params/health) —
      likely a downstream crate, not core.
- [ ] Strategy ensemble / brain composer (an outer `Brain` that blends inner
      brains' decisions) — the trait already allows it; ship a reference impl.
- [ ] Live + replay hybrid: warm-start a brain's indicators from history before
      it goes live.
- [ ] Multi-account / sub-account routing.
- [ ] Built-in indicator library — stays a separate crate (`indicators-ta`);
      `rustrade` only defines the `Brain` that consumes them.
- [ ] Web dashboard.
- [ ] Migrate `docs/` to mdbook if it grows past ~6 files.

---

## Open design decisions for 0.2

Answer these in code, not docs, as each track lands.

- [ ] **StateStore granularity.** One snapshot per `(bot, symbol)`, or one
      per bot? Per-symbol is more parallel and matches `SymbolRisk`; per-bot is
      simpler to make atomic. (Leaning per-symbol.)
- [ ] **Stop semantics.** Does an honoured `decision.stop_price` become a
      native exchange stop (`Order.stop`), or a framework-managed reduce-only
      order the `OrderTracker` watches? Native is simpler but depends on adapter
      `Capability::StopOrders`; framework-managed works everywhere but adds a
      monitoring loop.
- [ ] **Order intent shape.** Extend `Decision` with `kind`/`limit_price`
      fields, or introduce a separate `OrderIntent` the brain returns? Adding
      fields keeps the one-trait simplicity; a struct is cleaner if intents
      grow (brackets, OCO).
- [ ] **Reconciliation source of truth.** On reconnect, trust the exchange's
      position/orders wholesale, or diff against tracked state and alert on
      mismatch? (Leaning trust-exchange + warn-on-diff.)

## Resolved design decisions (0.1)

Kept for the record — see `NEXT_STEPS.md §"Things to explicitly decide"`.

- [x] **Leverage on orders** — per-adapter via constructor; no `Order` change.
- [x] **Stop orders** — `Order.stop: Option<StopAttachment>`, adapter-interpreted,
      gated by `Capability::StopOrders`. *(Wiring brain→order lands in 0.2b.)*
- [x] **Contract multipliers** — `ExchangeClient::contract_value(&Symbol) -> f64`,
      default `1.0`.
- [x] **Parameter overrides** — each subsystem owns its config
      (`SizingConfig`/`SessionPnlConfig`/`CircuitBreakerConfig` → `RiskConfig`);
      brain strategy params stay opaque to the framework.

---

## How to use this file

- Tick boxes as work lands. Reference the closing PR in `CHANGELOG.md`, not here.
- New items: pick the right milestone, keep them actionable ("honour
  `decision.stop_price` in `build_order`" not "improve execution"), and link
  the source with `file:line` when it's a concrete code change.
- Milestone order is a dependency order. 0.2 tracks (a–d) are mostly parallel;
  0.2c (lifecycle) builds on 0.2b (limit orders).
