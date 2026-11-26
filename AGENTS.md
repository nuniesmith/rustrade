# AGENTS.md — rustrade

Open-source trading bot framework in Rust (published on crates.io as `rustrade-framework`): service lifecycle, supervision, risk primitives, buses and traits — plug in an exchange adapter, an indicator stack and a strategy (`Brain`) and get a production-ready bot. Five crates: core, supervisor, risk, backtest, facade.

## Shared context

Read the shared agent context first: `soul.md` and `memory.md` (especially §9) in the [agents repo](https://github.com/nuniesmith/agents).

## Rules

- **Downstream is real:** the FKS bots (fks-spawner's `fks-bot-example`, the live crypto bot's scaffolding) consume the published crates. Public API changes are breaking events for them — call them out in the PR and `CHANGELOG.md`, and bump versions properly.
- MSRV is **1.94.1** (see `rust-toolchain.toml`); CI runs Linux + macOS across MSRV and stable. Don't use features past the MSRV.
- Publishing runs through the reusable crates.io lane in `nuniesmith/actions`; per-repo `CRATES_IO_TOKEN`. Follow the existing `publish.yml` / `release.yml` pattern.
- Verify before calling anything done: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` — and a PR is ready only when its own CI is green on its own head.
- This framework is scaffolding, not a strategy: no trading edge lives here, and no live-trading behaviour changes belong in this repo.
