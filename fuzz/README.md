# rustrade fuzz targets

Coverage-guided fuzzing for the parts of rustrade that ingest untrusted
input. Currently one target:

| Target     | Entry point                                   |
| ---------- | --------------------------------------------- |
| `load_csv` | `rustrade_backtest::load_csv_str` (CSV loader) |

## Why this is a separate, excluded crate

`cargo-fuzz` builds with libFuzzer and `-Zsanitizer=address`, which need a
**nightly** toolchain. This crate is excluded from the workspace
(`exclude = ["fuzz"]` in the root `Cargo.toml`, plus its own empty
`[workspace]` table) so the stable CI matrix never tries to build it. The
every-PR safety net on stable is the proptest in
`crates/rustrade-backtest/tests/loader_robustness.rs`, which asserts the
same "never panics, never emits a non-finite candle" invariant; this
harness is for deeper, coverage-guided exploration.

## Running

```bash
cargo install cargo-fuzz          # one-time
cargo +nightly fuzz run load_csv  # fuzz until Ctrl-C or a crash

# bounded run (e.g. CI on a schedule):
cargo +nightly fuzz run load_csv -- -max_total_time=60
```

Crashing inputs are written to `fuzz/artifacts/load_csv/`; reproduce with:

```bash
cargo +nightly fuzz run load_csv fuzz/artifacts/load_csv/<crash-file>
```

## Invariant

`load_csv_str` must, for **any** byte input:

- never panic, abort, or hang, and
- on `Ok`, return only finite, positive OHLC and finite, non-negative
  volume (the `Error::Data` guard rejects everything else).
