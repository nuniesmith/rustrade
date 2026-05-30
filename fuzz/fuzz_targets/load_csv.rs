//! Coverage-guided fuzz target for the CSV candle loader.
//!
//! Invariant: `load_csv_str` must never panic, abort, or hang on any
//! input — it returns `Ok(candles)` or a structured `Err`. We additionally
//! assert that every candle in a successful parse is finite and sane, so
//! the validation guard can't be silently bypassed.
//!
//! Run (nightly + `cargo install cargo-fuzz`):
//!
//! ```text
//! cargo +nightly fuzz run load_csv
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use rustrade_backtest::load_csv_str;

fuzz_target!(|data: &[u8]| {
    // The loader takes &str; feed it the lossy decode of arbitrary bytes
    // so the fuzzer can explore both valid UTF-8 and replacement paths.
    let s = String::from_utf8_lossy(data);
    if let Ok(candles) = load_csv_str(&s) {
        for c in candles {
            assert!(c.open.is_finite() && c.open > 0.0);
            assert!(c.high.is_finite() && c.high > 0.0);
            assert!(c.low.is_finite() && c.low > 0.0);
            assert!(c.close.is_finite() && c.close > 0.0);
            assert!(c.volume.is_finite() && c.volume >= 0.0);
        }
    }
});
