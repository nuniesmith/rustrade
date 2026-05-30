//! Robustness fuzzing for the CSV loader.
//!
//! `load_csv_str` parses untrusted text, so the one invariant that must
//! hold for *any* input is: it never panics. It returns `Ok(candles)` or
//! a structured `Err` — never an unwind, never an OOM-by-construction,
//! never a silent `NaN` candle.
//!
//! This is the stable, every-PR safety net. A coverage-guided
//! `cargo-fuzz` harness over the same entry point lives in `fuzz/` for
//! deeper, nightly-only exploration; see `fuzz/README.md`.

use proptest::prelude::*;
use rustrade_backtest::load_csv_str;

/// Every candle a successful parse returns must be finite and sane — the
/// loader's `Error::Data` guard should have rejected anything else.
fn assert_candles_clean(input: &str) {
    if let Ok(candles) = load_csv_str(input) {
        for c in candles {
            assert!(c.open.is_finite() && c.open > 0.0);
            assert!(c.high.is_finite() && c.high > 0.0);
            assert!(c.low.is_finite() && c.low > 0.0);
            assert!(c.close.is_finite() && c.close > 0.0);
            assert!(c.volume.is_finite() && c.volume >= 0.0);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// Arbitrary bytes (lossy-decoded to UTF-8) must never panic the
    /// parser. This is the closest stable-Rust analogue to throwing
    /// random input at the entry point.
    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let s = String::from_utf8_lossy(&bytes);
        assert_candles_clean(&s);
    }

    /// Structured-but-hostile CSV: a real header plus rows of arbitrary
    /// field tokens (numbers, blanks, `NaN`/`inf`, huge values, junk,
    /// wrong column counts). Exercises the deserialize + validation path
    /// far more densely than raw bytes, which rarely form a valid header.
    #[test]
    fn structured_hostile_rows_never_panic(
        rows in prop::collection::vec(
            prop::collection::vec(
                prop_oneof![
                    Just("".to_string()),
                    Just("NaN".to_string()),
                    Just("inf".to_string()),
                    Just("-inf".to_string()),
                    Just("0".to_string()),
                    Just("-1".to_string()),
                    Just("1e308".to_string()),
                    Just("1e-308".to_string()),
                    Just("99999999999999999999999999".to_string()),
                    "[a-zA-Z0-9.+\\-eE]{0,12}",
                    any::<f64>().prop_map(|f| f.to_string()),
                    any::<i64>().prop_map(|i| i.to_string()),
                ],
                0..8, // arbitrary column count per row
            ),
            0..16, // arbitrary row count
        )
    ) {
        let mut csv = String::from("time,open,high,low,close,volume\n");
        for row in &rows {
            csv.push_str(&row.join(","));
            csv.push('\n');
        }
        assert_candles_clean(&csv);
    }

    /// Comment / blank-line handling must also be panic-free under
    /// arbitrary interleaving with junk rows.
    #[test]
    fn comments_and_blanks_never_panic(
        lines in prop::collection::vec(
            prop_oneof![
                Just("# comment".to_string()),
                Just("".to_string()),
                Just("   ".to_string()),
                "[^\\n]{0,24}",
            ],
            0..24,
        )
    ) {
        let mut csv = String::from("time,open,high,low,close,volume\n");
        for l in &lines {
            csv.push_str(l);
            csv.push('\n');
        }
        assert_candles_clean(&csv);
    }
}

/// A grab-bag of hand-picked adversarial inputs that have historically
/// broken naive CSV parsers. None may panic.
#[test]
fn known_adversarial_inputs_never_panic() {
    let cases = [
        "",
        "\n\n\n",
        "time,open,high,low,close,volume",            // header only
        "time,open,high,low,close,volume\n",          // header + empty
        "garbage",                                    // no header match
        "time,open,high,low,close,volume\n,,,,,",     // all-empty fields
        "time,open,high,low,close,volume\n1,1,1,1,1", // short row
        "time,open,high,low,close,volume\n1,1,1,1,1,1,1,1", // long row
        "time,open,high,low,close,volume\nNaN,NaN,NaN,NaN,NaN,NaN",
        "time,open,high,low,close,volume\n0,inf,-inf,1,1,1",
        "time,open,high,low,close,volume\n9223372036854775807,1,1,1,1,1",
        "\u{feff}time,open,high,low,close,volume\n1,1,1,1,1,1", // BOM
        "time,open,high,low,close,volume\n1,1,1,1,1,1\0",       // NUL
    ];
    for c in cases {
        assert_candles_clean(c);
    }
}
