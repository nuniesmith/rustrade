# rustrade-risk

Generic risk primitives for the [`rustrade`](../../README.md) trading bot
framework. Nothing strategy- or exchange-specific lives here; if a piece
of logic needs to special-case a particular brain or exchange, it belongs
in the brain implementation, not in this crate.

## What's inside

| Module            | Purpose                                                          |
| ----------------- | ---------------------------------------------------------------- |
| `circuit_breaker` | Sliding-window loss breaker with configurable cooldown           |
| `session_pnl`     | Realised PnL tracker, drawdown cap, automatic 00:00 UTC rollover |
| `sizing`          | Notional-based contract sizing from margin × leverage            |

## Usage

```rust,ignore
use rustrade_risk::{
    CircuitBreaker, CircuitBreakerConfig,
    PositionSizer,  SizingConfig,
    SessionPnl,     SessionPnlConfig,
};

let mut breaker = CircuitBreaker::new(CircuitBreakerConfig::default());
let     sizer   = PositionSizer::new(SizingConfig::default());
let mut pnl     = SessionPnl::new("BTCUSDT", SessionPnlConfig::default());

// Pre-trade gates (this order matters — see the workspace TODO.md):
if pnl.is_session_halted() || breaker.is_tripped() {
    return; // skip this entry
}

let contracts = sizer.contracts(price, contract_value);
// ... place order ...

// Post-close:
pnl.record_close(gross_pnl, fee);
if net < 0.0 { breaker.record_loss(); } else { breaker.record_win(); }
```

## Design notes

- **`CircuitBreaker`** uses a rolling window rather than consecutive
  losses, because losses spaced hours apart would reset a consecutive
  counter before ever tripping it. A single win does *not* clear the
  tripped state — only elapsed cooldown does.
- **`SessionPnl`** classifies trades as W/L/B on **net** (after fees) PnL
  so that fee-flipped trades (small gross win, large fee = real loss) are
  counted correctly.
- **`PositionSizer`** returns `0` on any degenerate input rather than
  panicking; callers must treat `0` as "skip this trade, too small."

## Status

Complete. 13 unit tests + 2 doc tests pass on the workspace's pinned
toolchain. See [`TODO.md`](../../TODO.md) for the small polish items
(`proptest` coverage, injectable clock for time-dependent tests).

## Licence

MIT.
