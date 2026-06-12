//! Pending-entry reservations — closing the portfolio-gate race.
//!
//! The portfolio gate reads the position cache, but a position only
//! appears there after its fill is processed (or never, without a fill
//! source). Between gate-check and fill, the gate is blind to the order
//! it just allowed — so two brains deciding concurrently could *both*
//! pass `max_concurrent_positions` / `max_gross_exposure` and both place.
//!
//! The ledger fixes this by making the check **check-and-reserve**: the
//! execution service holds the ledger lock while it assembles the
//! aggregate state (cache + outstanding reservations), runs
//! `PortfolioRisk::check_entry`, and records its own reservation. A
//! reservation is released when:
//!
//! - the exchange **rejects** the order (released immediately by the
//!   execution service),
//! - the fill lands and the **position cache refresh** makes the position
//!   visible (released by `FillRoutingService`), or
//! - it **expires** after a TTL — the safety net for setups without a
//!   fill source, where the cache never updates anyway. Until expiry the
//!   gate counts the reservation, which errs on the side of blocking;
//!   over-blocking is the safe failure mode for a risk gate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustrade_core::Symbol;
use tokio::sync::{Mutex, MutexGuard};

/// One outstanding reservation: the entry's quote-currency notional and
/// when it was reserved (for TTL expiry).
#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingEntry {
    pub notional: f64,
    reserved_at: Instant,
}

/// The map of outstanding reservations, exposed through
/// [`PendingEntryLedger::lock`] so a gate check and its reservation
/// happen under one critical section.
#[derive(Debug, Default)]
pub(crate) struct PendingMap {
    entries: HashMap<Symbol, PendingEntry>,
}

impl PendingMap {
    /// Drop reservations older than `ttl`.
    fn expire_stale(&mut self, ttl: Duration, now: Instant) {
        self.entries
            .retain(|_, e| now.duration_since(e.reserved_at) < ttl);
    }

    /// Is there an outstanding reservation for `symbol`?
    pub(crate) fn contains(&self, symbol: &Symbol) -> bool {
        self.entries.contains_key(symbol)
    }

    /// Sum of reserved notional across all symbols.
    pub(crate) fn gross_notional(&self) -> f64 {
        self.entries.values().map(|e| e.notional).sum()
    }

    /// Number of reserved symbols for which `is_open` is false — i.e.
    /// reservations that will consume a *new* concurrency slot.
    pub(crate) fn new_slots(&self, mut is_open: impl FnMut(&Symbol) -> bool) -> u32 {
        self.entries
            .keys()
            .filter(|s| !is_open(s))
            .count()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    /// Record (or top up) a reservation for `symbol`.
    pub(crate) fn reserve(&mut self, symbol: Symbol, notional: f64) {
        let now = Instant::now();
        self.entries
            .entry(symbol)
            .and_modify(|e| {
                e.notional += notional;
                e.reserved_at = now;
            })
            .or_insert(PendingEntry {
                notional,
                reserved_at: now,
            });
    }

    /// Drop the reservation for `symbol`, if any.
    pub(crate) fn release(&mut self, symbol: &Symbol) {
        self.entries.remove(symbol);
    }
}

/// Shared, cheaply-cloneable handle to the reservation map. One per bot,
/// shared by every `ExecutionService` and the `FillRoutingService`.
#[derive(Debug, Clone)]
pub(crate) struct PendingEntryLedger {
    inner: Arc<Mutex<PendingMap>>,
    ttl: Duration,
}

impl PendingEntryLedger {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PendingMap::default())),
            ttl,
        }
    }

    /// Lock the map for an atomic check-and-reserve. Stale reservations
    /// are expired on every acquisition, so TTL needs no background task.
    pub(crate) async fn lock(&self) -> MutexGuard<'_, PendingMap> {
        let mut guard = self.inner.lock().await;
        guard.expire_stale(self.ttl, Instant::now());
        guard
    }

    /// Release the reservation for `symbol` (rejection / fill landed).
    pub(crate) async fn release(&self, symbol: &Symbol) {
        self.inner.lock().await.release(symbol);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sym(s: &str) -> Symbol {
        Symbol::from(s)
    }

    #[tokio::test]
    async fn reserve_release_roundtrip() {
        let ledger = PendingEntryLedger::new(Duration::from_secs(30));
        {
            let mut m = ledger.lock().await;
            m.reserve(sym("AAA"), 1_000.0);
            m.reserve(sym("BBB"), 500.0);
            assert!(m.contains(&sym("AAA")));
            assert_eq!(m.gross_notional(), 1_500.0);
            assert_eq!(m.new_slots(|_| false), 2);
            // Symbols already open consume no new slot.
            assert_eq!(m.new_slots(|s| s == &sym("AAA")), 1);
        }
        ledger.release(&sym("AAA")).await;
        let m = ledger.lock().await;
        assert!(!m.contains(&sym("AAA")));
        assert_eq!(m.gross_notional(), 500.0);
    }

    #[tokio::test]
    async fn same_symbol_reservations_accumulate() {
        let ledger = PendingEntryLedger::new(Duration::from_secs(30));
        let mut m = ledger.lock().await;
        m.reserve(sym("AAA"), 1_000.0);
        m.reserve(sym("AAA"), 250.0);
        assert_eq!(m.gross_notional(), 1_250.0);
        assert_eq!(m.new_slots(|_| false), 1, "one symbol, one slot");
    }

    #[tokio::test]
    async fn reservations_expire_after_ttl() {
        let ledger = PendingEntryLedger::new(Duration::from_millis(20));
        ledger.lock().await.reserve(sym("AAA"), 1_000.0);
        tokio::time::sleep(Duration::from_millis(40)).await;
        let m = ledger.lock().await;
        assert!(
            !m.contains(&sym("AAA")),
            "stale reservation must expire on the next lock"
        );
        assert_eq!(m.gross_notional(), 0.0);
    }
}
