//! Per-symbol risk state shared between framework services.
//!
//! Lives behind a single `Arc<RwLock<...>>` that the [`Bot`](crate::Bot)
//! constructs and hands to the [`ExecutionService`](crate::execution::ExecutionService)
//! and (eventually) the `FillRoutingService`. The shape is intentionally
//! coarse — one `SessionPnl` and one `CircuitBreaker` per symbol — so a
//! single read-lock acquires everything needed for the pre-trade gate
//! sequence.
//!
//! # PnL feeding
//!
//! Phase 2b does not yet automate "compute realised PnL from a fill". The
//! gates exist and run; what feeds them is the brain or the host calling
//! [`BotHandle::record_trade_outcome`](crate::BotHandle::record_trade_outcome).
//! Future phases may add a built-in PnL computer that watches the fill
//! stream and entry-price cache.

use std::collections::HashMap;
use std::sync::Arc;

use rustrade_core::{Position, StateStore, Symbol};
use rustrade_risk::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerSnapshot, PortfolioRisk,
    PortfolioRiskConfig, SessionPnl, SessionPnlConfig, SessionPnlSnapshot,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Schema version for [`SymbolRiskSnapshot`]. Bump on any incompatible
/// change to the persisted shape; older/newer snapshots are ignored on
/// load (the bot starts fresh) rather than misinterpreted.
pub(crate) const SNAPSHOT_VERSION: u32 = 1;

/// Combined risk primitives held per trading symbol.
#[derive(Debug)]
pub struct SymbolRisk {
    pub session_pnl: SessionPnl,
    pub circuit_breaker: CircuitBreaker,
}

impl SymbolRisk {
    pub fn new(
        symbol: &Symbol,
        pnl_config: SessionPnlConfig,
        breaker_config: CircuitBreakerConfig,
    ) -> Self {
        Self {
            session_pnl: SessionPnl::new(symbol.as_str(), pnl_config),
            circuit_breaker: CircuitBreaker::new(breaker_config),
        }
    }

    /// Capture this symbol's risk state for persistence.
    pub(crate) fn snapshot(&self) -> SymbolRiskSnapshot {
        SymbolRiskSnapshot {
            version: SNAPSHOT_VERSION,
            session_pnl: self.session_pnl.snapshot(),
            circuit_breaker: self.circuit_breaker.snapshot(),
        }
    }

    /// Restore this symbol's risk state from a snapshot, then apply the
    /// stale-snapshot policy via `tick`: a session from an earlier UTC day
    /// rolls over to fresh, losses outside the rolling window are evicted,
    /// and a breaker whose cooldown elapsed during downtime auto-resets.
    /// Config (loss limits) and clock stay those of the live instance.
    pub(crate) fn restore(&mut self, snap: SymbolRiskSnapshot) {
        self.session_pnl.restore(snap.session_pnl);
        self.circuit_breaker.restore(snap.circuit_breaker);
        self.session_pnl.tick();
        self.circuit_breaker.tick();
    }
}

/// Restart-durable snapshot of one symbol's combined risk state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct SymbolRiskSnapshot {
    pub version: u32,
    pub session_pnl: SessionPnlSnapshot,
    pub circuit_breaker: CircuitBreakerSnapshot,
}

/// Stable storage key for a `(bot, symbol)` risk snapshot.
pub(crate) fn state_key(bot_name: &str, symbol: &Symbol) -> String {
    format!("{bot_name}/risk/{}", symbol.as_str())
}

/// Bridges the per-symbol [`RiskStateMap`] to a [`StateStore`]: restores
/// snapshots on startup and persists them after every realised trade and
/// on graceful shutdown.
///
/// Cheaply cloneable — shared into the [`BotHandle`](crate::BotHandle) and
/// the `FillRoutingService` so whichever path records a trade also persists
/// it.
#[derive(Clone)]
pub(crate) struct RiskPersister {
    store: Arc<dyn StateStore>,
    bot_name: String,
}

impl RiskPersister {
    pub(crate) fn new(store: Arc<dyn StateStore>, bot_name: String) -> Self {
        Self { store, bot_name }
    }

    /// Load and apply a snapshot for every symbol currently in `risk`.
    /// Missing snapshots (first boot) and unknown versions are skipped;
    /// genuine load failures are logged and skipped (never fatal).
    pub(crate) async fn restore_into(&self, risk: &RiskStateMap) {
        let symbols: Vec<Symbol> = risk.read().await.keys().cloned().collect();
        for symbol in symbols {
            let key = state_key(&self.bot_name, &symbol);
            match self.store.load(&key).await {
                Ok(Some(value)) => match serde_json::from_value::<SymbolRiskSnapshot>(value) {
                    Ok(snap) if snap.version == SNAPSHOT_VERSION => {
                        if let Some(sr) = risk.write().await.get_mut(&symbol) {
                            sr.restore(snap);
                            tracing::info!(
                                symbol = %symbol,
                                net_pnl = sr.session_pnl.net_pnl(),
                                halted = sr.session_pnl.is_session_halted(),
                                breaker_tripped = sr.circuit_breaker.is_tripped(),
                                "restored risk state from store"
                            );
                        }
                    }
                    Ok(snap) => tracing::warn!(
                        symbol = %symbol,
                        found = snap.version,
                        expected = SNAPSHOT_VERSION,
                        "ignoring risk snapshot with unknown version"
                    ),
                    Err(e) => tracing::warn!(
                        symbol = %symbol,
                        error = %e,
                        "ignoring corrupt risk snapshot"
                    ),
                },
                Ok(None) => {
                    tracing::debug!(symbol = %symbol, "no risk snapshot to restore (first boot)");
                }
                Err(e) => tracing::warn!(
                    symbol = %symbol,
                    error = %e,
                    "failed to load risk snapshot; starting fresh"
                ),
            }
        }
    }

    /// Snapshot a single symbol from the live map and persist it. Used by
    /// the per-trade paths (`record_trade_outcome`, fill auto-PnL) so a
    /// crash right after a realised trade doesn't lose that trade's effect
    /// on the risk gates.
    pub(crate) async fn persist_symbol(&self, risk: &RiskStateMap, symbol: &Symbol) {
        let snap = {
            let map = risk.read().await;
            match map.get(symbol) {
                Some(sr) => sr.snapshot(),
                None => return,
            }
        };
        self.save_snapshot(symbol, snap).await;
    }

    /// Persist an already-captured snapshot (avoids re-locking the map).
    pub(crate) async fn save_snapshot(&self, symbol: &Symbol, snap: SymbolRiskSnapshot) {
        let key = state_key(&self.bot_name, symbol);
        match serde_json::to_value(&snap) {
            Ok(value) => {
                if let Err(e) = self.store.save(&key, value).await {
                    tracing::warn!(symbol = %symbol, error = %e, "failed to persist risk snapshot");
                }
            }
            Err(e) => {
                tracing::error!(symbol = %symbol, error = %e, "failed to serialize risk snapshot")
            }
        }
    }

    /// Snapshot + persist every symbol, then flush. Called on shutdown.
    pub(crate) async fn persist_all(&self, risk: &RiskStateMap) {
        let snaps: Vec<(Symbol, SymbolRiskSnapshot)> = risk
            .read()
            .await
            .iter()
            .map(|(s, sr)| (s.clone(), sr.snapshot()))
            .collect();
        for (symbol, snap) in snaps {
            self.save_snapshot(&symbol, snap).await;
        }
        if let Err(e) = self.store.flush().await {
            tracing::warn!(error = %e, "failed to flush state store on shutdown");
        }
    }
}

/// Shared per-symbol risk state. Cheaply cloneable.
pub type RiskStateMap = Arc<RwLock<HashMap<Symbol, SymbolRisk>>>;

/// Shared per-symbol position cache. Cheaply cloneable.
///
/// Populated on `Bot::run_until_shutdown` startup via
/// `ExchangeClient::get_position`. Phase 2b does not refresh entries
/// after startup; brains that need real-time position awareness should
/// be wired to a `FillSource` in Phase 2c.
pub type PositionCache = Arc<RwLock<HashMap<Symbol, Position>>>;

/// Shared account-wide [`PortfolioRisk`]. Cheaply cloneable — read in the
/// execution pre-trade gate and mutated by the periodic risk sweep.
///
/// Unlike per-symbol risk this is **not** persisted: its only durable state is
/// the daily-loss latch, which the sweep re-derives from the (restored)
/// per-symbol session PnLs on the first tick after a restart.
pub type PortfolioRiskState = Arc<RwLock<PortfolioRisk>>;

/// Build the shared portfolio-risk state from its config.
pub fn build_portfolio_risk(config: PortfolioRiskConfig) -> PortfolioRiskState {
    Arc::new(RwLock::new(PortfolioRisk::new(config)))
}

/// Build a risk-state map seeded with one [`SymbolRisk`] per configured
/// symbol. `cfg_for` resolves the `(SessionPnlConfig, CircuitBreakerConfig)`
/// for each symbol — callers pass a closure that applies any per-symbol
/// override over the bot's default (see `BotConfig::risk_for`).
pub fn build_risk_state(
    symbols: &[Symbol],
    cfg_for: impl Fn(&Symbol) -> (SessionPnlConfig, CircuitBreakerConfig),
) -> RiskStateMap {
    let mut map = HashMap::with_capacity(symbols.len());
    for sym in symbols {
        let (pnl, breaker) = cfg_for(sym);
        map.insert(sym.clone(), SymbolRisk::new(sym, pnl, breaker));
    }
    Arc::new(RwLock::new(map))
}

/// Build an empty position cache. Entries are inserted lazily by
/// `Bot::run_until_shutdown` once the exchange has been queried.
pub fn build_position_cache(symbols: &[Symbol]) -> PositionCache {
    let mut map = HashMap::with_capacity(symbols.len());
    for sym in symbols {
        map.insert(sym.clone(), Position::FLAT);
    }
    Arc::new(RwLock::new(map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustrade_core::InMemoryStore;

    fn cb_cfg(loss_limit: u32) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            loss_limit,
            window_secs: 14_400,
            cooldown_secs: 3_600,
        }
    }

    #[test]
    fn state_key_is_namespaced() {
        assert_eq!(
            state_key("my-bot", &Symbol::from("BTCUSDT")),
            "my-bot/risk/BTCUSDT"
        );
    }

    #[tokio::test]
    async fn persist_then_restore_preserves_trip_and_pnl() {
        let sym = Symbol::from("BTCUSDT");
        let pnl = SessionPnlConfig {
            loss_limit: -1000.0,
        };
        let cb = cb_cfg(2);

        // Trip the breaker + book a loss in map A.
        let map_a = build_risk_state(std::slice::from_ref(&sym), |_| (pnl.clone(), cb.clone()));
        {
            let mut w = map_a.write().await;
            let sr = w.get_mut(&sym).unwrap();
            sr.session_pnl.record_close(-30.0, 1.0); // net -31
            sr.circuit_breaker.record_loss();
            sr.circuit_breaker.record_loss();
            assert!(sr.circuit_breaker.is_tripped());
        }

        let store = Arc::new(InMemoryStore::new());
        let persister = RiskPersister::new(store.clone(), "test-bot".into());
        persister.persist_all(&map_a).await;
        assert_eq!(store.len(), 1);

        // Restore into a fresh map B.
        let map_b = build_risk_state(std::slice::from_ref(&sym), |_| (pnl.clone(), cb.clone()));
        assert!(
            !map_b
                .read()
                .await
                .get(&sym)
                .unwrap()
                .circuit_breaker
                .is_tripped()
        );
        persister.restore_into(&map_b).await;

        let r = map_b.read().await;
        let sr = r.get(&sym).unwrap();
        assert!(
            sr.circuit_breaker.is_tripped(),
            "restored breaker must remain tripped"
        );
        assert!((sr.session_pnl.net_pnl() - (-31.0)).abs() < 1e-9);
        assert_eq!(sr.session_pnl.losses, 1);
    }

    #[tokio::test]
    async fn restore_without_snapshot_is_noop() {
        let sym = Symbol::from("ETHUSDT");
        let map = build_risk_state(std::slice::from_ref(&sym), |_| {
            (SessionPnlConfig::default(), cb_cfg(4))
        });
        let persister = RiskPersister::new(Arc::new(InMemoryStore::new()), "test-bot".into());
        persister.restore_into(&map).await;
        let r = map.read().await;
        let sr = r.get(&sym).unwrap();
        assert!(!sr.circuit_breaker.is_tripped());
        assert!(!sr.session_pnl.is_session_halted());
    }

    #[tokio::test]
    async fn restore_ignores_unknown_version() {
        let sym = Symbol::from("BTCUSDT");
        let map = build_risk_state(std::slice::from_ref(&sym), |_| {
            (SessionPnlConfig::default(), cb_cfg(4))
        });
        let store = Arc::new(InMemoryStore::new());
        // Seed a future-version blob that must be ignored, not misread.
        let bogus = serde_json::json!({
            "version": SNAPSHOT_VERSION + 99,
            "session_pnl": {
                "realised": -100.0, "fees": 0.0, "trades": 1,
                "wins": 0, "losses": 1, "breakevens": 0,
                "halted": true, "last_reset_day": 0
            },
            "circuit_breaker": { "recent_losses": [], "tripped_at_unix_secs": null }
        });
        store
            .save(&state_key("test-bot", &sym), bogus)
            .await
            .unwrap();

        let persister = RiskPersister::new(store, "test-bot".into());
        persister.restore_into(&map).await;
        // Unknown version ignored → fresh state, not the halted blob.
        assert!(
            !map.read()
                .await
                .get(&sym)
                .unwrap()
                .session_pnl
                .is_session_halted()
        );
    }

    #[tokio::test]
    async fn build_risk_state_applies_per_symbol_overrides() {
        // BTC gets a tight 1-loss breaker; ETH keeps a loose 5-loss one.
        let btc = Symbol::from("BTCUSDT");
        let eth = Symbol::from("ETHUSDT");
        let symbols = [btc.clone(), eth.clone()];
        let map = build_risk_state(&symbols, |s| {
            let limit = if s == &btc { 1 } else { 5 };
            (SessionPnlConfig::default(), cb_cfg(limit))
        });

        let mut w = map.write().await;
        // One loss trips BTC (limit 1) but not ETH (limit 5).
        w.get_mut(&btc).unwrap().circuit_breaker.record_loss();
        w.get_mut(&eth).unwrap().circuit_breaker.record_loss();
        assert!(
            w.get(&btc).unwrap().circuit_breaker.is_tripped(),
            "BTC's 1-loss breaker should trip"
        );
        assert!(
            !w.get(&eth).unwrap().circuit_breaker.is_tripped(),
            "ETH's 5-loss breaker should not trip on one loss"
        );
    }
}
