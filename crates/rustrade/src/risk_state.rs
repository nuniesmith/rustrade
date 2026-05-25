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

use rustrade_core::{Position, Symbol};
use rustrade_risk::{CircuitBreaker, CircuitBreakerConfig, SessionPnl, SessionPnlConfig};
use tokio::sync::RwLock;

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

/// Build an empty risk state map seeded with one [`SymbolRisk`] per
/// configured symbol.
pub fn build_risk_state(
    symbols: &[Symbol],
    pnl_config: &SessionPnlConfig,
    breaker_config: &CircuitBreakerConfig,
) -> RiskStateMap {
    let mut map = HashMap::with_capacity(symbols.len());
    for sym in symbols {
        map.insert(
            sym.clone(),
            SymbolRisk::new(sym, pnl_config.clone(), breaker_config.clone()),
        );
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
