//! Optional framework-side services wired in via builder methods on
//! [`Bot`](crate::Bot):
//!
//! - [`MarketFeedService`] — `Bot::with_market_source(...)`. Drives a
//!   [`MarketSource`] under supervisor control; the source publishes
//!   events to the in-process [`MarketDataBus`](rustrade_core::MarketDataBus)
//!   (the bus reference is the source implementor's responsibility —
//!   typically obtained via `bot.market_data_bus().clone()` before
//!   construction).
//! - [`FillRoutingService`] — `Bot::with_fill_source(...)`. Polls a
//!   [`FillSource`], calls [`Brain::on_fill`] on each brain, and
//!   refreshes the per-symbol position cache from the exchange so the
//!   next `brain.on_event` call sees the updated position.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rustrade_core::{Brain, ExchangeClient, FillSource, MarketSource};
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio_util::sync::CancellationToken;

use crate::risk_state::PositionCache;

// ───────────────────────────────────────────────────────────────────────
// MarketFeedService
// ───────────────────────────────────────────────────────────────────────

/// Drives a [`MarketSource`] under supervisor control.
///
/// The wrapper does not interact with the bus directly — the source's
/// `run` method is expected to publish events to whatever bus it was
/// constructed with. This service just makes the source restartable and
/// drop-safe under the supervisor's cancellation contract.
pub struct MarketFeedService {
    name: String,
    source: Arc<dyn MarketSource>,
}

impl MarketFeedService {
    /// Wrap a [`MarketSource`] into a [`TradingService`].
    pub fn new(source: Arc<dyn MarketSource>) -> Self {
        let name = format!("market-feed[{}]", source.name());
        Self { name, source }
    }
}

#[async_trait]
impl TradingService for MarketFeedService {
    fn name(&self) -> &str {
        &self.name
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(service = %self.name, "market feed starting");
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!(service = %self.name, "market feed cancelled");
                Ok(())
            }
            r = self.source.run() => {
                match &r {
                    Ok(()) => tracing::info!(service = %self.name, "market feed exited cleanly"),
                    Err(e) => tracing::warn!(service = %self.name, error = %e, "market feed exited with error"),
                }
                r.map_err(|e| anyhow::anyhow!("market source error: {e}"))
            }
        }
    }
}

// ───────────────────────────────────────────────────────────────────────
// FillRoutingService
// ───────────────────────────────────────────────────────────────────────

/// Routes fills from a [`FillSource`] to every brain and refreshes the
/// position cache.
///
/// Does **not** auto-feed realised PnL into the risk state — that would
/// require entry-price-aware PnL accounting which is the brain's
/// concern. Hosts that want gates fed automatically continue to call
/// [`BotHandle::record_trade_outcome`](crate::BotHandle::record_trade_outcome)
/// from their own fill flow.
pub struct FillRoutingService {
    source: Arc<dyn FillSource>,
    brains: Arc<Vec<Arc<dyn Brain>>>,
    exchange: Arc<dyn ExchangeClient>,
    positions: PositionCache,
    fills_routed: AtomicU64,
    refresh_errors: AtomicU64,
}

impl FillRoutingService {
    pub(crate) fn new(
        source: Arc<dyn FillSource>,
        brains: Arc<Vec<Arc<dyn Brain>>>,
        exchange: Arc<dyn ExchangeClient>,
        positions: PositionCache,
    ) -> Self {
        Self {
            source,
            brains,
            exchange,
            positions,
            fills_routed: AtomicU64::new(0),
            refresh_errors: AtomicU64::new(0),
        }
    }

    /// Total fills delivered to brains since service start.
    pub fn fills_routed(&self) -> u64 {
        self.fills_routed.load(Ordering::Relaxed)
    }

    /// Total `exchange.get_position` failures during cache refresh.
    pub fn refresh_errors(&self) -> u64 {
        self.refresh_errors.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TradingService for FillRoutingService {
    fn name(&self) -> &str {
        "fill-routing"
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        tracing::info!("fill-routing service starting");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(
                        routed = self.fills_routed(),
                        refresh_errors = self.refresh_errors(),
                        "fill-routing service shutting down"
                    );
                    return Ok(());
                }
                next = self.source.next_fill() => {
                    let Some(fill) = next else {
                        tracing::info!("fill source closed; exiting");
                        return Ok(());
                    };

                    let symbol = fill.symbol.clone();

                    // Route to every brain. Errors are logged but don't
                    // stop the service — the brain's on_fill is
                    // informational by contract.
                    for brain in self.brains.iter() {
                        if let Err(e) = brain.on_fill(&fill).await {
                            tracing::warn!(
                                brain = brain.name(),
                                error = %e,
                                "brain on_fill returned error"
                            );
                        }
                    }

                    // Refresh position cache from the exchange.
                    match self.exchange.get_position(&symbol).await {
                        Ok(p) => {
                            self.positions.write().await.insert(symbol.clone(), p);
                            tracing::debug!(symbol = %symbol, qty = p.qty, "refreshed position");
                        }
                        Err(e) => {
                            self.refresh_errors.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                symbol = %symbol,
                                error = %e,
                                "failed to refresh position after fill"
                            );
                        }
                    }

                    self.fills_routed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}
