//! Optional framework-side services wired in via builder methods on
//! [`Bot`](crate::Bot):
//!
//! - [`MarketFeedService`] — `Bot::with_market_source(...)`. Drives a
//!   [`MarketSource`] under supervisor control; the source publishes
//!   events to the in-process `MarketDataBus` (the bus reference is the
//!   source implementor's responsibility — typically obtained via
//!   `bot.market_data_bus().clone()` before construction).
//! - [`FillRoutingService`] — `Bot::with_fill_source(...)`. Polls a
//!   [`FillSource`], calls [`Brain::on_fill`] on each brain, refreshes
//!   the per-symbol position cache from the exchange, and auto-feeds
//!   realised PnL into the risk gates using weighted-average entry
//!   accounting.
//! - [`CandlePollerService`] — `Bot::with_candle_poller(...)`. Periodic
//!   poll of a [`CandleSource`]; publishes the newest closed candle for
//!   each `(symbol, interval)` pair to the market-data bus.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rustrade_core::{
    Brain, CandleSource, Exchange, ExchangeClient, Fill, FillSource, MarketDataBus,
    MarketDataEvent, MarketSource, MetricsSink, Side, Symbol,
};
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio_util::sync::CancellationToken;

use crate::risk_state::{PositionCache, RiskPersister, RiskStateMap};

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

/// Routes fills from a [`FillSource`] to every brain, refreshes the
/// position cache, and auto-feeds realised PnL into the risk state.
///
/// # PnL accounting
///
/// The service uses a **weighted-average entry** model (the same model
/// the backtest engine uses). It reads the cached `Position` *before*
/// refreshing it from the exchange, so the `entry_price` available is
/// the pre-fill average. From that:
///
/// - A fill in the same direction as the open position **adds** to it.
///   No realised PnL emitted; the post-refresh average from
///   `exchange.get_position` becomes the new entry.
/// - A fill in the opposite direction **reduces** the position. Gross
///   PnL = `(fill_price - entry) * closed_qty * direction`. The
///   service calls `BotHandle::record_trade_outcome` on the closed
///   portion to feed `SessionPnl` + `CircuitBreaker`.
/// - A fill that **flips** the position emits realised PnL for the
///   closed portion only; the opening leg is left for the next
///   reducing fill.
///
/// Fees come from `Fill.fee`. Hosts that need a different accounting
/// model (FIFO, LIFO, tax-lot) should compute PnL themselves and call
/// `BotHandle::record_trade_outcome` directly — but cannot also wire a
/// `FillRoutingService`, since the two would double-count.
pub struct FillRoutingService {
    source: Arc<dyn FillSource>,
    brains: Arc<Vec<Arc<dyn Brain>>>,
    exchange: Arc<dyn ExchangeClient>,
    positions: PositionCache,
    risk: RiskStateMap,
    metrics: Arc<dyn MetricsSink>,
    persister: Option<RiskPersister>,
    oco: Option<crate::order_tracker::OcoRegistry>,
    fills_routed: AtomicU64,
    refresh_errors: AtomicU64,
    trades_recorded: AtomicU64,
    oco_cancels: AtomicU64,
}

impl FillRoutingService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<dyn FillSource>,
        brains: Arc<Vec<Arc<dyn Brain>>>,
        exchange: Arc<dyn ExchangeClient>,
        positions: PositionCache,
        risk: RiskStateMap,
        metrics: Arc<dyn MetricsSink>,
        persister: Option<RiskPersister>,
        oco: Option<crate::order_tracker::OcoRegistry>,
    ) -> Self {
        Self {
            source,
            brains,
            exchange,
            positions,
            risk,
            metrics,
            persister,
            oco,
            fills_routed: AtomicU64::new(0),
            refresh_errors: AtomicU64::new(0),
            trades_recorded: AtomicU64::new(0),
            oco_cancels: AtomicU64::new(0),
        }
    }

    /// Total OCO siblings cancelled in response to a bracket leg filling.
    pub fn oco_cancels(&self) -> u64 {
        self.oco_cancels.load(Ordering::Relaxed)
    }

    /// Total fills delivered to brains since service start.
    pub fn fills_routed(&self) -> u64 {
        self.fills_routed.load(Ordering::Relaxed)
    }

    /// Total `exchange.get_position` failures during cache refresh.
    pub fn refresh_errors(&self) -> u64 {
        self.refresh_errors.load(Ordering::Relaxed)
    }

    /// Total realised-PnL closures fed into the risk state.
    pub fn trades_recorded(&self) -> u64 {
        self.trades_recorded.load(Ordering::Relaxed)
    }

    /// Compute realised PnL from a reducing fill and feed the risk state.
    /// Returns the gross PnL portion attributable to this fill.
    async fn maybe_record_pnl(&self, fill: &Fill, prior_qty: f64, prior_entry: Option<f64>) {
        // Only reducing or flipping fills produce realised PnL.
        let signed_fill_qty = match fill.side {
            Side::Buy => fill.size.value(),
            Side::Sell => -fill.size.value(),
        };
        if prior_qty == 0.0 || prior_qty.signum() == signed_fill_qty.signum() {
            return;
        }
        let Some(entry) = prior_entry else {
            // Reducing fill but no entry price recorded — can't compute
            // PnL. Log and skip.
            tracing::debug!(
                symbol = %fill.symbol,
                "reducing fill but cached position has no entry price; skipping auto-PnL"
            );
            return;
        };
        let closed_qty = prior_qty.abs().min(fill.size.value());
        if closed_qty <= 0.0 {
            return;
        }
        let direction = prior_qty.signum();
        let gross = (fill.price.value() - entry) * direction * closed_qty;
        // Apportion fee by closing fraction so a flip fill charges
        // fees pro-rata to the closing portion.
        let fee_share = if fill.size.value() > 0.0 {
            fill.fee * (closed_qty / fill.size.value())
        } else {
            0.0
        };

        // The fill itself is validated at ingestion, but `entry` comes from
        // the position cache (ultimately `exchange.get_position`) and could
        // still be non-finite. A NaN fed into `record_close` would disable
        // the loss-limit gate, so refuse it here.
        if !gross.is_finite() || !fee_share.is_finite() {
            tracing::error!(
                symbol = %fill.symbol,
                gross,
                fee_share,
                entry,
                "auto-PnL: computed non-finite realised PnL — NOT recorded \
                 (risk gates unchanged)"
            );
            return;
        }

        // Update the per-symbol risk state directly.
        let recorded = {
            let mut map = self.risk.write().await;
            if let Some(risk) = map.get_mut(&fill.symbol) {
                risk.session_pnl.record_close(gross, fee_share);
                let net = gross - fee_share;
                if net > 0.0 {
                    risk.circuit_breaker.record_win();
                } else if net < 0.0 {
                    risk.circuit_breaker.record_loss();
                }
                self.trades_recorded.fetch_add(1, Ordering::Relaxed);
                self.metrics.histogram(
                    "rustrade_realised_pnl_quote",
                    &[("symbol", fill.symbol.as_str())],
                    net,
                );
                true
            } else {
                // A fill the risk layer never sees: its losses won't count
                // toward the session PnL halt or the circuit breaker. Loud
                // by design — this usually means a symbol was traded that
                // isn't in `BotConfig.symbols`.
                self.metrics.counter(
                    "rustrade_unrecorded_fills_total",
                    &[("symbol", fill.symbol.as_str())],
                    1,
                );
                tracing::warn!(
                    symbol = %fill.symbol,
                    "auto-PnL: fill for a symbol not in the risk-state map — \
                     realised PnL NOT recorded by any risk gate \
                     (is it missing from BotConfig.symbols?)"
                );
                false
            }
        };

        // Persist the updated risk state (lock released) if a store is wired.
        if recorded && let Some(persister) = &self.persister {
            persister.persist_symbol(&self.risk, &fill.symbol).await;
        }
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
                        trades_recorded = self.trades_recorded(),
                        "fill-routing service shutting down"
                    );
                    return Ok(());
                }
                next = self.source.next_fill() => {
                    let Some(fill) = next else {
                        tracing::info!("fill source closed; exiting");
                        return Ok(());
                    };

                    // Ingestion-boundary validation: a non-finite price /
                    // size / fee would poison the weighted-average PnL and
                    // — because every NaN comparison is false — silently
                    // disable the session-PnL halt. Drop the fill entirely
                    // (not routed, not recorded) and say so loudly.
                    if !fill_is_finite(&fill) {
                        self.metrics.counter(
                            "rustrade_invalid_fills_total",
                            &[("symbol", fill.symbol.as_str())],
                            1,
                        );
                        tracing::error!(
                            symbol = %fill.symbol,
                            order_id = %fill.order_id,
                            price = fill.price.value(),
                            size = fill.size.value(),
                            fee = fill.fee,
                            "fill source produced a non-finite fill — dropped \
                             (not routed to brains, not recorded in risk state)"
                        );
                        continue;
                    }

                    let symbol = fill.symbol.clone();

                    // OCO: if this fill belongs to a bracket leg, cancel its
                    // sibling so the position isn't closed twice.
                    if let Some(oco) = &self.oco
                        && let Some((sym, sibling)) = oco.take_sibling(&fill.order_id).await
                    {
                        match self.exchange.cancel_order(&sym, &sibling).await {
                            Ok(_) => {
                                self.oco_cancels.fetch_add(1, Ordering::Relaxed);
                                self.metrics.inc("rustrade_oco_cancels_total");
                                tracing::info!(symbol = %sym, filled = %fill.order_id, cancelled = %sibling, "OCO: cancelled sibling after bracket leg filled");
                            }
                            Err(e) => tracing::warn!(symbol = %sym, sibling = %sibling, error = %e, "OCO: failed to cancel sibling (it may already be gone)"),
                        }
                    }

                    // Snapshot the pre-fill position so we can compute
                    // realised PnL before the exchange refreshes the
                    // entry price.
                    let (prior_qty, prior_entry) = {
                        let map = self.positions.read().await;
                        let p = map.get(&symbol).copied().unwrap_or(rustrade_core::Position::FLAT);
                        (p.qty, p.entry_price)
                    };

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

                    self.maybe_record_pnl(&fill, prior_qty, prior_entry).await;

                    // Refresh position cache from the exchange.
                    match self.exchange.get_position(&symbol).await {
                        Ok(p) if p.qty.is_finite()
                            && p.entry_price.is_none_or(f64::is_finite) =>
                        {
                            self.positions.write().await.insert(symbol.clone(), p);
                            tracing::debug!(symbol = %symbol, qty = p.qty, "refreshed position");
                        }
                        Ok(p) => {
                            // A non-finite qty/entry would poison every
                            // PnL computed from the cache — keep the old
                            // snapshot instead.
                            self.refresh_errors.fetch_add(1, Ordering::Relaxed);
                            self.metrics.inc("rustrade_position_refresh_errors_total");
                            tracing::error!(
                                symbol = %symbol,
                                qty = p.qty,
                                entry = ?p.entry_price,
                                "exchange returned a non-finite position — cache NOT updated"
                            );
                        }
                        Err(e) => {
                            self.refresh_errors.fetch_add(1, Ordering::Relaxed);
                            self.metrics.inc("rustrade_position_refresh_errors_total");
                            tracing::warn!(
                                symbol = %symbol,
                                error = %e,
                                "failed to refresh position after fill"
                            );
                        }
                    }

                    self.fills_routed.fetch_add(1, Ordering::Relaxed);
                    self.metrics.counter(
                        "rustrade_fills_routed_total",
                        &[("symbol", symbol.as_str())],
                        1,
                    );
                }
            }
        }
    }
}

/// Every numeric field a [`Fill`] carries must be finite (and the size
/// non-negative) before the framework will route or record it.
fn fill_is_finite(f: &Fill) -> bool {
    f.price.value().is_finite()
        && f.size.value().is_finite()
        && f.size.value() >= 0.0
        && f.fee.is_finite()
}

// ───────────────────────────────────────────────────────────────────────
// CandlePollerService
// ───────────────────────────────────────────────────────────────────────

/// Periodic poll of a [`CandleSource`] for a single `(symbol, interval)`
/// pair. Publishes each newly-closed candle to the
/// [`MarketDataBus`].
///
/// Per-symbol cadences are achieved by spawning multiple services —
/// `Bot::with_candle_poller(...)` accepts repeated calls and spawns one
/// service per registered tuple.
///
/// # Deduplication
///
/// The service tracks the highest `Candle::time` it has already
/// published; only candles with a strictly greater timestamp are
/// re-published. This is robust against exchanges that return overlapping
/// windows on consecutive polls.
pub struct CandlePollerService {
    name: String,
    source: Arc<dyn CandleSource>,
    symbol: Symbol,
    interval: Duration,
    poll_cadence: Duration,
    limit: usize,
    bus: MarketDataBus,
    metrics: Arc<dyn MetricsSink>,
    last_time: std::sync::Mutex<i64>,
    polled: AtomicU64,
    poll_errors: AtomicU64,
    published: AtomicU64,
}

impl CandlePollerService {
    pub(crate) fn new(
        source: Arc<dyn CandleSource>,
        symbol: Symbol,
        interval: Duration,
        poll_cadence: Duration,
        limit: usize,
        bus: MarketDataBus,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        let name = format!("candle-poller[{}@{}s]", symbol.as_str(), interval.as_secs());
        Self {
            name,
            source,
            symbol,
            interval,
            poll_cadence,
            limit,
            bus,
            metrics,
            last_time: std::sync::Mutex::new(i64::MIN),
            polled: AtomicU64::new(0),
            poll_errors: AtomicU64::new(0),
            published: AtomicU64::new(0),
        }
    }

    /// Total successful polls.
    pub fn polled(&self) -> u64 {
        self.polled.load(Ordering::Relaxed)
    }
    /// Total failed polls.
    pub fn poll_errors(&self) -> u64 {
        self.poll_errors.load(Ordering::Relaxed)
    }
    /// Total candles published (deduplicated).
    pub fn published(&self) -> u64 {
        self.published.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl TradingService for CandlePollerService {
    fn name(&self) -> &str {
        &self.name
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(service = %self.name, "candle poller starting");
        let exchange = Exchange::from(self.source.name());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(
                        service = %self.name,
                        polled = self.polled(),
                        published = self.published(),
                        errors = self.poll_errors(),
                        "candle poller shutting down"
                    );
                    return Ok(());
                }
                _ = tokio::time::sleep(self.poll_cadence) => {
                    match self.source.poll(&self.symbol, self.interval, self.limit).await {
                        Ok(candles) => {
                            self.polled.fetch_add(1, Ordering::Relaxed);
                            let mut last = self.last_time.lock().expect("last_time poisoned");
                            let mut new_high = *last;
                            for candle in candles {
                                if candle.time <= *last {
                                    continue;
                                }
                                new_high = new_high.max(candle.time);
                                self.bus.publish(MarketDataEvent::Candle {
                                    exchange: exchange.clone(),
                                    symbol: self.symbol.clone(),
                                    candle,
                                });
                                self.published.fetch_add(1, Ordering::Relaxed);
                                self.metrics.counter(
                                    "rustrade_candles_published_total",
                                    &[("symbol", self.symbol.as_str())],
                                    1,
                                );
                            }
                            *last = new_high;
                        }
                        Err(e) => {
                            self.poll_errors.fetch_add(1, Ordering::Relaxed);
                            self.metrics.inc("rustrade_candle_poll_errors_total");
                            tracing::warn!(
                                service = %self.name,
                                error = %e,
                                "candle poll failed"
                            );
                        }
                    }
                }
            }
        }
    }
}
