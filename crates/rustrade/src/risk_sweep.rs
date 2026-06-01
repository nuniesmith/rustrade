//! Periodic risk sweep.
//!
//! [`SessionPnl`](rustrade_risk::SessionPnl),
//! [`CircuitBreaker`](rustrade_risk::CircuitBreaker), and
//! [`PortfolioRisk`](rustrade_risk::PortfolioRisk) all roll their daily state
//! over at 00:00 UTC via `tick()` — but nothing calls `tick()` during a live
//! run (only `restore` does, on restart). A bot that runs for days would never
//! roll a daily-loss halt over.
//!
//! [`RiskSweepService`] closes that hole: a supervised service that, on a
//! cadence, ticks every symbol's per-symbol risk and the account-wide
//! portfolio risk, and refreshes the portfolio daily-loss latch from the live
//! sum of the per-symbol session PnLs (the account's single source of truth —
//! no separate bookkeeping to drift).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rustrade_supervisor::{RestartPolicy, TradingService};
use tokio_util::sync::CancellationToken;

use crate::risk_state::{PortfolioRiskState, RiskStateMap};

/// Supervised periodic sweep of per-symbol + account-wide risk state.
pub(crate) struct RiskSweepService {
    risk: RiskStateMap,
    portfolio: PortfolioRiskState,
    cadence: Duration,
    sweeps: AtomicU64,
}

impl RiskSweepService {
    pub(crate) fn new(
        risk: RiskStateMap,
        portfolio: PortfolioRiskState,
        cadence: Duration,
    ) -> Self {
        Self {
            risk,
            portfolio,
            cadence,
            sweeps: AtomicU64::new(0),
        }
    }

    /// Number of sweeps performed (observability / tests).
    pub(crate) fn sweeps(&self) -> u64 {
        self.sweeps.load(Ordering::Relaxed)
    }

    /// One sweep: tick each symbol's gates (summing account net PnL in the same
    /// pass), then roll the portfolio latch over and re-observe the account net.
    async fn sweep_once(&self) {
        let account_net = {
            let mut map = self.risk.write().await;
            let mut net = 0.0;
            for sr in map.values_mut() {
                sr.session_pnl.tick();
                sr.circuit_breaker.tick();
                net += sr.session_pnl.net_pnl();
            }
            net
        };
        {
            let mut pf = self.portfolio.write().await;
            pf.tick(); // clear the latch first if the UTC day rolled over …
            pf.observe(account_net); // … then re-latch if still breached today
        }
        self.sweeps.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl TradingService for RiskSweepService {
    fn name(&self) -> &str {
        "risk-sweep"
    }

    fn restart_policy(&self) -> RestartPolicy {
        RestartPolicy::OnFailure
    }

    async fn run(&self, cancel: CancellationToken) -> anyhow::Result<()> {
        tracing::info!(cadence_secs = self.cadence.as_secs(), "risk-sweep starting");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!(sweeps = self.sweeps(), "risk-sweep shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(self.cadence) => {
                    self.sweep_once().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::risk_state::{build_portfolio_risk, build_risk_state};
    use rustrade_core::Symbol;
    use rustrade_risk::{CircuitBreakerConfig, PortfolioRiskConfig, SessionPnlConfig};

    #[tokio::test]
    async fn sweep_latches_portfolio_halt_from_symbol_pnl() {
        let sym = Symbol::from("BTCUSDT");
        // Per-symbol cap is loose so only the *account* limit binds.
        let risk = build_risk_state(std::slice::from_ref(&sym), |_| {
            (
                SessionPnlConfig {
                    loss_limit: -10_000.0,
                },
                CircuitBreakerConfig::default(),
            )
        });
        // Book a -200 net loss on the one symbol.
        risk.write()
            .await
            .get_mut(&sym)
            .unwrap()
            .session_pnl
            .record_close(-200.0, 0.0);

        // Account daily-loss limit of -150 → the sweep should latch the halt.
        let portfolio = build_portfolio_risk(PortfolioRiskConfig {
            max_daily_loss: -150.0,
            ..Default::default()
        });
        let svc = RiskSweepService::new(risk, portfolio.clone(), Duration::from_secs(1));

        assert!(!portfolio.read().await.is_halted());
        svc.sweep_once().await;
        assert!(
            portfolio.read().await.is_halted(),
            "account loss must latch"
        );
        assert_eq!(svc.sweeps(), 1);
    }

    #[tokio::test]
    async fn sweep_leaves_healthy_account_untouched() {
        let sym = Symbol::from("ETHUSDT");
        let risk = build_risk_state(std::slice::from_ref(&sym), |_| {
            (SessionPnlConfig::default(), CircuitBreakerConfig::default())
        });
        risk.write()
            .await
            .get_mut(&sym)
            .unwrap()
            .session_pnl
            .record_close(50.0, 1.0); // a net win

        let portfolio = build_portfolio_risk(PortfolioRiskConfig {
            max_daily_loss: -150.0,
            ..Default::default()
        });
        let svc = RiskSweepService::new(risk, portfolio.clone(), Duration::from_secs(1));
        svc.sweep_once().await;
        assert!(!portfolio.read().await.is_halted());
    }
}
