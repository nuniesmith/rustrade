//! In-process broadcast buses for market events and signals.
//!
//! Both buses are thin wrappers around `tokio::sync::broadcast` with
//! framework-appropriate defaults. The broadcast pattern suits this use case:
//! we have one producer (the market feed) and multiple consumers (brain,
//! logger, metrics exporter, persistence layer).
//!
//! # Sizing
//!
//! Broadcast channels drop oldest messages when a slow consumer falls behind.
//! The default capacity of 1024 is generous — consumers should be fast, but
//! a brief stall (e.g. GC pause, disk flush) won't lose data.

use tokio::sync::broadcast;

use crate::market::MarketDataEvent;
use crate::signal::Signal;

const DEFAULT_CAPACITY: usize = 1024;

/// Broadcast channel for normalized market-data events.
///
/// # Example
///
/// ```ignore
/// let bus = MarketDataBus::new();
/// let mut rx = bus.subscribe();
///
/// // Producer side:
/// bus.publish(MarketDataEvent::Candle { ... });
///
/// // Consumer side:
/// while let Ok(event) = rx.recv().await {
///     // ...
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MarketDataBus {
    tx: broadcast::Sender<MarketDataEvent>,
}

impl MarketDataBus {
    /// Create a new bus with the default capacity.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a new bus with an explicit channel capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe a new consumer. The returned receiver sees all events
    /// published *after* this call.
    pub fn subscribe(&self) -> broadcast::Receiver<MarketDataEvent> {
        self.tx.subscribe()
    }

    /// Publish an event. Returns the number of active subscribers that
    /// received it.
    ///
    /// If the send fails (no subscribers) the event is silently dropped —
    /// this is the correct behaviour for a broadcast bus where the producer
    /// doesn't care whether anyone is listening.
    pub fn publish(&self, event: MarketDataEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Current number of subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for MarketDataBus {
    fn default() -> Self {
        Self::new()
    }
}

/// Broadcast channel for trading signals.
#[derive(Debug, Clone)]
pub struct SignalBus {
    tx: broadcast::Sender<Signal>,
}

impl SignalBus {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Signal> {
        self.tx.subscribe()
    }

    pub fn publish(&self, signal: Signal) -> usize {
        self.tx.send(signal).unwrap_or(0)
    }

    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for SignalBus {
    fn default() -> Self {
        Self::new()
    }
}
