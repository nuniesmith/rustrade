# Writing an exchange adapter

`rustrade` ships zero exchange adapters by design — they live in
downstream crates that depend on `rustrade-core`. This guide walks
through the three traits an adapter implements and what each is for.

## The three traits

| Trait             | Purpose                                                                 |
| ----------------- | ----------------------------------------------------------------------- |
| `ExchangeClient`  | Imperative trading surface: place / cancel orders, query position, balance |
| `MarketSource`    | Push public market data into the bot's `MarketDataBus`                  |
| `FillSource`      | Push private fill events into the bot's `FillRoutingService`            |

Implementing `ExchangeClient` is mandatory. `MarketSource` and
`FillSource` are optional — wire them only if the host wants the
framework to manage the source's lifecycle for them.

## 1. `ExchangeClient`

```rust
use async_trait::async_trait;
use rustrade::{
    Capability, ExchangeClient, Order, Position, Result, Symbol,
};

pub struct MyAdapter {
    api_key: String,
    leverage: u32,
    // …
}

#[async_trait]
impl ExchangeClient for MyAdapter {
    fn name(&self) -> &str { "my-exchange" }

    async fn place_order(&self, order: &Order) -> Result<String> {
        // Translate `Order` into the exchange's native payload, POST,
        // return the exchange-assigned order id.
        todo!()
    }

    async fn cancel_all(&self, symbol: &Symbol) -> Result<usize> { todo!() }
    async fn close_position(&self, symbol: &Symbol, position: &Position) -> Result<String> { todo!() }
    async fn get_position(&self, symbol: &Symbol) -> Result<Position> { todo!() }
    async fn get_balance(&self, currency: &str) -> Result<f64> { todo!() }

    // Optional overrides:

    fn supports(&self, capability: Capability) -> bool {
        matches!(
            capability,
            Capability::StopOrders | Capability::ReduceOnly | Capability::PostOnly
        )
    }

    fn contract_value(&self, symbol: &Symbol) -> f64 {
        // Spot adapters return 1.0. Futures adapters override per symbol.
        match symbol.as_str() {
            "XBTUSDTM" => 0.001,
            "ETHUSDTM" => 0.01,
            _          => 1.0,
        }
    }
}
```

### `Capability` introspection

The framework consults `supports(Capability::*)` *before* invoking
features that aren't part of every exchange's surface. New adapters
should return `true` only for what they genuinely support; the default
returns `false` for every variant, which is the conservative choice.

| Capability     | What the framework asks                                                |
| -------------- | ---------------------------------------------------------------------- |
| `StopOrders`   | Will you honour `Order.stop: Some(StopAttachment)`?                    |
| `PostOnly`     | Will you reject post-only orders that would cross the book?            |
| `ReduceOnly`   | Will you reject reduce-only orders that would increase the position?   |
| `Ioc` / `Fok`  | Will you honour `OrderKind::Ioc` / `Fok`?                              |
| `PublicFeed`   | Do you provide a public market-data feed alongside trading?            |
| `PrivateFeed`  | Do you push fill / order-update events on a private feed?              |

### `contract_value`

Returns base-asset units per one contract. Spot exchanges return `1.0`
universally — one BTC traded is one BTC. Futures adapters need to
override per symbol so the framework's
[`PositionSizer`](https://docs.rs/rustrade-risk) can convert margin ×
leverage into a contract count.

### Leverage

Leverage is **per-adapter, not per-order**. Configure it on your
adapter's constructor and apply it however your exchange requires.
Most live exchanges set leverage at the account or symbol level; if
yours allows per-order overrides, expose it via your adapter's
own surface, not via `Order`.

## 2. `MarketSource` (optional)

If you have a WebSocket feed pushing public market data, implement
`MarketSource` so the framework can drive it under supervisor control:

```rust
use async_trait::async_trait;
use rustrade::{MarketSource, Result};

pub struct MyMarketFeed {
    bus: rustrade::MarketDataBus,
    // …
}

#[async_trait]
impl MarketSource for MyMarketFeed {
    fn name(&self) -> &str { "my-exchange-public-feed" }
    fn is_live(&self) -> bool { /* …connected? */ true }

    async fn run(&self) -> Result<()> {
        // Stream loop. Publish `MarketDataEvent`s to `self.bus` as they
        // arrive. Return when the feed closes.
        loop {
            let event = self.next_event_from_socket().await?;
            self.bus.publish(event);
        }
    }
}
```

### Cancellation contract

`MarketSource::run` does NOT take a `CancellationToken`. Cancellation
flows through the wrapping
[`MarketFeedService`](https://docs.rs/rustrade) — when the supervisor
cancels that service's token, your `run` future is dropped at its next
`.await` point. Implementors must be **drop-safe**: open sockets, file
handles, etc. must release cleanly when their containing future is
destroyed. In practice that means:

- Don't hold a `MutexGuard` across an `.await` that the supervisor
  might drop mid-flight.
- If you need explicit teardown (rare), put it in a `Drop` impl on
  your struct, not at the end of `run`.

Wire the source with:

```rust
let source = Arc::new(MyMarketFeed::new(bot.market_data_bus().clone(), ...));
let bot   = bot.with_market_source(source);
```

## 3. `FillSource` (optional)

For private fill events:

```rust
#[async_trait]
impl rustrade::FillSource for MyFillFeed {
    async fn next_fill(&self) -> Option<rustrade::Fill> {
        // Block until the next fill arrives. Return `None` on stream
        // close — the framework will stop polling.
        self.rx.recv().await
    }
}
```

Wire it with:

```rust
let bot = bot.with_fill_source(Arc::new(MyFillFeed::new(...)));
```

The framework's `FillRoutingService` will:

1. Call `Brain::on_fill` on every brain
2. Refresh the per-symbol position cache via `ExchangeClient::get_position`

## 4. Symbol typing

All trait methods use `&Symbol` rather than `&str`. `Symbol` is a thin
newtype over `String` with `From<&str>`, `From<String>`, `AsRef<str>`,
and `Borrow<str>` — so call sites can pass string literals and the
adapter can index a `HashMap<Symbol, _>` by `&str` without allocation.

## 5. Error handling

Adapters return `rustrade::Result<T>` (which is `rustrade_core::Result`).
On failure, wrap the underlying error:

```rust
async fn get_position(&self, symbol: &Symbol) -> Result<Position> {
    let resp = self.http.get(...).await
        .map_err(|e| rustrade::Error::exchange(format!("http error: {e}")))?;
    // …
}
```

Use `Error::Exchange` for transport / exchange-side failures and
`Error::Internal` for unexpected bugs.

## 6. Testing

Unit-test the adapter's translation logic (Order → native payload,
native response → `Position`) directly. Use the framework's mock
patterns from `examples/sma-cross-bot/` to exercise the bot end-to-end
against your adapter without hitting the live exchange.

## Next steps

- The framework's reference implementations for `Brain` / `ExchangeClient`
  live in [`examples/`](../examples).
- For the live adapter pattern, see `examples/embed-in-service/` —
  it shows how the host wires the bot, the adapter, and shutdown
  together.
- The `Capability` enum is `#[non_exhaustive]`; future versions may
  add variants. Match exhaustively with `_ => false` to stay
  future-proof.
