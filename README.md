# The Rusty Vault

A multi-venue cryptocurrency market-data gateway written in Rust. It connects to Kraken, Coinbase, and Binance.US concurrently, converts their different WebSocket payloads into one venue-independent event model, and sends those events through a bounded Tokio channel to a shared consumer.

Why? Because I wanted an excuse to use Rust.

## Cool stuff in the repo

- Concurrent public WebSocket connections
- No exchange accounts or API keys required
- Venue-specific JSON deserialization
- Exact decimal handling with `rust_decimal`
- Normalized trades, top-of-book quotes, book snapshots, and book updates
- Exchange and gateway-receipt timestamps
- Symbol conversion for each venue's naming convention
- Bounded Tokio `mpsc` channel for backpressure

## Architecture

```text
Kraken WebSocket ────> Kraken adapter ────┐
Coinbase WebSocket ──> Coinbase adapter ──┼──> bounded mpsc channel ──> consumer
Binance.US WebSocket ─> Binance adapter ───┘             |
                                                        v
                                                  MarketEvent
                                            Trade | Quote | Book*
```

Each adapter does this:

1. Connect and subscribe to public market-data channels.
2. Deserialize the venue's wire format.
3. Convert the message into a normalized `MarketEvent`.
4. Send ownership of that event to the shared consumer.


## Normalized events

The gateway currently emits:

- `Trade` — venue, instrument, trade ID, side, price, quantity, and timestamps
- `Quote` — best bid/ask prices and quantities with timestamps
- `BookSnapshot` — initial bid and ask levels when supplied by the venue
- `BookUpdate` — changed bid and ask levels; a zero quantity represents deletion

Prices and quantities use decimal values rather than binary floating-point values. Book checksums are optional because the venues do not expose identical integrity metadata, womp womp.

## Supported venues

| Venue | Trades | Quotes | Book snapshot | Book updates |
|---|---:|---:|---:|---:|
| Kraken | Yes | Yes | Yes | Yes |
| Coinbase | Yes | Yes | Yes | Yes |
| Binance.US | Yes | Yes | No | Yes |
| more coming soon! | | | | |

## Running locally

It's so easy! All you need is...

- A current stable Rust toolchain
- Internet access to the three public WebSocket endpoints

```bash
git clone https://github.com/aryanghbd/the-rusty-vault.git
cd the-rusty-vault
cargo run -- <symbol-in-canonical-form> # eg cargo run -- BTC/USD
```

The process prints normalized events received by the shared consumer. Press Ctrl-C to stop it.