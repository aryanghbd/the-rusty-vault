use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;
use serde::{Deserialize, Serialize};
use std::io;
use rust_decimal::Decimal;
mod venues;
mod events;
use venues::kraken::KrakenAdapter;
use tokio::sync::mpsc;

use crate::venues::coinbase::CoinbaseAdapter;
use crate::venues::binance::BinanceAdapter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    

    let mut user_input = std::env::args().nth(1).expect("Usage: cargo run -- <SYMBOL");

    let symbol = user_input.trim();
    
    let (tx, mut rx) = mpsc::channel(100);

    
    let adapter = KrakenAdapter::new(symbol.to_owned());
    let coinbase_adapter = CoinbaseAdapter::new(symbol.to_owned());
    let binance_adapter: BinanceAdapter = BinanceAdapter::new(symbol.to_owned());
    let kraken_tx = tx.clone();
    let coinbase_tx = tx.clone();
    let binance_tx = tx.clone();

    tokio::spawn(async move {
        if let Err(error) = binance_adapter.run(binance_tx).await {
            println!("{}", error);
        }
    });
    // need to give each of these their own tx to use otherwise borrowing issues arise.
    tokio::spawn(async move {
        if let Err(error) = adapter.run(kraken_tx).await {
            println!("{}", error);
        }
    });

    tokio::spawn(async move {
        if let Err(error) = coinbase_adapter.run(coinbase_tx).await {
            println!("{}", error);
        }
    });
    

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                println!("Processing: {:#?}", event);
            }
            _ = signal::ctrl_c() => {
                println!("Shutting down gateway");
                break;
            }
        }
    }

    Ok(())


}
