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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    

    println!("Choose a symbol");
    // take the string
    let mut user_input = String::new();

    io::stdin()
        .read_line(&mut user_input)
        .expect("Failed to read symbol");

    let symbol = user_input.trim();
    
    let (tx, mut rx) = mpsc::channel(100);

    let adapter = KrakenAdapter::new(symbol.to_owned());
    tokio::spawn(async move {
        if let Err(error) = adapter.run(tx).await {
            println!("{}", error);
        }
    });
    
    while let msg = rx.recv().unwrap() {
        if let Some(event) = 
    }

    Ok(())


}
