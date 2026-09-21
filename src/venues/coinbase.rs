use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;
use serde::{Deserialize, Serialize};
use std::io;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc, Local, TimeZone, NaiveDateTime, Duration};
use crate::events::{MarketEvent, PriceLevel as NormalizedPriceLevel, BookUpdate as NormalizedBookUpdate, BookSnapshot as NormalizedBookSnapshot, Quote as NormalizedQuote, Trade as NormalizedTrade};
use tokio::sync::mpsc;


#[derive(Deserialize, Debug)]
struct TickerData {
    product_id: String,
    best_bid: rust_decimal::Decimal,
    best_bid_quantity: rust_decimal::Decimal,
    best_ask: rust_decimal::Decimal,
    best_ask_quantity: rust_decimal::Decimal
}

#[derive(Deserialize, Debug)]
struct TickerEvent {
    r#type: String,
    tickers: Vec<TickerData>
}

#[derive(Deserialize, Debug)]
struct Ticker {
    channel: String,
    timestamp: DateTime<Utc>,
    sequence_num: u64,
    events: Vec<TickerEvent>
}

#[derive(Deserialize, Debug)]
struct L2Update {
    side: String,
    event_time: DateTime<Utc>,
    price_level: rust_decimal::Decimal,
    new_quantity: rust_decimal::Decimal
}

#[derive(Deserialize, Debug)]
struct L2Event {
    r#type: String,
    product_id: String,
    updates: Vec<L2Update>
}

#[derive(Deserialize, Debug)]
struct L2Message {
    channel: String,
    timestamp: DateTime<Utc>,
    sequence_num: u64,
    events: Vec<L2Event>
}

#[derive(Deserialize, Debug)]
struct MarketTrade {
    product_id: String,
    trade_id: String,
    price: rust_decimal::Decimal,
    size: rust_decimal::Decimal,
    time: DateTime<Utc>,
    side: String
}

#[derive(Deserialize, Debug)]
struct MarketTradeEvent {
    r#type: String,
    trades: Vec<MarketTrade>
}

#[derive(Deserialize, Debug)]
struct MarketTradeMessage {
    channel: String,
    timestamp: DateTime<Utc>,
    sequence_num: u64,
    events: Vec<MarketTradeEvent>
}

pub struct CoinbaseAdapter {
    symbol: String
}

impl CoinbaseAdapter {
    const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com"

    pub fn new(symbol: String) -> Self {
        return Self { symbol };
    }

    pub async fn run(&self, tx : mpsc::Sender<MarketEvent>) -> Result<(), Box<dyn std::error::Error>> {
        let (mut ws_stream, _response) = connect_async(Self::WS_URL).await?;

        let ticker = format!(
            r#"{{ "type": "subscribe", "channel": "ticker", "product_ids": ["{}"] }}"#,
            self.symbol
        );

        let trade = format!(
            r#"{{ "type": "subscribe", "channel": "market_trades", "product_ids": ["{}"] }}"#,
            self.symbol
        );

        let book_l2 = format!(
            r#"{{ "type": "subscribe", "channel": "level2", "product_ids": ["{}"] }}"#,
            self.symbol
        );

        // let heartbeats: String "{{ "type": "subscribe", "channel": "heartbeats" }}";

        let (mut write, mut read) = ws_stream.split();
        write.send(Message::Text(ticker.into())).await?;
        write.send(Message::Text(trade.into())).await?;
        write.send(Message::Text(book_l2.into())).await?;

        loop {
            tokio::select! {
                Some(result) = read.next() => {
                    match result {
                        Ok(Message::Text(text)) => {
                            
                        }
                    }
                }
            }
        }
        Ok(())
    }
}