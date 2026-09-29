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
    symbol: String,
    bid: rust_decimal::Decimal,
    bid_qty: rust_decimal::Decimal,
    ask: rust_decimal::Decimal,
    ask_qty: rust_decimal::Decimal,
    last: rust_decimal::Decimal,
    volume: rust_decimal::Decimal,
    vwap: rust_decimal::Decimal,
    low: rust_decimal::Decimal,
    high: rust_decimal::Decimal,
    change: rust_decimal::Decimal,
    change_pct: rust_decimal::Decimal,
    trades: u64,
    timestamp: DateTime<Utc>, 
}

#[derive(Deserialize, Debug)]
struct Ticker {
    channel: String,
    r#type: String,
    data: Vec<TickerData>
}

#[derive(Deserialize, Debug)]
struct TradeData {
    symbol: String,
    side: String,
    price: rust_decimal::Decimal,
    qty: rust_decimal::Decimal,
    ord_type: String,
    trade_id: u64,
    timestamp: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
struct Trade {
    channel: String,
    r#type: String,
    data: Vec<TradeData>
}

#[derive(Deserialize, Debug)]
struct PriceLevel {
    price: Decimal,
    qty: Decimal // amt of asset not necessarily amt of orders
}

#[derive(Deserialize, Debug)]
struct BookData {
    symbol: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
    checksum: u64,
    timestamp: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
struct Book {
    channel: String,
    r#type: String,
    data: Vec<BookData>
}

#[derive(Deserialize, Debug)]
struct Instrument {
    channel: String,
    r#type: String,
    data: InstrumentData // instrument data contains pairs which has its own shit in there
}

#[derive(Deserialize, Debug)]
struct InstrumentData {
    // ignore assets
    pairs: Vec<Pair>
}

#[derive(Deserialize, Debug)]
struct Pair {
    symbol: String,
    base: String,
    quote: String,
    status: String,
    qty_precision: u64,
    qty_increment: Decimal,
    price_precision: u64,
    price_increment: Decimal,
    qty_min: Decimal,
    cost_min: Decimal,
    marginable: bool,
    has_index: bool
}
// const KRAKEN_WS_URL = "wss://ws.kraken.com/v2";

pub struct KrakenAdapter {
    symbol: String
}

impl KrakenAdapter {
    const WS_URL : &str = "wss://ws.kraken.com/v2";


    pub fn new(symbol : String) -> Self {
        return Self { symbol: symbol.replace("-", "/") };
    }

    pub async fn run(&self, tx : mpsc::Sender<MarketEvent>) -> Result<(), Box<dyn std::error::Error>> {
        let (mut ws_stream, _response) = connect_async(Self::WS_URL).await?; // ? for resolving any errors after
        let ticker = format!(
            r#"{{ "method": "subscribe", "params": {{ "channel": "ticker", "symbol": ["{}"] }} }}"#,
            self.symbol
        );
        let trade = format!(
            r#"{{ "method": "subscribe", "params": {{ "channel": "trade", "symbol": ["{}"], "snapshot": true }} }}"#,
            self.symbol
        );
        let book_l2 = format!(
            r#"{{ "method": "subscribe", "params": {{ "channel": "book", "symbol": ["{}"] }} }}"#,
            self.symbol
        );
        let instrument = format!(
            r#"{{ "method": "subscribe", "params": {{ "channel": "instrument"}} }}"#,
        );
        // split websocket stream into a sender and receiver for fun bidirectionality.
        let (mut write, mut read) = ws_stream.split();
        write.send(Message::Text(ticker.into())).await?;
        write.send(Message::Text(trade.into())).await?;
        write.send(Message::Text(book_l2.into())).await?;
        write.send(Message::Text(instrument.into())).await?;

        loop {
            tokio::select! {
                //Option has two possibilities, some value exists, or nothing
                // branch 1, did data arrive
                Some(result) = read.next() => {
                    match result {
                        Ok(Message::Text(text)) => {
                            println!("msg received {}", text);
                            let rec_timestamp = Utc::now();
                            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();                 
                            // now index by channel

                            if let Some(channel) = msg.get("channel") {
                                if let Some(channel_name) = channel.as_str() {

                                    match channel_name {
                                        "ticker" => {
                                            let ticker : Ticker = serde_json::from_str(&text).unwrap();
                                            // println!("Ticker received {:#?}", ticker);
                                            
                                            for td in ticker.data {
                                                let event : NormalizedQuote = NormalizedQuote {
                                                    venue: "kraken".to_owned(),
                                                    instrument: td.symbol,
                                                    bid_price: td.bid,
                                                    bid_quantity: td.bid_qty,
                                                    ask_price: td.ask,
                                                    ask_quantity: td.ask_qty,
                                                    exch_timestamp: td.timestamp,
                                                    gateway_rec_timestamp: rec_timestamp
                                                };
                                                let marketevent : MarketEvent = MarketEvent::Quote(event);
                                                println!("Market event {:#?}", marketevent);
                                                tx.send(marketevent).await?;
                                            }
                                        }
                                        "trade" => {
                                            let trade : Trade = serde_json::from_str(&text).unwrap();

                                            // emit generalized 

                                            for t in trade.data {
                                                let event : NormalizedTrade = NormalizedTrade {
                                                        venue: "kraken".to_owned(),
                                                        instrument: t.symbol,
                                                        trade_id: t.trade_id.to_string(),
                                                        side: t.side,
                                                        price: t.price,
                                                        quantity: t.qty,
                                                        exch_timestamp: t.timestamp,
                                                        gateway_rec_timestamp: rec_timestamp
                                                };
                                                println!("Trade event {:#?}", event);

                                                let marketevent : MarketEvent = MarketEvent::Trade(event);
                                                println!("Market Event {:#?}", marketevent);
                                                tx.send(marketevent).await?;
                                            }
                                        }
                                        "book" => {
                                            let book: Book = serde_json::from_str(&text).unwrap();
                                            // println!("Book received {:#?}", book);
                                            if book.r#type == "snapshot" {
                                                for bd in book.data {
                                                    let event : NormalizedBookSnapshot = NormalizedBookSnapshot {
                                                        venue: "kraken".to_owned(),
                                                        instrument: bd.symbol,
                                                        bids: bd.bids.into_iter().map(|level| NormalizedPriceLevel { price: level.price, quantity: level.qty }).collect(),
                                                        asks: bd.asks.into_iter().map(|level| NormalizedPriceLevel { price: level.price, quantity: level.qty }).collect(),
                                                        source_checksum: Some(bd.checksum),
                                                        exch_timestamp: bd.timestamp,
                                                        gateway_rec_timestamp: rec_timestamp
                                                    };

                                                    let marketevent : MarketEvent = MarketEvent::BookSnapshot(event);
                                                    println!("Market event {:#?}", marketevent);
                                                    tx.send(marketevent).await?;
                                                }
                                            }
                                            else if book.r#type == "update" {
                                                for bd in book.data {
                                                    let event: NormalizedBookUpdate = NormalizedBookUpdate {
                                                        venue: "kraken".to_owned(),
                                                        instrument: bd.symbol,
                                                        bid_changes: bd.bids.into_iter().map(|level| NormalizedPriceLevel { price: level.price, quantity: level.qty }).collect(),
                                                        ask_changes: bd.asks.into_iter().map(|level| NormalizedPriceLevel { price: level.price, quantity: level.qty }).collect(),
                                                        source_checksum: Some(bd.checksum),
                                                        exch_timestamp: bd.timestamp,
                                                        gateway_rec_timestamp: rec_timestamp
                                                    };

                                                    let marketevent : MarketEvent = MarketEvent::BookUpdate(event);
                                                    println!("Market event {:#?}", marketevent);
                                                    tx.send(marketevent).await?;
                                                    
                                                }
                                            }
                                        }
                                        "instrument" => {
                                            let instrument : Instrument = serde_json::from_str(&text).unwrap();
                                            // we only care about instruments pertaining to our chosen symbol
                                            // go into pairs
                                            let pairs = &instrument.data.pairs;
                                            // println!("Instrument received {:#?}", instrument);
                                            // println!("Here are the pairs {:#?}", pairs);
                                            
                                        
                                            let result = pairs.iter().find(|pair| pair.symbol ==self.symbol);
                                            if let Some(found) = result {
                                                println!("Relevant is here {:#?}", found);
                                            }
                                        }
                                        _ => {
                                            println!("Not implemented yet");
                                        } 
                                    }
                                }
                            }
                        }

                        Ok(Message::Close(_)) => {
                            println!("Client closed");
                        },
                        _ => {} //ignore
                    }
                },

                end = signal::ctrl_c() => {
                    println!("Funky close");
                    break Ok(())
                },
            }
        }
    }
}



