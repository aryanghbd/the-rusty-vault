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
    side: String, // bid or offer.
    event_time: DateTime<Utc>,
    price_level: rust_decimal::Decimal,
    new_quantity: rust_decimal::Decimal
}

#[derive(Deserialize, Debug)]
struct L2Event {
    r#type: String, // can be either snapshot or update
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
    const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

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
                            let rec_timestamp = Utc::now();
                            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();

                            if let Some(channel) = msg.get("channel") {
                                if let Some(channel_name) = channel.as_str() {
                                    match channel_name {
                                        "ticker" => {
                                            let ticker: Ticker = serde_json::from_str(&text).unwrap();

                                            // now we need to convert to normalized again

                                            for event in ticker.events {
                                                for tic in event.tickers {
                                                    let norm_event : NormalizedQuote = NormalizedQuote {
                                                        venue: "coinbase".to_owned(),
                                                        instrument: tic.product_id,
                                                        bid_price: tic.best_bid,
                                                        bid_quantity: tic.best_bid_quantity,
                                                        ask_price: tic.best_ask,
                                                        ask_quantity: tic.best_ask_quantity,
                                                        exch_timestamp: ticker.timestamp,
                                                        gateway_rec_timestamp: rec_timestamp
                                                    };

                                                    let marketevent : MarketEvent = MarketEvent::Quote(norm_event);
                                                    println!("Market event {:#?}", marketevent);
                                                    tx.send(marketevent).await?;
                                                }
                                            }
    
                                        }

                                        "market_trades" => {
                                            let market_trade : MarketTradeMessage = serde_json::from_str(&text).unwrap();

                                            for event in market_trade.events {
                                                for trade in event.trades {
                                                    let norm_event : NormalizedTrade = NormalizedTrade {
                                                        venue: "coinbase".to_owned(),
                                                        instrument: trade.product_id,
                                                        trade_id: trade.trade_id,
                                                        side: trade.side,
                                                        price: trade.price,
                                                        quantity: trade.size,
                                                        exch_timestamp: trade.time, //since multiple trades can be bundled in one message we have to be more specific
                                                        gateway_rec_timestamp: rec_timestamp
                                                    };

                                                    let marketevent : MarketEvent = MarketEvent::Trade(norm_event);
                                                    println!("Market Event {:#?}", marketevent);
                                                    tx.send(marketevent).await?;
                                                }
                                            }
                                        }

                                        "level2" => {
                                            let l2m : L2Message = serde_json::from_str(&text).unwrap();

                                            // need to check type
                                            for event in l2m.events {
                                                
                                                if event.r#type == "snapshot" {
                                                    let product_id = event.product_id.clone();
                                                    // separate into bids and asks
                                                    // go into the 'updates' field and separate bids and asks

                                                    let mut bids:Vec<NormalizedPriceLevel> = Vec::new();
                                                    let mut asks:Vec<NormalizedPriceLevel> = Vec::new();
                                                    

                                                    for update in event.updates {
                                                        if update.side == "bid" {
                                                            let normalized_price_level : NormalizedPriceLevel = NormalizedPriceLevel {
                                                                price : update.price_level,
                                                                quantity: update.new_quantity
                                                            };
                                                            bids.push(normalized_price_level);
                                                        }

                                                        else if update.side == "offer" {
                                                            let normalized_price_level : NormalizedPriceLevel = NormalizedPriceLevel {
                                                                price : update.price_level,
                                                                quantity: update.new_quantity
                                                            };
                                                            asks.push(normalized_price_level);
                                                        }
                                                    }

                                                    // Now that we have the bids and asks, we can construct the BookSnapshot event and emit 

                                                    let norm_event : NormalizedBookSnapshot = NormalizedBookSnapshot {
                                                        venue: "coinbase".to_owned(),
                                                        instrument: product_id,
                                                        bids: bids,
                                                        asks: asks,
                                                        exch_timestamp: l2m.timestamp,
                                                        source_checksum: None,
                                                        gateway_rec_timestamp: rec_timestamp
                                                    };

                                                    let marketevent : MarketEvent = MarketEvent::BookSnapshot(norm_event);
                                                    println!("Market Event {:#?}", marketevent);
                                                    tx.send(marketevent).await?;

                                                }

                                                else if event.r#type == "update" {
                                                    // separate into bid changes and ask changes
                                                    let mut bid_updates:Vec<NormalizedPriceLevel> = Vec::new();
                                                    let mut ask_updates:Vec<NormalizedPriceLevel> = Vec::new();
                                                    let product_id = event.product_id.clone();

                                                    for update in event.updates {
                                                        if update.side == "bid" {
                                                            let normalized_price_level : NormalizedPriceLevel = NormalizedPriceLevel {
                                                                price : update.price_level,
                                                                quantity: update.new_quantity
                                                            };
                                                            bid_updates.push(normalized_price_level);
                                                        }

                                                        else if update.side == "offer" {
                                                            let normalized_price_level : NormalizedPriceLevel = NormalizedPriceLevel {
                                                                price : update.price_level,
                                                                quantity: update.new_quantity
                                                            };
                                                            ask_updates.push(normalized_price_level);
                                                        }
                                                    }
                                                    let norm_event : NormalizedBookUpdate = NormalizedBookUpdate {
                                                        venue: "coinbase".to_owned(),
                                                        instrument: product_id.clone(),
                                                        bid_changes: bid_updates,
                                                        ask_changes: ask_updates,
                                                        source_checksum: None,
                                                        exch_timestamp: l2m.timestamp,
                                                        gateway_rec_timestamp: rec_timestamp
                                                    };

                                                    let marketevent : MarketEvent = MarketEvent::BookUpdate(norm_event);
                                                    println!("Market Event {:#?}", marketevent);
                                                    tx.send(marketevent).await?;
                                                }

                                                


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
                        }
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