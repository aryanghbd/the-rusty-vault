use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;
use std::io;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc, Local, TimeZone, NaiveDateTime, Duration};
use crate::events::{MarketEvent, PriceLevel as NormalizedPriceLevel, BookUpdate as NormalizedBookUpdate, BookSnapshot as NormalizedBookSnapshot, Quote as NormalizedQuote, Trade as NormalizedTrade};
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
struct BinanceDepthUpdate {
    #[serde(rename = "E")]
    event_timestamp : i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    asks: Vec<(Decimal, Decimal)>
}
#[derive(Deserialize, Debug)]
struct BinanceTicker {
    #[serde(rename = "E")]
    event_timestamp: i64,
    #[serde(rename = "s")]
    symbol: String, 
    #[serde(rename = "b")]
    best_bid_price: rust_decimal::Decimal,
    #[serde(rename = "B")]
    best_bid_quantity: rust_decimal::Decimal,    
    #[serde(rename = "a")]
    best_ask_price: rust_decimal::Decimal,
    #[serde(rename = "A")]
    best_ask_quantity: rust_decimal::Decimal
}

#[derive(Deserialize, Debug)]
struct BinanceTrade {
    #[serde(rename = "E")]
    event_timestamp: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: u64,
    #[serde(rename = "p")]
    price: rust_decimal::Decimal,
    #[serde(rename = "q")]
    quantity: rust_decimal::Decimal,
    #[serde(rename = "T")]
    trade_timestamp: i64,
    #[serde(rename = "m")]
    buyer_is_market_maker: bool //if true then this normalizes as a sell 
}
pub struct BinanceAdapter {
    symbol: String,
    // normalized instrument name (e.g. "BTC/USD") since binance returns it unseparated (e.g. "BTCUSD")
    display_symbol: String
}

impl BinanceAdapter {
    const WS_URL: &str = "wss://stream.binance.us:9443/ws";

    pub fn new(symbol: String) -> Self {
        // binance seems to have lower case symbols w no separator
        // so BTC/USD becomes btcusd
        return Self { symbol : symbol.to_lowercase().replace("/", ""), display_symbol: symbol.to_uppercase() };

    }

    pub async fn run(&self, tx : mpsc::Sender<MarketEvent>) -> Result<(), Box<dyn std::error::Error>> {
        let (mut ws_stream, _response) = connect_async(Self::WS_URL).await?;
        
        let subscription_message: String = format!(
            r#"{{ "method": "SUBSCRIBE", "params": ["{}@ticker", "{}@trade", "{}@depth@100ms"], "id": 1 }}"#,
            self.symbol, self.symbol, self.symbol
        );


        let (mut write, mut read) = ws_stream.split();

        write.send(Message::Text(subscription_message.into())).await?;

        loop {
            tokio::select! {
                Some(result) = read.next() => {

                    // binance routes by the lower case 'e' in the wss received msg:

                    // "e": "trade", "24hrTicker" or "depthUpdate"
                    match result {
                        Ok(Message::Text(text)) => {
                            let rec_timestamp = Utc::now();
                            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();
                            println!("Binance message received! {}", msg);

                            if let Some(channel) = msg.get("e") {
                                if let Some(channel_name) = channel.as_str() {
                                    match channel_name {
                                        "trade" => {
                                            let _trade: BinanceTrade = serde_json::from_str(&text).unwrap();

                                            let norm_event: NormalizedTrade = NormalizedTrade {
                                                venue: "binance".to_owned(),
                                                instrument: self.display_symbol.clone(),
                                                trade_id: _trade.trade_id.to_string(),
                                                side: if _trade.buyer_is_market_maker { "sell".to_string() } else { "buy".to_string() },
                                                price: _trade.price,
                                                quantity: _trade.quantity,
                                                exch_timestamp: Utc.timestamp_millis_opt(_trade.trade_timestamp).unwrap(),
                                                gateway_rec_timestamp: rec_timestamp,
                                            };

                                            let marketevent : MarketEvent = MarketEvent::Trade(norm_event);
                                            tx.send(marketevent).await?;
                                        }
                                        "24hrTicker" => {
                                            let _ticker: BinanceTicker = serde_json::from_str(&text).unwrap();

                                            let norm_event : NormalizedQuote = NormalizedQuote {
                                                venue: "binance".to_owned(),
                                                instrument: self.display_symbol.clone(),
                                                bid_price: _ticker.best_bid_price,
                                                bid_quantity: _ticker.best_bid_quantity,
                                                ask_price: _ticker.best_ask_price,
                                                ask_quantity: _ticker.best_ask_quantity,
                                                exch_timestamp: Utc.timestamp_millis_opt(_ticker.event_timestamp).unwrap(),
                                                gateway_rec_timestamp: rec_timestamp,
                                            };

                                            let marketevent : MarketEvent = MarketEvent::Quote(norm_event);
                                            tx.send(marketevent).await?;
                                        }
                                        "depthUpdate" => {
                                            let _depthupdate : BinanceDepthUpdate = serde_json::from_str(&text).unwrap();

                                            let norm_event : NormalizedBookUpdate = NormalizedBookUpdate {
                                                venue: "binance".to_owned(),
                                                instrument: self.display_symbol.clone(),
                                                bid_changes: _depthupdate.bids.into_iter().map(|(price, quantity)| NormalizedPriceLevel { price, quantity }).collect(),
                                                ask_changes: _depthupdate.asks.into_iter().map(|(price, quantity)| NormalizedPriceLevel { price, quantity }).collect(),
                                                source_checksum: None,
                                                exch_timestamp: Utc.timestamp_millis_opt(_depthupdate.event_timestamp).unwrap(),
                                                gateway_rec_timestamp: rec_timestamp,
                                            };

                                            let marketevent = MarketEvent::BookUpdate(norm_event);
                                            tx.send(marketevent).await?;
                                        }
                                        _ => { println!("Unimplemented or irrelevant"); }
                                    }
                                }


                            }
                        }

                        Ok(Message::Close(_)) => {
                            println!("Client closed");
                        }
                        Ok(_) => {}
                        Err(_) => todo!(),
                    }
                }
                end = signal::ctrl_c() => {
                    println!("Received shutdown signal, closing Binance connection");
                    break;
                }
                    
                }
            }
        Ok(())
    }


}