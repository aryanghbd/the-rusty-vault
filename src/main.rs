use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;
use serde::{Deserialize, Serialize};
use std::io;
use rust_decimal::Decimal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

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
        timestamp: String, 
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
        timestamp: String
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
        timestamp: String
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

    println!("Hi");
    let url = "wss://ws.kraken.com/v2";
    
    // mut because both sending and receiving messages modify the internal state of the websocket stream object.
    let (mut ws_stream, _response) = connect_async(url).await?;
    println!("Connected to {}", url);

    println!("Choose a symbol");
    // take the string
    let mut user_input = String::new();

    io::stdin()
        .read_line(&mut user_input)
        .expect("Failed to read symbol");

    let symbol = user_input.trim();

    println!("Pulling up ticker updates for {symbol}");

    let ticker = format!(
        r#"{{ "method": "subscribe", "params": {{ "channel": "ticker", "symbol": ["{}"] }} }}"#,
        symbol
    );
    let trade = format!(
        r#"{{ "method": "subscribe", "params": {{ "channel": "trade", "symbol": ["{}"], "snapshot": true }} }}"#,
        symbol
    );
    let book_l2 = format!(
        r#"{{ "method": "subscribe", "params": {{ "channel": "book", "symbol": ["{}"] }} }}"#,
        symbol
    );
    let instrument = format!(
        r#"{{ "method": "subscribe", "params": {{ "channel": "instrument"}} }}"#,
    );
    // split websocket stream into a sender and receiver for fun bidirectionality.
    let (mut write, mut read) = ws_stream.split();

    write.send(Message::Text(instrument.into())).await?;
    println!("Subscription request sent!");


    // now we should listen and see whats up
    loop {
        tokio::select! {
            
            //Option has two possibilities, some value exists, or nothing
            // branch 1, did data arrive
            Some(result) = read.next() => {
                match result {
                    Ok(Message::Text(text)) => {
                        println!("msg received {}", text);

                        let msg: serde_json::Value = serde_json::from_str(&text).unwrap();                 
                        // now index by channel

                        if let Some(channel) = msg.get("channel") {
                            if let Some(channel_name) = channel.as_str() {

                                match channel_name {
                                    "ticker" => {
                                        let ticker : Ticker = serde_json::from_str(&text).unwrap();
                                        println!("Ticker received {:#?}", ticker);
                                    }
                                    "trade" => {
                                        let trade : Trade = serde_json::from_str(&text).unwrap();
                                        println!("Trade received {:#?}", trade);
                                    }
                                    "book" => {
                                        let book: Book = serde_json::from_str(&text).unwrap();
                                        println!("Book received {:#?}", book);
                                    }
                                    "instrument" => {
                                        let instrument : Instrument = serde_json::from_str(&text).unwrap();
                                        // we only care about instruments pertaining to our chosen symbol
                                        // go into pairs
                                        let pairs = &instrument.data.pairs;
                                        println!("Instrument received {:#?}", instrument);
                                        println!("Here are the pairs {:#?}", pairs);
                                        
                                    
                                        let result = pairs.iter().find(|pair| pair.symbol == symbol);
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
