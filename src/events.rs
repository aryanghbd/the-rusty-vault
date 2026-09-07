use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;
use serde::{Deserialize, Serialize};
use std::io;
use rust_decimal::Decimal;

struct Trade {
    venue: String,
    instrument: String,
    trade_id: String,
    side: String,
    price: Decimal,
    quantity: Decimal,
    exch_timestamp: String,
    gateway_rec_timestamp: String
}