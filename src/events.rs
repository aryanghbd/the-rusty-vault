use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;
use serde::{Deserialize, Serialize};
use std::io;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc, Local, TimeZone, NaiveDateTime, Duration};

// generalized market events that must be normalized from each provider into this format
#[derive(Deserialize, Debug)]
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    BookSnapshot(BookSnapshot),
    BookUpdate(BookUpdate)
}

#[derive(Deserialize, Debug)]
pub struct Trade {
    pub(crate) venue: String,
    pub(crate) instrument: String,
    pub(crate) trade_id: String,
    pub(crate) side: String,
    pub(crate) price: Decimal,
    pub(crate) quantity: Decimal,
    pub(crate) exch_timestamp: DateTime<Utc>,
    pub(crate) gateway_rec_timestamp: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
pub struct Quote {
    pub(crate) venue: String,
    pub(crate) instrument: String,
    pub(crate) bid_price: Decimal,
    pub(crate) bid_quantity: Decimal,
    pub(crate) ask_price: Decimal,
    pub(crate) ask_quantity: Decimal,
    pub(crate) exch_timestamp: DateTime<Utc>,
    pub(crate) gateway_rec_timestamp: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
pub struct PriceLevel {
    pub(crate) price: Decimal,
    pub(crate) quantity: Decimal
}

#[derive(Deserialize, Debug)]
pub struct BookSnapshot {
    pub(crate) venue: String,
    pub(crate) instrument: String,
    pub(crate) bids: Vec<PriceLevel>,
    pub(crate) asks: Vec<PriceLevel>,
    pub(crate) source_checksum: Option<u64>, // this may or may not be present
    pub(crate) exch_timestamp: DateTime<Utc>,
    pub(crate) gateway_rec_timestamp: DateTime<Utc>
}

#[derive(Deserialize, Debug)]
pub struct BookUpdate {
    pub(crate) venue: String,
    pub(crate) instrument: String,
    pub(crate) bid_changes: Vec<PriceLevel>,
    pub(crate) ask_changes: Vec<PriceLevel>,
    pub(crate) source_checksum: Option<u64>,
    pub(crate) exch_timestamp: DateTime<Utc>,
    pub(crate) gateway_rec_timestamp: DateTime<Utc>
}