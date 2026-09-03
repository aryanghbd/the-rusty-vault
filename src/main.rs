use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hi");
    let url = "wss://ws.kraken.com/v2";
    
    // mut because both sending and receiving messages modify the internal state of the websocket stream object.
    let (mut ws_stream, _response) = connect_async(url).await?;
    println!("Connected to {}", url);

    let ticker = r#"
    {
        "method": "subscribe",
        "params": {
            "channel": "ticker",
            "symbol": [
                "ALGO/USD"
            ]
        }
    }
    "#;

    // split websocket stream into a sender and receiver for fun bidirectionality.
    let (mut write, mut read) = ws_stream.split();

    write.send(Message::Text(ticker.into())).await?;
    println!("Subscription request sent!");


    // now we should listen and see whats up
    loop {
        tokio::select! {

            // branch 1, did data arrive
            Some(result) = read.next() => {
                match result {
                    Ok(Message::Text(text)) => {
                        println!("msg received {}", text);
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
