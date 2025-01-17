use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{StreamExt, SinkExt};
use serde_json::json;
use flate2::read::GzDecoder;
use std::io::prelude::*;

fn decompress_and_convert(binary_data: &[u8]) -> String {
    let mut decoder = GzDecoder::new(binary_data);
    let mut decompressed_data = String::new();
    decoder.read_to_string(&mut decompressed_data).expect("Failed to decompress data");
    decompressed_data
}

fn compress_to_binary(data: &str) -> Result<Vec<u8>, String> {
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data.as_bytes())
        .map_err(|e| format!("Compression error: {}", e))?;
    encoder
        .finish()
        .map_err(|e| format!("Compression finish error: {}", e))
}

const MARKET_URL: &str = "wss://api.huobi.pro/ws";

#[derive(Serialize, Deserialize)]
struct PingMessage {
    ping: i64,
}

#[derive(Serialize, Deserialize)]
struct PongMessage {
    pong: i64,
}


pub async fn test_market() {
   let (ws_stream, _) = connect_async(MARKET_URL)
        .await
        .expect("Failed to connect to WebSocket server");

    println!("Connected to WebSocket server");

    let (mut write, mut read) = ws_stream.split();

    // 构造订阅请求
    let subscription_message = json!({
        "sub": "market.btcusdt.kline.1min", 
        "id": "id1",
    });

    // 发送订阅请求
    write
        .send(Message::Text(subscription_message.to_string().into()))
        .await
        .expect("Failed to send subscription message");

    println!("Subscription message sent");

    // 监听并处理 WebSocket 消息
    while let Some(msg) = read.next().await {
        match msg {
            Ok(message) => match message {
                Message::Text(text) => {
                    println!("Received text message: {}", text);
                }
                Message::Binary(data) => {
                    let receive_data = decompress_and_convert(&data);
                    println!("Received binary message: {:?}", receive_data);

                    match serde_json::from_str::<PingMessage>(&receive_data) {
                        Ok(ping_message) => {
                            let m = ping_message.ping;
                            let pong_msg = PongMessage { pong: m };
                            let pong_msg_str = serde_json::to_string(&pong_msg).unwrap();
                            let pong_msg_binnary = compress_to_binary(&pong_msg_str).unwrap();
                            let pong_msg_to_send = Message::Text(pong_msg_str.into()); 
                            write.send(pong_msg_to_send)
                                .await
                                .expect("Failed to send pong");
                            }
                        Err(_) => {
                            println!("do nothing");
                        }
                    }
                }
                Message::Ping(data) => {
                    println!("Received Ping: {:?}", data);
                    write.send(Message::Pong(data)).await.expect("failed to send pong");
                }
                Message::Pong(data) => {
                    println!("Received Pong: {:?}", data);
                }
                other => {
                    println!("Received a unknown: {:?}", other);
                }
                // _ => {}
            },
            Err(e) => {
                eprintln!("Error reading message: {}", e);
                break;
            }
        }
    }

    println!("WebSocket connection closed");

}