//! Echo client example.
//!
//! Start any WebSocket echo server, e.g. `websocat -s 8080`, then:
//!
//! ```sh
//! cargo run --example echo_client
//! # or with a custom endpoint and debug logging:
//! WS_URL=wss://echo.websocket.org RUST_LOG=debug cargo run --example echo_client
//! ```

use actix::prelude::*;
use actix_ws_client::{
    Connect, Disconnect, Error, GenericWebSocketClient, HandlerAction, SendBinary, SendText,
    WebSocketConfig, WebSocketMessageHandler,
};
use bytes::Bytes;
use env_logger::Env;
use std::time::Duration;

struct EchoHandler {
    received: u32,
}

impl WebSocketMessageHandler for EchoHandler {
    fn on_text(&mut self, text: String) -> Vec<HandlerAction> {
        self.received += 1;
        println!("[echo #{}] {text}", self.received);
        Vec::new()
    }

    fn on_binary(&mut self, data: Bytes) -> Vec<HandlerAction> {
        self.received += 1;
        println!("[echo #{}] {} bytes binary", self.received, data.len());
        Vec::new()
    }

    fn on_connected(&mut self) -> Vec<HandlerAction> {
        println!("Connected!");
        Vec::new()
    }

    fn on_disconnected(&mut self) {
        println!("Disconnected.");
    }

    fn on_error(&mut self, error: &Error) -> Vec<HandlerAction> {
        eprintln!("Error: {error}");
        Vec::new()
    }
}

#[actix::main]
async fn main() -> Result<(), Error> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let url = std::env::var("WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080".to_string());
    let config = WebSocketConfig::builder()
        .ping_interval(Duration::from_secs(10))
        .build()
        .expect("valid config");

    let client =
        GenericWebSocketClient::new("echo", url.as_str(), config, EchoHandler { received: 0 })?
            .start();

    println!("Connecting to {url} ...");
    client.send(Connect).await.expect("mailbox error")?;

    for i in 1..=3 {
        client
            .send(SendText::new(format!("Test message #{i}")))
            .await
            .expect("mailbox error")?;
    }
    client
        .send(SendBinary::new(Bytes::from_static(b"\x01\x02\x03")))
        .await
        .expect("mailbox error")?;

    // wait for the echoes
    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Disconnecting...");
    client.send(Disconnect).await.expect("mailbox error");

    Ok(())
}
