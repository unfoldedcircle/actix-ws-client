//! End-to-end tests against a real, local `tokio-tungstenite` server.
//!
//! These cover the happy path over actual TCP: connect (including an HTTP redirect),
//! echo round-trips, and the graceful close handshake. All hard logic paths
//! (reconnection, keep-alive, stale generations) are covered deterministically by the
//! unit tests inside the crate.

use actix::prelude::*;
use actix_ws_client::{
    Connect, Disconnect, Error, GenericWebSocketClient, HandlerAction, IsConnected, SendBinary,
    SendText, WebSocketConfig, WebSocketMessageHandler,
};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::Message;

#[derive(Clone, Default)]
struct RecordingHandler {
    log: Arc<Mutex<Vec<String>>>,
}

impl RecordingHandler {
    fn contains(&self, entry: &str) -> bool {
        self.log.lock().unwrap().iter().any(|e| e == entry)
    }

    fn count(&self, entry: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == entry)
            .count()
    }
}

impl WebSocketMessageHandler for RecordingHandler {
    fn on_text(&mut self, text: String) -> Vec<HandlerAction> {
        self.log.lock().unwrap().push(format!("text:{text}"));
        Vec::new()
    }
    fn on_binary(&mut self, data: Bytes) -> Vec<HandlerAction> {
        self.log
            .lock()
            .unwrap()
            .push(format!("binary:{}", data.len()));
        Vec::new()
    }
    fn on_connected(&mut self) -> Vec<HandlerAction> {
        self.log.lock().unwrap().push("connected".into());
        Vec::new()
    }
    fn on_disconnected(&mut self) {
        self.log.lock().unwrap().push("disconnected".into());
    }
    fn on_error(&mut self, error: &Error) {
        self.log.lock().unwrap().push(format!("error:{error}"));
    }
}

/// Spawns a WebSocket echo server; returns its `ws://` URL.
async fn spawn_echo_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let ws = match tokio_tungstenite::accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(_) => return,
                };
                let (mut sink, mut source) = ws.split();
                while let Some(Ok(msg)) = source.next().await {
                    match msg {
                        Message::Text(_) | Message::Binary(_) => {
                            if sink.send(msg).await.is_err() {
                                break;
                            }
                        }
                        Message::Close(_) => {
                            let _ = sink.send(Message::Close(None)).await;
                            break;
                        }
                        Message::Ping(payload) => {
                            let _ = sink.send(Message::Pong(payload)).await;
                        }
                        _ => {}
                    }
                }
            });
        }
    });
    format!("ws://{addr}")
}

/// Spawns a plain HTTP server that 302-redirects every request to `target`.
async fn spawn_redirect_server(target: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let target = target.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\n\r\n"
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    format!("ws://{addr}")
}

fn start_client(
    url: &str,
    handler: RecordingHandler,
) -> Addr<GenericWebSocketClient<RecordingHandler>> {
    let config = WebSocketConfig::builder().build().unwrap();
    GenericWebSocketClient::new("e2e", url, config, handler)
        .unwrap()
        .start()
}

async fn wait_until<F: Fn() -> bool>(cond: F, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[actix_rt::test]
async fn echo_roundtrip() {
    let url = spawn_echo_server().await;
    let handler = RecordingHandler::default();
    let client = start_client(&url, handler.clone());

    client.send(Connect).await.unwrap().unwrap();
    assert!(client.send(IsConnected).await.unwrap());
    assert!(handler.contains("connected"));

    client
        .send(SendText::new("hello e2e"))
        .await
        .unwrap()
        .unwrap();
    wait_until(|| handler.contains("text:hello e2e"), "text echo").await;

    client
        .send(SendBinary::new(Bytes::from_static(b"\x01\x02\x03")))
        .await
        .unwrap()
        .unwrap();
    wait_until(|| handler.contains("binary:3"), "binary echo").await;
}

#[actix_rt::test]
async fn graceful_disconnect_close_handshake() {
    let url = spawn_echo_server().await;
    let handler = RecordingHandler::default();
    let client = start_client(&url, handler.clone());

    client.send(Connect).await.unwrap().unwrap();
    client.send(Disconnect).await.unwrap();

    assert!(!client.send(IsConnected).await.unwrap());
    wait_until(
        || handler.count("disconnected") == 1,
        "disconnected callback",
    )
    .await;

    // no reconnection after a graceful disconnect
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(handler.count("connected"), 1);
    assert_eq!(handler.count("disconnected"), 1);
}

#[actix_rt::test]
async fn connect_follows_http_redirect() {
    let echo_url = spawn_echo_server().await;
    let redirect_url = spawn_redirect_server(echo_url).await;

    let handler = RecordingHandler::default();
    let client = start_client(&redirect_url, handler.clone());

    client.send(Connect).await.unwrap().unwrap();
    assert!(client.send(IsConnected).await.unwrap());

    client
        .send(SendText::new("via redirect"))
        .await
        .unwrap()
        .unwrap();
    wait_until(
        || handler.contains("text:via redirect"),
        "echo via redirect",
    )
    .await;
}

#[actix_rt::test]
async fn oversized_inbound_message_drops_and_reconnects() {
    let url = spawn_echo_server().await;
    let handler = RecordingHandler::default();
    let config = WebSocketConfig::builder()
        .max_message_size(1024usize)
        .initial_backoff(Duration::from_millis(50))
        .build()
        .unwrap();
    let client = GenericWebSocketClient::new("e2e", url.as_str(), config, handler.clone())
        .unwrap()
        .start();

    client.send(Connect).await.unwrap().unwrap();

    // small message passes
    client.send(SendText::new("small")).await.unwrap().unwrap();
    wait_until(|| handler.contains("text:small"), "small echo").await;

    // the 4 KiB echo exceeds the 1 KiB inbound limit -> read error -> reconnect
    let big = "x".repeat(4096);
    client
        .send(SendText::new(big.clone()))
        .await
        .unwrap()
        .unwrap();

    wait_until(
        || handler.count("connected") == 2,
        "reconnect after capacity error",
    )
    .await;
    assert!(
        !handler.contains(&format!("text:{big}")),
        "oversized echo must not be delivered"
    );
    assert_eq!(handler.count("disconnected"), 1);
}

#[actix_rt::test]
async fn connect_to_unreachable_server_fails() {
    let handler = RecordingHandler::default();
    // reserved port with nothing listening; short timeout in case the port is
    // silently dropped instead of refused
    let config = WebSocketConfig::builder()
        .connect_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let client = GenericWebSocketClient::new("e2e", "ws://127.0.0.1:9", config, handler.clone())
        .unwrap()
        .start();

    let result = client.send(Connect).await.unwrap();
    assert!(result.is_err());
    assert!(!client.send(IsConnected).await.unwrap());
    // failed connect is not a disconnect (edge-triggered contract)
    assert_eq!(handler.count("disconnected"), 0);
}
