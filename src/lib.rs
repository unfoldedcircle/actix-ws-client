//! # Actix WS Client
//!
//! A flexible, actor-based WebSocket client for [Actix](https://actix.rs).
//!
//! This crate provides a [`GenericWebSocketClient`] actor that manages a WebSocket
//! connection, including automatic reconnection with jittered exponential backoff and
//! ping/pong keep-alive.
//!
//! ## Design
//!
//! The crate follows the [sans-IO](https://sans-io.readthedocs.io/) pattern: all
//! connection/reconnection/keep-alive policy lives in a pure, synchronous state
//! machine (`core`), which is fully unit-tested without sockets or timers. A thin
//! Actix adapter (`actor`) executes the machine's effects and feeds real-world
//! happenings back as events. See `README.md` for the full design description.
//!
//! ## Callback contract
//!
//! [`WebSocketMessageHandler::on_connected`] and
//! [`WebSocketMessageHandler::on_disconnected`] are edge-triggered and balanced:
//! exactly one `on_disconnected` per preceding `on_connected`, never duplicated.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use actix::prelude::*;
//! use actix_ws_client::{GenericWebSocketClient, HandlerAction, WebSocketConfig, WebSocketMessageHandler, Connect, SendText};
//! use bytes::Bytes;
//!
//! struct MyHandler;
//!
//! impl WebSocketMessageHandler for MyHandler {
//!     fn on_text(&mut self, text: String) -> Vec<HandlerAction> {
//!         println!("Received text: {text}");
//!         Vec::new()
//!     }
//!
//!     fn on_binary(&mut self, data: Bytes) -> Vec<HandlerAction> {
//!         println!("Received binary: {} bytes", data.len());
//!         Vec::new()
//!     }
//! }
//!
//! #[actix_rt::main]
//! async fn main() {
//!     let config = WebSocketConfig::builder().build().unwrap();
//!     let handler = MyHandler;
//!     let url = "ws://127.0.0.1:8080";
//!
//!     let client = GenericWebSocketClient::new("my-client", url, config, handler)
//!         .unwrap()
//!         .start();
//!
//!     // Connect to the server
//!     client.send(Connect).await.unwrap().unwrap();
//!
//!     // Send a message
//!     client.send(SendText::new("Hello!")).await.unwrap().unwrap();
//! }
//! ```

#![forbid(non_ascii_idents)]
#![deny(unsafe_code)]
#![deny(missing_docs)]

mod actor;
mod config;
mod core;
mod error;
mod handler;
mod message;
mod tls;

pub use actor::GenericWebSocketClient;
pub use config::{WebSocketConfig, WebSocketConfigBuilder, WebSocketConfigBuilderError};
pub use error::Error;
pub use handler::{HandlerAction, WebSocketMessageHandler};
pub use message::{Connect, Disconnect, IsConnected, Reconnect, SendBinary, SendText};
pub use tls::Certificate;
#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
pub use tls::TlsConnector;
