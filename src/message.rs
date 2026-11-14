//! Public Actix messages accepted by the WebSocket client actor.

use crate::Error;
use actix::prelude::*;
use bytes::Bytes;

/// Get connection state.
#[derive(Message)]
#[rtype(result = "bool")]
pub struct IsConnected;

/// Message sent to the [`crate::GenericWebSocketClient`] to initiate a connection.
#[derive(Message)]
#[rtype(result = "Result<(), Error>")]
pub struct Connect;

/// Message sent to the [`crate::GenericWebSocketClient`] to send a text frame.
#[derive(Message, Clone)]
#[rtype(result = "Result<(), Error>")]
pub struct SendText {
    /// The text to send.
    pub text: String,
}

impl SendText {
    /// Creates a new text-frame message.
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

/// Message sent to the [`crate::GenericWebSocketClient`] to send a binary frame.
#[derive(Message, Clone)]
#[rtype(result = "Result<(), Error>")]
pub struct SendBinary {
    /// The binary data to send.
    pub data: Bytes,
}

impl SendBinary {
    /// Creates a new binary-frame message.
    pub fn new(data: impl Into<Bytes>) -> Self {
        Self { data: data.into() }
    }
}

/// Message sent to the [`crate::GenericWebSocketClient`] to trigger a graceful disconnect.
///
/// This will send a Close frame to the server before tearing down the connection.
/// Automatic reconnection is stopped; a subsequent [`Connect`] is required to
/// re-establish the connection.
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect;

/// Message sent to the [`crate::GenericWebSocketClient`] to gracefully close the
/// current connection (if any) and establish a new one.
///
/// In contrast to [`Disconnect`], the client reconnects according to its reconnect
/// policy, starting with a fresh attempt counter. Useful to force a new connection
/// after configuration or address changes.
#[derive(Message)]
#[rtype(result = "()")]
pub struct Reconnect;
