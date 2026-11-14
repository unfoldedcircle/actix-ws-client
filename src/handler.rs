//! Callback trait implemented by users of the WebSocket client.

use crate::Error;
use bytes::Bytes;
use std::borrow::Cow;

/// Direction of a text message that is about to be logged by message tracing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceDirection {
    /// A message received from the server (`msg_tracing_in`).
    Incoming,
    /// A message sent to the server (`msg_tracing_out`).
    Outgoing,
}

/// An action returned from a handler callback, executed by the client actor right
/// after the callback returns (same invocation — effectively synchronous).
///
/// This is how a handler sends outbound frames (e.g. an authentication reply) or
/// disconnects itself on a fatal message, without holding a reference to the client:
/// the handler stays a pure, unit-testable function of its input.
#[derive(Debug, PartialEq)]
pub enum HandlerAction {
    /// Send a text frame.
    SendText(String),
    /// Send a binary frame.
    SendBinary(Bytes),
    /// Gracefully close the connection and stop reconnecting — same semantics as the
    /// [`crate::Disconnect`] message. Use for fatal conditions (e.g. authentication
    /// failure) where retrying would be pointless.
    Disconnect,
    /// Gracefully close the connection and reconnect according to the reconnect
    /// policy — same semantics as the [`crate::Reconnect`] message. Use to recover
    /// from a broken session with a fresh connection.
    Reconnect,
}

/// Trait to be implemented by handlers for WebSocket messages.
///
/// # Actions
///
/// `on_text`, `on_binary` and `on_connected` return a list of [`HandlerAction`]s that
/// the client executes in order right after the callback returns. Return an empty
/// `Vec` when nothing should happen. Send actions returned while the connection is no
/// longer up (e.g. after a `Disconnect` action in the same batch) are dropped with a
/// warning.
///
/// # Callback contract
///
/// `on_connected` and `on_disconnected` are **edge-triggered and balanced**:
/// `on_connected` is called exactly once per established connection, and
/// `on_disconnected` is called exactly once when that connection ends — never
/// duplicated, never unpaired.
pub trait WebSocketMessageHandler: Send + Unpin + 'static {
    /// Called when a text message is received from the server.
    fn on_text(&mut self, text: String) -> Vec<HandlerAction>;
    /// Called when a binary message is received from the server.
    ///
    /// The default implementation ignores the data.
    fn on_binary(&mut self, _data: Bytes) -> Vec<HandlerAction> {
        Vec::new()
    }
    /// Called when the connection is established.
    ///
    /// Returned send actions are transmitted on the new connection.
    fn on_connected(&mut self) -> Vec<HandlerAction> {
        Vec::new()
    }
    /// Called when the connection is lost or closed.
    fn on_disconnected(&mut self) {}
    /// Called when an error occurs.
    #[allow(unused_variables)]
    fn on_error(&mut self, error: &Error) {}
    /// Called before a text message is logged by message tracing
    /// ([`crate::WebSocketConfig`] `msg_tracing_in` / `msg_tracing_out`), to keep
    /// secrets out of the log.
    ///
    /// Return the text to log — the message itself or a redacted copy — or `None` to
    /// not log this message at all. Only the log output is affected, never the
    /// message on the wire. Not called while message tracing is disabled.
    ///
    /// The default implementation logs every message unchanged.
    #[allow(unused_variables)]
    fn redact_trace<'a>(&self, direction: TraceDirection, text: &'a str) -> Option<Cow<'a, str>> {
        Some(Cow::Borrowed(text))
    }

    /// Called when the configured maximum number of reconnection attempts
    /// ([`crate::WebSocketConfig`] `max_reconnect_attempts`) has been reached.
    ///
    /// The client stops reconnecting; a new [`crate::Connect`] or [`crate::Reconnect`]
    /// message starts a fresh attempt sequence.
    fn on_reconnect_exhausted(&mut self) {}
}
