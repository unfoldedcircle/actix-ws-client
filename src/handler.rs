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

/// Why an established connection ended, see [`WebSocketMessageHandler::on_closed`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// The server closed the connection with a close frame.
    ClosedByServer {
        /// The close status code (RFC 6455 §7.4), e.g. 1000 (normal), 1001 (going
        /// away), 1008 (policy violation) or an application code 4000–4999. `None`
        /// if the close frame carried no status.
        code: Option<u16>,
        /// The close reason; empty if the server gave none.
        reason: String,
    },
    /// The client closed the connection: [`crate::Disconnect`], [`crate::Close`] or
    /// [`crate::Reconnect`] message, the corresponding [`HandlerAction`], or the
    /// actor stopped.
    ClosedLocally,
    /// The server stopped answering keep-alive pings.
    KeepAliveTimeout,
    /// Reading from or writing to the socket failed.
    Error(Error),
    /// The connection ended without a close frame and without an error.
    StreamEnded,
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
    /// Like [`HandlerAction::Disconnect`], with a specific close status code and
    /// reason in the close frame, optionally followed by a reconnect according to the
    /// reconnect policy — same semantics as the [`crate::Close`] message.
    /// An invalid code or a too long reason is logged and replaced by a normal
    /// close (1000).
    Close {
        /// The close status code, see [`crate::Close`].
        code: u16,
        /// The close reason, see [`crate::Close`].
        reason: String,
        /// Reconnect according to the reconnect policy after closing instead of
        /// stopping, see [`crate::Close::with_reconnect`].
        reconnect: bool,
    },
    /// Gracefully close the connection and reconnect according to the reconnect
    /// policy — same semantics as the [`crate::Reconnect`] message. Use to recover
    /// from a broken session with a fresh connection.
    Reconnect,
}

/// Trait to be implemented by handlers for WebSocket messages.
///
/// # Actions
///
/// Most callbacks return a list of [`HandlerAction`]s that the client executes in
/// order right after the callback returns. Return an empty `Vec` when nothing should
/// happen. Send actions returned while the connection is not up (after a `Disconnect`
/// action in the same batch, or from `on_closed`, `on_error` and
/// `on_reconnect_exhausted`) are dropped with a warning.
///
/// This is also how a handler decides about reconnecting. The client reconnects after
/// every failure, whatever its cause: return [`HandlerAction::Disconnect`] from
/// `on_error` or `on_closed` to stop when retrying is pointless — the server rejected
/// the credentials, or closed with a status that means "do not come back". The action
/// cancels the reconnect that is already scheduled.
///
/// Actions are ignored while the client stops.
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
    /// Called when an established connection ended, with the cause, directly before
    /// [`WebSocketMessageHandler::on_disconnected`].
    ///
    /// Use it to react to the server's close status code, e.g. to stop reconnecting
    /// with [`HandlerAction::Disconnect`] after an application-specific code.
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn on_closed(&mut self, reason: &DisconnectReason) -> Vec<HandlerAction> {
        Vec::new()
    }

    /// Called when the connection is lost or closed.
    fn on_disconnected(&mut self) {}
    /// Called when an error occurs: a connection attempt failed, or reading from or
    /// writing to an established connection failed.
    ///
    /// The client reconnects afterwards according to its reconnect policy. Return
    /// [`HandlerAction::Disconnect`] to stop instead, e.g. after
    /// [`Error::Http`] with status 401 or a rejected certificate ([`Error::Tls`]).
    ///
    /// The default implementation does nothing.
    #[allow(unused_variables)]
    fn on_error(&mut self, error: &Error) -> Vec<HandlerAction> {
        Vec::new()
    }
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
    /// message, or a returned [`HandlerAction::Reconnect`], starts a fresh attempt
    /// sequence.
    ///
    /// The default implementation does nothing.
    fn on_reconnect_exhausted(&mut self) -> Vec<HandlerAction> {
        Vec::new()
    }
}
