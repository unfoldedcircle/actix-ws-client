//! Input events consumed by the pure core state machine.

use crate::Error;
use bytes::Bytes;

/// A WebSocket frame as seen by the core.
///
/// This is a transport-agnostic representation so the core does not depend on
/// tungstenite types.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Frame {
    /// A text frame.
    Text(String),
    /// A binary frame.
    Binary(Bytes),
    /// A ping frame with its application payload.
    Ping(Bytes),
    /// A pong frame with its application payload.
    Pong(Bytes),
    /// A close frame.
    Close,
}

/// Everything that can happen in the real world, translated into an input for
/// [`crate::core::Machine::on_event`].
///
/// Events originating from a connection attempt or an established connection carry the
/// `generation` that was current when the originating task or timer was created. The
/// core ignores any event whose generation does not match the current one — this is
/// what makes stale writers, readers and timers harmless by construction.
#[derive(Debug)]
pub(crate) enum Event {
    /// The user requested a connection ([`crate::Connect`] message or `connect_on_start`).
    ConnectRequested,
    /// The user requested a graceful disconnect ([`crate::Disconnect`] message).
    DisconnectRequested,
    /// The user requested to drop the current connection and establish a new one
    /// ([`crate::Reconnect`] message or [`crate::HandlerAction::Reconnect`]).
    ReconnectRequested,
    /// The connect task finished successfully.
    ConnectSucceeded {
        /// Generation of the connect attempt.
        generation: u64,
    },
    /// The connect task failed (including timeout).
    ConnectFailed {
        /// Generation of the connect attempt.
        generation: u64,
        /// The connect error.
        error: Error,
    },
    /// The reader task received a frame.
    FrameReceived {
        /// Generation of the connection the frame belongs to.
        generation: u64,
        /// The received frame.
        frame: Frame,
    },
    /// The reader task stopped: end of stream, or a read error (connection lost).
    StreamEnded {
        /// Generation of the connection.
        generation: u64,
        /// The read error, if the stream ended due to one.
        error: Option<Error>,
    },
    /// The writer task failed to write to the sink.
    WriteFailed {
        /// Generation of the connection.
        generation: u64,
        /// The write error.
        error: Error,
    },
    /// The keep-alive interval timer fired.
    KeepAliveTick {
        /// Generation of the connection the timer belongs to.
        generation: u64,
    },
    /// The reconnect timer fired.
    ReconnectTimerFired,
}
