//! Output effects produced by the pure core state machine.

use crate::Error;
use crate::core::event::Frame;
use crate::handler::DisconnectReason;
use bytes::Bytes;
use std::time::Duration;

/// A callback to be delivered to the user's [`crate::WebSocketMessageHandler`].
#[allow(clippy::enum_variant_names)]
#[derive(Debug, PartialEq)]
pub(crate) enum Callback {
    /// Deliver a received text frame.
    OnText(String),
    /// Deliver a received binary frame.
    OnBinary(Bytes),
    /// The connection was established (edge-triggered, balanced with `OnDisconnected`).
    OnConnected,
    /// Why the connection ended; always directly followed by `OnDisconnected`.
    OnClosed(DisconnectReason),
    /// The connection ended (edge-triggered, balanced with `OnConnected`).
    OnDisconnected,
    /// An error occurred.
    OnError(Error),
    /// The maximum number of reconnection attempts was reached; the client stops
    /// reconnecting until a new connect/reconnect request.
    OnReconnectExhausted,
}

/// How the adapter ends a connection, see [`Effect::TearDownConnection`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Teardown {
    /// Drop the connection at once; messages that are not written yet fail.
    ///
    /// Used when the connection is broken (read or write error, keep-alive timeout)
    /// and when there is no established connection (cancelled connection attempt or
    /// reconnect timer).
    Abort,
    /// A close frame was enqueued: write the queue including the close frame, then
    /// wait until the server answered it and closed the connection — at most for the
    /// close timeout.
    Close,
    /// The server sent a close frame, so nothing can be written anymore. The
    /// transport answers the close frame itself: wait until the server closed the
    /// connection — at most for the close timeout.
    PeerClosed,
}

/// An instruction from the core to the I/O adapter (the actor).
///
/// The core decides *what* should happen; the adapter executes it. Effects never carry
/// I/O resources — the adapter owns those, keyed by generation.
#[derive(Debug, PartialEq)]
pub(crate) enum Effect {
    /// Start a connection attempt for `generation` with the given timeout.
    ///
    /// Executing this cancels any armed reconnect timer.
    StartConnect {
        /// Generation assigned to this attempt.
        generation: u64,
        /// Connect timeout (including redirects).
        timeout: Duration,
    },
    /// Start the connection task owning the transport of connection `generation`.
    StartConnectionTask {
        /// Generation of the connection.
        generation: u64,
    },
    /// Enqueue a core-originated frame (ping, close) to the writer.
    SendFrame {
        /// The frame to send.
        frame: Frame,
    },
    /// Tear down all per-connection resources of `generation`: end the connection
    /// task as given by `mode`, cancel the connect task, keep-alive and reconnect
    /// timers.
    TearDownConnection {
        /// Generation of the connection to tear down.
        generation: u64,
        /// How the connection is ended.
        mode: Teardown,
    },
    /// Arm the reconnect timer with the given delay.
    ArmReconnectTimer {
        /// Generation of the attempt or connection that ended: the tick of the timer
        /// carries it back and is ignored once a newer attempt was started.
        generation: u64,
        /// Backoff delay until the next connection attempt.
        delay: Duration,
    },
    /// Start the keep-alive interval timer for connection `generation`.
    StartKeepAlive {
        /// Generation of the connection.
        generation: u64,
        /// Ping interval.
        interval: Duration,
    },
    /// Stop the keep-alive timer.
    StopKeepAlive,
    /// Invoke a user callback.
    Emit(Callback),
    /// Resolve the pending [`crate::Connect`] requests, if any.
    CompleteConnect {
        /// Result to deliver to the caller.
        result: Result<(), Error>,
    },
}
