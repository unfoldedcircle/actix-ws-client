//! Connection states of the pure core state machine.

/// Connection state of the WebSocket client.
///
/// Note: there is no dedicated `Closing` state. A graceful disconnect enqueues the
/// Close frame and tears the connection down in the same transition — the writer task
/// drains its queue (including the Close frame) before it terminates, so no
/// intermediate state is required.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnState {
    /// No connection and no reconnection scheduled.
    Disconnected,
    /// A connection attempt is in flight.
    Connecting,
    /// Connection established; writer/reader tasks and keep-alive are running.
    Connected,
    /// Connection lost or failed; a reconnect timer is armed.
    WaitingToReconnect,
}
