//! Error type of the `actix-ws-client` crate.

/// Errors that can occur when using the `actix-ws-client` crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Building the client request failed.
    InvalidRequest(String),
    /// The connection attempt timed out.
    ConnectTimeout,
    /// The underlying WebSocket stack returned an error (connect/read/write/etc.).
    WebSocket(String),
    /// Operation cannot proceed because the client is not connected.
    NotConnected,
    /// The actor state changed during an in-flight operation, invalidating the result.
    StateChanged,
    /// Sending a message over the socket failed.
    SendFailed(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidRequest(e) => write!(f, "Invalid request: {e}"),
            Error::ConnectTimeout => write!(f, "Connection timeout"),
            Error::WebSocket(e) => write!(f, "WebSocket error: {e}"),
            Error::NotConnected => write!(f, "Not connected"),
            Error::StateChanged => write!(f, "State changed during operation"),
            Error::SendFailed(e) => write!(f, "Send failed: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(value: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(value.to_string())
    }
}
