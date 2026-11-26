//! Error type of the `actix-ws-client` crate.

use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::http::StatusCode;

/// Errors that can occur when using the `actix-ws-client` crate.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Building the client request failed.
    InvalidRequest(String),
    /// The connection attempt timed out.
    ConnectTimeout,
    /// A message could not be written within [`crate::WebSocketConfig`]
    /// `write_timeout`: the connection does not take data anymore.
    WriteTimeout,
    /// The TLS configuration is invalid or the TLS handshake failed (e.g. the server
    /// certificate was rejected).
    Tls(String),
    /// The server answered the WebSocket handshake with an HTTP error status
    /// (e.g. 401, 404).
    Http {
        /// The HTTP status code.
        status: u16,
    },
    /// A network I/O error (connection refused, reset, unreachable host, ...).
    Io {
        /// The kind of the I/O error.
        kind: std::io::ErrorKind,
        /// The error description.
        message: String,
    },
    /// Any other error of the underlying WebSocket stack (protocol violation, message
    /// too large, invalid redirect, ...).
    WebSocket(String),
    /// Operation cannot proceed because the client is not connected.
    NotConnected,
    /// The operation was cancelled by a state change of the client: a
    /// [`crate::Connect`] request whose connection attempt was cancelled by a
    /// disconnect.
    StateChanged,
    /// Sending a message over the socket failed: the answer to the
    /// [`crate::SendText`] / [`crate::SendBinary`] request of that message. The
    /// connection ends with the classified error of the socket, see
    /// [`crate::DisconnectReason::Error`].
    SendFailed(String),
    /// The message was not accepted because the send queue is full, see
    /// [`crate::WebSocketConfig`] `max_send_queue`.
    SendQueueFull,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidRequest(e) => write!(f, "Invalid request: {e}"),
            Error::ConnectTimeout => write!(f, "Connection timeout"),
            Error::WriteTimeout => write!(f, "Write timeout"),
            Error::Tls(e) => write!(f, "WebSocket error: TLS error: {e}"),
            Error::Http { status } => match StatusCode::from_u16(*status) {
                Ok(status) => write!(f, "WebSocket error: HTTP error: {status}"),
                Err(_) => write!(f, "WebSocket error: HTTP error: {status}"),
            },
            Error::Io { message, .. } => write!(f, "WebSocket error: IO error: {message}"),
            Error::WebSocket(e) => write!(f, "WebSocket error: {e}"),
            Error::NotConnected => write!(f, "Not connected"),
            Error::StateChanged => write!(f, "State changed during operation"),
            Error::SendFailed(e) => write!(f, "Send failed: {e}"),
            Error::SendQueueFull => write!(f, "Send queue full"),
        }
    }
}

impl std::error::Error for Error {}

impl From<WsError> for Error {
    fn from(value: WsError) -> Self {
        match value {
            WsError::Io(e) => from_io(e),
            WsError::Tls(e) => Error::Tls(e.to_string()),
            WsError::Http(response) => Error::Http {
                status: response.status().as_u16(),
            },
            other => Error::WebSocket(other.to_string()),
        }
    }
}

/// Classifies an I/O error. rustls reports TLS failures (e.g. a rejected certificate)
/// wrapped in an I/O error.
fn from_io(error: std::io::Error) -> Error {
    #[cfg(feature = "rustls-platform-verifier")]
    if let Some(tls) = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        return Error::Tls(tls.to_string());
    }
    Error::Io {
        kind: error.kind(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::http::Response;

    #[test]
    fn io_errors_keep_their_kind() {
        let error = Error::from(WsError::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionRefused,
        )));
        assert!(matches!(
            error,
            Error::Io {
                kind: std::io::ErrorKind::ConnectionRefused,
                ..
            }
        ));
    }

    #[test]
    fn http_errors_carry_the_status() {
        let response = Response::builder().status(401).body(None).unwrap();
        let error = Error::from(WsError::Http(Box::new(response)));
        assert_eq!(error, Error::Http { status: 401 });
    }

    /// The messages are surfaced to users by consumers: they stay as they were before
    /// the error variants were split up.
    #[test]
    fn display_is_unchanged_for_classified_errors() {
        assert_eq!(
            Error::Http { status: 401 }.to_string(),
            "WebSocket error: HTTP error: 401 Unauthorized"
        );
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let expected = format!("WebSocket error: {}", WsError::Io(io));
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(Error::from(WsError::Io(io)).to_string(), expected);
    }

    #[test]
    fn other_errors_stay_generic() {
        let error = Error::from(WsError::ConnectionClosed);
        assert_eq!(
            error,
            Error::WebSocket(WsError::ConnectionClosed.to_string())
        );
    }
}
