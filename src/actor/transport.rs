//! Transport abstraction: connecting yields a boxed (sink, stream) pair.
//!
//! The production implementation wraps `tokio-tungstenite`. Tests inject an in-memory
//! implementation to drive the client deterministically without TCP.

use crate::Error;
use crate::core::event::Frame;
use futures::stream::StreamExt;
use futures::{Sink, Stream};
use log::debug;
use std::pin::Pin;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request as WsRequest;
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, COOKIE, LOCATION};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig as WsProtocolConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{Connector, connect_async_tls_with_config};

/// Boxed message sink of an established connection (owned by the writer task).
pub(crate) type BoxSink = Pin<Box<dyn Sink<Message, Error = WsError> + Send>>;
/// Boxed message stream of an established connection (owned by the reader task).
pub(crate) type BoxStream = Pin<Box<dyn Stream<Item = Result<Message, WsError>> + Send>>;
/// Boxed future returned by [`Transport::connect`].
pub(crate) type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<(BoxSink, BoxStream), Error>> + Send>>;

/// Options for a single connection attempt.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectOptions {
    /// Timeout for the whole attempt (including redirects).
    pub timeout: Duration,
    /// Maximum number of HTTP redirects to follow.
    pub max_redirects: u8,
    /// WebSocket protocol configuration (frame/message size limits etc.).
    pub ws_config: WsProtocolConfig,
    /// Accept invalid TLS certificates and hostnames (wss only). See
    /// [`crate::WebSocketConfig::danger_accept_invalid_certs`].
    pub danger_accept_invalid_certs: bool,
}

/// Connects a WebSocket and returns the split transport halves.
///
/// Object safe by returning a boxed future (no `async-trait` dependency).
pub(crate) trait Transport: Unpin + 'static {
    /// Establishes a connection for `request` according to `options`.
    fn connect(&self, request: WsRequest, options: ConnectOptions) -> ConnectFuture;
}

/// Production transport backed by `tokio-tungstenite`.
pub(crate) struct TungsteniteTransport;

impl Transport for TungsteniteTransport {
    fn connect(&self, request: WsRequest, options: ConnectOptions) -> ConnectFuture {
        Box::pin(async move {
            match tokio::time::timeout(options.timeout, connect_follow_redirects(request, options))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(Error::ConnectTimeout),
            }
        })
    }
}

/// Builds the TLS connector for a connection attempt.
///
/// Returns `None` (tungstenite's default connector: native-tls with system CA roots and
/// full verification) unless certificate verification is disabled.
fn build_connector(danger_accept_invalid_certs: bool) -> Result<Option<Connector>, Error> {
    if !danger_accept_invalid_certs {
        return Ok(None);
    }
    let tls = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()
        .map_err(|e| Error::WebSocket(format!("Cannot build TLS connector: {e}")))?;
    Ok(Some(Connector::NativeTls(tls)))
}

/// Attempts to establish a websocket connection, following HTTP redirects up to
/// `options.max_redirects`.
async fn connect_follow_redirects(
    mut request: WsRequest,
    options: ConnectOptions,
) -> Result<(BoxSink, BoxStream), Error> {
    let mut max_redirects = options.max_redirects;
    loop {
        let connector = build_connector(options.danger_accept_invalid_certs)?;
        match connect_async_tls_with_config(
            request.clone(),
            Some(options.ws_config),
            false,
            connector,
        )
        .await
        {
            Ok((ws_stream, _resp)) => {
                let (sink, stream) = ws_stream.split();
                return Ok((Box::pin(sink), Box::pin(stream)));
            }
            Err(WsError::Http(resp)) if is_redirect(resp.status()) => {
                if max_redirects == 0 {
                    return Err(Error::WebSocket(format!(
                        "Too many redirects (stopped at {})",
                        resp.status()
                    )));
                }
                let Some(location) = resp.headers().get(LOCATION) else {
                    return Err(Error::WebSocket(format!(
                        "Redirect ({}) without Location header",
                        resp.status()
                    )));
                };
                let location = location
                    .to_str()
                    .map_err(|e| Error::WebSocket(format!("Invalid Location header: {e}")))?;

                debug!(
                    "Following redirect to {location} (status {})",
                    resp.status()
                );
                request = build_redirect_request(&request, location)?;
                max_redirects -= 1;
            }
            Err(WsError::Http(resp)) => {
                return Err(Error::WebSocket(format!("HTTP error: {}", resp.status())));
            }
            Err(e) => return Err(Error::from(e)),
        }
    }
}

fn is_redirect(status: StatusCode) -> bool {
    matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308)
}

/// Builds the request for a redirect target.
///
/// Supports absolute (`ws://`, `wss://`, `http://`, `https://`), scheme-relative
/// (`//host/path`) and absolute-path (`/path`) Location values. On a cross-origin
/// redirect, hop-sensitive headers (`Authorization`, `Cookie`) are **not** forwarded.
fn build_redirect_request(current: &WsRequest, location: &str) -> Result<WsRequest, Error> {
    let current_uri = current.uri();
    let scheme = current_uri.scheme_str().unwrap_or("ws");
    let authority = current_uri
        .authority()
        .map(|a| a.as_str())
        .unwrap_or_default();

    // Normalize to an absolute ws(s) URL
    let target = if let Some(rest) = location.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = location.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if location.starts_with("ws://") || location.starts_with("wss://") {
        location.to_string()
    } else if location.starts_with("//") {
        format!("{scheme}:{location}")
    } else if location.starts_with('/') {
        format!("{scheme}://{authority}{location}")
    } else {
        return Err(Error::WebSocket(format!(
            "Unsupported redirect Location: {location}"
        )));
    };

    let mut new_request = target
        .as_str()
        .into_client_request()
        .map_err(|e| Error::InvalidRequest(e.to_string()))?;

    let cross_origin = current_uri.authority() != new_request.uri().authority();

    // Preserve original headers; don't overwrite ones tungstenite generated, and
    // don't leak credentials to a different origin.
    for (name, value) in current.headers() {
        if new_request.headers().contains_key(name) {
            continue;
        }
        if cross_origin && (name == AUTHORIZATION || name == COOKIE) {
            debug!("Dropping {name} header on cross-origin redirect");
            continue;
        }
        new_request
            .headers_mut()
            .insert(name.clone(), value.clone());
    }

    Ok(new_request)
}

// ============================================================================
// Frame conversions (tungstenite boundary)
// ============================================================================

/// Converts a received tungstenite message into a core [`Frame`].
///
/// Returns `None` for messages the core does not handle (raw frames).
pub(crate) fn frame_from_message(msg: Message) -> Option<Frame> {
    match msg {
        Message::Text(text) => Some(Frame::Text(text.as_str().to_owned())),
        Message::Binary(data) => Some(Frame::Binary(data)),
        Message::Ping(payload) => Some(Frame::Ping(payload)),
        Message::Pong(payload) => Some(Frame::Pong(payload)),
        Message::Close(_) => Some(Frame::Close),
        Message::Frame(_) => None,
    }
}

/// Converts a core [`Frame`] into a tungstenite message for sending.
pub(crate) fn message_from_frame(frame: Frame) -> Message {
    match frame {
        Frame::Text(text) => Message::Text(Utf8Bytes::from(text)),
        Frame::Binary(data) => Message::Binary(data),
        Frame::Ping(payload) => Message::Ping(payload),
        Frame::Pong(payload) => Message::Pong(payload),
        Frame::Close => Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: Utf8Bytes::from(String::new()),
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    fn request(url: &str) -> WsRequest {
        url.into_client_request().unwrap()
    }

    #[test]
    fn redirect_statuses() {
        for status in [301u16, 302, 303, 307, 308] {
            assert!(is_redirect(StatusCode::from_u16(status).unwrap()));
        }
        assert!(!is_redirect(StatusCode::OK));
        assert!(!is_redirect(StatusCode::NOT_FOUND));
    }

    #[test]
    fn absolute_redirect_normalizes_http_schemes() {
        let req = request("ws://example.com/socket");
        let new = build_redirect_request(&req, "https://other.example.com/ws").unwrap();
        assert_eq!(new.uri().scheme_str(), Some("wss"));
        assert_eq!(new.uri().host(), Some("other.example.com"));
        assert_eq!(new.uri().path(), "/ws");
    }

    #[test]
    fn path_relative_redirect_keeps_origin() {
        let req = request("ws://example.com:8080/old");
        let new = build_redirect_request(&req, "/new-path").unwrap();
        assert_eq!(new.uri().scheme_str(), Some("ws"));
        assert_eq!(new.uri().authority().unwrap().as_str(), "example.com:8080");
        assert_eq!(new.uri().path(), "/new-path");
    }

    #[test]
    fn scheme_relative_redirect_keeps_scheme() {
        let req = request("wss://example.com/old");
        let new = build_redirect_request(&req, "//other.example.com/ws").unwrap();
        assert_eq!(new.uri().scheme_str(), Some("wss"));
        assert_eq!(new.uri().host(), Some("other.example.com"));
    }

    #[test]
    fn unsupported_relative_redirect_is_rejected() {
        let req = request("ws://example.com/old");
        assert!(build_redirect_request(&req, "relative/path").is_err());
    }

    #[test]
    fn cross_origin_redirect_drops_credentials() {
        let mut req = request("ws://example.com/socket");
        req.headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        req.headers_mut()
            .insert("x-custom", HeaderValue::from_static("kept"));

        let new = build_redirect_request(&req, "ws://other.example.com/ws").unwrap();
        assert!(new.headers().get(AUTHORIZATION).is_none());
        assert_eq!(new.headers().get("x-custom").unwrap(), "kept");
    }

    #[test]
    fn same_origin_redirect_keeps_credentials() {
        let mut req = request("ws://example.com/socket");
        req.headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));

        let new = build_redirect_request(&req, "/other").unwrap();
        assert_eq!(new.headers().get(AUTHORIZATION).unwrap(), "Bearer secret");
    }

    #[test]
    fn frame_message_roundtrip() {
        assert_eq!(
            frame_from_message(message_from_frame(Frame::Text("hi".into()))),
            Some(Frame::Text("hi".into()))
        );
        let data = Bytes::from_static(b"\x00\x01");
        assert_eq!(
            frame_from_message(message_from_frame(Frame::Binary(data.clone()))),
            Some(Frame::Binary(data))
        );
        assert_eq!(
            frame_from_message(message_from_frame(Frame::Close)),
            Some(Frame::Close)
        );
    }
}
