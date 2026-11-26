//! Transport abstraction: connecting yields a boxed (sink, stream) pair.
//!
//! The production implementation wraps `tokio-tungstenite`. Tests inject an in-memory
//! implementation to drive the client deterministically without TCP.

use crate::Error;
use crate::actor::tcp;
use crate::core::event::{CloseInfo, Frame};
use crate::tls::TlsContext;
use futures::stream::StreamExt;
use futures::{Sink, Stream};
use log::debug;
use std::pin::Pin;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::error::UrlError;
use tokio_tungstenite::tungstenite::handshake::client::{Request as WsRequest, generate_key};
use tokio_tungstenite::tungstenite::http::header::{
    AUTHORIZATION, COOKIE, LOCATION, SEC_WEBSOCKET_KEY,
};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig as WsProtocolConfig;
use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

/// Boxed message sink of an established connection (owned by the writer task).
pub(crate) type BoxSink = Pin<Box<dyn Sink<Message, Error = WsError> + Send>>;
/// Boxed message stream of an established connection (owned by the reader task).
pub(crate) type BoxStream = Pin<Box<dyn Stream<Item = Result<Message, WsError>> + Send>>;
/// Boxed future returned by [`Transport::connect`].
pub(crate) type ConnectFuture =
    Pin<Box<dyn Future<Output = Result<(BoxSink, BoxStream), Error>> + Send>>;

/// Options for a single connection attempt.
#[derive(Clone)]
pub(crate) struct ConnectOptions {
    /// Timeout for the whole attempt (including redirects).
    pub timeout: Duration,
    /// Maximum number of HTTP redirects to follow.
    pub max_redirects: u8,
    /// WebSocket protocol configuration (frame/message size limits etc.).
    pub ws_config: WsProtocolConfig,
    /// Whether Nagle's algorithm is disabled on the connection.
    pub tcp_nodelay: bool,
    /// TLS settings for `wss://` targets.
    #[cfg_attr(
        not(any(feature = "native-tls", feature = "rustls-platform-verifier")),
        allow(dead_code)
    )]
    pub tls: TlsContext,
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

/// Attempts to establish a websocket connection, following HTTP redirects up to
/// `options.max_redirects`.
async fn connect_follow_redirects(
    mut request: WsRequest,
    options: ConnectOptions,
) -> Result<(BoxSink, BoxStream), Error> {
    let mut max_redirects = options.max_redirects;
    loop {
        match connect(request.clone(), &options).await? {
            Attempt::Connected(sink, stream) => return Ok((sink, stream)),
            Attempt::Failed(WsError::Http(resp)) if is_redirect(resp.status()) => {
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
            Attempt::Failed(e) => return Err(Error::from(e)),
        }
    }
}

/// Result of a single connection attempt.
enum Attempt {
    Connected(BoxSink, BoxStream),
    /// The transport failed; a redirect response is handled by the caller.
    Failed(WsError),
}

impl Attempt {
    /// Splits an established connection into its boxed halves.
    fn connected<S>(connection: WebSocketStream<S>) -> Self
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (sink, stream) = connection.split();
        Attempt::Connected(Box::pin(sink), Box::pin(stream))
    }
}

/// A single connection attempt: TCP connection, TLS for a `wss://` target, WebSocket
/// handshake.
async fn connect(mut request: WsRequest, options: &ConnectOptions) -> Result<Attempt, Error> {
    // RFC 6455 §4.1: the key is a nonce, selected randomly for each connection. The
    // request is reused for every attempt, so its key has to be replaced.
    let key = HeaderValue::try_from(generate_key())
        .map_err(|e| Error::InvalidRequest(format!("handshake key: {e}")))?;
    request.headers_mut().insert(SEC_WEBSOCKET_KEY, key);

    let (host, port) = match target(&request) {
        Ok(target) => target,
        Err(e) => return Ok(Attempt::Failed(e)),
    };
    let tls = tls_setup(&request, options)?;
    let socket = match tcp::connect(&host, port, options.tcp_nodelay).await {
        Ok(socket) => socket,
        Err(e) => return Ok(Attempt::Failed(WsError::Io(e))),
    };
    Ok(match handshake(request, socket, options, tls).await {
        Ok(attempt) => attempt,
        Err(e) => Attempt::Failed(e),
    })
}

/// Host and port to connect to. The host of an IPv6 address is returned without its
/// brackets.
fn target(request: &WsRequest) -> Result<(String, u16), WsError> {
    let uri = request.uri();
    let host = uri.host().ok_or(WsError::Url(UrlError::NoHostName))?;
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    let port = uri
        .port_u16()
        .or_else(|| match uri.scheme_str() {
            Some("wss") => Some(443),
            Some("ws") => Some(80),
            _ => None,
        })
        .ok_or(WsError::Url(UrlError::UnsupportedUrlScheme))?;
    Ok((host.to_owned(), port))
}

/// The TLS connector for the attempt, set up before the connection is established:
/// a broken TLS configuration fails the attempt without a connection.
///
/// The connector is only set up for a `wss://` target, so plain `ws://` connections
/// never depend on the TLS configuration.
#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
type TlsSetup = Option<crate::tls::Connector>;

#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
fn tls_setup(request: &WsRequest, options: &ConnectOptions) -> Result<TlsSetup, Error> {
    if request.uri().scheme_str() == Some("wss") {
        options.tls.connector()
    } else {
        Ok(None)
    }
}

#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
async fn handshake(
    request: WsRequest,
    socket: TcpStream,
    options: &ConnectOptions,
    connector: TlsSetup,
) -> Result<Attempt, WsError> {
    let (connection, _response) = tokio_tungstenite::client_async_tls_with_config(
        request,
        socket,
        Some(options.ws_config),
        connector,
    )
    .await?;
    Ok(Attempt::connected(connection))
}

/// Without a TLS backend: plain `ws://` only.
#[cfg(not(any(feature = "native-tls", feature = "rustls-platform-verifier")))]
struct TlsSetup;

#[cfg(not(any(feature = "native-tls", feature = "rustls-platform-verifier")))]
fn tls_setup(request: &WsRequest, _options: &ConnectOptions) -> Result<TlsSetup, Error> {
    if request.uri().scheme_str() == Some("wss") {
        return Err(Error::from(WsError::Url(UrlError::TlsFeatureNotEnabled)));
    }
    Ok(TlsSetup)
}

#[cfg(not(any(feature = "native-tls", feature = "rustls-platform-verifier")))]
async fn handshake(
    request: WsRequest,
    socket: TcpStream,
    options: &ConnectOptions,
    _tls: TlsSetup,
) -> Result<Attempt, WsError> {
    let (connection, _response) =
        tokio_tungstenite::client_async_with_config(request, socket, Some(options.ws_config))
            .await?;
    Ok(Attempt::connected(connection))
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
        Message::Close(close) => Some(Frame::Close(close.map(|close| CloseInfo {
            code: close.code.into(),
            reason: close.reason.as_str().to_owned(),
        }))),
        Message::Frame(_) => None,
    }
}

/// Validates the status code and reason of a close frame to send (RFC 6455 §7.4, §5.5).
pub(crate) fn validate_close(code: u16, reason: &str) -> Result<(), Error> {
    if !CloseCode::from(code).is_allowed() {
        return Err(Error::InvalidRequest(format!(
            "close code {code} must not be sent in a close frame"
        )));
    }
    // A control frame carries at most 125 bytes: 2 bytes status code + reason.
    if reason.len() > 123 {
        return Err(Error::InvalidRequest(format!(
            "close reason too long: {} bytes (max 123)",
            reason.len()
        )));
    }
    Ok(())
}

/// Converts a core [`Frame`] into a tungstenite message for sending.
pub(crate) fn message_from_frame(frame: Frame) -> Message {
    match frame {
        Frame::Text(text) => Message::Text(Utf8Bytes::from(text)),
        Frame::Binary(data) => Message::Binary(data),
        Frame::Ping(payload) => Message::Ping(payload),
        Frame::Pong(payload) => Message::Pong(payload),
        Frame::Close(None) => Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: Utf8Bytes::from(String::new()),
        })),
        Frame::Close(Some(close)) => Message::Close(Some(CloseFrame {
            code: CloseCode::from(close.code),
            reason: Utf8Bytes::from(close.reason),
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
    fn target_uses_the_port_of_the_url_or_of_its_scheme() {
        let target = |url| target(&request(url)).unwrap();
        assert_eq!(target("ws://example.com/x"), ("example.com".into(), 80));
        assert_eq!(target("wss://example.com/x"), ("example.com".into(), 443));
        assert_eq!(
            target("ws://example.com:8080"),
            ("example.com".into(), 8080)
        );
        assert_eq!(target("wss://192.0.2.7:8443"), ("192.0.2.7".into(), 8443));
    }

    #[test]
    fn target_of_an_ipv6_address_has_no_brackets() {
        assert_eq!(
            target(&request("ws://[2001:db8::1]:8080/x")).unwrap(),
            ("2001:db8::1".into(), 8080)
        );
        assert_eq!(
            target(&request("wss://[::1]/")).unwrap(),
            ("::1".into(), 443)
        );
    }

    #[test]
    fn target_needs_a_websocket_scheme_for_the_default_port() {
        assert!(matches!(
            target(&request("http://example.com/x")),
            Err(WsError::Url(UrlError::UnsupportedUrlScheme))
        ));
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
            frame_from_message(message_from_frame(Frame::Close(None))),
            Some(Frame::Close(Some(CloseInfo {
                code: 1000,
                reason: String::new()
            })))
        );
    }
}
