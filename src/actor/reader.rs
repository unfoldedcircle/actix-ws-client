//! Read side of the connection task, see [`crate::actor::connection`].
//!
//! The reader owns the `Stream` half of the connection and forwards each received
//! frame. Forwarding is awaited: a consumer that is not ready slows down reading.

use crate::Error;
use crate::actor::transport::{BoxStream, frame_from_message};
use crate::core::event::Frame;
use futures::StreamExt;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::error::ProtocolError;

/// Output of the reader, forwarded to the actor.
#[derive(Debug, PartialEq)]
pub(crate) enum ReaderEvent {
    /// A frame was received.
    Frame(Frame),
    /// The stream ended: `None` for a clean end, `Some` for a read error.
    Ended(Option<Error>),
}

/// Runs the reader loop until the stream ends or a read error occurs.
///
/// The final event is always [`ReaderEvent::Ended`].
pub(crate) async fn run_reader<F>(mut stream: BoxStream, mut forward: F)
where
    F: AsyncFnMut(ReaderEvent),
{
    while let Some(item) = stream.next().await {
        match item {
            Ok(msg) => {
                if let Some(frame) = frame_from_message(msg) {
                    forward(ReaderEvent::Frame(frame)).await;
                }
            }
            Err(e) => {
                forward(ReaderEvent::Ended(read_error(e))).await;
                return;
            }
        }
    }
    forward(ReaderEvent::Ended(None)).await;
}

/// Classifies the error that ended reading; `None` is a clean end of the stream.
fn read_error(error: WsError) -> Option<Error> {
    match error {
        // The server closed the TCP connection without a close frame: its process
        // ended, or the device restarted. The transport calls that a protocol error,
        // but for the client it is simply the end of the connection — the most
        // common one.
        WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => None,
        // The same on a TLS connection: it ended without the TLS close alert.
        WsError::Io(error) if is_unexpected_eof(&error) => None,
        error => Some(Error::from(error)),
    }
}

/// Whether a TLS backend reports that the connection ended without the TLS close
/// alert (`close_notify`).
fn is_unexpected_eof(error: &std::io::Error) -> bool {
    // rustls
    error.kind() == std::io::ErrorKind::UnexpectedEof
        // OpenSSL 3 has no error kind for it: "... unexpected eof while reading"
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("unexpected eof")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio_tungstenite::tungstenite::protocol::Message;

    async fn collect(items: Vec<Result<Message, WsError>>) -> Vec<ReaderEvent> {
        let stream: BoxStream = Box::pin(futures::stream::iter(items));
        let mut events = Vec::new();
        run_reader(stream, async |ev| events.push(ev)).await;
        events
    }

    #[actix_rt::test]
    async fn forwards_frames_and_reports_clean_end() {
        let events = collect(vec![
            Ok(Message::Text("hello".into())),
            Ok(Message::Binary(Bytes::from_static(b"\x01"))),
            Ok(Message::Ping(Bytes::from_static(b"p"))),
            Ok(Message::Pong(Bytes::new())),
            Ok(Message::Close(None)),
        ])
        .await;

        assert_eq!(
            events,
            vec![
                ReaderEvent::Frame(Frame::Text("hello".into())),
                ReaderEvent::Frame(Frame::Binary(Bytes::from_static(b"\x01"))),
                ReaderEvent::Frame(Frame::Ping(Bytes::from_static(b"p"))),
                ReaderEvent::Frame(Frame::Pong(Bytes::new())),
                ReaderEvent::Frame(Frame::Close(None)),
                ReaderEvent::Ended(None),
            ]
        );
    }

    /// The server dropped the TCP connection without a close frame: its process
    /// ended, or the device restarted. That is the end of the stream, not an error
    /// of this client and not a protocol violation.
    #[actix_rt::test]
    async fn connection_dropped_by_the_server_is_a_clean_end() {
        let events = collect(vec![
            Ok(Message::Text("ok".into())),
            Err(WsError::Protocol(
                ProtocolError::ResetWithoutClosingHandshake,
            )),
        ])
        .await;

        assert_eq!(
            events,
            vec![
                ReaderEvent::Frame(Frame::Text("ok".into())),
                ReaderEvent::Ended(None),
            ]
        );
    }

    /// TLS backends report a connection that ended without the TLS close alert in
    /// their own ways.
    #[actix_rt::test]
    async fn tls_connection_dropped_by_the_server_is_a_clean_end() {
        use std::io::{Error as IoError, ErrorKind};

        let rustls = IoError::new(
            ErrorKind::UnexpectedEof,
            "peer closed connection without sending TLS close_notify",
        );
        let openssl =
            IoError::other("error:0A000126:SSL routines:ssl3_read_n:unexpected eof while reading");
        for error in [rustls, openssl] {
            let events = collect(vec![Err(WsError::Io(error))]).await;
            assert_eq!(events, vec![ReaderEvent::Ended(None)]);
        }
    }

    /// A reset connection is a network error, reported with its kind.
    #[actix_rt::test]
    async fn reset_connection_stays_an_error() {
        let reset = std::io::Error::from(std::io::ErrorKind::ConnectionReset);

        let events = collect(vec![Err(WsError::Io(reset))]).await;

        assert!(matches!(
            events[..],
            [ReaderEvent::Ended(Some(Error::Io {
                kind: std::io::ErrorKind::ConnectionReset,
                ..
            }))]
        ));
    }

    #[actix_rt::test]
    async fn other_protocol_errors_stay_errors() {
        let events = collect(vec![Err(WsError::Protocol(
            ProtocolError::MaskedFrameFromServer,
        ))])
        .await;

        assert!(matches!(
            events[..],
            [ReaderEvent::Ended(Some(Error::WebSocket(_)))]
        ));
    }

    #[actix_rt::test]
    async fn read_error_ends_the_stream_with_error() {
        let events = collect(vec![
            Ok(Message::Text("ok".into())),
            Err(WsError::ConnectionClosed),
            // never reached: reader stops at the first error
            Ok(Message::Text("unreachable".into())),
        ])
        .await;

        assert_eq!(events.len(), 2);
        assert_eq!(events[0], ReaderEvent::Frame(Frame::Text("ok".into())));
        assert!(matches!(events[1], ReaderEvent::Ended(Some(_))));
    }
}
