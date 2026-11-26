//! Read side of the connection task, see [`crate::actor::connection`].
//!
//! The reader owns the `Stream` half of the connection and forwards each received
//! frame. Forwarding is awaited: a consumer that is not ready slows down reading.

use crate::Error;
use crate::actor::transport::{BoxStream, frame_from_message};
use crate::core::event::Frame;
use futures::StreamExt;

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
                forward(ReaderEvent::Ended(Some(Error::from(e)))).await;
                return;
            }
        }
    }
    forward(ReaderEvent::Ended(None)).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio_tungstenite::tungstenite::Error as WsError;
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
