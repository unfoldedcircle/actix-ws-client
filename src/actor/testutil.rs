//! Shared test doubles for the actor layer: in-memory sink/stream and a scripted
//! [`Transport`] so tests drive the client deterministically without TCP.

use super::transport::{BoxSink, BoxStream, ConnectFuture, ConnectOptions, Transport};
use crate::Error;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::handshake::client::Request as WsRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Sink that records sent messages; fails once `fail_after` messages were accepted.
pub(crate) struct MockSink {
    sent: Arc<Mutex<Vec<Message>>>,
    fail_after: usize,
}

impl futures::Sink<Message> for MockSink {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
        let mut sent = self.sent.lock().unwrap();
        if sent.len() >= self.fail_after {
            return Err(WsError::ConnectionClosed);
        }
        sent.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

/// Creates a recording sink; `fail_after = usize::MAX` never fails.
pub(crate) fn mock_sink(fail_after: usize) -> (BoxSink, Arc<Mutex<Vec<Message>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let sink = MockSink {
        sent: sent.clone(),
        fail_after,
    };
    (Box::pin(sink), sent)
}

/// Sink that never becomes ready: every write blocks forever (stalled socket).
struct BlockedSink;

impl futures::Sink<Message> for BlockedSink {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Pending
    }

    fn start_send(self: Pin<&mut Self>, _: Message) -> Result<(), WsError> {
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

/// Creates a sink that never accepts a message.
pub(crate) fn blocked_sink() -> BoxSink {
    Box::pin(BlockedSink)
}

/// In-memory stream: push items via the sender; drop the sender to end the stream.
pub(crate) type StreamScript = futures::channel::mpsc::UnboundedSender<Result<Message, WsError>>;

pub(crate) fn scripted_stream() -> (StreamScript, BoxStream) {
    let (tx, rx) = futures::channel::mpsc::unbounded();
    (tx, Box::pin(rx))
}

/// One connection attempt observed by the [`MockTransport`].
pub(crate) struct RecordedAttempt {
    /// Request URI of the attempt.
    pub uri: String,
    /// Connect options of the attempt.
    pub options: ConnectOptions,
}

/// Transport returning pre-scripted connection results and recording every attempt.
pub(crate) struct MockTransport {
    connections: Mutex<VecDeque<Result<(BoxSink, BoxStream), Error>>>,
    attempts: Arc<Mutex<Vec<RecordedAttempt>>>,
}

impl MockTransport {
    /// Returns the transport and the shared attempt recording.
    pub(crate) fn scripted(
        connections: Vec<Result<(BoxSink, BoxStream), Error>>,
    ) -> (Self, Arc<Mutex<Vec<RecordedAttempt>>>) {
        let attempts = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                connections: Mutex::new(connections.into()),
                attempts: attempts.clone(),
            },
            attempts,
        )
    }
}

impl Transport for MockTransport {
    fn connect(&self, request: WsRequest, options: ConnectOptions) -> ConnectFuture {
        self.attempts.lock().unwrap().push(RecordedAttempt {
            uri: request.uri().to_string(),
            options,
        });
        let next = self.connections.lock().unwrap().pop_front();
        Box::pin(async move {
            match next {
                Some(result) => result,
                None => Err(Error::WebSocket("no scripted connection left".into())),
            }
        })
    }
}
