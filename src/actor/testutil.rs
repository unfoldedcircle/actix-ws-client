//! Shared test doubles for the actor layer: in-memory sink/stream and a scripted
//! [`Transport`] so tests drive the client deterministically without TCP.

use super::transport::{BoxSink, BoxStream, ConnectFuture, ConnectOptions, Transport};
use crate::Error;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::handshake::client::Request as WsRequest;
use tokio_tungstenite::tungstenite::http::HeaderMap;
use tokio_tungstenite::tungstenite::protocol::Message;

/// Sink that records sent messages; fails once `fail_after` messages were accepted.
pub(crate) struct MockSink {
    sent: Arc<Mutex<Vec<Message>>>,
    fail_after: usize,
    /// Creates the error of a failing write.
    error: fn() -> WsError,
}

impl futures::Sink<Message> for MockSink {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
        let mut sent = self.sent.lock().unwrap();
        if sent.len() >= self.fail_after {
            return Err((self.error)());
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
    failing_sink(fail_after, || WsError::ConnectionClosed)
}

/// Creates a recording sink that fails with `error` once `fail_after` messages were
/// accepted.
pub(crate) fn failing_sink(
    fail_after: usize,
    error: fn() -> WsError,
) -> (BoxSink, Arc<Mutex<Vec<Message>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let sink = MockSink {
        sent: sent.clone(),
        fail_after,
        error,
    };
    (Box::pin(sink), sent)
}

/// The error of a write to a socket the server closed.
pub(crate) fn broken_pipe() -> WsError {
    WsError::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
}

/// Sink of a slow connection: every message takes `delay` to write.
struct SlowSink {
    sent: Arc<Mutex<Vec<Message>>>,
    delay: std::time::Duration,
    writing: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl futures::Sink<Message> for SlowSink {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
        self.sent.lock().unwrap().push(item);
        self.writing = Some(Box::pin(tokio::time::sleep(self.delay)));
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        if let Some(writing) = self.writing.as_mut() {
            futures::ready!(writing.as_mut().poll(cx));
            self.writing = None;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

/// Creates a recording sink that takes `delay` to write a message. The message is
/// recorded when its write starts.
pub(crate) fn slow_sink(delay: std::time::Duration) -> (BoxSink, Arc<Mutex<Vec<Message>>>) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let sink = SlowSink {
        sent: sent.clone(),
        delay,
        writing: None,
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

/// Set when the tracked transport half has been dropped, see [`track_sink`] and
/// [`track_stream`].
pub(crate) type Dropped = Arc<AtomicBool>;

/// Sets its flag when dropped.
struct DropFlag(Dropped);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Sink wrapper reporting when the client dropped its half of the connection.
struct TrackedSink {
    inner: BoxSink,
    _flag: DropFlag,
}

impl futures::Sink<Message> for TrackedSink {
    type Error = WsError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.inner.as_mut().poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
        self.inner.as_mut().start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.inner.as_mut().poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.inner.as_mut().poll_close(cx)
    }
}

/// Wraps a sink; the returned flag is set once the client dropped it.
pub(crate) fn track_sink(sink: BoxSink) -> (BoxSink, Dropped) {
    let dropped = Dropped::default();
    let sink = TrackedSink {
        inner: sink,
        _flag: DropFlag(dropped.clone()),
    };
    (Box::pin(sink), dropped)
}

/// Stream wrapper reporting when the client dropped its half of the connection.
struct TrackedStream {
    inner: BoxStream,
    _flag: DropFlag,
}

impl futures::Stream for TrackedStream {
    type Item = Result<Message, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Wraps a stream; the returned flag is set once the client dropped it.
pub(crate) fn track_stream(stream: BoxStream) -> (BoxStream, Dropped) {
    let dropped = Dropped::default();
    let stream = TrackedStream {
        inner: stream,
        _flag: DropFlag(dropped.clone()),
    };
    (Box::pin(stream), dropped)
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
    /// Request headers of the attempt.
    pub headers: HeaderMap,
    /// Connect options of the attempt.
    pub options: ConnectOptions,
}

/// Transport returning pre-scripted connection results and recording every attempt.
pub(crate) struct MockTransport {
    connections: Mutex<VecDeque<Result<(BoxSink, BoxStream), Error>>>,
    attempts: Arc<Mutex<Vec<RecordedAttempt>>>,
    /// Number of attempts that never complete, before the scripted results are used.
    hanging: AtomicUsize,
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
                hanging: AtomicUsize::new(0),
            },
            attempts,
        )
    }

    /// Lets the first `count` attempts hang: they never complete.
    pub(crate) fn hang_attempts(self, count: usize) -> Self {
        self.hanging.store(count, Ordering::SeqCst);
        self
    }
}

impl Transport for MockTransport {
    fn connect(&self, request: WsRequest, options: ConnectOptions) -> ConnectFuture {
        self.attempts.lock().unwrap().push(RecordedAttempt {
            uri: request.uri().to_string(),
            headers: request.headers().clone(),
            options,
        });
        if self.hanging.load(Ordering::SeqCst) > 0 {
            self.hanging.fetch_sub(1, Ordering::SeqCst);
            return Box::pin(std::future::pending());
        }
        let next = self.connections.lock().unwrap().pop_front();
        Box::pin(async move {
            match next {
                Some(result) => result,
                None => Err(Error::WebSocket("no scripted connection left".into())),
            }
        })
    }
}

/// Everything the crate logged since the first call, at every level.
///
/// The logger is installed for the whole test process on first use, so the records
/// of tests that run in parallel are mixed: search them for texts that are unique to
/// a test.
pub(crate) fn captured_logs() -> Vec<String> {
    struct Capture(Mutex<Vec<String>>);

    impl log::Log for Capture {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }

        fn log(&self, record: &log::Record) {
            let line = format!("{} {}", record.level(), record.args());
            self.0.lock().unwrap().push(line);
        }

        fn flush(&self) {}
    }

    static CAPTURE: Capture = Capture(Mutex::new(Vec::new()));
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        log::set_logger(&CAPTURE).expect("no other logger in unit tests");
        log::set_max_level(log::LevelFilter::Trace);
    });
    CAPTURE.0.lock().unwrap().clone()
}
