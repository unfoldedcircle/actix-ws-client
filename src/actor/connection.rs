//! Per-connection task.
//!
//! One detached task owns both halves of an established connection for its entire
//! lifetime and talks to the actor over channels only — it holds no address of the
//! actor:
//!
//! - The actor pushes outbound messages into the write queue; the task reports
//!   received frames and failures as [`ConnectionEvent`]s. The event channel is
//!   bounded: a slow handler slows down reading instead of growing a queue.
//! - The actor ends the connection with a [`Teardown`] mode. The task then finishes
//!   on its own, in the background, while the actor moves on:
//!   - [`Teardown::Abort`] drops the connection at once.
//!   - [`Teardown::Close`] writes the queue (the close frame is its last entry) and
//!     keeps reading until the server answered and closed the connection.
//!   - [`Teardown::PeerClosed`] stops writing and keeps reading until the server
//!     closed the connection.
//!
//!   Waiting for the server is limited by the close timeout. Messages that are not
//!   written when the connection is dropped fail with [`Error::NotConnected`]: their
//!   completion channels are dropped with the queue.
//!
//! Because the task is detached, a connection can finish its close handshake after
//! the actor stopped.

use crate::Error;
use crate::actor::reader::{ReaderEvent, run_reader};
use crate::actor::transport::{BoxSink, BoxStream};
use crate::actor::writer::{WriteItem, run_writer};
use crate::core::effect::Teardown;
use log::debug;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};

/// Number of events the connection task may have waiting for the actor.
const EVENT_QUEUE: usize = 32;

/// What the connection task reports to the actor.
#[derive(Debug, PartialEq)]
pub(crate) enum ConnectionEvent {
    /// A frame was received, or the read side ended.
    Read(ReaderEvent),
    /// Writing to the socket failed.
    WriteFailed(Error),
}

/// The actor's side of a running connection task.
pub(crate) struct ConnectionHandle {
    writes: mpsc::UnboundedSender<WriteItem>,
    // Number of messages waiting in the write queue.
    queued: Arc<AtomicUsize>,
    shutdown: oneshot::Sender<Teardown>,
    closed: Closed,
}

impl ConnectionHandle {
    /// Spawns the task for an established connection.
    ///
    /// Returns the handle and the events of the connection. Dropping the event
    /// receiver tells the task that nobody is interested in received frames anymore.
    pub(crate) fn spawn(
        id: &str,
        sink: BoxSink,
        stream: BoxStream,
        close_timeout: Duration,
    ) -> (Self, mpsc::Receiver<ConnectionEvent>) {
        let (writes_tx, writes_rx) = mpsc::unbounded_channel();
        let (events_tx, events_rx) = mpsc::channel(EVENT_QUEUE);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (closed_tx, closed_rx) = watch::channel(());
        let queued = Arc::new(AtomicUsize::new(0));
        actix::spawn(run_connection(ConnectionTask {
            id: id.to_owned(),
            sink,
            stream,
            writes: writes_rx,
            queued: queued.clone(),
            events: events_tx,
            shutdown: shutdown_rx,
            close_timeout,
            _closed: closed_tx,
        }));
        let handle = Self {
            writes: writes_tx,
            queued,
            shutdown: shutdown_tx,
            closed: Closed(closed_rx),
        };
        (handle, events_rx)
    }

    /// Enqueues a message to write. Returns false if the task is gone.
    pub(crate) fn enqueue(&self, item: WriteItem) -> bool {
        let enqueued = self.writes.send(item).is_ok();
        if enqueued {
            self.queued.fetch_add(1, Ordering::Relaxed);
        }
        enqueued
    }

    /// Number of messages waiting to be written.
    pub(crate) fn queue_len(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Ends the connection as given by `mode`. No message can be enqueued anymore.
    ///
    /// Returns the signal to wait for the connection to be closed.
    pub(crate) fn shut_down(self, mode: Teardown) -> Closed {
        // The task is gone already if it cannot receive the mode.
        let _ = self.shutdown.send(mode);
        self.closed
    }
}

/// Resolves once a connection task ended and dropped its connection.
#[derive(Clone)]
pub(crate) struct Closed(watch::Receiver<()>);

impl Closed {
    /// Whether the connection is closed already.
    pub(crate) fn is_closed(&self) -> bool {
        // The task never sends a value: it only drops its sender when it ends.
        self.0.has_changed().is_err()
    }

    /// Waits until the connection is closed.
    pub(crate) async fn wait(mut self) {
        while self.0.changed().await.is_ok() {}
    }
}

/// Everything a connection task owns.
struct ConnectionTask {
    /// Client identifier for logging.
    id: String,
    sink: BoxSink,
    stream: BoxStream,
    writes: mpsc::UnboundedReceiver<WriteItem>,
    queued: Arc<AtomicUsize>,
    events: mpsc::Sender<ConnectionEvent>,
    shutdown: oneshot::Receiver<Teardown>,
    close_timeout: Duration,
    /// Dropped with the task: the connection is closed.
    _closed: watch::Sender<()>,
}

/// Runs a connection until the actor ends it, then finishes it as requested.
async fn run_connection(task: ConnectionTask) {
    let ConnectionTask {
        id,
        sink,
        stream,
        writes,
        queued,
        events,
        mut shutdown,
        close_timeout,
        _closed,
    } = task;

    let write_events = events.clone();
    let mut writer = Some(Box::pin(async move {
        if let Some(error) = run_writer(sink, writes, queued).await {
            let _ = write_events.send(ConnectionEvent::WriteFailed(error)).await;
        }
    }));
    let mut reader = Some(Box::pin(run_reader(stream, async move |event| {
        // Fails once the actor stopped listening: the frame is discarded then.
        let _ = events.send(ConnectionEvent::Read(event)).await;
    })));

    // Connected: write and read until the actor ends the connection. Both parts run
    // in this one task, so they are polled in random order: none of them can starve
    // the other, or delay the teardown for long.
    let mode = loop {
        tokio::select! {
            // A dropped handle means the actor is gone without a teardown.
            mode = &mut shutdown => break mode.unwrap_or(Teardown::Abort),
            () = run(&mut writer) => writer = None,
            () = run(&mut reader) => reader = None,
        }
    };

    match mode {
        Teardown::Abort => return,
        // Nothing can be written after the close frame of the server.
        Teardown::PeerClosed => writer = None,
        Teardown::Close => {
            // Write the close frame even with a zero timeout, if the socket takes it.
            if let Some(part) = writer.as_mut()
                && futures::poll!(part.as_mut()).is_ready()
            {
                writer = None;
            }
        }
    }
    if reader.is_none() {
        return; // the server closed the connection already
    }

    // Closing: keep reading until the server closed the connection. Received frames
    // are discarded, as the actor does not listen anymore.
    let deadline = tokio::time::sleep(close_timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut deadline => {
                debug!("[{id}] Server did not close the connection within {close_timeout:?}");
                return;
            }
            () = run(&mut writer) => writer = None,
            () = run(&mut reader) => return,
        }
    }
}

/// Runs a part of the connection, or waits forever if that part ended already.
async fn run<F: Future<Output = ()>>(part: &mut Option<Pin<Box<F>>>) {
    match part {
        Some(part) => part.as_mut().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::testutil::{
        blocked_sink, mock_sink, scripted_stream, track_sink, track_stream,
    };
    use crate::core::event::Frame;
    use std::sync::atomic::Ordering::SeqCst;
    use tokio_tungstenite::tungstenite::protocol::Message;

    const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

    fn text(msg: &str) -> Message {
        Message::Text(msg.into())
    }

    /// A message to write and the channel reporting its result.
    fn item(msg: Message) -> (WriteItem, oneshot::Receiver<Result<(), Error>>) {
        let (tx, rx) = oneshot::channel();
        (WriteItem { msg, tx: Some(tx) }, rx)
    }

    fn close_item() -> WriteItem {
        WriteItem {
            msg: Message::Close(None),
            tx: None,
        }
    }

    #[actix_rt::test]
    async fn writes_messages_and_reports_received_frames() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (handle, mut events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        let (write, written) = item(text("out"));
        assert!(handle.enqueue(write));
        assert_eq!(written.await.unwrap(), Ok(()));
        assert_eq!(*sent.lock().unwrap(), vec![text("out")]);
        assert_eq!(handle.queue_len(), 0);

        script.unbounded_send(Ok(text("in"))).unwrap();
        assert_eq!(
            events.recv().await,
            Some(ConnectionEvent::Read(ReaderEvent::Frame(Frame::Text(
                "in".into()
            ))))
        );

        // The server drops the connection: reported, but the task waits for the actor.
        drop(script);
        assert_eq!(
            events.recv().await,
            Some(ConnectionEvent::Read(ReaderEvent::Ended(None)))
        );
        assert!(!handle.closed.is_closed());
        handle.shut_down(Teardown::Abort).wait().await;
    }

    #[actix_rt::test]
    async fn write_failure_is_reported() {
        let (sink, _sent) = mock_sink(0);
        let (_script, stream) = scripted_stream();
        let (handle, mut events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        let (write, written) = item(text("out"));
        handle.enqueue(write);
        assert!(matches!(written.await.unwrap(), Err(Error::SendFailed(_))));
        assert!(matches!(
            events.recv().await,
            Some(ConnectionEvent::WriteFailed(Error::WebSocket(_)))
        ));
    }

    #[actix_rt::test]
    async fn abort_drops_a_stalled_connection_and_fails_its_messages() {
        let (sink, sink_dropped) = track_sink(blocked_sink());
        let (_script, stream) = scripted_stream();
        let (stream, stream_dropped) = track_stream(stream);
        let (handle, _events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        let (in_flight, in_flight_result) = item(text("in flight"));
        let (queued, queued_result) = item(text("queued"));
        handle.enqueue(in_flight);
        handle.enqueue(queued);
        tokio::task::yield_now().await;

        handle.shut_down(Teardown::Abort).wait().await;

        // Dropped completion channels: the actor reports `NotConnected`.
        assert!(in_flight_result.await.is_err());
        assert!(queued_result.await.is_err());
        assert!(sink_dropped.load(SeqCst));
        assert!(stream_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn dropped_handle_aborts_the_connection() {
        let (sink, sink_dropped) = track_sink(blocked_sink());
        let (_script, stream) = scripted_stream();
        let (handle, _events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);
        let closed = handle.closed.clone();

        drop(handle);

        closed.wait().await;
        assert!(sink_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn close_writes_the_queue_and_waits_for_the_server() {
        tokio::time::pause();
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (stream, stream_dropped) = track_stream(stream);
        let (handle, events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        let (write, written) = item(text("last words"));
        handle.enqueue(write);
        handle.enqueue(close_item());
        drop(events);
        let closed = handle.shut_down(Teardown::Close);

        assert_eq!(written.await.unwrap(), Ok(()));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            *sent.lock().unwrap(),
            vec![text("last words"), Message::Close(None)]
        );
        assert!(!closed.is_closed(), "the server did not close yet");

        // Frames of the server are discarded until it closes the connection.
        script.unbounded_send(Ok(text("ignored"))).unwrap();
        script.unbounded_send(Ok(Message::Close(None))).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!closed.is_closed());
        drop(script);

        closed.wait().await;
        assert!(stream_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn close_gives_up_after_the_close_timeout() {
        tokio::time::pause();
        let (sink, sink_dropped) = track_sink(blocked_sink());
        let (_script, stream) = scripted_stream();
        let (handle, _events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        let (write, written) = item(text("stuck"));
        handle.enqueue(write);
        handle.enqueue(close_item());
        let closed = handle.shut_down(Teardown::Close);

        tokio::time::sleep(CLOSE_TIMEOUT - Duration::from_millis(1)).await;
        assert!(!closed.is_closed());
        tokio::time::sleep(Duration::from_millis(2)).await;

        assert!(closed.is_closed());
        assert!(written.await.is_err());
        assert!(sink_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn close_with_a_zero_timeout_still_writes_the_close_frame() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (handle, _events) = ConnectionHandle::spawn("test", sink, stream, Duration::ZERO);

        handle.enqueue(close_item());
        handle.shut_down(Teardown::Close).wait().await;

        assert_eq!(*sent.lock().unwrap(), vec![Message::Close(None)]);
    }

    #[actix_rt::test]
    async fn peer_close_stops_writing_and_waits_for_the_server() {
        tokio::time::pause();
        let (sink, sink_dropped) = track_sink(blocked_sink());
        let (script, stream) = scripted_stream();
        let (handle, _events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        let (write, written) = item(text("too late"));
        handle.enqueue(write);
        tokio::task::yield_now().await;
        let closed = handle.shut_down(Teardown::PeerClosed);

        // The message cannot be written anymore: it fails without waiting.
        assert!(written.await.is_err());
        assert!(!closed.is_closed(), "the server did not close yet");

        drop(script);
        closed.wait().await;
        assert!(sink_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn teardown_after_the_server_closed_ends_at_once() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (handle, mut events) = ConnectionHandle::spawn("test", sink, stream, CLOSE_TIMEOUT);

        drop(script);
        assert_eq!(
            events.recv().await,
            Some(ConnectionEvent::Read(ReaderEvent::Ended(None)))
        );

        // Not paused: this would take the close timeout if the task waited.
        tokio::time::timeout(
            Duration::from_secs(1),
            handle.shut_down(Teardown::Close).wait(),
        )
        .await
        .expect("nothing to wait for");
    }
}
