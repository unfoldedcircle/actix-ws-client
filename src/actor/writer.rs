//! Per-connection writer task.
//!
//! The task owns the `Sink` half of the connection for the connection's entire
//! lifetime — it is never lent back to the actor, so the take/restore race of the old
//! implementation (P0-1) cannot exist. The actor only holds the channel `Sender`:
//!
//! - Sending is a synchronous, non-awaiting channel push in the actor.
//! - Writes are serialized FIFO by the single-consumer channel.
//! - Teardown = drop the `Sender`: the task drains the remaining queue (so a final
//!   Close frame still reaches the wire) and ends, dropping the sink.

use crate::Error;
use crate::actor::transport::BoxSink;
use futures::SinkExt;
use log::debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::protocol::Message;

/// A message queued for the writer task, with an optional completion channel.
pub(crate) struct WriteItem {
    /// The message to write.
    pub msg: Message,
    /// Resolved with the write result; `None` for fire-and-forget frames (pings etc.).
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
}

/// Runs the writer loop until the channel is closed or a write fails.
///
/// `queued` is the number of messages waiting in the channel, maintained together with
/// the actor: the actor increments it for every message it enqueues.
///
/// Returns `Some(error)` if a write failed (the caller reports it to the actor as a
/// `WriteFailed` event) or `None` on graceful termination.
pub(crate) async fn run_writer(
    mut sink: BoxSink,
    mut rx: mpsc::UnboundedReceiver<WriteItem>,
    queued: Arc<AtomicUsize>,
) -> Option<Error> {
    while let Some(item) = rx.recv().await {
        queued.fetch_sub(1, Ordering::Relaxed);
        // `send` flushes each message, so a queued Close frame is on the wire before
        // the task ends.
        match sink.send(item.msg).await {
            Ok(()) => {
                if let Some(tx) = item.tx {
                    let _ = tx.send(Ok(()));
                }
            }
            Err(e) => {
                let error = Error::SendFailed(e.to_string());
                if let Some(tx) = item.tx {
                    let _ = tx.send(Err(error.clone()));
                }
                // Fail everything still queued, then report.
                rx.close();
                while let Ok(item) = rx.try_recv() {
                    if let Some(tx) = item.tx {
                        let _ = tx.send(Err(Error::NotConnected));
                    }
                }
                debug!("Writer task ending after write failure: {error}");
                return Some(error);
            }
        }
    }
    // All senders dropped: normal teardown. Dropping the sink (together with the
    // reader dropping the stream half) closes the underlying connection.
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::testutil::mock_sink;

    fn text(msg: &str) -> Message {
        Message::Text(msg.into())
    }

    /// Queue counter as the actor maintains it: `n` messages were enqueued.
    fn queued(n: usize) -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(n))
    }

    #[actix_rt::test]
    async fn fifo_order_is_preserved() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (tx, rx) = mpsc::unbounded_channel();

        let mut completions = Vec::new();
        for i in 0..100 {
            let (done_tx, done_rx) = oneshot::channel();
            tx.send(WriteItem {
                msg: text(&format!("message {i}")),
                tx: Some(done_tx),
            })
            .unwrap();
            completions.push(done_rx);
        }
        drop(tx);

        let queue = queued(100);
        assert_eq!(run_writer(sink, rx, queue.clone()).await, None);
        assert_eq!(queue.load(Ordering::Relaxed), 0);

        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 100);
        for (i, msg) in sent.iter().enumerate() {
            assert_eq!(*msg, text(&format!("message {i}")));
        }
        for done in completions {
            assert_eq!(done.await.unwrap(), Ok(()));
        }
    }

    #[actix_rt::test]
    async fn write_failure_fails_queued_messages_and_reports_error() {
        let (sink, sent) = mock_sink(1); // second write fails
        let (tx, rx) = mpsc::unbounded_channel();

        let mut completions = Vec::new();
        for i in 0..3 {
            let (done_tx, done_rx) = oneshot::channel();
            tx.send(WriteItem {
                msg: text(&format!("message {i}")),
                tx: Some(done_tx),
            })
            .unwrap();
            completions.push(done_rx);
        }
        drop(tx);

        let error = run_writer(sink, rx, queued(3)).await;
        assert!(matches!(error, Some(Error::SendFailed(_))));

        assert_eq!(sent.lock().unwrap().len(), 1);
        let mut results = Vec::new();
        for done in completions {
            results.push(done.await.unwrap());
        }
        assert_eq!(results[0], Ok(()));
        assert!(matches!(results[1], Err(Error::SendFailed(_))));
        assert_eq!(results[2], Err(Error::NotConnected));
    }

    #[actix_rt::test]
    async fn queue_is_drained_after_sender_is_dropped() {
        // Graceful disconnect: Close frame is enqueued, then the sender is dropped.
        let (sink, sent) = mock_sink(usize::MAX);
        let (tx, rx) = mpsc::unbounded_channel();

        tx.send(WriteItem {
            msg: Message::Close(None),
            tx: None,
        })
        .unwrap();
        drop(tx);

        assert_eq!(run_writer(sink, rx, queued(1)).await, None);
        assert_eq!(sent.lock().unwrap().len(), 1);
    }
}
