//! Actix adapter around the pure [`crate::core::Machine`].
//!
//! The adapter performs the I/O the core asks for — connect attempts, connection
//! tasks, timers, user callbacks — and translates real-world happenings back into core
//! [`Event`]s. It contains **no branching policy**: every decision lives in the core,
//! every `Handler` here is "wrap as event → `dispatch` → execute effects".

pub(crate) mod connection;
pub(crate) mod reader;
pub(crate) mod tcp;
#[cfg(test)]
pub(crate) mod testutil;
pub(crate) mod transport;
pub(crate) mod writer;

use crate::config::WebSocketConfig;
use crate::core::effect::{Callback, Effect, Teardown};
use crate::core::event::{CloseInfo, Event, Frame};
use crate::core::{CoreConfig, Machine};
use crate::error::Error;
use crate::handler::{HandlerAction, TraceDirection, WebSocketMessageHandler};
use crate::message::{
    Close, Connect, Disconnect, IsConnected, Reconnect, SendBinary, SendText, Stop,
};
use crate::redact::{self, ActionLog, EffectLog};
use crate::tls::TlsContext;
use actix::fut::{ActorFutureExt, ActorStreamExt, wrap_future, wrap_stream};
use actix::prelude::*;
use connection::{Closed, ConnectionEvent, ConnectionHandle, Timeouts};
use log::{debug, info, trace, warn};
use reader::ReaderEvent;
use std::pin::Pin;
use std::task::{self, Poll};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request as WsRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig as WsProtocolConfig;
use transport::{
    BoxSink, BoxStream, ConnectOptions, Transport, TungsteniteTransport, copy_headers,
    message_from_frame, validate_close, validate_request,
};
use writer::WriteItem;

/// Per-attempt URL provider, see [`GenericWebSocketClient::with_url_provider`].
type UrlProvider = Box<dyn FnMut() -> Option<String> + Send>;

/// Stops the actor once its last address is dropped.
///
/// actix keeps an actor running for as long as its context has work to do — here the
/// events of a connection, a connection attempt, the keep-alive and reconnect timers.
/// Without this watch a client would stay connected, or keep reconnecting, with nobody
/// left to talk to it or to stop it.
///
/// The watch needs no waker of its own: actix wakes the actor when its last address is
/// dropped, and polls every future of the context whenever the actor is polled.
struct AddressWatch;

impl<H: WebSocketMessageHandler> ActorFuture<GenericWebSocketClient<H>> for AddressWatch {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        actor: &mut GenericWebSocketClient<H>,
        ctx: &mut Context<GenericWebSocketClient<H>>,
        _task: &mut task::Context<'_>,
    ) -> Poll<Self::Output> {
        if ctx.connected() {
            return Poll::Pending;
        }
        debug!("[{}] Last address dropped: stopping", actor.id);
        // `stopped` closes the connection.
        ctx.stop();
        Poll::Ready(())
    }
}

/// The established connection: its task and the actor future receiving its events.
struct Connection {
    task: ConnectionHandle,
    events: SpawnHandle,
}

// ============================================================================
// GENERIC WEBSOCKET CLIENT ACTOR
// ============================================================================

/// A generic WebSocket client actor.
///
/// This actor manages a WebSocket connection, handles reconnection with jittered
/// exponential backoff and keep-alive pings, and forwards received messages to a
/// provided [`WebSocketMessageHandler`].
///
/// All connection policy lives in a pure, synchronous state machine; this actor only
/// executes its effects.
///
/// # Lifecycle
///
/// The address returned by `start()` is the handle of the client. The client runs for
/// as long as an address (or a recipient) of it exists — keep it also for a client
/// that only receives. When the last address is dropped, the client closes its
/// connection gracefully and stops, as it does on a [`Stop`] message. Weak addresses
/// do not keep a client running.
pub struct GenericWebSocketClient<H>
where
    H: WebSocketMessageHandler,
{
    id: String,
    request: WsRequest,
    config: WebSocketConfig,
    handler: H,
    machine: Machine,
    transport: Box<dyn Transport>,

    // Per-connection resources, created and destroyed by effects. The active
    // connection generation is tracked by the machine.
    connection: Option<Connection>,
    connect_handle: Option<SpawnHandle>,
    keepalive_handle: Option<SpawnHandle>,
    reconnect_handle: Option<SpawnHandle>,
    // Connections that were torn down and may still be closing in the background.
    closing: Vec<Closed>,

    // Transport halves stashed between connect completion and the
    // StartConnectionTask effect consuming them.
    pending_transport: Option<(BoxSink, BoxStream)>,
    // Reply channels of the `Connect` requests waiting for the running attempt.
    pending_connects: Vec<oneshot::Sender<Result<(), Error>>>,
    // Optional per-attempt URL refresh hook.
    url_provider: Option<UrlProvider>,
    // TLS settings shared by all connection attempts.
    tls: TlsContext,
    // Timestamp of the last traced outbound message (msg_tracing_out).
    last_msg_send_time: Option<Instant>,
}

impl<H> GenericWebSocketClient<H>
where
    H: WebSocketMessageHandler,
{
    /// Creates a new `GenericWebSocketClient` actor.
    ///
    /// # Arguments
    ///
    /// * `id` - A unique identifier for this client (used in logging).
    /// * `request` - The WebSocket URL or request to connect to.
    /// * `config` - Connection, reconnection and keep-alive configuration.
    /// * `handler` - An implementation of [`WebSocketMessageHandler`] to handle received messages.
    ///
    /// # Errors
    ///
    /// Fails with [`Error::InvalidRequest`] if the request cannot be connected: it is
    /// not a valid request, its URL scheme is not `ws` or `wss`, it has no host, or it
    /// is a `wss` URL and the crate is built without a TLS backend.
    pub fn new<R, S>(id: S, request: R, config: WebSocketConfig, handler: H) -> Result<Self, Error>
    where
        R: IntoClientRequest + Unpin,
        S: Into<String>,
    {
        Self::with_transport(id, request, config, handler, Box::new(TungsteniteTransport))
    }

    /// Creates a client with an injected [`Transport`] (used by tests).
    pub(crate) fn with_transport<R, S>(
        id: S,
        request: R,
        config: WebSocketConfig,
        handler: H,
        transport: Box<dyn Transport>,
    ) -> Result<Self, Error>
    where
        R: IntoClientRequest + Unpin,
        S: Into<String>,
    {
        let request = request
            .into_client_request()
            .map_err(|e| Error::InvalidRequest(e.to_string()))?;
        validate_request(&request)?;
        Ok(Self {
            request,
            id: id.into(),
            machine: Machine::new(CoreConfig::from(&config)),
            tls: TlsContext::new(&config),
            config,
            handler,
            transport,
            connection: None,
            connect_handle: None,
            keepalive_handle: None,
            reconnect_handle: None,
            closing: Vec::new(),
            pending_transport: None,
            pending_connects: Vec::new(),
            url_provider: None,
            last_msg_send_time: None,
        })
    }

    /// Sets a URL provider that is polled once before every connection attempt.
    ///
    /// Returning `Some(url)` replaces the request URL for this and subsequent
    /// attempts (headers of the original request are preserved); `None` keeps the
    /// current request. An invalid URL fails the attempt with
    /// [`Error::InvalidRequest`] and follows the normal reconnect policy.
    ///
    /// This allows consumers with their own address resolution (DNS/IP caching) to
    /// refresh the target between attempts.
    pub fn with_url_provider<F>(mut self, provider: F) -> Self
    where
        F: FnMut() -> Option<String> + Send + 'static,
    {
        self.url_provider = Some(Box::new(provider));
        self
    }

    // ========================================================================
    // EVENT DISPATCH & EFFECT EXECUTION
    // ========================================================================

    /// Feeds an event into the core and executes the returned effects.
    fn dispatch(&mut self, event: Event, ctx: &mut Context<Self>) {
        let effects = self.machine.on_event(event, Instant::now());
        // Handler actions are collected during effect execution and run after the
        // whole batch, so an action cannot re-enter the machine while the current
        // effect list is still being executed.
        let mut pending_actions: Vec<HandlerAction> = Vec::new();
        for effect in effects {
            trace!("[{}] Executing effect: {}", self.id, EffectLog(&effect));
            match effect {
                Effect::StartConnect {
                    generation,
                    timeout,
                } => self.start_connect(generation, timeout, ctx),
                Effect::StartConnectionTask { generation } => {
                    self.start_connection(generation, ctx)
                }
                Effect::SendFrame { frame } => self.send_core_frame(frame),
                Effect::TearDownConnection { generation, mode } => {
                    self.teardown(generation, mode, ctx)
                }
                Effect::ArmReconnectTimer { generation, delay } => {
                    self.arm_reconnect_timer(generation, delay, ctx)
                }
                Effect::StartKeepAlive {
                    generation,
                    interval,
                } => self.start_keepalive(generation, interval, ctx),
                Effect::StopKeepAlive => self.stop_keepalive(ctx),
                Effect::Emit(callback) => pending_actions.extend(self.emit(callback)),
                Effect::CompleteConnect { result } => {
                    for tx in self.pending_connects.drain(..) {
                        let _ = tx.send(result.clone());
                    }
                }
            }
        }
        trace!(
            "[{}] State: {:?} (#{})",
            self.id,
            self.machine.state(),
            self.machine.generation()
        );
        // A stopping client only closes its connection: it must not send or connect.
        if matches!(ctx.state(), ActorState::Stopping | ActorState::Stopped) {
            return;
        }
        for action in pending_actions {
            self.execute_action(action, ctx);
        }
    }

    /// Executes a [`HandlerAction`] returned from a handler callback.
    fn execute_action(&mut self, action: HandlerAction, ctx: &mut Context<Self>) {
        trace!(
            "[{}] Executing handler action: {}",
            self.id,
            ActionLog(&action)
        );
        match action {
            HandlerAction::SendText(text) => self.send_handler_frame(Message::Text(text.into())),
            HandlerAction::SendBinary(data) => self.send_handler_frame(Message::Binary(data)),
            HandlerAction::Disconnect => {
                self.dispatch(Event::DisconnectRequested, ctx);
            }
            HandlerAction::Close {
                code,
                reason,
                reconnect,
            } => {
                let close = match validate_close(code, &reason) {
                    Ok(()) => CloseInfo { code, reason },
                    Err(error) => {
                        warn!("[{}] {error}: closing with a normal closure", self.id);
                        CloseInfo {
                            code: 1000,
                            reason: String::new(),
                        }
                    }
                };
                self.dispatch(Event::CloseRequested { close, reconnect }, ctx);
            }
            HandlerAction::Reconnect => {
                self.dispatch(Event::ReconnectRequested, ctx);
            }
        }
    }

    /// Sends a handler-originated frame. Frames returned while the connection is not
    /// up (e.g. after a `Disconnect` action in the same batch) are dropped.
    fn send_handler_frame(&mut self, msg: Message) {
        if !self.machine.is_connected() {
            warn!(
                "[{}] Dropping handler frame: not connected (state: {:?})",
                self.id,
                self.machine.state()
            );
            return;
        }
        if self.send_queue_full() {
            warn!("[{}] Dropping handler frame: send queue full", self.id);
            return;
        }
        self.enqueue_write(WriteItem { msg, tx: None });
    }

    /// Enqueues a message to the connection task, tracing outbound text messages when
    /// enabled. Returns false if there is no connection.
    fn enqueue_write(&mut self, item: WriteItem) -> bool {
        let Some(connection) = &self.connection else {
            trace!("[{}] No connection, dropping outbound frame", self.id);
            return false;
        };
        if self.config.msg_tracing_out
            && let Message::Text(text) = &item.msg
            && let Some(text) = self.handler.redact_trace(TraceDirection::Outgoing, text)
        {
            let elapsed = self
                .last_msg_send_time
                .take()
                .map(|i| i.elapsed())
                .unwrap_or_default();
            debug!("[{}] {:4} <- {text}", self.id, elapsed.as_millis());
            self.last_msg_send_time = Some(Instant::now());
        }
        connection.task.enqueue(item)
    }

    /// Whether the write queue reached the configured `max_send_queue`.
    fn send_queue_full(&self) -> bool {
        let queued = self
            .connection
            .as_ref()
            .map_or(0, |connection| connection.task.queue_len());
        self.config.max_send_queue.is_some_and(|max| queued >= max)
    }

    fn start_connect(&mut self, generation: u64, timeout: Duration, ctx: &mut Context<Self>) {
        // A new attempt supersedes any armed reconnect timer.
        if let Some(handle) = self.reconnect_handle.take() {
            ctx.cancel_future(handle);
        }

        // Per-attempt URL refresh hook (e.g. DNS/IP cache of the consumer).
        if let Err(error) = self.refresh_request() {
            // Invalid refreshed URL: fail this attempt through the normal path, as
            // an immediately-failing connect task.
            let handle = ctx.spawn(wrap_future::<_, Self>(async {}).map(move |_, actor, ctx| {
                if actor.owns_handles_of(generation) {
                    actor.connect_handle = None;
                }
                actor.dispatch(Event::ConnectFailed { generation, error }, ctx);
            }));
            self.connect_handle = Some(handle);
            return;
        }

        debug!("[{}] Connecting (#{generation})", self.id);

        let mut ws_config = WsProtocolConfig::default();
        if let Some(size) = self.config.max_message_size {
            ws_config = ws_config.max_message_size(Some(size));
        }
        if let Some(size) = self.config.max_frame_size {
            ws_config = ws_config.max_frame_size(Some(size));
        }
        let options = ConnectOptions {
            timeout,
            max_redirects: self.config.max_redirects,
            ws_config,
            tcp_nodelay: self.config.tcp_nodelay,
            tls: self.tls.clone(),
        };
        let future = self.transport.connect(self.request.clone(), options);
        let handle = ctx.spawn(
            wrap_future::<_, Self>(future).map(move |result, actor, ctx| {
                if actor.owns_handles_of(generation) {
                    actor.connect_handle = None;
                }
                let event = match result {
                    Ok(transport) => {
                        actor.pending_transport = Some(transport);
                        Event::ConnectSucceeded { generation }
                    }
                    Err(error) => Event::ConnectFailed { generation, error },
                };
                actor.dispatch(event, ctx);
                // Drop unclaimed halves if the core ignored the result
                // (stale generation or cancelled attempt).
                actor.pending_transport = None;
            }),
        );
        self.connect_handle = Some(handle);
    }

    /// Polls the URL provider and swaps the request URL if a new one is supplied.
    /// Headers of the current request are preserved.
    fn refresh_request(&mut self) -> Result<(), Error> {
        let Some(provider) = &mut self.url_provider else {
            return Ok(());
        };
        let Some(url) = provider() else {
            return Ok(());
        };
        let mut new_request = url.as_str().into_client_request().map_err(|e| {
            Error::InvalidRequest(format!("provider URL {:?}: {e}", redact::url(&url)))
        })?;
        validate_request(&new_request)?;
        copy_headers(&self.request, &mut new_request, |_| true);
        if new_request.uri() != self.request.uri() {
            debug!("[{}] Using refreshed URL: {}", self.id, redact::url(&url));
        }
        self.request = new_request;
        Ok(())
    }

    /// Starts the connection task and the actor future receiving its events.
    fn start_connection(&mut self, generation: u64, ctx: &mut Context<Self>) {
        let Some((sink, stream)) = self.pending_transport.take() else {
            warn!("[{}] StartConnectionTask without a transport", self.id);
            return;
        };
        let timeouts = Timeouts {
            close: self.config.close_timeout,
            write: self.config.write_timeout,
        };
        let (task, mut events) = ConnectionHandle::spawn(&self.id, sink, stream, timeouts);
        // Actor-owned future: cancelled via its SpawnHandle on teardown, which drops
        // the event receiver. Events are handled without a detour over the mailbox.
        let events = futures::stream::poll_fn(move |cx| events.poll_recv(cx));
        let events = ctx.spawn(
            wrap_stream::<_, Self>(events)
                .map(move |event, actor, ctx| actor.on_connection_event(generation, event, ctx))
                .finish(),
        );
        self.connection = Some(Connection { task, events });
    }

    /// Feeds an event of the connection task into the core.
    fn on_connection_event(
        &mut self,
        generation: u64,
        event: ConnectionEvent,
        ctx: &mut Context<Self>,
    ) {
        let event = match event {
            ConnectionEvent::Read(ReaderEvent::Frame(frame)) => {
                Event::FrameReceived { generation, frame }
            }
            ConnectionEvent::Read(ReaderEvent::Ended(error)) => {
                Event::StreamEnded { generation, error }
            }
            ConnectionEvent::WriteFailed(error) => Event::WriteFailed { generation, error },
        };
        self.dispatch(event, ctx);
    }

    /// Hands a core-originated frame (ping, close) to the connection task.
    fn send_core_frame(&mut self, frame: Frame) {
        match frame {
            // Written before the messages that wait in the queue.
            Frame::Ping(payload) => {
                if let Some(connection) = &self.connection {
                    connection.task.ping(payload);
                }
            }
            // A close frame is the last entry of the queue.
            frame => {
                self.enqueue_write(WriteItem {
                    msg: message_from_frame(frame),
                    tx: None,
                });
            }
        }
    }

    fn teardown(&mut self, generation: u64, mode: Teardown, ctx: &mut Context<Self>) {
        debug!(
            "[{}] Tearing down connection (#{generation}, {mode:?})",
            self.id
        );
        if let Some(connection) = self.connection.take() {
            // Stop listening: events of this connection are of no interest anymore.
            ctx.cancel_future(connection.events);
            // The task finishes the connection on its own, see `Teardown`.
            self.closing.retain(|closed| !closed.is_closed());
            self.closing.push(connection.task.shut_down(mode));
        }
        if let Some(handle) = self.connect_handle.take() {
            ctx.cancel_future(handle);
        }
        if let Some(handle) = self.keepalive_handle.take() {
            ctx.cancel_future(handle);
        }
        if let Some(handle) = self.reconnect_handle.take() {
            ctx.cancel_future(handle);
        }
        self.pending_transport = None;
    }

    /// Resolves once every connection that was torn down so far is closed.
    fn closed(&mut self) -> impl Future<Output = ()> + use<H> {
        self.closing.retain(|closed| !closed.is_closed());
        let closing = self.closing.clone();
        async move {
            for closed in closing {
                closed.wait().await;
            }
        }
    }

    fn arm_reconnect_timer(&mut self, generation: u64, delay: Duration, ctx: &mut Context<Self>) {
        if let Some(handle) = self.reconnect_handle.take() {
            ctx.cancel_future(handle);
        }
        info!("[{}] Reconnecting in {delay:?}", self.id);
        let handle = ctx.run_later(delay, move |actor, ctx| {
            if actor.owns_handles_of(generation) {
                actor.reconnect_handle = None;
            }
            actor.dispatch(Event::ReconnectTimerFired { generation }, ctx);
        });
        self.reconnect_handle = Some(handle);
    }

    /// Whether the stored connect and reconnect handles belong to `generation`.
    ///
    /// A cancelled future can still complete once, if it was ready when it was
    /// cancelled. The core ignores its event; this check keeps it from clearing the
    /// handle of a newer attempt or timer, which could not be cancelled anymore.
    fn owns_handles_of(&self, generation: u64) -> bool {
        self.machine.generation() == generation
    }

    fn start_keepalive(&mut self, generation: u64, interval: Duration, ctx: &mut Context<Self>) {
        self.stop_keepalive(ctx);
        debug!(
            "[{}] Keep-alive started (interval {interval:?}, #{generation})",
            self.id
        );
        let handle = ctx.run_interval(interval, move |actor, ctx| {
            actor.dispatch(Event::KeepAliveTick { generation }, ctx);
        });
        self.keepalive_handle = Some(handle);
    }

    fn stop_keepalive(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.keepalive_handle.take() {
            ctx.cancel_future(handle);
        }
    }

    /// Invokes a user callback and returns the actions it requested.
    fn emit(&mut self, callback: Callback) -> Vec<HandlerAction> {
        match callback {
            Callback::OnText(text) => {
                if self.config.msg_tracing_in
                    && let Some(text) = self.handler.redact_trace(TraceDirection::Incoming, &text)
                {
                    debug!("[{}] -> {text}", self.id);
                }
                self.handler.on_text(text)
            }
            Callback::OnBinary(data) => self.handler.on_binary(data),
            Callback::OnConnected => {
                info!("[{}] Connected", self.id);
                self.handler.on_connected()
            }
            Callback::OnClosed(reason) => {
                debug!("[{}] Connection closed: {reason:?}", self.id);
                self.handler.on_closed(&reason)
            }
            Callback::OnDisconnected => {
                info!("[{}] Disconnected", self.id);
                self.handler.on_disconnected();
                Vec::new()
            }
            Callback::OnError(error) => {
                info!("[{}] Error: {error}", self.id);
                self.handler.on_error(&error)
            }
            Callback::OnReconnectExhausted => {
                warn!("[{}] Reconnection attempts exhausted, giving up", self.id);
                self.handler.on_reconnect_exhausted()
            }
        }
    }

    /// Common implementation for `SendText` / `SendBinary`: enqueue to the connection
    /// task, never awaiting in the handler. Returns `NotConnected` immediately when
    /// there is no active connection, and `SendQueueFull` when the queue limit is
    /// reached.
    fn send_message(&mut self, msg: Message) -> ResponseFuture<Result<(), Error>> {
        if !self.machine.is_connected() {
            return Box::pin(async { Err(Error::NotConnected) });
        }
        if self.send_queue_full() {
            return Box::pin(async { Err(Error::SendQueueFull) });
        }
        let (tx, rx) = oneshot::channel();
        if !self.enqueue_write(WriteItem { msg, tx: Some(tx) }) {
            return Box::pin(async { Err(Error::NotConnected) });
        }
        Box::pin(async move {
            match rx.await {
                Ok(result) => result,
                // The connection ended before the message was written.
                Err(_) => Err(Error::NotConnected),
            }
        })
    }
}

// ============================================================================
// ACTOR & MESSAGE HANDLERS
// ============================================================================

impl<H: WebSocketMessageHandler> Actor for GenericWebSocketClient<H> {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        debug!("[{}] WebSocket actor started", self.id);
        ctx.spawn(AddressWatch);
        if self.config.connect_on_start {
            debug!("[{}] Auto-connecting on start", self.id);
            self.dispatch(Event::ConnectRequested, ctx);
        }
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        debug!("[{}] WebSocket actor stopped", self.id);
        // Graceful close: the detached connection task completes the close handshake
        // after the actor is gone.
        self.dispatch(Event::DisconnectRequested, ctx);
    }
}

impl<H: WebSocketMessageHandler> Handler<IsConnected> for GenericWebSocketClient<H> {
    type Result = bool;

    fn handle(&mut self, _msg: IsConnected, _ctx: &mut Context<Self>) -> Self::Result {
        self.machine.is_connected()
    }
}

impl<H: WebSocketMessageHandler> Handler<Connect> for GenericWebSocketClient<H> {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, _msg: Connect, ctx: &mut Context<Self>) -> Self::Result {
        // Resolved by the CompleteConnect effect: at once if the client is connected,
        // else with the result of the connection attempt — the one this request
        // starts, or the one that is running already.
        let (tx, rx) = oneshot::channel();
        self.pending_connects.push(tx);
        self.dispatch(Event::ConnectRequested, ctx);
        Box::pin(async move {
            match rx.await {
                Ok(result) => result,
                Err(_) => Err(Error::StateChanged),
            }
        })
    }
}

impl<H: WebSocketMessageHandler> Handler<Disconnect> for GenericWebSocketClient<H> {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: Disconnect, ctx: &mut Context<Self>) -> Self::Result {
        debug!("[{}] Explicit disconnect requested", self.id);
        self.dispatch(Event::DisconnectRequested, ctx);
        Box::pin(self.closed())
    }
}

impl<H: WebSocketMessageHandler> Handler<Stop> for GenericWebSocketClient<H> {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, _msg: Stop, ctx: &mut Context<Self>) -> Self::Result {
        debug!("[{}] Stop requested", self.id);
        self.dispatch(Event::DisconnectRequested, ctx);
        ctx.stop();
        Box::pin(self.closed())
    }
}

impl<H: WebSocketMessageHandler> Handler<Close> for GenericWebSocketClient<H> {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: Close, ctx: &mut Context<Self>) -> Self::Result {
        if let Err(error) = validate_close(msg.code, &msg.reason) {
            return Box::pin(async { Err(error) });
        }
        debug!(
            "[{}] Explicit disconnect requested (close code {})",
            self.id, msg.code
        );
        self.dispatch(
            Event::CloseRequested {
                close: CloseInfo {
                    code: msg.code,
                    reason: msg.reason,
                },
                reconnect: msg.reconnect,
            },
            ctx,
        );
        let closed = self.closed();
        Box::pin(async move {
            closed.await;
            Ok(())
        })
    }
}

impl<H: WebSocketMessageHandler> Handler<Reconnect> for GenericWebSocketClient<H> {
    type Result = ();

    fn handle(&mut self, _msg: Reconnect, ctx: &mut Context<Self>) {
        debug!("[{}] Explicit reconnect requested", self.id);
        self.dispatch(Event::ReconnectRequested, ctx);
    }
}

impl<H: WebSocketMessageHandler> Handler<SendText> for GenericWebSocketClient<H> {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: SendText, _ctx: &mut Context<Self>) -> Self::Result {
        self.send_message(Message::Text(msg.text.into()))
    }
}

impl<H: WebSocketMessageHandler> Handler<SendBinary> for GenericWebSocketClient<H> {
    type Result = ResponseFuture<Result<(), Error>>;

    fn handle(&mut self, msg: SendBinary, _ctx: &mut Context<Self>) -> Self::Result {
        self.send_message(Message::Binary(msg.data))
    }
}

// ============================================================================
// INTEGRATION TESTS (mock transport, no TCP)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::testutil::{
        MockTransport, blocked_sink, broken_pipe, captured_logs, failing_sink, mock_sink,
        scripted_stream, slow_sink, track_sink, track_stream,
    };
    use super::*;
    use crate::{DisconnectReason, WebSocketConfig};
    use bytes::Bytes;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::{Arc, Mutex};
    use tokio_tungstenite::tungstenite::protocol::frame::CloseFrame;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    #[derive(Clone, Default)]
    struct TestHandler {
        log: Arc<Mutex<Vec<String>>>,
        /// Actions returned (and drained) by the next `on_text` call.
        text_actions: Arc<Mutex<Vec<HandlerAction>>>,
        /// Actions returned (and drained) by the next `on_connected` call.
        connected_actions: Arc<Mutex<Vec<HandlerAction>>>,
        /// Actions returned (and drained) by the next `on_closed` call.
        closed_actions: Arc<Mutex<Vec<HandlerAction>>>,
        /// Actions returned (and drained) by the next `on_error` call.
        error_actions: Arc<Mutex<Vec<HandlerAction>>>,
        /// Actions returned (and drained) by the next `on_reconnect_exhausted` call.
        exhausted_actions: Arc<Mutex<Vec<HandlerAction>>>,
    }

    impl TestHandler {
        fn log_contains(&self, entry: &str) -> bool {
            self.log.lock().unwrap().iter().any(|e| e == entry)
        }

        fn count(&self, entry: &str) -> usize {
            self.log
                .lock()
                .unwrap()
                .iter()
                .filter(|e| *e == entry)
                .count()
        }

        fn set_text_actions(&self, actions: Vec<HandlerAction>) {
            *self.text_actions.lock().unwrap() = actions;
        }

        fn set_connected_actions(&self, actions: Vec<HandlerAction>) {
            *self.connected_actions.lock().unwrap() = actions;
        }
    }

    impl WebSocketMessageHandler for TestHandler {
        fn on_text(&mut self, text: String) -> Vec<HandlerAction> {
            self.log.lock().unwrap().push(format!("text:{text}"));
            std::mem::take(&mut *self.text_actions.lock().unwrap())
        }
        fn on_binary(&mut self, data: Bytes) -> Vec<HandlerAction> {
            self.log
                .lock()
                .unwrap()
                .push(format!("binary:{}", data.len()));
            Vec::new()
        }
        fn on_connected(&mut self) -> Vec<HandlerAction> {
            self.log.lock().unwrap().push("connected".into());
            std::mem::take(&mut *self.connected_actions.lock().unwrap())
        }
        fn on_closed(&mut self, reason: &DisconnectReason) -> Vec<HandlerAction> {
            self.log.lock().unwrap().push(format!("closed:{reason:?}"));
            std::mem::take(&mut *self.closed_actions.lock().unwrap())
        }
        fn on_disconnected(&mut self) {
            self.log.lock().unwrap().push("disconnected".into());
        }
        fn on_error(&mut self, error: &Error) -> Vec<HandlerAction> {
            self.log.lock().unwrap().push(format!("error:{error}"));
            std::mem::take(&mut *self.error_actions.lock().unwrap())
        }
        fn on_reconnect_exhausted(&mut self) -> Vec<HandlerAction> {
            self.log.lock().unwrap().push("exhausted".into());
            std::mem::take(&mut *self.exhausted_actions.lock().unwrap())
        }
        fn redact_trace<'a>(
            &self,
            direction: TraceDirection,
            text: &'a str,
        ) -> Option<std::borrow::Cow<'a, str>> {
            self.log
                .lock()
                .unwrap()
                .push(format!("trace:{direction:?}:{text}"));
            None
        }
    }

    fn config() -> WebSocketConfig {
        WebSocketConfig::builder()
            .initial_backoff(Duration::from_millis(50))
            .build()
            .unwrap()
    }

    fn client(
        transport: MockTransport,
        handler: TestHandler,
    ) -> Addr<GenericWebSocketClient<TestHandler>> {
        GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config(),
            handler,
            Box::new(transport),
        )
        .unwrap()
        .start()
    }

    /// Polls `cond` until it holds or the timeout expires.
    async fn wait_until<F: Fn() -> bool>(cond: F, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn sent_contains(sent: &Arc<Mutex<Vec<Message>>>, pred: impl Fn(&Message) -> bool) -> bool {
        sent.lock().unwrap().iter().any(pred)
    }

    #[actix_rt::test]
    async fn connect_send_and_receive() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();
        assert!(addr.send(IsConnected).await.unwrap());
        assert!(handler.log_contains("connected"));

        // outbound
        addr.send(SendText::new("hello")).await.unwrap().unwrap();
        assert!(sent_contains(
            &sent,
            |m| matches!(m, Message::Text(t) if t.as_str() == "hello")
        ));

        // inbound
        script
            .unbounded_send(Ok(Message::Text("world".into())))
            .unwrap();
        wait_until(|| handler.log_contains("text:world"), "inbound text").await;
    }

    #[actix_rt::test]
    async fn traced_messages_pass_the_redaction_hook() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let config = WebSocketConfig::builder()
            .msg_tracing_in(true)
            .msg_tracing_out(true)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        addr.send(SendText::new("secret")).await.unwrap().unwrap();
        script
            .unbounded_send(Ok(Message::Text("reply".into())))
            .unwrap();
        wait_until(|| handler.log_contains("text:reply"), "inbound text").await;

        assert!(handler.log_contains("trace:Outgoing:secret"));
        assert!(handler.log_contains("trace:Incoming:reply"));
        // The hook only affects the log: the message itself is sent unchanged.
        assert!(sent_contains(
            &sent,
            |m| matches!(m, Message::Text(t) if t.as_str() == "secret")
        ));
    }

    #[actix_rt::test]
    async fn redaction_hook_is_not_called_without_message_tracing() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        addr.send(SendText::new("hello")).await.unwrap().unwrap();
        script
            .unbounded_send(Ok(Message::Text("world".into())))
            .unwrap();
        wait_until(|| handler.log_contains("text:world"), "inbound text").await;

        assert!(
            !handler
                .log
                .lock()
                .unwrap()
                .iter()
                .any(|e| e.starts_with("trace:"))
        );
    }

    #[actix_rt::test]
    async fn full_send_queue_rejects_further_messages() {
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((blocked_sink(), stream))]);
        let config = WebSocketConfig::builder()
            .max_send_queue(2usize)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        // The first message is taken by the writer, which then blocks on the socket.
        addr.do_send(SendText::new("in flight"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Two more messages fill the queue.
        addr.do_send(SendText::new("queued 1"));
        addr.do_send(SendText::new("queued 2"));

        let result = addr.send(SendText::new("too many")).await.unwrap();

        assert_eq!(result, Err(Error::SendQueueFull));
        // A full queue is not a connection failure.
        assert!(addr.send(IsConnected).await.unwrap());
    }

    #[actix_rt::test]
    async fn connect_failure_is_reported_to_caller() {
        let (transport, _) = MockTransport::scripted(vec![Err(Error::WebSocket("refused".into()))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        let result = addr.send(Connect).await.unwrap();
        assert_eq!(result, Err(Error::WebSocket("refused".into())));
        assert!(!addr.send(IsConnected).await.unwrap());
        // connect failure must not produce a disconnected callback (edge contract)
        assert_eq!(handler.count("disconnected"), 0);
    }

    #[actix_rt::test]
    async fn connect_while_connected_succeeds() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = client(transport, TestHandler::default());

        addr.send(Connect).await.unwrap().unwrap();
        let second = addr.send(Connect).await.unwrap();

        assert_eq!(second, Ok(()));
        assert_eq!(attempts.lock().unwrap().len(), 1);
    }

    #[actix_rt::test]
    async fn connect_joins_the_attempt_of_connect_on_start() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let config = WebSocketConfig::builder()
            .connect_on_start(true)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .start();

        // The way to wait for the connection of a client that connects on start.
        let result = addr.send(Connect).await.unwrap();

        assert_eq!(result, Ok(()));
        assert!(addr.send(IsConnected).await.unwrap());
        assert_eq!(attempts.lock().unwrap().len(), 1);
    }

    #[actix_rt::test]
    async fn concurrent_connects_get_the_result_of_the_one_attempt() {
        let (transport, attempts) =
            MockTransport::scripted(vec![Err(Error::WebSocket("refused".into()))]);
        let addr = client(transport, TestHandler::default());

        let (first, second) = futures::join!(addr.send(Connect), addr.send(Connect));

        assert_eq!(first.unwrap(), Err(Error::WebSocket("refused".into())));
        assert_eq!(second.unwrap(), Err(Error::WebSocket("refused".into())));
        assert_eq!(attempts.lock().unwrap().len(), 1);
        addr.do_send(Disconnect);
    }

    #[actix_rt::test]
    async fn disconnect_cancels_every_pending_connect() {
        tokio::time::pause();
        let (transport, _) = MockTransport::scripted(vec![]);
        let addr = client(transport.hang_attempts(1), TestHandler::default());

        let first = addr.send(Connect);
        let second = addr.send(Connect);
        tokio::time::sleep(Duration::from_millis(10)).await;
        addr.do_send(Disconnect);

        let first = tokio::time::timeout(Duration::from_secs(1), first).await;
        let second = tokio::time::timeout(Duration::from_secs(1), second).await;
        assert_eq!(first.unwrap().unwrap(), Err(Error::StateChanged));
        assert_eq!(second.unwrap().unwrap(), Err(Error::StateChanged));
    }

    #[actix_rt::test]
    async fn reconnect_while_connecting_starts_a_fresh_attempt() {
        tokio::time::pause();
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();

        let refreshed_url = Arc::new(Mutex::new(None::<String>));
        let provider_url = refreshed_url.clone();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://original:1234",
            config(),
            handler.clone(),
            // The attempt to the original address never completes.
            Box::new(transport.hang_attempts(1)),
        )
        .unwrap()
        .with_url_provider(move || provider_url.lock().unwrap().clone())
        .start();
        let connect = addr.send(Connect);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // The address changed: do not wait for the running attempt.
        *refreshed_url.lock().unwrap() = Some("ws://refreshed:5678".into());
        addr.do_send(Reconnect);

        // The pending connect request is answered by the new attempt.
        let result = tokio::time::timeout(Duration::from_secs(1), connect)
            .await
            .expect("Reconnect must replace the running attempt");
        assert_eq!(result.unwrap(), Ok(()));
        assert_eq!(handler.count("connected"), 1);
        let attempts = attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert!(attempts[0].uri.contains("original:1234"));
        assert!(attempts[1].uri.contains("refreshed:5678"));
    }

    #[actix_rt::test]
    async fn send_when_disconnected_fails_immediately() {
        let (transport, _) = MockTransport::scripted(vec![]);
        let addr = client(transport, TestHandler::default());

        let result = addr.send(SendText::new("nope")).await.unwrap();
        assert_eq!(result, Err(Error::NotConnected));
    }

    #[actix_rt::test]
    async fn reconnects_after_stream_ends_with_balanced_callbacks() {
        let (sink1, _sent1) = mock_sink(usize::MAX);
        let (script1, stream1) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (_script2, stream2) = scripted_stream();
        let (transport, attempts) =
            MockTransport::scripted(vec![Ok((sink1, stream1)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        // server drops the connection
        drop(script1);
        wait_until(|| handler.count("connected") == 2, "automatic reconnect").await;
        assert_eq!(attempts.lock().unwrap().len(), 2);
        assert_eq!(handler.count("disconnected"), 1);
        assert!(addr.send(IsConnected).await.unwrap());
    }

    #[actix_rt::test]
    async fn disconnect_during_reconnect_stops_attempts() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        // connection drops -> reconnect scheduled (50ms backoff)
        drop(script);
        wait_until(|| handler.count("disconnected") == 1, "disconnect callback").await;

        // user disconnects before the timer fires
        addr.send(Disconnect).await.unwrap();

        // give any (wrongly still armed) timer ample time to fire
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            attempts.lock().unwrap().len(),
            1,
            "no further connect attempts"
        );
        assert!(!addr.send(IsConnected).await.unwrap());
        assert_eq!(handler.count("disconnected"), 1);
    }

    #[actix_rt::test]
    async fn graceful_disconnect_sends_close_frame_once() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();
        // Not awaited: the mock server never answers the close frame.
        addr.do_send(Disconnect);

        wait_until(
            || sent_contains(&sent, |m| matches!(m, Message::Close(_))),
            "close frame written",
        )
        .await;
        assert!(!addr.send(IsConnected).await.unwrap());
        assert_eq!(handler.count("disconnected"), 1);
    }

    fn sent_close(sent: &Arc<Mutex<Vec<Message>>>, code: u16, reason: &str) -> bool {
        sent_contains(sent, |m| {
            matches!(m, Message::Close(Some(close))
                if u16::from(close.code) == code && close.reason.as_str() == reason)
        })
    }

    #[actix_rt::test]
    async fn close_message_sends_code_and_reason_without_reconnect() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        // Not awaited: the mock server never answers the close frame.
        addr.do_send(Close::new(4000, "going to standby"));

        wait_until(
            || sent_close(&sent, 4000, "going to standby"),
            "close frame written",
        )
        .await;
        assert!(handler.log_contains("closed:ClosedLocally"));
        assert_eq!(handler.count("disconnected"), 1);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(attempts.lock().unwrap().len(), 1);
    }

    #[actix_rt::test]
    async fn close_message_rejects_invalid_code_and_long_reason() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = client(transport, TestHandler::default());
        addr.send(Connect).await.unwrap().unwrap();

        // 1006 (abnormal closure) must never be sent in a close frame
        let result = addr.send(Close::new(1006, "")).await.unwrap();
        assert!(matches!(result, Err(Error::InvalidRequest(_))));
        let result = addr.send(Close::new(1000, "x".repeat(124))).await.unwrap();
        assert!(matches!(result, Err(Error::InvalidRequest(_))));

        // the connection is left untouched
        assert!(addr.send(IsConnected).await.unwrap());
        assert!(!sent_contains(&sent, |m| matches!(m, Message::Close(_))));
    }

    #[actix_rt::test]
    async fn close_action_sends_code_and_reason() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        handler.set_text_actions(vec![HandlerAction::Close {
            code: 1003,
            reason: "binary not supported".into(),
            reconnect: false,
        }]);
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        script
            .unbounded_send(Ok(Message::Text("trigger".into())))
            .unwrap();

        wait_until(
            || sent_close(&sent, 1003, "binary not supported"),
            "close frame written",
        )
        .await;
        assert!(!addr.send(IsConnected).await.unwrap());
    }

    #[actix_rt::test]
    async fn close_with_reconnect_establishes_a_new_connection_after_the_backoff() {
        let (sink1, sent1) = mock_sink(usize::MAX);
        let (_script1, stream1) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (_script2, stream2) = scripted_stream();
        let (transport, attempts) =
            MockTransport::scripted(vec![Ok((sink1, stream1)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        // Not awaited: the mock server never answers the close frame.
        addr.do_send(Close::new(1002, "unexpected message").with_reconnect());

        wait_until(
            || sent_close(&sent1, 1002, "unexpected message"),
            "close frame written",
        )
        .await;
        // not immediately: the reconnect waits for the backoff delay
        assert_eq!(attempts.lock().unwrap().len(), 1);
        wait_until(|| handler.count("connected") == 2, "reconnected").await;
        assert_eq!(handler.count("disconnected"), 1);
    }

    #[actix_rt::test]
    async fn invalid_close_action_falls_back_to_a_normal_closure() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        handler.set_text_actions(vec![HandlerAction::Close {
            code: 1005,
            reason: String::new(),
            reconnect: false,
        }]);
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        script
            .unbounded_send(Ok(Message::Text("trigger".into())))
            .unwrap();

        wait_until(|| sent_close(&sent, 1000, ""), "close frame written").await;
        assert!(!addr.send(IsConnected).await.unwrap());
    }

    #[actix_rt::test]
    async fn server_close_code_is_reported_before_disconnected() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        script
            .unbounded_send(Ok(Message::Close(Some(CloseFrame {
                code: CloseCode::from(4001),
                reason: "session expired".into(),
            }))))
            .unwrap();

        wait_until(|| handler.count("disconnected") == 1, "disconnected").await;
        let log = handler.log.lock().unwrap();
        let closed = log
            .iter()
            .position(|e| {
                e == &format!(
                    "closed:{:?}",
                    DisconnectReason::ClosedByServer {
                        code: Some(4001),
                        reason: "session expired".into()
                    }
                )
            })
            .expect("close reason reported");
        assert_eq!(log[closed + 1], "disconnected");
    }

    #[actix_rt::test]
    async fn write_failure_triggers_reconnect() {
        let (sink1, _sent1) = mock_sink(0); // first write fails immediately
        let (_script1, stream1) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (_script2, stream2) = scripted_stream();
        let (transport, attempts) =
            MockTransport::scripted(vec![Ok((sink1, stream1)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        let result = addr.send(SendText::new("boom")).await.unwrap();
        assert!(matches!(result, Err(Error::SendFailed(_))));

        wait_until(
            || handler.count("connected") == 2,
            "reconnect after write failure",
        )
        .await;
        assert_eq!(attempts.lock().unwrap().len(), 2);
    }

    // ========================================================================
    // HandlerAction execution
    // ========================================================================

    #[actix_rt::test]
    async fn text_reply_actions_are_sent_in_order() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        handler.set_text_actions(vec![
            HandlerAction::SendText("reply-1".into()),
            HandlerAction::SendBinary(Bytes::from_static(b"\x02")),
            HandlerAction::SendText("reply-2".into()),
        ]);
        script
            .unbounded_send(Ok(Message::Text("trigger".into())))
            .unwrap();

        wait_until(
            || sent.lock().unwrap().len() == 3,
            "all reply actions written",
        )
        .await;
        let sent = sent.lock().unwrap();
        assert!(matches!(&sent[0], Message::Text(t) if t.as_str() == "reply-1"));
        assert!(matches!(&sent[1], Message::Binary(b) if b.as_ref() == b"\x02"));
        assert!(matches!(&sent[2], Message::Text(t) if t.as_str() == "reply-2"));
    }

    #[actix_rt::test]
    async fn on_connected_actions_are_sent_on_the_new_connection() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        handler.set_connected_actions(vec![HandlerAction::SendText("hello-server".into())]);
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        wait_until(
            || {
                sent_contains(
                    &sent,
                    |m| matches!(m, Message::Text(t) if t.as_str() == "hello-server"),
                )
            },
            "on_connected action written",
        )
        .await;
    }

    #[actix_rt::test]
    async fn disconnect_action_closes_without_reconnect() {
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        // fatal message: disconnect, and a send action after it must be dropped
        handler.set_text_actions(vec![
            HandlerAction::Disconnect,
            HandlerAction::SendText("after-disconnect".into()),
        ]);
        script
            .unbounded_send(Ok(Message::Text("fatal".into())))
            .unwrap();

        wait_until(|| handler.count("disconnected") == 1, "disconnected").await;
        assert!(!addr.send(IsConnected).await.unwrap());

        // no reconnect attempt, close frame sent, dropped frame not sent
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.lock().unwrap().len(), 1);
        assert!(sent_contains(&sent, |m| matches!(m, Message::Close(_))));
        assert!(!sent_contains(
            &sent,
            |m| matches!(m, Message::Text(t) if t.as_str() == "after-disconnect")
        ));
    }

    #[actix_rt::test]
    async fn reconnect_action_establishes_a_fresh_connection() {
        let (sink1, sent1) = mock_sink(usize::MAX);
        let (script1, stream1) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (_script2, stream2) = scripted_stream();
        let (transport, attempts) =
            MockTransport::scripted(vec![Ok((sink1, stream1)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();

        handler.set_text_actions(vec![HandlerAction::Reconnect]);
        script1
            .unbounded_send(Ok(Message::Text("bounce".into())))
            .unwrap();

        wait_until(|| handler.count("connected") == 2, "fresh connection").await;
        assert_eq!(attempts.lock().unwrap().len(), 2);
        assert_eq!(handler.count("disconnected"), 1);
        // old connection was closed gracefully
        assert!(sent_contains(&sent1, |m| matches!(m, Message::Close(_))));
        assert!(addr.send(IsConnected).await.unwrap());
    }

    // ========================================================================
    // Reconnect message
    // ========================================================================

    #[actix_rt::test]
    async fn reconnect_message_bounces_the_connection() {
        let (sink1, sent1) = mock_sink(usize::MAX);
        let (_script1, stream1) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (_script2, stream2) = scripted_stream();
        let (transport, attempts) =
            MockTransport::scripted(vec![Ok((sink1, stream1)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());

        addr.send(Connect).await.unwrap().unwrap();
        addr.send(Reconnect).await.unwrap();

        wait_until(|| handler.count("connected") == 2, "fresh connection").await;
        assert_eq!(attempts.lock().unwrap().len(), 2);
        assert_eq!(handler.count("disconnected"), 1);
        assert!(sent_contains(&sent1, |m| matches!(m, Message::Close(_))));
        assert!(addr.send(IsConnected).await.unwrap());
    }

    // ========================================================================
    // Maximum reconnection attempts
    // ========================================================================

    #[actix_rt::test]
    async fn reconnect_exhausted_after_attempt_cap() {
        let (transport, attempts) = MockTransport::scripted(vec![]); // every attempt fails
        let handler = TestHandler::default();
        let config = WebSocketConfig::builder()
            .initial_backoff(Duration::from_millis(20))
            .max_reconnect_attempts(1u32)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .start();

        let result = addr.send(Connect).await.unwrap();
        assert!(result.is_err());

        wait_until(|| handler.count("exhausted") == 1, "exhausted callback").await;

        // no further attempts: initial + 1 reconnect
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(attempts.lock().unwrap().len(), 2);
        assert!(!addr.send(IsConnected).await.unwrap());
    }

    // ========================================================================
    // Per-attempt URL refresh
    // ========================================================================

    #[actix_rt::test]
    async fn url_provider_is_used_per_attempt() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![
            Err(Error::WebSocket("refused".into())),
            Ok((sink, stream)),
        ]);
        let handler = TestHandler::default();

        let refreshed_url = Arc::new(Mutex::new(None::<String>));
        let provider_url = refreshed_url.clone();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://original:1234",
            config(),
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .with_url_provider(move || provider_url.lock().unwrap().clone())
        .start();

        let result = addr.send(Connect).await.unwrap();
        assert!(result.is_err());

        // refresh the URL before the retry fires
        *refreshed_url.lock().unwrap() = Some("ws://refreshed:5678".into());
        wait_until(|| handler.count("connected") == 1, "reconnected").await;

        let attempts = attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert!(attempts[0].uri.contains("original:1234"));
        assert!(attempts[1].uri.contains("refreshed:5678"));
    }

    #[actix_rt::test]
    async fn url_refresh_keeps_all_values_of_a_header() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let mut request = "ws://original:1234".into_client_request().unwrap();
        for protocol in ["v2", "v1"] {
            request
                .headers_mut()
                .append("sec-websocket-protocol", protocol.parse().unwrap());
        }

        let addr = GenericWebSocketClient::with_transport(
            "test",
            request,
            config(),
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .with_url_provider(|| Some("ws://refreshed:5678".into()))
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        let attempts = attempts.lock().unwrap();
        assert!(attempts[0].uri.contains("refreshed:5678"));
        let protocols: Vec<_> = attempts[0]
            .headers
            .get_all("sec-websocket-protocol")
            .iter()
            .collect();
        assert_eq!(protocols, ["v2", "v1"]);
        assert_eq!(attempts[0].headers.get_all("host").iter().count(), 1);
    }

    #[actix_rt::test]
    async fn invalid_provider_url_fails_the_attempt() {
        let (transport, attempts) = MockTransport::scripted(vec![]);
        let handler = TestHandler::default();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config(),
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .with_url_provider(|| Some("not a valid url".into()))
        .start();

        let result = addr.send(Connect).await.unwrap();
        assert!(matches!(result, Err(Error::InvalidRequest(_))));
        // the transport was never called with the invalid request
        assert!(attempts.lock().unwrap().is_empty());
    }

    /// Creates a client for `url` without starting it.
    fn new_client(url: &str) -> Result<GenericWebSocketClient<TestHandler>, Error> {
        GenericWebSocketClient::new("test", url, config(), TestHandler::default())
    }

    #[test]
    fn new_rejects_a_url_that_is_not_a_websocket_url() {
        for url in [
            "http://example.com/ws",
            "https://example.com/ws",
            "ftp://example.com",
        ] {
            assert!(
                matches!(new_client(url), Err(Error::InvalidRequest(_))),
                "{url} can never be connected"
            );
        }
        assert!(new_client("ws://example.com/ws").is_ok());
    }

    #[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
    #[test]
    fn new_accepts_wss_with_a_tls_backend() {
        assert!(new_client("wss://example.com/ws").is_ok());
    }

    #[cfg(not(any(feature = "native-tls", feature = "rustls-platform-verifier")))]
    #[test]
    fn new_rejects_wss_without_a_tls_backend() {
        assert!(
            matches!(
                new_client("wss://example.com/ws"),
                Err(Error::InvalidRequest(_))
            ),
            "wss:// can never be connected without a TLS backend"
        );
    }

    #[actix_rt::test]
    async fn provider_url_that_is_not_a_websocket_url_fails_the_attempt() {
        let (transport, attempts) = MockTransport::scripted(vec![]);
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config(),
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .with_url_provider(|| Some("http://localhost".into()))
        .start();

        let result = addr.send(Connect).await.unwrap();
        assert!(matches!(result, Err(Error::InvalidRequest(_))));
        // the transport was never called with the unusable request
        assert!(attempts.lock().unwrap().is_empty());
        addr.do_send(Disconnect);
    }

    // ========================================================================
    // Frame/message size limits
    // ========================================================================

    #[actix_rt::test]
    async fn frame_size_limits_are_passed_to_the_transport() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let config = WebSocketConfig::builder()
            .max_message_size(262_144usize)
            .max_frame_size(65_536usize)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .start();

        addr.send(Connect).await.unwrap().unwrap();

        let attempts = attempts.lock().unwrap();
        assert_eq!(
            attempts[0].options.ws_config.max_message_size,
            Some(262_144)
        );
        assert_eq!(attempts[0].options.ws_config.max_frame_size, Some(65_536));
    }

    #[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
    #[actix_rt::test]
    async fn danger_accept_invalid_certs_is_passed_to_the_transport() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let config = WebSocketConfig::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "wss://localhost",
            config,
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .start();

        addr.send(Connect).await.unwrap().unwrap();

        let attempts = attempts.lock().unwrap();
        // A dedicated connector without verification replaces the transport default.
        let _ = rustls::crypto::ring::default_provider().install_default();
        assert!(attempts[0].options.tls.connector().unwrap().is_some());
    }

    #[actix_rt::test]
    async fn default_config_keeps_transport_size_defaults() {
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = client(transport, TestHandler::default());

        addr.send(Connect).await.unwrap().unwrap();

        let attempts = attempts.lock().unwrap();
        let defaults = WsProtocolConfig::default();
        assert_eq!(
            attempts[0].options.ws_config.max_message_size,
            defaults.max_message_size
        );
        assert_eq!(
            attempts[0].options.ws_config.max_frame_size,
            defaults.max_frame_size
        );
        // A connector with the defaults of the backend: system roots, full
        // verification. It is built once and reused for every attempt.
        #[cfg(feature = "native-tls")]
        assert!(attempts[0].options.tls.connector().unwrap().is_some());
    }

    // ------------------------------------------------------------------
    // Connection teardown
    //
    // These tests run with a paused clock: timeouts elapse without waiting, and a
    // future that never resolves fails the test instead of hanging it.
    // ------------------------------------------------------------------

    fn is_close(msg: &Message) -> bool {
        matches!(msg, Message::Close(_))
    }

    #[actix_rt::test]
    async fn disconnect_with_a_stalled_writer_fails_pending_sends_and_drops_the_connection() {
        tokio::time::pause();
        let (sink, sink_dropped) = track_sink(blocked_sink());
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = client(transport, TestHandler::default());
        addr.send(Connect).await.unwrap().unwrap();

        // The socket does not take any data: one message is stuck in the writer, one
        // waits in the queue.
        let in_flight = addr.send(SendText::new("in flight"));
        let queued = addr.send(SendText::new("queued"));
        tokio::time::sleep(Duration::from_millis(10)).await;

        addr.do_send(Disconnect);

        let in_flight = tokio::time::timeout(Duration::from_secs(60), in_flight)
            .await
            .expect("the in-flight send must not hang after a disconnect");
        let queued = tokio::time::timeout(Duration::from_secs(60), queued)
            .await
            .expect("the queued send must not hang after a disconnect");
        assert_eq!(in_flight.unwrap(), Err(Error::NotConnected));
        assert_eq!(queued.unwrap(), Err(Error::NotConnected));
        assert!(
            sink_dropped.load(SeqCst),
            "the stalled connection must be dropped"
        );
    }

    #[actix_rt::test]
    async fn keep_alive_timeout_aborts_a_stalled_writer() {
        tokio::time::pause();
        let (sink1, sink1_dropped) = track_sink(blocked_sink());
        let (_script1, stream1) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (_script2, stream2) = scripted_stream();
        let (transport, _) =
            MockTransport::scripted(vec![Ok((sink1, stream1)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let config = WebSocketConfig::builder()
            .ping_interval(Duration::from_secs(1))
            .max_missed_pongs(1u32)
            .initial_backoff(Duration::from_millis(50))
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .start();
        addr.send(Connect).await.unwrap().unwrap();
        let pending = addr.send(SendText::new("stuck"));

        // The silent server is dropped after 2 s; the client reconnects.
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(handler.log_contains("closed:KeepAliveTimeout"));
        assert_eq!(handler.count("connected"), 2);

        let pending = tokio::time::timeout(Duration::from_millis(100), pending)
            .await
            .expect("a send on the dropped connection must not hang");
        assert_eq!(pending.unwrap(), Err(Error::NotConnected));
        assert!(
            sink1_dropped.load(SeqCst),
            "the dead connection must be dropped"
        );
    }

    #[actix_rt::test]
    async fn local_close_keeps_the_connection_until_the_server_closes() {
        tokio::time::pause();
        let (sink, sent) = mock_sink(usize::MAX);
        let (sink, sink_dropped) = track_sink(sink);
        let (script, stream) = scripted_stream();
        let (stream, stream_dropped) = track_stream(stream);
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = client(transport, TestHandler::default());
        addr.send(Connect).await.unwrap().unwrap();

        addr.do_send(Disconnect);
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The close frame is out, the reply of the server is not in yet.
        assert!(sent_contains(&sent, is_close));
        assert!(
            !stream_dropped.load(SeqCst),
            "the client must keep reading until the server closed"
        );

        // The server answers the close frame and closes the connection.
        script.unbounded_send(Ok(Message::Close(None))).unwrap();
        drop(script);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(stream_dropped.load(SeqCst));
        assert!(sink_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn disconnect_resolves_when_the_connection_is_closed() {
        tokio::time::pause();
        let (sink, sent) = mock_sink(usize::MAX);
        let (sink, sink_dropped) = track_sink(sink);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = client(transport, TestHandler::default());
        addr.send(Connect).await.unwrap().unwrap();

        let disconnect = addr.send(Disconnect);
        tokio::pin!(disconnect);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut disconnect)
                .await
                .is_err(),
            "Disconnect must not resolve before the server closed"
        );
        assert!(sent_contains(&sent, is_close));
        // The client is disconnected right away, only the socket lingers.
        assert!(!addr.send(IsConnected).await.unwrap());

        script.unbounded_send(Ok(Message::Close(None))).unwrap();
        drop(script);
        tokio::time::timeout(Duration::from_secs(1), disconnect)
            .await
            .expect("Disconnect must resolve once the server closed")
            .unwrap();
        assert!(sink_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn disconnected_client_stops_when_dropped_despite_a_stalled_writer() {
        tokio::time::pause();
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((blocked_sink(), stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();
        addr.do_send(SendText::new("stuck"));
        tokio::time::sleep(Duration::from_millis(10)).await;

        addr.do_send(Disconnect);
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(addr);
        tokio::time::sleep(Duration::from_secs(60)).await;

        // The actor owns the second reference to the handler's log.
        assert_eq!(
            Arc::strong_count(&handler.log),
            1,
            "the actor must stop: nothing may keep it alive"
        );
    }

    // ------------------------------------------------------------------
    // Actor lifecycle
    // ------------------------------------------------------------------

    /// Whether the actor is gone: it owns the second reference to the handler's log.
    fn actor_stopped(handler: &TestHandler) -> bool {
        Arc::strong_count(&handler.log) == 1
    }

    #[actix_rt::test]
    async fn dropping_the_last_address_closes_a_connected_client() {
        tokio::time::pause();
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        drop(addr);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            actor_stopped(&handler),
            "a client nobody can reach anymore must not stay connected"
        );
        // The connection is closed gracefully, with the usual callbacks.
        assert!(sent_contains(&sent, is_close));
        assert!(handler.log_contains("closed:ClosedLocally"));
        assert_eq!(handler.count("disconnected"), 1);
    }

    #[actix_rt::test]
    async fn dropping_the_last_address_stops_a_reconnecting_client() {
        tokio::time::pause();
        // Every connection attempt fails.
        let (transport, attempts) = MockTransport::scripted(vec![]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        assert!(addr.send(Connect).await.unwrap().is_err());

        drop(addr);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let attempts_at_drop = attempts.lock().unwrap().len();
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert!(
            actor_stopped(&handler),
            "a client nobody can reach anymore must not keep reconnecting"
        );
        assert_eq!(attempts.lock().unwrap().len(), attempts_at_drop);
    }

    #[actix_rt::test]
    async fn dropping_the_last_address_stops_an_idle_client() {
        let (transport, _) = MockTransport::scripted(vec![]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        assert!(!addr.send(IsConnected).await.unwrap());

        drop(addr);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(actor_stopped(&handler));
        assert_eq!(handler.count("disconnected"), 0);
    }

    #[actix_rt::test]
    async fn another_address_keeps_the_client_running() {
        tokio::time::pause();
        let (sink, sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        // A recipient is an address, too.
        let recipient = addr.clone().recipient::<SendText>();
        drop(addr);
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert!(!actor_stopped(&handler));
        recipient
            .send(SendText::new("still here"))
            .await
            .unwrap()
            .unwrap();
        assert!(sent_contains(
            &sent,
            |m| matches!(m, Message::Text(t) if t.as_str() == "still here")
        ));

        drop(recipient);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(actor_stopped(&handler));
    }

    #[actix_rt::test]
    async fn stop_closes_the_connection_and_stops_the_actor() {
        tokio::time::pause();
        let (sink, sent) = mock_sink(usize::MAX);
        let (sink, sink_dropped) = track_sink(sink);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        let stop = addr.send(Stop);
        tokio::pin!(stop);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut stop)
                .await
                .is_err(),
            "Stop must not resolve before the server closed"
        );
        // The actor is gone, although an address of it still exists.
        assert!(actor_stopped(&handler));
        assert!(sent_contains(&sent, is_close));
        assert!(handler.log_contains("closed:ClosedLocally"));
        assert_eq!(handler.count("disconnected"), 1);
        assert!(addr.send(IsConnected).await.is_err());

        // The close handshake completes without the actor.
        script.unbounded_send(Ok(Message::Close(None))).unwrap();
        drop(script);
        tokio::time::timeout(Duration::from_secs(1), stop)
            .await
            .expect("Stop must resolve once the server closed")
            .unwrap();
        assert!(sink_dropped.load(SeqCst));
    }

    #[actix_rt::test]
    async fn stop_ends_a_reconnecting_client() {
        tokio::time::pause();
        let (transport, attempts) = MockTransport::scripted(vec![]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        assert!(addr.send(Connect).await.unwrap().is_err());

        addr.send(Stop).await.unwrap();
        let attempts_at_stop = attempts.lock().unwrap().len();
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert!(actor_stopped(&handler));
        assert_eq!(attempts.lock().unwrap().len(), attempts_at_stop);
    }

    // ------------------------------------------------------------------
    // Actions of on_error, on_closed and on_reconnect_exhausted
    // ------------------------------------------------------------------

    #[actix_rt::test]
    async fn disconnect_from_on_error_stops_reconnecting() {
        tokio::time::pause();
        let (transport, attempts) = MockTransport::scripted(vec![Err(Error::Http { status: 401 })]);
        let handler = TestHandler::default();
        // Retrying with rejected credentials is pointless.
        *handler.error_actions.lock().unwrap() = vec![HandlerAction::Disconnect];
        let addr = client(transport, handler.clone());

        let result = addr.send(Connect).await.unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert_eq!(result, Err(Error::Http { status: 401 }));
        assert_eq!(
            attempts.lock().unwrap().len(),
            1,
            "the scheduled reconnect must be cancelled"
        );
        assert_eq!(handler.count("exhausted"), 0);
        // The client is usable: a new request connects again.
        assert!(addr.send(Connect).await.unwrap().is_err());
        assert_eq!(attempts.lock().unwrap().len(), 2);
    }

    #[actix_rt::test]
    async fn disconnect_from_on_closed_stops_reconnecting_after_a_server_close() {
        tokio::time::pause();
        let (sink, _sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        *handler.closed_actions.lock().unwrap() = vec![HandlerAction::Disconnect];
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        // An application status that means "do not come back".
        script
            .unbounded_send(Ok(Message::Close(Some(CloseFrame {
                code: CloseCode::from(4001),
                reason: "access revoked".into(),
            }))))
            .unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert!(handler.log_contains(
            "closed:ClosedByServer { code: Some(4001), reason: \"access revoked\" }"
        ));
        assert_eq!(handler.count("disconnected"), 1);
        assert_eq!(attempts.lock().unwrap().len(), 1, "no reconnect");
        assert!(!addr.send(IsConnected).await.unwrap());
    }

    #[actix_rt::test]
    async fn reconnect_from_on_reconnect_exhausted_starts_a_fresh_sequence() {
        tokio::time::pause();
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![
            Err(Error::WebSocket("refused".into())),
            Err(Error::WebSocket("refused".into())),
            Ok((sink, stream)),
        ]);
        let handler = TestHandler::default();
        *handler.exhausted_actions.lock().unwrap() = vec![HandlerAction::Reconnect];
        let config = WebSocketConfig::builder()
            .max_reconnect_attempts(1u32)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .start();

        assert!(addr.send(Connect).await.unwrap().is_err());
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Initial attempt and one retry failed: exhausted, then the handler's retry.
        assert_eq!(handler.count("exhausted"), 1);
        assert_eq!(attempts.lock().unwrap().len(), 3);
        assert!(addr.send(IsConnected).await.unwrap());
    }

    #[actix_rt::test]
    async fn send_actions_without_a_connection_are_dropped() {
        tokio::time::pause();
        let (transport, attempts) =
            MockTransport::scripted(vec![Err(Error::WebSocket("refused".into()))]);
        let handler = TestHandler::default();
        *handler.error_actions.lock().unwrap() =
            vec![HandlerAction::SendText("nobody to send to".into())];
        let addr = client(transport, handler.clone());

        assert!(addr.send(Connect).await.unwrap().is_err());
        tokio::time::sleep(Duration::from_millis(10)).await;

        // The reconnect policy is not affected: the client keeps retrying.
        assert_eq!(attempts.lock().unwrap().len(), 1);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(attempts.lock().unwrap().len() > 1);
        assert!(!addr.send(IsConnected).await.unwrap());
        addr.do_send(Disconnect);
    }

    #[actix_rt::test]
    async fn actions_of_a_stopping_client_are_ignored() {
        tokio::time::pause();
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        // The handler always wants a new connection when one closes.
        *handler.closed_actions.lock().unwrap() = vec![HandlerAction::Reconnect];
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        drop(addr);
        tokio::time::sleep(Duration::from_secs(60)).await;

        assert!(handler.log_contains("closed:ClosedLocally"));
        assert!(actor_stopped(&handler));
        assert_eq!(
            attempts.lock().unwrap().len(),
            1,
            "a stopped client stays stopped"
        );
    }

    // ------------------------------------------------------------------
    // Classification of a lost connection
    // ------------------------------------------------------------------

    #[actix_rt::test]
    async fn write_failure_is_reported_with_the_kind_of_the_io_error() {
        let (sink, _sent) = failing_sink(0, broken_pipe);
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        let addr = client(transport, handler.clone());
        addr.send(Connect).await.unwrap().unwrap();

        // The message itself fails with `SendFailed` ...
        let result = addr.send(SendText::new("lost")).await.unwrap();
        assert!(matches!(result, Err(Error::SendFailed(_))));

        // ... the connection ends with the classified error of the socket.
        wait_until(|| handler.count("disconnected") == 1, "disconnected").await;
        let log = handler.log.lock().unwrap();
        assert!(
            log.iter()
                .any(|e| e.starts_with("closed:Error(Io { kind: BrokenPipe")),
            "{log:?}"
        );
        drop(log);
        addr.do_send(Disconnect);
    }

    // ------------------------------------------------------------------
    // Logging
    // ------------------------------------------------------------------

    /// The log lines that contain `needle`.
    fn logged(needle: &str) -> Vec<String> {
        captured_logs()
            .into_iter()
            .filter(|line| line.contains(needle))
            .collect()
    }

    /// Message contents are only logged by message tracing, which the handler can
    /// redact. No other log line may contain them, at any level.
    #[actix_rt::test]
    async fn message_contents_are_not_logged() {
        captured_logs(); // install the logger
        let (sink, sent) = mock_sink(usize::MAX);
        let (script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let handler = TestHandler::default();
        handler.set_text_actions(vec![
            HandlerAction::SendText("secret-reply-4711".into()),
            HandlerAction::SendBinary(Bytes::from_static(b"\x01\x02\x03\x04\x05\x06\x07")),
        ]);
        let addr = GenericWebSocketClient::with_transport(
            "log-contents",
            "ws://localhost",
            config(),
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        script
            .unbounded_send(Ok(Message::Text("secret-inbound-4711".into())))
            .unwrap();
        script
            .unbounded_send(Ok(Message::Binary(Bytes::from_static(
                b"\x11\x12\x13\x14\x15\x16\x17",
            ))))
            .unwrap();
        addr.send(SendText::new("secret-outbound-4711"))
            .await
            .unwrap()
            .unwrap();
        wait_until(|| sent.lock().unwrap().len() == 3, "all frames sent").await;
        wait_until(|| handler.log_contains("binary:7"), "binary received").await;

        // The trace level was logged ...
        assert!(!logged("[log-contents] Executing effect").is_empty());
        assert!(!logged("[log-contents] Executing handler action").is_empty());
        // ... without the contents of the messages.
        assert_eq!(logged("secret-inbound-4711"), Vec::<String>::new());
        assert_eq!(logged("secret-reply-4711"), Vec::<String>::new());
        assert_eq!(logged("secret-outbound-4711"), Vec::<String>::new());
        // Binary payloads as Debug print them.
        assert_eq!(logged("\\x11\\x12\\x13"), Vec::<String>::new());
        assert_eq!(logged("\\x01\\x02\\x03"), Vec::<String>::new());
        addr.do_send(Disconnect);
    }

    /// A URL can carry credentials: as user info, or as a token in the query.
    #[actix_rt::test]
    async fn url_secrets_are_not_logged() {
        captured_logs(); // install the logger
        let (sink, _sent) = mock_sink(usize::MAX);
        let (_script, stream) = scripted_stream();
        let (transport, attempts) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let addr = GenericWebSocketClient::with_transport(
            "log-url",
            "ws://original:1234",
            config(),
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .with_url_provider(|| {
            Some("ws://user:password-4711@refreshed:5678/socket?token=token-4711".into())
        })
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        // The secrets are used for the connection ...
        assert!(attempts.lock().unwrap()[0].uri.contains("token-4711"));
        // ... but only the target is logged.
        assert!(!logged("refreshed:5678/socket").is_empty());
        assert_eq!(logged("token-4711"), Vec::<String>::new());
        assert_eq!(logged("password-4711"), Vec::<String>::new());
        addr.do_send(Disconnect);
    }

    /// The error of an invalid URL reaches the caller, `on_error` and the log.
    #[actix_rt::test]
    async fn url_secrets_are_not_part_of_an_error() {
        captured_logs(); // install the logger
        let (transport, _) = MockTransport::scripted(vec![]);
        let addr = GenericWebSocketClient::with_transport(
            "log-error",
            "ws://localhost",
            config(),
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .with_url_provider(|| Some("not a url/socket?token=token-4712".into()))
        .start();

        let error = addr.send(Connect).await.unwrap().unwrap_err();

        assert!(matches!(error, Error::InvalidRequest(_)));
        assert!(!error.to_string().contains("token-4712"), "{error}");
        assert_eq!(logged("token-4712"), Vec::<String>::new());
        addr.do_send(Disconnect);
    }

    // ------------------------------------------------------------------
    // Keep-alive on a busy connection
    // ------------------------------------------------------------------

    /// A ping must not wait behind the messages of the application: on a slow
    /// connection with a backlog, it would reach the server too late to be answered
    /// in time, and a working connection would be dropped.
    #[actix_rt::test]
    async fn keep_alive_ping_does_not_wait_behind_queued_messages() {
        tokio::time::pause();
        // The connection takes 100 ms per message.
        let (sink, sent) = slow_sink(Duration::from_millis(100));
        let (_script, stream) = scripted_stream();
        let (transport, _) = MockTransport::scripted(vec![Ok((sink, stream))]);
        let config = WebSocketConfig::builder()
            .ping_interval(Duration::from_secs(1))
            .max_missed_pongs(10u32)
            .build()
            .unwrap();
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config,
            TestHandler::default(),
            Box::new(transport),
        )
        .unwrap()
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        // A backlog of 5 seconds.
        for i in 0..50 {
            addr.do_send(SendText::new(format!("message {i}")));
        }
        // The first ping is due after 1 second, with 40 messages still waiting.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let sent = sent.lock().unwrap();
        let ping = sent.iter().position(|m| matches!(m, Message::Ping(_)));
        assert!(
            ping.is_some_and(|position| position <= 11),
            "the ping must be written as soon as the message in flight is out, \
             ping at {ping:?} of {} written messages",
            sent.len()
        );
        // The messages of the application keep their order.
        let texts: Vec<_> = sent
            .iter()
            .filter_map(|m| match m {
                Message::Text(text) => Some(text.as_str().to_owned()),
                _ => None,
            })
            .collect();
        let expected: Vec<_> = (0..texts.len()).map(|i| format!("message {i}")).collect();
        assert_eq!(texts, expected);
    }

    // ------------------------------------------------------------------
    // Write timeout
    // ------------------------------------------------------------------

    /// A server that keeps sending but does not read anymore: the socket takes no
    /// data, and every received frame tells the keep-alive that the server is alive.
    async fn client_of_a_server_that_does_not_read(
        write_timeout: Option<Duration>,
    ) -> (
        Addr<GenericWebSocketClient<TestHandler>>,
        TestHandler,
        actix::prelude::Request<GenericWebSocketClient<TestHandler>, SendText>,
    ) {
        let (script, stream) = scripted_stream();
        let (sink2, _sent2) = mock_sink(usize::MAX);
        let (script2, stream2) = scripted_stream();
        let (transport, _) =
            MockTransport::scripted(vec![Ok((blocked_sink(), stream)), Ok((sink2, stream2))]);
        let handler = TestHandler::default();
        let mut config = WebSocketConfig::builder()
            .ping_interval(Duration::from_secs(1))
            .initial_backoff(Duration::from_millis(50));
        if let Some(limit) = write_timeout {
            config = config.write_timeout(limit);
        }
        let addr = GenericWebSocketClient::with_transport(
            "test",
            "ws://localhost",
            config.build().unwrap(),
            handler.clone(),
            Box::new(transport),
        )
        .unwrap()
        .start();
        addr.send(Connect).await.unwrap().unwrap();

        // The server sends an event twice a second, for as long as it is connected.
        actix::spawn(async move {
            while script
                .unbounded_send(Ok(Message::Binary(Bytes::from_static(b"event"))))
                .is_ok()
            {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            // The second connection stays open.
            let _script2 = script2;
            std::future::pending::<()>().await;
        });
        let pending = addr.send(SendText::new("stuck"));
        (addr, handler, pending)
    }

    #[actix_rt::test]
    async fn write_timeout_drops_a_connection_that_takes_no_data() {
        tokio::time::pause();
        let limit = Duration::from_secs(10);
        let (addr, handler, pending) = client_of_a_server_that_does_not_read(Some(limit)).await;

        tokio::time::sleep(limit + Duration::from_secs(1)).await;

        // The message is answered, the connection is replaced.
        let result = tokio::time::timeout(Duration::from_millis(100), pending)
            .await
            .expect("the send must not hang");
        assert!(matches!(result.unwrap(), Err(Error::SendFailed(_))));
        assert!(handler.log_contains("error:Write timeout"));
        assert!(handler.log_contains("closed:Error(WriteTimeout)"));
        assert_eq!(handler.count("connected"), 2);
        assert!(addr.send(IsConnected).await.unwrap());
    }

    /// The same server without a write timeout, as a reference: nothing ends the
    /// connection, because the keep-alive sees a server that is alive.
    #[actix_rt::test]
    async fn without_a_write_timeout_a_server_that_does_not_read_is_not_noticed() {
        tokio::time::pause();
        let (addr, handler, pending) = client_of_a_server_that_does_not_read(None).await;

        tokio::time::sleep(Duration::from_secs(600)).await;

        assert_eq!(handler.count("connected"), 1);
        assert_eq!(handler.count("disconnected"), 0);
        assert!(addr.send(IsConnected).await.unwrap());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), pending)
                .await
                .is_err(),
            "the send is still waiting"
        );
    }
}
