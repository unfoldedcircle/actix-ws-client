//! The pure, synchronous connection state machine ([sans-IO] pattern).
//!
//! [`Machine`] owns **no** sockets, tasks, timers or clock. Its entire surface is
//! [`Machine::on_event`]: feed it an [`Event`] and the current time, get back the
//! [`Effect`]s the I/O adapter must execute. It is deterministic and unit-testable
//! without a network, an async runtime, or sleeps:
//!
//! - time is injected (`now: Instant`),
//! - backoff jitter comes from an injectable source (production: `fastrand`),
//! - all decisions (connect now, retry in `D`, drop a stale frame, emit a callback)
//!   are returned as data.
//!
//! # Generations
//!
//! Every connection attempt gets a fresh, strictly increasing `generation`. All
//! per-connection tasks and timers are tagged with it, and every event they produce
//! carries it back. The core ignores events from any generation but the current one,
//! so a late completion, frame or timer tick from a dead connection is dropped by
//! construction — the bug class of "stale resource re-entering the actor" cannot
//! occur.
//!
//! [sans-IO]: https://sans-io.readthedocs.io/

pub(crate) mod backoff;
pub(crate) mod effect;
pub(crate) mod event;
pub(crate) mod keepalive;
pub(crate) mod state;

use crate::Error;
use backoff::BackoffCfg;
use bytes::Bytes;
use effect::{Callback, Effect};
use event::{Event, Frame};
use keepalive::{KeepAlive, KeepAliveOutcome};
use state::ConnState;
use std::time::{Duration, Instant};

/// Source of backoff jitter values in `0.0..1.0`.
type JitterSource = Box<dyn FnMut() -> f64 + Send>;

/// Configuration snapshot used by the core, extracted from [`crate::WebSocketConfig`].
#[derive(Clone, Debug)]
pub(crate) struct CoreConfig {
    /// Timeout for a single connection attempt.
    pub connect_timeout: Duration,
    /// Keep-alive ping interval.
    pub ping_interval: Duration,
    /// Consecutive missed pongs before dropping the connection.
    pub max_missed_pongs: u32,
    /// Minimum uptime before the reconnection attempt counter is reset.
    pub min_stable_uptime: Duration,
    /// Maximum number of consecutive reconnection attempts (`None` = unlimited).
    pub max_reconnect_attempts: Option<u32>,
    /// Backoff policy.
    pub backoff: BackoffCfg,
}

impl From<&crate::WebSocketConfig> for CoreConfig {
    fn from(cfg: &crate::WebSocketConfig) -> Self {
        Self {
            connect_timeout: cfg.connect_timeout,
            ping_interval: cfg.ping_interval,
            max_missed_pongs: cfg.max_missed_pongs,
            min_stable_uptime: cfg.min_stable_uptime,
            max_reconnect_attempts: cfg.max_reconnect_attempts,
            backoff: BackoffCfg {
                initial: cfg.initial_backoff,
                max: cfg.max_backoff,
                factor: cfg.backoff_factor,
            },
        }
    }
}

/// The pure connection state machine.
pub(crate) struct Machine {
    cfg: CoreConfig,
    state: ConnState,
    generation: u64,
    should_reconnect: bool,
    reconnection_attempts: u32,
    connected_at: Option<Instant>,
    /// True while an `OnConnected` callback has been emitted without a matching
    /// `OnDisconnected` — guarantees the edge-triggered, balanced callback contract.
    signalled_connected: bool,
    keepalive: KeepAlive,
    jitter: JitterSource,
}

impl Machine {
    /// Creates a machine with the production jitter source (`fastrand`).
    pub(crate) fn new(cfg: CoreConfig) -> Self {
        Self::with_jitter(cfg, Box::new(fastrand::f64))
    }

    /// Creates a machine with an injected jitter source (deterministic tests).
    pub(crate) fn with_jitter(cfg: CoreConfig, jitter: JitterSource) -> Self {
        let keepalive = KeepAlive::new(cfg.max_missed_pongs);
        Self {
            cfg,
            state: ConnState::Disconnected,
            generation: 0,
            should_reconnect: false,
            reconnection_attempts: 0,
            connected_at: None,
            signalled_connected: false,
            keepalive,
            jitter,
        }
    }

    /// Current connection state.
    pub(crate) fn state(&self) -> ConnState {
        self.state
    }

    /// Current connection generation.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether the client is currently connected.
    pub(crate) fn is_connected(&self) -> bool {
        self.state == ConnState::Connected
    }

    /// Feeds an event into the machine and returns the effects to execute, in order.
    pub(crate) fn on_event(&mut self, event: Event, now: Instant) -> Vec<Effect> {
        match event {
            Event::ConnectRequested => self.on_connect_requested(),
            Event::DisconnectRequested => self.on_disconnect_requested(),
            Event::ReconnectRequested => self.on_reconnect_requested(),
            Event::ConnectSucceeded { generation } => self.on_connect_succeeded(generation, now),
            Event::ConnectFailed { generation, error } => self.on_connect_failed(generation, error),
            Event::FrameReceived { generation, frame } => self.on_frame(generation, frame, now),
            Event::StreamEnded { generation, error } => {
                self.on_connection_lost(generation, error, now)
            }
            Event::WriteFailed { generation, error } => {
                self.on_connection_lost(generation, Some(error), now)
            }
            Event::KeepAliveTick { generation } => self.on_keepalive_tick(generation, now),
            Event::ReconnectTimerFired => self.on_reconnect_timer(),
        }
    }

    // ========================================================================
    // Event handlers
    // ========================================================================

    fn on_connect_requested(&mut self) -> Vec<Effect> {
        match self.state {
            ConnState::Disconnected | ConnState::WaitingToReconnect => {
                self.should_reconnect = true;
                // A user-initiated connect starts a fresh backoff sequence.
                self.reconnection_attempts = 0;
                self.start_connect()
            }
            // Already connecting or connected: the adapter answers `StateChanged`
            // when no `StartConnect` effect is returned.
            ConnState::Connecting | ConnState::Connected => Vec::new(),
        }
    }

    fn on_disconnect_requested(&mut self) -> Vec<Effect> {
        self.should_reconnect = false;
        match self.state {
            ConnState::Connected => {
                let effects = self.close_connected();
                self.state = ConnState::Disconnected;
                effects
            }
            ConnState::Connecting => {
                self.state = ConnState::Disconnected;
                vec![
                    Effect::TearDownConnection {
                        generation: self.generation,
                    },
                    Effect::CompleteConnect {
                        result: Err(Error::StateChanged),
                    },
                ]
            }
            ConnState::WaitingToReconnect => {
                self.state = ConnState::Disconnected;
                // Teardown cancels the armed reconnect timer.
                vec![Effect::TearDownConnection {
                    generation: self.generation,
                }]
            }
            ConnState::Disconnected => Vec::new(),
        }
    }

    fn on_reconnect_requested(&mut self) -> Vec<Effect> {
        match self.state {
            ConnState::Connected => {
                // Graceful close of the current connection, then immediately start a
                // fresh one with a reset attempt counter.
                let mut effects = self.close_connected();
                self.should_reconnect = true;
                self.reconnection_attempts = 0;
                effects.extend(self.start_connect());
                effects
            }
            ConnState::Disconnected | ConnState::WaitingToReconnect => {
                // StartConnect execution cancels an armed reconnect timer.
                self.should_reconnect = true;
                self.reconnection_attempts = 0;
                self.start_connect()
            }
            // A connection attempt is already in progress.
            ConnState::Connecting => Vec::new(),
        }
    }

    fn on_connect_succeeded(&mut self, generation: u64, now: Instant) -> Vec<Effect> {
        if self.state != ConnState::Connecting || generation != self.generation {
            return Vec::new(); // stale or cancelled attempt
        }
        self.state = ConnState::Connected;
        self.connected_at = Some(now);
        self.keepalive.reset();
        self.signalled_connected = true;
        vec![
            Effect::StartWriterTask { generation },
            Effect::StartReaderTask { generation },
            Effect::StartKeepAlive {
                generation,
                interval: self.cfg.ping_interval,
            },
            Effect::Emit(Callback::OnConnected),
            Effect::CompleteConnect { result: Ok(()) },
        ]
    }

    fn on_connect_failed(&mut self, generation: u64, error: Error) -> Vec<Effect> {
        if self.state != ConnState::Connecting || generation != self.generation {
            return Vec::new(); // stale or cancelled attempt
        }
        let mut effects = vec![
            Effect::Emit(Callback::OnError(error.clone())),
            Effect::CompleteConnect { result: Err(error) },
        ];
        self.schedule_reconnect_or_idle(&mut effects);
        effects
    }

    fn on_frame(&mut self, generation: u64, frame: Frame, now: Instant) -> Vec<Effect> {
        if self.state != ConnState::Connected || generation != self.generation {
            return Vec::new(); // stale connection
        }
        match frame {
            Frame::Text(text) => vec![Effect::Emit(Callback::OnText(text))],
            Frame::Binary(data) => vec![Effect::Emit(Callback::OnBinary(data))],
            // Answer server pings promptly: with a split sink/stream, the transport's
            // automatic pong is only flushed on our next write, which may be up to a
            // ping interval away.
            Frame::Ping(payload) => vec![Effect::SendFrame {
                frame: Frame::Pong(payload),
            }],
            Frame::Pong(_) => {
                self.keepalive.on_pong();
                Vec::new()
            }
            Frame::Close => {
                // Reply to the peer's close handshake, then tear down.
                let mut effects = vec![Effect::SendFrame {
                    frame: Frame::Close,
                }];
                effects.extend(self.leave_connected(now));
                self.schedule_reconnect_or_idle(&mut effects);
                effects
            }
        }
    }

    fn on_connection_lost(
        &mut self,
        generation: u64,
        error: Option<Error>,
        now: Instant,
    ) -> Vec<Effect> {
        if self.state != ConnState::Connected || generation != self.generation {
            return Vec::new(); // stale connection
        }
        let mut effects = Vec::new();
        if let Some(error) = error {
            effects.push(Effect::Emit(Callback::OnError(error)));
        }
        effects.extend(self.leave_connected(now));
        self.schedule_reconnect_or_idle(&mut effects);
        effects
    }

    fn on_keepalive_tick(&mut self, generation: u64, now: Instant) -> Vec<Effect> {
        if self.state != ConnState::Connected || generation != self.generation {
            return Vec::new(); // stale timer
        }
        match self.keepalive.on_tick() {
            KeepAliveOutcome::SendPing => vec![Effect::SendFrame {
                frame: Frame::Ping(Bytes::new()),
            }],
            KeepAliveOutcome::Reconnect => {
                // RFC 6455 §7.1.7 "Fail the WebSocket Connection": the peer is
                // unresponsive, so drop the connection without a Close handshake.
                let mut effects = self.leave_connected(now);
                self.schedule_reconnect_or_idle(&mut effects);
                effects
            }
        }
    }

    fn on_reconnect_timer(&mut self) -> Vec<Effect> {
        if self.state != ConnState::WaitingToReconnect || !self.should_reconnect {
            return Vec::new(); // cancelled or superseded timer
        }
        self.start_connect()
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Graceful close of an established connection: enqueue the Close frame first,
    /// then drop the writer sender (the writer task drains its queue before
    /// terminating, so the Close frame is still sent).
    ///
    /// The caller decides the next state.
    fn close_connected(&mut self) -> Vec<Effect> {
        let mut effects = vec![
            Effect::StopKeepAlive,
            Effect::SendFrame {
                frame: Frame::Close,
            },
            Effect::TearDownConnection {
                generation: self.generation,
            },
        ];
        effects.extend(self.emit_disconnected_edge());
        self.connected_at = None;
        self.keepalive.reset();
        effects
    }

    /// Bumps the generation and transitions to `Connecting`.
    fn start_connect(&mut self) -> Vec<Effect> {
        self.generation += 1;
        self.state = ConnState::Connecting;
        vec![Effect::StartConnect {
            generation: self.generation,
            timeout: self.cfg.connect_timeout,
        }]
    }

    /// Common teardown when leaving `Connected`: resets the backoff counter if the
    /// connection was stable long enough (flapping protection), stops keep-alive,
    /// tears down connection resources and emits a balanced `OnDisconnected`.
    fn leave_connected(&mut self, now: Instant) -> Vec<Effect> {
        if let Some(connected_at) = self.connected_at
            && now.duration_since(connected_at) >= self.cfg.min_stable_uptime
        {
            self.reconnection_attempts = 0;
        }
        self.connected_at = None;
        self.keepalive.reset();
        let mut effects = vec![
            Effect::StopKeepAlive,
            Effect::TearDownConnection {
                generation: self.generation,
            },
        ];
        effects.extend(self.emit_disconnected_edge());
        effects
    }

    /// Emits `OnDisconnected` only on an actual `Connected →` leaving edge.
    fn emit_disconnected_edge(&mut self) -> Option<Effect> {
        if self.signalled_connected {
            self.signalled_connected = false;
            Some(Effect::Emit(Callback::OnDisconnected))
        } else {
            None
        }
    }

    /// Arms the reconnect timer with growing backoff, gives up if the attempt cap is
    /// reached, or goes idle if reconnection is disabled.
    fn schedule_reconnect_or_idle(&mut self, effects: &mut Vec<Effect>) {
        if !self.should_reconnect {
            self.state = ConnState::Disconnected;
            return;
        }
        if let Some(max) = self.cfg.max_reconnect_attempts
            && self.reconnection_attempts >= max
        {
            // `max` reconnection attempts have been made and failed: give up.
            // A new Connect/Reconnect request starts a fresh sequence.
            self.state = ConnState::Disconnected;
            self.should_reconnect = false;
            effects.push(Effect::Emit(Callback::OnReconnectExhausted));
            return;
        }
        self.reconnection_attempts += 1;
        let jitter = (self.jitter)();
        let delay = backoff::backoff(self.reconnection_attempts, &self.cfg.backoff, jitter);
        self.state = ConnState::WaitingToReconnect;
        effects.push(Effect::ArmReconnectTimer { delay });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PING_INTERVAL: Duration = Duration::from_secs(30);
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    const MIN_STABLE_UPTIME: Duration = Duration::from_secs(30);
    const INITIAL_BACKOFF: Duration = Duration::from_millis(500);

    fn test_config() -> CoreConfig {
        CoreConfig {
            connect_timeout: CONNECT_TIMEOUT,
            ping_interval: PING_INTERVAL,
            max_missed_pongs: 2,
            min_stable_uptime: MIN_STABLE_UPTIME,
            max_reconnect_attempts: None,
            backoff: BackoffCfg {
                initial: INITIAL_BACKOFF,
                max: Duration::from_secs(30),
                factor: 2.0,
            },
        }
    }

    /// Machine with deterministic (zero) jitter.
    fn machine() -> Machine {
        Machine::with_jitter(test_config(), Box::new(|| 0.0))
    }

    fn t0() -> Instant {
        Instant::now()
    }

    /// Drives the machine into `Connected` and returns the active generation.
    fn connect(m: &mut Machine, now: Instant) -> u64 {
        let effects = m.on_event(Event::ConnectRequested, now);
        assert!(matches!(effects[0], Effect::StartConnect { .. }));
        let generation = m.generation();
        m.on_event(Event::ConnectSucceeded { generation }, now);
        assert_eq!(m.state(), ConnState::Connected);
        generation
    }

    fn count_disconnected(effects: &[Effect]) -> usize {
        effects
            .iter()
            .filter(|e| **e == Effect::Emit(Callback::OnDisconnected))
            .count()
    }

    fn reconnect_delay(effects: &[Effect]) -> Option<Duration> {
        effects.iter().find_map(|e| match e {
            Effect::ArmReconnectTimer { delay } => Some(*delay),
            _ => None,
        })
    }

    // ------------------------------------------------------------------
    // Happy path
    // ------------------------------------------------------------------

    #[test]
    fn connect_success_starts_tasks_keepalive_and_callbacks() {
        let mut m = machine();
        let now = t0();

        let effects = m.on_event(Event::ConnectRequested, now);
        assert_eq!(
            effects,
            vec![Effect::StartConnect {
                generation: 1,
                timeout: CONNECT_TIMEOUT
            }]
        );
        assert_eq!(m.state(), ConnState::Connecting);

        let effects = m.on_event(Event::ConnectSucceeded { generation: 1 }, now);
        assert_eq!(
            effects,
            vec![
                Effect::StartWriterTask { generation: 1 },
                Effect::StartReaderTask { generation: 1 },
                Effect::StartKeepAlive {
                    generation: 1,
                    interval: PING_INTERVAL
                },
                Effect::Emit(Callback::OnConnected),
                Effect::CompleteConnect { result: Ok(()) },
            ]
        );
        assert!(m.is_connected());
    }

    #[test]
    fn text_and_binary_frames_are_dispatched() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        let effects = m.on_event(
            Event::FrameReceived {
                generation,
                frame: Frame::Text("hello".into()),
            },
            now,
        );
        assert_eq!(
            effects,
            vec![Effect::Emit(Callback::OnText("hello".into()))]
        );

        let data = Bytes::from_static(b"\x01\x02");
        let effects = m.on_event(
            Event::FrameReceived {
                generation,
                frame: Frame::Binary(data.clone()),
            },
            now,
        );
        assert_eq!(effects, vec![Effect::Emit(Callback::OnBinary(data))]);
    }

    #[test]
    fn connect_while_connected_or_connecting_is_rejected() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        // second request while Connecting: no effects -> adapter replies StateChanged
        assert!(m.on_event(Event::ConnectRequested, now).is_empty());

        m.on_event(Event::ConnectSucceeded { generation: 1 }, now);
        assert!(m.on_event(Event::ConnectRequested, now).is_empty());
        assert_eq!(m.generation(), 1);
    }

    // ------------------------------------------------------------------
    // Regression 1 (P0-1): stale writer events are ignored
    // ------------------------------------------------------------------

    #[test]
    fn stale_write_failed_does_not_touch_current_connection() {
        let mut m = machine();
        let now = t0();
        connect(&mut m, now);

        // connection 1 dies, reconnect fires, connection 2 comes up
        m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        m.on_event(Event::ReconnectTimerFired, now);
        m.on_event(Event::ConnectSucceeded { generation: 2 }, now);
        assert!(m.is_connected());
        assert_eq!(m.generation(), 2);

        // late write failure from the dead generation 1 writer
        let effects = m.on_event(
            Event::WriteFailed {
                generation: 1,
                error: Error::SendFailed("broken pipe".into()),
            },
            now,
        );
        assert!(
            effects.is_empty(),
            "stale event must be dropped: {effects:?}"
        );
        assert!(m.is_connected());
        assert_eq!(m.generation(), 2);
    }

    // ------------------------------------------------------------------
    // Regression 2 (P0-2): stale reader end must not tear down gen 2
    // ------------------------------------------------------------------

    #[test]
    fn stale_stream_ended_does_not_tear_down_new_connection() {
        let mut m = machine();
        let now = t0();
        connect(&mut m, now);

        m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        m.on_event(Event::ReconnectTimerFired, now);
        m.on_event(Event::ConnectSucceeded { generation: 2 }, now);

        // the old reader (generation 1) finally reports end-of-stream
        let effects = m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        assert!(
            effects.is_empty(),
            "stale event must be dropped: {effects:?}"
        );
        assert!(m.is_connected());
    }

    #[test]
    fn stale_frames_are_not_delivered() {
        let mut m = machine();
        let now = t0();
        connect(&mut m, now);
        m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        m.on_event(Event::ReconnectTimerFired, now);
        m.on_event(Event::ConnectSucceeded { generation: 2 }, now);

        // cross-talk from the old connection
        let effects = m.on_event(
            Event::FrameReceived {
                generation: 1,
                frame: Frame::Text("old".into()),
            },
            now,
        );
        assert!(effects.is_empty());
    }

    #[test]
    fn stale_connect_results_are_ignored() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now); // generation 1
        m.on_event(Event::DisconnectRequested, now); // cancels the attempt

        // the cancelled attempt reports late
        assert!(
            m.on_event(Event::ConnectSucceeded { generation: 1 }, now)
                .is_empty()
        );
        assert!(
            m.on_event(
                Event::ConnectFailed {
                    generation: 1,
                    error: Error::ConnectTimeout
                },
                now
            )
            .is_empty()
        );
        assert_eq!(m.state(), ConnState::Disconnected);
    }

    // ------------------------------------------------------------------
    // Regression 3 (P1-1): keep-alive budget
    // ------------------------------------------------------------------

    #[test]
    fn keepalive_reconnects_exactly_at_max_missed_pongs() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        // tick 1: ping sent
        let effects = m.on_event(Event::KeepAliveTick { generation }, now);
        assert_eq!(
            effects,
            vec![Effect::SendFrame {
                frame: Frame::Ping(Bytes::new())
            }]
        );

        // tick 2: 1 missed pong (< max 2) -> ping again, no reconnect
        let effects = m.on_event(Event::KeepAliveTick { generation }, now);
        assert_eq!(
            effects,
            vec![Effect::SendFrame {
                frame: Frame::Ping(Bytes::new())
            }]
        );
        assert!(m.is_connected());

        // tick 3: 2 missed pongs (== max) -> drop and reconnect
        let effects = m.on_event(Event::KeepAliveTick { generation }, now);
        assert_eq!(m.state(), ConnState::WaitingToReconnect);
        assert!(effects.contains(&Effect::TearDownConnection { generation }));
        assert!(reconnect_delay(&effects).is_some());
        // decision: keep-alive failure drops the connection, no Close handshake
        assert!(
            !effects.iter().any(|e| matches!(
                e,
                Effect::SendFrame {
                    frame: Frame::Close
                }
            )),
            "keep-alive teardown must not send a Close frame"
        );
        assert_eq!(count_disconnected(&effects), 1);
    }

    #[test]
    fn pong_keeps_the_connection_alive() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        for _ in 0..10 {
            let effects = m.on_event(Event::KeepAliveTick { generation }, now);
            assert_eq!(
                effects,
                vec![Effect::SendFrame {
                    frame: Frame::Ping(Bytes::new())
                }]
            );
            m.on_event(
                Event::FrameReceived {
                    generation,
                    frame: Frame::Pong(Bytes::new()),
                },
                now,
            );
        }
        assert!(m.is_connected());
    }

    // ------------------------------------------------------------------
    // Regression 4 (P1-2): flapping connection grows backoff; stable resets
    // ------------------------------------------------------------------

    #[test]
    fn flapping_connection_grows_backoff_and_stable_uptime_resets_it() {
        let mut m = machine();
        let mut now = t0();

        m.on_event(Event::ConnectRequested, now);
        m.on_event(Event::ConnectSucceeded { generation: 1 }, now);

        // drop after 1s (unstable) -> attempt 1 -> initial backoff
        now += Duration::from_secs(1);
        let effects = m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF));

        // flap again: connect, drop after 1s -> attempt 2 -> doubled
        m.on_event(Event::ReconnectTimerFired, now);
        m.on_event(Event::ConnectSucceeded { generation: 2 }, now);
        now += Duration::from_secs(1);
        let effects = m.on_event(
            Event::StreamEnded {
                generation: 2,
                error: None,
            },
            now,
        );
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF * 2));

        // and again -> attempt 3 -> quadrupled
        m.on_event(Event::ReconnectTimerFired, now);
        m.on_event(Event::ConnectSucceeded { generation: 3 }, now);
        now += Duration::from_secs(1);
        let effects = m.on_event(
            Event::StreamEnded {
                generation: 3,
                error: None,
            },
            now,
        );
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF * 4));

        // stable connection (>= min_stable_uptime) -> counter resets
        m.on_event(Event::ReconnectTimerFired, now);
        m.on_event(Event::ConnectSucceeded { generation: 4 }, now);
        now += MIN_STABLE_UPTIME;
        let effects = m.on_event(
            Event::StreamEnded {
                generation: 4,
                error: None,
            },
            now,
        );
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF));
    }

    #[test]
    fn connect_failures_grow_backoff() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        let effects = m.on_event(
            Event::ConnectFailed {
                generation: 1,
                error: Error::ConnectTimeout,
            },
            now,
        );
        // first retry uses the configured initial backoff (P2-2 off-by-one)
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF));
        assert!(effects.contains(&Effect::CompleteConnect {
            result: Err(Error::ConnectTimeout)
        }));

        m.on_event(Event::ReconnectTimerFired, now);
        let effects = m.on_event(
            Event::ConnectFailed {
                generation: 2,
                error: Error::ConnectTimeout,
            },
            now,
        );
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF * 2));
    }

    // ------------------------------------------------------------------
    // Regression 5: disconnect during reconnect
    // ------------------------------------------------------------------

    #[test]
    fn disconnect_while_waiting_to_reconnect_stops_all_attempts() {
        let mut m = machine();
        let now = t0();
        connect(&mut m, now);

        m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        assert_eq!(m.state(), ConnState::WaitingToReconnect);

        let effects = m.on_event(Event::DisconnectRequested, now);
        assert_eq!(m.state(), ConnState::Disconnected);
        assert!(effects.contains(&Effect::TearDownConnection { generation: 1 }));

        // a racing timer tick must not restart the connection
        let effects = m.on_event(Event::ReconnectTimerFired, now);
        assert!(effects.is_empty());
        assert_eq!(m.state(), ConnState::Disconnected);
    }

    #[test]
    fn disconnect_while_connecting_completes_connect_with_state_changed() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        let effects = m.on_event(Event::DisconnectRequested, now);
        assert!(effects.contains(&Effect::CompleteConnect {
            result: Err(Error::StateChanged)
        }));
        assert_eq!(m.state(), ConnState::Disconnected);
    }

    // ------------------------------------------------------------------
    // Callback contract (P2-3): edge-triggered and balanced
    // ------------------------------------------------------------------

    #[test]
    fn close_then_disconnect_emits_exactly_one_on_disconnected() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        // peer closes -> one OnDisconnected
        let effects = m.on_event(
            Event::FrameReceived {
                generation,
                frame: Frame::Close,
            },
            now,
        );
        assert_eq!(count_disconnected(&effects), 1);

        // subsequent explicit disconnect: no second OnDisconnected
        let effects = m.on_event(Event::DisconnectRequested, now);
        assert_eq!(count_disconnected(&effects), 0);
    }

    #[test]
    fn callbacks_are_balanced_over_reconnect_cycles() {
        let mut m = machine();
        let mut now = t0();
        let mut connected = 0usize;
        let mut disconnected = 0usize;

        let count = |effects: &[Effect], connected: &mut usize, disconnected: &mut usize| {
            for e in effects {
                match e {
                    Effect::Emit(Callback::OnConnected) => *connected += 1,
                    Effect::Emit(Callback::OnDisconnected) => *disconnected += 1,
                    _ => {}
                }
            }
        };

        let effects = m.on_event(Event::ConnectRequested, now);
        count(&effects, &mut connected, &mut disconnected);
        for cycle in 1..=5u64 {
            let effects = m.on_event(Event::ConnectSucceeded { generation: cycle }, now);
            count(&effects, &mut connected, &mut disconnected);
            now += Duration::from_secs(1);
            let effects = m.on_event(
                Event::StreamEnded {
                    generation: cycle,
                    error: None,
                },
                now,
            );
            count(&effects, &mut connected, &mut disconnected);
            let effects = m.on_event(Event::ReconnectTimerFired, now);
            count(&effects, &mut connected, &mut disconnected);
        }

        assert_eq!(connected, 5);
        assert_eq!(disconnected, 5);
    }

    #[test]
    fn failed_connect_emits_no_disconnected_callback() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        let effects = m.on_event(
            Event::ConnectFailed {
                generation: 1,
                error: Error::ConnectTimeout,
            },
            now,
        );
        assert_eq!(count_disconnected(&effects), 0);
        assert!(effects.contains(&Effect::Emit(Callback::OnError(Error::ConnectTimeout))));
    }

    // ------------------------------------------------------------------
    // Frame handling details
    // ------------------------------------------------------------------

    #[test]
    fn server_ping_is_answered_with_pong_carrying_the_payload() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        let payload = Bytes::from_static(b"app-data");
        let effects = m.on_event(
            Event::FrameReceived {
                generation,
                frame: Frame::Ping(payload.clone()),
            },
            now,
        );
        assert_eq!(
            effects,
            vec![Effect::SendFrame {
                frame: Frame::Pong(payload)
            }]
        );
    }

    #[test]
    fn peer_close_is_answered_and_reconnects() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        let effects = m.on_event(
            Event::FrameReceived {
                generation,
                frame: Frame::Close,
            },
            now,
        );
        // close reply is enqueued before the writer is torn down
        assert_eq!(
            effects[0],
            Effect::SendFrame {
                frame: Frame::Close
            }
        );
        assert!(effects.contains(&Effect::TearDownConnection { generation }));
        assert!(reconnect_delay(&effects).is_some());
        assert_eq!(m.state(), ConnState::WaitingToReconnect);
    }

    #[test]
    fn write_failure_reports_error_and_reconnects() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        let error = Error::SendFailed("io".into());
        let effects = m.on_event(
            Event::WriteFailed {
                generation,
                error: error.clone(),
            },
            now,
        );
        assert!(effects.contains(&Effect::Emit(Callback::OnError(error))));
        assert!(effects.contains(&Effect::TearDownConnection { generation }));
        assert_eq!(m.state(), ConnState::WaitingToReconnect);
    }

    // ------------------------------------------------------------------
    // Graceful disconnect
    // ------------------------------------------------------------------

    #[test]
    fn graceful_disconnect_sends_close_before_teardown() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        let effects = m.on_event(Event::DisconnectRequested, now);
        let close_pos = effects
            .iter()
            .position(|e| {
                matches!(
                    e,
                    Effect::SendFrame {
                        frame: Frame::Close
                    }
                )
            })
            .expect("close frame must be sent");
        let teardown_pos = effects
            .iter()
            .position(|e| *e == Effect::TearDownConnection { generation })
            .expect("teardown must happen");
        assert!(
            close_pos < teardown_pos,
            "close must be enqueued before the writer is dropped"
        );
        assert_eq!(count_disconnected(&effects), 1);
        assert_eq!(m.state(), ConnState::Disconnected);

        // no reconnect after a graceful disconnect
        assert!(reconnect_delay(&effects).is_none());
    }

    #[test]
    fn user_connect_after_disconnect_starts_fresh() {
        let mut m = machine();
        let now = t0();
        connect(&mut m, now);
        m.on_event(Event::DisconnectRequested, now);

        let effects = m.on_event(Event::ConnectRequested, now);
        assert_eq!(
            effects,
            vec![Effect::StartConnect {
                generation: 2,
                timeout: CONNECT_TIMEOUT
            }]
        );
    }

    // ------------------------------------------------------------------
    // Maximum reconnection attempts (§5.2)
    // ------------------------------------------------------------------

    /// Machine with a reconnection attempt cap.
    fn machine_with_cap(max_attempts: u32) -> Machine {
        let mut cfg = test_config();
        cfg.max_reconnect_attempts = Some(max_attempts);
        Machine::with_jitter(cfg, Box::new(|| 0.0))
    }

    fn fail_current_attempt(m: &mut Machine, now: Instant) -> Vec<Effect> {
        let generation = m.generation();
        m.on_event(
            Event::ConnectFailed {
                generation,
                error: Error::ConnectTimeout,
            },
            now,
        )
    }

    #[test]
    fn exactly_n_reconnect_attempts_then_exhausted() {
        let mut m = machine_with_cap(2);
        let now = t0();

        m.on_event(Event::ConnectRequested, now); // initial attempt (not a re-attempt)

        // initial failure -> retry 1 scheduled
        let effects = fail_current_attempt(&mut m, now);
        assert!(reconnect_delay(&effects).is_some());
        m.on_event(Event::ReconnectTimerFired, now);

        // retry 1 fails -> retry 2 scheduled
        let effects = fail_current_attempt(&mut m, now);
        assert!(reconnect_delay(&effects).is_some());
        m.on_event(Event::ReconnectTimerFired, now);

        // retry 2 fails -> cap reached: exactly one exhausted signal, no timer
        let effects = fail_current_attempt(&mut m, now);
        assert!(reconnect_delay(&effects).is_none());
        assert_eq!(
            effects
                .iter()
                .filter(|e| **e == Effect::Emit(Callback::OnReconnectExhausted))
                .count(),
            1
        );
        assert_eq!(m.state(), ConnState::Disconnected);

        // a racing timer tick must not restart anything
        assert!(m.on_event(Event::ReconnectTimerFired, now).is_empty());
    }

    #[test]
    fn connect_after_exhausted_starts_fresh_sequence() {
        let mut m = machine_with_cap(1);
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        fail_current_attempt(&mut m, now); // schedules retry 1
        m.on_event(Event::ReconnectTimerFired, now);
        fail_current_attempt(&mut m, now); // exhausted
        assert_eq!(m.state(), ConnState::Disconnected);

        // explicit reconnect request starts over with a fresh counter
        let effects = m.on_event(Event::ConnectRequested, now);
        assert!(matches!(effects[0], Effect::StartConnect { .. }));
        let effects = fail_current_attempt(&mut m, now);
        assert!(reconnect_delay(&effects).is_some(), "counter was reset");
    }

    #[test]
    fn cap_zero_disables_reconnection() {
        let mut m = machine_with_cap(0);
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        let effects = fail_current_attempt(&mut m, now);
        assert!(reconnect_delay(&effects).is_none());
        assert!(effects.contains(&Effect::Emit(Callback::OnReconnectExhausted)));
    }

    #[test]
    fn stable_uptime_resets_the_attempt_cap() {
        let mut m = machine_with_cap(2);
        let mut now = t0();

        // burn one attempt
        m.on_event(Event::ConnectRequested, now);
        fail_current_attempt(&mut m, now);
        m.on_event(Event::ReconnectTimerFired, now);

        // retry succeeds and stays up long enough -> counter resets
        let generation = m.generation();
        m.on_event(Event::ConnectSucceeded { generation }, now);
        now += MIN_STABLE_UPTIME;
        m.on_event(
            Event::StreamEnded {
                generation,
                error: None,
            },
            now,
        );

        // full budget of 2 attempts is available again
        m.on_event(Event::ReconnectTimerFired, now);
        let effects = fail_current_attempt(&mut m, now);
        assert!(reconnect_delay(&effects).is_some());
        m.on_event(Event::ReconnectTimerFired, now);
        let effects = fail_current_attempt(&mut m, now);
        assert!(
            effects.contains(&Effect::Emit(Callback::OnReconnectExhausted)),
            "second attempt of the fresh budget must exhaust the cap: {effects:?}"
        );
    }

    #[test]
    fn unstable_uptime_does_not_reset_the_attempt_cap() {
        let mut m = machine_with_cap(2);
        let mut now = t0();

        m.on_event(Event::ConnectRequested, now);
        fail_current_attempt(&mut m, now); // attempt 1
        m.on_event(Event::ReconnectTimerFired, now);

        // flapping: retry succeeds but drops before min_stable_uptime
        let generation = m.generation();
        m.on_event(Event::ConnectSucceeded { generation }, now);
        now += Duration::from_secs(1);
        m.on_event(
            Event::StreamEnded {
                generation,
                error: None,
            },
            now,
        ); // attempt 2 scheduled
        m.on_event(Event::ReconnectTimerFired, now);

        // attempt 2 fails -> cap reached
        let effects = fail_current_attempt(&mut m, now);
        assert!(effects.contains(&Effect::Emit(Callback::OnReconnectExhausted)));
    }

    #[test]
    fn default_config_retries_forever() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        for _ in 0..50 {
            let effects = fail_current_attempt(&mut m, now);
            assert!(reconnect_delay(&effects).is_some());
            assert!(!effects.contains(&Effect::Emit(Callback::OnReconnectExhausted)));
            m.on_event(Event::ReconnectTimerFired, now);
        }
    }

    // ------------------------------------------------------------------
    // ReconnectRequested (Reconnect message / HandlerAction::Reconnect)
    // ------------------------------------------------------------------

    #[test]
    fn reconnect_while_connected_closes_gracefully_and_reconnects() {
        let mut m = machine();
        let now = t0();
        let generation = connect(&mut m, now);

        let effects = m.on_event(Event::ReconnectRequested, now);

        // graceful close first ...
        let close_pos = effects
            .iter()
            .position(|e| {
                matches!(
                    e,
                    Effect::SendFrame {
                        frame: Frame::Close
                    }
                )
            })
            .expect("close frame");
        let teardown_pos = effects
            .iter()
            .position(|e| *e == Effect::TearDownConnection { generation })
            .expect("teardown");
        assert!(close_pos < teardown_pos);
        assert_eq!(count_disconnected(&effects), 1);

        // ... then an immediate fresh attempt with a new generation
        assert!(effects.contains(&Effect::StartConnect {
            generation: generation + 1,
            timeout: CONNECT_TIMEOUT
        }));
        assert_eq!(m.state(), ConnState::Connecting);
    }

    #[test]
    fn reconnect_while_waiting_connects_immediately() {
        let mut m = machine();
        let now = t0();
        connect(&mut m, now);
        m.on_event(
            Event::StreamEnded {
                generation: 1,
                error: None,
            },
            now,
        );
        assert_eq!(m.state(), ConnState::WaitingToReconnect);

        let effects = m.on_event(Event::ReconnectRequested, now);
        assert!(matches!(
            effects[0],
            Effect::StartConnect { generation: 2, .. }
        ));

        // the superseded timer tick is ignored
        assert!(m.on_event(Event::ReconnectTimerFired, now).is_empty());
    }

    #[test]
    fn reconnect_while_disconnected_behaves_like_connect() {
        let mut m = machine();
        let now = t0();

        let effects = m.on_event(Event::ReconnectRequested, now);
        assert_eq!(
            effects,
            vec![Effect::StartConnect {
                generation: 1,
                timeout: CONNECT_TIMEOUT
            }]
        );
    }

    #[test]
    fn reconnect_while_connecting_is_ignored() {
        let mut m = machine();
        let now = t0();

        m.on_event(Event::ConnectRequested, now);
        assert!(m.on_event(Event::ReconnectRequested, now).is_empty());
        assert_eq!(m.generation(), 1);
    }

    #[test]
    fn reconnect_resets_the_backoff_counter() {
        let mut m = machine();
        let now = t0();

        // grow the counter with failures
        m.on_event(Event::ConnectRequested, now);
        fail_current_attempt(&mut m, now);
        m.on_event(Event::ReconnectTimerFired, now);
        fail_current_attempt(&mut m, now);

        // explicit reconnect -> fresh sequence -> next failure uses initial backoff
        m.on_event(Event::ReconnectRequested, now);
        let effects = fail_current_attempt(&mut m, now);
        assert_eq!(reconnect_delay(&effects), Some(INITIAL_BACKOFF));
    }
}
