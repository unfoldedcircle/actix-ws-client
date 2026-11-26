//! WebSocket client configuration.

use crate::tls::Certificate;
#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
use crate::tls::TlsConnector;
use derive_builder::Builder;
use std::time::Duration;

const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Shortest accepted reconnect delay: anything below makes the client spin.
const MIN_BACKOFF: Duration = Duration::from_millis(1);

/// Configuration for WebSocket connection, reconnection and keep-alive handling.
///
/// Use [`WebSocketConfig::builder()`] to create a new configuration.
#[derive(Clone, Debug, Builder)]
#[builder(
    pattern = "owned",
    setter(into, strip_option),
    build_fn(validate = "Self::validate")
)]
pub struct WebSocketConfig {
    /// Whether the client sends WebSocket ping frames to detect a dead connection.
    ///
    /// Disable it for servers that require an application-level heartbeat instead:
    /// the consumer then sends its own heartbeat messages and requests a
    /// [`crate::HandlerAction::Reconnect`] or sends [`crate::Reconnect`] when the
    /// server stops answering. Pings of the server are answered in any case.
    ///
    /// Default is true.
    #[builder(default = "true")]
    pub(crate) ping_enabled: bool,
    /// Interval between pings sent by the client.
    ///
    /// Default is 30 seconds.
    #[builder(default = "Duration::from_secs(30)")]
    pub(crate) ping_interval: Duration,
    /// Number of consecutive unanswered pings before triggering a reconnect.
    ///
    /// A ping counts as answered when any frame is received after it, not only its
    /// pong. The keep-alive detection budget is `max_missed_pongs * ping_interval`.
    ///
    /// Default is 2.
    #[builder(default = "2")]
    pub(crate) max_missed_pongs: u32,
    /// Maximum number of HTTP redirects to follow on connect.
    ///
    /// Default is 5.
    #[builder(default = "5")]
    pub(crate) max_redirects: u8,
    /// Whether to automatically connect when the actor starts.
    ///
    /// If true, the client will initiate a connection as soon as the actor is started,
    /// without needing an explicit [`crate::Connect`] message.
    ///
    /// Default is false.
    #[builder(default = "false")]
    pub(crate) connect_on_start: bool,

    /// Timeout for a single connection attempt (including redirects).
    ///
    /// Must not be zero. Default is 10 seconds.
    #[builder(default = "Duration::from_secs(10)")]
    pub(crate) connect_timeout: Duration,

    /// Whether Nagle's algorithm is disabled on the connection (`TCP_NODELAY`).
    ///
    /// The client writes every message at once. With Nagle's algorithm, a small
    /// message is held back while the previous one is not acknowledged yet, which can
    /// delay it by 40 ms and more. Disabling the algorithm sends every message
    /// immediately; enable it only to trade latency for fewer packets.
    ///
    /// Default is true.
    #[builder(default = "true")]
    pub(crate) tcp_nodelay: bool,

    /// How long a closing connection waits for the server.
    ///
    /// A connection is closed with a close handshake: after the close frame was sent
    /// ([`crate::Disconnect`], [`crate::Close`], [`crate::Reconnect`]) or received,
    /// the client waits until the server closed the connection. If that takes longer
    /// than this timeout, the connection is dropped; messages that could not be
    /// written until then fail with [`crate::Error::NotConnected`].
    ///
    /// The wait happens in the background: the client is disconnected right away and
    /// a reconnect is not delayed. `Duration::ZERO` does not wait for the server.
    ///
    /// Default is 5 seconds.
    #[builder(default = "Duration::from_secs(5)")]
    pub(crate) close_timeout: Duration,

    /// Minimum time a connection must stay up before the reconnection attempt counter
    /// is reset to zero.
    ///
    /// This prevents a flapping connection (connect, then drop almost immediately) from
    /// restarting the backoff sequence at the minimum delay on every short-lived success.
    ///
    /// Default is 30 seconds.
    #[builder(default = "Duration::from_secs(30)")]
    pub(crate) min_stable_uptime: Duration,

    /// The initial backoff duration for the retry connection logic.
    ///
    /// Must be at least 1 millisecond. Default is 500 milliseconds.
    #[builder(default = "DEFAULT_INITIAL_BACKOFF")]
    pub(crate) initial_backoff: Duration,

    /// The maximum backoff duration for the retry connection logic.
    ///
    /// Must not be below `initial_backoff`. Default is 30 seconds.
    #[builder(default = "DEFAULT_MAX_BACKOFF")]
    pub(crate) max_backoff: Duration,

    /// The factor by which the backoff duration is multiplied after each failed attempt.
    ///
    /// Default is 2.0.
    #[builder(default = "2.0")]
    pub(crate) backoff_factor: f64,

    /// Maximum number of consecutive reconnection attempts.
    ///
    /// `None` (default) retries forever. When the cap is reached, the client stops
    /// reconnecting and calls
    /// [`crate::WebSocketMessageHandler::on_reconnect_exhausted`]; a new
    /// [`crate::Connect`] or [`crate::Reconnect`] message starts a fresh attempt
    /// sequence. The counter resets after a connection stayed up for
    /// `min_stable_uptime`. `Some(0)` disables reconnection entirely.
    #[builder(default)]
    pub(crate) max_reconnect_attempts: Option<u32>,

    /// The maximum size of an incoming message (complete payload, possibly multiple
    /// frames).
    ///
    /// A larger message is not delivered: the connection is dropped with an error and
    /// the client reconnects. The limit also bounds the memory a single message of the
    /// server can take. A text message is copied once when it is delivered, so it
    /// takes up to twice its size for a moment.
    ///
    /// Must not be zero. `None` (default) uses the transport default of 64 MiB, which
    /// is generous for a device with little memory: set a limit that fits the
    /// largest message of the application.
    #[builder(default)]
    pub(crate) max_message_size: Option<usize>,

    /// The maximum size of a single incoming message frame payload.
    ///
    /// Must not be zero, and not above `max_message_size` if both are set. `None`
    /// (default) uses the transport default of 16 MiB.
    #[builder(default)]
    pub(crate) max_frame_size: Option<usize>,

    /// Maximum number of messages waiting to be written to the socket.
    ///
    /// Messages are queued while the socket cannot take them as fast as they are
    /// sent (slow network or peer). With a limit, a message that does not fit is
    /// rejected with [`crate::Error::SendQueueFull`] instead of growing the queue —
    /// useful for streamed data such as audio, where stale data is worthless.
    /// Internal control frames (ping, close) are never rejected, but they take a
    /// place in the queue while they wait.
    ///
    /// The limit counts messages, not bytes, and not the message that is being
    /// written. `None` (default) does not limit the queue: it then grows for as long
    /// as the application sends faster than the socket takes the data. The queue is
    /// dropped with its connection — for example when the keep-alive timeout ends a
    /// connection that stopped taking data.
    #[builder(default)]
    pub(crate) max_send_queue: Option<usize>,

    /// **Danger:** accept invalid TLS certificates and hostnames on `wss://` connections.
    ///
    /// Disables server certificate verification entirely (certificate chain *and*
    /// hostname) — intended for development and testing against self-signed
    /// certificates only. Has no effect on plain `ws://` connections.
    ///
    /// Works with every TLS backend.
    ///
    /// Default is false (full verification against the system CA store).
    #[builder(default = "false")]
    #[cfg_attr(
        not(any(feature = "native-tls", feature = "rustls-platform-verifier")),
        allow(dead_code)
    )]
    pub(crate) danger_accept_invalid_certs: bool,

    /// Additional trusted root certificates for `wss://` connections, on top of the
    /// system CA store.
    ///
    /// Use it for a private certificate authority or the self-signed certificate of a
    /// server. See also [`WebSocketConfigBuilder::add_root_certificate`].
    ///
    /// Default is empty.
    #[builder(default)]
    #[cfg_attr(
        not(any(feature = "native-tls", feature = "rustls-platform-verifier")),
        allow(dead_code)
    )]
    pub(crate) root_certificates: Vec<Certificate>,

    /// A ready-made TLS configuration for `wss://` connections.
    ///
    /// Replaces the connector the client builds itself, for full control over the TLS
    /// setup (client certificates, protocol versions, a custom verifier, ...). Cannot
    /// be combined with `danger_accept_invalid_certs` or `root_certificates`.
    ///
    /// Default is `None`.
    #[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
    #[builder(default)]
    pub(crate) tls_connector: Option<TlsConnector>,

    /// Log every received text message at debug level.
    ///
    /// Messages can be redacted or skipped with
    /// [`crate::WebSocketMessageHandler::redact_trace`].
    ///
    /// Default is false.
    #[builder(default = "false")]
    pub(crate) msg_tracing_in: bool,

    /// Log every sent text message at debug level, with the elapsed time since the
    /// previous sent message.
    ///
    /// Messages can be redacted or skipped with
    /// [`crate::WebSocketMessageHandler::redact_trace`].
    ///
    /// Default is false.
    #[builder(default = "false")]
    pub(crate) msg_tracing_out: bool,
}

impl WebSocketConfig {
    /// Returns a new builder for [`WebSocketConfig`].
    pub fn builder() -> WebSocketConfigBuilder {
        WebSocketConfigBuilder::default()
    }
}

impl WebSocketConfigBuilder {
    /// Adds a trusted root certificate for `wss://` connections, see
    /// `root_certificates`.
    pub fn add_root_certificate(mut self, certificate: Certificate) -> Self {
        self.root_certificates
            .get_or_insert_with(Vec::new)
            .push(certificate);
        self
    }

    fn validate(&self) -> Result<(), String> {
        if let Some(factor) = self.backoff_factor
            && (factor < 1.0 || !factor.is_finite())
        {
            return Err(format!("backoff_factor must be >= 1.0, got {factor}"));
        }
        if let Some(max_missed) = self.max_missed_pongs
            && max_missed == 0
        {
            return Err("max_missed_pongs must be >= 1".into());
        }
        if let Some(interval) = self.ping_interval
            && interval.is_zero()
        {
            return Err("ping_interval must be > 0".into());
        }
        if self.max_send_queue == Some(Some(0)) {
            return Err("max_send_queue must be >= 1".into());
        }
        // Without a delay between the attempts the client spins.
        let initial = self.initial_backoff.unwrap_or(DEFAULT_INITIAL_BACKOFF);
        let max = self.max_backoff.unwrap_or(DEFAULT_MAX_BACKOFF);
        if initial < MIN_BACKOFF {
            return Err(format!(
                "initial_backoff must be at least {MIN_BACKOFF:?}, got {initial:?}"
            ));
        }
        if max < initial {
            return Err(format!(
                "max_backoff ({max:?}) must not be below initial_backoff ({initial:?})"
            ));
        }
        if let Some(timeout) = self.connect_timeout
            && timeout.is_zero()
        {
            return Err("connect_timeout must be > 0".into());
        }
        // A limit of zero rejects every message.
        if self.max_message_size == Some(Some(0)) {
            return Err("max_message_size must be >= 1".into());
        }
        if self.max_frame_size == Some(Some(0)) {
            return Err("max_frame_size must be >= 1".into());
        }
        if let (Some(Some(frame)), Some(Some(message))) =
            (self.max_frame_size, self.max_message_size)
            && frame > message
        {
            return Err(format!(
                "max_frame_size ({frame}) must not exceed max_message_size ({message})"
            ));
        }
        #[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
        if matches!(self.tls_connector, Some(Some(_)))
            && (self.danger_accept_invalid_certs == Some(true)
                || self
                    .root_certificates
                    .as_ref()
                    .is_some_and(|c| !c.is_empty()))
        {
            return Err(
                "tls_connector cannot be combined with danger_accept_invalid_certs or root_certificates"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn builder() -> WebSocketConfigBuilder {
        WebSocketConfig::builder()
    }

    /// The reason the builder gives for rejecting a configuration.
    fn rejected(builder: WebSocketConfigBuilder) -> String {
        builder
            .build()
            .expect_err("the configuration must be rejected")
            .to_string()
    }

    #[test]
    fn default_configuration_is_valid() {
        let config = builder().build().unwrap();
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.initial_backoff, Duration::from_millis(500));
        assert_eq!(config.max_backoff, Duration::from_secs(30));
        assert_eq!(config.max_send_queue, None);
        assert_eq!(config.max_message_size, None);
    }

    #[test]
    fn existing_rules_are_enforced() {
        assert!(rejected(builder().backoff_factor(0.5)).contains("backoff_factor"));
        assert!(rejected(builder().backoff_factor(f64::NAN)).contains("backoff_factor"));
        assert!(rejected(builder().max_missed_pongs(0u32)).contains("max_missed_pongs"));
        assert!(rejected(builder().ping_interval(Duration::ZERO)).contains("ping_interval"));
        assert!(rejected(builder().max_send_queue(0usize)).contains("max_send_queue"));
        assert!(rejected(builder().initial_backoff(Duration::ZERO)).contains("initial_backoff"));
    }

    /// Without a delay between the attempts the client spins.
    #[test]
    fn backoff_that_gives_no_delay_is_rejected() {
        assert!(rejected(builder().max_backoff(Duration::ZERO)).contains("max_backoff"));
        assert!(
            rejected(builder().initial_backoff(Duration::from_micros(999)))
                .contains("initial_backoff")
        );
        assert!(
            builder()
                .initial_backoff(Duration::from_millis(1))
                .build()
                .is_ok()
        );
    }

    /// The cap would silently replace the initial delay.
    #[test]
    fn max_backoff_below_the_initial_backoff_is_rejected() {
        let config = builder()
            .initial_backoff(Duration::from_secs(5))
            .max_backoff(Duration::from_secs(1));
        assert!(rejected(config).contains("max_backoff"));

        let equal = builder()
            .initial_backoff(Duration::from_secs(5))
            .max_backoff(Duration::from_secs(5));
        assert!(equal.build().is_ok());
        // The default cap also counts.
        assert!(
            rejected(builder().initial_backoff(Duration::from_secs(31))).contains("max_backoff")
        );
    }

    /// Every attempt would time out.
    #[test]
    fn zero_connect_timeout_is_rejected() {
        assert!(rejected(builder().connect_timeout(Duration::ZERO)).contains("connect_timeout"));
    }

    /// Every message would be rejected, followed by a reconnect.
    #[test]
    fn zero_size_limits_are_rejected() {
        assert!(rejected(builder().max_message_size(0usize)).contains("max_message_size"));
        assert!(rejected(builder().max_frame_size(0usize)).contains("max_frame_size"));
    }

    #[test]
    fn frame_limit_above_the_message_limit_is_rejected() {
        let config = builder()
            .max_message_size(1024usize)
            .max_frame_size(2048usize);
        assert!(rejected(config).contains("max_frame_size"));

        assert!(
            builder()
                .max_message_size(2048usize)
                .max_frame_size(1024usize)
                .build()
                .is_ok()
        );
        // One limit alone is fine: the other one keeps the transport default.
        assert!(builder().max_message_size(1024usize).build().is_ok());
    }

    /// Not waiting for the server is a valid choice.
    #[test]
    fn zero_close_timeout_is_accepted() {
        assert!(builder().close_timeout(Duration::ZERO).build().is_ok());
    }
}
