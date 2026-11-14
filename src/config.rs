//! WebSocket client configuration.

use crate::tls::Certificate;
#[cfg(any(feature = "native-tls", feature = "rustls-platform-verifier"))]
use crate::tls::TlsConnector;
use derive_builder::Builder;
use std::time::Duration;

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
    /// Default is 10 seconds.
    #[builder(default = "Duration::from_secs(10)")]
    pub(crate) connect_timeout: Duration,

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
    /// Default is 500 milliseconds.
    #[builder(default = "Duration::from_millis(500)")]
    pub(crate) initial_backoff: Duration,

    /// The maximum backoff duration for the retry connection logic.
    ///
    /// Default is 30 seconds.
    #[builder(default = "Duration::from_secs(30)")]
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
    /// `None` (default) uses the transport default of 64 MiB.
    #[builder(default)]
    pub(crate) max_message_size: Option<usize>,

    /// The maximum size of a single incoming message frame payload.
    ///
    /// `None` (default) uses the transport default of 16 MiB.
    #[builder(default)]
    pub(crate) max_frame_size: Option<usize>,

    /// Maximum number of messages waiting to be written to the socket.
    ///
    /// Messages are queued while the socket cannot take them as fast as they are
    /// sent (slow network or peer). With a limit, a message that does not fit is
    /// rejected with [`crate::Error::SendQueueFull`] instead of growing the queue —
    /// useful for streamed data such as audio, where stale data is worthless.
    /// Internal control frames (ping, pong, close) are never rejected.
    ///
    /// `None` (default) does not limit the queue.
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
        if let Some(initial) = self.initial_backoff
            && initial.is_zero()
        {
            return Err("initial_backoff must be > 0".into());
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
