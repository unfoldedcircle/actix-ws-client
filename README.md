# actix-ws-client

An actor-based WebSocket client for [Actix](https://actix.rs) with automatic
reconnection (jittered exponential backoff) and ping/pong keep-alive. It is the
WebSocket foundation of the Unfolded Circle Remote Two/3 core service (voice, dock and
integration driver connections).

## Usage

```toml
# Default: TLS with the platform's native library (OpenSSL on Linux)
actix-ws-client = { git = "https://github.com/unfoldedcircle/actix-ws-client", tag = "v0.2.0" }

# rustls, certificates verified against the platform's trust store
actix-ws-client = { git = "https://github.com/unfoldedcircle/actix-ws-client", tag = "v0.2.0", default-features = false, features = ["rustls-platform-verifier"] }
```

## TLS

The TLS backend for `wss://` connections is selected with cargo features:

| Feature                    | Backend                                                                      |
|----------------------------|------------------------------------------------------------------------------|
| `native-tls` (default)     | The platform's TLS library: OpenSSL on Linux, Secure Transport on macOS, SChannel on Windows |
| `rustls-platform-verifier` | [rustls](https://github.com/rustls/rustls); server certificates are verified against the platform's trust store with [rustls-platform-verifier](https://github.com/rustls/rustls-platform-verifier) |

- With `rustls-platform-verifier` the application must install a rustls
  `CryptoProvider` at startup (e.g. `rustls::crypto::aws_lc_rs::default_provider().install_default()`);
  the crate does not choose one. Without a provider, connecting fails with `Error::Tls`.
- If both features are enabled (cargo feature unification), `native-tls` is used
  unless a rustls configuration is passed with `tls_connector`.
- With `default-features = false` and no TLS feature, only plain `ws://` connections
  are possible.

TLS options of `WebSocketConfig`, working with both backends:

- `root_certificates` / `add_root_certificate(Certificate::from_pem(..) | from_der(..))` —
  additional trusted roots on top of the system store: a private certificate authority,
  or the self-signed certificate of a server. Host name verification stays active.
- `danger_accept_invalid_certs` — disables certificate **and** host name verification.
  Development and testing only.
- `tls_connector(TlsConnector::NativeTls(..) | TlsConnector::Rustls(..))` — a ready-made
  connector for full control (client certificates, protocol versions, custom
  verifier). Cannot be combined with the two options above.

## Public API

- `GenericWebSocketClient::<H>::new(id, request, config, handler)` — construct the
  actor; optionally chain `.with_url_provider(...)`; start it with `.start()`.
- Messages: `Connect`, `Disconnect` (close, stop reconnecting), `Close` (like
  `Disconnect`, with a close status code and reason), `Reconnect` (close, connect
  again at once with a fresh attempt counter), `Stop` (close, stop the actor),
  `SendText`, `SendBinary`, `IsConnected`.
- `WebSocketMessageHandler` — callback trait (`on_text`, `on_binary`, `on_connected`,
  `on_closed`, `on_disconnected`, `on_error`, `on_reconnect_exhausted`, `redact_trace`). `on_text` / `on_binary` /
  `on_connected` return a `Vec<HandlerAction>` executed by the client right after the
  callback: `SendText`, `SendBinary`, `Disconnect` / `Close { code, reason, reconnect }`
  (terminal, or reconnect per policy) or `Reconnect` (fresh connection). This lets a handler implement request/reply protocols (e.g. an
  authentication handshake) or disconnect on fatal messages without holding a client
  address — the handler stays a pure, unit-testable function of its input. Send
  actions returned while the connection is no longer up are dropped with a warning.
- `WebSocketConfig` + builder, `Certificate`, `TlsConnector`.
- `Error` — `#[non_exhaustive]`; transport failures are classified as `Tls`,
  `Http { status }` (handshake rejected, e.g. 401), `Io { kind, message }` (refused,
  reset, unreachable) or the generic `WebSocket`.

See the crate-level rustdoc for a quick-start example, and
[`examples/echo_client.rs`](examples/echo_client.rs) for a runnable client.

## Lifecycle

The address returned by `start()` is the handle of the client. The client runs for as
long as an address of it exists; a `Recipient` counts as an address, a `WeakAddr` does
not.

| What happens                         | Effect                                                         |
|--------------------------------------|----------------------------------------------------------------|
| `Disconnect` / `Close`               | Connection closed, no reconnect. The actor keeps running and accepts a new `Connect` |
| `Stop`                               | Connection closed, actor stopped. Other addresses get a closed mailbox |
| Last address dropped                 | Like `Stop`, without a reply                                   |

A client that is stopped closes its connection gracefully, in every state: the close
handshake is completed in the background, a running connection attempt or a pending
reconnect is cancelled, and `on_closed` / `on_disconnected` are called if it was
connected. `Stop` resolves once the connection is closed — await it for a clean
shutdown.

Keep the address also for a client that only receives (`connect_on_start`): a client
without an address stops right after it started.

## Architecture: sans-IO core + thin actor adapter

The crate is split into two layers ([sans-IO](https://sans-io.readthedocs.io/) pattern):

```
              Event                          Effect
 real world ─────────▶  core::Machine  ─────────────▶  actor executes I/O
 (timers,     (pure, sync,                 (spawn tasks, arm timers,
  tasks,       no I/O, no clock)            call handler callbacks)
  mailbox)  ◀───────── returns Vec<Effect> ───────────
```

**`core`** is a pure, synchronous state machine. Its entire surface is
`Machine::on_event(&mut self, event, now) -> Vec<Effect>`. It owns no sockets, tasks,
timers or clock: time is injected per call, backoff jitter comes from an injectable
source. Every decision — connect now, retry in *D*, drop a stale frame, emit a
callback — is returned as data. This is what makes the historically buggy logic fully
unit-testable without a network, an async runtime, or sleeps.

**`actor`** is a thin Actix adapter with no branching policy: every message handler
wraps its input as an `Event`, calls the machine, and executes the returned effects.

### States

`Disconnected → Connecting → Connected → WaitingToReconnect (→ Connecting …)`

There is no dedicated `Closing` state: a graceful disconnect enqueues the Close frame
and hands the connection over to its task in a single transition. The task completes
the close handshake in the background (see [Closing a connection](#closing-a-connection)),
while the machine is `Disconnected` — or already connecting again — right away.

### Events (inputs)

`ConnectRequested`, `DisconnectRequested`, `ConnectSucceeded{gen}`,
`ConnectFailed{gen, error}`, `FrameReceived{gen, frame}`, `StreamEnded{gen, error}`,
`WriteFailed{gen, error}`, `KeepAliveTick{gen}`, `ReconnectTimerFired`.

### Effects (outputs)

`StartConnect{gen, timeout}`, `StartConnectionTask{gen}`,
`SendFrame{frame}`, `TearDownConnection{gen, mode}`, `ArmReconnectTimer{delay}`,
`StartKeepAlive{gen, interval}`, `StopKeepAlive`, `Emit(callback)`,
`CompleteConnect{result}`.

## The generation mechanism

Every connection attempt gets a fresh, strictly increasing `generation: u64`. All
per-connection tasks and timers are tagged with it, and every event they produce
carries it back. The core ignores events from any generation but the current one.

This makes the entire class of "stale resource re-enters the actor" bugs (REVIEW.md
P0-1, P0-2) harmless **by construction**: a late write failure, frame, end-of-stream
or timer tick from a dead connection is simply dropped.

## Connection I/O ownership

One detached **connection task** owns both halves of an established connection for its
entire lifetime. It talks to the actor over channels only and holds no address of it.

- **Writing.** The actor holds only an `mpsc::Sender`; sending is a synchronous channel
  push (never `await` in a handler), ordering is FIFO. Send completions are delivered
  through per-message oneshot channels carried alongside the frame.
- **Reading.** Received frames are reported over a bounded channel to an actor-owned
  future, which is cancelled via its `SpawnHandle` on teardown. A slow handler slows
  down reading instead of growing a queue. No `ctx.add_stream`.
- **Teardown.** The core decides how a connection ends, the task carries it out on its
  own while the actor moves on, see the next section.

## Closing a connection

| Cause                                              | Teardown                                    |
|----------------------------------------------------|---------------------------------------------|
| `Disconnect` / `Close` / `Reconnect`, actor stopped | Close handshake started by the client       |
| Close frame of the server                          | Close handshake started by the server       |
| Read or write error, keep-alive timeout            | Connection dropped at once                  |

**Close handshake started by the client.** Messages sent before are still written,
followed by the Close frame. The client then keeps reading — received messages are
discarded — until the server answered the Close frame and closed the connection.

**Close handshake started by the server.** The transport answers the Close frame with
the status of the server. Nothing else can be written anymore. The client keeps
reading until the server closed the connection.

Waiting for the server is limited by `close_timeout`: after that, the connection is
dropped. The wait happens in the background. The client is disconnected right away
(`on_closed` / `on_disconnected` are called, sending fails with `NotConnected`), and a
reconnect is not delayed by a server that is slow to close.

`Disconnect` and `Close` resolve once the connection is closed. Await them for a clean
shutdown before the process exits; use `do_send` to not wait.

**Connection dropped at once.** A broken connection is not closed gracefully: there is
nobody to complete a handshake with (RFC 6455 §7.1.7 "Fail the WebSocket Connection").

Whenever a connection ends, the messages that are not written yet fail with
`Error::NotConnected` — a `SendText` / `SendBinary` never stays pending on a
connection that is gone.

## Reconnection & backoff

- Delay for retry *n* (1-based): `initial_backoff * backoff_factor^(n-1)`, capped at
  `max_backoff`, then reduced by up to 20% jitter (`delay * (1.0 - j * 0.2)`,
  `j ∈ [0, 1)` from `fastrand`).
- The attempt counter resets only after a connection stayed up for at least
  `min_stable_uptime` — a flapping link keeps growing its backoff instead of hammering
  the server (P1-2).
- A user-initiated `Connect` or `Reconnect` starts a fresh backoff sequence.
- `Connect` resolves with the result of the connection attempt: `Ok` once connected
  (at once if the client is connected already), the error of a failed attempt, or
  `Error::StateChanged` if the attempt was cancelled by a disconnect. A request while
  an attempt is running joins that attempt — also the one of `connect_on_start`.
- `Reconnect` connects again at once, in every state: it closes an established
  connection, skips a pending backoff delay, and replaces a running connection
  attempt (the URL provider is asked again). Use it after an address change, or when
  the network came back.
- `Disconnect` during `WaitingToReconnect` cancels the timer; a racing timer tick is
  ignored by the core.
- **Attempt cap:** with `max_reconnect_attempts = Some(n)`, exactly `n` consecutive
  reconnection attempts are made; when the `n`-th fails, the client stops, calls
  `on_reconnect_exhausted` once and goes `Disconnected`. Default is unlimited.
- **Per-attempt URL refresh:** an optional `with_url_provider(FnMut() -> Option<String>)`
  hook is polled before every attempt. `Some(url)` swaps the request URL (headers
  preserved), `None` keeps the current one, an invalid URL fails the attempt with
  `InvalidRequest` and follows the normal retry policy. Consumers with their own
  DNS/IP caching use this to refresh the target between attempts.

## Keep-alive

Single interval timer, evaluated on the tick (no second monitor timer, no moving
reference instant — P1-1): if the previous ping is unanswered, the missed counter
increments; at `max_missed_pongs` the connection is dropped **without** a Close
handshake (RFC 6455 §7.1.7 "Fail the WebSocket Connection") and reconnection starts.
The detection budget is exactly `max_missed_pongs * ping_interval` after the first
unanswered ping. A ping counts as answered when **any** frame is received after it,
not only its pong: a peer that keeps sending data is alive. Server pings are answered
by the transport with a matching pong.

For servers with an application-level heartbeat, set `ping_enabled(false)`: no ping
frames are sent and no keep-alive timer runs. The consumer then sends its own
heartbeat messages and requests `HandlerAction::Reconnect` (or sends `Reconnect`) when
the server stops answering.

## Send queue

Messages are queued while the socket cannot take them as fast as they are sent. By
default the queue is unbounded. With `max_send_queue(n)`, at most `n` messages wait in
the queue; a further `SendText` / `SendBinary` fails immediately with
`Error::SendQueueFull` (a handler action is dropped with a warning). Internal control
frames (ping, pong, close) are never rejected.

## Message tracing

`msg_tracing_in` / `msg_tracing_out` log every received / sent text message at debug
level. To keep secrets out of the log, implement
`WebSocketMessageHandler::redact_trace(direction, text)`: return the text to log, a
redacted copy, or `None` to skip the message. Only the log output is affected.

## Callback contract

`on_connected` / `on_disconnected` are **edge-triggered and balanced**: `on_connected`
fires exactly once per established connection, `on_disconnected` exactly once when
that connection ends — never duplicated, never unpaired (P2-3). A failed connect
attempt emits `on_error`, not `on_disconnected`. `SendText` / `SendBinary` while not
connected fail immediately with `Error::NotConnected`.

## Close status code and reason

**Sending.** `Disconnect` closes with status 1000 (normal closure) and no reason. The
`Close::new(code, reason)` message and `HandlerAction::Close { code, reason }` close
with a specific status, e.g. 1003 (unsupported data), 1008 (policy violation) or an
application code 4000–4999. Both stop reconnecting, like `Disconnect` — unless
`Close::with_reconnect()` / `reconnect: true` is set: then the client reconnects
according to the reconnect policy, as if the connection had been lost (backoff,
flapping protection, attempt cap). That is the way to give up on a broken session
without hammering the server; `Reconnect` in contrast reconnects immediately with a
fresh attempt counter. A status that
must not be sent (RFC 6455 §7.4, e.g. 1005, 1006) or a reason longer than 123 bytes is
rejected: the message returns `Error::InvalidRequest` and leaves the connection
untouched; the handler action logs a warning and closes with 1000.

**Receiving.** `on_closed(&DisconnectReason)` is called once per ended connection,
directly before `on_disconnected`:

| `DisconnectReason`                | Cause                                                    |
|-----------------------------------|----------------------------------------------------------|
| `ClosedByServer { code, reason }` | Close frame of the server (`code` is `None` without a status) |
| `ClosedLocally`                   | `Disconnect` / `Close` / `Reconnect`, or the actor stopped |
| `KeepAliveTimeout`                | The server stopped answering pings                       |
| `Error(error)`                    | Read or write failure                                    |
| `StreamEnded`                     | Connection ended without a close frame and without an error |

The reconnect policy does not depend on the reason: after a server close the client
reconnects as before. A consumer that must not reconnect after a specific status
(e.g. an application code for "access revoked") sends `Disconnect` from its
`on_closed` handling.

## Configuration

| Knob                | Default | Meaning                                           |
|---------------------|---------|---------------------------------------------------|
| `ping_enabled`      | true    | Send keep-alive ping frames                       |
| `ping_interval`     | 30 s    | Keep-alive ping interval                          |
| `max_missed_pongs`  | 2       | Consecutive unanswered pings before reconnecting  |
| `connect_timeout`   | 10 s    | Per-attempt connect timeout (including redirects) |
| `close_timeout`     | 5 s     | How long a closing connection waits for the server |
| `max_redirects`     | 5       | HTTP redirects followed on connect                |
| `connect_on_start`  | false   | Connect as soon as the actor starts               |
| `initial_backoff`   | 500 ms  | First retry delay                                 |
| `max_backoff`       | 30 s    | Backoff cap                                       |
| `backoff_factor`    | 2.0     | Backoff growth factor (>= 1.0)                    |
| `min_stable_uptime` | 30 s    | Uptime required before the retry counter resets   |
| `max_reconnect_attempts` | None | Reconnect attempt cap (`None` = unlimited, `Some(0)` = never reconnect) |
| `max_message_size`  | None    | Inbound message size limit (`None` = transport default, 64 MiB) |
| `max_frame_size`    | None    | Inbound frame size limit (`None` = transport default, 16 MiB) |
| `danger_accept_invalid_certs` | false | **Danger:** accept invalid TLS certs & hostnames on `wss://` (dev/test only) |
| `root_certificates` | empty   | Additional trusted root certificates for `wss://` |
| `tls_connector`     | None    | Ready-made TLS connector (replaces the two options above) |
| `max_send_queue`    | None    | Limit of messages waiting to be written (`None` = unlimited) |
| `msg_tracing_in`    | false   | Log received text messages at debug level         |
| `msg_tracing_out`   | false   | Log sent text messages at debug level (with ms since previous send) |

The former `pong_timeout` knob was removed: with tick-based keep-alive it had no
effect. The builder validates `backoff_factor >= 1.0`, `max_missed_pongs >= 1` and
non-zero `ping_interval` / `initial_backoff` / `max_send_queue`.

Redirects support absolute, scheme-relative and absolute-path `Location` values;
`Authorization` / `Cookie` headers are not forwarded on cross-origin redirects.

## Testing

| Layer             | Location                      | Covers                                                                                                                         |
|-------------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Pure unit tests   | `src/core/*` (`#[cfg(test)]`) | All state transitions, generation filtering, keep-alive budget, backoff math, flapping protection, callback balance            |
| Connection task   | `src/actor/connection.rs` tests | Teardown of a connection against mock transport halves: abort, close handshake, close timeout, stalled socket                |
| Actor integration | `src/actor/mod.rs` tests      | Effect wiring against a mock `Transport` (no TCP): reconnect, disconnect during reconnect, write-failure recovery, close frame |
| End-to-end        | `tests/e2e.rs`                | Real local server: echo roundtrip, close handshake, ping/pong, HTTP redirect, unreachable host                                 |
| End-to-end TLS    | `tests/tls.rs`                | Real local `wss://` server with a private CA: rejection of an untrusted certificate, trusted roots, disabled verification, ready-made connectors |

Run with `cargo test`. The TLS tests run against the backend the crate is built with:

```sh
cargo test                                                                # native-tls
cargo test --no-default-features --features rustls-platform-verifier      # rustls
cargo test --no-default-features                                          # no TLS
```

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the
[tags on this repository](https://github.com/unfoldedcircle/actix-ws-client/tags).

The major changes found in each new release are listed in the [changelog](./CHANGELOG.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
