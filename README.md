# actix-ws-client

An actor-based WebSocket client for [Actix](https://actix.rs) with automatic
reconnection (jittered exponential backoff) and ping/pong keep-alive. It is the
WebSocket foundation of the Unfolded Circle Remote Two/3 core service (voice, dock and
integration driver connections).

## Public API

- `GenericWebSocketClient::<H>::new(id, request, config, handler)` — construct the
  actor; optionally chain `.with_url_provider(...)`; start it with `.start()`.
- Messages: `Connect`, `Disconnect` (close, stop reconnecting), `Reconnect` (close,
  reconnect per policy with a fresh attempt counter), `SendText`, `SendBinary`,
  `IsConnected`.
- `WebSocketMessageHandler` — callback trait (`on_text`, `on_binary`, `on_connected`,
  `on_disconnected`, `on_error`, `on_reconnect_exhausted`). `on_text` / `on_binary` /
  `on_connected` return a `Vec<HandlerAction>` executed by the client right after the
  callback: `SendText`, `SendBinary`, `Disconnect` (terminal) or `Reconnect` (fresh
  connection). This lets a handler implement request/reply protocols (e.g. an
  authentication handshake) or disconnect on fatal messages without holding a client
  address — the handler stays a pure, unit-testable function of its input. Send
  actions returned while the connection is no longer up are dropped with a warning.
- `WebSocketConfig` + builder, `Error`.

See the crate-level rustdoc for a quick-start example, and
[`examples/echo_client.rs`](examples/echo_client.rs) for a runnable client.

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

There is no dedicated `Closing` state (the task plan listed one): a graceful
disconnect enqueues the Close frame and tears down in a single transition, because the
writer task drains its queue before terminating.

### Events (inputs)

`ConnectRequested`, `DisconnectRequested`, `ConnectSucceeded{gen}`,
`ConnectFailed{gen, error}`, `FrameReceived{gen, frame}`, `StreamEnded{gen, error}`,
`WriteFailed{gen, error}`, `KeepAliveTick{gen}`, `ReconnectTimerFired`.

### Effects (outputs)

`StartConnect{gen, timeout}`, `StartWriterTask{gen}`, `StartReaderTask{gen}`,
`SendFrame{frame}`, `TearDownConnection{gen}`, `ArmReconnectTimer{delay}`,
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

- **Writer task** owns the sink for the connection's lifetime. The actor holds only an
  `mpsc::Sender`; sending is a synchronous channel push (never `await` in a handler),
  ordering is FIFO. Teardown = drop the sender: the task drains its queue (a final
  Close frame still reaches the wire) and ends, dropping the sink. Send completions
  are delivered through per-message oneshot channels carried alongside the frame (the
  task plan's `CompleteSend` registry turned out unnecessary).
- **Reader task** owns the stream and forwards frames to the actor. It is an
  actor-owned future, cancelled via its `SpawnHandle` on teardown. No `ctx.add_stream`.

## Reconnection & backoff

- Delay for retry *n* (1-based): `initial_backoff * backoff_factor^(n-1)`, capped at
  `max_backoff`, then reduced by up to 20% jitter (`delay * (1.0 - j * 0.2)`,
  `j ∈ [0, 1)` from `fastrand`).
- The attempt counter resets only after a connection stayed up for at least
  `min_stable_uptime` — a flapping link keeps growing its backoff instead of hammering
  the server (P1-2).
- A user-initiated `Connect` or `Reconnect` starts a fresh backoff sequence.
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
unanswered ping. Server pings are answered explicitly with a matching pong: with a
split sink/stream, the transport's automatic pong would only be flushed on our next
write (P2-4).

## Callback contract

`on_connected` / `on_disconnected` are **edge-triggered and balanced**: `on_connected`
fires exactly once per established connection, `on_disconnected` exactly once when
that connection ends — never duplicated, never unpaired (P2-3). A failed connect
attempt emits `on_error`, not `on_disconnected`. `SendText` / `SendBinary` while not
connected fail immediately with `Error::NotConnected`.

## Configuration

| Knob                | Default | Meaning                                           |
|---------------------|---------|---------------------------------------------------|
| `ping_interval`     | 30 s    | Keep-alive ping interval                          |
| `max_missed_pongs`  | 2       | Consecutive unanswered pings before reconnecting  |
| `connect_timeout`   | 10 s    | Per-attempt connect timeout (including redirects) |
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
| `msg_tracing_in`    | false   | Log received text messages at debug level         |
| `msg_tracing_out`   | false   | Log sent text messages at debug level (with ms since previous send) |

The former `pong_timeout` knob was removed: with tick-based keep-alive it had no
effect. The builder validates `backoff_factor >= 1.0`, `max_missed_pongs >= 1` and
non-zero `ping_interval` / `initial_backoff`.

Redirects support absolute, scheme-relative and absolute-path `Location` values;
`Authorization` / `Cookie` headers are not forwarded on cross-origin redirects.

## Testing

| Layer             | Location                      | Covers                                                                                                                         |
|-------------------|-------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Pure unit tests   | `src/core/*` (`#[cfg(test)]`) | All state transitions, generation filtering, keep-alive budget, backoff math, flapping protection, callback balance            |
| Actor integration | `src/actor/mod.rs` tests      | Effect wiring against a mock `Transport` (no TCP): reconnect, disconnect during reconnect, write-failure recovery, close frame |
| End-to-end        | `tests/e2e.rs`                | Real local server: echo roundtrip, close handshake, HTTP redirect, unreachable host                                            |

Run with `cargo test`.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
