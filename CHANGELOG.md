# actix-ws-client Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

_Changes in the next release_

### Added
- git-tag.py script for creating annotated git tag releases.
- Dependabot for Cargo and GitHub Actions.
- `close_timeout` option: how long a closing connection waits for the server. Default is 5 seconds.
- `Stop` message: closes the connection and stops the actor.
- `tcp_nodelay` option, enabled by default.

### Changed
- **Breaking:** `on_closed`, `on_error` and `on_reconnect_exhausted` of `WebSocketMessageHandler` return a
  `Vec<HandlerAction>`, like `on_text`. Return `Vec::new()` to keep the former behaviour. A handler can now stop
  reconnecting after a fatal error or close status with `HandlerAction::Disconnect`, without an address of its client.
- A connection is closed with a complete close handshake: after the close frame the client keeps reading until the
  server closed the connection, limited by `close_timeout`. This happens in the background, the client is
  disconnected right away.
- `Disconnect` and `Close` resolve once the connection is closed, instead of at once. Use `do_send` to not wait.
- A message that is not written when its connection ends fails with `Error::NotConnected`.
- A client stops when its last address is dropped: it closes its connection gracefully and no longer reconnects.
  Before, a connected or reconnecting client kept running without an address. Keep the address of a client for as
  long as it should run.

- `Connect` resolves with `Ok` when the client is connected already, and joins a running connection attempt instead
  of failing with `Error::StateChanged`. `StateChanged` now only means that the attempt was cancelled by a disconnect.
- `Reconnect` during a connection attempt cancels the attempt and starts a new one with a fresh attempt counter. Before,
  it was ignored.

- `TCP_NODELAY` is set on the connection by default. Before, a small message could be delayed until the previous one
  was acknowledged.
- The addresses of a host name are raced when connecting (RFC 8305) instead of being tried one after another.

- A redirect from `wss://` to a plain text target is refused.
- On a redirect to another origin only `Origin`, `User-Agent`, `Sec-WebSocket-Protocol` and
  `Sec-WebSocket-Extensions` are forwarded. Before, only `Authorization` and `Cookie` were dropped. Another scheme
  or port is another origin now; the default port of the scheme is not.

- A URL that can never be connected is rejected with `Error::InvalidRequest` when the client is created: a scheme
  other than `ws` / `wss`, a missing host, or `wss` without a TLS backend. Before, every connection attempt failed
  and was retried forever.

### Fixed
- A header with several values keeps all of them on a redirect and on a URL refresh. Before, only the first value
  was kept.
- A host name with an address that does not answer, typically IPv6 without a route, no longer uses up the whole
  connect timeout: the next address is tried after 250 ms.
- Every connection attempt uses a new `Sec-WebSocket-Key`. Before, the key of the first attempt was reused.
- An IPv6 address in the URL (`ws://[::1]:8080`) is connected correctly with the rustls backend.
- A stalled socket is dropped when its connection ends. Before, the connection stayed open and pending `SendText` /
  `SendBinary` requests never resolved, also after a keep-alive timeout.
- A disconnected client with a stalled socket stops when all of its addresses are dropped.
- A local close no longer resets the connection while the server is still sending.
- A server ping is answered with one pong instead of two.
- The reply to a close frame of the server is no longer attempted twice, which failed and was logged as a write error.

---

## v0.1.0 - 2026-11-14
### Added
- Initial release.
