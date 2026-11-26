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

### Changed
- A connection is closed with a complete close handshake: after the close frame the client keeps reading until the
  server closed the connection, limited by `close_timeout`. This happens in the background, the client is
  disconnected right away.
- `Disconnect` and `Close` resolve once the connection is closed, instead of at once. Use `do_send` to not wait.
- A message that is not written when its connection ends fails with `Error::NotConnected`.

### Fixed
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
