# Changelog

## Standalone repository - 2026-10-20

- Published the SDK source in a separate S7 Works repository, without the HumChat server or its repository history.
- Adapted development, test and release-artifact tooling to the repository root. Real-server end-to-end harnesses remain in the upstream server repository.
- Kept runtime version `0.1.0-alpha.6`, npm publication disabled and licensing pending (`UNLICENSED`). Public source visibility does not grant an open-source license.

## 0.1.0-alpha.6 - 2026-10-20

- Added administrator-managed outgoing webhook subscriptions, delivery inspection, pause/resume, signing-secret rotation and explicit dead-letter replay on upgraded servers.
- Added raw-body HMAC verification with timestamp and event-scope checks, plus a persistent deduplicating receiver example.
- Management mutations use a single request attempt because creation/rotation secrets are returned once; uncertain outcomes require reconciliation.
- Existing WebSocket transport and persistence formats remain compatible. The npm package remains unpublished; production machine authentication and staging acceptance remain open.

## 0.1.0-alpha.5 - 2026-10-20

- Added opt-in `realtime.transport: 'multiplex'`, negotiated through an optional capability, for one physical WebSocket across explicit channels. The per-channel transport remains the default.
- Preserved independent channel replay, checkpoints, cancellation barriers and callback ordering while sharing connection backoff, heartbeat and token rotation.
- Added correlated subscription lifecycle, bounded admission/buffers and channel-local failures; permanent stream failures and session revocation affect all registrations.
- Fixed a durable replay-cancellation race and bounded-buffer retry loops. Added controlled and installed-package coverage plus a persistent multiplex worker example.
- Requires the new server `/ws/events` route for multiplex mode. Persistence formats are unchanged. Machine authentication, staging acceptance and npm publication remained open at this version.

## 0.1.0-alpha.4 — 2026-10-19

- Added installed-package guides for authentication, API methods/events, recovery, migration, troubleshooting and release/support status.
- Added CommonJS and multi-channel examples with package-consumer verification.
- Added local release preparation that checks packed paths, documentation links, known credential markers and source/artifact hashes, then tests the exact tarball in independent consumers.
- Added a manual cross-platform artifact workflow. It does not publish; npm ownership/access, licensing/support and production acceptance remain open.
- Corrected the recovery-mode declaration comment. Runtime behavior and wire protocols are unchanged from alpha.3.

## 0.1.0-alpha.3 — 2026-10-19

- Prevented automatic reconnect from invoking an acknowledged consumer while its cancelled previous invocation still runs; its original deadline now blocks uncooperative work without acknowledging it.
- Preserved permanent WebSocket handshake denials instead of masking them with later network errors; released durable connection slots before slow checkpoint/replay work.
- Added regressions for large backlogs, concurrent-channel buffer pressure, checkpoint/close races and actual file-store process crashes.
- Hardened server replay authorization after database waits and enabled granted guests to discover SDK capabilities and replay their allowed channels.
- Added local two-server signed-session checks, channel/load/outage measurements and a cross-platform package CI matrix in the upstream repository. Staging machine authentication and full pilot acceptance still gate a release candidate.

## 0.1.0-alpha.2 — 2026-10-19

- Added negotiated durable event v1 replay and default durable recovery on upgraded servers; older servers require explicit best-effort mode.
- Added `events.sync()`, typed durable envelopes, acknowledged `consume()`, scoped checkpoints and explicit expired-cursor errors.
- Added memory/file checkpoint and outbox stores, CAS, exclusive file locks, bounded atomic writes, `outbox.send()`/`flush()`, and a persistent responder example.
- Added server transactional event append across message producers, per-channel commit ordering, captured replay watermarks, epoch/floor validation and conservative retention.
- Added controlled crash/reconnect/storage tests and upstream real Rust/PostgreSQL rollback/restart coverage. Machine authentication and npm publication remained pending.

## 0.1.0-alpha.1 — 2026-10-19

- Added typed per-channel WebSocket subscriptions, shared handles, thread replies, explicit typing/read signals, and a runnable `!status` responder.
- Added bounded queues/caches, reconnect backoff, ping/pong, shared token refresh and socket rotation, revocation handling, and connection metrics.
- Added explicitly opted-in best-effort history/state reconciliation, revision-aware deduplication, and incomplete-coverage events. Durable replay remains unsupported.
- Added controlled-peer fault tests, real Rust-server HTTP/WebSocket tests, and installed-package checks for both module formats.
- Added pinned `ws` runtime dependency. Production machine authentication and npm publication remained pending.

## 0.1.0-alpha.0 — 2026-10-19

- Initial HTTP preview with supplied-token authentication and verified organization/actor binding.
- Typed channel/conversation discovery, send/reply, message state/history, edit/delete and desired-state reactions.
- Bounded retries, stable intent IDs, cancellation, token-refresh coordination, and typed errors.
- ESM/CommonJS builds, TypeScript declarations, local package-consumer checks, and mock-server integration tests.
- Real-time WebSockets and production service authentication remain pending; no npm release was published.
