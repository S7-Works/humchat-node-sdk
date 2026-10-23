# Changelog

## Unreleased

- Fixed automatic inbox conversation rejoining while a cancelled handler or checkpoint write is still pending. Discovery now waits for bounded settlement before replacement, preventing overlapping callbacks and checkpoint conflicts. Explicit watcher/client shutdown remains cancellable.

## 0.1.0-alpha.8 - 2026-10-23 (local review preview; unpublished)

This preview versions the SDK-key work from Phases 2–7. It remains `private: true` and `UNLICENSED`; no npm release is published. Production messaging/app acceptance, delegated-admin enforcement, the server expiry companion and pilot acceptance remain open.

- Added mutually exclusive typed `auth: { sdkKey, actAs }` and `tokenProvider` configuration, with SDK-key input validation, browser refusal and HTTPS requirements outside literal loopback development origins.
- Kept credentials out of inspectable configuration and retained existing token-provider behavior, including opaque/mock organization IDs.
- Added same-origin SDK-key minting through HumChat, cached single-flight session acquisition, organization/member verification, bounded login retries and typed sanitized errors. The key is never sent as Bearer authentication.
- Reject mint responses that reflect an SDK key as the user token. Redact preceding tokens from response request IDs during renewal, including rejected mint results; suppress remote request IDs after bounded credential history fills.
- Exposed normalized `authentication.sdkKeyUserTokens` capabilities; older token-provider servers remain compatible. Login metadata cannot redirect the credential to another endpoint or header.
- Integrated the existing expiry schedule and one-retry resource HTTP 401 lifecycle. Validated native key renewal across compatibility, durable-channel and multiplex sockets, including checkpoint recovery.
- Transient timer-refresh failures now reconnect with backoff; native mint 401/403 is latched until a new client. Revocation cancels pending acquisition and prevents late token verification/use.
- Added shared backoff after retryable SDK-key acquisition failures, preventing many channels from triggering successive login attempts during an outage. Covered 50 channel sockets and 100 multiplex subscriptions, checkpoint recovery, cancellation and bounded acquisition retries with local peers.
- Clarified explicit workspace team selection and preserved missing-workspace errors without automatic bootstrap writes.
- Extended release credential checks to detect SDK-key strings without echoing them in failures.
- Allowed numbered phase-report filenames under the existing package documentation policy and corrected repository-only links in shipped reports.
- Added native SDK-key direct-message acceptance across two member identities, including managed inbox discovery, replies, reactions, ordinary user-session interoperability and checkpoint restart. Added an explicit live-server acceptance runner with synthetic verification; production send/reply and app UI acceptance remain pending a designated recipient.
- Added capability-gated `TOKEN_EXPIRED` handling. Only a server advertising `authentication.socket_session_end_codes` can authorize recovery through that explicit code; legacy/unknown endings remain terminal. The server companion is prepared separately and requires build/deployment acceptance. The delegated-admin restriction mismatch remains server work. These preview changes have not been published.

## 0.1.0-alpha.7 - 2026-10-23

- Added organization member lookup, conversation creation, exact-peer DM lookup and direct-send helpers.
- Added a managed conversation inbox with bounded discovery polling, earliest-retained recovery, persistent per-channel checkpoints, explicit capacity/errors and cancellable lifecycle.
- Added a persistent DM responder example and controlled coverage for first-message recovery, restart, membership changes and failures. Existing SDK capability negotiation remains required.
- Conversation creation is not automatically retried; machine authentication, production acceptance, licensing and npm publication remain open.

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
