# Captured HumChat wire contract

`fixtures/http-ws.json` was captured from a local HumChat Rust server with a real PostgreSQL database and mock ButtrBase identities. Its identities and message content are test data. The fixture records raw wire names and values before the SDK's camelCase mapping.

The `provenance` object records capture time, Node version, the upstream server checkout's Git HEAD at test time, binary SHA-256, and the use of mock authentication. The Git value is not a commit in this standalone SDK repository and does not guarantee that an independently built binary came from a clean tree.

| Sample | What it records |
| --- | --- |
| `identity`, `channels`, `conversation` | `/me`, team-scoped channel discovery, and DM creation with its canonical routing key. |
| `sendRequest`, `sendReceipt`, `message` | Incoming root send, detailed ACK, and bare WebSocket message. |
| `replyRequest`, `reply`, `legacyAck` | Threaded response and legacy JSON ACK string. |
| `mutationRequest`, `mutationReceipt`, `reaction` | Desired-state reaction and authoritative receipt/event state. |
| `history`, `threadHistory` | Version 2 top-level and thread history pages. |
| `typing`, `receipt` | Typing and Unix-second read-horizon events. |
| `edit`, `delete`, `state` | Versioned edit, deletion, and final message tombstone. |

The upstream server harness also checked send/reaction replay, changed-intent conflicts, stale revisions, wrong-author rejection, private-channel restrictions, cross-tenant rejection and channel separation. That harness remains in the server repository; this fixture is not a transcript of every request or a replacement for server acceptance tests.

No live samples were captured for `presence`, `resync`, `session_revoked`, `call_invite`, forward/undo/restore, attachments, or oversized histories. Their presence in source does not make them runtime-certified here. Future decoder tests should include separate, explicitly source-derived edge-case fixtures rather than relabeling invented data as captured traffic.

SDK unit tests consume this fixture through `npm test`. Regeneration requires the separate HumChat server harness and a disposable mock-authentication database; it cannot be performed from this repository alone. The npm tarball excludes this directory. Keep future captures limited to synthetic identities and content, without credentials.
