# Troubleshooting

Start with the error class/code, HTTP status when available, `subscription.status` and `client.realtime.getStats()`. SDK errors omit raw response bodies, tokens and provider messages. Log a bounded selection of fields; do not log provider results, request headers, token-bearing configuration or message/store contents by default.

```js
import { HumChatError } from '@humchat/node';

function report(error) {
  if (error instanceof HumChatError) {
    console.error({ code: error.code, status: error.status,
      retryable: error.retryable, requestId: error.requestId,
      channelId: error.channelId, messageId: error.messageId });
  } else {
    console.error('Unexpected application error');
  }
}
// subscription.on('error', report);
// subscription.on('handlerError', ({ error }) => report(error));
```

## Setup and access

| Symptom | Check / action |
| --- | --- |
| `CONFIGURATION` at construction | Required `orgId`, HTTP(S) origin-only `baseUrl`, an authentication mode, supported option names and positive integer limits. SDK-key mode requires HTTPS outside literal loopback development origins. Unknown top-level options fail. |
| `VALIDATION` for authentication options | Use exactly one of `auth` or `tokenProvider`; omit the other property entirely. SDK-key mode validates key/email shape and canonical org UUID. |
| `UNSUPPORTED_FEATURE` with `auth.sdkKey` | The login endpoint is absent (404/405), SDK-key capability is missing/disabled, or this is a detected browser context. Use a compatible HumChat backend from Node.js. |
| Mint 401 / 403 | Invalid/revoked key or refused delegated login. The failure is latched for this client; verify the key's organization and permitted member, then construct a new client with valid credentials. |
| Mint 502 / 503 | Login is unavailable. Bounded retries apply; 503 can mean configuration or upstream failure, so its text is not treated as a reliable distinction. |
| `UNSUPPORTED_FEATURE` with `apiKey` | Bot API-key exchange is unsupported. Use `auth: { sdkKey, actAs }` for delegated member login or an authorized `tokenProvider`; do not reinterpret a key as a session token. |
| `AUTHENTICATION` from a static environment token | The example does not renew tokens. Verify the supplied session and implement renewal inside the provider. Expiry values are Unix seconds. |
| `AUTHENTICATION` after refresh | Replacement actor/org must match the already-bound client. Create a new client for a different principal; do not reuse its persistence identity. |
| `UNSUPPORTED_FEATURE` during `connect()` | Server lacks capability discovery or send v1/state v1/history v2. Coordinate a server upgrade; disabling durable mode does not remove these HTTP requirements. |
| `UNSUPPORTED_FEATURE` on subscription readiness | Upgrade to event protocol v1/durable replay, or explicitly choose best-effort mode and accept incomplete coverage. |
| `VALIDATION` on a channel | Use `Channel.id` or `Conversation.channelId`; it must be a canonical key in the configured organization, within supported identifier bounds. |
| `NOT_FOUND` / 404 from `channels.list()` | Confirm the intended workspace exists and supply its known team ID with `channels.list({ teamId })`. The SDK does not list all teams or bootstrap missing workspaces. Use `conversations.list()` for DM/group conversation channels. |
| 403/404 | Confirm channel/team membership or guest grants and the exact routing key. Reconnect/retry does not create access. |
| `SESSION_REVOKED` | All work on this client is blocked. Complete the application's authorized credential lifecycle and construct a new client. |

The SDK does not perform OTP login. Built-in SDK-key exchange contacts HumChat only and reuses the session cache and refresh coordination. Recovery from an explicit `TOKEN_EXPIRED` frame requires the server to advertise and implement that capability; ambiguous `session_revoked` stays terminal. See the [login guide](SDK_KEY_LOGIN_GUIDE.md) and [authentication guide](AUTHENTICATION.md).

For a workspace 404, obtain the intended team ID from your HumChat administrator. A successful login or organization roster lookup does not establish that a workspace exists. Do not create or modify workspaces automatically as a discovery fallback.

## Writes, errors and repeated work

| Symptom | Check / action |
| --- | --- |
| Timeout/network error after sending | Acceptance may already have committed. Preserve the original message ID, idempotency key and exact intent for retry, or resume the persisted outbox entry. A fresh generated ID can create a second message. |
| 409 / `CONFLICT` on mutation | Fetch current state. If the expected revision is stale, decide a new action with a new operation ID; never change the request bound to an old operation. |
| `CONFLICT` from outbox | That scoped key already belongs to a different normalized request. Repeat the original input, including whether IDs/key were omitted; use a new key only for a genuinely new action. |
| `CONFLICT` after a consumer callback | Checkpoint CAS may have lost ownership. Stop duplicate owners and reconcile possible side effects before resuming. CAS is not a distributed lease. |
| 429 / `RATE_LIMIT` | Observe Retry-After and the shared HTTP cooldown. Persistent overload requires lower application concurrency/traffic, not a tighter retry loop. |
| `PROTOCOL` | Check SDK/server protocol compatibility, response scope and malformed data. Do not reinterpret this as an empty successful response. A failed handler also uses this category; inspect `handlerError`. |
| Duplicate consumer callback | A crash/disconnect can occur after an effect and before acknowledgement. Deduplicate with an application inbox or the send outbox; delivery is at least once. |

The `retryable` flag describes the failure, not proof that an arbitrary new application action is safe. Automatic SDK retries retain exact intent. HTTP 500 is not among the automatically retried statuses. The total deadline can expire while waiting for Retry-After. See [Reliability](RELIABILITY.md).

## Subscriptions and handlers

Register listeners and the one `consume()` callback synchronously after `subscribe()`. Awaiting another operation first lets startup begin and causes late consumer registration to fail. Registering a second consumer on a shared channel also fails. Extra handles may observe, but they must use the same start option.

`ready` can wait for a large backlog and its callbacks; durable replay is not capped by best-effort `maxPages`. It can reject for missing capability, invalid/expired cursor, storage failure or permanent access denial. After it resolves, keep watching `error`/`connection`: readiness is a one-time promise.

| Symptom | Check / action |
| --- | --- |
| `handlerError` and `blocked` | Await all intended work, respect cancellation and complete within `handlerTimeoutMs`. Repair the failure, settle/reconcile old work, close all channel handles, then replace the subscription using its existing checkpoint. |
| Reconnect waits after cancellation | The acknowledged callback may still be running. The SDK waits to avoid overlap; its original deadline eventually blocks it. A new client cannot forcibly stop that promise. |
| `BACKPRESSURE` when subscribing | Default admission is 50 channels. Close unused handles or assign channels to independently owned workers; budgets are not automatic eviction policies. |
| Retryable durable `BACKPRESSURE` | Another channel temporarily occupies the shared delivery budget. The worker reconnects from its existing cursor/checkpoint. Observe whether handlers or store I/O are slow. |
| Non-retryable durable `BACKPRESSURE` | A page itself exceeds local configured limits. Review event sizes and buffer/page budgets before replacing the blocked subscription. |
| `recoveryComplete` with `complete: false` | Expected best-effort semantics: the bounded scan finished but complete event coverage is unprovable. |
| `typing()` / `markRead()` fails | The channel must be live. Read `through` is a nonnegative safe integer in Unix seconds. These signals are not automatically retried as durable effects. |

Control notifications are coalesced. A slow observer may miss intermediate connection or checkpoint notifications, and close may cancel its terminal notification. Inspect `subscription.status` and the store for authoritative local progress. `resync()` acknowledges scheduling; await a relevant `recoveryComplete` event if your workflow needs attempt completion.

## Retention gaps

For `RECOVERY_GAP`, record the affected scope and old/earliest cursor in your private operational record. Stop the consumer, determine what expired or changed epoch, and reconcile missing business work. No automatic retry can recover deleted events. History provides current snapshots only.

An existing checkpoint overrides `start`, so resubscribing with `'earliest'` alone does not reset it. Any checkpoint advancement/reset must be an explicit application-owned decision with concurrent owners stopped. Reusing a new consumer ID changes persistence/outbox scope and can repeat effects. The gap procedure and deployment retention limits are described in [Reliability](RELIABILITY.md).

## File stores

| Symptom | Check / action |
| --- | --- |
| `PERSISTENCE`: locked store | Find the owner using the sibling `.lock` record and your process supervisor. Close a live owner. Remove a stale lock only after confirming that exact process is gone; a PID alone can be reused. |
| Invalid format/corruption | Stop writers, preserve the original file, and recover from a verified backup or reconcile explicitly. Do not replace it with an empty file just to make startup pass. |
| Size budget reached | Default is 64 MiB per store. Completed outbox entries are retained; plan application-managed archival or a database adapter. Raising the limit does not establish an archival policy. |
| Write/rename/permission failure | Check a private writable local persistent directory, free space and exclusive ownership. Preserve the prior file and pending intent. |
| Close or shutdown waits | Custom adapter or application promises may still be active. Bound their I/O and settle them before closing stores. Client close does not await or undo arbitrary external actions. |

File stores are intended for one process on a supported local filesystem. Network filesystems, power-loss behavior and distributed lease ownership remain **unverified**. Memory stores do not survive restart. See [Migration](MIGRATION.md) before changing versions or ownership.

## Reporting a reproducible issue

Provide the exact installed alpha version, Node/OS version, protocol capability flags, error category/status, bounded statistics, and whether the failure used durable or best-effort mode. Include a minimal reproduction with placeholder credentials and synthetic messages. Do not include raw tokens, production store files or complete response bodies. Preserve local evidence privately when it contains message or tenant identifiers.

Use the [repository issue tracker](https://github.com/S7-Works/humchat-node-sdk/issues) for nonsensitive bugs and questions. No response-time commitment is established. See [Support](SUPPORT.md) before reporting a security concern; npm scope ownership, licensing and registry access remain pending.
