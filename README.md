# HumChat Node.js SDK — durable delivery preview

`@humchat/node` is a backend TypeScript SDK for sending and receiving HumChat messages. The `0.1.0-alpha.8` local review preview adds HumChat SDK-key login and automatic renewal to recipient lookup, direct-message helpers, a managed conversation inbox, optional shared WebSockets, durable replay, persistent checkpoints and a send outbox.

Preview status: `auth: { sdkKey, actAs }` logs in through HumChat, caches and renews tokens, shares acquisition across callers, verifies the member identity and backs off during outages. Existing `tokenProvider` authentication continues to work. Login and proactive socket renewal passed read-only production checks; send, receive and threaded replies passed local synthetic-peer tests. Production message/app acceptance, delegated-admin restrictions, the server expiry-code companion and the pilot remain open. This version is prepared for review, not a production acceptance or npm publication milestone. See the [login guide](docs/SDK_KEY_LOGIN_GUIDE.md) and [release gates](docs/RELEASING.md).

The source is public at [S7-Works/humchat-node-sdk](https://github.com/S7-Works/humchat-node-sdk). The npm package remains unpublished and marked `private: true`. Licensing is pending: [LICENSE](LICENSE) remains `UNLICENSED`, with no open-source license granted.

Requires Node.js 22 or later and a compatible HumChat server with `/api/sdk/capabilities`, send receipts v1, message state v1, and history v2. The package supports ESM and CommonJS with matching TypeScript declarations. It has no install scripts or native runtime dependency. This repository contains the SDK; it does not include the HumChat server.

## Build and install locally

From the root of this repository:

```powershell
npm ci
npm run release:prepare
```

The command prints a unique `target/release/release-*` directory containing `humchat-node-0.1.0-alpha.8.tgz`, its SHA-256 and a validation manifest. Install a reviewed archive into your backend using `npm install <path-to-tarball>`. Registry publication is disabled. S7 Works is the intended future npm organization owner; its exact scope, registry access, license and support/security ownership remain unconfirmed.

For a supplied tarball, begin with the [runnable examples](examples/README.md). The package includes [authentication](docs/AUTHENTICATION.md), [reliability](docs/RELIABILITY.md), [shared connections](docs/MULTIPLEX.md), [API reference](docs/API.md), [migration](docs/MIGRATION.md), [troubleshooting](docs/TROUBLESHOOTING.md), [support](docs/SUPPORT.md), and [release preparation](docs/RELEASING.md) guides. They remain usable after installation outside this repository.

## Configure and send

For delegated member login in alpha.8, use an SDK key created by your organization administrator in HumChat:

```ts
import { HumChat } from '@humchat/node';

const humchat = new HumChat({
  orgId: process.env.HUMCHAT_ORG_ID!,
  baseUrl: process.env.HUMCHAT_BASE_URL!,
  auth: {
    sdkKey: process.env.HUMCHAT_SDK_KEY!,
    actAs: process.env.HUMCHAT_ACT_AS!,
  },
});
try {
  await humchat.connect();
  await humchat.messages.sendDirect({
    recipientId: process.env.HUMCHAT_RECIPIENT_ID!, content: 'Hello from my backend.',
  });
} finally {
  await humchat.close();
}
```

The SDK contacts only `baseUrl`. Keep the key server-side; it can act as members of its organization. Use messaging, channels, history and sockets with these delegated credentials; admin authorization must be enforced by the server. The following advanced example keeps the existing caller-managed `tokenProvider` mode. Configure exactly one mode.

For person-to-person messaging, use `members.list()`, `conversations.openDirect({ memberId })`, or `messages.sendDirect({ recipientId, content })`. `inbox.watch(handler, { consumerId, checkpointStore })` discovers conversations belonging to the authenticated identity and consumes retained/new events with per-channel checkpoints. It includes new DMs and groups, with polling and explicit limits. See the [direct-message guide](docs/DIRECT_MESSAGES.md) and [runnable responder](examples/dm-responder.mjs). The lower-level channel API below remains available.

```ts
import { HumChat, ConflictError, HumChatError } from '@humchat/node';

const humchat = new HumChat({
  orgId: process.env.HUMCHAT_ORG_ID!,
  baseUrl: process.env.HUMCHAT_BASE_URL!,
  tokenProvider: async ({ reason, signal }) => {
    // Obtain or renew a caller-managed session here. Expiry is Unix seconds.
    // The SDK never stores or exchanges a refresh token.
    return { token: process.env.HUMCHAT_TOKEN! };
  },
  timeout: 10_000,
});

try {
  const me = await humchat.connect();
  const { channels } = await humchat.channels.list();
  const channel = channels.find(c => c.slug === 'general');
  if (!channel) throw new Error('Choose an accessible channel');

  const sent = await humchat.messages.send({ channelId: channel.id, content: 'Build completed.' });
  const state = await humchat.messages.getState({ channelId: channel.id, messageId: sent.id });
  await humchat.messages.reply(state, { content: 'Tests passed too.' });
  await humchat.messages.setReaction({
    channelId: channel.id, messageId: sent.id, emoji: '✅', present: true,
  });
  console.log({ actorId: me.actorId, messageId: sent.id, status: sent.status });
} catch (error) {
  if (error instanceof ConflictError) {
    // A stale revision or a reused key with different intent requires a caller decision.
  } else if (error instanceof HumChatError) {
    console.error(error.code, error.status, error.messageId);
  } else throw error;
} finally {
  await humchat.close();
}
```

The environment-token example is static: it cannot renew an expired token. A long-running backend must implement renewal inside `tokenProvider`. Its context contains `reason` (`initial`, `expiring`, or `unauthorized`), an optional `previous.expiresAt`, and a shared acquisition `signal`. Return `{ token, expiresAt? }`. The SDK also uses the server's verified expiry when available. Tokens are supplied as Bearer headers; redirects are disabled.

Use `require('@humchat/node')` in CommonJS. `getConfig()` returns a credential-free copy; `getCapabilities()` returns normalized server capability metadata. Returned identity and capability objects can be safely modified by the caller without changing the client's bound identity.

`baseUrl` is required so each client explicitly chooses its HumChat server. It must be an HTTP(S) origin, without credentials, a path, query, or fragment. Use HTTPS for a deployed server. Top-level `apiKey` is rejected with `UnsupportedFeatureError`; delegated member login uses the separate `auth` configuration above.

## Durable consumption and restart recovery

Durable recovery is the default. It requires the server capability `durable_replay: true` and event protocol v1. An older server rejects `subscription.ready` with `UnsupportedFeatureError`; choose `bestEffort` explicitly to use compatibility recovery.

```ts
import { HumChat, FileCheckpointStore, FileOutboxStore } from '@humchat/node';

const checkpoints = await FileCheckpointStore.open('./private-state/checkpoints.json');
const outbox = await FileOutboxStore.open('./private-state/outbox.json');
const consumerId = 'status-responder-v1';
const humchat = new HumChat({
  orgId: process.env.HUMCHAT_ORG_ID!,
  baseUrl: process.env.HUMCHAT_BASE_URL!,
  tokenProvider: async () => ({ token: process.env.HUMCHAT_TOKEN! }),
  recovery: { mode: 'durable' },
});
const identity = await humchat.connect();
const sub = humchat.realtime.subscribe(process.env.HUMCHAT_CHANNEL_ID!, { start: 'now' });
sub.on('error', error => console.error(error.code));
sub.consume(async (event, { signal }) => {
  if (event.kind !== 'message' || event.state.senderId === identity.actorId) return;
  if (event.state.body.trim() !== '!status') return;
  await humchat.outbox.send({
    consumerId, key: event.eventId, store: outbox,
    message: { channelId: event.channelId, content: 'All systems operational.',
      threadRootId: event.state.threadRootId ?? event.state.id },
  }, { signal });
}, { consumerId, checkpointStore: checkpoints });
await sub.ready;
// At application shutdown, close the client before its caller-owned stores:
// await humchat.close(); await checkpoints.close(); await outbox.close();
```

Register `consume()` synchronously after subscribing, before awaiting any other work. One acknowledged consumer is supported per canonical channel; additional handles may observe events. `start` is `'now'` by default, `'earliest'` for the retained log, or `{ cursor }` for an explicit event position. An existing checkpoint takes precedence. On first use, the start position is persisted before any callback; a restart cannot silently establish a later start point.

The consumer receives `{ eventId, actorId, sequence, cursor, kind, state, ...scope }`. Kinds are `message`, `edit`, `delete` and `reaction`; restore/undo produce the corresponding state transition. Sequence values are decimal **strings**, including values beyond JavaScript's safe integer range. `state` is the immutable snapshot at that event, including the full body and original thread root. Do not replace it with a latest-state fetch when processing historical transitions.

Events run in committed channel order. A checkpoint advances only after awaited observers, the consumer callback, and the checkpoint CAS succeed. Failure blocks the channel without acknowledging that event. Close its handles and create a new subscription using the same consumer/store to retry. A disconnected generation reloads the persisted checkpoint. An `event` observer receives the same envelope but does not itself provide a persistent checkpoint. Normal `message`/mutation events remain available; use `consume()` for acknowledged work.

An interrupted `consume()` invocation receives an aborted signal. Automatic reconnect waits for that application's promise to settle before replaying the event; if it ignores cancellation past its original handler deadline, the channel blocks. Cancellation cannot undo external side effects. Closing a client or manually creating a replacement does not forcibly stop application promises; settle or reconcile those effects before restarting ownership. Observational `on()` callbacks do not have the acknowledged consumer's cross-connection settlement barrier.

Delivery is **at least once while the cursor is retained**, not exactly once for arbitrary external effects. A crash after a callback's side effect but before its checkpoint can repeat that callback. Use `eventId` as an application inbox key. `outbox.send()` binds a stable key to exact message intent and persists the generated message/idempotency IDs before sending; it marks completion before returning. Reusing a key with different intent raises `ConflictError`. `outbox.flush({ consumerId, channelId, store })` retries pending sends after restart. It sends sequentially and stops at the first failure. The checkpoint and outbox files are separate transactions; the responder's safe order is persist/send/record reply, then acknowledge input. Server idempotency-record retention remains unverified.

`events.sync({ channelId, after, through?, limit? })` exposes authenticated forward replay directly. Continue with `nextCursor`, keeping the first page's `throughCursor` to retain a fixed committed watermark. History cursors are incompatible. `RecoveryGapError` includes `earliestCursor`/`earliestSequence` when a cursor expired or its epoch changed. The SDK stops; it never skips to the new floor automatically. Restoring state cannot recreate expired business events, so the application must explicitly choose how to resume.

The server logs writes committed after the event-log migration; older message history is not backfilled. All server writers sharing a database must run the upgraded version before durable delivery can be relied on. Retention is at least seven days, configurable upward with `HUMCHAT_EVENT_RETENTION_DAYS`; automatic pruning is disabled in this preview. A server maintenance helper deletes only a contiguous expired prefix and atomically advances its floor. Ops ownership and storage sizing remain release decisions.

The worker negotiates a ready watermark over its channel socket, then reads bounded HTTP event pages. WebSocket events act as progress hints, with periodic SQL/HTTP checks to survive missed notifications. A finished durable catch-up emits `recoveryComplete` with `mode: 'durable', complete: true, cursor`; this covers the captured log watermark, not future events or pre-migration history. The SDK does not use the snapshot cache to suppress intermediate historical mutations. Large backlogs are processed page by page; the best-effort `maxPages`, `maxThreads`, `maxStateRefresh` and total `timeoutMs` scan settings do not truncate durable replay. Each page uses normal bounded HTTP deadlines, `recovery.maxBytes` bounds a decoded page, and every callback has `handlerTimeoutMs`.

`MemoryCheckpointStore`/`MemoryOutboxStore` are process-local alternatives. File stores serialize operations, enforce CAS, validate content, fsync a temporary file and replace the destination atomically. They hold an exclusive sibling `.lock` for one process and default to 64 MiB per store. On a process crash, inspect the stale lock and confirm its owner is gone before manually removing it; locks are never automatically stolen. Close stores explicitly. Store files contain message content and should be in a private persistent directory. Network filesystems, abrupt power loss and multi-host coordination are unverified; use application database adapters for distributed workers. Custom adapters must make CAS atomic, bound their own I/O, and preserve the full server/org/actor/consumer/channel scope. Completed outbox entries are retained until application-managed archival; reaching the size budget fails explicitly.

## Optional shared connection

Set `realtime: { transport: 'multiplex' }` to receive multiple channels through one socket on an upgraded server. Durable replay/checkpoints remain independent per channel. The default is `'channel'`; unsupported multiplex capability fails explicitly. See the [shared connection guide](docs/MULTIPLEX.md) and [runnable worker](examples/multiplex-worker.mjs).

## Compatibility listener and thread reply

```ts
const humchat = new HumChat({
  orgId: process.env.HUMCHAT_ORG_ID!,
  baseUrl: process.env.HUMCHAT_BASE_URL!,
  tokenProvider: async () => ({ token: process.env.HUMCHAT_TOKEN! }),
  recovery: { mode: 'bestEffort' },
});
const identity = await humchat.connect();
const subscription = humchat.realtime.subscribe(process.env.HUMCHAT_CHANNEL_ID!);
subscription.on('error', error => console.error(error.code));
subscription.on('recoveryIncomplete', event => console.warn(event.limitations));
subscription.on('message', async ({ message, source }) => {
  if (message.senderId === identity.actorId || source !== 'live') return;
  const body = message.isLong
    ? (await humchat.messages.getState({ channelId: message.channelId, messageId: message.id })).body
    : message.content;
  if (body.trim() === '!status') {
    await humchat.messages.reply(message, { content: 'All systems operational.' });
  }
});
await subscription.ready;
// Keep the process running. At application shutdown:
// await subscription.close();
// await humchat.close();
```

Install listeners before awaiting `ready`. It resolves once the initial bounded recovery attempt and buffered deliveries finish. It does not certify complete historical coverage. Initial history emits `stateReconciled`, so an old command is not delivered as a new command on startup. A newly buffered live creation can still produce `message`. The packaged `examples/thread-responder.mjs` adds stable reply IDs/keys and graceful shutdown.

| API | Behavior |
| --- | --- |
| `realtime.subscribe(channelId)` | Immediate `ChannelSubscription` with `ready`, `status`, typed `on`/`off`, and shared handles per channel; the selected transport determines physical socket count. |
| `realtime.subscribeMany(channelIds)` | Per-input `{ channelId, subscription }` or `{ channelId, error }`; later network errors arrive through each handle. |
| `subscription.typing()` | Sends a typing signal while live. |
| `subscription.markRead({ through })` | Explicit read watermark in Unix seconds. Receipt of a message does not automatically mark it read. |
| `subscription.resync()` | Schedules event-log replay in durable mode or bounded snapshot reconciliation in best-effort mode. Awaiting it acknowledges scheduling; observe `recoveryComplete` for attempt completion. Safe to call inside a handler. |
| `subscription.close()` | Removes this observer; removes the logical channel when its last observer closes; unrelated multiplexed channels remain connected. |
| `realtime.getStats()` | Socket/channel/subscription counts, queued events/bytes, reconnects, recovery attempts, auth-refresh failures, dropped ephemeral events. |
| `humchat.close()` | Cancels HTTP work, channel sockets, recovery, and timers. |

Typed events are `event`, `checkpoint`, `message`, `messageUpdated`, `messageDeleted`, `reactionUpdated`, `stateReconciled`, `typing`, `presence`, `receipt`, `callInvite`, `connection`, `resync`, `recoveryIncomplete`, `recoveryComplete`, `sessionRevoked`, `authRequired`, `error`, `handlerError`, and `unknownEvent`. Message events contain `{ message, source, replayed }`; mutation/reconciliation events contain `{ state, source }`. Durable `event` and `checkpoint` payloads are described in the [API reference](docs/API.md). Unknown events expose a bounded type name without raw payload. The declarations describe each payload.

Application deliveries are awaited in order per channel. Different channels run independently. A rejected or timed-out application handler blocks that channel and emits `handlerError`; the SDK cannot cancel side effects already started by your callback. Keep handlers short and use an application inbox/outbox for business actions. Control notifications such as connection/error are observational: each kind has at most one active dispatch and one latest pending value per handle, so a slow observer may skip intermediate notifications. Close cancels pending delivery and its timers; read `subscription.status` for the terminal state because a queued `connection: closed` notification is not guaranteed. Durable consumers require durable mode; they are not available on the compatibility listener.

## Compatibility recovery and shared real-time limits

The following snapshot recovery behavior applies only to explicit `recovery: { mode: 'bestEffort' }`. Every finished best-effort attempt emits `recoveryIncomplete` with reasons and `recoveryComplete` with **`complete: false`**. The latter means the attempt finished, not that every event was recovered; buffered live deliveries can still follow. Durable mode uses the ordered event log described above.

The SDK opens the socket before scanning history v2, buffers live frames, reconciles root messages and recently known threads, refreshes bounded cached states, and drains buffered events. Reconnect can emit inferred new creations with `source: 'recovery', replayed: true`; snapshots use `stateReconciled` rather than inventing historical mutation events. Duplicate filtering and revision tracking are bounded and process-local. Older unknown threads, mutations outside scanned/cached state, late commits behind a watermark, cache eviction, and process restarts can cause gaps or repeat observations. This is not a durable event stream or an exactly-once consumer.

Transport outages and HTTP upgrade 408/429/502/503/504 retry with exponential full jitter; reconnect continues until closed or blocked. Retry-After is honored. A stable connection resets the backoff after 30 seconds. Ping/pong detects half-open sockets. One failed 401 upgrade may refresh; repeated 401 and channel 403/404 stop that channel. Verified replacement tokens must retain the bound actor and organization. A changed token rotates affected sockets through the connection limit; the old socket closes before its replacement, so that gap uses the selected recovery mode. A `session_revoked` frame is terminal unless it carries exactly `code: "TOKEN_EXPIRED"` and that code was advertised by the verified session's capabilities. Only that negotiated expiry invalidates the old token and reconnects through normal verified acquisition. Reason text and local clock proximity never authorize renewal.

| Setting | Default / scope |
| --- | --- |
| `realtime.maxChannels`, `connectConcurrency` | 50 channels, 5 simultaneous authenticated handshakes in durable mode; best-effort mode holds a slot through initial recovery. Durable replay releases its connection slot before checkpoint I/O and application callbacks so slow recovery cannot starve another channel. These are configurable budgets, not measured server capacity. |
| `maxBufferedEvents`, `maxBufferedBytes` | 1,000 events / 16 MiB across the client, including in-flight application delivery. |
| `maxPayloadBytes` | 10 MiB per WebSocket frame. |
| `maxCachedMessages`, `maxCachedBytes` | 1,000 entries / 8 MiB per channel; LRU eviction. |
| `reconnectInitialDelayMs`, `reconnectMaxDelayMs` | 500 ms / 30 seconds. |
| `pingIntervalMs`, `pongTimeoutMs`, `handlerTimeoutMs` | 30 / 10 / 30 seconds. |
| `recovery.maxPages`, `maxThreads` | 3 pages per root/thread scan, 20 recently observed roots. |
| `recovery.maxBytes`, `maxStateRefresh`, `timeoutMs` | 16 MiB decoded snapshots, 50 additional cached states, 30-second recovery deadline including awaited recovery handlers. |

When buffers fill, transient presence/typing/receipt/call/unknown frames can be dropped and counted. Overflow of message or mutation delivery emits incomplete-recovery diagnostics and blocks the channel with `BackpressureError`. The SDK never silently evicts subscriptions to fit more channels. Inspect `connection`/`error` and close blocked handles before explicitly creating a replacement. Distribute channels across application workers with your own lease scheme; replicas do not share deduplication state. Local debug-server tests covered 1/10/50/100 channels and a five-minute soak; production capacity and a multiplexed bot stream remain unverified.

## HTTP methods

All resource methods accept optional `{ signal, timeout }`. IDs are opaque and need not be ULIDs, but this preview requires the state protocol's identifier grammar: ASCII letters, digits, `.`, `_`, `:`, or `-`, excluding `.` and `..` alone. Message, thread, and operation IDs are 1–128 characters; channel keys allow 1–1024 and actor IDs 1–256. Legacy IDs outside this grammar are unsupported by this SDK's required state/history contract. The SDK also restricts idempotency keys to the same 1–128-character grammar as an explicit preview policy. Generated outgoing IDs are monotonic ULIDs. Times are Unix seconds. Unsafe numeric revisions are rejected.

| Method | Result / important behavior |
| --- | --- |
| `connect()` | Validates `/me`, organization, actor, and required protocol capabilities; also called lazily. |
| `channels.list({ teamId? })` | Team-scoped envelope with `channels`, `orgId`, `teamId`, `teamName`. |
| `conversations.list()` | Accessible conversations; `channelId` comes from `channel_key`, distinct from management `id`. |
| `messages.send({ channelId, content, id?, threadRootId?, priority? }, { idempotencyKey?, ...options })` | Acceptance receipt, not a complete message or a read receipt. |
| `messages.reply(message, { content, id?, priority? }, options)` | Reuses the channel and original thread root, including when replying to a reply. |
| `messages.getState({ channelId, messageId })` | Validated state, original/current body, content/reaction revisions, deletion flag and attachments. |
| `messages.history({ channelId, threadRootId?, cursor?, limit? })` | One history v2 page; roots by default, replies when a thread root is supplied. |
| `messages.iterateHistory(input, options)` | Async iterator of pages from newest page toward older pages; rows within each page are ascending. |
| `messages.edit({ channelId, messageId, content, expectedRevision, operationId? })` | Author-only edit with optimistic concurrency. |
| `messages.delete({ channelId, messageId, expectedRevision, operationId? })` | Author-only tombstone with optimistic concurrency. |
| `messages.setReaction({ channelId, messageId, emoji, present, operationId? })` | Sets desired reaction membership; never toggles. |
| `close()` | Cancels this client's requests/waits and prevents further work; does not log out the shared user session. |

State tombstones retain text in the wire response; respect `deleted` when displaying or processing state. `originalBody` and `body` are distinct. `revision` and `reactionRevision` are independent. `requestDigest` is opaque server evidence; the SDK does not recompute the Rust BLAKE3 digest.

History cursors traverse older snapshots. They are **not** event-resume tokens. Root history excludes replies; neither history mode is a replay log for edits, deletions, or reactions. Iteration has a caller-selected stopping point: break the loop or cancel its signal. Durable event replay uses `events.sync()` and a separate cursor format.

## Retries, errors, and identity

Defaults are a 10-second timeout per HTTP attempt, 3 retries, full jitter from 250 ms up to 2 seconds, and a 30-second total request deadline. Configure these through `retry: { maxRetries, initialDelayMs, maxDelayMs, totalTimeoutMs }`. An underlying HTTP operation's total deadline covers waiting for authentication, response reading, retry delays, and one possible 401 refresh. A shared authentication acquisition has its own bounded deadline; aborting one waiter does not cancel other callers. `close()` cancels it for everyone.

`events.sync()` also shares one total deadline across capability negotiation and its page request. An entire `outbox.send()` or `outbox.flush()` does not have that combined deadline: identity acquisition, store I/O and each send run sequentially, and separate HTTP operations receive their own budgets. File/store work is awaited without an SDK timeout; custom adapters must bound their own I/O. Pass a caller-owned abort signal to limit network work across a batch, and allow any started persistence operation to finish before closing its store.

GETs and stable send/mutation intents retry transport failures, attempt timeouts, and HTTP 408/429/502/503/504. Other 4xx errors and generic 500 are not automatically retried. `Retry-After` is honored within the deadline, with a shared cooldown for 429. One 401 can request a new token; a second 401 ends that operation. Every newly acquired token is checked through `/me` and capability discovery before a write can use it. A changed organization or actor blocks the client and requires a new instance.

IDs, idempotency keys, operation IDs, and the serialized body remain unchanged during retries. An exhausted send can have been committed: use its error's `messageId` and `idempotencyKey` to retry the **same original intent**, or inspect state. Use the outbox or caller-persisted IDs/keys for process restart recovery. Server key-retention guarantees remain unverified.

Errors extend `HumChatError`, with stable `code`, `kind`, `retryable`, optional `status`/`requestId`, and available intent metadata. Categories include configuration, validation, authentication, permission, not-found, conflict, rate-limit, network, timeout, cancellation, server, protocol, unsupported-feature, session-revoked, backpressure, recovery-gap and persistence errors. `RecoveryGapError` exposes validated scoped floor metadata; raw server bodies and transport/provider error text are omitted. Responses are capped at 10 MiB. There is no default logger.

## Tests and examples

```powershell
npm ci
npm test
```

`npm test` builds and typechecks the SDK, then runs unit, controlled-peer integration, release-policy and installed-package tests. These tests use synthetic identities and local HTTP/WebSocket peers; they do not require production credentials or a server checkout. The historical Rust/PostgreSQL SDK harness was removed from current upstream server main; a maintained real-server fixture remains separate follow-up work. This repository does not start a Rust server or PostgreSQL.

Runnable examples include `send.mjs`, `list-channels.mjs`, `react.mjs`, `thread-responder.mjs` (best effort) and `durable-responder.mjs` (checkpoint/outbox), all under `examples/`. Set `HUMCHAT_BASE_URL`, `HUMCHAT_ORG_ID`, `HUMCHAT_TOKEN`, and the channel/message variables documented in each. The durable responder also requires `HUMCHAT_STATE_DIR`. The environment-token examples cannot renew credentials: implement a refreshing provider for a long-running deployment.

The repository configures Node 22/24 package checks on Windows, Linux and macOS. Check the [Actions results](https://github.com/S7-Works/humchat-node-sdk/actions) for runs actually completed; a configured matrix alone is not a passing result. Production bot credentials, staging acceptance, the agreed 24-hour pilot and npm publication remain open. Attachment upload helpers and distributed persistence adapters are not included. Local tests do not establish production capacity.

## Outgoing webhooks

Use `client.webhooks` to manage administrator-owned workspace-channel subscriptions on a configured server, and `verifyWebhook()` to authenticate exact raw request bytes at your receiver. Automatic retries, dead letters, explicit replay and signing-secret rotation are supported. Read the [webhook guide](docs/WEBHOOKS.md) for setup, authorization, delivery guarantees and the shipped receiver example. This does not provide production machine-token authentication.
