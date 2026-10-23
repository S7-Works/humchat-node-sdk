# API reference

Import public symbols from `@humchat/node`. ESM and CommonJS are supported; TypeScript declarations ship with both builds. Use `new HumChat(options)` to obtain resources; subscriptions are returned by `client.realtime.subscribe()`. The [bundled declarations](../dist/index.d.ts) are the complete type reference. Examples below use a configured `client` from [Authentication](AUTHENTICATION.md).

## Configuration and units

| Option | Default / requirement |
| --- | --- |
| `orgId`, `baseUrl`, `tokenProvider` | Required. See [authentication](AUTHENTICATION.md). |
| `timeout` | `10_000` milliseconds per HTTP attempt. |
| `retry.maxRetries` | `3` retries after the initial attempt; allowed range 0–10. |
| `retry.initialDelayMs`, `retry.maxDelayMs` | `250`, `2_000`; exponential full jitter, subject to Retry-After. |
| `retry.totalTimeoutMs` | `30_000`; total HTTP-operation budget including authentication and retries. |
| `realtime.transport` | `'channel'` by default; `'multiplex'` shares one socket and requires durable recovery plus server capability. See [shared connections](MULTIPLEX.md). |
| `recovery.mode` | `'durable'`; explicit `'bestEffort'` supports older servers. |
| `fetch` | Node's global fetch. Optional HTTP transport injection; not WebSocket injection. |

Resource methods accept `RequestOptions = { signal?: AbortSignal, timeout?: number }`. `timeout` overrides the HTTP attempt timeout; it does not replace `retry.totalTimeoutMs` with a whole-workflow deadline. `connect()` and `getCapabilities()` cap their wait by the smaller of an explicitly supplied `timeout` and the total budget. Iteration and `outbox.flush()` perform multiple operations; custom persistence I/O needs its own bounds. For a deadline covering your full workflow, pass an `AbortSignal.timeout(...)` signal and ensure your own adapters/actions observe cancellation.

Message timestamps, identity expiry, read watermarks, typing timestamps, presence `lastSeen` and call `createdAt` are Unix **seconds**. Configuration timeouts, `typing.ttlMs`, `retryAfterMs`, and `connection.nextRetryAt` are **milliseconds**; `nextRetryAt` is an absolute Unix-millisecond time. Revisions are safe integer numbers. Durable event `sequence`, `throughSequence` and `earliestSequence` are decimal strings; never convert them to `Number` for ordering.

Identifiers are opaque, case-sensitive strings, not necessarily ULIDs. Supported characters are ASCII letters/digits and `.`, `_`, `:`, `-`; `.` and `..` alone are invalid. Message/thread/operation/idempotency IDs allow 1–128 characters, channel keys 1–1024, and actor IDs 1–256. A channel key must be in the configured organization. `consumerId` additionally starts with a letter or digit. Send/edit text must contain non-whitespace content and fit 1 MiB of UTF-8 bytes. An emoji key is nonempty, at most 32 UTF-8 bytes, without surrounding whitespace or control characters. Invalid inputs raise `ValidationError` before the resource request.

## Client and discovery

| Method | Result |
| --- | --- |
| `connect(options?)` | `Promise<Identity>`; verifies and binds the actor/organization. |
| `getCapabilities(options?)` | `Promise<SdkCapabilities>`; cached with the verified session. |
| `getConfig()` | Credential-free `SafeConfig` copy. |
| `close()` | Cancels requests, subscriptions and timers; does not close caller-owned stores or revoke credentials. |
| `channels.list({ teamId? }?, options?)` | `{ orgId, teamId, teamName, channels }`; it is one team-scoped result, not an all-teams iterator. |
| `conversations.list(options?)` | `Conversation[]`; use `channelId` for messages. |
| `members.list(options?)` | Organization roster `{ actorId, name, email, role, status }[]`. |
| `conversations.create({ memberIds, name? }, options?)` | Create/reuse a conversation; creation is not automatically retried. |
| `conversations.openDirect({ memberId }, options?)` | Find an unnamed exact peer DM or ask the server to create/reuse it. |
| `inbox.watch(handler, { consumerId, checkpointStore, pollIntervalMs?, maxConversations? })` | Managed discovery and acknowledged consumption across the identity's conversations. See [direct messages](DIRECT_MESSAGES.md). |

```js
const { channels } = await client.channels.list();
for (const channel of channels) console.log(channel.id, channel.displayName);
const conversations = await client.conversations.list();
for (const conversation of conversations) {
  console.log(conversation.channelId, conversation.name);
}
```

`Channel` contains `id`, `orgId`, `teamId`, `slug`, `displayName`, `isPrivate`. `Conversation` contains management `id`, messaging `channelId`, `name`, `kind`, `memberCount`, `myRole`, `members`, `lastMessageAt`. Workspace channel creation and independent membership administration are not exposed.

## Send, reply and mutate

| Method | Input / result |
| --- | --- |
| `messages.send(input, options?)` | `{ channelId, content, id?, threadRootId?, priority? }` → `SendReceipt`. Options also allow `idempotencyKey`. |
| `messages.sendDirect(input, options?)` | `{ recipientId, content, id?, threadRootId?, priority? }` → `{ conversation, receipt }`. Uses `openDirect` followed by the normal send. |
| `messages.reply(reference, input, options?)` | Reference `{ id, channelId, threadRootId? }`; input `{ content, id?, priority? }` → `SendReceipt`. |
| `messages.getState({ channelId, messageId }, options?)` | Current full `MessageState`. |
| `messages.edit(input, options?)` | `{ channelId, messageId, content, expectedRevision, operationId? }` → `MutationReceipt`. |
| `messages.delete(input, options?)` | `{ channelId, messageId, expectedRevision, operationId? }` → `MutationReceipt`. |
| `messages.setReaction(input, options?)` | `{ channelId, messageId, emoji, present, operationId? }` → `MutationReceipt`. |

`priority` is `'standard'` (default), `'important'` or `'urgent'`. Replying to a reply preserves `reference.threadRootId`; replying to a root uses `reference.id`. `Message` and `MessageState` are valid references. Edit/delete require the current content revision and server permission; the SDK validates that successful edit/delete receipts belong to the bound sender. Reaction `present: true` adds your reaction and `false` removes it; this is not a toggle and uses no expected content revision.

```js
const { channels } = await client.channels.list();
const channel = channels.find(item => item.slug === 'general');
if (!channel) throw new Error('Choose an accessible channel');

const sent = await client.messages.send({
  channelId: channel.id, content: 'Build complete.', priority: 'important',
});
const original = await client.messages.getState({ channelId: channel.id, messageId: sent.id });
await client.messages.reply(original, { content: 'Tests passed.' });
await client.messages.setReaction({
  channelId: channel.id, messageId: sent.id, emoji: '✅', present: true,
});
const edited = await client.messages.edit({
  channelId: channel.id, messageId: sent.id,
  expectedRevision: original.revision, content: 'Build and tests complete.',
});
// If deletion is intended, use the revision in edited.state:
// await client.messages.delete({ channelId: channel.id, messageId: sent.id,
//   expectedRevision: edited.state.revision });
```

`SendReceipt` contains `id`, `channelId`, `status: 'accepted'`, `createdAt`, `idempotencyKey`, `attachmentCount`, `attachmentManifestBlake3`. Acceptance does not mean every recipient has read the message. `MutationReceipt` contains `schema: 1`, `operationId`, `actorId`, `requestDigest`, `state`. A repeated no-op edit can preserve the revision. Mutation responses are the result recorded for that operation; an idempotent replay need not return today's latest state.

Generated message and operation IDs are monotonic ULIDs. A send defaults its idempotency key to its generated/provided message ID. Automatic retries retain the exact serialized intent. A new method call generates new IDs unless you supply the originals. Preserve IDs, key, content, priority, target and revision when retrying an uncertain write. See [reliability](RELIABILITY.md) before adding application retries.

`MessageState` includes `schema: 1`, `id`, `orgId`, `channelId`, `senderId`, `createdAt`, `threadRootId`, `priority`, `originalBody`, `body`, `revision`, `updatedAt`, `deleted`, `reactionRevision`, `reactions`, `attachments`. `reactions` maps emoji keys to actor ID arrays. Content and reactions have separate revisions. Attachment metadata is readable; uploading/sending attachments, pinning, forwarding, restore/undo and arbitrary raw requests are not exposed by this SDK.

## History and durable events

| Method | Behavior |
| --- | --- |
| `messages.history({ channelId, threadRootId?, cursor?, limit? }, options?)` | One `HistoryPage`, schema 2, with full current states and `nextCursor`. Limit 1–200. Omitted/null thread selects root messages. |
| `messages.iterateHistory(input, options?)` | Async generator of **pages**, moving from newer pages to older pages. Each page's messages are in ascending order. |
| `events.sync({ channelId, after, through?, limit? }, options?)` | One forward `EventPage` from the durable log; `after` required, limit 1–200. |

```js
for await (const page of client.messages.iterateHistory({ channelId, limit: 100 })) {
  for (const state of page.messages) console.log(state.id, state.deleted);
}
```

History has separate cursors for each organization/channel/thread. It returns current snapshots, not every historical transition. Event cursors belong to an organization/channel/epoch and are incompatible with history cursors. Treat both as opaque.

For manual event paging, obtain `after` from an application checkpoint, a received durable envelope, or the deployment's event-protocol bootstrap. Keep the first page's `throughCursor` fixed while continuing with `nextCursor`:

```js
// channelId and after are an existing channel key and its saved event cursor.
let next = after;
let through;
do {
  const page = await client.events.sync({ channelId, after: next, through, limit: 100 });
  through ??= page.throughCursor;
  for (const event of page.events) console.log(event.sequence, event.kind, event.state.id);
  next = page.nextCursor;
} while (next !== null);
```

`EventPage` contains `schema: 1`, `orgId`, `channelId`, `epoch`, `events`, `afterCursor`, `throughCursor`, `throughSequence`, `nextCursor`. `DurableEvent` contains the same scope plus `eventId`, `actorId`, `sequence`, `cursor`, `kind` and full `state`. Kinds are `message`, `edit`, `delete`, `reaction`. Its `state` is the snapshot at that transition, including the full body; `actorId` is the acting principal and can differ from the message's sender on reactions.

## Subscriptions

| Method / property | Behavior |
| --- | --- |
| `realtime.subscribe(channelId, { start? }?)` | Returns a handle immediately. Durable start is `'now'` by default, `'earliest'`, or `{ cursor }`; saved checkpoints take precedence. |
| `realtime.subscribeMany(channelIds)` | Array of per-input `{ channelId, subscription }` or `{ channelId, error }`. It accepts no start override; later network failures arrive on the handle. |
| `realtime.getStats()` | Counts sockets, channels, handles, queued events/bytes, reconnects, recoveries, auth-refresh failures and dropped ephemeral events. |
| `subscription.ready` | Promise for initial startup/recovery completion; a later error does not turn an already-resolved promise into a rejection. |
| `subscription.status` | `idle`, `connecting`, `syncing`, `live`, `backoff`, `blocked`, `closed`. |
| `subscription.on(name, listener)` / `off(name, listener)` | Add/remove a typed listener; return the handle. Listener may return a promise. |
| `subscription.consume(handler, { consumerId, checkpointStore })` | One acknowledged durable consumer per shared channel; register synchronously before startup. |
| `subscription.typing()` | Sends a transient signal while live. |
| `subscription.markRead({ through })` | Sends an explicit Unix-second read watermark while live. Receiving does not mark read automatically. |
| `subscription.resync()` | Schedules replay/reconciliation. Awaiting it is not a recovery-completion barrier; observe `recoveryComplete`. |
| `subscription.close()` | Detaches a handle; the last handle removes its channel registration. The shared socket closes when no registrations remain. |

Multiple handles for the same canonical channel share one logical worker and must use the same `start` option. With multiplex transport, distinct channel workers share one physical socket. Closing the acknowledged consumer while other handles remain blocks that channel. Close all its handles before creating a replacement. Explicit `start` and `consume()` are unavailable in best-effort mode.

| Event | Payload / meaning |
| --- | --- |
| `event` | `DurableEvent`; every ordered durable transition. |
| `checkpoint` | `{ channelId, consumerId, cursor, sequence }`; notification after a persisted acknowledgement. |
| `message` | `{ message, source, replayed }`; source is `live` or `recovery`. |
| `messageUpdated`, `messageDeleted`, `reactionUpdated` | `{ state, source }`. |
| `stateReconciled` | `{ state, source: 'recovery' }`; best-effort snapshot reconciliation. |
| `typing`, `presence`, `receipt`, `callInvite` | Typed transient payloads; do not use them as durable business events. |
| `connection` | `{ channelId, state, attempt, nextRetryAt? }`. |
| `resync` | `{ channelId, reason }`. |
| `recoveryComplete` | Durable: `complete: true` plus `cursor`; best effort: `complete: false`. Both include channel, mode, reason, pages and states. |
| `recoveryIncomplete` | Best-effort coverage/limitations; incomplete recovery can also accompany best-effort buffer failure. |
| `sessionRevoked`, `authRequired` | `{ channelId, reason }` or `{ channelId, error }`. |
| `error` | A `HumChatError`, including channel context when available. |
| `handlerError` | `{ event, error }`; event name can be `'consume'`. |
| `unknownEvent` | `{ channelId, type }`; bounded name, no raw frame. |

Legacy live `message.content` can be a snippet when `message.isLong` is true; fetch `getState()` for current full text. Durable consumers already receive full historical text in `event.state.body`. See [Reliability](RELIABILITY.md) for dispatch order, acknowledgements and limits.

## Persistence and outbox

`new MemoryCheckpointStore()` and `new MemoryOutboxStore()` are process-local. `await FileCheckpointStore.open(path, { maxBytes? })` and `await FileOutboxStore.open(path, { maxBytes? })` persist to separate files. Default maximum is 64 MiB per file; supported overrides are 1 KiB–1 GiB. Call each store's `close()` explicitly.

`outbox.send({ consumerId, key, store, message, idempotencyKey? }, options?)` persists exact send intent before network work and the accepted receipt before returning. It returns `SendReceipt`; repeating an already-sent identical intent returns its stored receipt. `outbox.flush({ consumerId, channelId, store }, options?)` retries that scope's pending intents sequentially and stops at the first failure, returning receipts on success. It does not retry arbitrary external effects or mutations.

Custom `CheckpointStore` adapters implement `load(scope)`, atomic `compareAndSet(scope, expectedCursor, nextCursor)`, and `close()`. Custom `OutboxStore` adapters implement `prepare`, `markSent`, `listPending`, and `close()`. Scope is `{ serverOrigin, orgId, actorId, consumerId, channelId }`; all five parts matter. See [Reliability](RELIABILITY.md) before implementing a distributed adapter.

## Error categories

All exported errors extend `HumChatError`, including `PersistenceError`. They expose `code`, `kind`, `retryable` and optional context: `status`, `requestId`, `channelId`, `messageId`, `operationId`, `idempotencyKey`, `retryAfterMs`, `earliestCursor`, `earliestSequence`. Fields are present only when available. Catch by exported class or `code`, not message text.

| Class / code | Meaning |
| --- | --- |
| `ConfigurationError` / `CONFIGURATION` | Invalid client/store setup. |
| `ValidationError` / `VALIDATION` | Invalid input, or HTTP 400/422. |
| `AuthenticationError` / `AUTHENTICATION` | Token/provider/identity failure, or HTTP 401. |
| `SessionRevokedError` / `SESSION_REVOKED` | Client credential lifecycle was explicitly revoked. |
| `PermissionError` / `PERMISSION`; `NotFoundError` / `NOT_FOUND` | HTTP 403 or 404. |
| `ConflictError` / `CONFLICT` | HTTP 409, changed intent, stale revision or checkpoint ownership conflict. |
| `RateLimitError` / `RATE_LIMIT` | HTTP 429. |
| `NetworkError` / `NETWORK`; `TimeoutError` / `TIMEOUT`; `AbortError` / `ABORT` | Transport failure, deadline or cancellation. |
| `ServerError` / `SERVER` | HTTP 5xx; not every 5xx is automatically retryable. |
| `ProtocolError` / `PROTOCOL` | Invalid/mismatched wire data or failed application delivery. |
| `UnsupportedFeatureError` / `UNSUPPORTED_FEATURE` | Missing protocol/capability or unsupported API-key mode. |
| `BackpressureError` / `BACKPRESSURE` | Channel, queue or delivery budget exceeded. Inspect `retryable`. |
| `RecoveryGapError` / `RECOVERY_GAP` | Event cursor expired or stream epoch changed; explicit recovery decision required. |
| `PersistenceError` / `PERSISTENCE` | Store/lock/format/write failure. |

Recovery guidance: [Troubleshooting](TROUBLESHOOTING.md). Upgrade guidance: [Migration](MIGRATION.md).

## Webhook management and verification

`client.webhooks` provides typed creation, listing, lookup, pause/resume, deletion, secret rotation, delivery inspection and dead-letter replay. `verifyWebhook(rawBody, signatureHeader, secrets, options?)` returns a verified envelope with a normalized `DurableEvent`, or throws `WebhookVerificationError`. Management mutations are single-attempt, including authentication failures; reconcile uncertain outcomes explicitly. See the complete [webhook API and receiver guide](WEBHOOKS.md).
