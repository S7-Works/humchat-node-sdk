# Delivery, recovery and persistence

The default mode consumes a retained, ordered event log. With an acknowledged consumer and a durable checkpoint adapter, delivery is at least once while its cursor remains retained. Application side effects are not exactly once: a process can finish a side effect and crash before recording its input checkpoint.

This alpha includes package and controlled-peer fault tests. Production machine authentication, staging acceptance, a production-sized workload and the agreed 24-hour pilot remain **unverified**. Passing local tests is not a capacity guarantee. See [Authentication](AUTHENTICATION.md) for the credential contract and [API](API.md) for exact signatures.

## Choose a recovery mode

| Mode | Coverage and prerequisites |
| --- | --- |
| `durable` (default) | Requires server durable replay and event protocol v1. Replays every retained transition in committed order per channel. Persistent restart progress requires `consume()` plus a persistent checkpoint store. |
| `bestEffort` (explicit) | Uses legacy WebSockets and bounded history/state reconciliation. Unknown older threads, unseen mutations, cache eviction and restarts can leave gaps or repeat observations. It has no acknowledged consumer or durable cursor. |

If a server lacks durable capability, `subscription.ready` rejects with `UnsupportedFeatureError`. The SDK does not silently downgrade. For compatibility, configure `recovery: { mode: 'bestEffort' }` explicitly and accept its weaker coverage.

## Acknowledged thread responder

Save this as `responder.mjs`. Install the package, set `HUMCHAT_BASE_URL`, `HUMCHAT_ORG_ID`, `HUMCHAT_TOKEN`, `HUMCHAT_CHANNEL_ID` and `HUMCHAT_STATE_DIR`, then run `node responder.mjs`. The state directory must be private and persistent. This example uses a static supplied token; implement renewal for a long-running service.

```js
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { HumChat, FileCheckpointStore, FileOutboxStore } from '@humchat/node';

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Set ${name}`);
  return value;
}

const client = new HumChat({
  orgId: required('HUMCHAT_ORG_ID'),
  baseUrl: required('HUMCHAT_BASE_URL'),
  tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }),
  recovery: { mode: 'durable' },
});
const consumerId = 'status-responder-v1';
const pending = new Set();
let checkpoints, outbox;
let requestStop;
const stopped = new Promise(resolve => { requestStop = resolve; });
for (const name of ['SIGINT', 'SIGTERM']) process.once(name, requestStop);

try {
  const directory = required('HUMCHAT_STATE_DIR');
  checkpoints = await FileCheckpointStore.open(join(directory, 'checkpoints.json'));
  outbox = await FileOutboxStore.open(join(directory, 'outbox.json'));
  const identity = await client.connect();
  const sub = client.realtime.subscribe(required('HUMCHAT_CHANNEL_ID'), { start: 'now' });
  sub.on('error', error => {
    console.error(error.code);
    if (!error.retryable) { process.exitCode = 1; requestStop(); }
  });
  // Register synchronously, before awaiting ready or any other work.
  sub.consume((event, { signal }) => {
    const work = (async () => {
      if (event.kind !== 'message' || event.state.senderId === identity.actorId) return;
      if (event.state.body.trim() !== '!status') return;
      const key = 'reply-' + createHash('sha256').update(event.eventId).digest('hex');
      await client.outbox.send({
        consumerId, key, store: outbox,
        message: {
          channelId: event.channelId,
          threadRootId: event.state.threadRootId ?? event.state.id,
          content: 'All systems operational.',
        },
      }, { signal });
    })();
    pending.add(work);
    return work.finally(() => pending.delete(work));
  }, { consumerId, checkpointStore: checkpoints });
  await Promise.race([sub.ready, stopped]);
  await stopped;
} finally {
  await client.close();
  // Client close cannot forcibly stop arbitrary application promises.
  await Promise.allSettled([...pending]);
  try { await checkpoints?.close(); }
  finally { await outbox?.close(); }
}
```

The callback's returned promise includes the outbox write/send/receipt. Returning early or launching unawaited work would acknowledge input before that work completes. The shutdown waits above can also wait for an uncooperative application/store promise; give custom I/O its own deadlines and a deliberate shutdown policy. No SDK can roll back an external effect that already happened.

## Starts, ordering and acknowledgements

By default, one physical socket serves one canonical channel within a client. Opt-in [multiplex transport](MULTIPLEX.md) shares one physical socket across logical channels. Multiple handles can observe a canonical channel, but only one `consume()` registration is allowed, before startup. An existing checkpoint wins over `start`. Without a checkpoint, `'now'` chooses the initial server watermark, `'earliest'` chooses the position immediately before the first retained event, and `{ cursor }` chooses an explicit position. The first chosen position is persisted before callbacks begin.

Each durable transition is delivered in this order:

1. Await `event` observers.
2. Await the corresponding `message`, `messageUpdated`, `messageDeleted` or `reactionUpdated` observers.
3. Await the acknowledged consumer, if registered.
4. Atomically compare-and-set its checkpoint from the previous cursor to the event cursor.

Only then does the worker advance. A `checkpoint` notification follows a successful write. Every observer therefore needs to be safe to repeat too; a failing observer can prevent the acknowledged consumer from running. Without `consume()`, the worker has only an in-memory cursor. Observing `event` alone does not create a persistent acknowledgement.

Ordering is per channel; different channels progress independently. An event contains the full canonical state at that transition. Processing an old event by fetching today's state can erase the transition you intended to process. `eventId` is a stable event identity within its stream; include server/organization/channel scope in a shared application inbox key. Sequence values are decimal strings and are not timestamps.

Rejected/timed-out delivery, failed checkpoint writes and lost checkpoint CAS ownership block the channel without acknowledging the failed event. Automatic reconnect waits for an interrupted consumer invocation to settle before replaying it. Its original handler deadline remains active; if it ignores cancellation beyond that deadline, the channel blocks. This settlement barrier applies to the acknowledged consumer, not arbitrary observational `on()` promises or a manually created replacement client.

After correcting a blocked failure, settle or reconcile outstanding effects, close all handles for that channel, and create a new subscription using the same consumer/store. A persisted checkpoint can then resume. Changing consumer identity to bypass a conflict creates a different persistence scope and can repeat work.

## Reconnect and coverage

Durable mode negotiates a ready watermark on the WebSocket and reads bounded HTTP pages from the event log. Socket events are progress hints; a five-second HTTP poll also detects a missed final hint. This means receiving a socket hint is not itself an acknowledgement. Replay keeps a captured upper watermark while paging, then processes later progress separately.

`recoveryComplete` with `mode: 'durable', complete: true, cursor` means that captured watermark was processed. It does not promise future delivery, pre-migration history, or availability after retention expires. `ready` resolves after initial recovery and deliveries complete; install listeners and `consume()` before waiting for it. `resync()` schedules work and can be called inside a handler; its returned promise is not an attempt-completion signal.

Transient network failures and WebSocket HTTP upgrade 408/429/502/503/504 retry using exponential full jitter and Retry-After. Reconnect continues until closed or blocked; the HTTP `maxRetries` setting is not a reconnect limit. A connection stable for 30 seconds resets the backoff. Ping/pong detects dead sockets. One failed 401 handshake can refresh; repeated denial or channel 403/404 blocks the channel. Changed credentials rotate sockets after same-principal verification. Explicit revocation ends the entire client's credential lifecycle.

## Writes and uncertain outcomes

HTTP GETs and SDK send/mutation requests may retry network failures, attempt timeouts and HTTP 408/429/502/503/504. Other 5xx responses, validation failures, conflicts and permission failures are not automatically retried. HTTP 429 Retry-After also establishes a shared HTTP cooldown. Default settings allow three retries with 250 ms initial/2 s maximum jitter, within a 30 s total operation budget; a server Retry-After can consume the remaining budget.

Retries preserve the serialized request, message ID/idempotency key or mutation operation ID. A timeout does not prove the server rolled back. For a manual retry after an uncertain outcome, keep the original intent and IDs. Error context includes generated IDs when available. Calling `send()` again without them creates a different message intent. After a stale-revision conflict, fetch current state and make a new deliberate decision with a new operation ID; do not bind changed intent to an old ID.

The outbox automates persistent send intent. It first stores the exact request and generated IDs, sends it, then records the receipt before returning. Repeating the same scoped key/request reuses that intent; changed content, IDs, thread, priority or idempotency options raise `ConflictError`. Even replacing an originally omitted ID with its generated value changes the stored caller request: repeat the original input shape/defaults. `flush()` retries pending intents sequentially for one consumer/channel and stops at the first failure.

Checkpoint and outbox files are separate transactions. Safe responder order is persist/send/record the reply, then return so input can be acknowledged. A crash between server acceptance and receipt persistence can still resend the same intent. Server idempotency-record retention is **unverified**; reconcile very old uncertain sends with the deployment owner before assuming replay remains safe indefinitely. The outbox does not make third-party side effects exactly once.

## Retention gaps and upgrades

The server records writes committed after event-log migration; existing historical messages are not backfilled as events. All writer instances sharing the database must be upgraded before durable coverage can be relied on. A mixed writer deployment can miss events even if one endpoint advertises the new capability.

The preview server's minimum retention is seven days, configurable upward by its operator. Automatic pruning is disabled. Its maintenance helper deletes a contiguous expired prefix and moves the floor atomically. Actual storage sizing, pruning ownership and recovery operations remain deployment decisions; the normalized SDK capability object does not expose retention metadata.

An expired cursor or replaced epoch raises `RecoveryGapError`, with `earliestCursor`/`earliestSequence` when available, and stops consumption. The SDK does not silently jump to the new floor. Current-state history can rebuild a view; it cannot reconstruct expired business events.

To resume after a gap, stop the stream owner, settle effects, and decide how to reconcile missing work. Preserve the old checkpoint for audit. An application-controlled atomic checkpoint change to the accepted cursor is one explicit recovery mechanism; it must preserve the full persistence scope and detect concurrent ownership. Merely setting `start: 'earliest'` does not override an existing checkpoint. Starting a new consumer ID also changes outbox scope and can duplicate effects, so it is not an automatic repair.

## Store ownership and limits

Persistence scope is the tuple of server origin, organization, verified actor, consumer ID and channel. Keep all five in custom database adapters. Checkpoint CAS must be atomic; returning success after a lost race is unsafe. CAS detects conflicting progress but does not prevent two owners from starting the same external effect. Distributed workers additionally need an application lease/fencing and idempotent-effect design. Bound adapter I/O yourself; the checkpoint interface supplies no cancellation signal.

Memory stores lose progress on restart. File stores validate their document, serialize operations, fsync a temporary file and atomically replace the destination on supported local filesystems. They hold an exclusive sibling `.lock` for their entire lifetime. Confirm the exact previous process is gone before manually removing a stale lock; locks are never automatically stolen. Close the client, settle application work, then close caller-owned stores.

Store files contain message content and are not encrypted by the SDK. Use a private persistent directory and appropriate OS permissions. The 64 MiB default is per file; completed outbox records remain until application-managed archival. The SDK has no prune/reset/archive API. Size exhaustion, corruption and lock conflicts fail explicitly. Network filesystems, abrupt power loss, directory-entry durability and multi-host ownership are **unverified**.

## Channels, buffers and compatibility mode

| Setting | Default / scope |
| --- | --- |
| `realtime.maxChannels` | 50 canonical channels per client; extra admission throws, without evicting existing channels. |
| `realtime.connectConcurrency` | 5 simultaneous channel admissions (logical registrations on a multiplexed socket). Durable mode releases a slot before checkpoint/replay work; best effort holds it through initial recovery. |
| `maxBufferedEvents`, `maxBufferedBytes` | 1,000 events / 16 MiB shared across channels, including in-flight delivery. |
| `maxPayloadBytes` | 10 MiB per WebSocket frame; HTTP bodies have a separate fixed 10 MiB SDK limit. |
| `maxCachedMessages`, `maxCachedBytes` | Best-effort state cache: 1,000 messages / 8 MiB per channel, with LRU eviction. |
| `reconnectInitialDelayMs`, `reconnectMaxDelayMs` | 500 ms / 30 seconds. |
| `pingIntervalMs`, `pongTimeoutMs`, `handlerTimeoutMs` | 30 / 10 / 30 seconds. |
| `recovery.maxBytes` | 16 MiB; per decoded page in durable mode, bounded snapshot scan in best-effort mode. |
| `recovery.maxPages`, `maxThreads`, `maxStateRefresh`, `timeoutMs` | Best-effort only: 3 pages per root/thread scan, 20 known thread roots, 50 cached state refreshes, 30 seconds. They do not truncate durable backlogs. |

Durable pages are capped by the server at 200 events and an 8 MiB budget. A page larger than configured local delivery limits blocks with `BackpressureError`. If another channel temporarily occupies the shared queue budget, the durable worker reports retryable backpressure and reconnects from its existing cursor/checkpoint. These cases have different remedies; inspect `retryable`.

Best-effort mode opens the socket before scanning current history, buffers live frames, then reconciles and drains. Initial history uses `stateReconciled`, not fabricated old command events. Reconnect can infer creations as `message` with `source: 'recovery', replayed: true`; this inference does not establish complete coverage. A finished attempt emits both `recoveryIncomplete` and `recoveryComplete` with `complete: false`. Bounded cache/deduplication cannot survive process restart or eviction.

In best-effort queues, transient frames may be dropped and counted; overflowing message/mutation delivery blocks instead of silently losing it. Control notifications are observational and coalesced: one active and one latest pending value per kind/handle. A slow control observer can miss intermediate connection/checkpoint notifications. Read `status` and persistent state rather than treating notification counts as an audit log; close does not guarantee a final `connection: closed` callback.

For many channels, inspect `getStats()`, keep handlers bounded, and distribute channel ownership with application leases. The default transport uses one socket per channel; [multiplex transport](MULTIPLEX.md) can share one socket on an upgraded server. Signed [outgoing webhooks](WEBHOOKS.md) offer HTTP delivery on configured servers. Production bot credentials remain unverified. Increase budgets only after deployment-specific tests.

## Outgoing webhook recovery

Webhooks use committed event snapshots and persisted per-channel delivery state. HTTP acknowledgement loss can redeliver an event. Receivers must verify raw-body signatures and persist scoped deduplication before acknowledging. Dead letters and retention gaps stop a channel; they do not silently advance its cursor. See [Webhooks](WEBHOOKS.md) for retry limits, explicit replay, revocation, secret rotation and crash recovery.
