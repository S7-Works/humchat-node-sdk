# Direct messages and the managed inbox

The SDK acts as the user returned by `client.connect()`. Messages to that identity are normal HumChat conversations; messages sent by the SDK use that same identity. The server decides which member an SDK key may sign in as and which conversations that member may access. This feature does not create a bot or bypass organization and conversation permissions. The server must expose `/api/sdk/capabilities` and the existing roster/conversation routes. Receiving also requires durable event protocol v1.

## Sign in through HumChat

The unreleased SDK-key branch supports this setup for the send and receive examples below:

```js
import { HumChat } from '@humchat/node';

const client = new HumChat({
  baseUrl: process.env.HUMCHAT_BASE_URL,
  orgId: process.env.HUMCHAT_ORG_ID,
  auth: {
    sdkKey: process.env.HUMCHAT_SDK_KEY,
    actAs: process.env.HUMCHAT_ACT_AS,
  },
});
await client.connect();
// Use the send and inbox examples below; close the client during shutdown.
```

Exchange and automatic renewal go only through HumChat. A caller-supplied `tokenProvider` remains an alternative; do not combine it with `auth`. The older direct identity-provider app-login proposal and top-level `apiKey` form are not implemented by this contract. See [authentication](AUTHENTICATION.md) and the [updated login guide](SDK_KEY_LOGIN_GUIDE.md).

## Find a recipient and send

```js
const members = await client.members.list();
const recipient = members.find(member => member.email === 'colleague@example.com' && member.status === 'active');
if (!recipient) throw new Error('Choose an accessible active organization member');
const { conversation, receipt } = await client.messages.sendDirect({
  recipientId: recipient.actorId, content: 'Build completed.',
});
console.log(conversation.channelId, receipt.id);
```

`members.list()` returns `{ actorId, name, email, role, status }[]` from the authenticated organization's roster. Guest visibility and creation permissions are decided by the server. The roster response currently has no required organization field; the SDK relies on its verified, organization-bound session and rejects a mismatched optional `org_uuid` if present. Names and email addresses are display/search data; use the exact `actorId` to address a recipient.

`conversations.openDirect({ memberId })` first finds an unnamed conversation with exactly the caller and that member, then calls `conversations.create({ memberIds: [memberId] })` if necessary. It rejects self-DMs. `create({ memberIds, name? })` also supports group conversations; the caller is always included by the server. Returned `Conversation.channelId` is the canonical routing key used by all message/replay APIs. The management `id` is not a routing key.

Creation has no idempotency-key contract. The SDK makes only one POST attempt, including after a 401; an ambiguous failure can leave a conversation created on the server. Reconcile with `conversations.list()`/`openDirect()` before trying again. Server find-before-create behavior is not a uniqueness guarantee under concurrent callers. Named creation intentionally creates a separate room. Send retries use the normal stable message ID and idempotency key; supply both when preserving intent across separate application attempts, or open the DM and use the persistent [outbox](RELIABILITY.md). `sendDirect` and `openDirect` bound their combined HTTP steps by `retry.totalTimeoutMs`.

The recipient can read these messages through HumChat's normal conversation/history APIs. An already-open app must refresh or discover new conversations to show a newly created DM in its sidebar; the SDK cannot update a separate app's UI.

## Watch incoming conversations

```js
import { FileCheckpointStore } from '@humchat/node';
const checkpoints = await FileCheckpointStore.open('./private-state/inbox.json');
const me = await client.connect();
const inbox = client.inbox.watch(async (event, { signal }) => {
  if (event.kind !== 'message' || event.state.senderId === me.actorId) return;
  // Await your durable business processing here; observe signal when cancelled.
  await processIncoming(event, { signal });
}, { consumerId: 'my-inbox-v1', checkpointStore: checkpoints });
try {
  await inbox.ready;
  await inbox.done;
} finally {
  await inbox.close();
  await client.close();
  // Settle any application promises before closing their stores.
  await checkpoints.close();
}
```

The watcher polls `conversations.list()` every 5000 ms after the preceding discovery finishes. It includes both direct and group conversations the identity belongs to, including its own sent messages and historical mutations. It does not watch arbitrary workspace channels. `pollIntervalMs` can be changed (minimum 100 ms), and `refresh()` performs an immediate, coalesced discovery request. Polling is an interim discovery mechanism, not a server-wide identity event stream.

Each newly discovered channel registers an acknowledged consumer with `start: 'earliest'`. A saved checkpoint takes precedence. This recovers a retained message committed **before** discovery; using the ordinary subscription default of `'now'` would skip it. On first use, existing conversations also replay their retained histories, potentially including old commands. Choose a new consumer's business cutover deliberately and make external effects idempotent. No claim covers events already pruned before first discovery or conversations that disappear entirely between polls. Persisted retention gaps block instead of skipping data.

| Handle / option | Behavior |
| --- | --- |
| `ready` | Initial capability check, discovery and consumer registration completed; per-channel replay may still be running. Rejects initial discovery/admission failures. |
| `done` | Rejects any terminal discovery, channel, checkpoint or handler failure. Resolves on explicit close. Always observe it, including after `ready`. |
| `status`, `error` | `starting`, `watching`, `blocked` or `closed`, plus the terminal error when present. `watching` describes the discovery loop, not every socket's connection state. |
| `lastChannelError` | Most recent channel error, including transient reconnect failures; error messages exclude raw server bodies. |
| `getStats()` | Watched conversation count and each channel's connection state. |
| `refresh()` | Coalesces with an existing discovery; errors are also reflected by `done`. |
| `maxConversations` | Defaults to `realtime.maxChannels` (50). Exceeding it explicitly blocks the watcher; nothing is silently omitted. Existing manual subscriptions share the same client channel budget. |
| `close()` | Cancels discovery/timers and closes owned handles. `client.close()` also closes the watcher. Caller-owned stores remain open. |

There is one active watcher per client and one acknowledged consumer per channel. Do not mix the watcher with independently consumed subscriptions to the same conversations. The existing connection-concurrency limit bounds connection admission, and callbacks are serialized per channel; different channels can process concurrently up to the admitted channel count. Optional multiplex transport shares one socket, while preserving these per-channel checkpoints.

A conversation missing from a successful subsequent list has its owned subscription closed. Discovery waits for its interrupted consumer and pending checkpoint operation to settle before it can automatically rejoin, retaining its checkpoint. The consumer keeps its original `realtime.handlerTimeoutMs` deadline; pending checkpoint settlement is bounded by `retry.totalTimeoutMs`. A missed deadline or checkpoint failure blocks the watcher and rejects `done`. Explicit watcher/client close cancels this wait promptly; it cannot stop an application's pending promise.

A channel-level denial or discovery failure after the normal bounded HTTP retries stops the whole watcher and rejects `done`; inspect the error, close the watcher, and explicitly restart after resolving it. Transient channel disconnections retain the existing reconnect/replay behavior. Closing or permission revocation cannot undo an already-running external side effect. Observe cancellation and settle application promises before restarting ownership.

Run [dm-responder.mjs](../examples/dm-responder.mjs) for a persistent `!status` responder that filters its own messages, replies in the same thread and saves outgoing intent before acknowledging input. See [example setup](../examples/README.md).
