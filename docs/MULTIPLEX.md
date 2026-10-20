# One connection for multiple channels

Alpha.5 adds an opt-in transport that carries progress notifications for multiple channels on one physical WebSocket. Message sends still use HTTP. Each channel keeps its own durable event log, checkpoint, callback order and failure state; there is no global cross-channel sequence.

## Configure

```ts
import { HumChat } from '@humchat/node';

const client = new HumChat({
  orgId: process.env.HUMCHAT_ORG_ID!,
  baseUrl: process.env.HUMCHAT_BASE_URL!,
  tokenProvider: async () => ({ token: process.env.HUMCHAT_TOKEN! }),
  recovery: { mode: 'durable' },
  realtime: { transport: 'multiplex', maxChannels: 50, connectConcurrency: 5 },
});
const subscriptions = client.realtime.subscribeMany(['canonical-channel-a', 'canonical-channel-b']);
for (const result of subscriptions) {
  if ('error' in result) throw result.error;
  result.subscription.on('error', error => console.error(error.code));
  result.subscription.on('message', ({ message }) => console.log(message.id));
}
await Promise.all(subscriptions.map(result => {
  if ('error' in result) throw result.error;
  return result.subscription.ready;
}));
console.log(client.realtime.getStats().activeSockets); // 1 while connected
// At application shutdown: await client.close();
```

Replace the sample channel IDs with accessible canonical routing keys. The environment token is static; implement renewal in `tokenProvider` for unattended operation. For acknowledged processing, register `consume()` synchronously before awaiting startup, using the [reliability guide](RELIABILITY.md). The runnable [multiplex worker](../examples/multiplex-worker.mjs) combines this transport with file checkpoints and a send outbox.

## Requirements and migration

The server must advertise `messaging.multiplexed_events.protocol_versions: [1]` and a `max_channels` limit. `getCapabilities()` exposes this as `messaging.multiplexedEvents: { protocolVersions, maxChannels }`, or `null` when absent. The preview server accepts at most 100 registrations per connection; the SDK default remains 50 distinct channels. Raise `maxChannels` explicitly after testing your workload.

`realtime.transport` defaults to `'channel'`, using one socket per canonical channel. `'multiplex'` requires durable recovery. Missing capability fails with `UnsupportedFeatureError`; combining it with `'bestEffort'` is rejected. The SDK never silently falls back. Upgrade all serving instances and ensure the proxy supports authenticated upgrades to `/ws/events` before enabling it.

To change transport, stop the old client, settle outstanding effects, then construct a new client with the new setting. Keep the origin, organization, actor, consumer ID, channel keys and persistence stores unchanged. No cursor, checkpoint or outbox format migration is needed. Application leases must still prevent overlapping owners.

## Behavior and failures

| Situation | Behavior |
| --- | --- |
| Multiple handles for one channel | Share one logical registration; one acknowledged consumer is allowed. Closing the last handle removes that registration. |
| Dynamic subscribe/unsubscribe | Uses correlated registration IDs; stale frames cannot be delivered to a replacement registration. |
| Connection loss | One reconnect/backoff/heartbeat cycle serves the client. Each channel resumes from its last processed cursor, using the persisted checkpoint when configured. |
| Credential renewal | A verified same-actor/same-organization replacement rotates the shared connection once. Explicit session revocation blocks the client. |
| Channel denial or durable delivery/consumer handler failure | Blocks that channel; other authorized channels can continue. Correct the cause and recreate its handles deliberately. Ephemeral/control observer failures report `handlerError` without blocking. |
| Whole-stream authentication or malformed protocol failure | Affects all registrations sharing that stream. Inspect errors rather than treating socket count as readiness. |
| Missed notification | Periodic HTTP replay checks catch committed transitions. A notification is a hint, not an acknowledgement. |

`connectConcurrency` bounds logical channel admission on the shared socket. Admission is released before application handlers or checkpoint/replay I/O. `activeSockets` reports physical sockets; `channels` and `subscriptions` report logical channels and handles. One channel's backlog or handler can progress independently of another, subject to the configured shared buffer budgets. Recovery and reconnect counters are operational observations, not business-event audit counts.

Typing/read signals carry channel correlation; received typing/presence/receipt/call notifications remain ephemeral. Durable message/edit/delete/reaction bodies come from authenticated `/api/events/sync`, not the shared socket. The existing at-least-once effects, retention-gap behavior and cancellation limits in [Reliability](RELIABILITY.md) still apply.

## Deployment limits

Multiplexing reduces physical WebSocket count. It does not remove per-channel HTTP replay, database polling, authorization work or checkpoint storage. The server bounds registration/control queues and rechecks channel access around asynchronous work and delivery; already-sent bytes cannot be recalled. Avoid interpreting local debug-server measurements as production capacity.

This alpha uses caller-managed user sessions. Production service identity/API-key exchange, staging acceptance, a 24-hour pilot and registry publication remain pending. Signed [outgoing webhooks](WEBHOOKS.md) are available separately on servers that advertise the webhook capability.
