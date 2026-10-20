# Outgoing webhooks

Private alpha.6 adds administrator-managed outgoing events and `verifyWebhook()`. A receiver accepts signed HTTP POSTs without maintaining a WebSocket worker. Sending messages still uses the normal authenticated SDK. Production machine-token authentication remains a separate requirement.

## Server setup and authorization

Use the upgraded PostgreSQL server with durable event storage and a stable `HUMCHAT_WEBHOOK_ENCRYPTION_KEY`: 32 random bytes encoded as 64 hexadecimal characters. Keep the same key on every instance in your secret manager. The server encrypts signing secrets using AES-256-GCM, binding ciphertext to the organization, subscription and secret version. Missing/invalid configuration disables the capability; SQLite test builds do not support delivery.

Subscriptions are **organization-owned grants**, created and managed by a currently authenticated organization/workspace administrator. They survive the creator's logout; a user session token is never stored in the webhook database. Pausing or deleting the subscription revokes future delivery. Every delivery rechecks the active grant and channel scope. Already-transmitted HTTP requests cannot be recalled.

Each subscription selects 1–50 existing workspace channels in one organization; DMs/conversations are not supported. An organization may have at most 100 nondeleted subscriptions. URLs and channel sets are immutable: create a replacement subscription to change them. New grants capture each channel's committed head and start **paused**, so earlier history is not sent. Install the returned signing secret at the receiver, then explicitly resume; retained events committed since registration will be delivered. Record the cutover when replacing a subscription; it is not an atomic transfer between destinations.

`getCapabilities().messaging.outgoingWebhooks` is null on unsupported/unconfigured servers. On an enabled server it describes schema version 1, the channel limit and workspace-only scope. The existing WebSocket transports remain available independently.

## Register and manage a destination

```ts
import { randomUUID } from 'node:crypto';
import { HumChat } from '@humchat/node';

const client = new HumChat({ orgId, baseUrl, tokenProvider });
const id = randomUUID(); // Persist before requesting creation.
const created = await client.webhooks.create({
  id,
  url: 'https://your-backend.example/humchat/webhook',
  channelIds: [channelId],
});
// Save created.signingSecret in your secret manager immediately.
// Configure the receiving app before enabling delivery.
await client.webhooks.resume(id);
const subscription = await client.webhooks.get(id);
await client.webhooks.pause(id);
await client.webhooks.resume(id);
const page = await client.webhooks.deliveries(id, { status: 'dead', limit: 20 });
if (page.deliveries[0]) await client.webhooks.replay(id, page.deliveries[0].id);
await client.close();
```

| Method | Result |
| --- | --- |
| `webhooks.create({ id, url, channelIds }, options?)` | `{ subscription, signingSecret }` |
| `webhooks.list(options?)` | `WebhookSubscription[]` |
| `webhooks.get(id, options?)` | `WebhookSubscription` |
| `webhooks.pause(id, options?)` / `resume(id, options?)` | Updated subscription |
| `webhooks.rotateSecret(id, options?)` | `{ subscription, signingSecret }` |
| `webhooks.delete(id, options?)` | `{ id, deleted: true }` |
| `webhooks.deliveries(id, { status?, limit?, before? }, options?)` | `{ deliveries, nextBefore }`; limit 1–100 |
| `webhooks.replay(id, deliveryId, options?)` | Updated delivery |

Management mutations are single-attempt: no HTTP retry or automatic 401 replay. A timeout may still mean the operation committed. Use the persisted subscription ID and `get()`/delivery inspection to reconcile. Creation and rotation reveal a signing secret only in their successful response; list/get/errors never return it. A lost creation response requires reconciling the ID and rotating the secret. A lost rotation response may require waiting for the current rotation grace period before rotating again. Message-send idempotency is unchanged.

## Verify before parsing or acknowledging

The signature header is `X-HumChat-Signature: t=<Unix seconds>,v1=<hex HMAC>`. During rotation it contains a second `v1` signature. The signed bytes are the ASCII timestamp, a period, and the exact HTTP request body. The key is the 32 decoded bytes in `whsec_<64 lowercase hex>`, not the text of that prefix/string.

```ts
import { verifyWebhook } from '@humchat/node';

const envelope = verifyWebhook(rawBody, signatureHeader, signingSecrets, {
  expectedOrgId: orgId,
  expectedSubscriptionId: subscriptionId,
  expectedChannelId: channelId,
});
// Atomically persist a deduplicated inbox record before responding with 2xx.
// Key: orgId + subscriptionId + channelId + eventId.
```

Pass raw bytes before JSON middleware changes them. `verifyWebhook()` validates HMAC with constant-time comparison, timestamp freshness (300-second default tolerance), the schema and nested event scope. It accepts one or two signing secrets. Invalid/tampered/oversized payloads raise `WebhookVerificationError` without exposing payload or secret data. Enforce the size limit while reading the request too; verification cannot undo an oversized HTTP allocation. Keep receiver clocks synchronized.

The verified envelope contains `schema`, `subscriptionId`, `deliveryId`, `orgId`, `channelId`, `eventId`, `attempt`, `sentAt`, and `event`. The nested `event` is the same normalized `DurableEvent` used by durable WebSocket consumers, including its immutable message state and decimal-string channel sequence. Kinds are `message`, `edit`, `delete` and `reaction`; replies are message events with a thread root. Typing, presence and receipts are not durable webhook events.

## Delivery, retries and recovery

- Delivery is at least once. A lost HTTP acknowledgement can produce the same event again, including after a server restart. Persist deduplication and business changes atomically, or use an inbox/outbox for external effects.
- A subscription/channel has one ordered outstanding delivery. Later events in that channel wait for acknowledgement; other channels can continue. There is no global order across channels or subscriptions.
- A delivery ID and scoped event identity remain stable across automatic retries and manual replay. `sentAt`, the signature and `attempt` change per attempt. A manual replay starts a new bounded retry window; do not deduplicate by timestamp or attempt number.
- Only 2xx acknowledges delivery. Network/DNS failures, 408, 429 and 5xx retry with jitter. `Retry-After` seconds or HTTP dates are honored within a one-hour bound. A window permits at most eight attempts and 24 hours. Other HTTP responses become dead letters; 410 also pauses the subscription. Redirects are never followed.
- A dead letter stops that channel. Inspect its sanitized error/status, fix the receiver, resume a paused subscription when needed, then replay the failed delivery. Replay does not silently skip the failed event or rewind already acknowledged history.
- Missing retained history or a changed channel epoch stops the channel with a gap. `resume()` rejects a detected gap with HTTP 409. Investigate the lost range and explicitly create a new subscription if starting at the current head is acceptable. The SDK never performs this reset automatically.
- HTTP requests have a five-second bound, response-body reads have a 16 KiB budget, and outgoing bodies an 8 MiB bound. Larger response bodies are discarded; classification still follows the HTTP status. Keep acknowledgement fast by durably accepting into an inbox and doing longer work afterward.

Secret rotation keeps the previous signing secret for 300 seconds, signing each new attempt with both keys during that window. Install the returned new secret before the grace period ends. A second rotation during the window returns 409. Keep the previous verification key during the transition, then remove it. Encryption-key rotation is an operator migration, not this endpoint; do not replace the server encryption key without migrating stored ciphertext.

## Receiver example

Run the shipped [receiver](../examples/webhook-receiver.mjs) after installing the tarball. Set `HUMCHAT_WEBHOOK_SECRET`, `HUMCHAT_ORG_ID`, `HUMCHAT_WEBHOOK_SUBSCRIPTION_ID`, comma-separated `HUMCHAT_CHANNEL_IDS`, and a private `HUMCHAT_WEBHOOK_STATE_DIR`. `HUMCHAT_WEBHOOK_PREVIOUS_SECRET` is optional during rotation. `HUMCHAT_WEBHOOK_PORT` defaults to 43140.

```sh
node node_modules/@humchat/node/examples/webhook-receiver.mjs
```

The example binds only to `127.0.0.1` and accepts `POST /humchat/webhook`. An HTTPS reverse proxy is needed for a public destination. It saves a scoped deduplicated inbox record before returning success; it does not claim exactly-once arbitrary business effects. Its file store is single-process, bounded and requires private durable local storage. Clean shutdown releases its lock; after a crash, inspect the owner and remove a stale lock manually before restarting. Use database-backed atomic inbox processing for distributed production receivers.

## Destination and operational limits

Normal destinations must use HTTPS without embedded credentials or fragments. The dispatcher rejects local/private/metadata/reserved IPs, checks all DNS answers and pins the HTTP connection to validated addresses while preserving TLS hostname verification. Proxies and redirects are disabled. A debug build with mock authentication has a separate exact-origin loopback fixture exception; it is unavailable to production authentication/release builds.

Retain the server encryption key, monitor delivery backlog/dead letters/gaps, and assign ownership for database storage and recovery. Delivery inspection omits message bodies and secrets. Deletion revokes the grant and clears its encrypted signing keys; delivery/history rows remain in storage. No automatic webhook-table pruning is configured in this alpha. This alpha does not establish a production latency/capacity SLO, an independent integration pilot, machine credentials or npm publication. See [Authentication](AUTHENTICATION.md), [Reliability](RELIABILITY.md), and [Releasing](RELEASING.md).
