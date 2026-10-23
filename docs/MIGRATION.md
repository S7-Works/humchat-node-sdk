# Upgrading alpha packages

Pin an exact tarball/version and test the installed package before rollout. These alpha builds are unpublished on npm; minor alpha increments can change behavior. Source is public, while licensing remains pending. The current install artifact is listed in the [README](../README.md), and the [changelog](../CHANGELOG.md) records package changes.

## Version progression

| Version | Available behavior / upgrade concern |
| --- | --- |
| `0.1.0-alpha.0` | HTTP resources, caller-supplied tokens, typed errors, bounded retries and idempotent intent IDs. No real-time API. |
| `0.1.0-alpha.1` | Per-channel subscriptions and explicitly selected best-effort history/state recovery. No durable consumer/checkpoint contract. |
| `0.1.0-alpha.2` | Durable recovery becomes the default on an upgraded server. Adds event paging, `consume()`, checkpoints, outbox and explicit gap errors. |
| `0.1.0-alpha.3` | Prevents overlapping acknowledged invocations across reconnect, releases durable handshake slots before replay, and preserves permanent handshake denials. Server authorization is rechecked after database waits. |
| `0.1.0-alpha.7` | Adds member lookup, direct-message helpers and a managed conversation inbox. |
| `0.1.0-alpha.8` | Local review preview: adds native HumChat SDK-key login, renewal, shared outage backoff and capability-gated socket expiry recovery. Unpublished; production message/app and server acceptance gates remain open. |

Alpha.4 adds guides/release tooling without runtime changes. Alpha.5 adds opt-in [multiplex transport](MULTIPLEX.md), with the existing per-channel transport still the default. These changes do not imply a production authentication or publishing milestone. Check their changelog before assuming a runtime/API change. Native SDK-key authentication first appears in the alpha.8 preview described below; earlier versions require a caller-supplied token.

## HTTP-only applications

Continue using `HumChat`, `tokenProvider`, canonical channel keys and the same HTTP resource methods. `connect()` still requires capability discovery, send receipt v1, state v1 and history v2. Durable capability is checked when using durable subscriptions or `events.sync()`, not simply because you send a message.

Keep generated/supplied message IDs, idempotency keys and mutation operation IDs when retrying uncertain outcomes. Do not replace conversation `channelId` with management `id`. History iteration yields pages; message/reaction revisions remain separate. See [API](API.md).

## Keep a legacy listener intentionally

On a server without event protocol v1, explicitly retain compatibility behavior:

```js
const client = new HumChat({
  orgId, baseUrl, tokenProvider,
  recovery: { mode: 'bestEffort' },
});
```

`HumChat`, `orgId`, `baseUrl` and `tokenProvider` above are your existing import/configuration. Handle `recoveryIncomplete`; `recoveryComplete.complete` is always false in this mode. Do not add `consume()` or a durable `start` option to it. A protocol mismatch fails explicitly instead of downgrading automatically.

## Move a listener to durable consumption

1. Upgrade every server writer sharing the database and verify event protocol v1/durable capability. Checkpoint replay cannot repair writes made by old writers that bypass the event log.
2. Choose a stable consumer ID and a persistent checkpoint store with the full server/org/actor/consumer/channel scope. Use a send outbox or application inbox/idempotency key for effects that can repeat.
3. Decide the first start position: `'now'`, retained `'earliest'`, or a known event cursor. Old history cursors, `since_id` values and process-local best-effort watermarks cannot be converted into a durable checkpoint.
4. Stop the old listener and settle its work before assigning the same channel to the new owner. Register `consume()` synchronously after `subscribe()`, then await `ready`.
5. Test reconnect, restart, uncertain writes, failed callbacks and a retained-cursor gap before accepting traffic.

Do not leave a business action in an existing `message` listener and also add it to `consume()`: both deliveries run for the same durable creation. Move acknowledged work into the consumer, or explicitly keep the observer read-only. Use `event.kind === 'message'` to select creations; edit/reaction events can carry the same body and should not rerun a command accidentally. Use `event.state.body` for historical full content and preserve `state.threadRootId ?? state.id` when replying.

Durable `ready`/`recoveryComplete` mean the captured log watermark was processed. They do not mean pre-migration history was backfilled. Existing checkpoints override a new `start` option, and gaps require an explicit recovery policy. See [Reliability](RELIABILITY.md) for a complete responder and acknowledgement order.

## Moving from alpha.2 to alpha.3

Keep persisted scope and file formats unchanged; no file conversion is defined or needed for these releases. Back up state while the owning process is stopped before changing versions. A pending outbox entry must keep its original input intent, IDs and key.

Handlers that ignore cancellation can now block instead of overlapping automatically with reconnect replay. Respect the consumer signal and original handler deadline; resolve or reconcile outstanding effects before manually replacing a blocked client. A slow checkpoint/replay no longer occupies a durable handshake slot, but shared delivery budgets still apply.

A denied WebSocket handshake remains a denial, even if transport error/close events follow. Treat channel 403/404 as a permission/configuration issue; repeated reconnect is not the repair. Upgrade the server as well to obtain the guest discovery/replay allowlist and post-read authorization checks.

## Rollback and state ownership

Alpha.0/alpha.1 have no durable checkpoint/outbox consumer contract. Rolling back to them is not a transparent resume from alpha.2+ state. Preserve checkpoint/outbox files and reconcile effects before selecting a different recovery mode or consumer ID. Never run old and new file-store owners concurrently or delete a lock while its owner can still run.

The SDK supplies no automatic checkpoint reset, gap repair, store archival or distributed lease migration. Those operations require application ownership and a deliberate data-recovery decision. Public npm access, license/support arrangements, production message/app acceptance, server authorization/expiry work and the independent pilot remain pending.

## Alpha.5 to alpha.6

Webhook resources and verification helpers are additive. Existing channel and multiplex clients, checkpoints and outbox files need no format migration. Servers advertise the optional webhook capability only with PostgreSQL webhook tables and a configured encryption key. The server runs the additive migration at startup. See [Webhooks](WEBHOOKS.md) before enabling outgoing delivery; existing sessions and channel grants do not automatically create subscriptions.

## Alpha.7 to alpha.8: delegated member login

Existing `tokenProvider` applications can keep their current configuration. To let the SDK obtain and renew a member token, replace that property with `auth`:

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
```

An organization administrator obtains the key in HumChat. The SDK sends it only to that HumChat server's user-token endpoint, then uses the verified member's Bearer token for messaging and sockets. There is no direct identity-provider configuration or client credential exchange. Top-level `apiKey` still raises `UnsupportedFeatureError`.

Choose exactly one of `auth` and `tokenProvider`; omit the unused property entirely, including an explicitly undefined property. Native mode requires a UUID organization ID, an `hsk_` key, a valid member email and HTTPS outside literal loopback development hosts. It refuses browser execution. Existing token-provider mode retains opaque organization IDs for supported mock/provider environments. See [authentication](AUTHENTICATION.md) for validation and error handling.

The SDK caches and renews a verified session, shares acquisition across HTTP calls and sockets, and adds a shared cooldown after retryable native login failures. A native mint 401 or 403 blocks that client: correct credentials or access, then create a new client. Revocation remains terminal. Only an explicitly advertised `TOKEN_EXPIRED` socket code allows reactive renewal; an unknown or unadvertised `session_revoked` frame never becomes an automatic retry. Timer-based proactive renewal works independently of that server extension.

The authentication switch needs no checkpoint or outbox file conversion. Resume an existing consumer only when base URL, organization, verified actor and consumer ID remain the same. Switching `actAs` changes the actor and therefore its checkpoint scope; do not copy another member's cursors. Stop and settle the old client before a replacement takes ownership of its stores. Outbox intent IDs and idempotency keys must remain stable when retrying uncertain sends.

Member lookup, `messages.sendDirect()` and `inbox.watch()` retain their alpha.7 APIs. Messages are sent as the verified member and incoming messages are limited to conversations that member can access. Native-auth roundtrip tests pass against synthetic peers; the designated production recipient and visual app check remain outstanding. See [direct messaging](DIRECT_MESSAGES.md) and [live acceptance](SDK_KEY_MESSAGE_ACCEPTANCE.md).

Keep delegated credentials on messaging, channel, history and socket operations. The server's delegated-admin enforcement gap is still open; client configuration cannot enforce that boundary. The separate server expiry companion also needs build and deployment acceptance. Alpha.8 is a local review package, not a production acceptance or npm publication milestone. See [release gates](RELEASING.md).
