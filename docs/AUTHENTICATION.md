# Authentication and client setup

`@humchat/node` accepts a caller-supplied session token through `tokenProvider`. The unpublished alpha.8 preview also supports delegated member login through `auth: { sdkKey, actAs }`. Supplying a top-level `apiKey` property still throws `UnsupportedFeatureError`; a key must never be passed as a Bearer token.

SDK-key exchange happens entirely through HumChat. `connect()` and lazy resource calls mint once, verify `/me`, and discover capabilities before exposing a session. No identity-provider URL or client credentials belong in SDK configuration. See the [login guide](SDK_KEY_LOGIN_GUIDE.md) for setup, the server contract and deployment validation requirements.

The two authentication modes are exclusive at runtime and in TypeScript. Supplying both properties, even with one explicitly `undefined`, raises `ValidationError`. SDK-key mode requires a canonical UUID organization, an `hsk_` key containing only base64url characters after its prefix (at most 128 characters total), and a member email. It rejects browser/window/worker environments and requires HTTPS except for the literal development origins `localhost`, `127.0.0.1` and `[::1]`. Existing token-provider mode retains support for opaque/mock organization IDs. `getConfig().authentication` identifies the selected mode without including credentials or the requested member email.

This alpha has a public source repository and is unpublished on npm. Node.js 22 or later is required. Build a local tarball or use a reviewed archive; registry access, licensing and support commitments remain pending. See the [README](../README.md) for the current package version and installation instructions.

## Minimal configuration

For built-in member login in alpha.8:

```js
import { HumChat } from '@humchat/node';

const client = new HumChat({
  orgId: process.env.HUMCHAT_ORG_ID,
  baseUrl: process.env.HUMCHAT_BASE_URL,
  auth: { sdkKey: process.env.HUMCHAT_SDK_KEY, actAs: process.env.HUMCHAT_ACT_AS },
});
try {
  await client.connect();
  const members = await client.members.list();
  // Use an authorized recipient's actorId with messages.sendDirect().
} finally {
  await client.close();
}
```

An org owner/admin creates a key in HumChat using `POST /api/workspace/sdk-keys` with `{ "name": "Backend integration" }`. The returned `key` is shown only once. Administrators can list metadata with `GET /api/workspace/sdk-keys` and revoke with `DELETE /api/workspace/sdk-keys/{key_id}`. At most 50 active keys are permitted per org. Give the key to the backend operator privately; delegated messaging credentials are not intended for these admin operations.

The SDK snapshots and normalizes the member email, sends `org_uuid`, `email` and `expires_in: 900` to `POST /api/sdk/auth/user-token`, and puts the key only in `X-HumChat-Sdk-Key`. It disables redirects and uses Bearer tokens for subsequent calls. It validates `token_type`, token shape and a positive integer response lifetime of at most 3600 seconds; a malformed success raises `ProtocolError`. Expiry is measured from the start of the successful mint attempt, then capped by `/me`'s verified expiry.

Acquisition is shared across concurrent callers, cached, and renewed through the same mint endpoint when due. It uses the existing up-to-60-second early refresh schedule and one coordinated retry after an eligible resource HTTP 401. Non-replayable conversation creation and one-time admin-secret operations retain their existing retry exclusions. Mint 401/403 failures are terminal for this client; 429/502/503 and transient transport failures use bounded backoff within the acquisition deadline. Repeating a mint after a lost response can issue another short-lived token. There is no refresh token and no mint on every normal request.

The configured organization and normalized `actAs` must match the server-verified identity. Login bootstraps at a fixed endpoint/header because capability discovery itself requires a token. The discovered SDK-key capability must be configured and match that supported path/header; it cannot redirect the credential. Existing token-provider clients do not require that extra capability.

Native key renewal supports all three socket transports and durable checkpoint recovery. Proactive renewal works without explicit socket expiry codes. When `authentication.socketSessionEndCodes` advertises `TOKEN_EXPIRED`, a frame carrying that exact code can invalidate the old token and trigger verified reacquisition/reconnect. Verify that behavior against the configured server before relying on it. An ambiguous `session_revoked` stays terminal. Key revocation stops future mints; issued tokens may remain valid until expiry unless separately revoked or denied by access checks.

Transient scheduled-renewal failures use socket reconnect backoff after the bounded HTTP attempts finish. Native mint 401/403 failures are cached as terminal for the client, preventing a failed key/member from being retried by every channel or later HTTP call. Supply valid credentials in a new client after resolving the failure. This differs from a channel-specific permission denial, which does not revoke unrelated channels.

After a retryable SDK-key acquisition fails, all HTTP and socket callers also share an authentication cooldown. The next acquisition waits 1–2 seconds initially, doubling after consecutive failures to a 15–30-second range; `Retry-After` can extend that wait. The delay is within the acquisition deadline and resets after mint, identity verification and capability discovery all succeed. Closing the client or receiving terminal revocation cancels the shared wait. Cancelling one caller only detaches that caller. This prevents many channels from repeatedly minting after a fast outage response; it does not coordinate quotas across separate clients/processes or change the existing `tokenProvider` behavior.

For an advanced caller-managed token, keep the existing provider form below.

Save this as `send.mjs`, install the package, set the four environment variables below, then run `node send.mjs`:

```js
import { HumChat } from '@humchat/node';

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Set ${name}`);
  return value;
}

const client = new HumChat({
  orgId: required('HUMCHAT_ORG_ID'),
  baseUrl: required('HUMCHAT_BASE_URL'),
  tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }),
  timeout: 10_000,
});

try {
  const identity = await client.connect();
  const receipt = await client.messages.send({
    channelId: required('HUMCHAT_CHANNEL_ID'),
    content: 'Hello from my backend.',
  });
  console.log({ actorId: identity.actorId, messageId: receipt.id });
} finally {
  await client.close();
}
```

The environment-token provider above is static. It does not log in, renew a token or survive token expiry. Use it for a short supplied-session test. A long-running application needs its own supported credential acquisition and renewal flow.

`baseUrl` remains explicit. It must be an HTTP(S) origin such as `https://chat.example.invalid`, with no credentials, path prefix, query or fragment. Use HTTPS outside local development. The SDK sends Bearer headers after acquisition and disables redirects. A custom `fetch` implementation receives credentials, including the SDK key during minting, so supply only an application-controlled transport; it affects HTTP, not the SDK's `ws` WebSocket implementation.

Construction performs no network work. `connect()` acquires a token, verifies `/me`, and discovers `/api/sdk/capabilities`. Resource calls connect lazily if necessary. The server must advertise send receipts v1, message state v1 and history v2 even when your application uses only one HTTP method. Durable subscriptions additionally require event protocol v1 and durable replay.

## Token provider contract

The provider receives the following context and returns a promise of `{ token, expiresAt? }`:

| Field | Meaning |
| --- | --- |
| `reason` | `initial`, `expiring`, or `unauthorized`. |
| `previous?.expiresAt` | Effective expiry of the preceding session, in Unix seconds, or `null` when unknown. The old token is not included. |
| `signal` | Shared acquisition cancellation/deadline. Pass it to your credential service or other cancellable work. |
| returned `token` | A nonempty string without whitespace or control characters. |
| returned `expiresAt` | Optional future safe integer in Unix seconds, not milliseconds. |

Concurrent calls share one acquisition/refresh. Cancelling one request stops that request's wait; it does not cancel acquisition needed by other callers. Closing the client cancels the shared acquisition. Provider error messages are not copied into SDK errors.

The SDK uses the earlier of provider expiry and server-verified expiry. It refreshes up to 60 seconds before that effective expiry, or halfway through the remaining lifetime for a short-lived token. When neither expiry is known, there is no proactive expiry timer. A resource HTTP 401 triggers one coordinated refresh and one replay of that operation, within its existing total HTTP budget. A repeated failure reaches the caller.

There is no built-in OTP, refresh-token storage or refresh-token exchange. When using `tokenProvider`, implement renewal inside your provider using your application's confirmed authentication contract. SDK-key mode instead renews by minting through HumChat. A separate bot/service-identity token contract remains **unverified**; delegated member login does not establish bot permissions or lifecycle.

## Identity, scope and revocation

`connect()` returns `{ actorId, orgId, email, roles, scopes, expiresAt, appId }`. Its `expiresAt` is the server-reported expiry and can be `null` even if the provider supplied an expiry for scheduling. `scopes` defaults to an empty array and `appId` to `null` when the server does not provide verified values. Do not infer service-identity permissions from these defaults.

The configured organization must match the verified identity. The first successful connection binds the client to its actor and organization. Refresh cannot switch either; a mismatch blocks that client with `AuthenticationError`. Create a new client for a different principal. Returned identity and capability objects are copies; changing them does not change authorization.

The server checks channel access. An organization match is not a channel grant. Use `channels.list()` and `conversations.list()` for discovery, and use the returned canonical routing key: `Channel.id` or `Conversation.channelId`. A conversation's management `id` is not its messaging key.

An actual real-time revocation blocks all subscriptions and future authenticated work and aborts pending acquisition. The SDK does not refresh around revocation. Only the separately negotiated `TOKEN_EXPIRED` code permits renewal; prose such as `reason: "session expired"` alone does not. Re-establish credentials through the application's authorized lifecycle and create a new client after terminal revocation. `close()` cancels the SDK's work; it does not log out or revoke a token shared with other applications.

`getConfig()` excludes the provider, SDK key, requested email and token. Internal SDK-key credentials use JavaScript private fields or closures rather than inspectable configuration. Transport errors discard response prose and credential-bearing request IDs. `getCapabilities()` exposes normalized protocol/authentication flags, including nullable `authentication.sdkKeyUserTokens`; it does not currently expose the server's retention-policy metadata. Neither method grants permission. Returned verified identities intentionally contain the member email.

Request-ID redaction covers credentials from earlier renewals, including rejected mint results. The client privately remembers up to 128 distinct credentials short enough to fit the 128-character request-ID limit. If that history fills, it permanently omits remote request IDs from errors; error types, statuses and retry behavior remain available. This bounds memory without forgetting old credentials while exposing server-controlled IDs. Mint responses containing the SDK key, or starting with `hsk_`, are rejected before Bearer verification.

## Local development

The HumChat server's local fixture supports mock session tokens only in a debug/test build configured for mock authentication. For example, `mock-jwt-token-123::org-mock-1` is a local fixture token for `org-mock-1`; it is not a production credential. Obtain the local server origin and existing channel key from the fixture operator. The server and its fixture are maintained separately; this repository and npm package do not start PostgreSQL or a HumChat server.

Related guides: [API](API.md), [reliability](RELIABILITY.md), [troubleshooting](TROUBLESHOOTING.md).
