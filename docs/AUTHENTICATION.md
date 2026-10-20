# Authentication and client setup

`@humchat/node` currently accepts a caller-supplied session token. API-key exchange and a verified production service-identity flow are pending. Supplying an `apiKey` property throws `UnsupportedFeatureError`; an API key must not be passed as a Bearer token to work around that check.

This alpha has a public source repository and is unpublished on npm. Node.js 22 or later is required. Build a local tarball or use a reviewed archive; registry access, licensing and support commitments remain pending. See the [README](../README.md) for the current package version and installation instructions.

## Minimal configuration

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

`baseUrl` is required because the official production origin is unverified. It must be an HTTP(S) origin such as `https://chat.example.invalid`, with no credentials, path prefix, query or fragment. Use HTTPS outside local development. The SDK sends Bearer headers and disables redirects. A custom `fetch` implementation receives credentials, so supply only an application-controlled transport; it affects HTTP, not the SDK's `ws` WebSocket implementation.

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

There is no built-in OTP, refresh-token storage or refresh-token exchange. Implement renewal inside your provider using your application's confirmed authentication contract. The exact production machine-token issuance, actor kind, action/channel grants, rotation and revocation contract remains **unverified**. Human OTP examples do not establish that contract.

## Identity, scope and revocation

`connect()` returns `{ actorId, orgId, email, roles, scopes, expiresAt, appId }`. Its `expiresAt` is the server-reported expiry and can be `null` even if the provider supplied an expiry for scheduling. `scopes` defaults to an empty array and `appId` to `null` when the server does not provide verified values. Do not infer service-identity permissions from these defaults.

The configured organization must match the verified identity. The first successful connection binds the client to its actor and organization. Refresh cannot switch either; a mismatch blocks that client with `AuthenticationError`. Create a new client for a different principal. Returned identity and capability objects are copies; changing them does not change authorization.

The server checks channel access. An organization match is not a channel grant. Use `channels.list()` and `conversations.list()` for discovery, and use the returned canonical routing key: `Channel.id` or `Conversation.channelId`. A conversation's management `id` is not its messaging key.

An explicit real-time `session_revoked` blocks all subscriptions and future authenticated work on the client. The SDK does not refresh around that revocation. Re-establish credentials through the application's authorized lifecycle and create a new client. `close()` cancels the SDK's work; it does not log out or revoke a token shared with other applications.

`getConfig()` excludes the provider and token. `getCapabilities()` exposes normalized protocol/authentication flags; it does not currently expose the server's retention-policy metadata. Neither method grants permission.

## Local development

The HumChat server's local fixture supports mock session tokens only in a debug/test build configured for mock authentication. For example, `mock-jwt-token-123::org-mock-1` is a local fixture token for `org-mock-1`; it is not a production credential. Obtain the local server origin and existing channel key from the fixture operator. The server and its fixture are maintained separately; this repository and npm package do not start PostgreSQL or a HumChat server.

Related guides: [API](API.md), [reliability](RELIABILITY.md), [troubleshooting](TROUBLESHOOTING.md).
