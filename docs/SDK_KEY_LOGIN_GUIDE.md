# HumChat SDK-key login

The unpublished alpha.8 preview supports delegated member login through your configured HumChat server.
Use an SDK key and a member email to send and receive as that member.
The SDK caches and renews the returned user token for HTTP requests and sockets.
Keep the key in your backend's private configuration.
Validate the SDK and your server deployment together before production use.

## Login flow

1. An organization administrator creates an SDK key in HumChat and gives it to the backend operator through a private channel.
2. The backend sends the key, organization UUID and member email to HumChat.
3. HumChat checks the key and member eligibility, then returns a short-lived user access token.
4. The SDK uses that token as Bearer authentication for HumChat HTTP requests and sockets. Messages use the verified member identity and that member's permitted conversations/channels.

The SDK does not perform OTP login or contact the identity backend. No identity-provider client ID or client secret belongs in SDK configuration. A key can request a token for members of its own organization; it does not authorize access to another organization.

## SDK configuration in the alpha.8 preview

This configuration performs token exchange, verifies the requested identity and checks capabilities. The package is an unpublished preview; install a reviewed local archive as described in the [README](../README.md).

```ts
import { HumChat } from '@humchat/node';

const humchat = new HumChat({
  baseUrl: process.env.HUMCHAT_BASE_URL!,
  orgId: process.env.HUMCHAT_ORG_ID!,
  auth: {
    sdkKey: process.env.HUMCHAT_SDK_KEY!,
    actAs: process.env.HUMCHAT_ACT_AS!,
  },
});
try {
  await humchat.connect();
  const members = await humchat.members.list();
  // Select an authorized recipient from members for messages.sendDirect().
} finally {
  await humchat.close();
}
```

Keep the existing `tokenProvider` interface for applications that bring a user token. Combining it with `auth` raises `ValidationError`, including explicitly undefined alternate properties. The distinct top-level `apiKey` option remains unsupported; this feature is delegated member login, not a bot mode.

Concurrent connection and resource calls share one token acquisition. Later requests reuse the verified token until renewal is needed. See [authentication](AUTHENTICATION.md) for configuration validation, caller-managed tokens and identity binding.

## Server contract

```http
POST /api/sdk/auth/user-token
X-HumChat-Sdk-Key: <secret from private configuration>
Content-Type: application/json

{"org_uuid":"<org UUID>","email":"<member email>","expires_in":900}
```

Success is `{ access_token, token_type: "Bearer", expires_in }`. The key belongs only in `X-HumChat-Sdk-Key`. Normal authenticated requests use the returned token in `Authorization: Bearer ...`. Requested lifetime defaults to 900 seconds and is clamped to 60–3600 seconds. Maximum request body size is 4096 bytes.

Mint before calling `/me` and `/api/sdk/capabilities`, because capabilities require authentication. After login, validate the advertised `authentication.sdk_key_user_tokens` path/header/configured flag. Never forward a key to another origin based on capability metadata.

| Status | Handling |
| --- | --- |
| 400 / 413 / 415 / 422 | Invalid request; fix it, do not retry unchanged |
| 401 | Invalid, missing or revoked SDK key; no retry loop |
| 403 | Organization mismatch or refused sign-in; do not infer private membership details |
| 404 / 405 | SDK-key login endpoint unsupported; `UnsupportedFeatureError` |
| 429 | Bounded backoff; honor `Retry-After` when provided |
| 502 / 503 | Bounded retry for unavailable login; 503 alone does not identify its configuration/upstream cause |

The implementation caches the token, shares concurrent mint requests and renews up to 60 seconds before expiry. An eligible HTTP 401 triggers one coordinated refresh and retry; conversation creation and other non-replayable operations keep their existing exceptions. Mint 401/403 is terminal for that client. Retryable acquisition failures share a cooldown: initially 1–2 seconds, doubling up to 15–30 seconds, with a longer `Retry-After` honored. Successful full verification resets it. This coordinates one client; applications with multiple processes must manage their combined request rate.

## Sockets and delegated access

Token renewal rotates sockets after verifying that the member and organization remain unchanged. A server advertising `authentication.socket_session_end_codes: ["TOKEN_EXPIRED"]` can explicitly request renewal when a socket token expires. An ambiguous or unadvertised `session_revoked` frame remains terminal. See [reliability](RELIABILITY.md) for reconnect, checkpoints and delivery guarantees.

Use delegated member sessions for messaging, channel discovery, history and sockets. Administrative operations require a separately authorized interactive administrator session. Verify these boundaries on your deployment; SDK configuration and capability flags do not establish server authorization.

Supply a known team ID to `channels.list({ teamId })` when needed. The SDK does not discover all teams or create a workspace automatically after a 404. Use `conversations.list()` for direct and group conversations, and [direct messages](DIRECT_MESSAGES.md) for sending and receiving through a managed inbox.

## Deployment validation

This preview's package tests use controlled local peers. They do not certify production capacity, a particular deployment's authorization rules, or display in the HumChat app. Before production use, verify login, permitted messaging, denied administration, renewal and recovery on your target deployment.

The opt-in [message acceptance procedure](SDK_KEY_MESSAGE_ACCEPTANCE.md) checks sending and receiving with two designated members. It creates visible messages, requires permission for both identities, and includes a separate manual app check. Production send/reply and app acceptance remain pending; do not infer them from a successful login.

Key revocation stops future mints. Issued tokens may remain usable until expiry unless separately revoked or denied by current access checks. Use a disposable key for revocation tests; do not revoke an integration's active key as a test side effect.

Keep keys and tokens out of source, logs, error messages and public examples. Use [troubleshooting](TROUBLESHOOTING.md) for error handling and [release preparation](RELEASING.md) for the remaining publication and deployment validation requirements.
