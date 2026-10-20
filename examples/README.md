# Runnable backend examples

Use Node 22 or newer. Install a reviewed tarball into a backend project (`npm install ./humchat-node-0.1.0-alpha.6.tgz`), then run the files below from that project's directory. Source is public; the package is unpublished on npm and licensing remains pending. Messaging examples use an existing user-session token; API-key exchange and service identities are not implemented. The signed webhook receiver instead uses its subscription signing secret.

Supply an accessible HumChat server, its organization ID, and a valid token through the process environment. The token's account needs access to the selected channels. Tokens are read from the environment and never printed. For unattended use, replace the static `tokenProvider` with your application's renewal flow. A static environment token cannot renew itself.

## Environment

| Variable | Used by | Meaning |
| --- | --- | --- |
| `HUMCHAT_BASE_URL` | Messaging examples | HTTP(S) server origin, with no path or query. |
| `HUMCHAT_ORG_ID` | All | Organization for this verified session. |
| `HUMCHAT_TOKEN` | Messaging examples | Bearer user-session token supplied by your application. |
| `HUMCHAT_CHANNEL_ID` | Send, react, single-channel responders, CJS send | Canonical channel routing key, including for a DM. |
| `HUMCHAT_TEAM_ID` | Channel lists | Optional workspace team filter. |
| `HUMCHAT_MESSAGE_ID` | React | Existing message in the selected channel. |
| `HUMCHAT_SEND_MESSAGE` | CJS smoke | Optional message text. The CJS example sends only when this variable is explicitly present. |
| `HUMCHAT_CHANNEL_IDS` | Multi-channel and multiplex workers | Comma-separated canonical IDs; duplicates share one subscription. Maximum 50 distinct channels in this example. |
| `HUMCHAT_STATE_DIR` | Durable responders | Private persistent directory owned by this process. Contains checkpoint and outbox files. |
| `HUMCHAT_CONSUMER_ID` | Durable responders | Optional stable worker identity. Defaults to `status-responder-v1`, `multi-channel-status-v1`, or `multiplex-status-v1`. |
| `HUMCHAT_START` | Durable responders | `now` by default; `earliest` explicitly replays retained historical commands on first use. A saved checkpoint takes precedence. |
| `HUMCHAT_RUN_FOR_MS` | Durable responders | Optional positive duration after readiness, for a bounded demo followed by clean shutdown. Otherwise runs until Ctrl+C/SIGTERM or a terminal error. |

## Commands and output

Each command below uses the configured server. Run send/react/responder commands only in a channel where their writes are intended.

| Command | Result |
| --- | --- |
| `node node_modules/@humchat/node/examples/list-channels.mjs` | Prints channel IDs and display names. |
| `node node_modules/@humchat/node/examples/send.mjs "Hello from my backend"` | Sends one message; prints its message ID and `accepted` status. |
| `node node_modules/@humchat/node/examples/react.mjs "✅"` | Sets the reaction present; prints operation ID and reaction revision. |
| `node node_modules/@humchat/node/examples/send-and-list.cjs` | Uses CommonJS `require`; prints a JSON channel list. If `HUMCHAT_SEND_MESSAGE` is set, also sends it and prints an accepted receipt. |
| `node node_modules/@humchat/node/examples/thread-responder.mjs` | Older-server, best-effort example. Replies to live `!status` messages in their existing thread, ignoring its own messages. Downtime commands can be missed. |
| `node node_modules/@humchat/node/examples/durable-responder.mjs` | Durable single-channel `!status` responder using checkpoint and outbox files. |
| `node node_modules/@humchat/node/examples/multi-channel-worker.mjs` | Durable responder across `HUMCHAT_CHANNEL_IDS`; one socket per distinct channel. |
| `node node_modules/@humchat/node/examples/multiplex-worker.mjs` | Same durable responder using one physical socket; requires the optional server multiplex capability. |

Durable examples print JSON `ready`, `reply`, and `stopped` records. They reply with `All systems operational.` and retain an existing thread root. They ignore their own messages and non-command events. Errors print SDK error codes without tokens or server response bodies. A terminal channel error stops the worker and exits unsuccessfully; transient failures use SDK reconnect behavior.

## Durable restart and shutdown

Use a server that advertises durable event protocol v1. The multiplex example additionally requires multiplex protocol v1; see [shared connections](../docs/MULTIPLEX.md). Keep the same origin, organization, actor, consumer ID, channel IDs, and state directory across restarts. The worker retries pending outbox entries before subscribing. Its handler returns only after the reply receipt is saved; the SDK then advances that input's checkpoint. Processing is at least once, and stable outbox keys keep retries bound to the same outgoing intent.

Ctrl+C/SIGTERM and `HUMCHAT_RUN_FOR_MS` abort outstanding SDK requests, close sockets, wait for active handlers, and release the file-store locks. An interrupted input remains unacknowledged and is replayed on restart. One process owns each state directory. A crash can leave `.lock` files: verify the previous process has exited before manually removing those lock files. Do not discard checkpoint/outbox data to bypass a retention gap; decide explicitly how the application should recover.

## TypeScript quickstart

This exact snippet is typechecked against the installed tarball by the package tests. It is a one-shot backend send; the durable examples above cover continuous receiving.

```ts
import { HumChat } from '@humchat/node';

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

const client = new HumChat({
  orgId: required('HUMCHAT_ORG_ID'),
  baseUrl: required('HUMCHAT_BASE_URL'),
  tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }),
});
try {
  const channels = await client.channels.list();
  console.log(channels.channels.map(channel => channel.id));
  const receipt = await client.messages.send({
    channelId: required('HUMCHAT_CHANNEL_ID'),
    content: 'Hello from TypeScript',
  });
  console.log(receipt.id, receipt.status);
} finally {
  await client.close();
}
```

The package tests execute the installed send/list/react/CommonJS/thread/durable examples against controlled loopback HTTP and WebSocket peers using synthetic credentials. They make no production HumChat requests.

## Signed webhook receiver

`webhook-receiver.mjs` accepts signed events at `POST /humchat/webhook` on loopback and commits a deduplicated local inbox before acknowledging. It uses `webhook-inbox.mjs`; it does not need a user-session token. Set the signing secret, organization, subscription ID, channel allowlist and private state directory described in [Webhooks](../docs/WEBHOOKS.md). Use a public HTTPS reverse proxy for an external destination. This is a single-process inbox example, not a distributed business transaction.
