# SDK-key direct-message acceptance

This opt-in runner checks two real organization members through the configured HumChat server.
It sends one labeled direct message, then one labeled reply in that message's thread.
Both messages stay in HumChat; the runner does not delete them or mark them read.
It verifies inbox delivery and reads both messages back through both members' history APIs.
A passing result verifies SDK and server behavior; a person must still check the HumChat app UI.

## Before running

Use Node.js 22 or newer and the built SDK from this checkout. `npm test` exercises this runner only against synthetic loopback peers; it never invokes the live test automatically. Running it explicitly against production changes real conversation history and may notify the chosen members.

Have the organization owner designate two distinct, active test members. Obtain permission both to send the test messages and to sign in as the recipient for its automated reply. Do not choose a recipient automatically from the roster. The SDK key must belong to their organization and allow delegated login for both members.

Configure the following environment variables through your private process environment or secret manager. Do not put keys in command arguments, source files, logs, screenshots, or this document.

| Variable | Required value |
| --- | --- |
| `HUMCHAT_BASE_URL` | Explicit HumChat origin, such as `https://humchat.example.com`; no path, credentials, query, or fragment |
| `HUMCHAT_ORG_ID` | The designated organization's UUID |
| `HUMCHAT_SDK_KEY` | Its private `hsk_` credential |
| `HUMCHAT_ACT_AS` | Designated sender's member email |
| `HUMCHAT_TEST_RECIPIENT_EMAIL` | Designated recipient's different member email |
| `HUMCHAT_ACCEPTANCE_SEND` | Exactly `true`, enabling this two-identity test: two persistent messages, possible DM creation, recipient login and its thread reply |
| `HUMCHAT_ACCEPTANCE_TIMEOUT_MS` | Optional total deadline, 10000–600000 milliseconds; default 180000 |

All HTTP and socket traffic goes through `HUMCHAT_BASE_URL`. There is no identity-provider URL, client ID, client secret, or top-level `apiKey` exchange. The current implementation uses `auth: { sdkKey, actAs }`, including cached user-token exchange and renewal.

## Run once

From the SDK repository root, after supplying the private environment:

```sh
npm run build
node test/acceptance/sdk-key-direct-messaging.mjs
```

Do not pass any arguments. The script refuses them to avoid putting credentials on the command line. It writes one sanitized JSON report and exits with code 0 only when every acceptance check passes. Redirect that report to an approved evidence location if needed; the script itself writes no evidence or checkpoint files.

## Checks and visible changes

Before any conversation creation or message send, the runner verifies both minted identities, organization/email binding, messaging capabilities, organization rosters, conversation membership and inbox readiness. Each roster must contain exactly one matching active entry for each designated member, with the verified actor IDs. A missing, ambiguous, inactive or mismatched member stops the run. No administration or workspace-bootstrap routes are called.

The runner starts one durable inbox watcher for each member. These discover all conversations visible to that member, replay available events and keep checkpoints only in memory. Unrelated messages are discarded by the acceptance handler and never printed or retained in a file. Each member must initially have fewer than 50 conversations, leaving room for a newly created DM. Very large retained histories can exceed the SDK's recovery limits; a failed recovery stops this test rather than silently skipping it.

After preflight succeeds:

1. The sender calls `messages.sendDirect()` with the recipient's verified actor ID. It reuses an exact two-person DM when present; otherwise the server may create one. The content starts with `[HumChat SDK acceptance <random run label>] Test 1/2`.
2. The recipient inbox must deliver that exact root message with the expected sender, organization, channel, text and no thread root. Discovery of a new conversation can deliver it through durable replay; the report distinguishes live and recovery deliveries.
3. Both members' conversation streams must reach `live` before the recipient calls `messages.reply()` once. The reply starts with the same run label and `Test 2/2` and references the first message as its thread root.
4. The sender inbox must deliver the exact reply from the recipient. Root and thread histories must each return the correct message for both identities: four successful history matches.

Each send has a fixed per-run message ID and idempotency key. Transport retries reuse them. Duplicate event deliveries are counted and do not trigger extra replies. The normal successful run creates exactly two message intents, though a network failure can leave an accepted message without a confirmed receipt. There is no automatic second run, delete cleanup, reaction, typing notification, read receipt, invitation or administrative mutation.

## Report and interpretation

The JSON contains only pass/fail, fixed check names, sanitized error category/status, send-attempt and accepted-message counts, delivery counts by source, and history-match counts. It omits keys, tokens, raw errors, request IDs, member IDs, emails, message IDs, bodies, roster contents and conversation identifiers.

`passed: true` means both messages were accepted, delivered to the opposite member's SDK inbox and retrieved from each member's history. Delivery is at least once, so counters may exceed one. The runner accepts durable recovery delivery and reports it separately; success alone does not claim both events arrived directly on a live socket.

On failure, inspect the fixed `failure.code`, optional HTTP status and completed checks. A failed run may already have created the DM or one or both messages. Do not rerun blindly: a new invocation gets a new run label and message IDs, so it intentionally creates new messages. Inspect the designated conversation before repeating. Unknown errors are reported as `UNEXPECTED_FAILURE` without their original text.

The total deadline and termination signals abort requests, then close both clients and in-memory checkpoint stores. After the SDK exits, manually open the designated recipient's HumChat app, find the sender's DM, confirm Test 1/2 appears as that sender and Test 2/2 appears in its thread as the recipient. Confirm the sender can see the same reply. Record that UI observation separately; server history does not prove a particular app build renders it correctly.

This runner does not verify every organization member, administrator restrictions, key revocation, deployment of explicit token-expiry socket codes, long-running renewal or fleet capacity. Those remain separate acceptance checks. Do not describe production messaging as verified until this opt-in run and the app check have actually completed.
