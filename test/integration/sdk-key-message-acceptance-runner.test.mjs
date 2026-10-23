import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { org, sdkKey, members, alice, bob, peer } from '../fixtures/sdk-key-direct-messaging.mjs';

const runner = fileURLToPath(new URL('../acceptance/sdk-key-direct-messaging.mjs', import.meta.url));
const roster = status => members.map(member => ({ user_uuid: member.id, org_uuid: org,
  name: member.name, email: member.email, role: 'member', status: member.id === bob.id ? status : 'active' }));
function assertSanitized(fixture, output) {
  for (const value of [sdkKey, org, ...members.flatMap(member => [member.id, member.email]),
    'synthetic-native-dm-', 'synthetic-app-session-', 'server-error-private-data',
    ...[...fixture.states.values()].flatMap(state => [state.message_id, state.body])]) {
    assert.equal(output.includes(value), false, 'Acceptance output must contain only sanitized status and counts');
  }
}
async function execute(t, fixture, overrides = {}) {
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    ['PATH', 'SYSTEMROOT', 'TEMP', 'TMP', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA'].includes(key.toUpperCase())));
  const configured = { HUMCHAT_BASE_URL: fixture.origin, HUMCHAT_ORG_ID: org, HUMCHAT_SDK_KEY: sdkKey,
    HUMCHAT_ACT_AS: alice.email, HUMCHAT_TEST_RECIPIENT_EMAIL: bob.email,
    HUMCHAT_ACCEPTANCE_SEND: 'true', HUMCHAT_ACCEPTANCE_TIMEOUT_MS: '10000', ...overrides };
  const env = { ...inherited, ...Object.fromEntries(Object.entries(configured).filter(([, value]) => value !== undefined)) };
  const child = spawn(process.execPath, [runner], { env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  let stdout = '', stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  const killTimer = setTimeout(() => child.kill(), 15000);
  let exit;
  try {
    exit = await new Promise((resolve, reject) => {
      child.once('error', reject); child.once('close', (code, signal) => resolve({ code, signal }));
    });
  } finally { clearTimeout(killTimer); }
  assert.equal(exit.signal, null, 'Runner must end by itself and close all handles');
  assert.equal(stderr, '');
  assertSanitized(fixture, stdout + stderr);
  const lines = stdout.trim().split('\n'); assert.equal(lines.length, 1, 'CLI emits exactly one JSON report');
  const report = JSON.parse(lines[0]);
  assert.equal(exit.code, report.passed ? 0 : 1);
  return report;
}
function assertNoMessageWrites(fixture, report) {
  assert.equal(report.messageSendAttempts, 0); assert.equal(report.acceptedMessages, 0);
  assert.equal(fixture.states.size, 0); assert.equal(fixture.conversations.size, 0);
  assert.equal(fixture.requests.some(request => request.method === 'POST' &&
    ['/api/conversations', '/api/messages/send'].includes(request.path)), false);
}

test('acceptance CLI sends exactly two native-auth messages and verifies both actors, inboxes, and four history reads', { timeout: 15000 }, async t => {
  const fixture = await peer(t, 'channel');
  const report = await execute(t, fixture);
  assert.equal(report.passed, true); assert.equal(report.failure, undefined);
  assert.equal(report.messageSendAttempts, 2); assert.equal(report.acceptedMessages, 2); assert.equal(report.historyMatches, 4);
  assert.equal(report.receiverDeliveries, 1); assert.equal(report.senderDeliveries, 1);
  assert.equal(report.receiverLiveDeliveries + report.receiverRecoveryDeliveries, 1);
  assert.equal(report.senderLiveDeliveries + report.senderRecoveryDeliveries, 1);
  assert.deepEqual(report.checks, ['both_verified_member_identities', 'both_messaging_capabilities',
    'both_rosters_identify_exact_active_participants', 'both_conversation_scopes_and_capacity',
    'both_inboxes_ready_before_writes', 'sender_direct_message_accepted', 'receiver_inbox_received_direct_message',
    'both_conversation_streams_live_before_reply', 'sender_inbox_received_thread_reply',
    'both_messages_persisted_in_both_member_histories']);
  const messages = [...fixture.states.values()];
  assert.equal(messages.length, 2); assert.equal(fixture.conversations.size, 1);
  assert.equal(messages[0].sender_id, alice.id); assert.equal(messages[0].thread_root_id, null);
  assert.equal(messages[1].sender_id, bob.id); assert.equal(messages[1].thread_root_id, messages[0].message_id);
  assert.equal(messages[0].channel_id, messages[1].channel_id);
  assert.match(messages[0].body, /intentionally retained/); assert.match(messages[1].body, /intentionally retained/);
  assert.deepEqual(fixture.mints.sort(), [alice.id, bob.id].sort());
  assert.equal(fixture.requests.filter(request => request.path === '/api/messages/send').length, 2);
});

for (const [label, overrides, code] of [
  ['missing explicit write opt-in', { HUMCHAT_ACCEPTANCE_SEND: undefined }, 'OPT_IN_REQUIRED'],
  ['missing designated recipient', { HUMCHAT_TEST_RECIPIENT_EMAIL: undefined }, 'MISSING_CONFIGURATION'],
  ['same sender and recipient', { HUMCHAT_TEST_RECIPIENT_EMAIL: ` ${alice.email.toUpperCase()} ` }, 'DISTINCT_RECIPIENT_REQUIRED'],
  ['invalid deadline', { HUMCHAT_ACCEPTANCE_TIMEOUT_MS: '1' }, 'INVALID_DEADLINE'],
  ['credential-bearing origin', { HUMCHAT_BASE_URL: 'https://secret@example.invalid' }, 'INVALID_ORIGIN'],
]) {
  test(`acceptance CLI refuses ${label} before any network call`, { timeout: 10000 }, async t => {
    const fixture = await peer(t, 'channel');
    const report = await execute(t, fixture, overrides);
    assert.equal(report.passed, false); assert.deepEqual(report.failure, { code });
    assert.equal(fixture.requests.length, 0); assertNoMessageWrites(fixture, report);
  });
}

test('acceptance CLI refuses an inactive recipient from either verified roster before conversation or message writes', { timeout: 10000 }, async t => {
  const fixture = await peer(t, 'channel', { route: ({ url, member }) =>
    url.pathname === '/api/org/members' ? { status: 200, body: roster(member.id === bob.id ? 'disabled' : 'active') } : undefined });
  const report = await execute(t, fixture);
  assert.deepEqual(report.failure, { code: 'ROSTER_MISMATCH' }); assertNoMessageWrites(fixture, report);
});

test('acceptance CLI refuses different emails resolving to the same actor before any writes', { timeout: 10000 }, async t => {
  const fixture = await peer(t, 'channel', { route: ({ url, member }) =>
    url.pathname === '/me' ? { status: 200, body: { user_uuid: alice.id, org, email: member.email, roles: ['member'], scopes: [] } } : undefined });
  const report = await execute(t, fixture);
  assert.deepEqual(report.failure, { code: 'IDENTITY_MISMATCH' }); assertNoMessageWrites(fixture, report);
});

test('acceptance CLI reports an HTTP preflight failure without leaking the response or writing messages', { timeout: 10000 }, async t => {
  const fixture = await peer(t, 'channel', { route: ({ url }) => url.pathname === '/api/conversations'
    ? { status: 403, body: { reason: `server-error-private-data ${sdkKey} synthetic-native-dm-secret ${bob.email}` } } : undefined });
  const report = await execute(t, fixture);
  assert.deepEqual(report.failure, { code: 'PERMISSION', status: 403 }); assertNoMessageWrites(fixture, report);
});

test('acceptance CLI rejects a callback payload mismatch and stops before sending a reply', { timeout: 10000 }, async t => {
  const fixture = await peer(t, 'channel', { event: event => {
    event.state.body = 'server-error-private-data'; event.state.original_body = event.state.body;
  } });
  const report = await execute(t, fixture);
  assert.deepEqual(report.failure, { code: 'DELIVERY_MISMATCH' });
  assert.equal(report.messageSendAttempts, 1); assert.equal(report.acceptedMessages, 1);
  assert.equal(report.historyMatches, 0); assert.equal(fixture.states.size, 1);
});

test('acceptance CLI deadline cancels stalled inbox replay, closes handles, and never sends a reply', { timeout: 18000 }, async t => {
  const fixture = await peer(t, 'channel', { route: ({ url }) => url.pathname === '/api/events/sync'
    ? new Promise(() => {}) : undefined });
  const report = await execute(t, fixture);
  assert.deepEqual(report.failure, { code: 'DEADLINE_EXCEEDED' });
  assert.equal(report.messageSendAttempts, 1); assert.equal(report.acceptedMessages, 1);
  assert.equal(report.receiverDeliveries, 0); assert.equal(fixture.states.size, 1);
});
