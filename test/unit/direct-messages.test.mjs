import test from 'node:test';
import assert from 'node:assert/strict';
import { HumChat } from '../../dist/index.js';

const org = 'dm-org'; const actor = 'dm-self'; const peer = 'dm-peer';
const identity = { user_uuid: actor, org, id: 1, email: 'self@example.invalid', roles: ['member'], scopes: [], expires_at: null, app_uuid: null };
const capabilities = { sdk_protocol_version: 1,
  authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
  messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: true, event_protocol_versions: [1], websocket_scope: 'channel' } };
const conversation = (id = 'existing', changes = {}) => ({ id, channel_key: `${org}:conv:${id}`, name: null, kind: 'dm', member_count: 2,
  my_role: 'member', members: [{ user_uuid: actor, role: 'member' }, { user_uuid: peer, role: 'member' }], last_message_at: null, ...changes });
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
function client(t, route) {
  const requests = [];
  const sdk = new HumChat({ orgId: org, baseUrl: 'http://127.0.0.1:43123', tokenProvider: async () => ({ token: 'synthetic-dm-token' }),
    retry: { maxRetries: 3, initialDelayMs: 1, maxDelayMs: 2, totalTimeoutMs: 500 }, timeout: 200,
    fetch: async (url, init) => {
      const path = new URL(url).pathname; requests.push({ path, ...init });
      if (path === '/me') return json(identity);
      if (path === '/api/sdk/capabilities') return json(capabilities);
      return route(path, init);
    } });
  t.after(() => sdk.close()); return { sdk, requests };
}

test('members normalize the authenticated roster and reject duplicates, malformed rows, and additive foreign scope', async t => {
  let rows = [{ user_uuid: peer, name: 'Peer', email: 'peer@example.invalid', role: 'member', status: 'active' }];
  const { sdk } = client(t, path => { assert.equal(path, '/api/org/members'); return json(rows); });
  assert.deepEqual(await sdk.members.list(), [{ actorId: peer, name: 'Peer', email: 'peer@example.invalid', role: 'member', status: 'active' }]);
  rows = [...rows, ...rows]; await assert.rejects(sdk.members.list(), { code: 'PROTOCOL' });
  rows = [{ ...rows[0], org_uuid: 'foreign' }]; await assert.rejects(sdk.members.list(), { code: 'PROTOCOL' });
  rows = [{ user_uuid: peer }]; await assert.rejects(sdk.members.list(), { code: 'PROTOCOL' });
});

test('openDirect reuses only an unnamed exact peer pair and sendDirect uses its canonical routing key', async t => {
  const sent = [];
  const { sdk, requests } = client(t, (path, init) => {
    if (path === '/api/conversations') {
      assert.equal(init.method, 'GET');
      return json([conversation('named', { name: 'Separate group' }), conversation('other', { members: [{ user_uuid: actor, role: 'member' }, { user_uuid: 'other', role: 'member' }] }), conversation()]);
    }
    assert.equal(path, '/api/messages/send');
    const value = JSON.parse(init.body); sent.push(value);
    assert.equal(init.headers.get('X-Idempotency-Key'), 'request-key');
    return json({ status: 'ACK', message_id: value.id, created_at: 1000, attachment_count: 0, attachment_manifest_blake3: '0'.repeat(64) });
  });
  const result = await sdk.messages.sendDirect({ recipientId: peer, content: 'hello', id: 'message-one' }, { idempotencyKey: 'request-key' });
  assert.equal(result.conversation.id, 'existing'); assert.equal(result.receipt.id, 'message-one');
  assert.equal(sent[0].channel_id, `${org}:conv:existing`);
  assert.equal(requests.filter(request => request.path === '/api/conversations' && request.method === 'POST').length, 0);
});

test('openDirect creates once with the peer and validates the returned participant set', async t => {
  let returned = conversation('new'); const bodies = [];
  const { sdk } = client(t, (_path, init) => {
    if (init.method === 'GET') return json([]);
    bodies.push(JSON.parse(init.body)); return json(returned);
  });
  assert.equal((await sdk.conversations.openDirect({ memberId: peer })).channelId, `${org}:conv:new`);
  assert.deepEqual(bodies, [{ member_uuids: [peer], name: null }]);
  returned = conversation('new', { members: [{ user_uuid: actor, role: 'member' }, { user_uuid: 'stranger', role: 'member' }] });
  await assert.rejects(sdk.conversations.create({ memberIds: [peer] }), { code: 'PROTOCOL' });
});

test('creation never retries an ambiguous transport failure, 503, or 401', async t => {
  for (const status of [null, 503, 401]) {
    let posts = 0;
    const { sdk } = client(t, (_path, init) => {
      if (init.method === 'GET') return json([]);
      posts++;
      if (status === null) throw new Error('Synthetic fetch failure');
      return json({}, status);
    });
    await assert.rejects(sdk.conversations.openDirect({ memberId: peer })); assert.equal(posts, 1);
  }
});

test('invalid DM intent is rejected before creation and caller cannot direct-message itself', async t => {
  const { sdk, requests } = client(t, () => { throw new Error('No data requests expected'); });
  await assert.rejects(sdk.messages.sendDirect({ recipientId: peer, content: '' }), { code: 'VALIDATION' });
  await assert.rejects(sdk.conversations.create({ memberIds: [peer, peer] }), { code: 'VALIDATION' });
  assert.equal(requests.length, 0);
  await assert.rejects(sdk.conversations.openDirect({ memberId: actor }), { code: 'VALIDATION' });
  assert.equal(requests.filter(request => request.path === '/api/conversations').length, 0);
});

test('direct send preserves explicit capability negotiation with no legacy fallback', async t => {
  let routes = [];
  const sdk = new HumChat({ orgId: org, baseUrl: 'http://127.0.0.1:43123', tokenProvider: async () => ({ token: 'synthetic' }),
    fetch: async url => { const path = new URL(url).pathname; routes.push(path); return path === '/me' ? json(identity) : json({}, 404); } });
  t.after(() => sdk.close());
  await assert.rejects(sdk.messages.sendDirect({ recipientId: peer, content: 'hello' }), { code: 'UNSUPPORTED_FEATURE' });
  assert.deepEqual(routes, ['/me', '/api/sdk/capabilities']);
});
