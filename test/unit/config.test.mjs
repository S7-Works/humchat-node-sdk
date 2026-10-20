import test from 'node:test';
import assert from 'node:assert/strict';
import { HumChat, ConfigurationError, UnsupportedFeatureError, ProtocolError } from '../../dist/index.js';

const capabilities = {
  sdk_protocol_version: 1,
  authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
  messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: false, websocket_scope: 'channel' },
};
function config(extra = {}) {
  return { orgId: 'org-mock-1', baseUrl: 'http://127.0.0.1:43125', tokenProvider: async () => ({ token: 'fake-config-secret' }), ...extra };
}

test('construction is lazy and safe config has no credentials or mutable defaults', async () => {
  let calls = 0;
  const client = new HumChat(config({ fetch: async () => { calls++; throw new Error('unexpected IO'); } }));
  const safe = client.getConfig();
  assert.equal(safe.timeout, 10_000);
  assert.equal(safe.retry.maxRetries, 3);
  assert.equal(safe.retry.totalTimeoutMs, 30_000);
  assert.equal(safe.realtime.transport, 'channel');
  safe.retry.maxRetries = 8;
  assert.equal(client.getConfig().retry.maxRetries, 3);
  assert.equal(JSON.stringify(safe).includes('secret'), false);
  assert.equal(JSON.stringify(client).includes('secret'), false);
  assert.equal('tokenProvider' in safe, false);
  assert.equal(calls, 0);
  await client.close();
});

test('invalid or unsupported config fails before fetching', () => {
  for (const extra of [
    { orgId: '' }, { orgId: ' org ' }, { baseUrl: undefined }, { baseUrl: 'not a URL' },
    { baseUrl: 'file:///tmp/a' }, { baseUrl: 'https://user:secret@example.test' },
    { baseUrl: 'https://example.test/api' }, { baseUrl: 'https://example.test/?token=secret' },
    { baseUrl: 'https://example.test/#secret' }, { timeout: 0 }, { timeout: Infinity },
    { tokenProvider: null }, { fetch: false }, { retry: null }, { retry: { maxRetries: -1 } },
    { retry: { maxRetries: 11 } }, { retry: { totalTimeoutMs: NaN } },
    { retry: { misspelled: 1 } }, { recovery: { mode: 'unknown' } }, { realtime: { maxChannels: 0 } }, { realtime: { transport: 'auto' } },
  ]) assert.throws(() => new HumChat(config(extra)), ConfigurationError);
  assert.throws(() => new HumChat(config({ apiKey: 'fake-key' })), UnsupportedFeatureError);
  assert.throws(() => new HumChat(config({ recovery: { mode: 'bestEffort' }, realtime: { transport: 'multiplex' } })), UnsupportedFeatureError);
});

test('identity and capabilities are copied, additive server fields are harmless', async () => {
  const client = new HumChat(config({ fetch: async url => {
    if (new URL(url).pathname === '/me') return Response.json({
      user_uuid: 'user-mock-123', org: 'org-mock-1', email: 'test@example.invalid', roles: ['admin'], scopes: [],
      expires_at: null, app_uuid: null, future: 'field',
    });
    return Response.json({ ...capabilities, future: 'field' });
  } }));
  const identity = await client.connect();
  identity.actorId = 'different'; identity.roles.push('invented');
  assert.equal((await client.connect()).actorId, 'user-mock-123');
  assert.deepEqual((await client.connect()).roles, ['admin']);
  const caps = await client.getCapabilities();
  caps.messaging.sendReceiptVersions.push(99);
  assert.deepEqual((await client.getCapabilities()).messaging.sendReceiptVersions, [1]);
  assert.equal(caps.authentication.serviceIdentities, false);
  assert.equal(caps.messaging.multiplexedEvents, null);
  await client.close();
});

test('missing and incompatible capability negotiation fails explicitly', async () => {
  for (const [caps, expected] of [
    [null, UnsupportedFeatureError],
    [{ ...capabilities, messaging: { ...capabilities.messaging, history_versions: [1] } }, UnsupportedFeatureError],
    [{ sdk_protocol_version: 1 }, ProtocolError],
  ]) {
    const client = new HumChat(config({ fetch: async url => new URL(url).pathname === '/me'
      ? Response.json({ user_uuid: 'user-mock-123', org: 'org-mock-1', email: 'test@example.invalid', roles: [] })
      : caps === null ? new Response('not found', { status: 404 }) : Response.json(caps) }));
    await assert.rejects(client.connect(), expected);
    await client.close();
  }
});
