import test from 'node:test';
import assert from 'node:assert/strict';
import { HumChat, ProtocolError, UnsupportedFeatureError, ValidationError } from '../../dist/index.js';
import { org, channel, actor, subscriptionId, deliveryId, secret, previousSecret, timestamp, epoch,
  subscription, delivery, capabilities } from '../unit/webhooks.fixtures.mjs';

function fixture(t, handler, overrides = {}) {
  const requests = [], reasons = [];
  const client = new HumChat({ orgId: org, baseUrl: 'http://127.0.0.1:43123',
    tokenProvider: async ({ reason }) => { reasons.push(reason); return { token: 'synthetic-webhook-admin' }; },
    retry: { maxRetries: 2, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 2000 },
    fetch: async (input, init) => {
      const url = new URL(input); requests.push({ url, ...init });
      if (url.pathname === '/me') return Response.json({ user_uuid: actor, org, email: 'webhook@example.invalid', roles: ['admin'] });
      if (url.pathname === '/api/sdk/capabilities') return Response.json(overrides.capabilities ?? capabilities);
      return handler({ url, ...init });
    } });
  t.after(() => client.close()); return { client, requests, reasons };
}
const result = value => ({ schema: 1, subscription: value });
const createInput = { id: subscriptionId, url: 'https://receiver.example.invalid/humchat', channelIds: [channel] };

test('webhook management maps all routes, methods, one-time secrets and delivery views', async t => {
  const calls = [];
  const { client } = fixture(t, request => {
    calls.push(request); const path = request.url.pathname;
    if (path === '/api/webhooks' && request.method === 'POST') {
      assert.deepEqual(JSON.parse(request.body), { id: subscriptionId, url: createInput.url, channel_ids: [channel] });
      return Response.json({ ...result(subscription({ status: 'paused' })), signing_secret: secret });
    }
    if (path === '/api/webhooks') return Response.json({ schema: 1, org_uuid: org, subscriptions: [subscription()] });
    if (request.method === 'DELETE') return Response.json({ schema: 1, id: subscriptionId, deleted: true });
    if (path.endsWith('/pause')) return Response.json(result(subscription({ status: 'paused' })));
    if (path.endsWith('/resume')) return Response.json(result(subscription()));
    if (path.endsWith('/rotate-secret')) return Response.json({ ...result(subscription({ secret_version: 2, previous_secret_expires_at: timestamp + 300 })), signing_secret: previousSecret });
    if (path.endsWith('/replay')) return Response.json({ schema: 1, delivery: delivery({ status: 'pending', attempts: 0 }) });
    if (path.endsWith('/deliveries')) {
      assert.equal(request.url.searchParams.get('status'), 'dead'); assert.equal(request.url.searchParams.get('limit'), '10');
      assert.equal(request.url.searchParams.get('before'), subscriptionId);
      return Response.json({ schema: 1, org_uuid: org, deliveries: [delivery()], next_before: deliveryId });
    }
    return Response.json({ ...result(subscription()), signing_secret: secret }); // Public read must strip even an unexpected secret.
  });
  const created = await client.webhooks.create(createInput);
  assert.equal(created.signingSecret, secret); assert.equal(created.subscription.orgId, org);
  assert.equal(created.subscription.status, 'paused');
  assert.equal(created.subscription.channels[0].channelId, channel);
  assert.deepEqual((await client.getCapabilities()).messaging.outgoingWebhooks,
    { schemaVersions: [1], maxChannels: 50, workspaceChannelsOnly: true });
  assert.equal((await client.webhooks.list())[0].id, subscriptionId);
  const fetched = await client.webhooks.get(subscriptionId); assert.equal(fetched.id, subscriptionId); assert.ok(!JSON.stringify(fetched).includes(secret));
  assert.equal((await client.webhooks.pause(subscriptionId)).status, 'paused');
  assert.equal((await client.webhooks.resume(subscriptionId)).status, 'active');
  const rotated = await client.webhooks.rotateSecret(subscriptionId);
  assert.equal(rotated.signingSecret, previousSecret); assert.equal(rotated.subscription.secretVersion, 2);
  const page = await client.webhooks.deliveries(subscriptionId, { status: 'dead', limit: 10, before: subscriptionId });
  assert.equal(page.nextBefore, deliveryId); assert.equal(page.deliveries[0].eventId, epoch + ':1'); assert.equal(page.deliveries[0].lastStatus, 503);
  assert.equal((await client.webhooks.replay(subscriptionId, deliveryId)).status, 'pending');
  assert.deepEqual(await client.webhooks.delete(subscriptionId), { id: subscriptionId, deleted: true });
  assert.equal(calls.length, 9);
  for (const call of calls) assert.equal(new Headers(call.headers).get('authorization'), 'Bearer synthetic-webhook-admin');
});

test('webhook mutations never automatically repeat on network, retryable HTTP or unauthorized results', async t => {
  const methods = [client => client.webhooks.create(createInput), client => client.webhooks.pause(subscriptionId),
    client => client.webhooks.resume(subscriptionId), client => client.webhooks.rotateSecret(subscriptionId),
    client => client.webhooks.delete(subscriptionId), client => client.webhooks.replay(subscriptionId, deliveryId)];
  for (const status of [401, 503, 'network']) {
    for (let index = 0; index < methods.length; index++) await t.test(status + ' mutation ' + index, async child => {
      let writes = 0;
      const { client, reasons } = fixture(child, () => {
        writes++; if (status === 'network') throw new Error('Never copy this raw transport body');
        return Response.json({ message: 'Never copy this server body' }, { status });
      });
      await assert.rejects(methods[index](client), error => ['AUTHENTICATION', 'SERVER', 'NETWORK'].includes(error.code));
      assert.equal(writes, 1); assert.deepEqual(reasons, ['initial']);
    });
  }
});

test('an ambiguous create can be reconciled by caller ID and a new secret is obtained only through rotation', async t => {
  let exists = false, createCalls = 0, rotations = 0;
  const { client } = fixture(t, request => {
    if (request.url.pathname === '/api/webhooks' && request.method === 'POST') { exists = true; createCalls++; throw new Error('Lost committed response'); }
    assert.ok(exists); assert.ok(request.url.pathname.includes(subscriptionId));
    if (request.url.pathname.endsWith('/rotate-secret')) { rotations++; return Response.json({ ...result(subscription({ secret_version: 2 })), signing_secret: previousSecret }); }
    return Response.json(result(subscription()));
  });
  await assert.rejects(client.webhooks.create(createInput), error => error.code === 'NETWORK');
  const known = await client.webhooks.get(subscriptionId);
  assert.equal(known.id, subscriptionId); assert.equal('signingSecret' in known, false);
  assert.equal((await client.webhooks.rotateSecret(subscriptionId)).signingSecret, previousSecret);
  assert.equal(createCalls, 1); assert.equal(rotations, 1);
});

test('webhook reads retain bounded retry and management authorization errors remain typed', async t => {
  let reads = 0;
  const { client } = fixture(t, () => ++reads === 1 ? Response.json({}, { status: 503 }) : Response.json(result(subscription())));
  assert.equal((await client.webhooks.get(subscriptionId)).id, subscriptionId); assert.equal(reads, 2);
  for (const [status, code] of [[403, 'PERMISSION'], [404, 'NOT_FOUND'], [409, 'CONFLICT']]) {
    const failed = fixture(t, () => Response.json({ code: 'WEBHOOK_ROTATION_PENDING', message: secret }, { status })).client;
    await assert.rejects(failed.webhooks.rotateSecret(subscriptionId), error => error.code === code && !JSON.stringify(error).includes(secret));
  }
});

test('unsupported capabilities and malformed local grants fail before management traffic', async t => {
  const legacy = structuredClone(capabilities); delete legacy.messaging.outgoing_webhooks;
  const old = fixture(t, () => assert.fail('No management request expected'), { capabilities: legacy });
  await assert.rejects(old.client.webhooks.list(), UnsupportedFeatureError);
  assert.equal((await old.client.getCapabilities()).messaging.outgoingWebhooks, null);
  const valid = fixture(t, () => assert.fail('Invalid local input must never reach HTTP'));
  for (const value of [{ ...createInput, id: 'not-uuid' }, { ...createInput, channelIds: [] }, { ...createInput, channelIds: [channel, channel] },
    { ...createInput, channelIds: ['other-org:team:channel'] }, { ...createInput, url: 'https://user:secret@example.invalid/' },
    { ...createInput, url: 'https://example.invalid/#fragment' }, { ...createInput, url: 'file:///private' },
    { ...createInput, url: ' https://example.invalid/' }, { ...createInput, url: 'https://example.invalid/a\nb' },
    { ...createInput, url: 'https://example.invalid/' + 'x'.repeat(2048) }]) await assert.rejects(valid.client.webhooks.create(value), ValidationError);
  await assert.rejects(valid.client.webhooks.deliveries(subscriptionId, { limit: 101 }), ValidationError);
  await assert.rejects(valid.client.webhooks.replay(subscriptionId, '../escape'), ValidationError);
  assert.equal(valid.requests.length, 0);
  const limitedCap = structuredClone(capabilities); limitedCap.messaging.outgoing_webhooks.max_channels = 1;
  const limited = fixture(t, () => assert.fail('Grant exceeds advertised channel limit'), { capabilities: limitedCap });
  await assert.rejects(limited.client.webhooks.create({ ...createInput, channelIds: [channel, org + ':team:second'] }), ValidationError);
});

test('management responses cannot forge organization, grant, delivery or pagination scope', async t => {
  for (const [method, response] of [
    [client => client.webhooks.get(subscriptionId), result(subscription({ org_uuid: 'other' }))],
    [client => client.webhooks.get(subscriptionId), result(subscription({ id: deliveryId }))],
    [client => client.webhooks.get(subscriptionId), result(subscription({ channels: [{ channel_id: channel, epoch: 'not-a-uuid', sequence: '0', status: 'active', last_error: null }] }))],
    [client => client.webhooks.list(), { schema: 1, org_uuid: 'other', subscriptions: [] }],
    [client => client.webhooks.create(createInput), { ...result(subscription({ channels: [{ channel_id: org + ':team:other', epoch, sequence: '0', status: 'active', last_error: null }] })), signing_secret: secret }],
    [client => client.webhooks.create(createInput), { ...result(subscription()), signing_secret: 'invalid-secret' }],
    [client => client.webhooks.create(createInput), { ...result(subscription({ status: 'active' })), signing_secret: secret }],
    [client => client.webhooks.pause(subscriptionId), result(subscription())],
    [client => client.webhooks.delete(subscriptionId), { schema: 1, id: deliveryId, deleted: true }],
    [client => client.webhooks.deliveries(subscriptionId), { schema: 1, org_uuid: org, deliveries: [delivery({ channel_id: 'other:team:channel' })], next_before: null }],
    [client => client.webhooks.deliveries(subscriptionId), { schema: 1, org_uuid: org, deliveries: [delivery()], next_before: subscriptionId }],
    [client => client.webhooks.replay(subscriptionId, deliveryId), { schema: 1, delivery: delivery({ subscription_id: deliveryId }) }],
    [client => client.webhooks.replay(subscriptionId, deliveryId), { schema: 1, delivery: delivery({ epoch: 'not-a-uuid', event_id: 'not-a-uuid:1' }) }],
    [client => client.webhooks.replay(subscriptionId, deliveryId), { schema: 1, delivery: delivery({ event_id: 'epoch:99' }) }],
  ]) {
    const { client } = fixture(t, () => Response.json(response)); await assert.rejects(method(client), ProtocolError);
  }
});
