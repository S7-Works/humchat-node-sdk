import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat, MemoryCheckpointStore, SessionRevokedError } from '../../dist/index.js';

// This loopback protocol peer exercises the real SDK-key HTTP mint and real
// sockets together. All identities and credentials are synthetic.
const org = '00000000-0000-4000-8000-000000000041';
const actor = 'synthetic-renewal-member';
const email = 'synthetic.renewal@example.invalid';
const sdkKey = 'hsk_synthetic_renewal_fixture_only';
const channels = ['first', 'second'].map(name => `${org}:team:${name}`);
const token = number => `synthetic-renewal-token-${number}`;
const epoch = 'synthetic-renewal-epoch';
const cursor = (channelId, sequence = 0) => Buffer.from(JSON.stringify({ schema: 1, org_uuid: org,
  channel_id: channelId, epoch, sequence: String(sequence) })).toString('base64url');
const position = value => Number(JSON.parse(Buffer.from(value, 'base64url')).sequence);
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function until(condition, label, timeout = 4500) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await condition()) return; await sleep(5); }
  assert.fail(`Timed out: ${label}`);
}

async function peer(t, mode, options = {}) {
  const requests = [], mints = [], upgrades = [], sockets = [], errors = [], clients = [];
  const streams = new Map();
  const stream = channelId => { if (!streams.has(channelId)) streams.set(channelId, []); return streams.get(channelId); };
  const wss = new WebSocketServer({ noServer: true });
  const reply = (response, status, body, headers = {}) => {
    if (response.destroyed) return;
    response.writeHead(status, { 'content-type': 'application/json', ...headers });
    response.end(JSON.stringify(body));
  };
  const capabilities = { sdk_protocol_version: 1, authentication: { user_sessions: true, service_identities: false,
    api_key_exchange: false, app_binding_required: false, mock_authentication: true,
    sdk_key_user_tokens: { endpoint: '/api/sdk/auth/user-token', key_header: 'X-HumChat-Sdk-Key', configured: true },
    ...(options.advertiseExpiry === false ? {} : { socket_session_end_codes: ['TOKEN_EXPIRED'] }) },
    messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
      durable_replay: mode !== 'bestEffort', event_protocol_versions: [1], websocket_scope: 'channel',
      multiplexed_events: { protocol_versions: [1], max_channels: 100 } } };
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      const item = { url, headers: request.headers, at: Date.now(), request, response };
      requests.push(item);
      if (url.pathname === '/api/sdk/auth/user-token') {
        let body = '';
        for await (const part of request) body += part;
        item.body = JSON.parse(body);
        item.number = mints.length + 1;
        mints.push(item);
        const custom = await options.mint?.(item);
        if (custom?.status) return reply(response, custom.status, {}, custom.headers);
        const expiresIn = custom?.expiresIn ?? (item.number === 1 ? options.firstTtl ?? 900 : 900);
        return reply(response, 200, { access_token: token(item.number), token_type: 'Bearer', expires_in: expiresIn });
      }
      assert.equal(request.headers['x-humchat-sdk-key'], undefined, 'SDK key belongs only on mint');
      assert.match(request.headers.authorization ?? '', /^Bearer synthetic-renewal-token-\d+$/);
      if (url.pathname === '/me') {
        const identity = await options.identity?.(item);
        return reply(response, 200, { user_uuid: actor, org, email, roles: ['member'], scopes: [], ...identity });
      }
      if (url.pathname === '/api/sdk/capabilities') return reply(response, 200, capabilities);
      if (url.pathname === '/api/org/members') {
        const custom = await options.members?.(item);
        if (custom?.status) return reply(response, custom.status, {});
        return reply(response, 200, [{ user_uuid: actor, org_uuid: org, name: 'Synthetic member', email,
          role: 'member', status: 'active' }]);
      }
      if (url.pathname === '/api/messages/sync') return reply(response, 200, { schema: 2, org_uuid: org,
        channel_id: url.searchParams.get('channel_id'), thread_root_id: null, messages: [], next_cursor: null });
      if (url.pathname === '/api/events/sync') {
        const channelId = url.searchParams.get('channel_id');
        const after = url.searchParams.get('after');
        const through = url.searchParams.get('through') ?? cursor(channelId, stream(channelId).length);
        const events = stream(channelId).slice(position(after), position(through));
        return reply(response, 200, { schema: 1, org_uuid: org, channel_id: channelId, epoch, events,
          after_cursor: after, through_cursor: through, through_sequence: String(position(through)), next_cursor: null });
      }
      return reply(response, 404, {});
    } catch (error) { errors.push(error); reply(response, 500, {}); }
  });
  const ready = (channelId, subscriptionId) => ({ type: 'ready', schema: 1, org_uuid: org,
    channel_id: channelId, epoch, cursor: cursor(channelId, stream(channelId).length),
    sequence: String(stream(channelId).length), earliest_cursor: cursor(channelId),
    ...(subscriptionId ? { subscription_id: subscriptionId } : {}) });
  server.on('upgrade', (request, socket, head) => {
    const item = { url: new URL(request.url, 'http://127.0.0.1'), headers: request.headers, at: Date.now() };
    upgrades.push(item);
    try {
      assert.equal(request.headers['x-humchat-sdk-key'], undefined, 'Socket uses only its minted Bearer');
      assert.match(request.headers.authorization ?? '', /^Bearer synthetic-renewal-token-\d+$/);
      assert.equal(item.url.searchParams.has('token'), false);
      assert.equal(item.url.searchParams.has('sdk_key'), false);
      wss.handleUpgrade(request, socket, head, connected => {
        const entry = { ...item, socket: connected, subscriptions: new Map() };
        sockets.push(entry);
        connected.on('error', () => {});
        if (mode === 'multiplex') {
          connected.send(JSON.stringify({ type: 'stream_ready', schema: 1, org_uuid: org, max_channels: 100 }));
          connected.on('message', bytes => {
            try {
              const frame = JSON.parse(bytes.toString());
              if (frame.type === 'subscribe') {
                entry.subscriptions.set(frame.subscription_id, frame.channel_id);
                connected.send(JSON.stringify(ready(frame.channel_id, frame.subscription_id)));
              } else if (frame.type === 'unsubscribe') {
                entry.subscriptions.delete(frame.subscription_id);
                connected.send(JSON.stringify({ type: 'unsubscribed', schema: 1, subscription_id: frame.subscription_id }));
              }
            } catch (error) { errors.push(error); connected.terminate(); }
          });
        } else if (mode === 'durable') connected.send(JSON.stringify(ready(item.url.searchParams.get('channel_id'))));
      });
    } catch (error) { errors.push(error); socket.destroy(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const entry of sockets) entry.socket.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(errors, [], 'Synthetic peer must not hide fixture failures');
    for (const item of mints) {
      assert.equal(item.headers.authorization, undefined);
      assert.equal(item.headers['x-humchat-sdk-key'], sdkKey);
      assert.deepEqual(item.body, { org_uuid: org, email, expires_in: 900 });
    }
  });
  return {
    requests, mints, upgrades, sockets,
    scope(consumerId, channelId) { return { serverOrigin: `http://127.0.0.1:${server.address().port}`,
      orgId: org, actorId: actor, consumerId, channelId }; },
    append(channelId, messageId) {
      const rows = stream(channelId), sequence = String(rows.length + 1), timestamp = 1_792_370_346;
      const event = { type: 'event', schema: 1, org_uuid: org, channel_id: channelId, epoch, sequence,
        event_id: `${epoch}:${sequence}`, cursor: cursor(channelId, sequence), actor_id: actor, kind: 'message',
        state: { schema: 1, org_uuid: org, channel_id: channelId, message_id: messageId, sender_id: actor,
          created_at: timestamp, thread_root_id: null, priority: 'standard', original_body: messageId,
          body: messageId, revision: 1, updated_at: timestamp, deleted: false, reaction_revision: 0, reactions: {}, attachments: [] } };
      rows.push(event);
      return event;
    },
    client(extra = {}) {
      const client = new HumChat({ orgId: org, baseUrl: `http://127.0.0.1:${server.address().port}`,
        auth: { sdkKey, actAs: email }, timeout: 1500, retry: { maxRetries: 0, totalTimeoutMs: 4000 },
        recovery: { mode: mode === 'bestEffort' ? 'bestEffort' : 'durable' }, ...extra,
        realtime: { transport: mode === 'multiplex' ? 'multiplex' : 'channel', maxChannels: 5, connectConcurrency: 2,
          reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20, pingIntervalMs: 1000, pongTimeoutMs: 500,
          ...extra.realtime } });
      clients.push(client);
      return client;
    },
    send(frame) { for (const { socket } of sockets) if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(frame)); },
    drop() { for (const { socket } of sockets) socket.terminate(); },
  };
}

for (const mode of ['durable', 'multiplex']) {
  test(`${mode}: native renewal resumes durable consumption from its committed checkpoint`, { timeout: 8000 }, async t => {
    const entered = deferred(), release = deferred();
    t.after(() => release.resolve());
    const fixture = await peer(t, mode, { firstTtl: 3, mint: async item => {
      if (item.number === 2) { entered.resolve(); await release.promise; }
    } });
    const store = new MemoryCheckpointStore();
    t.after(() => store.close());
    const client = fixture.client(), consumerId = 'synthetic-renewal-checkpoint', seen = [];
    fixture.append(channels[0], 'before-native-renewal');
    const first = client.realtime.subscribe(channels[0], { start: 'earliest' });
    first.consume(event => { seen.push(event.state.id); }, { consumerId, checkpointStore: store });
    const second = client.realtime.subscribe(channels[1]);
    await Promise.all([first.ready, second.ready]);
    const scope = fixture.scope(consumerId, channels[0]);
    await until(async () => await store.load(scope) === cursor(channels[0], 1), 'first event checkpoint committed');
    await entered.promise;
    // No live hint is sent. Rotation must recover the new event through HTTP,
    // starting from the consumer's existing checkpoint rather than "now".
    fixture.append(channels[0], 'during-native-renewal');
    release.resolve();
    await until(async () => first.status === 'live' && second.status === 'live' &&
      await store.load(scope) === cursor(channels[0], 2), 'backlog consumed after token rotation');
    assert.deepEqual(seen, ['before-native-renewal', 'during-native-renewal']);
    assert.equal(fixture.mints.length, 2);
    assert.equal(fixture.upgrades.length, physicalSockets(mode) * 2);
    const catchup = fixture.requests.filter(item => item.url.pathname === '/api/events/sync' &&
      item.headers.authorization === `Bearer ${token(2)}` && item.url.searchParams.get('channel_id') === channels[0]);
    assert.ok(catchup.some(item => item.url.searchParams.get('after') === cursor(channels[0], 1)),
      'New Bearer catches up from the acknowledged position');
  });
}

async function subscribe(client) {
  const subscriptions = channels.map(channel => client.realtime.subscribe(channel));
  const errors = [];
  for (const sub of subscriptions) sub.on('error', error => { errors.push(error); });
  await Promise.all(subscriptions.map(sub => sub.ready));
  return { subscriptions, errors, live: () => subscriptions.every(sub => sub.status === 'live'),
    blocked: () => subscriptions.every(sub => sub.status === 'blocked') };
}
const physicalSockets = mode => mode === 'multiplex' ? 1 : 2;

for (const mode of ['bestEffort', 'durable', 'multiplex']) {
  test(`${mode}: native SDK-key expiry shares one mint and rotates only Bearer credentials`, { timeout: 8000 }, async t => {
    const fixture = await peer(t, mode, { firstTtl: 3 });
    const client = fixture.client();
    const active = await subscribe(client);
    await until(() => fixture.upgrades.length === physicalSockets(mode) * 2 && active.live(), 'replacement sockets');
    assert.equal(fixture.mints.length, 2, 'Both channels share exactly one renewal');
    const renewed = fixture.upgrades.slice(physicalSockets(mode));
    assert.ok(renewed.every(item => item.headers.authorization === `Bearer ${token(2)}`));
    const firstExpiry = (Math.floor(fixture.mints[0].at / 1000) + 3) * 1000;
    assert.ok(renewed.every(item => item.at < firstExpiry), 'Sockets rotate before the original token expires');
    assert.equal((await client.connect()).actorId, actor);
    assert.equal((await client.members.list()).length, 1);
    assert.equal(fixture.mints.length, 2, 'Verified renewed identity is cached for HTTP');
    assert.equal(client.realtime.getStats().activeSockets, physicalSockets(mode));
  });

  test(`${mode}: an old HTTP 401 joins the socket renewal already in flight`, { timeout: 8000 }, async t => {
    const oldRequest = deferred(), release401 = deferred(), renewal = deferred(), releaseMint = deferred();
    t.after(() => { release401.resolve(); releaseMint.resolve(); });
    const fixture = await peer(t, mode, { firstTtl: 3,
      mint: async item => { if (item.number === 2) { renewal.resolve(); await releaseMint.promise; } },
      members: async item => { if (item.headers.authorization === `Bearer ${token(1)}`) {
        oldRequest.resolve(); await release401.promise; return { status: 401 };
      } },
    });
    const client = fixture.client();
    const active = await subscribe(client);
    const pending = client.members.list();
    await oldRequest.promise;
    await renewal.promise;
    release401.resolve();
    await sleep(30);
    assert.equal(fixture.mints.length, 2, '401 must not start a competing mint');
    releaseMint.resolve();
    assert.equal((await pending).length, 1);
    await until(() => fixture.upgrades.length === physicalSockets(mode) * 2 && active.live(), 'all sockets adopt shared renewal');
    assert.equal(fixture.mints.length, 2);
    const roster = fixture.requests.filter(item => item.url.pathname === '/api/org/members');
    assert.deepEqual(roster.map(item => item.headers.authorization), [`Bearer ${token(1)}`, `Bearer ${token(2)}`]);
  });

  test(`${mode}: ordinary disconnect reuses the verified token without another mint`, { timeout: 6000 }, async t => {
    const fixture = await peer(t, mode), client = fixture.client();
    const active = await subscribe(client);
    fixture.drop();
    await until(() => fixture.upgrades.length >= physicalSockets(mode) * 2 && active.live(), 'network reconnection');
    assert.equal(fixture.mints.length, 1);
    assert.ok(fixture.upgrades.every(item => item.headers.authorization === `Bearer ${token(1)}`));
    assert.equal((await client.connect()).actorId, actor);
  });

  test(`${mode}: transient mint failures retry within one shared renewal`, { timeout: 8000 }, async t => {
    const fixture = await peer(t, mode, { firstTtl: 3, mint: item => item.number === 2 || item.number === 3 ? { status: 503 } : undefined });
    const client = fixture.client({ retry: { maxRetries: 2, initialDelayMs: 5, maxDelayMs: 10, totalTimeoutMs: 4000 } });
    const active = await subscribe(client);
    await until(() => fixture.upgrades.length === physicalSockets(mode) * 2 && active.live(), 'renewal after bounded transient retries');
    assert.equal(fixture.mints.length, 4, 'One renewal makes two retries after its first attempt');
    assert.ok(fixture.upgrades.slice(physicalSockets(mode)).every(item => item.headers.authorization === `Bearer ${token(4)}`));
    await sleep(60);
    assert.equal(fixture.mints.length, 4);
  });

  test(`${mode}: exhausted transient mint attempts reconnect and recover without permanent blocking`, { timeout: 8000 }, async t => {
    const fixture = await peer(t, mode, { firstTtl: 3, mint: item => item.number === 2 ? { status: 503 } : undefined });
    const client = fixture.client();
    const active = await subscribe(client);
    await until(() => fixture.mints.length === 3 && fixture.upgrades.length === physicalSockets(mode) * 2 && active.live(),
      'a fresh reconnect cycle retries the transient login outage');
    assert.ok(fixture.upgrades.slice(physicalSockets(mode)).every(item => item.headers.authorization === `Bearer ${token(3)}`));
    assert.ok(active.errors.every(error => error.status !== 401 && error.status !== 403));
    assert.equal(client.realtime.getStats().activeSockets, physicalSockets(mode));
  });

  test(`${mode}: rate-limited renewal honors Retry-After and still shares one token`, { timeout: 8000 }, async t => {
    const fixture = await peer(t, mode, { firstTtl: 3,
      mint: item => item.number === 2 ? { status: 429, headers: { 'retry-after': '0.08' } } : undefined });
    const client = fixture.client({ retry: { maxRetries: 1, initialDelayMs: 1, maxDelayMs: 1, totalTimeoutMs: 4000 } });
    const active = await subscribe(client);
    await until(() => fixture.upgrades.length === physicalSockets(mode) * 2 && active.live(), 'recovery after login rate limit');
    assert.equal(fixture.mints.length, 3);
    assert.ok(fixture.mints[2].at - fixture.mints[1].at >= 70, 'Retry-After delays the next mint attempt');
    assert.ok(fixture.upgrades.slice(physicalSockets(mode)).every(item => item.headers.authorization === `Bearer ${token(3)}`));
  });

  for (const status of [401, 403]) test(`${mode}: mint ${status} stops refresh and socket reconnect`, { timeout: 8000 }, async t => {
    const fixture = await peer(t, mode, { firstTtl: 3, mint: item => item.number > 1 ? { status } : undefined });
    const client = fixture.client({ retry: { maxRetries: 2, initialDelayMs: 5, maxDelayMs: 10, totalTimeoutMs: 4000 } });
    const active = await subscribe(client);
    await until(active.blocked, 'permanent login failure blocks both subscriptions');
    await sleep(100);
    assert.equal(fixture.mints.length, 2, 'Permanent mint rejection must not retry');
    assert.equal(fixture.upgrades.length, physicalSockets(mode));
    assert.equal(client.realtime.getStats().activeSockets, 0);
    assert.ok(active.errors.some(error => error.status === status));
    await assert.rejects(client.members.list(), error => error.status === status);
    assert.equal(fixture.mints.length, 2, 'Permanent key rejection remains latched for future HTTP calls');
  });

  test(`${mode}: ambiguous session_revoked is terminal even when its reason says expired`, { timeout: 6000 }, async t => {
    const fixture = await peer(t, mode), client = fixture.client();
    const active = await subscribe(client);
    fixture.send({ type: 'session_revoked', reason: 'session expired' });
    await until(active.blocked, 'revocation blocks every subscription');
    await assert.rejects(client.members.list(), SessionRevokedError);
    await sleep(100);
    assert.equal(fixture.mints.length, 1, 'A reason string must never authorize reminting');
    assert.equal(fixture.upgrades.length, physicalSockets(mode));
    assert.equal(client.realtime.getStats().activeSockets, 0);
  });

  test(`${mode}: only explicit TOKEN_EXPIRED control permits shared remint and reconnect`, { timeout: 6000 }, async t => {
    const entered = deferred(), release = deferred();
    t.after(() => release.resolve());
    const fixture = await peer(t, mode, { mint: async item => {
      if (item.number === 2) { entered.resolve(); await release.promise; }
    } });
    const client = fixture.client();
    const active = await subscribe(client);
    const revoked = [];
    for (const sub of active.subscriptions) sub.on('sessionRevoked', event => { revoked.push(event); });
    // Multiple channels and duplicate queued frames all refer to the same old
    // session; they must not produce independent renewals or revoke the client.
    fixture.send({ type: 'session_revoked', code: 'TOKEN_EXPIRED', reason: 'session expired' });
    fixture.send({ type: 'session_revoked', code: 'TOKEN_EXPIRED', reason: 'session expired' });
    await entered.promise;
    await sleep(20);
    assert.equal(fixture.mints.length, 2);
    release.resolve();
    await until(() => fixture.upgrades.length === physicalSockets(mode) * 2 && active.live(), 'explicit expiry recovery');
    await sleep(50);
    assert.equal(fixture.mints.length, 2, 'Queued control from old sockets cannot cause a third mint');
    assert.deepEqual(revoked, []);
    assert.equal((await client.members.list()).length, 1);
    assert.ok(fixture.upgrades.slice(physicalSockets(mode)).every(item => item.headers.authorization === `Bearer ${token(2)}`));
  });

  test(`${mode}: unknown control code remains terminal regardless of expiry reason`, { timeout: 6000 }, async t => {
    const fixture = await peer(t, mode), client = fixture.client();
    const active = await subscribe(client);
    fixture.send({ type: 'session_revoked', code: 'FUTURE_OR_UNKNOWN_REASON', reason: 'session expired' });
    await until(active.blocked, 'unknown reason stays terminal');
    await assert.rejects(client.members.list(), SessionRevokedError);
    await sleep(60);
    assert.equal(fixture.mints.length, 1);
    assert.equal(fixture.upgrades.length, physicalSockets(mode));
  });

  test(`${mode}: unadvertised TOKEN_EXPIRED control never authorizes minting`, { timeout: 6000 }, async t => {
    const fixture = await peer(t, mode, { advertiseExpiry: false }), client = fixture.client();
    const active = await subscribe(client);
    fixture.send({ type: 'session_revoked', code: 'TOKEN_EXPIRED', reason: 'session expired' });
    await until(active.blocked, 'unadvertised control stays terminal');
    await assert.rejects(client.members.list(), SessionRevokedError);
    await sleep(60);
    assert.equal(fixture.mints.length, 1);
    assert.equal(fixture.upgrades.length, physicalSockets(mode));
  });

  test(`${mode}: revocation invalidates a pending mint even if custom fetch ignores cancellation`, { timeout: 8000 }, async t => {
    const entered = deferred(), release = deferred();
    t.after(() => release.resolve());
    const fixture = await peer(t, mode, { firstTtl: 3, mint: async item => {
      if (item.number === 2) { entered.resolve(); await release.promise; }
    } });
    const client = fixture.client({ fetch: (url, init) => globalThis.fetch(url, { ...init, signal: undefined }) });
    const active = await subscribe(client);
    await entered.promise;
    fixture.send({ type: 'session_revoked', reason: 'session deprovisioned' });
    await until(active.blocked, 'revocation interrupts pending renewal');
    await assert.rejects(client.members.list(), SessionRevokedError);
    release.resolve();
    await sleep(100);
    assert.equal(fixture.mints.length, 2);
    assert.equal(fixture.requests.filter(item => item.url.pathname === '/me').length, 1,
      'A late minted token must not reach identity verification after revocation');
    assert.equal(fixture.upgrades.length, physicalSockets(mode));
    assert.equal(client.realtime.getStats().activeSockets, 0);
  });

  test(`${mode}: close cancels a pending mint and prevents late sockets or timer work`, { timeout: 8000 }, async t => {
    const entered = deferred(), release = deferred(), cancelled = deferred();
    t.after(() => release.resolve());
    const fixture = await peer(t, mode, { firstTtl: 3, mint: async item => {
      if (item.number === 2) {
        item.response.once('close', () => cancelled.resolve());
        entered.resolve(); await release.promise;
      }
    } });
    const client = fixture.client();
    await subscribe(client);
    await entered.promise;
    await client.close();
    await Promise.race([cancelled.promise, sleep(1500).then(() => assert.fail('Pending mint was not cancelled'))]);
    release.resolve();
    await sleep(100);
    assert.equal(fixture.mints.length, 2);
    assert.equal(fixture.upgrades.length, physicalSockets(mode));
    assert.equal(client.realtime.getStats().activeSockets, 0);
    await assert.rejects(client.connect(), error => error.code === 'ABORT');
  });

  test(`${mode}: renewed identity drift blocks sockets before a new Bearer is used`, { timeout: 8000 }, async t => {
    const fixture = await peer(t, mode, { firstTtl: 3, identity: item => item.headers.authorization === `Bearer ${token(2)}`
      ? { user_uuid: 'synthetic-different-member' } : undefined });
    const client = fixture.client();
    const active = await subscribe(client);
    await until(active.blocked, 'identity drift blocks both subscriptions');
    await assert.rejects(client.members.list(), error => error.code === 'AUTHENTICATION');
    await sleep(60);
    assert.equal(fixture.mints.length, 2);
    assert.equal(fixture.upgrades.length, physicalSockets(mode), 'Mismatched token never reaches any socket');
    assert.equal(client.realtime.getStats().activeSockets, 0);
  });
}
