import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat, MemoryCheckpointStore, UnsupportedFeatureError, PermissionError, ProtocolError, SessionRevokedError } from '../../dist/index.js';

const org = 'multiplex-org', actor = 'multiplex-actor', epoch = 'multiplex-epoch';
const channel = name => `${org}:team:${name}`;
const first = channel('first'), second = channel('second');
const cursor = (id, sequence) => Buffer.from(JSON.stringify({ schema: 1, org_uuid: org, channel_id: id, epoch, sequence: String(sequence) })).toString('base64url');
const position = value => Number(JSON.parse(Buffer.from(value, 'base64url')).sequence);
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function until(condition, message, timeout = 5000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { if (await condition()) return; await sleep(5); }
  assert.fail('Timed out: ' + message);
}
function observe(subscription) {
  const result = { event: [], error: [], checkpoint: [], typing: [], receipt: [], connection: [] };
  for (const name of Object.keys(result)) subscription.on(name, value => { result[name].push(value); });
  return result;
}
async function peer(t, options = {}) {
  const streams = new Map(), clients = [], sockets = [], upgrades = [], commands = [], requests = [], failures = [];
  const stream = id => { if (!streams.has(id)) streams.set(id, []); return streams.get(id); };
  const wss = new WebSocketServer({ noServer: true, autoPong: options.autoPong ?? true });
  const reply = (response, status, body) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(body)); };
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      if (url.pathname === '/me') return reply(response, 200, { user_uuid: actor, org, email: 'multiplex@example.invalid', roles: ['member'], scopes: [] });
      if (url.pathname === '/api/sdk/capabilities') return reply(response, 200, { sdk_protocol_version: 1,
        authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
        messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: true, event_protocol_versions: [1], websocket_scope: 'channel',
          ...(options.supported === false ? {} : { multiplexed_events: { protocol_versions: [1], max_channels: options.maxChannels ?? 100 } }) } });
      if (url.pathname === '/api/events/sync') {
        const id = url.searchParams.get('channel_id'), rows = stream(id), after = url.searchParams.get('after');
        const through = url.searchParams.get('through') ?? cursor(id, rows.length);
        requests.push({ channelId: id, after, through });
        const events = rows.slice(position(after), position(through)).slice(0, Number(url.searchParams.get('limit') ?? 200));
        const last = events.at(-1)?.cursor ?? after;
        const page = { schema: 1, org_uuid: org, channel_id: id, epoch, events, after_cursor: after, through_cursor: through,
          through_sequence: String(position(through)), next_cursor: last === through ? null : last };
        return reply(response, 200, await options.sync?.({ page, channelId: id }) ?? page);
      }
      reply(response, 404, {});
    } catch (error) { failures.push(error); if (!response.headersSent) reply(response, 500, {}); else response.end(); }
  });
  server.on('upgrade', (request, socket, head) => {
    upgrades.push({ url: new URL(request.url, 'http://127.0.0.1'), headers: request.headers });
    const rejection = options.upgrade?.({ request, upgrades });
    if (rejection) { socket.end(`HTTP/1.1 ${rejection} Rejected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`); return; }
    wss.handleUpgrade(request, socket, head, connected => {
      const item = { socket: connected, subscriptions: new Map() }; sockets.push(item); connected.on('error', () => {});
      connected.send(JSON.stringify({ type: 'stream_ready', schema: 1, org_uuid: org, max_channels: options.maxChannels ?? 100 }));
      connected.on('message', bytes => {
        try {
          const raw = JSON.parse(bytes.toString()); commands.push(raw);
          if (raw.type === 'subscribe') {
            assert.match(raw.subscription_id, /^[\x21-\x7e]{1,128}$/);
            assert.ok(raw.channel_id.startsWith(org + ':'));
            item.subscriptions.set(raw.subscription_id, raw.channel_id);
            if (options.deny === raw.channel_id) return connected.send(JSON.stringify({ type: 'channel_error', schema: 1,
              subscription_id: raw.subscription_id, channel_id: raw.channel_id, code: 'FORBIDDEN', status: 403, retryable: false }));
            if (options.noReady === raw.channel_id) return;
            connected.send(JSON.stringify(ready(raw.channel_id, raw.subscription_id)));
          } else if (raw.type === 'unsubscribe') {
            item.subscriptions.delete(raw.subscription_id);
            connected.send(JSON.stringify({ type: 'unsubscribed', schema: 1, subscription_id: raw.subscription_id }));
          } else {
            assert.equal(item.subscriptions.get(raw.subscription_id), raw.channel_id);
            if (raw.type === 'typing') connected.send(JSON.stringify({ type: 'typing', org_uuid: org, channel_id: raw.channel_id,
              subscription_id: raw.subscription_id, user_uuid: actor, ts: 1_792_370_346, ttl_ms: 5000 }));
            else if (raw.type === 'read') connected.send(JSON.stringify({ type: 'receipt', org_uuid: org, channel_id: raw.channel_id,
              subscription_id: raw.subscription_id, user_uuid: actor, ts: raw.ts }));
            else assert.fail('Unexpected client command');
          }
        } catch (error) { failures.push(error); connected.terminate(); }
      });
    });
  });
  const ready = (id, subscriptionId) => ({ type: 'ready', schema: 1, org_uuid: org, channel_id: id, subscription_id: subscriptionId,
    epoch, cursor: cursor(id, stream(id).length), sequence: String(stream(id).length), earliest_cursor: cursor(id, 0) });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = 'http://127.0.0.1:' + server.address().port;
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const { socket } of sockets) socket.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(failures, [], 'Multiplex fixture errors');
  });
  return {
    origin, streams, sockets, upgrades, commands, requests,
    client(extra = {}) {
      const client = new HumChat({ orgId: org, baseUrl: origin, tokenProvider: async () => ({ token: 'synthetic-multiplex-token' }),
        timeout: 500, retry: { maxRetries: 0, totalTimeoutMs: 1500 }, ...extra,
        realtime: { transport: 'multiplex', reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20, handlerTimeoutMs: 1500,
          pingIntervalMs: 1000, pongTimeoutMs: 500, ...extra.realtime } });
      clients.push(client); return client;
    },
    scope(consumerId, channelId) { return { serverOrigin: origin, orgId: org, actorId: actor, consumerId, channelId }; },
    append(id, messageId) {
      const rows = stream(id), sequence = String(rows.length + 1);
      const event = { type: 'event', schema: 1, org_uuid: org, channel_id: id, epoch, sequence, event_id: epoch + ':' + sequence,
        cursor: cursor(id, sequence), actor_id: actor, kind: 'message', state: { schema: 1, org_uuid: org, channel_id: id,
          message_id: messageId, sender_id: actor, created_at: 1_792_370_346, thread_root_id: null, priority: 'standard', original_body: messageId,
          body: messageId, revision: 1, updated_at: 1_792_370_346, deleted: false, reaction_revision: 0, reactions: {}, attachments: [] } };
      rows.push(event); return event;
    },
    hint(id) { for (const item of sockets) if (item.socket.readyState === WebSocket.OPEN) for (const [key, channelId] of item.subscriptions) {
      if (channelId === id) item.socket.send(JSON.stringify({ ...ready(id, key), type: 'watermark' }));
    } },
    send(raw) { for (const { socket } of sockets) if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(raw)); },
    drop() { for (const { socket } of sockets) socket.terminate(); },
  };
}

test('multiplex shares one header-authenticated physical socket and preserves channel/refcount isolation', { timeout: 8000 }, async t => {
  const fixture = await peer(t), client = fixture.client();
  const a = client.realtime.subscribe(first), duplicate = client.realtime.subscribe(first), b = client.realtime.subscribe(second);
  const seenA = observe(a), seenB = observe(b);
  await Promise.all([a.ready, duplicate.ready, b.ready]);
  assert.equal(fixture.upgrades.length, 1);
  assert.equal(fixture.commands.filter(value => value.type === 'subscribe').length, 2);
  assert.equal(fixture.upgrades[0].url.pathname, '/ws/events');
  assert.equal(fixture.upgrades[0].url.search, '');
  assert.equal(fixture.upgrades[0].headers.authorization, 'Bearer synthetic-multiplex-token');
  assert.equal(fixture.upgrades[0].headers['x-humchat-stream-protocol'], 'v1');
  assert.equal(client.realtime.getStats().activeSockets, 1);
  assert.equal(client.realtime.getStats().subscriptions, 3);
  fixture.append(first, 'a1'); fixture.append(second, 'b1'); fixture.hint(first); fixture.hint(second);
  await until(() => seenA.event.length === 1 && seenB.event.length === 1, 'independent replay');
  assert.equal(seenA.event[0].channelId, first); assert.equal(seenB.event[0].channelId, second);
  await a.typing(); await b.markRead({ through: 123 });
  await until(() => seenA.typing.length === 1 && seenB.receipt.length === 1, 'scoped ephemeral signals');
  assert.equal(seenA.receipt.length, 0); assert.equal(seenB.typing.length, 0);
  await a.close(); assert.equal(client.realtime.getStats().activeSockets, 1);
  assert.equal(fixture.commands.filter(value => value.type === 'unsubscribe').length, 0);
  await duplicate.close(); await until(() => fixture.commands.some(value => value.type === 'unsubscribe'), 'last-handle unsubscribe');
  fixture.append(second, 'b2'); fixture.hint(second); await until(() => seenB.event.length === 2, 'sibling remains live');
  await b.close(); await until(() => client.realtime.getStats().activeSockets === 0, 'idle stream closed');
});

test('multiplex requires advertised support and does not fall back to per-channel sockets', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { supported: false }), client = fixture.client();
  const subscription = client.realtime.subscribe(first);
  await assert.rejects(subscription.ready, UnsupportedFeatureError);
  assert.equal(subscription.status, 'blocked'); assert.equal(fixture.upgrades.length, 0);
});

test('channel denial and ready timeout block only their channel', { timeout: 6000 }, async t => {
  const denied = channel('denied'), timeout = channel('timeout');
  const fixture = await peer(t, { deny: denied, noReady: timeout }), client = fixture.client();
  const a = client.realtime.subscribe(first), forbidden = client.realtime.subscribe(denied), slow = client.realtime.subscribe(timeout);
  await a.ready; await assert.rejects(forbidden.ready, PermissionError); await assert.rejects(slow.ready);
  assert.equal(a.status, 'live'); assert.equal(forbidden.status, 'blocked'); assert.equal(slow.status, 'blocked');
  assert.equal(client.realtime.getStats().activeSockets, 1); assert.equal(fixture.upgrades.length, 1);
});

test('replacement subscription ignores stale ready/watermark/ephemeral frames from old IDs', { timeout: 6000 }, async t => {
  const fixture = await peer(t), client = fixture.client();
  const keep = client.realtime.subscribe(second), old = client.realtime.subscribe(first);
  await Promise.all([keep.ready, old.ready]);
  const prior = fixture.commands.find(command => command.type === 'subscribe' && command.channel_id === first).subscription_id;
  await old.close();
  const next = client.realtime.subscribe(first), seen = observe(next); await next.ready;
  const current = fixture.commands.filter(command => command.type === 'subscribe' && command.channel_id === first).at(-1).subscription_id;
  assert.notEqual(prior, current);
  fixture.send({ type: 'ready', subscription_id: prior, channel_id: second });
  fixture.send({ type: 'watermark', subscription_id: prior, channel_id: second });
  fixture.send({ type: 'typing', subscription_id: prior, channel_id: second });
  fixture.append(first, 'fresh'); fixture.hint(first);
  await until(() => seen.event.length === 1, 'replacement receives current event');
  assert.equal(seen.error.length, 0); assert.equal(seen.typing.length, 0); assert.equal(keep.status, 'live');
});

test('shared socket loss replays each checkpoint once and preserves start=now after reconnect', { timeout: 8000 }, async t => {
  const fixture = await peer(t), client = fixture.client();
  fixture.append(first, 'before-start');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second);
  const seenA = [], seenB = [];
  a.consume(event => { seenA.push(event.state.id); }, { consumerId: 'a', checkpointStore: store });
  b.consume(event => { seenB.push(event.state.id); }, { consumerId: 'b', checkpointStore: store });
  await Promise.all([a.ready, b.ready]);
  fixture.drop(); fixture.append(first, 'after-loss-a'); fixture.append(second, 'after-loss-b');
  await until(() => seenA.length === 1 && seenB.length === 1, 'both channels catch up');
  assert.deepEqual(seenA, ['after-loss-a']); assert.deepEqual(seenB, ['after-loss-b']);
  assert.equal(fixture.upgrades.length, 2); assert.equal(client.realtime.getStats().reconnectCount, 1);
  assert.equal(await store.load(fixture.scope('a', first)), cursor(first, 2));
  assert.equal(await store.load(fixture.scope('b', second)), cursor(second, 1));
});

test('slow replay handlers do not consume socket admission slots or overlap across reconnect', { timeout: 8000 }, async t => {
  const fixture = await peer(t), client = fixture.client({ realtime: { connectConcurrency: 1 } });
  fixture.append(first, 'slow'); fixture.append(second, 'fast');
  const entered = deferred(), gate = deferred(), store = new MemoryCheckpointStore(); t.after(() => store.close());
  let active = 0, maximum = 0, calls = 0;
  const a = client.realtime.subscribe(first, { start: 'earliest' });
  a.consume(async () => { active++; maximum = Math.max(maximum, active); calls++; entered.resolve(); if (calls === 1) await gate.promise; active--; },
    { consumerId: 'slow', checkpointStore: store });
  const b = client.realtime.subscribe(second, { start: 'earliest' }), seen = observe(b);
  await entered.promise; await b.ready;
  assert.equal(seen.event.length, 1); assert.equal(a.status, 'syncing');
  fixture.drop(); await until(() => fixture.upgrades.length === 2 && b.status === 'live', 'other channel reconnects');
  assert.equal(calls, 1); assert.equal(await store.load(fixture.scope('slow', first)), cursor(first, 0));
  gate.resolve(); await a.ready;
  assert.equal(calls, 2); assert.equal(maximum, 1);
  assert.equal(await store.load(fixture.scope('slow', first)), cursor(first, 1));
});

test('shared authentication retries one 401 exactly once for all channels', { timeout: 6000 }, async t => {
  const fixture = await peer(t, { upgrade: ({ upgrades }) => upgrades.length === 1 ? 401 : undefined });
  const reasons = [];
  const client = fixture.client({ tokenProvider: async ({ reason }) => { reasons.push(reason); return { token: 'synthetic-' + reasons.length }; } });
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second);
  await Promise.all([a.ready, b.ready]);
  assert.deepEqual(reasons, ['initial', 'unauthorized']); assert.equal(fixture.upgrades.length, 2);
  assert.equal(fixture.upgrades[1].headers.authorization, 'Bearer synthetic-2');
  assert.equal(fixture.commands.filter(command => command.type === 'subscribe').length, 2, 'Shared refresh must not cause redundant per-channel retries');
  assert.equal(client.realtime.getStats().activeSockets, 1);
});

test('permanent 403 and repeated 401 preserve their original error and never fan out refreshes', { timeout: 7000 }, async t => {
  for (const status of [403, 401]) await t.test(String(status), async child => {
    const fixture = await peer(child, { upgrade: () => status }); let calls = 0;
    const client = fixture.client({ tokenProvider: async () => ({ token: 'synthetic-' + ++calls }) });
    const subscriptions = [client.realtime.subscribe(first), client.realtime.subscribe(second)];
    for (const sub of subscriptions) await assert.rejects(sub.ready, error => error.status === status);
    await sleep(60);
    assert.equal(calls, status === 401 ? 2 : 1); assert.equal(fixture.upgrades.length, status === 401 ? 2 : 1);
    assert.ok(subscriptions.every(sub => sub.status === 'blocked'));
  });
});

test('one session_revoked terminates every channel without credential refresh or reconnect', { timeout: 5000 }, async t => {
  const fixture = await peer(t); let calls = 0;
  const client = fixture.client({ tokenProvider: async () => ({ token: 'synthetic-' + ++calls }) });
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second); observe(a); observe(b);
  await Promise.all([a.ready, b.ready]); fixture.send({ type: 'session_revoked', reason: 'session no longer valid' });
  await until(() => a.status === 'blocked' && b.status === 'blocked', 'global revocation');
  await sleep(60); assert.equal(calls, 1); assert.equal(fixture.upgrades.length, 1);
  assert.equal(client.realtime.getStats().activeSockets, 0);
  assert.throws(() => client.realtime.subscribe(channel('new')), SessionRevokedError);
});

test('shared expiry rotates one stream and preserves both subscriptions', { timeout: 7000 }, async t => {
  const fixture = await peer(t); let calls = 0;
  const client = fixture.client({ tokenProvider: async () => ({ token: 'synthetic-' + ++calls,
    expiresAt: Math.ceil(Date.now() / 1000) + (calls === 1 ? 2 : 300) }) });
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second);
  await Promise.all([a.ready, b.ready]);
  await until(() => fixture.upgrades.length === 2 && a.status === 'live' && b.status === 'live', 'shared credential rotation');
  assert.equal(calls, 2); assert.equal(client.realtime.getStats().reconnectCount, 1);
  assert.equal(client.realtime.getStats().activeSockets, 1);
});

test('missed final watermark is recovered by one shared periodic poll', { timeout: 8500 }, async t => {
  const fixture = await peer(t), client = fixture.client();
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second), seenA = observe(a), seenB = observe(b);
  await Promise.all([a.ready, b.ready]); fixture.append(first, 'unhinted-a'); fixture.append(second, 'unhinted-b');
  await until(() => seenA.event.length === 1 && seenB.event.length === 1, 'periodic authoritative replay', 6500);
  assert.equal(fixture.upgrades.length, 1); assert.equal(client.realtime.getStats().reconnectCount, 0);
});

test('cross-channel correlation forgery and stream_error terminate the stream', { timeout: 6000 }, async t => {
  for (const type of ['forged', 'stream_error']) await t.test(type, async child => {
    const fixture = await peer(child), client = fixture.client();
    const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second), seen = observe(a);
    await Promise.all([a.ready, b.ready]);
    const id = fixture.commands.find(command => command.channel_id === first).subscription_id;
    fixture.send(type === 'forged' ? { type: 'watermark', subscription_id: id, channel_id: second } : { type: 'stream_error', schema: 1, code: 'INVALID_REQUEST' });
    await until(() => a.status === 'blocked' && b.status === 'blocked', 'terminal stream protocol error');
    assert.ok(seen.error.some(error => error instanceof ProtocolError)); assert.equal(client.realtime.getStats().activeSockets, 0);
    await sleep(30); assert.equal(fixture.upgrades.length, 1);
  });
});

test('100 explicit channels share one socket and server limits count pending admissions', { timeout: 12000 }, async t => {
  const fixture = await peer(t), client = fixture.client({ realtime: { maxChannels: 100, connectConcurrency: 7 } });
  const handles = Array.from({ length: 100 }, (_, index) => client.realtime.subscribe(channel('c' + index)));
  await Promise.all(handles.map(handle => handle.ready));
  assert.equal(fixture.upgrades.length, 1); assert.equal(client.realtime.getStats().activeSockets, 1);
  assert.equal(client.realtime.getStats().channels, 100);
  assert.throws(() => client.realtime.subscribe(channel('overflow')), error => error.code === 'BACKPRESSURE');
  const limited = await peer(t, { maxChannels: 1 }), limitedClient = limited.client();
  const a = limitedClient.realtime.subscribe(first), b = limitedClient.realtime.subscribe(second);
  await a.ready; await assert.rejects(b.ready, error => error.code === 'BACKPRESSURE');
  assert.equal(a.status, 'live'); assert.equal(limited.upgrades.length, 1);
});

test('large durable backlog and sibling delivery remain ordered within shared page budgets', { timeout: 10000 }, async t => {
  const fixture = await peer(t), client = fixture.client({ realtime: { maxBufferedEvents: 400 } });
  for (let i = 1; i <= 601; i++) fixture.append(first, 'message-' + i);
  fixture.append(second, 'sibling');
  const a = client.realtime.subscribe(first, { start: 'earliest' }), b = client.realtime.subscribe(second, { start: 'earliest' });
  const seenA = observe(a), seenB = observe(b); await Promise.all([a.ready, b.ready]);
  assert.deepEqual(seenA.event.map(event => Number(event.sequence)), Array.from({ length: 601 }, (_, i) => i + 1));
  assert.equal(seenB.event.length, 1); assert.equal(fixture.upgrades.length, 1);
  assert.equal(client.realtime.getStats().queuedEvents, 0); assert.equal(client.realtime.getStats().queuedBytes, 0);
});

test('impossible command and oversized frame limits fail once without an endless shared reconnect loop', { timeout: 6000 }, async t => {
  await t.test('command cannot fit', async child => {
    const fixture = await peer(child), client = fixture.client({ realtime: { maxBufferedBytes: 16 } });
    const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second);
    for (const handle of [a, b]) await assert.rejects(handle.ready, error => error.code === 'BACKPRESSURE');
    await sleep(50); assert.equal(fixture.upgrades.length, 1); assert.equal(client.realtime.getStats().activeSockets, 0);
  });
  await t.test('frame cannot fit', async child => {
    const fixture = await peer(child), client = fixture.client({ realtime: { maxPayloadBytes: 4096 } });
    const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second), seen = observe(a);
    await Promise.all([a.ready, b.ready]); fixture.send({ type: 'oversized', data: 'x'.repeat(5000) });
    await until(() => a.status === 'blocked' && b.status === 'blocked', 'bounded frame rejection');
    assert.ok(seen.error.some(error => error.code === 'BACKPRESSURE'));
    await sleep(50); assert.equal(fixture.upgrades.length, 1); assert.equal(client.realtime.getStats().activeSockets, 0);
  });
});

test('one heartbeat reconnects a silent multiplex socket without multiplying physical attempts', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { autoPong: false });
  const client = fixture.client({ realtime: { pingIntervalMs: 30, pongTimeoutMs: 20 } });
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second);
  await Promise.all([a.ready, b.ready]); await until(() => fixture.upgrades.length >= 2, 'shared heartbeat reconnect');
  await client.close();
  assert.equal(fixture.upgrades.length, 2); assert.equal(client.realtime.getStats().reconnectCount, 1);
});

test('shared page pressure retries only the affected logical channel and leaves the physical stream open', { timeout: 6000 }, async t => {
  const fixture = await peer(t), client = fixture.client({ realtime: { maxBufferedEvents: 1 } });
  const a = client.realtime.subscribe(first), b = client.realtime.subscribe(second), entered = deferred(), gate = deferred();
  let attempts = 0; const seen = observe(b), store = new MemoryCheckpointStore(); t.after(() => store.close());
  a.consume(async () => { attempts++; entered.resolve(); await gate.promise; }, { consumerId: 'slow', checkpointStore: store });
  await Promise.all([a.ready, b.ready]); fixture.append(first, 'held'); fixture.hint(first); await entered.promise;
  fixture.append(second, 'waiting'); fixture.hint(second);
  await until(() => seen.error.some(error => error.code === 'BACKPRESSURE' && error.retryable), 'shared replay budget pressure');
  assert.equal(fixture.upgrades.length, 1); assert.equal(client.realtime.getStats().reconnectCount, 0);
  gate.resolve(); await until(() => seen.event.length === 1, 'logical retry after release');
  assert.equal(attempts, 1); assert.equal(fixture.upgrades.length, 1);
  assert.equal(client.realtime.getStats().queuedEvents, 0);
});
