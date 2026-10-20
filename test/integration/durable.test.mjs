import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdir, mkdtemp } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat, FileCheckpointStore, FileOutboxStore, MemoryCheckpointStore, RecoveryGapError,
  NetworkError, UnsupportedFeatureError } from '../../dist/index.js';

const org = 'durable-org';
const actor = 'durable-actor';
const channel = org + ':team:general';
const epoch = 'durable-local-epoch';
const timestamp = 1_792_370_346;
const encodeCursor = (sequence, streamEpoch = epoch, channelId = channel) => Buffer.from(JSON.stringify({
  schema: 1, org_uuid: org, channel_id: channelId, epoch: streamEpoch, sequence: String(sequence),
})).toString('base64url');
const position = cursor => BigInt(JSON.parse(Buffer.from(cursor, 'base64url').toString()).sequence);
const state = (id, extra = {}) => ({ schema: 1, org_uuid: org, channel_id: channel, message_id: id, sender_id: actor,
  created_at: timestamp, thread_root_id: null, priority: 'standard', original_body: 'Message ' + id, body: 'Message ' + id,
  revision: 1, updated_at: timestamp, deleted: false, reaction_revision: 0, reactions: {}, attachments: [], ...extra });
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
async function until(predicate, label, timeout = 4000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await predicate()) return; await sleep(5); }
  assert.fail('Timed out: ' + label);
}
function capture(subscription) {
  const events = { event: [], error: [], handlerError: [], checkpoint: [], recoveryComplete: [] };
  for (const name of Object.keys(events)) subscription.on(name, value => { events[name].push(value); });
  return events;
}
async function fileCheckpoint(t) {
  const root = fileURLToPath(new URL('../../target/durable/', import.meta.url));
  await mkdir(root, { recursive: true });
  const dir = await mkdtemp(join(root, 'durable-peer-'));
  const path = join(dir, 'checkpoint.json');
  const store = await FileCheckpointStore.open(path);
  t.after(() => store.close());
  return { path, store };
}

async function peer(t, options = {}) {
  const events = [];
  const streams = new Map([[channel, events]]);
  const stream = channelId => { if (!streams.has(channelId)) streams.set(channelId, []); return streams.get(channelId); };
  const sockets = [];
  const socketChannels = new WeakMap();
  const requests = [];
  const upgrades = [];
  const clients = [];
  const fixtureErrors = [];
  const sendAttempts = [];
  const sent = new Map();
  let floor = options.floor ?? 0;
  const wss = new WebSocketServer({ noServer: true, autoPong: options.autoPong ?? true });
  const respond = (response, status, value) => { response.writeHead(status, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(value)); };
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      if (url.pathname === '/me') return respond(response, 200, { user_uuid: actor, org, id: 1,
        email: 'durable@example.invalid', roles: ['member'], scopes: [], expires_at: null, app_uuid: null });
      if (url.pathname === '/api/sdk/capabilities') return respond(response, 200, { sdk_protocol_version: 1,
        authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
        messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
          durable_replay: options.durableSupported !== false, event_protocol_versions: options.durableSupported === false ? [] : [1], websocket_scope: 'channel' } });
      if (url.pathname === '/api/messages/send') {
        let raw = '';
        for await (const chunk of request) raw += chunk;
        const body = JSON.parse(raw);
        const key = request.headers['x-idempotency-key'];
        sendAttempts.push({ key, body });
        const previous = sent.get(key);
        if (previous && JSON.stringify(previous.body) !== JSON.stringify(body)) return respond(response, 409, {});
        const receipt = previous?.receipt ?? { status: 'ACK', message_id: body.id, created_at: timestamp,
          attachment_count: 0, attachment_manifest_blake3: '0'.repeat(64) };
        sent.set(key, { body, receipt });
        if (options.failFirstSendAfterCommit && sendAttempts.length === 1) { request.socket.destroy(); return; }
        return respond(response, 200, receipt);
      }
      if (url.pathname === '/api/events/sync') {
        const channelId = url.searchParams.get('channel_id');
        const channelEvents = stream(channelId);
        const after = url.searchParams.get('after');
        const through = url.searchParams.get('through') ?? encodeCursor(channelEvents.length, epoch, channelId);
        requests.push({ after, through, channel: channelId });
        assert.ok(channelId.startsWith(org + ':'));
        const channelFloor = channelId === channel ? floor : 0;
        if (position(after) < BigInt(channelFloor)) return respond(response, 410, { code: 'EVENT_GAP', org_uuid: org, channel_id: channelId,
          epoch, earliest_cursor: encodeCursor(channelFloor, epoch, channelId), earliest_sequence: String(channelFloor) });
        const rows = channelEvents.filter(event => BigInt(event.sequence) > position(after) && BigInt(event.sequence) <= position(through))
          .slice(0, Math.min(options.pageSize ?? 200, Number(url.searchParams.get('limit') ?? 200)));
        const last = rows.at(-1)?.cursor ?? after;
        const page = { schema: 1, org_uuid: org, channel_id: channelId, epoch, events: rows,
          after_cursor: after, through_cursor: through, through_sequence: position(through).toString(),
          next_cursor: position(last) === position(through) ? null : last };
        const custom = await options.sync?.({ page, requests, events: channelEvents, sockets, channelId });
        return respond(response, 200, custom ?? page);
      }
      return respond(response, 404, {});
    } catch (error) { fixtureErrors.push(error); if (!response.headersSent) respond(response, 500, {}); else response.end(); }
  });
  server.on('upgrade', (request, socket, head) => {
    upgrades.push({ headers: request.headers, url: new URL(request.url, 'http://127.0.0.1') });
    const rejected = options.upgrade?.({ request, upgrades });
    if (rejected) {
      socket.end('HTTP/1.1 ' + rejected + ' Rejected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');
      return;
    }
    wss.handleUpgrade(request, socket, head, connected => wss.emit('connection', connected, request));
  });
  wss.on('connection', (socket, request) => {
    const channelId = new URL(request.url, 'http://127.0.0.1').searchParams.get('channel_id');
    const channelEvents = stream(channelId);
    sockets.push(socket);
    socketChannels.set(socket, channelId);
    socket.on('error', () => {});
    socket.send(JSON.stringify({ type: 'ready', schema: 1, org_uuid: org, channel_id: channelId,
      epoch, cursor: encodeCursor(channelEvents.length, epoch, channelId), sequence: String(channelEvents.length),
      earliest_cursor: encodeCursor(channelId === channel ? options.readyFloor ?? floor : 0, epoch, channelId) }));
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const origin = 'http://127.0.0.1:' + server.address().port;
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const socket of sockets) socket.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(fixtureErrors, [], 'Controlled durable peer must not hide fixture errors');
  });
  return {
    origin, events, requests, upgrades, sockets, sendAttempts, sent,
    scope(consumerId = 'consumer', channelId = channel) { return { serverOrigin: origin, orgId: org, actorId: actor, consumerId, channelId }; },
    client(overrides = {}) {
      const client = new HumChat({ orgId: org, baseUrl: origin, tokenProvider: async () => ({ token: 'synthetic-durable-token' }),
        timeout: 1000, retry: { maxRetries: 0, totalTimeoutMs: 2000 }, recovery: { mode: 'durable' },
        realtime: { handlerTimeoutMs: 1000, reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20, pingIntervalMs: 1000, pongTimeoutMs: 500 }, ...overrides });
      clients.push(client); return client;
    },
    append(id, extra = {}) { return this.appendTo(channel, id, extra); },
    appendTo(channelId, id, extra = {}) {
      const channelEvents = stream(channelId);
      const sequence = String(channelEvents.length + 1);
      const event = { type: 'event', schema: 1, org_uuid: org, channel_id: channelId, epoch, sequence,
        event_id: epoch + ':' + sequence, cursor: encodeCursor(sequence, epoch, channelId), actor_id: actor,
        kind: 'message', state: state(id, { channel_id: channelId }), ...extra };
      channelEvents.push(event); return event;
    },
    publish(event) { for (const socket of sockets) if (socket.readyState === WebSocket.OPEN &&
      (!event.channel_id || socketChannels.get(socket) === event.channel_id)) socket.send(JSON.stringify(event)); },
    prune(sequence) { floor = sequence; },
  };
}

test('durable consumption checkpoints only after an awaited handler, then resumes across a file-store restart', { timeout: 10000 }, async t => {
  const fixture = await peer(t, { pageSize: 1 });
  const first = fixture.append('root');
  const second = fixture.append('thread-reply', { state: state('thread-reply', { thread_root_id: 'root' }) });
  const disk = await fileCheckpoint(t);
  const gate = deferred();
  const started = deferred();
  const client = fixture.client();
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  const observed = capture(subscription);
  const handled = [];
  subscription.consume(async event => {
    handled.push(event.eventId);
    if (event.eventId === first.event_id) { started.resolve(); await gate.promise; }
  }, { consumerId: 'consumer', checkpointStore: disk.store });
  await started.promise;
  assert.equal(await disk.store.load(fixture.scope()), encodeCursor(0));
  gate.resolve();
  await subscription.ready;
  await until(async () => await disk.store.load(fixture.scope()) === second.cursor, 'second durable acknowledgement');
  assert.deepEqual(handled, [first.event_id, second.event_id]);
  assert.ok(fixture.requests.length >= 2);
  assert.ok(observed.recoveryComplete.some(event => event.complete === true));
  assert.equal(fixture.upgrades[0].headers['x-humchat-event-protocol'], 'v1');
  assert.equal(fixture.upgrades[0].headers.authorization, 'Bearer synthetic-durable-token');
  assert.equal(fixture.upgrades[0].url.searchParams.has('token'), false);
  await client.close(); await disk.store.close();
  const reopened = await FileCheckpointStore.open(disk.path); t.after(() => reopened.close());
  const third = fixture.append('offline-message');
  const next = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  const replayed = [];
  next.consume(event => { replayed.push(event.eventId); }, { consumerId: 'consumer', checkpointStore: reopened });
  await next.ready;
  await until(async () => await reopened.load(fixture.scope()) === third.cursor, 'restart acknowledgement');
  assert.deepEqual(replayed, [third.event_id]);
});

test('failed handlers leave the checkpoint unadvanced and retry the event after restart', { timeout: 8000 }, async t => {
  const fixture = await peer(t);
  const event = fixture.append('failure');
  const checkpoints = new MemoryCheckpointStore(); t.after(() => checkpoints.close());
  const client = fixture.client();
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  const observed = capture(subscription);
  subscription.consume(() => { throw new Error('Synthetic application failure'); }, { consumerId: 'consumer', checkpointStore: checkpoints });
  await assert.rejects(subscription.ready);
  await until(() => subscription.status === 'blocked', 'failed consumer blocked');
  assert.equal(await checkpoints.load(fixture.scope()), encodeCursor(0));
  assert.ok(observed.handlerError.length >= 1);
  await client.close();
  const received = [];
  const restarted = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  restarted.consume(value => { received.push(value.eventId); }, { consumerId: 'consumer', checkpointStore: checkpoints });
  await restarted.ready;
  assert.deepEqual(received, [event.event_id]);
  assert.equal(await checkpoints.load(fixture.scope()), event.cursor);
});

test('a checkpoint CAS conflict blocks progress instead of acknowledging competing work', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  fixture.append('cas-conflict');
  let handled = 0;
  let attempts = 0;
  const store = { async load() { return encodeCursor(0); }, async compareAndSet() { attempts++; return false; }, async close() {} };
  const subscription = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  subscription.consume(() => { handled++; }, { consumerId: 'consumer', checkpointStore: store });
  await assert.rejects(subscription.ready);
  assert.equal(subscription.status, 'blocked');
  assert.equal(handled, 1);
  assert.equal(attempts, 1);
});

test('retention gaps expose recovery failure and never overwrite the stored checkpoint', { timeout: 6000 }, async t => {
  // Retention advances after the advertised ready floor, so the HTTP 410 path is exercised.
  const fixture = await peer(t, { readyFloor: 0 });
  fixture.append('pruned'); fixture.append('retained'); fixture.prune(1);
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  await store.compareAndSet(fixture.scope(), null, encodeCursor(0));
  const subscription = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  const observed = capture(subscription);
  let handled = 0;
  subscription.consume(() => { handled++; }, { consumerId: 'consumer', checkpointStore: store });
  await assert.rejects(subscription.ready, error => error instanceof RecoveryGapError && error.status === 410);
  assert.equal(subscription.status, 'blocked');
  assert.equal(handled, 0);
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
  assert.equal(observed.event.length, 0);
});

test('captured replay watermark and overlapping live frames deliver contiguous events once', { timeout: 7000 }, async t => {
  let fixture;
  let injected = false;
  fixture = await peer(t, { sync({ page }) {
    if (!injected) {
      injected = true;
      const next = fixture.append('racing-live');
      fixture.publish(fixture.events[0]); // replay overlap must not duplicate delivery
      fixture.publish(next);
    }
    return page;
  } });
  const historical = fixture.append('historical');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const subscription = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  const handled = [];
  subscription.consume(event => { handled.push(event.eventId); }, { consumerId: 'consumer', checkpointStore: store });
  await subscription.ready;
  await until(() => handled.length === 2, 'overlapping live event acknowledged');
  assert.deepEqual(handled, [historical.event_id, epoch + ':2']);
  assert.equal(await store.load(fixture.scope()), encodeCursor(2));
});

test('a skipped sequence in a server page fails closed before calling the consumer', { timeout: 6000 }, async t => {
  const fixture = await peer(t, { sync({ page }) { return { ...page, events: page.events.slice(1) }; } });
  fixture.append('one'); fixture.append('two');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  let handled = 0;
  const subscription = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  subscription.consume(() => { handled++; }, { consumerId: 'consumer', checkpointStore: store });
  await assert.rejects(subscription.ready, error => error.code === 'PROTOCOL');
  assert.equal(handled, 0);
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
});

test('stored checkpoints bound to another channel cannot become replay positions', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  fixture.append('one');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  await store.compareAndSet(fixture.scope(), null, encodeCursor(0, epoch, org + ':team:other'));
  const subscription = fixture.client().realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  subscription.consume(() => {}, { consumerId: 'consumer', checkpointStore: store });
  await assert.rejects(subscription.ready, error => error.code === 'PROTOCOL');
  assert.equal(fixture.requests.length, 0);
});

test('reconnect reloads the acknowledged position and catches up only unseen committed events', { timeout: 7000 }, async t => {
  const fixture = await peer(t);
  const first = fixture.append('initial');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const client = fixture.client();
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  const handled = [];
  subscription.consume(event => { handled.push(event.eventId); }, { consumerId: 'consumer', checkpointStore: store });
  await subscription.ready;
  const second = fixture.append('created-while-disconnected');
  fixture.sockets.at(-1).terminate();
  await until(async () => await store.load(fixture.scope()) === second.cursor, 'reconnect catch-up');
  assert.deepEqual(handled, [first.event_id, second.event_id]);
  assert.ok(client.realtime.getStats().reconnectCount >= 1);
  assert.ok(fixture.requests.some(request => request.after === first.cursor));
});

test('a replaced stream epoch requires explicit recovery without rewriting its old checkpoint', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  fixture.append('new-epoch');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const previous = encodeCursor(1, 'previous-stream-epoch');
  await store.compareAndSet(fixture.scope(), null, previous);
  const subscription = fixture.client().realtime.subscribe(channel);
  capture(subscription);
  subscription.consume(() => {}, { consumerId: 'consumer', checkpointStore: store });
  await assert.rejects(subscription.ready, RecoveryGapError);
  assert.equal(await store.load(fixture.scope()), previous);
  assert.equal(fixture.requests.length, 0);
});

test('an older server cannot silently downgrade the default durable subscription', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { durableSupported: false });
  const subscription = fixture.client().realtime.subscribe(channel);
  capture(subscription);
  await assert.rejects(subscription.ready, UnsupportedFeatureError);
  assert.equal(subscription.status, 'blocked');
  assert.equal(fixture.upgrades.length, 0);
});

test('persisted outbox retries an ambiguous committed send with the same message and idempotency IDs', { timeout: 8000 }, async t => {
  const fixture = await peer(t, { failFirstSendAfterCommit: true });
  const artifactRoot = fileURLToPath(new URL('../../target/durable/', import.meta.url));
  await mkdir(artifactRoot, { recursive: true });
  const path = join(await mkdtemp(join(artifactRoot, 'outbox-peer-')), 'outbox.json');
  const firstStore = await FileOutboxStore.open(path); t.after(() => firstStore.close());
  const firstClient = fixture.client();
  await assert.rejects(firstClient.outbox.send({ consumerId: 'consumer', key: 'reply-to-event-1', store: firstStore,
    message: { channelId: channel, content: 'Persist before sending' } }), NetworkError);
  const pending = await firstStore.listPending(fixture.scope());
  assert.equal(pending.length, 1);
  await firstClient.close(); await firstStore.close();
  const restarted = await FileOutboxStore.open(path); t.after(() => restarted.close());
  const secondClient = fixture.client();
  const receipts = await secondClient.outbox.flush({ consumerId: 'consumer', channelId: channel, store: restarted });
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0].id, pending[0].intent.id);
  assert.equal(receipts[0].idempotencyKey, pending[0].idempotencyKey);
  assert.equal(fixture.sendAttempts.length, 2);
  assert.deepEqual(fixture.sendAttempts[1], fixture.sendAttempts[0]);
  assert.equal(fixture.sent.size, 1);
  assert.deepEqual(await restarted.listPending(fixture.scope()), []);
});

test('resync requested inside an active consumer survives until that delivery finishes', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const subscription = fixture.client().realtime.subscribe(channel);
  capture(subscription);
  const handled = [];
  subscription.consume(async event => {
    handled.push(event.eventId);
    if (event.sequence === '1') {
      await subscription.resync();
      fixture.append('committed-without-live-hint');
    }
  }, { consumerId: 'consumer', checkpointStore: store });
  await subscription.ready;
  fixture.publish(fixture.append('resync-trigger'));
  // This deadline is shorter than the worker's five-second fallback poll.
  await until(async () => await store.load(fixture.scope()) === encodeCursor(2), 'resync queued during delivery', 3500);
  assert.deepEqual(handled, [epoch + ':1', epoch + ':2']);
});

test('session revocation blocks the whole client without refreshing or reconnecting', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  let providerCalls = 0;
  const client = fixture.client({ tokenProvider: async () => { providerCalls++; return { token: 'synthetic-revoked-session' }; } });
  const subscription = client.realtime.subscribe(channel);
  capture(subscription);
  await subscription.ready;
  fixture.publish({ type: 'session_revoked', reason: 'fixture session ended' });
  await until(() => subscription.status === 'blocked', 'revoked channel blocked');
  await assert.rejects(client.connect(), error => error.code === 'SESSION_REVOKED');
  assert.throws(() => client.realtime.subscribe(org + ':team:other'), error => error.code === 'SESSION_REVOKED');
  await sleep(30);
  assert.equal(providerCalls, 1);
  assert.equal(fixture.upgrades.length, 1);
  assert.equal(client.realtime.getStats().reconnectCount, 0);
});

test('durable heartbeat reconnects a socket that stops answering ping frames', { timeout: 6000 }, async t => {
  const fixture = await peer(t, { autoPong: false });
  const client = fixture.client({ realtime: { pingIntervalMs: 15, pongTimeoutMs: 10,
    reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 10 } });
  const subscription = client.realtime.subscribe(channel);
  capture(subscription);
  await subscription.ready;
  await until(() => fixture.upgrades.length >= 2, 'durable heartbeat reconnect');
  assert.ok(client.realtime.getStats().reconnectCount >= 1);
  await client.close();
});

test('expiring credentials rotate the durable handshake through the shared token provider', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  let providerCalls = 0;
  const client = fixture.client({ tokenProvider: async () => {
    providerCalls++;
    return { token: 'synthetic-rotated-' + providerCalls, expiresAt: Math.floor(Date.now() / 1000) + (providerCalls === 1 ? 2 : 300) };
  } });
  const subscription = client.realtime.subscribe(channel);
  capture(subscription);
  await subscription.ready;
  await until(() => fixture.upgrades.some(upgrade => upgrade.headers.authorization === 'Bearer synthetic-rotated-2'), 'rotated durable credential', 4000);
  assert.equal(providerCalls, 2);
  assert.ok(client.realtime.getStats().reconnectCount >= 1);
});

test('typed event-page reads reject forged boundary cursors and cross-tenant event scope', async t => {
  const cases = [
    ['forged boundary', page => ({ ...page, through_cursor: encodeCursor(1, 'forged-epoch') })],
    ['cross-tenant event', page => ({ ...page, events: page.events.map(event => ({ ...event, org_uuid: 'another-org' })) })],
    ['event identity mismatch', page => ({ ...page, events: page.events.map(event => ({ ...event, event_id: epoch + ':2' })) })],
  ];
  for (const [name, transform] of cases) await t.test(name, async child => {
    const fixture = await peer(child, { sync({ page }) { return transform(page); } });
    fixture.append('one');
    await assert.rejects(fixture.client().events.sync({ channelId: channel, after: encodeCursor(0), through: encodeCursor(1) }),
      error => error.code === 'PROTOCOL');
  });
});

test('cold event sync shares one total deadline across authentication and the response', { timeout: 6000 }, async t => {
  const fixture = await peer(t, { async sync({ page }) { await sleep(250); return page; } });
  fixture.append('deadline-event');
  const checkpoints = new MemoryCheckpointStore(); t.after(() => checkpoints.close());
  await checkpoints.compareAndSet(fixture.scope(), null, encodeCursor(0));
  let delivered = 0;
  const client = fixture.client({ timeout: 1000, retry: { maxRetries: 0, totalTimeoutMs: 400 },
    tokenProvider: async () => { await sleep(250); return { token: 'synthetic-slow-authentication' }; } });
  const startedAt = performance.now();
  await assert.rejects(client.events.sync({ channelId: channel, after: encodeCursor(0), through: encodeCursor(1) }).then(async page => {
    delivered += page.events.length;
    await checkpoints.compareAndSet(fixture.scope(), encodeCursor(0), page.events.at(-1).cursor);
  }), error => error.code === 'TIMEOUT');
  const elapsed = performance.now() - startedAt;
  assert.ok(elapsed >= 350 && elapsed < 650, 'Authentication and response must consume one 400ms operation budget');
  assert.equal(delivered, 0);
  assert.equal(await checkpoints.load(fixture.scope()), encodeCursor(0));
  assert.equal(fixture.requests.length, 1, 'The deadline must expire during the response, after authentication succeeded');
});

test('credential rotation remains active during a slow checkpoint load without overlapping store operations', { timeout: 8000 }, async t => {
  const fixture = await peer(t);
  const event = fixture.append('after-slow-load');
  const memory = new MemoryCheckpointStore(); t.after(() => memory.close());
  const gate = deferred(); t.after(() => gate.resolve());
  let providerCalls = 0;
  let loadCalls = 0;
  let activeOperations = 0;
  let peakOperations = 0;
  const operation = async callback => {
    activeOperations++;
    peakOperations = Math.max(peakOperations, activeOperations);
    try { return await callback(); } finally { activeOperations--; }
  };
  const store = {
    load(scope) { return operation(async () => { if (++loadCalls === 1) await gate.promise; return memory.load(scope); }); },
    compareAndSet(scope, previous, next) { return operation(() => memory.compareAndSet(scope, previous, next)); },
    async close() {},
  };
  const client = fixture.client({ tokenProvider: async () => {
    providerCalls++;
    return { token: 'synthetic-load-rotation-' + providerCalls, expiresAt: Math.floor(Date.now() / 1000) + (providerCalls === 1 ? 2 : 300) };
  } });
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  const handled = [];
  subscription.consume(value => { handled.push(value.eventId); }, { consumerId: 'consumer', checkpointStore: store });
  try {
    await until(() => loadCalls === 1, 'first checkpoint load started');
    await until(() => providerCalls === 2, 'credentials rotated while storage is pending');
    assert.equal(loadCalls, 1, 'A new connection generation must wait for the existing store operation');
    assert.equal(peakOperations, 1);
  } finally { gate.resolve(); }
  await subscription.ready;
  assert.equal(loadCalls, 2);
  assert.equal(peakOperations, 1);
  assert.deepEqual(handled, [event.event_id]);
  assert.equal(await memory.load(fixture.scope()), event.cursor);
});

test('reconnect waits for an interrupted consumer to settle before replaying the unacknowledged event', { timeout: 8000 }, async t => {
  const fixture = await peer(t);
  const event = fixture.append('interrupted-handler');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const gate = deferred(); t.after(() => gate.resolve());
  const entered = deferred();
  const client = fixture.client();
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  let calls = 0, active = 0, peak = 0;
  let interruptedSignal;
  subscription.consume(async (_event, context) => {
    calls++; active++; peak = Math.max(peak, active);
    try {
      if (calls === 1) { interruptedSignal = context.signal; entered.resolve(); await gate.promise; }
    } finally { active--; }
  }, { consumerId: 'consumer', checkpointStore: store });
  await entered.promise;
  fixture.sockets.at(-1).terminate();
  await until(() => interruptedSignal.aborted, 'interrupted handler cancellation');
  await sleep(60);
  assert.equal(calls, 1, 'A cancelled application promise can still run; reconnect must not overlap it');
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
  gate.resolve();
  await subscription.ready;
  assert.equal(calls, 2, 'The cancelled attempt is replayed after settling');
  assert.equal(peak, 1);
  assert.equal(await store.load(fixture.scope()), event.cursor);
});

test('reconnect waits for an in-flight checkpoint write and reloads its committed result', { timeout: 8000 }, async t => {
  const fixture = await peer(t);
  const event = fixture.append('checkpoint-race');
  const memory = new MemoryCheckpointStore(); t.after(() => memory.close());
  await memory.compareAndSet(fixture.scope(), null, encodeCursor(0));
  const gate = deferred(); t.after(() => gate.resolve());
  const writing = deferred();
  let loads = 0, writes = 0, active = 0, peak = 0;
  const operation = async callback => { active++; peak = Math.max(peak, active); try { return await callback(); } finally { active--; } };
  const store = {
    load(scope) { loads++; return operation(() => memory.load(scope)); },
    compareAndSet(scope, previous, next) { return operation(async () => {
      writes++; writing.resolve(); await gate.promise; return memory.compareAndSet(scope, previous, next);
    }); },
    async close() {},
  };
  const client = fixture.client();
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  const handled = [];
  subscription.consume(value => { handled.push(value.eventId); }, { consumerId: 'consumer', checkpointStore: store });
  await writing.promise;
  fixture.sockets.at(-1).terminate();
  await until(() => client.realtime.getStats().reconnectCount > 0, 'checkpoint-write reconnect');
  await sleep(30);
  assert.equal(loads, 1);
  assert.equal(await memory.load(fixture.scope()), encodeCursor(0));
  gate.resolve();
  await subscription.ready;
  assert.equal(peak, 1);
  assert.equal(loads, 2);
  assert.equal(writes, 1);
  assert.deepEqual(handled, [event.event_id]);
  assert.equal(await memory.load(fixture.scope()), event.cursor);
});

test('an interrupted consumer that ignores cancellation blocks at its original deadline without overlapping replay', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  fixture.append('non-cooperative-handler');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const gate = deferred(); t.after(() => gate.resolve());
  const entered = deferred();
  const client = fixture.client({ realtime: { handlerTimeoutMs: 120, reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 10 } });
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  const observed = capture(subscription);
  let calls = 0;
  subscription.consume(async () => { calls++; entered.resolve(); await gate.promise; }, { consumerId: 'consumer', checkpointStore: store });
  await entered.promise;
  fixture.sockets.at(-1).terminate();
  await assert.rejects(subscription.ready, error => error.code === 'PROTOCOL');
  assert.equal(subscription.status, 'blocked');
  assert.equal(calls, 1);
  assert.ok(observed.handlerError.some(event => event.event === 'consume'));
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
  gate.resolve();
  await sleep(25);
  assert.equal(calls, 1);
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
  assert.equal(client.realtime.getStats().queuedEvents, 0);
});

test('closing during a consumer aborts delivery without acknowledging its later completion', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  fixture.append('close-during-handler');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const gate = deferred(); t.after(() => gate.resolve());
  const started = deferred();
  let signal;
  const client = fixture.client();
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(subscription);
  subscription.consume(async (_event, context) => { signal = context.signal; started.resolve(); await gate.promise; },
    { consumerId: 'consumer', checkpointStore: store });
  await started.promise;
  await client.close();
  assert.equal(signal.aborted, true);
  await assert.rejects(subscription.ready, error => error.code === 'ABORT');
  gate.resolve();
  await sleep(25);
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
  assert.equal(fixture.upgrades.length, 1);
});

test('durable replay crosses 500 events without applying best-effort page, thread or time limits', { timeout: 10000 }, async t => {
  const fixture = await peer(t);
  for (let index = 0; index < 701; index++) fixture.append('large-replay-' + index,
    { state: state('large-replay-' + index, { thread_root_id: index ? 'large-replay-0' : null }) });
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const client = fixture.client({ recovery: { mode: 'durable', maxPages: 1, maxThreads: 0, maxStateRefresh: 0, timeoutMs: 1 },
    realtime: { maxBufferedEvents: 50, maxBufferedBytes: 128 * 1024 } });
  const subscription = client.realtime.subscribe(channel, { start: 'earliest' });
  const observed = capture(subscription);
  const handled = [];
  subscription.consume(event => {
    handled.push(event.sequence);
    assert.ok(client.realtime.getStats().queuedEvents <= 50);
  }, { consumerId: 'consumer', checkpointStore: store });
  await subscription.ready;
  assert.deepEqual(handled, Array.from({ length: 701 }, (_, index) => String(index + 1)));
  assert.equal(await store.load(fixture.scope()), encodeCursor(701));
  assert.equal(fixture.requests.length, 15);
  assert.ok(fixture.requests.every(request => request.through === encodeCursor(701)));
  assert.ok(observed.recoveryComplete.some(event => event.complete && event.states === 701 && event.pages === 15));
  assert.equal(client.realtime.getStats().queuedEvents, 0);
  assert.equal(client.realtime.getStats().queuedBytes, 0);
});

test('shared channel delivery pressure retries from its own checkpoint and releases the global budget', { timeout: 8000 }, async t => {
  const fixture = await peer(t);
  const otherChannel = org + ':team:other';
  const first = fixture.append('held-page');
  const second = fixture.appendTo(otherChannel, 'waiting-page');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const gate = deferred(); t.after(() => gate.resolve());
  const entered = deferred();
  let providerCalls = 0;
  const client = fixture.client({ tokenProvider: async () => { providerCalls++; return { token: 'synthetic-shared-session' }; },
    realtime: { connectConcurrency: 2, maxBufferedEvents: 1, maxBufferedBytes: 16 * 1024,
      handlerTimeoutMs: 2000, reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20 } });
  const firstSubscription = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(firstSubscription);
  firstSubscription.consume(async () => { entered.resolve(); await gate.promise; }, { consumerId: 'consumer', checkpointStore: store });
  await entered.promise;
  const secondSubscription = client.realtime.subscribe(otherChannel, { start: 'earliest' });
  const observed = capture(secondSubscription);
  const handled = [];
  secondSubscription.consume(event => { handled.push(event.eventId); }, { consumerId: 'consumer', checkpointStore: store });
  await until(() => observed.error.some(error => error.code === 'BACKPRESSURE' && error.retryable), 'shared queue backoff');
  assert.deepEqual(handled, []);
  assert.equal(await store.load(fixture.scope('consumer', otherChannel)), encodeCursor(0, epoch, otherChannel));
  assert.equal(client.realtime.getStats().queuedEvents, 1);
  gate.resolve();
  await Promise.all([firstSubscription.ready, secondSubscription.ready]);
  assert.equal(await store.load(fixture.scope()), first.cursor);
  assert.equal(await store.load(fixture.scope('consumer', otherChannel)), second.cursor);
  assert.deepEqual(handled, [second.event_id]);
  assert.equal(providerCalls, 1);
  assert.equal(client.realtime.getStats().queuedEvents, 0);
  assert.equal(client.realtime.getStats().queuedBytes, 0);
  assert.ok(client.realtime.getStats().reconnectCount >= 1);
});

test('a slow durable consumer releases handshake admission so another channel can become live', { timeout: 8000 }, async t => {
  const fixture = await peer(t);
  const otherChannel = org + ':team:independent';
  const first = fixture.append('slow-consumer');
  const second = fixture.appendTo(otherChannel, 'independent-consumer');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const gate = deferred(); t.after(() => gate.resolve());
  const entered = deferred();
  const client = fixture.client({ realtime: { connectConcurrency: 1, handlerTimeoutMs: 3000 } });
  const slow = client.realtime.subscribe(channel, { start: 'earliest' });
  capture(slow);
  slow.consume(async () => { entered.resolve(); await gate.promise; }, { consumerId: 'consumer', checkpointStore: store });
  await entered.promise;
  const independent = client.realtime.subscribe(otherChannel, { start: 'earliest' });
  capture(independent);
  const handled = [];
  independent.consume(event => { handled.push(event.eventId); }, { consumerId: 'consumer', checkpointStore: store });
  await until(() => independent.status === 'live', 'another channel admitted before the slow handler finishes', 1500);
  assert.deepEqual(handled, [second.event_id]);
  assert.equal(await store.load(fixture.scope('consumer', otherChannel)), second.cursor);
  assert.equal(await store.load(fixture.scope()), encodeCursor(0));
  gate.resolve();
  await Promise.all([slow.ready, independent.ready]);
  assert.equal(await store.load(fixture.scope()), first.cursor);
});

test('a reconnect denied after access revocation preserves HTTP 403 despite trailing socket errors', { timeout: 6000 }, async t => {
  let denied = false;
  const fixture = await peer(t, { upgrade() { return denied ? 403 : undefined; } });
  let providerCalls = 0;
  const client = fixture.client({ tokenProvider: async () => { providerCalls++; return { token: 'synthetic-revoked-grant' }; } });
  const subscription = client.realtime.subscribe(channel);
  const observed = capture(subscription);
  await subscription.ready;
  denied = true;
  fixture.sockets.at(-1).terminate();
  await until(() => subscription.status === 'blocked', 'permanent permission failure stops reconnect', 1500);
  assert.ok(observed.error.some(error => error.code === 'PERMISSION' && error.status === 403));
  const attempts = fixture.upgrades.length;
  await sleep(50);
  assert.equal(fixture.upgrades.length, attempts);
  assert.equal(attempts, 2);
  assert.equal(providerCalls, 1);
  assert.equal(client.realtime.getStats().activeSockets, 0);
});
