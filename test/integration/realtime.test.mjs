import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat, HumChatError } from '../../dist/index.js';

// A loopback protocol peer, not an alternative authentication authority. Every
// credential is a fixed synthetic string; no private keys or user tokens exist.
const org = 'realtime-org';
const actor = 'realtime-actor';
const channel = `${org}:team:general`;
const secondChannel = `${org}:team:second`;
const thirdChannel = `${org}:team:third`;
const timestamp = 1_792_370_346;
const caps = { sdk_protocol_version: 1, authentication: { user_sessions: true, service_identities: false,
  api_key_exchange: false, app_binding_required: false, mock_authentication: true }, messaging: {
  send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: false,
  websocket_scope: 'channel' } };
const state = (id, extra = {}) => ({ schema: 1, org_uuid: org, channel_id: channel, message_id: id,
  sender_id: actor, created_at: timestamp, thread_root_id: null, priority: 'standard',
  original_body: `Message ${id}`, body: `Message ${id}`, revision: 1, updated_at: timestamp,
  deleted: false, reaction_revision: 0, reactions: {}, attachments: [], ...extra });
const message = (id, extra = {}) => ({ id, org_uuid: org, channel_id: channel, sender_id: actor,
  created_at: timestamp, thread_root_id: null, priority: 'standard', content: `Message ${id}`,
  is_long: false, ...extra });
function deferred() { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; }
async function until(predicate, label, timeout = 2500) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (predicate()) return; await sleep(5); }
  assert.fail('Timed out: ' + label);
}
function capture(subscription, names = ['message', 'messageUpdated', 'messageDeleted', 'reactionUpdated',
  'stateReconciled', 'recoveryComplete', 'recoveryIncomplete', 'resync', 'error', 'handlerError',
  'connection', 'sessionRevoked', 'authRequired', 'unknownEvent']) {
  const events = Object.fromEntries(names.map(name => [name, []]));
  for (const name of names) subscription.on(name, value => { events[name].push(value); });
  return events;
}
const code = expected => error => { assert.ok(error instanceof HumChatError); assert.equal(error.code, expected); return true; };

async function peer(t, options = {}) {
  const sockets = [];
  const upgrades = [];
  const requests = [];
  const frames = [];
  const fixtureErrors = [];
  const clients = [];
  const states = new Map();
  let pingCount = 0;
  const wss = new WebSocketServer({ noServer: true, autoPong: options.autoPong ?? true });
  const respond = (response, status, value) => { response.writeHead(status, { 'content-type': 'application/json' }); response.end(JSON.stringify(value)); };
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      requests.push({ url, headers: request.headers, at: Date.now() });
      if (url.pathname === '/me') return respond(response, 200, { user_uuid: actor, org, id: 1,
        email: 'realtime@example.test', roles: ['member'], scopes: [], expires_at: null, app_uuid: null });
      if (url.pathname === '/api/sdk/capabilities') return respond(response, 200, caps);
      if (url.pathname === '/api/messages/sync') {
        const scope = { channel: url.searchParams.get('channel_id'), thread: url.searchParams.get('thread_root_id'),
          cursor: url.searchParams.get('native_cursor'), request };
        if (options.history) {
          const custom = await options.history(scope);
          if (custom !== undefined) return respond(response, 200, custom);
        }
        let rows = [...states.values()].filter(row => row.channel_id === scope.channel && row.thread_root_id === scope.thread)
          .sort((a, b) => a.created_at - b.created_at || a.message_id.localeCompare(b.message_id));
        if (scope.cursor) {
          const cursor = JSON.parse(Buffer.from(scope.cursor, 'hex').toString('utf8'));
          rows = rows.filter(row => row.created_at < cursor.created_at ||
            (row.created_at === cursor.created_at && row.message_id < cursor.message_id));
        }
        const limit = Math.min(options.pageSize ?? 200, Number(url.searchParams.get('limit') ?? 200));
        const more = rows.length > limit;
        rows = rows.slice(-limit);
        const oldest = rows[0];
        const next = more ? Buffer.from(JSON.stringify({ schema: 2, org_uuid: org, channel_id: scope.channel,
          thread_root_id: scope.thread, created_at: oldest.created_at, message_id: oldest.message_id })).toString('hex') : null;
        return respond(response, 200, { schema: 2, org_uuid: org, channel_id: scope.channel,
          thread_root_id: scope.thread, messages: rows, next_cursor: next });
      }
      const statePath = /^\/api\/messages\/([^/]+)\/state$/.exec(url.pathname);
      if (statePath) {
        const value = states.get(decodeURIComponent(statePath[1]));
        return respond(response, value ? 200 : 404, value ?? {});
      }
      return respond(response, 404, {});
    } catch (error) { fixtureErrors.push(error); if (!response.headersSent) respond(response, 500, {}); else response.end(); }
  });
  server.on('upgrade', async (request, socket, head) => {
    const url = new URL(request.url, 'http://127.0.0.1');
    const upgrade = { url, headers: request.headers, at: Date.now(), number: upgrades.length + 1 };
    upgrades.push(upgrade);
    try {
      const rejected = await options.upgrade?.(upgrade);
      if (rejected) {
        socket.end(`HTTP/1.1 ${rejected} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
        return;
      }
      if (!socket.destroyed) wss.handleUpgrade(request, socket, head, ws => wss.emit('connection', ws, request));
    } catch (error) { fixtureErrors.push(error); socket.destroy(); }
  });
  wss.on('connection', (socket, request) => {
    const bound = new URL(request.url, 'http://127.0.0.1').searchParams.get('channel_id');
    sockets.push({ socket, channel: bound, headers: request.headers, at: Date.now() });
    socket.on('error', () => {});
    socket.on('message', data => { frames.push({ channel: bound, frame: JSON.parse(data.toString()) }); });
    socket.on('ping', () => { pingCount++; });
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const baseUrl = 'http://127.0.0.1:' + server.address().port;
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const { socket } of sockets) socket.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(fixtureErrors, [], 'Controlled peer must not hide fixture errors');
  });
  return {
    requests, upgrades, sockets, frames, states,
    get pingCount() { return pingCount; },
    client(overrides = {}) {
      const client = new HumChat({ orgId: org, baseUrl, tokenProvider: async () => ({ token: 'synthetic-realtime-token' }),
        timeout: 700, retry: { maxRetries: 0, totalTimeoutMs: 1500 }, recovery: { mode: 'bestEffort', maxPages: 3, maxThreads: 20 },
        realtime: { maxChannels: 5, connectConcurrency: 2, maxBufferedEvents: 100, maxBufferedBytes: 100_000,
          reconnectInitialDelayMs: 10, reconnectMaxDelayMs: 30, pingIntervalMs: 500, pongTimeoutMs: 200, handlerTimeoutMs: 500 },
        ...overrides });
      clients.push(client);
      return client;
    },
    latest(bound = channel) { return sockets.findLast(entry => entry.channel === bound && entry.socket.readyState === WebSocket.OPEN)?.socket; },
    send(frame, bound = channel) {
      const socket = this.latest(bound);
      assert.ok(socket, 'Expected an open controlled socket');
      socket.send(typeof frame === 'string' ? frame : JSON.stringify(frame));
    },
  };
}

test('headers authenticate one shared channel socket; closing one handle preserves the other', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const client = fixture.client();
  const first = client.realtime.subscribe(channel);
  const second = client.realtime.subscribe(channel);
  const seenFirst = capture(first);
  const seenSecond = capture(second);
  await Promise.all([first.ready, second.ready]);
  assert.equal(fixture.upgrades.length, 1);
  assert.equal(fixture.upgrades[0].url.searchParams.get('channel_id'), channel);
  assert.equal(fixture.upgrades[0].url.searchParams.has('token'), false);
  assert.equal(fixture.upgrades[0].headers.authorization, 'Bearer synthetic-realtime-token');
  assert.equal(client.realtime.getStats().activeSockets, 1);
  await first.close();
  fixture.send(message('after-first-close'));
  await until(() => seenSecond.message.length === 1, 'remaining handle receives live message');
  assert.equal(seenFirst.message.length, 0);
  assert.equal(seenSecond.message[0].message.id, 'after-first-close');
  await second.close();
  await until(() => !fixture.latest(), 'last handle releases physical socket');
  assert.equal(client.realtime.getStats().activeSockets, 0);
});

test('bootstrap history reconciles state without replaying old message commands', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  fixture.states.set('historic-root', state('historic-root'));
  fixture.states.set('historic-reply', state('historic-reply', { thread_root_id: 'historic-root' }));
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  assert.equal(seen.message.length, 0);
  assert.ok(seen.stateReconciled.some(event => event.state.id === 'historic-root'));
  assert.ok(fixture.requests.some(request => request.url.searchParams.get('thread_root_id') === 'historic-root'));
  assert.ok(seen.recoveryIncomplete.length > 0, 'Best-effort history cannot claim complete event recovery');
});

test('buffered duplicate live frames are delivered once after recovery finishes', { timeout: 5000 }, async t => {
  const gate = deferred();
  const historyStarted = deferred();
  let blocked = false;
  const fixture = await peer(t, { history: async scope => {
    if (!blocked && scope.thread === null) { blocked = true; historyStarted.resolve(); await gate.promise; }
  } });
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await historyStarted.promise;
  fixture.send(message('buffered-message'));
  fixture.send(message('buffered-message'));
  // The same committed creation can appear in both the HTTP snapshot and the
  // already-open socket. Its live notification must still be delivered once.
  fixture.states.set('buffered-message', state('buffered-message'));
  await sleep(30);
  assert.equal(seen.message.length, 0);
  gate.resolve();
  await sub.ready;
  await until(() => seen.message.length === 1, 'one buffered message delivered');
  fixture.send(message('buffered-message'));
  await sleep(40);
  assert.equal(seen.message.length, 1);
});

test('disconnect and resync reconcile roots and known threads without fake edit events', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  fixture.states.set('known-root', state('known-root'));
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  fixture.states.set('missed-reply', state('missed-reply', { thread_root_id: 'known-root', created_at: timestamp + 1 }));
  fixture.latest().terminate();
  await until(() => fixture.sockets.length >= 2 && sub.status === 'live', 'reconnection and recovery');
  await until(() => seen.stateReconciled.some(event => event.state.id === 'missed-reply'), 'known-thread catch-up');
  assert.ok(seen.message.some(event => event.message.id === 'missed-reply' && event.source === 'recovery' && event.replayed),
    'Previously empty, scanned threads must retain their baseline for new reply recovery');
  const recovered = fixture.requests.filter(request => request.url.searchParams.get('thread_root_id') === 'known-root').length;
  assert.ok(recovered >= 2);
  fixture.states.set('known-root', state('known-root', { body: 'Edited while offline', revision: 2, updated_at: timestamp + 2 }));
  fixture.send({ type: 'resync', channel_id: channel });
  await until(() => seen.stateReconciled.some(event => event.state.id === 'known-root' && event.state.revision === 2), 'resync state reconciliation');
  assert.equal(seen.messageUpdated.length, 0, 'Snapshots must not become fabricated edit notifications');
  assert.ok(seen.recoveryIncomplete.length >= 2);
});

test('403 handshake blocks without refreshing or repeatedly reconnecting', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { upgrade: () => 403 });
  let providerCalls = 0;
  const client = fixture.client({ tokenProvider: async () => { providerCalls++; return { token: 'synthetic-forbidden-token' }; } });
  const sub = client.realtime.subscribe(channel);
  capture(sub);
  await assert.rejects(sub.ready, code('PERMISSION'));
  await sleep(80);
  assert.equal(sub.status, 'blocked');
  assert.equal(providerCalls, 1);
  assert.equal(fixture.upgrades.length, 1);
});

test('401 handshake refreshes once and preserves header-only authentication', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { upgrade: request => request.number === 1 ? 401 : undefined });
  const reasons = [];
  const client = fixture.client({ tokenProvider: async context => { reasons.push(context.reason); return { token: `synthetic-token-${reasons.length}` }; } });
  const sub = client.realtime.subscribe(channel);
  capture(sub);
  await sub.ready;
  assert.deepEqual(reasons, ['initial', 'unauthorized']);
  assert.equal(fixture.upgrades.length, 2);
  assert.equal(fixture.upgrades[1].headers.authorization, 'Bearer synthetic-token-2');
  assert.equal(fixture.upgrades[1].url.searchParams.has('token'), false);
});

test('repeated 401 handshake is terminal for its connection attempt', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { upgrade: () => 401 });
  let providerCalls = 0;
  const sub = fixture.client({ tokenProvider: async () => ({ token: `synthetic-token-${++providerCalls}` }) }).realtime.subscribe(channel);
  capture(sub);
  await assert.rejects(sub.ready, code('AUTHENTICATION'));
  await sleep(80);
  assert.equal(fixture.upgrades.length, 2);
  assert.equal(providerCalls, 2);
  assert.equal(sub.status, 'blocked');
});

test('session revocation closes every channel and cannot be bypassed by automatic refresh', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  let providerCalls = 0;
  const client = fixture.client({ tokenProvider: async () => ({ token: `synthetic-token-${++providerCalls}` }) });
  const first = client.realtime.subscribe(channel);
  const second = client.realtime.subscribe(secondChannel);
  const firstSeen = capture(first);
  capture(second);
  await Promise.all([first.ready, second.ready]);
  fixture.send({ type: 'session_revoked', reason: 'session deprovisioned' });
  await until(() => first.status === 'blocked' && second.status === 'blocked', 'all channels blocked on revocation');
  assert.equal(firstSeen.sessionRevoked.length, 1);
  await sleep(100);
  assert.equal(providerCalls, 1);
  assert.equal(fixture.upgrades.length, 2);
  assert.equal(client.realtime.getStats().activeSockets, 0);
  await assert.rejects(client.connect(), code('SESSION_REVOKED'));
});

test('a silent peer triggers pong timeout and reconnect instead of staying falsely live', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { autoPong: false });
  const client = fixture.client({ realtime: { pingIntervalMs: 30, pongTimeoutMs: 20, reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 10 } });
  const sub = client.realtime.subscribe(channel);
  capture(sub);
  await sub.ready;
  await until(() => fixture.pingCount > 0 && fixture.upgrades.length >= 2, 'pong timeout caused reconnect');
});

test('active pongs keep a connection stable across several heartbeat intervals', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const sub = fixture.client({ realtime: { pingIntervalMs: 25, pongTimeoutMs: 100 } }).realtime.subscribe(channel);
  capture(sub);
  await sub.ready;
  await until(() => fixture.pingCount >= 3, 'three answered pings');
  assert.equal(fixture.upgrades.length, 1);
  assert.equal(sub.status, 'live');
});

test('wrong-scope frames and malformed embedded state block instead of becoming commands', { timeout: 6000 }, async t => {
  const fixture = await peer(t);
  const cases = [
    message('foreign-org', { org_uuid: 'another-org' }),
    message('foreign-channel', { channel_id: secondChannel }),
    { type: 'edit', org_uuid: org, channel_id: channel, message_id: 'malformed-state', state: null },
  ];
  for (const frame of cases) {
    const sub = fixture.client().realtime.subscribe(channel);
    const seen = capture(sub);
    await sub.ready;
    fixture.send(frame);
    await until(() => sub.status === 'blocked', 'malformed channel is blocked');
    assert.ok(seen.error.some(error => error.code === 'PROTOCOL'));
    assert.equal(seen.message.length, 0);
    assert.equal(seen.messageUpdated.length, 0);
    await sub.close();
  }
});

test('stale content cannot replace a newer edit or resurrect a tombstone', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const current = state('versioned-root', { revision: 3, body: 'Newest edited body', updated_at: timestamp + 3 });
  fixture.states.set('versioned-root', current);
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  fixture.send({ type: 'edit', org_uuid: org, channel_id: channel, message_id: 'versioned-root',
    state: { ...current, revision: 2, body: 'Older edit', updated_at: timestamp + 2 } });
  await sleep(40);
  assert.equal(seen.messageUpdated.length, 0);
  fixture.send({ type: 'reaction', org_uuid: org, channel_id: channel, message_id: 'versioned-root',
    state: state('versioned-root', { reaction_revision: 2, reactions: { '👍': [actor] } }) });
  await until(() => seen.reactionUpdated.length === 1, 'independent reaction revision merged');
  assert.equal(seen.reactionUpdated[0].state.revision, 3);
  assert.equal(seen.reactionUpdated[0].state.body, 'Newest edited body');
  fixture.send({ type: 'delete', org_uuid: org, channel_id: channel, message_id: 'versioned-root',
    state: { ...current, revision: 4, deleted: true, reaction_revision: 2, reactions: { '👍': [actor] }, updated_at: timestamp + 4 } });
  await until(() => seen.messageDeleted.length === 1, 'tombstone delivered');
  fixture.send(message('versioned-root'));
  await sleep(40);
  assert.equal(seen.message.length, 0);
  assert.equal(sub.status, 'live');
});

test('equal-revision conflicting state is a protocol failure', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const original = state('conflicting-root', { revision: 2, body: 'First edit', updated_at: timestamp + 1 });
  fixture.states.set('conflicting-root', original);
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  fixture.send({ type: 'edit', org_uuid: org, channel_id: channel, message_id: 'conflicting-root',
    state: { ...original, body: 'Conflicting edit at same revision' } });
  await until(() => sub.status === 'blocked', 'equal revision conflict detected');
  assert.ok(seen.error.some(error => error.code === 'PROTOCOL'));
  assert.equal(seen.messageUpdated.length, 0);
});

test('a hung handler cannot grow the event queue beyond its configured budget', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const client = fixture.client({ realtime: { maxBufferedEvents: 2, maxBufferedBytes: 50_000, handlerTimeoutMs: 1000 } });
  const sub = client.realtime.subscribe(channel);
  const seen = capture(sub);
  const started = deferred();
  sub.on('message', async () => { started.resolve(); await new Promise(() => {}); });
  await sub.ready;
  fixture.send(message('hung-handler-first'));
  await started.promise;
  for (let i = 0; i < 5; i++) fixture.send(message(`overload-${i}`));
  await until(() => sub.status === 'blocked', 'bounded queue blocks overloaded channel', 700);
  assert.ok(seen.error.some(error => error.code === 'BACKPRESSURE'));
  assert.ok(client.realtime.getStats().queuedEvents <= 2);
  assert.ok(client.realtime.getStats().queuedBytes <= 50_000);
});

test('handler failures are sanitized and do not automatically replay business callbacks', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  let calls = 0;
  sub.on('message', () => { calls++; throw new Error('SYNTHETIC_SECRET_FROM_HANDLER'); });
  await sub.ready;
  fixture.send(message('failing-handler'));
  await until(() => sub.status === 'blocked', 'failed handler blocks channel');
  assert.equal(seen.handlerError.length, 1);
  assert.ok(!JSON.stringify(seen.handlerError).includes('SYNTHETIC_SECRET_FROM_HANDLER'));
  await sleep(100);
  assert.equal(calls, 1);
  assert.equal(fixture.upgrades.length, 1);
});

test('channel admission and connection concurrency are bounded', { timeout: 5000 }, async t => {
  let active = 0;
  let maximum = 0;
  const fixture = await peer(t, { upgrade: async () => {
    maximum = Math.max(maximum, ++active);
    await sleep(40);
    active--;
  } });
  const client = fixture.client({ realtime: { maxChannels: 2, connectConcurrency: 1 } });
  const results = client.realtime.subscribeMany([channel, secondChannel, thirdChannel]);
  assert.equal(results.length, 3);
  assert.ok(results[0].subscription);
  assert.ok(results[1].subscription);
  assert.ok(results[2].error instanceof HumChatError);
  for (const result of results) if (result.subscription) capture(result.subscription);
  await Promise.all(results.flatMap(result => result.subscription ? [result.subscription.ready] : []));
  assert.equal(maximum, 1);
  assert.equal(fixture.upgrades.length, 2);
  assert.equal(client.realtime.getStats().activeSockets, 2);
  assert.equal(client.realtime.getStats().channels, 2);
});

test('multiple channels rotate an expiring token through one shared refresh', { timeout: 7000 }, async t => {
  const fixture = await peer(t);
  const reasons = [];
  const client = fixture.client({ tokenProvider: async context => {
    reasons.push(context.reason);
    return { token: `synthetic-rotation-${reasons.length}`,
      expiresAt: Math.floor(Date.now() / 1000) + (reasons.length === 1 ? 3 : 120) };
  } });
  const first = client.realtime.subscribe(channel);
  const second = client.realtime.subscribe(secondChannel);
  capture(first);
  capture(second);
  await Promise.all([first.ready, second.ready]);
  await until(() => fixture.sockets.filter(entry => entry.headers.authorization === 'Bearer synthetic-rotation-2').length === 2,
    'both channels use refreshed credential', 5000);
  await until(() => first.status === 'live' && second.status === 'live', 'rotated channels recover');
  assert.deepEqual(reasons, ['initial', 'expiring']);
  assert.equal(client.realtime.getStats().activeSockets, 2);
});

test('bounded history stops at its page budget and reports incomplete coverage', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { pageSize: 1 });
  fixture.states.set('older-root', state('older-root'));
  fixture.states.set('newer-root', state('newer-root', { created_at: timestamp + 1 }));
  const sub = fixture.client({ recovery: { mode: 'bestEffort', maxPages: 1, maxThreads: 0, maxStateRefresh: 0 } })
    .realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  assert.equal(fixture.requests.filter(request => request.url.pathname === '/api/messages/sync').length, 1);
  assert.equal(seen.message.length, 0);
  assert.equal(seen.stateReconciled.length, 1);
  assert.equal(seen.recoveryComplete.at(-1).complete, false);
  assert.ok(seen.recoveryIncomplete.some(event => event.limitations.some(reason => /page|limit|budget/i.test(reason))));
});

test('repeated resync requests coalesce into one running attempt and one rerun', { timeout: 5000 }, async t => {
  const gate = deferred();
  const started = deferred();
  let hold = false;
  const fixture = await peer(t, { history: async scope => {
    if (hold && scope.thread === null) { started.resolve(); await gate.promise; }
  } });
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  const before = seen.recoveryComplete.length;
  hold = true;
  await sub.resync();
  await started.promise;
  await Promise.all(Array.from({ length: 5 }, () => sub.resync()));
  gate.resolve();
  await until(() => seen.recoveryComplete.length >= before + 2 && sub.status === 'live', 'coalesced resync rerun');
  await sleep(40);
  assert.equal(fixture.requests.filter(request => request.url.pathname === '/api/messages/sync').length, 3);
  assert.equal(seen.recoveryComplete.length, before + 2);
});

test('one oversized queued event cannot bypass the byte budget', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const client = fixture.client({ realtime: { maxBufferedBytes: 400, maxBufferedEvents: 100 } });
  const sub = client.realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  fixture.send(message('large-event', { content: 'x'.repeat(1000) }));
  await until(() => sub.status === 'blocked', 'byte budget enforcement');
  assert.ok(seen.error.some(error => error.code === 'BACKPRESSURE'));
  assert.equal(seen.message.length, 0);
  assert.ok(client.realtime.getStats().queuedBytes <= 400);
});

test('a transient 503 upgrade retries with backoff and becomes live', { timeout: 5000 }, async t => {
  const fixture = await peer(t, { upgrade: request => request.number === 1 ? 503 : undefined });
  const client = fixture.client();
  const sub = client.realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  assert.equal(fixture.upgrades.length, 2);
  assert.ok(seen.connection.some(event => event.state === 'backoff'));
  assert.equal(client.realtime.getStats().reconnectCount, 1);
});

test('a long creation shared by catch-up and buffered live delivery is deduplicated', { timeout: 6000 }, async t => {
  const gate = deferred();
  const recovering = deferred();
  let rootReads = 0;
  const fixture = await peer(t, { history: async scope => {
    if (scope.thread === null && ++rootReads === 2) { recovering.resolve(); await gate.promise; }
  } });
  const sub = fixture.client().realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  fixture.latest().terminate();
  await recovering.promise;
  const fullBody = 'Long synthetic body. '.repeat(300);
  fixture.states.set('long-recovered-root', state('long-recovered-root', {
    original_body: fullBody, body: fullBody, created_at: timestamp + 10,
  }));
  fixture.send(message('long-recovered-root', { content: Array.from(fullBody).slice(0, 900).join('') + '…', is_long: true,
    body_blake3: 'a'.repeat(64), created_at: timestamp + 10 }));
  gate.resolve();
  await until(() => sub.status === 'live' || sub.status === 'blocked', 'long-message overlap processed');
  assert.equal(sub.status, 'live');
  await sleep(40);
  assert.equal(seen.message.filter(event => event.message.id === 'long-recovered-root').length, 1);
  assert.equal(seen.error.filter(error => error.code === 'PROTOCOL').length, 0);
});

test('a near-expiry credential rotates before the server revokes its old socket', { timeout: 6000 }, async t => {
  // Align within a second so the initial integer expiry leaves enough time for
  // handshakes, while always remaining shorter than an incorrect 1-second floor.
  await until(() => Date.now() % 1000 >= 100 && Date.now() % 1000 < 250, 'short-token clock window', 1500);
  const expiresAt = Math.floor(Date.now() / 1000) + 1;
  const fixture = await peer(t);
  const reasons = [];
  const sub = fixture.client({ tokenProvider: async context => {
    reasons.push(context.reason);
    return { token: `synthetic-short-${reasons.length}`,
      expiresAt: reasons.length === 1 ? expiresAt : Math.floor(Date.now() / 1000) + 120 };
  } }).realtime.subscribe(channel);
  const seen = capture(sub);
  await sub.ready;
  const old = fixture.latest();
  const expiryTimer = setTimeout(() => {
    if (old.readyState === WebSocket.OPEN) old.send(JSON.stringify({ type: 'session_revoked', reason: 'session expired' }));
  }, Math.max(0, expiresAt * 1000 - Date.now()));
  t.after(() => clearTimeout(expiryTimer));
  await until(() => fixture.upgrades.length >= 2 || sub.status === 'blocked', 'short-token refresh', 2000);
  assert.equal(sub.status === 'blocked', false);
  await until(() => sub.status === 'live', 'short-token replacement socket ready');
  assert.deepEqual(reasons, ['initial', 'expiring']);
  assert.equal(seen.sessionRevoked.length, 0);
  assert.ok(fixture.upgrades[1].at < expiresAt * 1000, 'Replacement must open before the old credential expires');
});

test('closing during a resync waiting on a handler cannot revive a stale generation', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const client = fixture.client();
  const sub = client.realtime.subscribe(channel);
  const seen = capture(sub);
  const handlerStarted = deferred();
  sub.on('message', async () => { handlerStarted.resolve(); await new Promise(() => {}); });
  await sub.ready;
  fixture.send(message('resync-before-close'));
  await handlerStarted.promise;
  await sub.resync();
  await client.close();
  const eventCountsAtClose = Object.fromEntries(Object.entries(seen).map(([name, values]) => [name, values.length]));
  await sleep(30);
  assert.equal(sub.status, 'closed');
  assert.deepEqual(Object.fromEntries(Object.entries(seen).map(([name, values]) => [name, values.length])),
    eventCountsAtClose, 'Close cancels queued control notifications and stale recovery callbacks');
  assert.equal(client.realtime.getStats().activeSockets, 0);
  assert.equal(client.realtime.getStats().queuedEvents, 0);
});

test('a handle attached during initial buffered delivery waits for the shared ready boundary', { timeout: 5000 }, async t => {
  const historyGate = deferred();
  const historyStarted = deferred();
  const handlerGate = deferred();
  const handlerStarted = deferred();
  let holdHistory = true;
  const fixture = await peer(t, { history: async scope => {
    if (holdHistory && scope.thread === null) {
      holdHistory = false;
      historyStarted.resolve();
      await historyGate.promise;
    }
  } });
  const client = fixture.client();
  const first = client.realtime.subscribe(channel);
  capture(first);
  first.on('message', async () => { handlerStarted.resolve(); await handlerGate.promise; });
  await historyStarted.promise;
  fixture.send(message('held-initial-delivery'));
  historyGate.resolve();
  await handlerStarted.promise;
  const second = client.realtime.subscribe(channel);
  capture(second);
  let secondReady = false;
  void second.ready.then(() => { secondReady = true; }, () => {});
  await sleep(20);
  assert.equal(secondReady, false, 'A live socket alone is not completed initial recovery');
  handlerGate.resolve();
  await Promise.all([first.ready, second.ready]);
  assert.equal(fixture.upgrades.length, 1);
});

test('a resync notification flood keeps only one active callback and one pending notification', { timeout: 5000 }, async t => {
  const historyGate = deferred();
  const historyStarted = deferred();
  const callbackGate = deferred();
  const callbackStarted = deferred();
  t.after(() => { historyGate.resolve(); callbackGate.resolve(); });
  let rootReads = 0;
  const fixture = await peer(t, { history: async scope => {
    if (scope.thread === null && ++rootReads === 2) {
      historyStarted.resolve();
      await historyGate.promise;
    }
  } });
  const sub = fixture.client({ realtime: { handlerTimeoutMs: 2000 } }).realtime.subscribe(channel);
  const seen = capture(sub);
  let calls = 0;
  let active = 0;
  let maximumActive = 0;
  sub.on('resync', async () => {
    const invocation = ++calls;
    maximumActive = Math.max(maximumActive, ++active);
    try {
      if (invocation === 1) { callbackStarted.resolve(); await callbackGate.promise; }
    } finally { active--; }
  });
  await sub.ready;
  fixture.send({ type: 'resync', channel_id: channel });
  await Promise.all([callbackStarted.promise, historyStarted.promise]);
  for (let index = 0; index < 100; index++) fixture.send({ type: 'resync', channel_id: channel });
  // The pong follows the preceding frames on this socket, so their notification
  // scheduling is observed before checking the callback concurrency bound.
  await new Promise(resolve => {
    fixture.latest().once('pong', resolve);
    fixture.latest().ping('resync-flood-barrier');
  });
  assert.equal(calls, 1);
  assert.equal(maximumActive, 1);
  callbackGate.resolve();
  await until(() => calls === 2, 'one coalesced pending resync notification');
  await sleep(25);
  assert.equal(calls, 2);
  assert.equal(maximumActive, 1);
  historyGate.resolve();
  await until(() => seen.recoveryComplete.length === 3 && sub.status === 'live', 'bounded recovery after notification flood');
});

test('client shutdown cancels active control dispatch and discards pending notifications', { timeout: 5000 }, async t => {
  const fixture = await peer(t);
  const client = fixture.client({ realtime: { handlerTimeoutMs: 2000 } });
  const sub = client.realtime.subscribe(channel);
  const callbackGate = deferred();
  const callbackStarted = deferred();
  t.after(() => callbackGate.resolve());
  let firstListenerCalls = 0;
  let laterListenerCalls = 0;
  let handlerErrors = 0;
  sub.on('resync', async () => {
    firstListenerCalls++;
    callbackStarted.resolve();
    await callbackGate.promise;
  });
  sub.on('resync', () => { laterListenerCalls++; });
  sub.on('handlerError', () => { handlerErrors++; });
  await sub.ready;
  await sub.resync();
  await callbackStarted.promise;
  await sub.resync();
  await client.close();
  callbackGate.resolve();
  await sleep(30);
  assert.equal(sub.status, 'closed');
  assert.equal(firstListenerCalls, 1, 'The pending notification must be discarded on close');
  assert.equal(laterListenerCalls, 0, 'Closing must abort remaining listeners in the active dispatch');
  assert.equal(handlerErrors, 0, 'Cancelling a control handler is not an application failure');
  assert.equal(client.realtime.getStats().subscriptions, 0);
});

test('a resync requested during the final buffered drain schedules another recovery', { timeout: 5000 }, async t => {
  const historyGate = deferred();
  const historyStarted = deferred();
  const handlerGate = deferred();
  const handlerStarted = deferred();
  t.after(() => { historyGate.resolve(); handlerGate.resolve(); });
  let rootReads = 0;
  const fixture = await peer(t, { history: async scope => {
    if (scope.thread === null && ++rootReads === 2) {
      historyStarted.resolve();
      await historyGate.promise;
    }
  } });
  const sub = fixture.client({ recovery: { mode: 'bestEffort', maxThreads: 0, maxStateRefresh: 0 } })
    .realtime.subscribe(channel);
  const seen = capture(sub);
  sub.on('message', async () => { handlerStarted.resolve(); await handlerGate.promise; });
  await sub.ready;
  await sub.resync();
  await historyStarted.promise;
  fixture.send(message('buffered-before-resync-rerun'));
  await new Promise(resolve => {
    fixture.latest().once('pong', resolve);
    fixture.latest().ping('buffered-drain-barrier');
  });
  historyGate.resolve();
  await handlerStarted.promise;
  assert.equal(seen.recoveryComplete.length, 2);
  await sub.resync();
  handlerGate.resolve();
  await until(() => rootReads === 3 && seen.recoveryComplete.length === 3 && sub.status === 'live',
    'resync requested inside the previous recovery drain');
  await sleep(25);
  assert.equal(rootReads, 3);
  assert.equal(seen.message.length, 1);
});
