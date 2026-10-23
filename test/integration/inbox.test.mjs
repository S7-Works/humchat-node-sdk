import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdir, mkdtemp, access } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat, MemoryCheckpointStore } from '../../dist/index.js';

const org = 'inbox-org'; const actor = 'inbox-self'; const peerId = 'inbox-peer'; const epoch = 'inbox-epoch';
const channel = id => `${org}:conv:${id}`;
const cursor = (id, sequence) => Buffer.from(JSON.stringify({ schema: 1, org_uuid: org, channel_id: channel(id), epoch, sequence: String(sequence) })).toString('base64url');
const sequence = value => Number(JSON.parse(Buffer.from(value, 'base64url').toString()).sequence);
const conversation = id => ({ id, channel_key: channel(id), name: null, kind: 'dm', member_count: 2, my_role: 'member',
  members: [{ user_uuid: actor, role: 'member' }, { user_uuid: peerId, role: 'member' }], last_message_at: null });
async function until(predicate, message, timeoutMs = 5000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) { if (await predicate()) return; await sleep(10); }
  assert.fail('Timed out: ' + message);
}
function deferred() {
  let resolve; let reject; const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}
async function fixture(t) {
  const rows = new Map(); const streams = new Map(); const sockets = new Map(); const clients = [];
  const errors = []; const requests = []; const upgrades = []; const sends = [];
  let discoveryStatus = 200; let discoveryGate; let durable = true; let denied = false;
  const respond = (response, value, status = 200) => { response.writeHead(status, { 'content-type': 'application/json' }); response.end(JSON.stringify(value)); };
  const wss = new WebSocketServer({ noServer: true });
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1'); requests.push(url.pathname);
      if (url.pathname === '/me') return respond(response, { user_uuid: actor, org, id: 1, email: 'inbox@example.invalid', roles: ['member'], scopes: [], expires_at: null, app_uuid: null });
      if (url.pathname === '/api/sdk/capabilities') return respond(response, { sdk_protocol_version: 1,
        authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
        messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: durable, event_protocol_versions: durable ? [1] : [], websocket_scope: 'channel' } });
      if (url.pathname === '/api/conversations') {
        if (discoveryGate) await discoveryGate;
        return respond(response, [...rows.values()], discoveryStatus);
      }
      if (url.pathname === '/api/messages/send') {
        let raw = ''; for await (const chunk of request) raw += chunk;
        const message = JSON.parse(raw); sends.push(message);
        return respond(response, { status: 'ACK', message_id: message.id, created_at: 1000,
          attachment_count: 0, attachment_manifest_blake3: '0'.repeat(64) });
      }
      if (url.pathname === '/api/events/sync') {
        if (denied) return respond(response, {}, 403);
        const id = url.searchParams.get('channel_id').slice(`${org}:conv:`.length);
        const events = streams.get(id) ?? [];
        const after = url.searchParams.get('after'); const through = url.searchParams.get('through') ?? cursor(id, events.length);
        const page = events.filter(event => Number(event.sequence) > sequence(after) && Number(event.sequence) <= sequence(through)).slice(0, 200);
        const last = page.at(-1)?.cursor ?? after;
        return respond(response, { schema: 1, org_uuid: org, channel_id: channel(id), epoch, events: page, after_cursor: after,
          through_cursor: through, through_sequence: String(sequence(through)), next_cursor: sequence(last) === sequence(through) ? null : last });
      }
      return respond(response, {}, 404);
    } catch (error) { errors.push(error); if (!response.headersSent) respond(response, {}, 500); else response.end(); }
  });
  server.on('upgrade', (request, socket, head) => {
    upgrades.push(request.url);
    const id = new URL(request.url, 'http://127.0.0.1').searchParams.get('channel_id').slice(`${org}:conv:`.length);
    wss.handleUpgrade(request, socket, head, ws => {
      sockets.set(ws, id); ws.on('error', () => {});
      const count = streams.get(id)?.length ?? 0;
      ws.send(JSON.stringify({ type: 'ready', schema: 1, org_uuid: org, channel_id: channel(id), epoch,
        cursor: cursor(id, count), sequence: String(count), earliest_cursor: cursor(id, 0) }));
    });
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const ws of sockets.keys()) ws.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(errors, [], 'Fixture errors must not masquerade as SDK failures');
  });
  return { rows, requests, upgrades, sockets, origin, sends,
    client(overrides = {}) {
      const client = new HumChat({ orgId: org, baseUrl: origin, tokenProvider: async () => ({ token: 'synthetic-inbox-token' }),
        timeout: 500, retry: { maxRetries: 0, totalTimeoutMs: 1000 },
        realtime: { handlerTimeoutMs: 500, pingIntervalMs: 1000, pongTimeoutMs: 500, reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20 }, ...overrides });
      clients.push(client); return client;
    },
    add(id) { rows.set(id, conversation(id)); },
    append(id, messageId) {
      const events = streams.get(id) ?? []; streams.set(id, events);
      const number = events.length + 1;
      const event = { type: 'event', schema: 1, org_uuid: org, channel_id: channel(id), epoch, sequence: String(number),
        cursor: cursor(id, number), event_id: `${epoch}:${number}`, kind: 'message', actor_id: peerId,
        state: { schema: 1, org_uuid: org, channel_id: channel(id), message_id: messageId, sender_id: peerId,
          created_at: 1000, thread_root_id: null, priority: 'standard', original_body: messageId, body: messageId,
          revision: 1, updated_at: 1000, deleted: false, reaction_revision: 0, reactions: {}, attachments: [] } };
      events.push(event); return event;
    },
    publish(event) { for (const [ws, id] of sockets) if (ws.readyState === WebSocket.OPEN && channel(id) === event.channel_id) ws.send(JSON.stringify(event)); },
    deny() { denied = true; }, setDiscoveryStatus(status) { discoveryStatus = status; },
    setDurable(value) { durable = value; }, gate(promise) { discoveryGate = promise; },
    scope(id, consumerId = 'inbox') { return { serverOrigin: origin, orgId: org, actorId: actor, consumerId, channelId: channel(id) }; },
  };
}
function watch(client, store, handler, options = {}) {
  return client.inbox.watch(handler, { consumerId: 'inbox', checkpointStore: store, pollIntervalMs: 100, ...options });
}

test('inbox discovers a remotely-created DM and consumes its first pre-discovery message, then resumes from checkpoint', { timeout: 10000 }, async t => {
  const peer = await fixture(t); const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const first = peer.client(); const observed = [];
  const inbox = watch(first, store, event => { observed.push(event.state.id); });
  await inbox.ready; assert.equal(inbox.getStats().conversations, 0);
  peer.add('new'); const initial = peer.append('new', 'before-discovery');
  await until(async () => await store.load(peer.scope('new')) === initial.cursor, 'first message checkpoint');
  assert.deepEqual(observed, ['before-discovery']);
  const second = peer.append('new', 'live'); peer.publish(second);
  await until(async () => await store.load(peer.scope('new')) === second.cursor, 'live checkpoint');
  await first.close(); await inbox.done;
  const third = peer.append('new', 'while-offline');
  const resumed = [];
  const next = watch(peer.client(), store, event => { resumed.push(event.state.id); });
  await next.ready;
  await until(async () => await store.load(peer.scope('new')) === third.cursor, 'resumed checkpoint');
  assert.deepEqual(resumed, ['while-offline']);
});

test('membership removal closes only owned conversation handles and rejoining uses retained checkpoint', { timeout: 10000 }, async t => {
  const peer = await fixture(t); peer.add('a'); peer.add('b');
  const store = new MemoryCheckpointStore(); t.after(() => store.close()); const client = peer.client();
  const seen = []; const inbox = watch(client, store, event => { seen.push(event.state.id); });
  await inbox.ready; await until(() => inbox.getStats().channels.every(entry => entry.status === 'live'), 'two live channels');
  const event = peer.append('a', 'a-first'); peer.publish(event);
  await until(async () => await store.load(peer.scope('a')) === event.cursor, 'first checkpoint');
  peer.rows.delete('a'); await inbox.refresh();
  assert.equal(inbox.getStats().conversations, 1); assert.equal(client.realtime.getStats().channels, 1);
  const missed = peer.append('a', 'a-away'); peer.publish(missed); await sleep(30); assert.deepEqual(seen, ['a-first']);
  peer.add('a'); await inbox.refresh();
  await until(async () => await store.load(peer.scope('a')) === missed.cursor, 'rejoin checkpoint');
  assert.deepEqual(seen, ['a-first', 'a-away']);
});

test('automatic inbox rejoin waits for a cancelled consumer to finish cleanup before replaying', { timeout: 10000 }, async t => {
  const peer = await fixture(t); peer.add('cleanup'); const event = peer.append('cleanup', 'command');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const client = peer.client({ realtime: { handlerTimeoutMs: 3000 } });
  const cleanup = deferred(); t.after(() => cleanup.resolve());
  let active = 0; let peak = 0; let cancellationObserved = false; let cleanupFinished = false;
  const starts = []; const signals = [];
  const inbox = watch(client, store, async (item, { signal }) => {
    starts.push(item.eventId); signals.push(signal); active++; peak = Math.max(peak, active);
    try {
      if (starts.length === 1) {
        await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
        cancellationObserved = true; await cleanup.promise; cleanupFinished = true;
      } else assert.equal(cleanupFinished, true, 'Replacement must wait for cancelled application cleanup');
    } finally { active--; }
  }, { pollIntervalMs: 10000 });
  await until(() => starts.length === 1, 'original consumer starts');
  peer.rows.delete('cleanup'); let removed = false; const removal = inbox.refresh();
  void removal.then(() => { removed = true; }, () => { removed = true; });
  await until(() => cancellationObserved && [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'removal cancels the consumer and closes its socket');
  assert.equal(signals[0].aborted, true); assert.equal(active, 1);
  assert.equal(removed, false, 'Removal must await cancelled application cleanup');
  assert.equal(inbox.getStats().conversations, 0); assert.equal(starts.length, 1);
  peer.add('cleanup'); assert.equal(inbox.refresh(), removal, 'Rejoin discovery coalesces with pending retirement');
  assert.notEqual(await store.load(peer.scope('cleanup')), event.cursor);
  cleanup.resolve(); await removal; await inbox.refresh();
  await until(async () => await store.load(peer.scope('cleanup')) === event.cursor, 'replacement replay acknowledgement');
  assert.deepEqual(starts, [event.event_id, event.event_id]); assert.equal(peak, 1); assert.equal(active, 0);
  assert.equal(inbox.status, 'watching'); assert.equal(inbox.error, null);
});

test('automatic inbox rejoin waits for an in-flight checkpoint write before loading the retained cursor', { timeout: 10000 }, async t => {
  const peer = await fixture(t); peer.add('commit'); const event = peer.append('commit', 'command');
  const storage = new MemoryCheckpointStore(); t.after(() => storage.close());
  const write = deferred(); t.after(() => write.resolve());
  let writeStarted = false; const casResults = [];
  const store = {
    load: scope => storage.load(scope),
    async compareAndSet(scope, previous, next) {
      if (scope.channelId === channel('commit') && next === event.cursor) { writeStarted = true; await write.promise; }
      const result = await storage.compareAndSet(scope, previous, next);
      if (scope.channelId === channel('commit') && next === event.cursor) casResults.push(result);
      return result;
    },
  };
  const client = peer.client({ realtime: { handlerTimeoutMs: 3000 } }); const observed = [];
  const inbox = watch(client, store, item => { observed.push(item.eventId); }, { pollIntervalMs: 10000 });
  await until(() => writeStarted, 'original checkpoint write starts');
  peer.rows.delete('commit'); let removed = false; const removal = inbox.refresh();
  void removal.then(() => { removed = true; }, () => { removed = true; });
  await until(() => [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'removal closes the socket with its checkpoint write still pending');
  assert.equal(removed, false, 'Removal must await the original checkpoint write');
  assert.equal(inbox.getStats().conversations, 0); assert.deepEqual(observed, [event.event_id]); assert.deepEqual(casResults, []);
  peer.add('commit'); assert.equal(inbox.refresh(), removal, 'Rejoin discovery coalesces with pending checkpoint retirement');
  write.resolve(); await removal; await inbox.refresh();
  await until(() => inbox.getStats().channels.length === 1 && inbox.getStats().channels[0].status === 'live', 'replacement resumes from committed checkpoint');
  assert.equal(await storage.load(peer.scope('commit')), event.cursor);
  assert.deepEqual(observed, [event.event_id]); assert.deepEqual(casResults, [true]);
  const next = peer.append('commit', 'next'); peer.publish(next);
  await until(async () => await storage.load(peer.scope('commit')) === next.cursor, 'replacement can acknowledge a new message');
  assert.deepEqual(observed, [event.event_id, next.event_id]);
  assert.equal(inbox.status, 'watching'); assert.equal(inbox.error, null); assert.equal(inbox.lastChannelError, null);
});

for (const failure of ['rejection', 'timeout', 'conflict']) {
  const errorCode = failure === 'conflict' ? 'CONFLICT' : 'PERSISTENCE';
  test(`automatic inbox rejoin blocks with sanitized ${errorCode} after checkpoint retirement ${failure}`, { timeout: 6000 }, async t => {
    const peer = await fixture(t); peer.add('store-failure'); const event = peer.append('store-failure', 'command');
    const storage = new MemoryCheckpointStore(); const write = deferred(); let pendingWrite;
    t.after(async () => { write.resolve(); await pendingWrite?.catch(() => {}); await storage.close(); });
    const store = {
      load: scope => storage.load(scope),
      compareAndSet(scope, previous, next) {
        if (next === event.cursor) return pendingWrite = write.promise.then(result => result === false ? false : storage.compareAndSet(scope, previous, next));
        return storage.compareAndSet(scope, previous, next);
      },
    };
    const client = peer.client({ retry: { maxRetries: 0, totalTimeoutMs: 500 } }); let deliveries = 0;
    const inbox = watch(client, store, () => { deliveries++; }, { pollIntervalMs: 10000 });
    await until(() => Boolean(pendingWrite), 'checkpoint write is held');
    peer.rows.delete('store-failure'); const removal = inbox.refresh(); void removal.catch(() => {});
    await until(() => [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'retirement closes the socket before the store settles');
    peer.add('store-failure'); assert.equal(inbox.refresh(), removal);
    const privateDetail = 'private-checkpoint-backend-password';
    if (failure === 'rejection') write.reject(new Error(privateDetail));
    if (failure === 'conflict') write.resolve(false);
    await until(() => inbox.status === 'blocked', 'retirement reports the store failure within its 500 ms budget', 1500);
    await assert.rejects(removal, error => {
      assert.equal(error.code, errorCode); assert.ok(!error.message.includes(privateDetail)); assert.equal(error.cause, undefined); return true;
    });
    await assert.rejects(inbox.done, { code: errorCode });
    assert.equal(inbox.error.code, errorCode); assert.ok(!inbox.error.message.includes(privateDetail));
    assert.equal(deliveries, 1); assert.equal(peer.upgrades.length, 1); assert.equal(inbox.getStats().conversations, 0);
    assert.notEqual(await storage.load(peer.scope('store-failure')), event.cursor);
    if (failure === 'timeout') {
      write.resolve(); await pendingWrite;
      assert.equal(await storage.load(peer.scope('store-failure')), event.cursor, 'An already-started write may finish after retirement times out');
    }
    await assert.rejects(inbox.refresh(), { code: errorCode });
    assert.equal(inbox.status, 'blocked'); assert.equal(deliveries, 1); assert.equal(peer.upgrades.length, 1);
  });
}

test('client close cancels automatic inbox retirement while a checkpoint write remains pending', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('store-close'); const event = peer.append('store-close', 'command');
  const storage = new MemoryCheckpointStore(); const write = deferred(); let pendingWrite;
  t.after(async () => { write.resolve(); await pendingWrite?.catch(() => {}); await storage.close(); });
  const store = {
    load: scope => storage.load(scope),
    compareAndSet(scope, previous, next) {
      if (next === event.cursor) return pendingWrite = write.promise.then(() => storage.compareAndSet(scope, previous, next));
      return storage.compareAndSet(scope, previous, next);
    },
  };
  const client = peer.client({ retry: { maxRetries: 0, totalTimeoutMs: 3000 } }); let deliveries = 0;
  const inbox = watch(client, store, () => { deliveries++; }, { pollIntervalMs: 10000 });
  await until(() => Boolean(pendingWrite), 'checkpoint write is held');
  peer.rows.delete('store-close'); const removal = inbox.refresh();
  const rejectedRemoval = assert.rejects(removal, { code: 'ABORT' }); void rejectedRemoval.catch(() => {});
  await until(() => [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'retirement reaches its pending checkpoint write');
  peer.add('store-close'); assert.equal(inbox.refresh(), removal);
  let closed = false; const close = client.close(); void close.then(() => { closed = true; }, () => {});
  await until(() => closed, 'client close completes before the 3 s checkpoint deadline', 1000); await close;
  await rejectedRemoval; await inbox.done;
  assert.equal(inbox.status, 'closed'); assert.notEqual(await storage.load(peer.scope('store-close')), event.cursor);
  assert.equal(client.realtime.getStats().channels, 0); assert.equal(deliveries, 1);
  write.resolve(); await pendingWrite;
  assert.equal(await storage.load(peer.scope('store-close')), event.cursor, 'An in-flight write may commit after close');
  assert.equal(inbox.status, 'closed'); assert.equal(deliveries, 1); assert.equal(peer.upgrades.length, 1);
});

test('removing multiple conversations cancels every consumer before awaiting any cleanup', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('a'); peer.add('b');
  const first = peer.append('a', 'a-command'); const second = peer.append('b', 'b-command');
  const store = new MemoryCheckpointStore(); t.after(() => store.close()); const cleanup = deferred(); t.after(() => cleanup.resolve());
  const signals = new Map(); const cancelled = new Set(); const client = peer.client({ realtime: { handlerTimeoutMs: 3000 } });
  const inbox = watch(client, store, async (item, { signal }) => {
    signals.set(item.state.id, signal);
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
    cancelled.add(item.state.id);
    if (item.state.id === 'a-command') await cleanup.promise;
  }, { pollIntervalMs: 10000 });
  await until(() => signals.size === 2, 'both consumers start');
  peer.rows.clear(); let removed = false; const removal = inbox.refresh();
  void removal.then(() => { removed = true; }, () => { removed = true; });
  await until(() => cancelled.size === 2 && [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'both removed conversations are cancelled while the first cleanup is held', 1000);
  assert.equal(removed, false); assert.equal(inbox.getStats().conversations, 0);
  assert.ok([...signals.values()].every(signal => signal.aborted));
  cleanup.resolve(); await removal;
  assert.notEqual(await store.load(peer.scope('a')), first.cursor); assert.notEqual(await store.load(peer.scope('b')), second.cursor);
  assert.equal(inbox.status, 'watching'); assert.equal(client.realtime.getStats().channels, 0);
});

test('removing an inbox consumer with a shared observer waits for cleanup and preserves other conversations', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('a'); peer.add('b'); const first = peer.append('a', 'a-command');
  const store = new MemoryCheckpointStore(); t.after(() => store.close()); const cleanup = deferred(); t.after(() => cleanup.resolve());
  const client = peer.client({ realtime: { handlerTimeoutMs: 3000 } }); const observed = []; let cancellationObserved = false;
  const inbox = watch(client, store, async (item, { signal }) => {
    observed.push(item.state.id);
    if (item.state.id === 'a-command') {
      await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
      cancellationObserved = true; await cleanup.promise;
    }
  }, { pollIntervalMs: 10000 });
  await until(() => observed.length === 1 && inbox.getStats().channels.some(entry => entry.channelId === channel('b') && entry.status === 'live'), 'consumer starts and the other channel becomes live');
  const observer = client.realtime.subscribe(channel('a'), { start: 'earliest' }); t.after(() => observer.close());
  peer.rows.delete('a'); let removed = false; const removal = inbox.refresh();
  void removal.then(() => { removed = true; }, () => { removed = true; });
  await until(() => cancellationObserved && observer.status === 'blocked' && [...peer.sockets].filter(([, id]) => id === 'a').every(([ws]) => ws.readyState === WebSocket.CLOSED), 'shared channel stops while consumer cleanup remains held');
  assert.equal(removed, false, 'Blocking the remaining observer must preserve the consumer cleanup deadline');
  assert.equal(inbox.status, 'watching'); assert.equal(inbox.getStats().conversations, 1);
  cleanup.resolve(); await removal;
  assert.equal(inbox.status, 'watching'); assert.equal(inbox.error, null);
  const next = peer.append('b', 'b-command'); peer.publish(next);
  await until(async () => await store.load(peer.scope('b')) === next.cursor, 'remaining inbox conversation still processes messages');
  assert.deepEqual(observed, ['a-command', 'b-command']); assert.notEqual(await store.load(peer.scope('a')), first.cursor);
  await observer.close(); assert.equal(client.realtime.getStats().channels, 1);
});

test('automatic inbox rejoin after settled work retains the checkpoint without replaying acknowledged events', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('settled'); const event = peer.append('settled', 'command');
  const store = new MemoryCheckpointStore(); t.after(() => store.close()); const observed = [];
  const inbox = watch(peer.client(), store, item => { observed.push(item.eventId); }, { pollIntervalMs: 10000 });
  await until(async () => await store.load(peer.scope('settled')) === event.cursor, 'original work is acknowledged');
  peer.rows.delete('settled'); await inbox.refresh();
  peer.add('settled'); await inbox.refresh();
  await until(() => inbox.getStats().channels.length === 1 && inbox.getStats().channels[0].status === 'live', 'rejoined channel catches up');
  assert.deepEqual(observed, [event.event_id]); assert.equal(await store.load(peer.scope('settled')), event.cursor);
  const next = peer.append('settled', 'after-rejoin'); peer.publish(next);
  await until(async () => await store.load(peer.scope('settled')) === next.cursor, 'rejoined channel acknowledges new work');
  assert.deepEqual(observed, [event.event_id, next.event_id]); assert.equal(inbox.status, 'watching');
});

for (const closing of ['watcher', 'client']) {
  test(`closing the ${closing} cancels a pending automatic rejoin without awaiting consumer cleanup`, { timeout: 6000 }, async t => {
    const peer = await fixture(t); peer.add('closing'); const event = peer.append('closing', 'command');
    const store = new MemoryCheckpointStore(); t.after(() => store.close());
    const client = peer.client({ realtime: { handlerTimeoutMs: 3000 } });
    const cleanup = deferred(); t.after(() => cleanup.resolve());
    let deliveries = 0; let cancellationObserved = false; let cleanupFinished = false;
    const inbox = watch(client, store, async (_item, { signal }) => {
      deliveries++;
      await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
      cancellationObserved = true; await cleanup.promise; cleanupFinished = true;
    }, { pollIntervalMs: 10000 });
    await until(() => deliveries === 1, 'original consumer starts');
    peer.rows.delete('closing'); const removal = inbox.refresh();
    const rejectedRejoin = assert.rejects(removal, { code: 'ABORT' });
    void rejectedRejoin.catch(() => {});
    await until(() => cancellationObserved && [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'retirement reaches the held consumer cleanup');
    peer.add('closing'); assert.equal(inbox.refresh(), removal, 'Rejoin remains pending while retirement awaits cleanup');
    let closed = false; const close = closing === 'watcher' ? inbox.close() : client.close();
    void close.then(() => { closed = true; }, () => {});
    await until(() => closed, 'explicit close completes while application cleanup is held', 1000); await close;
    await rejectedRejoin; await inbox.done;
    assert.equal(inbox.status, 'closed'); assert.equal(cleanupFinished, false); assert.equal(deliveries, 1);
    assert.equal(client.realtime.getStats().channels, 0);
    assert.notEqual(await store.load(peer.scope('closing')), event.cursor);
    const upgrades = peer.upgrades.length; cleanup.resolve();
    await until(() => cleanupFinished, 'application cleanup finishes after explicit close');
    assert.equal(peer.upgrades.length, upgrades); assert.equal(deliveries, 1);
  });
}

test('automatic inbox rejoin blocks at the handler deadline when a cancelled consumer never settles', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('stuck'); const event = peer.append('stuck', 'command');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const client = peer.client({ realtime: { handlerTimeoutMs: 500 } });
  let deliveries = 0; let observedSignal;
  const inbox = watch(client, store, (_item, { signal }) => {
    deliveries++; observedSignal = signal; return new Promise(() => {});
  }, { pollIntervalMs: 10000 });
  await until(() => deliveries === 1, 'never-settling consumer starts');
  peer.rows.delete('stuck'); const removal = inbox.refresh(); void removal.catch(() => {});
  await until(() => observedSignal.aborted && [...peer.sockets.keys()].every(ws => ws.readyState === WebSocket.CLOSED), 'retirement cancels the stuck consumer and closes its socket');
  peer.add('stuck'); assert.equal(inbox.refresh(), removal, 'Rejoin waits for the never-settling consumer');
  await until(() => inbox.status === 'blocked', 'rejoin observes the 500 ms handler deadline', 1500);
  await assert.rejects(removal, { code: 'PROTOCOL' });
  await assert.rejects(inbox.done, { code: 'PROTOCOL' });
  assert.equal(inbox.status, 'blocked'); assert.equal(inbox.error.code, 'PROTOCOL'); assert.equal(deliveries, 1);
  assert.equal(peer.upgrades.length, 1); assert.notEqual(await store.load(peer.scope('stuck')), event.cursor);
  await assert.rejects(inbox.refresh(), { code: 'PROTOCOL' }); assert.equal(deliveries, 1);
});

test('discovery failure is terminal, observable, and stops polling and delivery', { timeout: 6000 }, async t => {
  const peer = await fixture(t); const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const inbox = watch(peer.client(), store, () => {}); await inbox.ready;
  peer.setDiscoveryStatus(403);
  await assert.rejects(inbox.refresh(), { code: 'PERMISSION' }); await assert.rejects(inbox.done, { code: 'PERMISSION' });
  assert.equal(inbox.status, 'blocked'); assert.equal(inbox.error.code, 'PERMISSION');
  const count = peer.requests.length; await sleep(250); assert.equal(peer.requests.length, count);
});

test('initial discovery failures reject ready and unsupported durable replay does not silently fall back', { timeout: 6000 }, async t => {
  const peer = await fixture(t); const store = new MemoryCheckpointStore(); t.after(() => store.close());
  peer.setDiscoveryStatus(500);
  const failed = watch(peer.client(), store, () => {});
  await assert.rejects(failed.ready, { code: 'SERVER' }); await assert.rejects(failed.done, { code: 'SERVER' });
  peer.setDiscoveryStatus(200); peer.setDurable(false);
  const unsupported = watch(peer.client(), store, () => {});
  await assert.rejects(unsupported.ready, { code: 'UNSUPPORTED_FEATURE' });
  assert.equal(peer.upgrades.length, 0);
});

test('conversation capacity rejects explicitly before partially subscribing', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('a'); peer.add('b');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const inbox = watch(peer.client(), store, () => {}, { maxConversations: 1 });
  await assert.rejects(inbox.ready, { code: 'BACKPRESSURE' }); await assert.rejects(inbox.done, { code: 'BACKPRESSURE' });
  assert.equal(peer.upgrades.length, 0); assert.equal(inbox.getStats().conversations, 0);
});

test('channel permission failure and consumer failure are visible and do not advance checkpoints', { timeout: 10000 }, async t => {
  const peer = await fixture(t); peer.add('a'); const event = peer.append('a', 'first');
  const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const client = peer.client();
  const failing = watch(client, store, () => { throw new Error('Application failure with private details'); });
  await assert.rejects(failing.done, { code: 'PROTOCOL' }); assert.equal(failing.status, 'blocked');
  assert.notEqual(await store.load(peer.scope('a')), event.cursor);
  assert.ok(!failing.error.message.includes('private details')); await failing.close();
  peer.deny();
  const denied = watch(client, store, () => {});
  await assert.rejects(denied.done, { code: 'PERMISSION' }); assert.equal(denied.status, 'blocked');
  assert.notEqual(await store.load(peer.scope('a')), event.cursor);
});

test('closing during discovery cancels polling without opening sockets and leaves the caller store open', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('late');
  let release; peer.gate(new Promise(resolve => { release = resolve; }));
  const store = new MemoryCheckpointStore(); t.after(() => store.close()); const client = peer.client();
  const inbox = watch(client, store, () => assert.fail('No callback after close'));
  await until(() => peer.requests.includes('/api/conversations'), 'held discovery request');
  const pending = inbox.refresh();
  await client.close(); release();
  await assert.rejects(pending, { code: 'ABORT' }); await assert.rejects(inbox.ready, { code: 'ABORT' }); await inbox.done;
  const count = peer.requests.length; await sleep(250);
  assert.equal(inbox.status, 'closed'); assert.equal(peer.requests.length, count); assert.equal(peer.upgrades.length, 0);
  assert.equal(await store.load(peer.scope('late')), null);
});

test('discovery refresh coalesces and rejects foreign routing or missing self membership', { timeout: 6000 }, async t => {
  const peer = await fixture(t); const store = new MemoryCheckpointStore(); t.after(() => store.close());
  const client = peer.client(); const inbox = watch(client, store, () => {}); await inbox.ready;
  let release; peer.gate(new Promise(resolve => { release = resolve; }));
  const first = inbox.refresh(); const second = inbox.refresh(); assert.equal(first, second);
  peer.rows.set('foreign', { ...conversation('foreign'), channel_key: 'other-org:conv:foreign' }); release();
  await assert.rejects(first, { code: 'PROTOCOL' }); assert.equal(peer.upgrades.length, 0);
  await inbox.close(); peer.rows.clear(); peer.rows.set('missing', { ...conversation('missing'), members: [{ user_uuid: 'outsider', role: 'member' }, { user_uuid: peerId, role: 'member' }] });
  const next = watch(client, store, () => {});
  await assert.rejects(next.ready, { code: 'PROTOCOL' }); assert.equal(peer.upgrades.length, 0);
});

test('closing an inbox aborts its active consumer without acknowledging and preserves an unrelated manual subscription', { timeout: 6000 }, async t => {
  const peer = await fixture(t); peer.add('a'); const event = peer.append('a', 'held');
  const store = new MemoryCheckpointStore(); t.after(() => store.close()); const client = peer.client();
  const manual = client.realtime.subscribe(channel('manual'), { start: 'earliest' }); await manual.ready;
  let started; const began = new Promise(resolve => { started = resolve; }); let observedSignal;
  const inbox = watch(client, store, async (_event, { signal }) => {
    observedSignal = signal; started();
    await new Promise(resolve => signal.addEventListener('abort', resolve, { once: true }));
  });
  await began; await inbox.close(); await inbox.done;
  assert.equal(observedSignal.aborted, true); assert.equal(manual.status, 'live');
  assert.equal(client.realtime.getStats().channels, 1);
  assert.notEqual(await store.load(peer.scope('a')), event.cursor);
});

test('runnable DM responder discovers a retained command, replies in its thread, and releases file-store locks', { timeout: 10000 }, async t => {
  const peer = await fixture(t); peer.add('example');
  const event = peer.append('example', 'command'); event.state.body = '!status'; event.state.original_body = '!status';
  const root = fileURLToPath(new URL('../../', import.meta.url));
  const artifacts = join(root, 'target', 'dm-inbox'); await mkdir(artifacts, { recursive: true });
  const directory = await mkdtemp(join(artifacts, 'responder-'));
  const environment = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    ['PATH', 'SYSTEMROOT', 'TEMP', 'TMP', 'USERPROFILE', 'APPDATA', 'LOCALAPPDATA'].includes(key.toUpperCase())));
  const child = spawn(process.execPath, [join(root, 'examples', 'dm-responder.mjs')], { cwd: root, windowsHide: true,
    env: { ...environment, HUMCHAT_ORG_ID: org, HUMCHAT_BASE_URL: peer.origin, HUMCHAT_TOKEN: 'synthetic-example-token',
      HUMCHAT_STATE_DIR: directory, HUMCHAT_CONSUMER_ID: 'example', HUMCHAT_RUN_FOR_MS: '600' }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; let stderr = ''; child.stdout.on('data', data => { output += data; }); child.stderr.on('data', data => { stderr += data; });
  t.after(() => { if (child.exitCode === null) child.kill(); });
  const result = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', (code, signal) => resolve({ code, signal })); });
  assert.deepEqual(result, { code: 0, signal: null }, stderr);
  assert.equal(stderr, ''); assert.ok(output.includes('"event":"watching"')); assert.ok(output.includes('"event":"reply"'));
  assert.ok(output.includes('"event":"stopped"')); assert.equal(peer.sends.length, 1);
  assert.equal(peer.sends[0].channel_id, channel('example')); assert.equal(peer.sends[0].thread_root_id, 'command');
  assert.equal(peer.sends[0].content, 'All systems operational.');
  await assert.rejects(access(join(directory, 'dm-checkpoints.json.lock')), { code: 'ENOENT' });
  await assert.rejects(access(join(directory, 'dm-outbox.json.lock')), { code: 'ENOENT' });
});
