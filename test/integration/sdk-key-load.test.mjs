import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { inspect } from 'node:util';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat, MemoryCheckpointStore } from '../../dist/index.js';

// These are bounded loopback concurrency checks, not production throughput measurements.
// No credentials or customer data are used, and metrics contain counts only.
const org = '00000000-0000-4000-8000-000000000051';
const actor = 'synthetic-load-member';
const email = 'synthetic.load@example.invalid';
const sdkKey = 'hsk_synthetic_load_fixture_only';
const token = number => `synthetic-load-token-${number}`;
const epoch = 'synthetic-load-epoch';
const cursor = (channelId, sequence = 0) => Buffer.from(JSON.stringify({ schema: 1, org_uuid: org,
  channel_id: channelId, epoch, sequence: String(sequence) })).toString('base64url');
const sequence = value => Number(JSON.parse(Buffer.from(value, 'base64url')).sequence);
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
async function until(condition, label, timeout = 8000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await condition()) return; await sleep(5); }
  assert.fail(`Timed out: ${label}`);
}

async function peer(t, mode, count, options = {}) {
  const channels = Array.from({ length: count }, (_, index) => `${org}:team:load-${index}`);
  const requests = [], mints = [], upgrades = [], sockets = [], errors = [], clients = [];
  const streams = new Map(channels.map(channelId => [channelId, []]));
  let activeMints = 0, maximumMints = 0;
  const wss = new WebSocketServer({ noServer: true });
  const reply = (response, status, body, headers = {}) => {
    if (response.destroyed) return;
    response.writeHead(status, { 'content-type': 'application/json', ...headers });
    response.end(JSON.stringify(body));
  };
  const capabilities = { sdk_protocol_version: 1,
    authentication: { user_sessions: true, service_identities: false, api_key_exchange: false,
      app_binding_required: false, mock_authentication: true, socket_session_end_codes: ['TOKEN_EXPIRED'],
      sdk_key_user_tokens: { endpoint: '/api/sdk/auth/user-token', key_header: 'X-HumChat-Sdk-Key', configured: true } },
    messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
      durable_replay: true, event_protocol_versions: [1], websocket_scope: 'channel',
      multiplexed_events: { protocol_versions: [1], max_channels: 100 } } };
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      const item = { url, headers: request.headers, at: Date.now(), response };
      requests.push(item);
      if (url.pathname === '/api/sdk/auth/user-token') {
        let body = '';
        for await (const part of request) body += part;
        item.body = JSON.parse(body);
        item.number = mints.length + 1;
        mints.push(item);
        activeMints++;
        maximumMints = Math.max(maximumMints, activeMints);
        try {
          const custom = await options.mint?.(item);
          if (custom?.status) return reply(response, custom.status,
            { message: `Deliberately hostile response containing ${sdkKey} ${token(1)}` },
            { 'x-request-id': sdkKey, ...custom.headers });
          return reply(response, 200, { access_token: token(item.number), token_type: 'Bearer', expires_in: 900 });
        } finally { activeMints--; }
      }
      assert.equal(request.headers['x-humchat-sdk-key'], undefined);
      assert.match(request.headers.authorization ?? '', /^Bearer synthetic-load-token-\d+$/);
      if (url.pathname === '/me') return reply(response, 200, { user_uuid: actor, org, email, roles: ['member'], scopes: [] });
      if (url.pathname === '/api/sdk/capabilities') return reply(response, 200, capabilities);
      if (url.pathname === '/api/org/members') return reply(response, 200,
        [{ user_uuid: actor, org_uuid: org, name: 'Synthetic member', email, role: 'member', status: 'active' }]);
      if (url.pathname === '/api/events/sync') {
        const channelId = url.searchParams.get('channel_id');
        const after = url.searchParams.get('after');
        const through = url.searchParams.get('through') ?? cursor(channelId, streams.get(channelId).length);
        return reply(response, 200, { schema: 1, org_uuid: org, channel_id: channelId, epoch,
          events: streams.get(channelId).slice(sequence(after), sequence(through)), after_cursor: after,
          through_cursor: through, through_sequence: String(sequence(through)), next_cursor: null });
      }
      return reply(response, 404, {});
    } catch (error) { errors.push(error); reply(response, 500, {}); }
  });
  const ready = (channelId, subscriptionId) => ({ type: 'ready', schema: 1, org_uuid: org,
    channel_id: channelId, epoch, cursor: cursor(channelId, streams.get(channelId).length),
    sequence: String(streams.get(channelId).length), earliest_cursor: cursor(channelId),
    ...(subscriptionId ? { subscription_id: subscriptionId } : {}) });
  server.on('upgrade', (request, socket, head) => {
    try {
      const item = { url: new URL(request.url, 'http://127.0.0.1'), headers: request.headers };
      assert.equal(request.headers['x-humchat-sdk-key'], undefined);
      assert.match(request.headers.authorization ?? '', /^Bearer synthetic-load-token-\d+$/);
      assert.equal(item.url.searchParams.has('token'), false);
      assert.equal(item.url.searchParams.has('sdk_key'), false);
      upgrades.push(item);
      wss.handleUpgrade(request, socket, head, connected => {
        sockets.push(connected);
        connected.on('error', () => {});
        if (mode === 'multiplex') {
          connected.send(JSON.stringify({ type: 'stream_ready', schema: 1, org_uuid: org, max_channels: 100 }));
          connected.on('message', bytes => {
            try {
              const frame = JSON.parse(bytes.toString());
              if (frame.type === 'subscribe') connected.send(JSON.stringify(ready(frame.channel_id, frame.subscription_id)));
              if (frame.type === 'unsubscribe') connected.send(JSON.stringify({ type: 'unsubscribed', schema: 1, subscription_id: frame.subscription_id }));
            } catch (error) { errors.push(error); connected.terminate(); }
          });
        } else connected.send(JSON.stringify(ready(item.url.searchParams.get('channel_id'))));
      });
    } catch (error) { errors.push(error); socket.destroy(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const socket of sockets) socket.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(errors, [], 'Loopback peer must not hide fixture failures');
    for (const item of mints) {
      assert.equal(item.headers.authorization, undefined);
      assert.equal(item.headers['x-humchat-sdk-key'], sdkKey);
      assert.deepEqual(item.body, { org_uuid: org, email, expires_in: 900 });
    }
  });
  return {
    channels, requests, mints, upgrades, sockets,
    get maximumMints() { return maximumMints; },
    scope(consumerId, channelId) { return { serverOrigin: baseUrl, orgId: org, actorId: actor, consumerId, channelId }; },
    append(round) {
      for (const channelId of channels) {
        const rows = streams.get(channelId), index = rows.length + 1, timestamp = 1_792_370_346;
        const id = `${round}-${channelId}`;
        rows.push({ type: 'event', schema: 1, org_uuid: org, channel_id: channelId, epoch,
          sequence: String(index), event_id: `${epoch}:${index}`, cursor: cursor(channelId, index), actor_id: actor, kind: 'message',
          state: { schema: 1, org_uuid: org, channel_id: channelId, message_id: id, sender_id: actor,
            created_at: timestamp, thread_root_id: null, priority: 'standard', original_body: id, body: id,
            revision: 1, updated_at: timestamp, deleted: false, reaction_revision: 0, reactions: {}, attachments: [] } });
      }
    },
    client() {
      const client = new HumChat({ orgId: org, baseUrl, auth: { sdkKey, actAs: email }, timeout: 3000,
        retry: { maxRetries: 0, totalTimeoutMs: 6000 }, recovery: { mode: 'durable' },
        realtime: { transport: mode, ...(count > 50 ? { maxChannels: count } : {}),
          reconnectInitialDelayMs: 20, reconnectMaxDelayMs: 80, pingIntervalMs: 1000, pongTimeoutMs: 500 } });
      clients.push(client);
      return client;
    },
    expire() { for (const socket of sockets) if (socket.readyState === WebSocket.OPEN)
      socket.send(JSON.stringify({ type: 'session_revoked', code: 'TOKEN_EXPIRED', reason: 'session expired' })); },
    drop() { for (const socket of sockets) socket.terminate(); },
  };
}

async function closeAndCheck(client, fixture) {
  await client.close();
  await until(() => fixture.sockets.every(socket => socket.readyState === WebSocket.CLOSED), 'all peer sockets closed');
  const stats = client.realtime.getStats();
  assert.equal(stats.activeSockets, 0);
  assert.equal(stats.channels, 0);
  assert.equal(stats.subscriptions, 0);
  assert.equal(stats.queuedEvents, 0);
  assert.equal(stats.queuedBytes, 0);
  const counts = [fixture.requests.length, fixture.mints.length, fixture.upgrades.length];
  await sleep(180); // Longer than the configured reconnect cap, to catch orphaned reconnect timers.
  assert.deepEqual([fixture.requests.length, fixture.mints.length, fixture.upgrades.length], counts,
    'Closed client must not start another HTTP call, mint, or socket');
}

for (const [mode, count] of [['channel', 50], ['multiplex', 100]]) {
  test(`${mode}: ${count} channels share auth and recover every checkpoint across rotation and disconnect storms`,
    { timeout: 20_000 }, async t => {
      const initial = deferred(), renewal = deferred(), releaseInitial = deferred(), releaseRenewal = deferred();
      t.after(() => { releaseInitial.resolve(); releaseRenewal.resolve(); });
      const fixture = await peer(t, mode, count, { mint: async item => {
        if (item.number === 1) { initial.resolve(); await releaseInitial.promise; }
        if (item.number === 2) { renewal.resolve(); await releaseRenewal.promise; }
      } });
      const client = fixture.client(), store = new MemoryCheckpointStore();
      t.after(() => store.close());
      fixture.append('initial');
      const received = new Map(fixture.channels.map(channelId => [channelId, []]));
      const subscriptions = fixture.channels.map(channelId => {
        const sub = client.realtime.subscribe(channelId, { start: 'earliest' });
        sub.consume(event => { received.get(channelId).push(event.state.id); },
          { consumerId: 'load-consumer', checkpointStore: store });
        return sub;
      });
      const rosters = Array.from({ length: count }, () => client.members.list());
      await initial.promise;
      await sleep(40);
      assert.equal(fixture.mints.length, 1, 'All initial HTTP and channel waiters join one mint');
      releaseInitial.resolve();
      await Promise.all([...subscriptions.map(sub => sub.ready), ...rosters]);
      const settledAt = async position => subscriptions.every(sub => sub.status === 'live') &&
        (await Promise.all(fixture.channels.map(channelId => store.load(fixture.scope('load-consumer', channelId)))))
          .every((value, index) => value === cursor(fixture.channels[index], position));
      await until(() => settledAt(1), 'initial checkpoints on every channel');
      const sockets = mode === 'channel' ? count : 1;
      assert.equal(client.realtime.getStats().activeSockets, sockets);
      assert.equal(fixture.upgrades.length, sockets);
      fixture.expire();
      await renewal.promise;
      const renewedRosters = Array.from({ length: count }, () => client.members.list());
      await sleep(40);
      assert.equal(fixture.mints.length, 2, 'Expiry storm and HTTP share one renewal');
      fixture.append('during-rotation');
      releaseRenewal.resolve();
      await Promise.all(renewedRosters);
      await until(() => settledAt(2), 'every checkpoint catches up after rotation');
      assert.equal(fixture.upgrades.length, sockets * 2);
      assert.ok(fixture.upgrades.slice(sockets).every(item => item.headers.authorization === `Bearer ${token(2)}`));
      for (const [round, position] of [['drop-one', 3], ['drop-two', 4]]) {
        fixture.drop();
        fixture.append(round);
        await until(() => settledAt(position), `all ${count} channels recover ${round}`);
      }
      assert.equal(fixture.mints.length, 2, 'Network disconnect storms reuse the verified renewal');
      assert.equal(fixture.maximumMints, 1);
      assert.equal(fixture.upgrades.length, sockets * 4);
      for (const channelId of fixture.channels) assert.deepEqual(received.get(channelId),
        ['initial', 'during-rotation', 'drop-one', 'drop-two'].map(round => `${round}-${channelId}`));
      t.diagnostic(JSON.stringify({ mode, channels: count, physicalSockets: sockets, mints: fixture.mints.length,
        maximumConcurrentMints: fixture.maximumMints, handshakes: fixture.upgrades.length,
        acknowledgedEvents: count * 4, reconnectCount: client.realtime.getStats().reconnectCount }));
      await closeAndCheck(client, fixture);
    });

  for (const status of [503, 429]) test(`${mode}: ${count} channels survive sustained mint ${status} without secret leaks or orphaned work`,
    { timeout: 15_000 }, async t => {
      let outage = false;
      const fixture = await peer(t, mode, count, { mint: item => outage && item.number > 1
        ? { status, ...(status === 429 ? { headers: { 'retry-after': '0.08' } } : {}) } : undefined });
      const client = fixture.client(), failures = [];
      const subscriptions = fixture.channels.map(channelId => {
        const sub = client.realtime.subscribe(channelId);
        sub.on('error', error => { failures.push(error); });
        return sub;
      });
      await Promise.all(subscriptions.map(sub => sub.ready));
      outage = true;
      fixture.expire();
      await until(() => fixture.mints.length > 1, 'first failed renewal reaches peer');
      const started = Date.now();
      await sleep(350);
      const outageMints = fixture.mints.length - 1;
      const observedOutageMs = Date.now() - started;
      assert.equal(outageMints, 1,
        'All channel reconnect loops share the native acquisition cooldown after one failed mint');
      outage = false;
      await until(() => subscriptions.every(sub => sub.status === 'live'), 'all channels recover after mint service returns');
      assert.ok(failures.some(error => error.status === status));
      assert.ok(subscriptions.every(sub => sub.status !== 'blocked'));
      assert.equal(fixture.maximumMints, 1, 'Only one native mint is in flight per client');
      const exposed = inspect(failures, { depth: null, showHidden: true }) + JSON.stringify(failures);
      assert.equal(exposed.includes(sdkKey), false);
      assert.equal(exposed.includes(token(1)), false);
      for (let index = 2; index < fixture.mints.length; index++) {
        assert.ok(fixture.mints[index].at - fixture.mints[index - 1].at >= 990,
          'Native acquisition cooldown persists across independently scheduled channel reconnects');
      }
      t.diagnostic(JSON.stringify({ mode, channels: count, outageStatus: status, observedOutageMs,
        outageMints, totalMints: fixture.mints.length, maximumConcurrentMints: fixture.maximumMints,
        deliveredErrors: failures.length }));
      await closeAndCheck(client, fixture);
    });

  test(`${mode}: ${count} channels share increasing backoff across consecutive mint failures`,
    { timeout: 15_000 }, async t => {
      const fixture = await peer(t, mode, count, { mint: item => [2, 3].includes(item.number)
        ? { status: 503 } : undefined });
      const client = fixture.client();
      const subscriptions = fixture.channels.map(channelId => client.realtime.subscribe(channelId));
      await Promise.all(subscriptions.map(sub => sub.ready));
      fixture.expire();
      await until(() => fixture.mints.length === 4 && subscriptions.every(sub => sub.status === 'live'),
        'two service failures recover through shared increasing backoff', 10_000);
      assert.equal(fixture.mints.length, 4, 'Each failed acquisition advances one shared backoff stage');
      assert.equal(fixture.maximumMints, 1);
      assert.ok(fixture.mints[2].at - fixture.mints[1].at >= 990, 'First failure imposes at least one second');
      assert.ok(fixture.mints[3].at - fixture.mints[2].at >= 1990, 'Second failure imposes at least two seconds');
      t.diagnostic(JSON.stringify({ mode, channels: count, failedMints: 2, totalMints: fixture.mints.length,
        firstCooldownMs: fixture.mints[2].at - fixture.mints[1].at,
        secondCooldownMs: fixture.mints[3].at - fixture.mints[2].at }));
      await closeAndCheck(client, fixture);
    });

  test(`${mode}: closing ${count} queued channels and HTTP waiters cancels shared login without late work`,
    { timeout: 10_000 }, async t => {
      const entered = deferred(), release = deferred(), cancelled = deferred();
      t.after(() => release.resolve());
      const fixture = await peer(t, mode, count, { mint: async item => {
        item.response.once('close', () => cancelled.resolve());
        entered.resolve();
        await release.promise;
      } });
      const client = fixture.client();
      const subscriptions = fixture.channels.map(channelId => client.realtime.subscribe(channelId));
      // Attach rejection observers immediately: close must settle every initial
      // ready promise, including workers queued behind the five admission slots.
      const pending = Promise.allSettled([...subscriptions.map(sub => sub.ready),
        ...Array.from({ length: count }, () => client.members.list())]);
      await entered.promise;
      await sleep(40);
      assert.equal(fixture.mints.length, 1);
      await client.close();
      const results = await pending;
      assert.ok(results.every(result => result.status === 'rejected' && result.reason.code === 'ABORT'));
      await Promise.race([cancelled.promise, sleep(1500).then(() => assert.fail('Shared mint HTTP request was not cancelled'))]);
      release.resolve();
      await closeAndCheck(client, fixture);
      assert.equal(fixture.mints.length, 1);
      assert.equal(fixture.upgrades.length, 0);
      assert.equal(fixture.requests.filter(item => item.url.pathname === '/me').length, 0);
      assert.equal(fixture.maximumMints, 1);
      t.diagnostic(JSON.stringify({ mode, cancelledChannels: count, cancelledHttpCalls: count,
        cancelledMints: fixture.mints.length, handshakes: fixture.upgrades.length }));
    });
}
