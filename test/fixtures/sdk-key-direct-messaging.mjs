import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { HumChat } from '../../dist/index.js';

// This is a synthetic HTTP/WebSocket protocol peer, not a running HumChat
// server or browser app. State is retained by the peer across SDK restarts.
const org = '00000000-0000-4000-8000-000000000061';
const sdkKey = 'hsk_synthetic_direct_messaging_fixture_only';
const members = ['alice', 'bob', 'carol'].map(name => ({
  id: `synthetic-dm-${name}`, email: `${name}@example.invalid`, name,
}));
const [alice, bob, carol] = members;
const epoch = 'synthetic-direct-messaging-epoch';
const consumerId = 'native-sdk-direct-messaging';
const channel = id => `${org}:conv:${id}`;
const cursor = (channelId, sequence) => Buffer.from(JSON.stringify({ schema: 1,
  org_uuid: org, channel_id: channelId, epoch, sequence: String(sequence) })).toString('base64url');
const position = value => Number(JSON.parse(Buffer.from(value, 'base64url')).sequence);
async function until(condition, label) {
  const deadline = Date.now() + 6000;
  while (Date.now() < deadline) { if (await condition()) return; await sleep(5); }
  assert.fail(`Timed out: ${label}`);
}

async function peer(t, transport, options = {}) {
  const conversations = new Map(), streams = new Map(), states = new Map(), tokens = new Map();
  const sends = new Map(), mutations = new Map();
  const requests = [], mints = [], sockets = [], clients = [], errors = [];
  let timestamp = 1_792_370_000;
  const stream = channelId => streams.get(channelId) ?? [];
  const permitted = (member, channelId) => [...conversations.values()].some(row =>
    row.channel_key === channelId && row.members.some(entry => entry.user_uuid === member.id));
  const reply = (response, status, value) => {
    response.writeHead(status, { 'content-type': 'application/json' });
    response.end(JSON.stringify(value));
  };
  const capabilities = { sdk_protocol_version: 1, authentication: {
    user_sessions: true, service_identities: false, api_key_exchange: false,
    app_binding_required: false, mock_authentication: true,
    sdk_key_user_tokens: { endpoint: '/api/sdk/auth/user-token', key_header: 'X-HumChat-Sdk-Key', configured: true },
  }, messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
    durable_replay: true, event_protocol_versions: [1], websocket_scope: 'channel',
    multiplexed_events: { protocol_versions: [1], max_channels: 100 } } };
  const publish = event => {
    for (const entry of sockets) {
      if (entry.socket.readyState !== WebSocket.OPEN || !permitted(entry.member, event.channel_id)) continue;
      for (const [subscriptionId, channelId] of entry.subscriptions) {
        if (channelId === event.channel_id) entry.socket.send(JSON.stringify({ ...event,
          ...(subscriptionId ? { subscription_id: subscriptionId } : {}) }));
      }
    }
  };
  const append = (state, kind, actorId) => {
    const rows = stream(state.channel_id), sequence = String(rows.length + 1);
    const event = { type: 'event', schema: 1, org_uuid: org, channel_id: state.channel_id,
      epoch, sequence, cursor: cursor(state.channel_id, sequence), event_id: `${epoch}:${sequence}`,
      actor_id: actorId, kind, state: structuredClone(state) };
    options.event?.(event);
    rows.push(event); streams.set(state.channel_id, rows); publish(event); return event;
  };
  const readBody = async request => {
    let text = ''; for await (const part of request) text += part;
    return JSON.parse(text);
  };
  const wss = new WebSocketServer({ noServer: true });
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      const entry = { path: url.pathname, method: request.method, url, headers: request.headers };
      requests.push(entry);
      if (url.pathname === '/api/sdk/auth/user-token') {
        assert.equal(request.method, 'POST');
        assert.equal(request.headers.authorization, undefined);
        assert.equal(request.headers['x-humchat-sdk-key'], sdkKey);
        const body = await readBody(request);
        assert.equal(body.org_uuid, org); assert.equal(body.expires_in, 900);
        const member = members.find(value => value.email === body.email);
        if (!member) return reply(response, 403, {});
        mints.push(member.id);
        const token = `synthetic-native-dm-${member.name}-${mints.length}`;
        tokens.set(token, member);
        return reply(response, 200, { access_token: token, token_type: 'Bearer', expires_in: 900 });
      }
      assert.equal(request.headers['x-humchat-sdk-key'], undefined, 'The SDK key is used only for minting');
      const member = tokens.get(request.headers.authorization?.replace(/^Bearer /, ''));
      if (!member) return reply(response, 401, {});
      entry.actorId = member.id;
      const override = await options.route?.({ url, method: request.method, member });
      if (override) return reply(response, override.status, override.body);
      if (url.pathname === '/me') return reply(response, 200, { user_uuid: member.id,
        org, email: member.email, roles: ['member'], scopes: [] });
      if (url.pathname === '/api/sdk/capabilities') return reply(response, 200, capabilities);
      if (url.pathname === '/api/org/members') return reply(response, 200, members.map(value => ({
        user_uuid: value.id, org_uuid: org, name: value.name, email: value.email, role: 'member', status: 'active',
      })));
      if (url.pathname === '/api/conversations') {
        if (request.method === 'GET') return reply(response, 200, [...conversations.values()]
          .filter(row => row.members.some(value => value.user_uuid === member.id)));
        const body = await readBody(request);
        assert.equal(body.name, null); assert.equal(body.member_uuids.length, 1);
        const recipient = members.find(value => value.id === body.member_uuids[0]);
        if (!recipient || recipient.id === member.id) return reply(response, 403, {});
        const pair = [member.id, recipient.id].sort();
        const existing = [...conversations.values()].find(row =>
          row.members.map(value => value.user_uuid).sort().join(':') === pair.join(':'));
        if (existing) return reply(response, 200, existing);
        const id = `synthetic-conversation-${conversations.size + 1}`;
        const row = { id, channel_key: channel(id), name: null, kind: 'dm', member_count: 2,
          my_role: 'member', members: pair.map(user_uuid => ({ user_uuid, role: 'member' })), last_message_at: null };
        conversations.set(id, row); return reply(response, 200, row);
      }
      if (url.pathname === '/api/messages/send') {
        const body = await readBody(request);
        if (!permitted(member, body.channel_id)) return reply(response, 403, {});
        assert.equal(request.headers['x-zlack-message-receipt'], 'v1');
        const key = `${member.id}:${body.channel_id}:${request.headers['x-idempotency-key']}`;
        const prior = sends.get(key);
        if (prior) { assert.deepEqual(body, prior.body); return reply(response, 200, prior.receipt); }
        const state = { schema: 1, org_uuid: org, channel_id: body.channel_id, message_id: body.id,
          sender_id: member.id, created_at: ++timestamp, thread_root_id: body.thread_root_id,
          priority: body.priority, original_body: body.content, body: body.content,
          revision: 1, updated_at: timestamp, deleted: false, reaction_revision: 0, reactions: {}, attachments: [] };
        states.set(body.id, state); append(state, 'message', member.id);
        const row = [...conversations.values()].find(value => value.channel_key === body.channel_id);
        row.last_message_at = timestamp;
        const receipt = { status: 'ACK', message_id: body.id, created_at: timestamp,
          attachment_count: 0, attachment_manifest_blake3: '0'.repeat(64) };
        sends.set(key, { body, receipt }); return reply(response, 200, receipt);
      }
      if (url.pathname === '/api/messages/mutate') {
        const body = await readBody(request);
        if (!permitted(member, body.channel_id)) return reply(response, 403, {});
        const key = `${member.id}:${body.operation_id}`;
        if (mutations.has(key)) return reply(response, 200, mutations.get(key));
        const state = states.get(body.message_id);
        assert.equal(state.channel_id, body.channel_id); assert.equal(body.mutation.kind, 'set_reaction');
        assert.equal(body.mutation.present, true);
        state.reactions[body.mutation.emoji] = [...new Set([...(state.reactions[body.mutation.emoji] ?? []), member.id])].sort();
        state.reaction_revision++; state.updated_at = ++timestamp;
        append(state, 'reaction', member.id);
        const receipt = { schema: 1, operation_id: body.operation_id, actor_id: member.id,
          request_digest: '0'.repeat(64), state: structuredClone(state) };
        mutations.set(key, receipt); return reply(response, 200, receipt);
      }
      const channelId = url.searchParams.get('channel_id');
      if (!permitted(member, channelId)) return reply(response, 403, {});
      if (url.pathname === '/api/messages/sync') {
        assert.equal(request.headers['x-zlack-message-state'], 'v2');
        const thread = url.searchParams.get('thread_root_id');
        return reply(response, 200, { schema: 2, org_uuid: org, channel_id: channelId, thread_root_id: thread,
          messages: [...states.values()].filter(state => state.channel_id === channelId && state.thread_root_id === thread),
          next_cursor: null });
      }
      if (url.pathname === '/api/events/sync') {
        const after = url.searchParams.get('after');
        const through = url.searchParams.get('through') ?? cursor(channelId, stream(channelId).length);
        return reply(response, 200, { schema: 1, org_uuid: org, channel_id: channelId, epoch,
          events: stream(channelId).slice(position(after), position(through)), after_cursor: after,
          through_cursor: through, through_sequence: String(position(through)), next_cursor: null });
      }
      return reply(response, 404, {});
    } catch (error) { errors.push(error); if (!response.headersSent) reply(response, 500, {}); else response.end(); }
  });
  const ready = (channelId, subscriptionId) => ({ type: 'ready', schema: 1, org_uuid: org,
    channel_id: channelId, epoch, sequence: String(stream(channelId).length),
    cursor: cursor(channelId, stream(channelId).length), earliest_cursor: cursor(channelId, 0),
    ...(subscriptionId ? { subscription_id: subscriptionId } : {}) });
  server.on('upgrade', (request, socket, head) => {
    try {
      const url = new URL(request.url, 'http://127.0.0.1');
      assert.equal(request.headers['x-humchat-sdk-key'], undefined);
      assert.equal(url.searchParams.has('token'), false); assert.equal(url.searchParams.has('sdk_key'), false);
      const member = tokens.get(request.headers.authorization?.replace(/^Bearer /, ''));
      assert.ok(member, 'Only a minted Bearer opens an SDK socket');
      assert.equal(url.pathname, transport === 'multiplex' ? '/ws/events' : '/ws/chat');
      wss.handleUpgrade(request, socket, head, connected => {
        const entry = { member, socket: connected, subscriptions: new Map() }; sockets.push(entry);
        connected.on('error', () => {});
        if (transport === 'channel') {
          const channelId = url.searchParams.get('channel_id'); assert.ok(permitted(member, channelId));
          entry.subscriptions.set('', channelId); connected.send(JSON.stringify(ready(channelId)));
        } else {
          connected.send(JSON.stringify({ type: 'stream_ready', schema: 1, org_uuid: org, max_channels: 100 }));
          connected.on('message', bytes => {
            try {
              const frame = JSON.parse(bytes.toString());
              if (frame.type === 'subscribe') {
                assert.ok(permitted(member, frame.channel_id));
                entry.subscriptions.set(frame.subscription_id, frame.channel_id);
                connected.send(JSON.stringify(ready(frame.channel_id, frame.subscription_id)));
              } else if (frame.type === 'unsubscribe') {
                entry.subscriptions.delete(frame.subscription_id);
                connected.send(JSON.stringify({ type: 'unsubscribed', schema: 1, subscription_id: frame.subscription_id }));
              }
            } catch (error) { errors.push(error); connected.terminate(); }
          });
        }
      });
    } catch (error) { errors.push(error); socket.destroy(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const origin = `http://127.0.0.1:${server.address().port}`;
  t.after(async () => {
    await Promise.all(clients.map(client => client.close()));
    for (const entry of sockets) entry.socket.terminate();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(errors, [], 'Protocol-peer assertion failures must not masquerade as SDK failures');
  });
  return { requests, mints, sockets, stream, publish, states, conversations,
    origin,
    scope(member, channelId) { return { serverOrigin: origin, orgId: org, actorId: member.id, consumerId, channelId }; },
    client(member) {
      const client = new HumChat({ orgId: org, baseUrl: origin, auth: { sdkKey, actAs: member.email },
        timeout: 1000, retry: { maxRetries: 0, totalTimeoutMs: 3000 },
        realtime: { transport, maxChannels: 5, connectConcurrency: 2, handlerTimeoutMs: 2000,
          reconnectInitialDelayMs: 5, reconnectMaxDelayMs: 20, pingIntervalMs: 1000, pongTimeoutMs: 500 } });
      clients.push(client); return client;
    },
    async appRead(member, channelId, threadRootId) {
      // Independent app-like REST consumer: a normal user token, no SDK instance.
      const token = `synthetic-app-session-${member.name}`; tokens.set(token, member);
      const query = new URLSearchParams({ channel_id: channelId });
      if (threadRootId) query.set('thread_root_id', threadRootId);
      const response = await fetch(`${origin}/api/messages/sync?${query}`, {
        headers: { Authorization: `Bearer ${token}`, 'X-Zlack-Message-State': 'v2' },
      });
      return { status: response.status, body: await response.json() };
    },
    async appSend(member, channelId, id, content) {
      const token = `synthetic-app-session-${member.name}`; tokens.set(token, member);
      const response = await fetch(`${origin}/api/messages/send`, { method: 'POST',
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json',
          'X-Idempotency-Key': id, 'X-Zlack-Message-Receipt': 'v1' },
        body: JSON.stringify({ id, channel_id: channelId, content, thread_root_id: null, priority: 'standard' }),
      });
      return { status: response.status, body: await response.json() };
    },
  };
}


export { org, sdkKey, members, alice, bob, carol, epoch, consumerId, channel, cursor, until, peer };
