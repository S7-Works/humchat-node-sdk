import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { HumChat, ProtocolError, ValidationError } from '../../dist/index.js';

const samples = JSON.parse(readFileSync(new URL('../../contract/fixtures/http-ws.json', import.meta.url), 'utf8')).samples;
const orgId = samples.identity.org;
const channelId = samples.sendRequest.channel_id;
const state = samples.state;
const caps = {
  sdk_protocol_version: 1,
  authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
  messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: false, websocket_scope: 'channel' },
};
function client(handler, identity = samples.identity) {
  return new HumChat({ orgId, baseUrl: 'https://sdk.example.test', tokenProvider: async () => ({ token: 'fixture-token' }),
    retry: { maxRetries: 0 }, fetch: async (url, init) => {
      const parsed = new URL(url);
      const value = parsed.pathname === '/me' ? identity : parsed.pathname === '/api/sdk/capabilities' ? caps : await handler(parsed, init);
      return new Response(JSON.stringify(value), {status: 200, headers: {'Content-Type': 'application/json'}});
    } });
}
function cursor(message, thread = null) {
  return Buffer.from(JSON.stringify({ schema: 2, org_uuid: orgId, channel_id: channelId, thread_root_id: thread,
    created_at: message.created_at, message_id: message.message_id })).toString('hex');
}

test('captured channels, DMs and versioned state normalize into public camelCase fields', async () => {
  const sdk = client(url => {
    if (url.pathname === '/api/workspace/channels') return { ...samples.channels, future_field: true };
    if (url.pathname === '/api/conversations') return [samples.conversation];
    return { ...state, future_field: { added: true } };
  });
  const listed = await sdk.channels.list({teamId: samples.channels.team_uuid});
  assert.equal(listed.channels[0].id, samples.channels.channels[0].channel_id);
  assert.equal(listed.teamId, samples.channels.team_uuid);
  const [dm] = await sdk.conversations.list();
  assert.equal(dm.channelId, `${orgId}:conv:${dm.id}`);
  assert.notEqual(dm.id, dm.channelId);
  const message = await sdk.messages.getState({channelId, messageId: state.message_id});
  assert.equal(message.body, state.body);
  assert.equal(message.originalBody, state.original_body);
  assert.equal(message.reactionRevision, state.reaction_revision);
  assert.equal(message.createdAt, state.created_at);
  assert.equal(message.deleted, true);
  await sdk.close();
});

for (const [description, modify] of [
  ['different organization', v => { v.org_uuid = 'other-org'; }],
  ['different channel', v => { v.channel_id = `${orgId}:other`; }],
  ['different message', v => { v.message_id = 'other-message'; }],
  ['unknown schema', v => { v.schema = 7; }],
  ['unsafe revision', v => { v.revision = Number.MAX_SAFE_INTEGER + 1; }],
  ['unsafe reaction revision', v => { v.reaction_revision = Number.MAX_SAFE_INTEGER + 1; }],
  ['self-referential thread', v => { v.thread_root_id = v.message_id; }],
  ['duplicate reaction actors', v => { v.reactions = { yes: ['a', 'a'] }; }],
  ['initial revision with changed content', v => { v.revision = 1; v.deleted = false; }],
]) {
  test(`rejects state with ${description}`, async () => {
    const invalid = structuredClone(state);
    modify(invalid);
    const sdk = client(() => invalid);
    await assert.rejects(sdk.messages.getState({channelId, messageId: state.message_id}), ProtocolError);
    await sdk.close();
  });
}

test('channel list and DM response cannot cross organization boundaries', async () => {
  const sdk = client(url => url.pathname === '/api/conversations'
    ? [{...samples.conversation, channel_key: `other-org:conv:${samples.conversation.id}`}]
    : {...samples.channels, channels: [{...samples.channels.channels[0], org_uuid: 'other-org'}]});
  await assert.rejects(sdk.channels.list(), ProtocolError);
  await assert.rejects(sdk.conversations.list(), ProtocolError);
  await sdk.close();
});

test('send allocates ULID and idempotency once; reply preserves an existing thread root', async () => {
  const requests = [];
  const sdk = client((_url, init) => {
    const body = JSON.parse(init.body);
    requests.push({body, headers: new Headers(init.headers)});
    return {...samples.sendReceipt, message_id: body.id};
  });
  const sent = await sdk.messages.send({channelId, content: 'hello'});
  assert.match(sent.id, /^[0-9A-HJKMNP-TV-Z]{26}$/);
  assert.equal(sent.idempotencyKey, sent.id);
  assert.equal(requests[0].headers.get('x-idempotency-key'), sent.id);
  assert.equal(requests[0].headers.get('x-zlack-message-receipt'), 'v1');
  assert.equal(requests[0].body.priority, 'standard');
  assert.equal(requests[0].body.thread_root_id, null);
  await sdk.messages.reply({id: 'existing-reply', channelId, threadRootId: 'root'}, {content: 'reply', id: 'known-id', priority: 'important'}, {idempotencyKey: 'stable-key'});
  assert.equal(requests[1].body.thread_root_id, 'root');
  assert.equal(requests[1].body.id, 'known-id');
  assert.equal(requests[1].body.priority, 'important');
  assert.equal(requests[1].headers.get('x-idempotency-key'), 'stable-key');
  await sdk.close();
});

test('malformed send acknowledgments retain generated intent for reconciliation', async () => {
  const sdk = client(() => 'ACK');
  await assert.rejects(sdk.messages.send({channelId, content: 'hello'}), error => {
    assert.ok(error instanceof ProtocolError);
    assert.match(error.messageId, /^[0-9A-HJKMNP-TV-Z]{26}$/);
    assert.equal(error.idempotencyKey, error.messageId);
    assert.equal(error.channelId, channelId);
    return true;
  });
  await sdk.close();
});

test('invalid local content, revisions, reaction size and foreign routing fail before IO', async () => {
  let calls = 0;
  const sdk = client(() => { calls++; throw new Error('unexpected request'); });
  await assert.rejects(sdk.messages.send({channelId, content: '  '}), ValidationError);
  await assert.rejects(sdk.messages.send({channelId, content: 'a'.repeat(1_048_577)}), ValidationError);
  await assert.rejects(sdk.messages.send({channelId: 'other-org:channel', content: 'hello'}), ValidationError);
  await assert.rejects(sdk.messages.edit({channelId, messageId: 'id', content: 'x', expectedRevision: 1.5}), ValidationError);
  await assert.rejects(sdk.messages.setReaction({channelId, messageId: 'id', emoji: '😀'.repeat(9), present: true}), ValidationError);
  await assert.rejects(sdk.messages.history({channelId, limit: 201}), ValidationError);
  assert.equal(calls, 0);
  await sdk.close();
});

test('reaction mutation uses strict wire fields and validates receipt actor and reflected intent', async () => {
  const fixture = samples.mutationReceipt;
  let wrongActor = false;
  let wrongIntent = false;
  const sdk = client((_url, init) => {
    const body = JSON.parse(init.body);
    assert.deepEqual(Object.keys(body), ['operation_id', 'channel_id', 'message_id', 'mutation']);
    assert.deepEqual(body.mutation, {kind: 'set_reaction', emoji: '✅', present: true});
    return {...fixture, operation_id: body.operation_id, actor_id: wrongActor ? 'another-actor' : fixture.actor_id,
      state: wrongIntent ? {...fixture.state, reactions: {}} : fixture.state};
  }, {...samples.identity, user_uuid: fixture.actor_id});
  const params = {channelId, messageId: fixture.state.message_id, emoji: '✅', present: true};
  const receipt = await sdk.messages.setReaction(params);
  assert.equal(receipt.actorId, fixture.actor_id);
  assert.match(receipt.operationId, /^[0-9A-HJKMNP-TV-Z]{26}$/);
  wrongActor = true;
  await assert.rejects(sdk.messages.setReaction(params), ProtocolError);
  wrongActor = false;
  wrongIntent = true;
  await assert.rejects(sdk.messages.setReaction(params), ProtocolError);
  await sdk.close();
});

test('history iterates older pages while retaining ascending row order and scoped cursor', async () => {
  const base = samples.history.messages[0];
  const older = {...base, message_id: 'older', created_at: base.created_at - 1, updated_at: base.created_at - 1};
  const next = cursor(base);
  let calls = 0;
  const sdk = client((url, init) => {
    assert.equal(new Headers(init.headers).get('x-zlack-message-state'), 'v2');
    assert.equal(url.searchParams.get('channel_id'), channelId);
    assert.equal(url.searchParams.has('since_id'), false);
    calls++;
    if (calls === 1) return {...samples.history, messages: [base], next_cursor: next};
    assert.equal(url.searchParams.get('native_cursor'), next);
    return {...samples.history, messages: [older], next_cursor: null};
  });
  const pages = [];
  for await (const page of sdk.messages.iterateHistory({channelId, limit: 1})) pages.push(page);
  assert.deepEqual(pages.map(p => p.messages[0].id), [base.message_id, 'older']);
  assert.equal(calls, 2);
  await sdk.close();
});

test('history rejects wrong thread scope, duplicate rows and fabricated cursor boundaries', async () => {
  const base = samples.history.messages[0];
  let invalid = {...samples.history, messages: [base, base]};
  const sdk = client(() => invalid);
  await assert.rejects(sdk.messages.history({channelId}), ProtocolError);
  invalid = {...samples.threadHistory};
  await assert.rejects(sdk.messages.history({channelId}), ProtocolError);
  invalid = {...samples.history, next_cursor: cursor({...base, message_id: 'skipped'})};
  await assert.rejects(sdk.messages.history({channelId}), ProtocolError);
  invalid = {...samples.history, next_cursor: cursor(base)};
  await assert.rejects(async () => { for await (const _ of sdk.messages.iterateHistory({channelId})) { /* exhaust */ } }, ProtocolError);
  await assert.rejects(sdk.messages.history({channelId, threadRootId: 'wrong-thread', cursor: cursor(base)}), ValidationError);
  await sdk.close();
});
