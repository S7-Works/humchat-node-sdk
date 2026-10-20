import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { build } from 'esbuild';

// Test internal invariants without making parser/cache implementation part of npm's public API.
const built = await build({ stdin: {contents: `
  export {parseFrame, messageFromState} from './src/realtime/protocol.ts';
  export {StateCache} from './src/realtime/state-cache.ts';
  export {parseState} from './src/protocol/wire.ts';
  export {ProtocolError, BackpressureError} from './src/errors.ts';`,
  resolveDir: fileURLToPath(new URL('../../', import.meta.url)), loader: 'ts'},
  bundle: true, write: false, platform: 'node', format: 'esm', target: 'node22', packages: 'bundle' });
const {parseFrame, messageFromState, StateCache, parseState, ProtocolError, BackpressureError} =
  await import(`data:text/javascript;base64,${Buffer.from(built.outputFiles[0].text).toString('base64')}`);
const f = JSON.parse(readFileSync(new URL('../../contract/fixtures/http-ws.json', import.meta.url), 'utf8')).samples;
const orgId = f.identity.org;
const channelId = f.message.channel_id;
const scoped = {org_uuid: orgId, channel_id: channelId};
const base = () => parseState(structuredClone(f.history.messages[0]), orgId, channelId);
const cache = () => new StateCache({maxEntries: 1000, maxBytes: 8 * 1024 * 1024});

test('captured creation and all state-bearing mutation frames normalize once', () => {
  const created = parseFrame({...f.message, additive: true}, orgId, channelId);
  assert.equal(created.kind, 'message');
  assert.equal(created.message.content, '!status');
  assert.equal(created.message.threadRootId, null);
  assert.equal(created.message.isLong, false);
  for (const [name, event] of [['edit', 'messageUpdated'], ['delete', 'messageDeleted'], ['reaction', 'reactionUpdated']]) {
    const frame = parseFrame(f[name], orgId, channelId);
    assert.equal(frame.kind, 'state');
    assert.equal(frame.event, event);
    assert.equal(frame.state.id, f[name].message_id);
  }
});

test('legacy edit/delete/reaction are hydration hints and malformed embedded state never downgrades', () => {
  for (const name of ['edit', 'delete', 'reaction']) {
    const legacy = structuredClone(f[name]);
    delete legacy.state;
    assert.equal(parseFrame(legacy, orgId, channelId).kind, 'legacyMutation');
    for (const state of [null, {}, {...f[name].state, schema: 2}, {...f[name].state, revision: Number.MAX_SAFE_INTEGER + 1}]) {
      assert.throws(() => parseFrame({...legacy, state}, orgId, channelId), ProtocolError);
    }
  }
});

test('all actual ephemeral and control frame shapes normalize with bound socket scope', () => {
  assert.equal(parseFrame(f.typing, orgId, channelId).payload.ttlMs, 6000);
  assert.equal(parseFrame(f.receipt, orgId, channelId).payload.through, f.receipt.ts);
  const presence = parseFrame({type: 'presence', ...scoped, user_uuid: 'actor', status: 'away', last_seen: 100}, orgId, channelId);
  assert.deepEqual(presence.payload, {orgId, channelId, actorId: 'actor', status: 'away', lastSeen: 100});
  const call = parseFrame({type: 'call_invite', ...scoped, meeting_id: 'meeting', inviter_id: 'actor', invitee_id: 'bot', created_at: 100}, orgId, channelId);
  assert.equal(call.kind, 'callInvite');
  assert.equal(call.payload.inviteeId, 'bot');
  assert.deepEqual(parseFrame({type: 'resync', channel_id: channelId}, orgId, channelId), {kind: 'resync'});
  for (const reason of ['session expired', 'session deprovisioned', 'session no longer valid']) {
    assert.deepEqual(parseFrame({type: 'session_revoked', reason}, orgId, channelId), {kind: 'sessionRevoked', reason});
  }
  assert.equal(parseFrame({type: 'session_revoked', reason: 'untrusted secret details'}, orgId, channelId).reason, 'session revoked');
});

test('every scoped frame rejects contradictory tenant/channel identities', () => {
  for (const frame of [f.message, f.typing, f.receipt, f.edit, f.delete, f.reaction,
    {type: 'resync', channel_id: channelId}, {type: 'session_revoked', reason: 'session deprovisioned'},
    {type: 'future_signal'}]) {
    assert.throws(() => parseFrame({...frame, org_uuid: 'foreign'}, orgId, channelId), ProtocolError);
    assert.throws(() => parseFrame({...frame, channel_id: `${orgId}:foreign`}, orgId, channelId), ProtocolError);
  }
  assert.throws(() => parseFrame({...f.edit, state: {...f.edit.state, message_id: 'foreign'}}, orgId, channelId), ProtocolError);
});

test('unknown event metadata is bounded; prototype names are never mutation kinds', () => {
  for (const type of ['constructor', '__proto__', 'toString', 'future_signal']) {
    const frame = parseFrame({type, secret: 'never exposed'}, orgId, channelId);
    assert.equal(frame.kind, 'unknown');
    assert.deepEqual(Object.keys(frame), ['kind', 'type']);
  }
  assert.equal(parseFrame({type: 'x'.repeat(10000)}, orgId, channelId).type, 'unknown');
  assert.throws(() => parseFrame({...f.message, type: null}, orgId, channelId), ProtocolError);
  assert.throws(() => parseFrame([], orgId, channelId), ProtocolError);
});

test('creation attachments preserve canonical data and optional asset URLs', () => {
  const attachment = {upload_id: 'upload', content_hash: 'a'.repeat(64), file_name: 'image.png', media_type: 'image/png', byte_len: 32,
    url: 'https://assets.example/image', thumb_url: 'https://assets.example/thumb'};
  const parsed = parseFrame({...f.message, attachments: [attachment]}, orgId, channelId).message;
  assert.equal(parsed.attachments[0].ordinal, 0);
  assert.equal(parsed.attachments[0].byteLength, 32);
  assert.equal(parsed.attachments[0].thumbnailUrl, attachment.thumb_url);
  assert.throws(() => parseFrame({...f.message, attachments: [{...attachment, byte_len: Number.MAX_SAFE_INTEGER + 1}]}, orgId, channelId), ProtocolError);
  const legacy = parseFrame({...f.message, attachment_hash: attachment.content_hash, attachment_name: attachment.file_name}, orgId, channelId).message;
  assert.equal(legacy.attachments[0].contentHash, attachment.content_hash);
  assert.equal(legacy.attachments[0].byteLength, null);
  const legacyVector = parseFrame({...f.message, attachments: [{upload_id: 'legacy:0', content_hash: attachment.content_hash,
    file_name: '', media_type: 'application/octet-stream', byte_len: -1}]}, orgId, channelId).message;
  assert.equal(legacyVector.attachments[0].fileName, null);
  assert.equal(legacyVector.attachments[0].byteLength, null);
  assert.throws(() => parseFrame({...f.message, attachments: [{...attachment, byte_len: -2}]}, orgId, channelId), ProtocolError);
  const singular = parseFrame({...f.message, attachment_hash: attachment.content_hash}, orgId, channelId).message;
  assert.deepEqual(singular.attachments, legacyVector.attachments);
});

test('attachment identities agree across live/state frames while refreshed URLs are ignored', () => {
  const live = parseFrame({...f.message, attachments: [{upload_id: 'upload', content_hash: 'a'.repeat(64), file_name: 'image.png',
    media_type: 'image/png', byte_len: 32, url: 'https://assets.example/first'}]}, orgId, channelId).message;
  const state = {...base(), attachments: live.attachments.map(({url, thumbnailUrl, ...a}) => a)};
  const store = cache();
  assert.equal(store.observeMessage(live), true);
  store.merge(state);
  assert.equal(store.observeMessage({...live, attachments: live.attachments.map(a => ({...a, url: 'https://assets.example/refreshed'}))}), false);
  assert.throws(() => store.merge({...state, attachments: state.attachments.map(a => ({...a, contentHash: 'b'.repeat(64)}))}), ProtocolError);
  const stateFirst = cache();
  stateFirst.merge(state);
  assert.throws(() => stateFirst.observeMessage({...live, attachments: []}), ProtocolError);
});

test('state merge preserves independently newer content and reactions in both arrival orders', () => {
  const a = base();
  const edit = {...a, revision: 2, body: 'changed', updatedAt: a.updatedAt + 1};
  const reaction = {...a, reactionRevision: 1, reactions: {'✅': ['actor']}};
  for (const ordered of [[edit, reaction], [reaction, edit]]) {
    const store = cache();
    store.merge(a);
    for (const next of ordered) store.merge(next);
    const final = store.get(a.id);
    assert.equal(final.body, 'changed');
    assert.equal(final.revision, 2);
    assert.equal(final.reactionRevision, 1);
    assert.deepEqual(final.reactions, {'✅': ['actor']});
    assert.equal(store.merge(a).changed, false);
    assert.equal(store.get(a.id).body, 'changed');
  }
});

test('cache rejects immutable changes and contradictory equal revisions', () => {
  const a = base();
  const store = cache();
  store.merge(a);
  for (const change of [{senderId: 'other'}, {orgId: 'other'}, {channelId: 'other'}, {originalBody: 'other'},
    {body: 'different'}, {updatedAt: a.updatedAt + 1}, {reactions: {yes: ['actor']}}]) {
    assert.throws(() => store.merge({...a, ...change}), ProtocolError);
  }
  assert.equal(store.get(a.id).body, a.body);
});

test('creation dedup remains separate from revision-one snapshots and rejects stale resurrection', () => {
  const a = base();
  const store = cache();
  store.merge(a);
  assert.equal(store.observeMessage(messageFromState(a)), true);
  assert.equal(store.observeMessage(messageFromState(a)), false);
  assert.throws(() => store.observeMessage({...messageFromState(a), content: 'conflict'}), ProtocolError);
  const tombstones = cache();
  tombstones.merge({...a, revision: 2, deleted: true, updatedAt: a.updatedAt + 1});
  assert.equal(tombstones.observeMessage(messageFromState(a)), false);
  const edited = cache();
  edited.merge({...a, revision: 2, body: 'edit', updatedAt: a.updatedAt + 1});
  assert.equal(edited.observeMessage(messageFromState(a)), false);
});

test('recovered full original text and a later live long snippet deduplicate compatibly', () => {
  const text = '😀'.repeat(1000);
  const a = {...base(), body: text, originalBody: text};
  const full = messageFromState(a);
  const snippet = {...full, isLong: true, content: '😀'.repeat(900) + '…', bodyDigest: 'a'.repeat(64)};
  for (const [first, second] of [[full, snippet], [snippet, full]]) {
    const store = cache();
    assert.equal(store.observeMessage(first), true);
    store.merge(a);
    assert.equal(store.observeMessage(second), false);
    assert.equal(store.observeMessage(full), false);
    assert.throws(() => store.observeMessage({...snippet, content: 'wrong snippet'}), ProtocolError);
    assert.throws(() => store.observeMessage({...snippet, bodyDigest: 'b'.repeat(64)}), ProtocolError);
  }
});

test('cache copies caller data, uses LRU eviction and enforces entry/byte budgets', () => {
  const a = base();
  const store = new StateCache({maxEntries: 2, maxBytes: 10000});
  const first = store.merge(a);
  a.body = 'caller changed input';
  first.state.body = 'caller changed output';
  assert.notEqual(store.get(a.id).body, a.body);
  const fetched = store.get(a.id);
  fetched.reactions.oops = ['caller'];
  assert.equal(store.get(a.id).reactions.oops, undefined);
  store.merge({...base(), id: 'second'});
  store.get(a.id);
  store.merge({...base(), id: 'third'});
  assert.deepEqual(store.ids(), [a.id, 'third']);
  assert.equal(store.size, 2);
  assert.ok(store.bytes <= 10000);
  const tiny = new StateCache({maxEntries: 1, maxBytes: 10});
  assert.throws(() => tiny.merge(base()), BackpressureError);
  assert.equal(tiny.size, 0);
  store.clear();
  assert.equal(store.size, 0);
  assert.equal(store.bytes, 0);
});
