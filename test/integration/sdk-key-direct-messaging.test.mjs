import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket } from 'ws';
import { MemoryCheckpointStore } from '../../dist/index.js';
import { members, alice, bob, carol, consumerId, channel, cursor, until, peer } from '../fixtures/sdk-key-direct-messaging.mjs';

for (const transport of ['channel', 'multiplex']) {
  test(`${transport}: native SDK-key DMs discover the first message, reply, react, and resume without echoing`, { timeout: 15000 }, async t => {
    const fixture = await peer(t, transport);
    const sender = fixture.client(alice), receiver = fixture.client(bob);
    const senderStore = new MemoryCheckpointStore(), receiverStore = new MemoryCheckpointStore();
    t.after(async () => { await senderStore.close(); await receiverStore.close(); });
    const received = [], observedBySender = [], senderInbound = [], ownMessages = [], replies = [];
    const inbox = receiver.inbox.watch(async event => {
      received.push(`${event.kind}:${event.state.id}`);
      if (event.kind !== 'message') return;
      if (event.state.senderId === bob.id) { ownMessages.push(event.state.id); return; }
      if (event.state.body !== '!status') return;
      replies.push(await receiver.messages.reply(event.state, { id: 'synthetic-reply', content: 'Ready.' }));
      await receiver.messages.setReaction({ channelId: event.channelId, messageId: event.state.id,
        operationId: 'synthetic-reaction', emoji: '✅', present: true });
    }, { consumerId, checkpointStore: receiverStore, pollIntervalMs: 10000 });
    await inbox.ready;
    assert.equal(inbox.getStats().conversations, 0);
    assert.equal(fixture.sockets.length, 0, 'There is no recipient socket before the first conversation exists');
    const roster = await sender.members.list();
    const target = roster.find(member => member.email === bob.email);
    assert.equal(target.actorId, bob.id);
    const sent = await sender.messages.sendDirect({ recipientId: target.actorId,
      content: '!status', id: 'synthetic-first-message' }, { idempotencyKey: 'synthetic-first-intent' });
    const channelId = sent.conversation.channelId;
    assert.equal(sent.receipt.status, 'accepted'); assert.equal(channelId, channel(sent.conversation.id));
    assert.deepEqual(fixture.stream(channelId).map(event => event.state.sender_id), [alice.id]);
    const senderInbox = sender.inbox.watch(event => {
      observedBySender.push(`${event.kind}:${event.state.id}`);
      if (event.kind === 'message' && event.state.senderId === bob.id) senderInbound.push(event.state);
    },
      { consumerId, checkpointStore: senderStore, pollIntervalMs: 10000 });
    await senderInbox.ready;
    await inbox.refresh();
    await until(async () => await receiverStore.load(fixture.scope(bob, channelId)) === cursor(channelId, 3) &&
      await senderStore.load(fixture.scope(alice, channelId)) === cursor(channelId, 3), 'both native clients commit the exchange');
    const expected = ['message:synthetic-first-message', 'message:synthetic-reply', 'reaction:synthetic-first-message'];
    assert.deepEqual(received, expected); assert.deepEqual(observedBySender, expected);
    assert.deepEqual(ownMessages, ['synthetic-reply']); assert.equal(replies.length, 1, 'Handler ignores its own reply');
    assert.equal(fixture.states.get('synthetic-reply').thread_root_id, 'synthetic-first-message');
    assert.deepEqual(fixture.states.get('synthetic-first-message').reactions, { '✅': [bob.id] });
    assert.equal(fixture.conversations.size, 1);
    assert.deepEqual(fixture.mints.sort(), [alice.id, bob.id].sort(), 'Each actor has a cached independent mint');

    // The server may replay an accepted request or repeat a live hint. Neither
    // creates a second message or calls an already-acknowledged handler again.
    await sender.messages.sendDirect({ recipientId: bob.id, content: '!status', id: 'synthetic-first-message' },
      { idempotencyKey: 'synthetic-first-intent' });
    for (const event of fixture.stream(channelId)) fixture.publish(event);
    await sleep(50);
    assert.equal(fixture.stream(channelId).length, 3); assert.deepEqual(received, expected);
    assert.equal(replies.length, 1);
    assert.equal(fixture.requests.filter(item => item.path === '/api/conversations' && item.method === 'POST').length, 1);

    const appRoot = await fixture.appRead(bob, channelId);
    assert.equal(appRoot.status, 200);
    assert.deepEqual(appRoot.body.messages.map(message => [message.message_id, message.sender_id, message.body]),
      [['synthetic-first-message', alice.id, '!status']]);
    assert.deepEqual(appRoot.body.messages[0].reactions, { '✅': [bob.id] });
    const appThread = await fixture.appRead(alice, channelId, 'synthetic-first-message');
    assert.deepEqual(appThread.body.messages.map(message => [message.message_id, message.sender_id, message.thread_root_id]),
      [['synthetic-reply', bob.id, 'synthetic-first-message']]);
    const history = await receiver.messages.history({ channelId, threadRootId: 'synthetic-first-message' });
    assert.equal(history.messages[0].body, 'Ready.');

    await receiver.close(); await inbox.done;
    await sender.messages.sendDirect({ recipientId: bob.id, id: 'synthetic-offline-message', content: 'While you were offline.' });
    assert.equal((await fixture.appRead(bob, channelId)).body.messages.length, 2, 'App-like REST can read the message while the SDK is offline');
    const resumed = fixture.client(bob), replayed = [];
    const next = resumed.inbox.watch(event => { replayed.push(event.state.id); },
      { consumerId, checkpointStore: receiverStore, pollIntervalMs: 10000 });
    await next.ready;
    await until(async () => await receiverStore.load(fixture.scope(bob, channelId)) === cursor(channelId, 4), 'restart catches up from the committed position');
    assert.deepEqual(replayed, ['synthetic-offline-message']);
    assert.ok(fixture.requests.some(item => item.path === '/api/events/sync' && item.actorId === bob.id &&
      item.url.searchParams.get('after') === cursor(channelId, 3)), 'Restart requests the prior durable checkpoint');
    assert.equal(await receiverStore.load(fixture.scope(carol, channelId)), null, 'Checkpoints are actor-scoped');
    const ordinarySessionSend = await fixture.appSend(bob, channelId, 'synthetic-from-app', 'Sent with a normal user session.');
    assert.equal(ordinarySessionSend.status, 200);
    await until(async () => await senderStore.load(fixture.scope(alice, channelId)) === cursor(channelId, 5),
      'SDK receives a message sent by a separate app-like REST client');
    assert.deepEqual(senderInbound.map(message => [message.id, message.senderId]),
      [['synthetic-reply', bob.id], ['synthetic-from-app', bob.id]]);
    await Promise.all([sender.close(), resumed.close()]);
    await until(() => fixture.sockets.every(entry => entry.socket.readyState === WebSocket.CLOSED), 'all managed inbox sockets close');
    assert.equal(sender.realtime.getStats().activeSockets, 0); assert.equal(resumed.realtime.getStats().activeSockets, 0);
  });

  test(`${transport}: native direct messaging reaches either same-org member while rejecting foreign recipients and private DM reads`, { timeout: 10000 }, async t => {
    const fixture = await peer(t, transport), sender = fixture.client(alice), unrelated = fixture.client(carol);
    const roster = await sender.members.list();
    assert.deepEqual(roster.map(member => member.actorId).sort(), members.map(member => member.id).sort());
    const receipts = [];
    for (const recipient of roster.filter(member => member.actorId !== alice.id)) {
      receipts.push(await sender.messages.sendDirect({ recipientId: recipient.actorId,
        id: `synthetic-for-${recipient.actorId}`, content: `Hello ${recipient.name}.` }));
    }
    assert.equal(new Set(receipts.map(receipt => receipt.conversation.channelId)).size, 2);
    const bobChannel = receipts.find(receipt => receipt.conversation.members.some(member => member.actorId === bob.id)).conversation.channelId;
    assert.equal((await fixture.appRead(bob, bobChannel)).status, 200);
    assert.equal((await fixture.appRead(carol, bobChannel)).status, 403);
    await assert.rejects(unrelated.messages.history({ channelId: bobChannel }), { code: 'PERMISSION' });
    assert.equal((await unrelated.conversations.list()).length, 1);
    const sendCount = fixture.requests.filter(item => item.path === '/api/messages/send').length;
    await assert.rejects(sender.messages.sendDirect({ recipientId: 'synthetic-foreign-member', content: 'Must not send.' }), { code: 'PERMISSION' });
    assert.equal(fixture.requests.filter(item => item.path === '/api/messages/send').length, sendCount,
      'An unauthorized recipient is rejected before sending message content');
    assert.equal(fixture.conversations.size, 2);
  });
}
