import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { HumChat, FileCheckpointStore, FileOutboxStore } from '@humchat/node';
import { required } from './config.mjs';

const client = new HumChat({ orgId: required('HUMCHAT_ORG_ID'), baseUrl: required('HUMCHAT_BASE_URL'),
  // Replace this static provider with your application's session-renewal implementation.
  tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }) });
const stop = new AbortController();
const active = new Set();
const onSignal = () => stop.abort();
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, onSignal);
const stopped = new Promise(resolve => stop.signal.addEventListener('abort', resolve, { once: true }));
let checkpoints, outbox, timer;
try {
  const runFor = process.env.HUMCHAT_RUN_FOR_MS === undefined ? undefined : Number(process.env.HUMCHAT_RUN_FOR_MS);
  if (runFor !== undefined && (!Number.isSafeInteger(runFor) || runFor < 1 || runFor > 2_147_483_647)) throw new Error('Invalid run duration');
  const directory = required('HUMCHAT_STATE_DIR');
  checkpoints = await FileCheckpointStore.open(join(directory, 'dm-checkpoints.json'));
  outbox = await FileOutboxStore.open(join(directory, 'dm-outbox.json'));
  const identity = await client.connect({ signal: stop.signal });
  const consumerId = process.env.HUMCHAT_CONSUMER_ID ?? 'dm-status-responder-v1';
  const recipientId = process.env.HUMCHAT_RECIPIENT_ID;
  if (recipientId) {
    const conversation = await client.conversations.openDirect({ memberId: recipientId }, { signal: stop.signal });
    // A persistent key prevents this optional greeting from being repeated on every restart.
    await client.outbox.send({ consumerId, key: 'initial-greeting', store: outbox,
      message: { channelId: conversation.channelId, content: 'Send !status here for a status reply.' } }, { signal: stop.signal });
  }
  stop.signal.throwIfAborted();
  const watcher = client.inbox.watch((event, { signal }) => {
    const work = (async () => {
      if (event.kind !== 'message' || event.state.senderId === identity.actorId || event.state.body.trim() !== '!status') return;
      const key = 'reply-' + createHash('sha256').update(JSON.stringify([event.orgId, event.channelId, event.eventId])).digest('hex');
      const receipt = await client.outbox.send({ consumerId, key, store: outbox, message: {
        channelId: event.channelId, content: 'All systems operational.', threadRootId: event.state.threadRootId ?? event.state.id,
      } }, { signal: AbortSignal.any([signal, stop.signal]) });
      console.log(JSON.stringify({ event: 'reply', channelId: event.channelId, messageId: receipt.id }));
    })();
    active.add(work); void work.finally(() => active.delete(work)).catch(() => {}); return work;
  }, { consumerId, checkpointStore: checkpoints });
  await Promise.race([watcher.ready, stopped]);
  if (!stop.signal.aborted) {
    console.log(JSON.stringify({ event: 'watching', conversations: watcher.getStats().conversations }));
    if (runFor !== undefined) timer = setTimeout(onSignal, runFor);
    await Promise.race([watcher.done, stopped]);
  }
} catch (error) {
  if (!stop.signal.aborted) { console.error('Inbox stopped:', error.code ?? 'CONFIGURATION_OR_UNEXPECTED'); process.exitCode = 1; }
} finally {
  stop.abort(); clearTimeout(timer);
  await client.close();
  await Promise.allSettled([...active]);
  const closed = await Promise.allSettled([checkpoints?.close(), outbox?.close()]);
  if (closed.some(result => result.status === 'rejected')) { console.error('Store shutdown failed: PERSISTENCE'); process.exitCode = 1; }
  for (const signal of ['SIGINT', 'SIGTERM']) process.off(signal, onSignal);
  console.log(JSON.stringify({ event: 'stopped' }));
}
