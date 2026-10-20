// Shared implementation for the single-channel and multi-channel durable examples.
import { createHash } from 'node:crypto';
import { join } from 'node:path';
import { HumChat, FileCheckpointStore, FileOutboxStore } from '@humchat/node';
import { required } from './config.mjs';

export async function runDurableWorker({ channelIds, consumerId, transport = 'channel' }) {
  const channels = [...new Set(channelIds.map(value => value.trim()))];
  if (!channels.length || channels.some(value => !value) || channels.length > 50) throw new Error('Supply between 1 and 50 channel IDs');
  const start = process.env.HUMCHAT_START ?? 'now';
  if (!['now', 'earliest'].includes(start)) throw new Error('HUMCHAT_START must be now or earliest');
  const runFor = process.env.HUMCHAT_RUN_FOR_MS === undefined ? undefined : Number(process.env.HUMCHAT_RUN_FOR_MS);
  if (runFor !== undefined && (!Number.isSafeInteger(runFor) || runFor < 1 || runFor > 2_147_483_647)) throw new Error('Invalid HUMCHAT_RUN_FOR_MS');
  const directory = required('HUMCHAT_STATE_DIR');
  const humchat = new HumChat({ baseUrl: required('HUMCHAT_BASE_URL'), orgId: required('HUMCHAT_ORG_ID'),
    // Replace this static provider with the application's renewal flow for unattended use.
    tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }), recovery: { mode: 'durable' },
    realtime: { transport, maxChannels: channels.length, connectConcurrency: Math.min(5, channels.length) } });
  const cancellation = new AbortController();
  const activeHandlers = new Set();
  let checkpoints, outbox, stopping, timer, startup;
  let finished;
  const done = new Promise(resolve => { finished = resolve; });
  const onSignal = () => { void shutdown().catch(() => { process.exitCode = 1; }); };
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, onSignal);

  function shutdown() {
    if (stopping) return stopping;
    cancellation.abort();
    stopping = (async () => {
      clearTimeout(timer);
      await humchat.close();
      // Startup can still be acquiring a store. Let it settle before releasing every lock.
      await startup?.catch(() => {});
      await Promise.allSettled([...activeHandlers]);
      const closed = await Promise.allSettled([checkpoints?.close(), outbox?.close()]);
      for (const signal of ['SIGINT', 'SIGTERM']) process.off(signal, onSignal);
      if (closed.some(result => result.status === 'rejected')) { console.error('Store shutdown failed: PERSISTENCE'); process.exitCode = 1; }
      console.log(JSON.stringify({ event: 'stopped' }));
      finished();
    })();
    return stopping;
  }

  startup = (async () => {
    checkpoints = await FileCheckpointStore.open(join(directory, 'checkpoints.json'));
    cancellation.signal.throwIfAborted();
    outbox = await FileOutboxStore.open(join(directory, 'outbox.json'));
    cancellation.signal.throwIfAborted();
    const identity = await humchat.connect({ signal: cancellation.signal });
    for (const channelId of channels) await humchat.outbox.flush({ consumerId, channelId, store: outbox }, { signal: cancellation.signal });
    cancellation.signal.throwIfAborted();
    const subscriptions = channels.map(channelId => {
      const subscription = humchat.realtime.subscribe(channelId, { start });
      subscription.on('error', error => {
        console.error('Subscription error:', error.code);
        if (!error.retryable) { process.exitCode = 1; onSignal(); }
      });
      subscription.consume((event, { signal }) => {
        const combined = AbortSignal.any([signal, cancellation.signal]);
        const work = (async () => {
          if (event.kind !== 'message' || event.state.senderId === identity.actorId || event.state.body.trim() !== '!status') return;
          const key = 'status-' + createHash('sha256').update(JSON.stringify([event.orgId, event.channelId, event.eventId])).digest('hex');
          const receipt = await humchat.outbox.send({ consumerId, key, store: outbox, message: {
            channelId: event.channelId, threadRootId: event.state.threadRootId ?? event.state.id, content: 'All systems operational.',
          } }, { signal: combined });
          console.log(JSON.stringify({ event: 'reply', channelId: event.channelId, messageId: receipt.id }));
          // Returning acknowledges this input only after its reply receipt has been persisted.
        })();
        activeHandlers.add(work);
        void work.finally(() => activeHandlers.delete(work)).catch(() => {});
        return work;
      }, { consumerId, checkpointStore: checkpoints });
      return subscription;
    });
    await Promise.all(subscriptions.map(subscription => subscription.ready));
    if (!cancellation.signal.aborted) {
      console.log(JSON.stringify({ event: 'ready', channels: channels.length, consumerId }));
      if (runFor !== undefined) timer = setTimeout(onSignal, runFor);
    }
  })();
  try { await startup; await done; }
  catch (error) {
    if (!cancellation.signal.aborted) { console.error('Worker stopped:', error.code ?? 'CONFIGURATION_OR_UNEXPECTED'); process.exitCode = 1; }
    await shutdown();
  }
}
