// Set HUMCHAT_BASE_URL, HUMCHAT_ORG_ID, HUMCHAT_TOKEN and HUMCHAT_CHANNEL_ID.
// This example opts into best-effort recovery for older servers.
import { createHash } from 'node:crypto';
import { HumChat } from '@humchat/node';
import { required } from './config.mjs';

const humchat = new HumChat({
  baseUrl: required('HUMCHAT_BASE_URL'),
  orgId: required('HUMCHAT_ORG_ID'),
  tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }),
  recovery: { mode: 'bestEffort' },
});

let subscription;
let stopping = false;
async function shutdown() {
  if (stopping) return;
  stopping = true;
  await subscription?.close();
  await humchat.close();
}
for (const signal of ['SIGINT', 'SIGTERM']) process.once(signal, () => { void shutdown(); });

try {
  const identity = await humchat.connect();
  subscription = humchat.realtime.subscribe(required('HUMCHAT_CHANNEL_ID'));
  subscription.on('error', error => console.error('Subscription failed:', error.code));
  subscription.on('handlerError', () => console.error('A message handler failed'));
  subscription.on('recoveryIncomplete', () => console.warn('Recovery is incomplete; commands sent during a gap may be missed.'));
  subscription.on('sessionRevoked', () => { console.error('Session revoked; stopping.'); void shutdown(); });
  subscription.on('message', async ({ message, source, replayed }) => {
    if (message.senderId === identity.actorId || source !== 'live' || replayed) return;
    const body = message.isLong
      ? (await humchat.messages.getState({ channelId: message.channelId, messageId: message.id })).body
      : message.content;
    if (body.trim() !== '!status') return;
    // Stable intent avoids a duplicate reply if this same incoming command is observed again.
    // It does not provide a durable inbox or guarantee commands received during downtime.
    const key = 'status-' + createHash('sha256').update(JSON.stringify([message.orgId, message.channelId, message.id])).digest('hex');
    await humchat.messages.reply(message, { id: key, content: 'All systems operational.' }, { idempotencyKey: key });
  });
  await subscription.ready;
  console.log('Listening for !status commands in the selected channel.');
} catch (error) {
  console.error('Responder stopped:', error.code ?? 'UNEXPECTED');
  process.exitCode = 1;
  await shutdown();
}
