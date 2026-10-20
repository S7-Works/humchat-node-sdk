// Explicit shared-socket durable transport; requires server multiplexed-events v1.
import { runDurableWorker } from './durable-worker.mjs';
import { required } from './config.mjs';

try {
  await runDurableWorker({
    channelIds: required('HUMCHAT_CHANNEL_IDS').split(','),
    consumerId: process.env.HUMCHAT_CONSUMER_ID ?? 'multiplex-status-v1',
    transport: 'multiplex',
  });
} catch (error) {
  console.error('Worker stopped:', error.code ?? 'CONFIGURATION_OR_UNEXPECTED');
  process.exitCode = 1;
}
