import { required } from './config.mjs';
import { runDurableWorker } from './durable-worker.mjs';

try {
  await runDurableWorker({ channelIds: required('HUMCHAT_CHANNEL_IDS').split(','),
    consumerId: process.env.HUMCHAT_CONSUMER_ID ?? 'multi-channel-status-v1' });
} catch (error) { console.error('Worker failed:', error.code ?? 'CONFIGURATION_OR_UNEXPECTED'); process.exitCode = 1; }
