// HUMCHAT_CHANNEL_ID is the canonical routing key, including for a DM.
import { client, required } from './config.mjs';
const humchat = client();
try {
  const receipt = await humchat.messages.send({ channelId: required('HUMCHAT_CHANNEL_ID'), content: process.argv[2] ?? 'Hello from the HumChat Node SDK' });
  console.log({ messageId: receipt.id, status: receipt.status });
} finally { await humchat.close(); }
