// Also set HUMCHAT_CHANNEL_ID and HUMCHAT_MESSAGE_ID.
import { client, required } from './config.mjs';
const humchat = client();
try {
  const receipt = await humchat.messages.setReaction({ channelId: required('HUMCHAT_CHANNEL_ID'), messageId: required('HUMCHAT_MESSAGE_ID'), emoji: process.argv[2] ?? '✅', present: true });
  console.log({ operationId: receipt.operationId, reactionRevision: receipt.state.reactionRevision });
} finally { await humchat.close(); }
