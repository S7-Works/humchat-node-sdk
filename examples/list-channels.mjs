import { client } from './config.mjs';
const humchat = client();
try {
  const result = await humchat.channels.list(process.env.HUMCHAT_TEAM_ID ? { teamId: process.env.HUMCHAT_TEAM_ID } : {});
  console.table(result.channels.map(({ id, displayName }) => ({ id, displayName })));
} finally { await humchat.close(); }
