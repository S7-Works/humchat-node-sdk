// CommonJS backend smoke example. Sending requires HUMCHAT_SEND_MESSAGE explicitly.
const { HumChat } = require('@humchat/node');
function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(name + ' is required');
  return value;
}

async function main() {
  const client = new HumChat({ baseUrl: required('HUMCHAT_BASE_URL'), orgId: required('HUMCHAT_ORG_ID'),
    tokenProvider: async () => ({ token: required('HUMCHAT_TOKEN') }) });
  try {
    const result = await client.channels.list(process.env.HUMCHAT_TEAM_ID ? { teamId: process.env.HUMCHAT_TEAM_ID } : {});
    console.log(JSON.stringify({ event: 'channels', channels: result.channels.map(({ id, displayName }) => ({ id, displayName })) }));
    if (process.env.HUMCHAT_SEND_MESSAGE !== undefined) {
      const receipt = await client.messages.send({ channelId: required('HUMCHAT_CHANNEL_ID'), content: required('HUMCHAT_SEND_MESSAGE') });
      console.log(JSON.stringify({ event: 'sent', messageId: receipt.id, status: receipt.status }));
    }
  } finally { await client.close(); }
}
main().catch(error => { console.error('Example failed:', error.code ?? 'CONFIGURATION_OR_UNEXPECTED'); process.exitCode = 1; });
