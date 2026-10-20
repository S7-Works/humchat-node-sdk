// Loopback receiver: a 2xx response means the verified event was persisted to the inbox.
// Apply business effects from that recoverable inbox with application idempotency.
import { createServer } from 'node:http';
import { verifyWebhook, WebhookVerificationError } from '@humchat/node';
import { WebhookInbox } from './webhook-inbox.mjs';
import { required } from './config.mjs';

const maxBytes = 8 * 1024 * 1024;
let server, inbox, stopping, timer, active = 0;
let finished;
const done = new Promise(resolve => { finished = resolve; });
function integer(value, fallback, minimum, maximum) {
  const result = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(result) || result < minimum || result > maximum) throw new Error('Invalid receiver option');
  return result;
}
function stop() {
  if (stopping) return stopping;
  clearTimeout(timer);
  stopping = (async () => {
    if (server?.listening) {
      const forced = setTimeout(() => server.closeAllConnections(), 5000);
      try { await new Promise(resolve => server.close(resolve)); } finally { clearTimeout(forced); }
    }
    await inbox?.close();
    console.log(JSON.stringify({ event: 'stopped' })); finished();
  })().catch(() => { console.error('Receiver stopped: WEBHOOK_INBOX'); process.exitCode = 1; finished(); });
  return stopping;
}
const onSignal = () => { void stop(); };
try {
  const orgId = required('HUMCHAT_ORG_ID'), subscriptionId = required('HUMCHAT_WEBHOOK_SUBSCRIPTION_ID');
  if (!/^[A-Za-z0-9._:-]{1,128}$/.test(orgId) || ['.', '..'].includes(orgId) || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(subscriptionId)) throw new Error('Invalid receiver scope');
  const channels = new Set(required('HUMCHAT_CHANNEL_IDS').split(',').map(value => value.trim()));
  if (!channels.size || channels.size > 50 || [...channels].some(value => !value.startsWith(orgId + ':'))) throw new Error('Invalid receiver channels');
  const secrets = [required('HUMCHAT_WEBHOOK_SECRET'), ...(process.env.HUMCHAT_WEBHOOK_PREVIOUS_SECRET ? [process.env.HUMCHAT_WEBHOOK_PREVIOUS_SECRET] : [])];
  if (secrets.some(secret => !/^whsec_[0-9a-f]{64}$/.test(secret)) || new Set(secrets).size !== secrets.length) throw new Error('Invalid receiver secrets');
  const port = integer(process.env.HUMCHAT_WEBHOOK_PORT, 43140, 0, 65535);
  const runFor = integer(process.env.HUMCHAT_RUN_FOR_MS, 0, 0, 2_147_483_647);
  inbox = await WebhookInbox.open(required('HUMCHAT_WEBHOOK_STATE_DIR'), orgId, subscriptionId);
  server = createServer({ requestTimeout: 10000, headersTimeout: 5000, maxHeaderSize: 16 * 1024 }, (request, response) => {
    const reject = status => { response.writeHead(status, { Connection: 'close' }); response.end('', () => request.destroy()); };
    if (stopping) { reject(503); return; }
    if (request.url !== '/humchat/webhook') { reject(404); return; }
    if (request.method !== 'POST') { reject(405); return; }
    if (active >= 8 || Number(request.headers['content-length'] ?? 0) > maxBytes) { reject(active >= 8 ? 503 : 413); return; }
    active++;
    void (async () => {
      try {
        const chunks = []; let length = 0;
        for await (const chunk of request) {
          length += chunk.length; if (length > maxBytes) { reject(413); return; } chunks.push(chunk);
        }
        const envelope = verifyWebhook(Buffer.concat(chunks, length), request.headers['x-humchat-signature'], secrets,
          { expectedOrgId: orgId, expectedSubscriptionId: subscriptionId });
        if (!channels.has(envelope.channelId)) throw new WebhookVerificationError();
        const inserted = await inbox.accept(envelope);
        response.writeHead(204); response.end();
        console.log(JSON.stringify({ event: 'accepted', eventId: envelope.eventId, duplicate: !inserted }));
      } catch (error) {
        if (!response.headersSent && !response.destroyed) reject(error instanceof WebhookVerificationError ? 401 : 503);
      } finally { active--; }
    })();
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
  for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, onSignal);
  console.log(JSON.stringify({ event: 'ready', host: '127.0.0.1', port: server.address().port, path: '/humchat/webhook' }));
  if (runFor) timer = setTimeout(onSignal, runFor);
  await done;
} catch {
  console.error('Receiver stopped: CONFIGURATION_OR_INBOX'); process.exitCode = 1; await stop();
} finally {
  for (const signal of ['SIGINT', 'SIGTERM']) process.off(signal, onSignal);
}
