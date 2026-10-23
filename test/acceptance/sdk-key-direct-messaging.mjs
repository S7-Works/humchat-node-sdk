// Opt-in acceptance against a real HumChat server: creates two persistent
// messages. Integration tests import runAcceptance only for synthetic peers.
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { ulid } from 'ulid';
import { HumChat, MemoryCheckpointStore } from '../../dist/index.js';

const SDK_CODES = new Set(['CONFIGURATION', 'VALIDATION', 'AUTHENTICATION', 'SESSION_REVOKED',
  'PERMISSION', 'NOT_FOUND', 'CONFLICT', 'RATE_LIMIT', 'NETWORK', 'TIMEOUT', 'ABORT', 'SERVER',
  'PROTOCOL', 'UNSUPPORTED_FEATURE', 'BACKPRESSURE', 'RECOVERY_GAP', 'PERSISTENCE']);
class AcceptanceFailure extends Error {
  constructor(code) { super('Acceptance check failed'); this.acceptanceCode = code; }
}
function requireCheck(condition, code) { if (!condition) throw new AcceptanceFailure(code); }
function email(value) { return value.trim().replace(/[A-Z]/g, letter => letter.toLowerCase()); }
function required(env, key) {
  requireCheck(typeof env[key] === 'string' && env[key].trim().length > 0, 'MISSING_CONFIGURATION');
  return env[key];
}
function safeFailure(error) {
  if (error instanceof AcceptanceFailure) return { code: error.acceptanceCode };
  const code = SDK_CODES.has(error?.code) ? error.code : 'UNEXPECTED_FAILURE';
  return { code, ...(Number.isInteger(error?.status) && error.status >= 400 && error.status <= 599 ? { status: error.status } : {}) };
}
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

/** Returns a sanitized report. Never passes credentials, identities or message bodies to a logger. */
export async function runAcceptance(env = process.env) {
  const report = { passed: false, checks: [], messageSendAttempts: 0, acceptedMessages: 0,
    receiverDeliveries: 0, senderDeliveries: 0, receiverLiveDeliveries: 0, receiverRecoveryDeliveries: 0,
    senderLiveDeliveries: 0, senderRecoveryDeliveries: 0, historyMatches: 0 };
  const clients = [], stores = [];
  const stop = new AbortController();
  const cancel = () => stop.abort(new AcceptanceFailure('CANCELLED'));
  let timer;
  const wait = async promise => {
    if (stop.signal.aborted) throw stop.signal.reason;
    let onAbort;
    const cancelled = new Promise((_, reject) => {
      onAbort = () => reject(stop.signal.reason);
      stop.signal.addEventListener('abort', onAbort, { once: true });
    });
    try { return await Promise.race([promise, cancelled]); }
    finally { stop.signal.removeEventListener('abort', onAbort); }
  };
  const pause = () => new Promise(resolve => {
    const timeout = setTimeout(done, 100);
    function done() { clearTimeout(timeout); stop.signal.removeEventListener('abort', done); resolve(); }
    stop.signal.addEventListener('abort', done, { once: true });
    if (stop.signal.aborted) done();
  });
  const check = name => report.checks.push(name);
  try {
    requireCheck(env.HUMCHAT_ACCEPTANCE_SEND === 'true', 'OPT_IN_REQUIRED');
    const baseUrl = required(env, 'HUMCHAT_BASE_URL');
    let origin;
    try {
      const url = new URL(baseUrl);
      requireCheck(['https:', 'http:'].includes(url.protocol) && !url.username && !url.password &&
        !url.search && !url.hash && url.pathname === '/', 'INVALID_ORIGIN');
      origin = url.origin;
    } catch { throw new AcceptanceFailure('INVALID_ORIGIN'); }
    const orgId = required(env, 'HUMCHAT_ORG_ID').toLowerCase();
    const sdkKey = required(env, 'HUMCHAT_SDK_KEY');
    const senderEmail = email(required(env, 'HUMCHAT_ACT_AS'));
    const recipientEmail = email(required(env, 'HUMCHAT_TEST_RECIPIENT_EMAIL'));
    requireCheck(senderEmail !== recipientEmail, 'DISTINCT_RECIPIENT_REQUIRED');
    const duration = env.HUMCHAT_ACCEPTANCE_TIMEOUT_MS === undefined ? 180_000 : Number(env.HUMCHAT_ACCEPTANCE_TIMEOUT_MS);
    requireCheck(Number.isSafeInteger(duration) && duration >= 10_000 && duration <= 600_000, 'INVALID_DEADLINE');
    timer = setTimeout(() => stop.abort(new AcceptanceFailure('DEADLINE_EXCEEDED')), duration);
    process.on('SIGINT', cancel); process.on('SIGTERM', cancel);
    let writesAllowed = false;
    const allowedGets = new Set(['/me', '/api/sdk/capabilities', '/api/org/members', '/api/conversations', '/api/messages/sync', '/api/events/sync']);
    const guardedFetch = (input, init = {}) => {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);
      const method = (init.method ?? (input instanceof Request ? input.method : 'GET')).toUpperCase();
      requireCheck(url.origin === origin && !url.username && !url.password, 'ORIGIN_MISMATCH');
      const login = method === 'POST' && url.pathname === '/api/sdk/auth/user-token';
      const write = method === 'POST' && ['/api/conversations', '/api/messages/send'].includes(url.pathname);
      requireCheck(login || (method === 'GET' && allowedGets.has(url.pathname)) || (write && writesAllowed), 'UNEXPECTED_REQUEST');
      return globalThis.fetch(input, { ...init, redirect: 'error', signal: AbortSignal.any([stop.signal, ...(init.signal ? [init.signal] : [])]) });
    };
    for (const actAs of [senderEmail, recipientEmail]) clients.push(new HumChat({ baseUrl, orgId,
      auth: { sdkKey, actAs }, fetch: guardedFetch, recovery: { mode: 'durable' },
      realtime: { maxChannels: 50 }, retry: { totalTimeoutMs: Math.min(duration, 30_000) } }));
    const [sender, receiver] = clients;
    const options = { signal: stop.signal };
    const [senderIdentity, receiverIdentity] = await wait(Promise.all(clients.map(client => client.connect(options))));
    requireCheck(senderIdentity.orgId === orgId && receiverIdentity.orgId === orgId &&
      email(senderIdentity.email) === senderEmail && email(receiverIdentity.email) === recipientEmail &&
      senderIdentity.actorId !== receiverIdentity.actorId, 'IDENTITY_MISMATCH');
    check('both_verified_member_identities');
    const capabilities = await wait(Promise.all(clients.map(client => client.getCapabilities(options))));
    for (const capability of capabilities) requireCheck(capability.authentication.sdkKeyUserTokens?.configured === true &&
      capability.authentication.userSessions && capability.messaging.sendReceiptVersions.includes(1) &&
      capability.messaging.messageStateVersions.includes(1) && capability.messaging.historyVersions.includes(2) &&
      capability.messaging.durableReplay && capability.messaging.eventProtocolVersions.includes(1), 'CAPABILITY_MISMATCH');
    check('both_messaging_capabilities');
    const rosters = await wait(Promise.all(clients.map(client => client.members.list(options))));
    for (const roster of rosters) for (const identity of [senderIdentity, receiverIdentity]) {
      const matches = roster.filter(member => email(member.email) === email(identity.email));
      requireCheck(matches.length === 1 && matches[0].actorId === identity.actorId && matches[0].status === 'active', 'ROSTER_MISMATCH');
    }
    check('both_rosters_identify_exact_active_participants');
    const listings = await wait(Promise.all(clients.map(client => client.conversations.list(options))));
    for (let index = 0; index < listings.length; index++) {
      const identity = [senderIdentity, receiverIdentity][index];
      const conversations = listings[index];
      requireCheck(conversations.length < 50, 'INBOX_CAPACITY_EXCEEDED');
      for (const conversation of conversations) requireCheck(conversation.channelId.startsWith(`${orgId}:conv:`) &&
        conversation.memberCount === conversation.members.length &&
        new Set(conversation.members.map(member => member.actorId)).size === conversation.members.length &&
        conversation.members.some(member => member.actorId === identity.actorId), 'CONVERSATION_SCOPE_MISMATCH');
    }
    check('both_conversation_scopes_and_capacity');

    const rootId = ulid(), replyId = ulid(), runLabel = randomUUID();
    const rootBody = `[HumChat SDK acceptance ${runLabel}] Test 1/2: direct message. This test message is intentionally retained.`;
    const replyBody = `[HumChat SDK acceptance ${runLabel}] Test 2/2: thread reply. This test message is intentionally retained.`;
    const rootReceived = deferred(), replyReceived = deferred();
    const matchesMessage = (state, id, actorId, body, threadRootId, channelId) => state.id === id &&
      state.orgId === orgId && state.senderId === actorId && !state.deleted && state.body === body &&
      state.threadRootId === threadRootId && (channelId === undefined || state.channelId === channelId);
    let expectedChannel;
    function handler(forReceiver) {
      return (event, context) => {
        if (event.kind !== 'message' || event.state.id !== (forReceiver ? rootId : replyId)) return;
        const valid = event.orgId === orgId && event.channelId === event.state.channelId &&
          event.channelId.startsWith(`${orgId}:conv:`) && matchesMessage(event.state,
            forReceiver ? rootId : replyId, forReceiver ? senderIdentity.actorId : receiverIdentity.actorId,
            forReceiver ? rootBody : replyBody, forReceiver ? null : rootId, expectedChannel);
        if (!valid) { stop.abort(new AcceptanceFailure('DELIVERY_MISMATCH')); return; }
        const side = forReceiver ? 'receiver' : 'sender';
        report[`${side}Deliveries`]++;
        report[`${side}${context.source === 'live' ? 'Live' : 'Recovery'}Deliveries`]++;
        (forReceiver ? rootReceived : replyReceived).resolve(event.state);
      };
    }
    for (let index = 0; index < 2; index++) stores.push(new MemoryCheckpointStore());
    const watchers = clients.map((client, index) => client.inbox.watch(handler(index === 1), {
      consumerId: `acceptance-${runLabel}-${index}`, checkpointStore: stores[index], pollIntervalMs: 1_000, maxConversations: 50,
    }));
    for (const watcher of watchers) void watcher.done.catch(error => stop.abort(error));
    await wait(Promise.all(watchers.map(watcher => watcher.ready)));
    const waitForLive = async channelId => {
      while (true) {
        stop.signal.throwIfAborted();
        const allLive = watchers.every(watcher => {
          const channels = watcher.getStats().channels;
          return watcher.status === 'watching' && (channelId === undefined
            ? channels.every(channel => channel.status === 'live')
            : channels.some(channel => channel.channelId === channelId && channel.status === 'live'));
        });
        if (allLive) return;
        await pause();
      }
    };
    await wait(waitForLive());
    check('both_inboxes_ready_before_writes');
    writesAllowed = true;
    stop.signal.throwIfAborted();
    report.messageSendAttempts++;
    const direct = await wait(sender.messages.sendDirect({ recipientId: receiverIdentity.actorId, content: rootBody, id: rootId },
      { ...options, idempotencyKey: rootId }));
    report.acceptedMessages++;
    expectedChannel = direct.conversation.channelId;
    requireCheck(direct.receipt.id === rootId && direct.receipt.channelId === expectedChannel &&
      direct.conversation.memberCount === 2 && direct.conversation.members.length === 2 &&
      new Set(direct.conversation.members.map(member => member.actorId)).size === 2 &&
      direct.conversation.members.some(member => member.actorId === senderIdentity.actorId) &&
      direct.conversation.members.some(member => member.actorId === receiverIdentity.actorId), 'DIRECT_RECEIPT_MISMATCH');
    check('sender_direct_message_accepted');
    const receivedRoot = await wait(rootReceived.promise);
    requireCheck(matchesMessage(receivedRoot, rootId, senderIdentity.actorId, rootBody, null, expectedChannel), 'DELIVERY_MISMATCH');
    check('receiver_inbox_received_direct_message');
    await wait(Promise.all(watchers.map(watcher => watcher.refresh())));
    await wait(waitForLive(expectedChannel));
    check('both_conversation_streams_live_before_reply');
    stop.signal.throwIfAborted();
    report.messageSendAttempts++;
    const reply = await wait(receiver.messages.reply(receivedRoot, { content: replyBody, id: replyId },
      { ...options, idempotencyKey: replyId }));
    report.acceptedMessages++;
    requireCheck(reply.id === replyId && reply.channelId === expectedChannel, 'REPLY_RECEIPT_MISMATCH');
    await wait(replyReceived.promise);
    check('sender_inbox_received_thread_reply');

    const findHistory = async (client, id, actorId, body, threadRootId) => {
      let pages = 0;
      for await (const page of client.messages.iterateHistory({ channelId: expectedChannel, threadRootId, limit: 200 }, options)) {
        const found = page.messages.find(state => state.id === id);
        if (found) {
          requireCheck(matchesMessage(found, id, actorId, body, threadRootId, expectedChannel), 'HISTORY_MISMATCH');
          report.historyMatches++;
          return;
        }
        if (++pages >= 5) break;
      }
      throw new AcceptanceFailure('MESSAGE_ABSENT_FROM_HISTORY');
    };
    await wait(Promise.all(clients.flatMap(client => [
      findHistory(client, rootId, senderIdentity.actorId, rootBody, null),
      findHistory(client, replyId, receiverIdentity.actorId, replyBody, rootId),
    ])));
    check('both_messages_persisted_in_both_member_histories');
    requireCheck(report.messageSendAttempts === 2 && report.acceptedMessages === 2 && report.historyMatches === 4, 'COUNT_MISMATCH');
    report.passed = true;
  } catch (error) { report.failure = safeFailure(error); }
  finally {
    clearTimeout(timer); stop.abort(new AcceptanceFailure('FINISHED'));
    const closed = await Promise.allSettled(clients.map(client => client.close()));
    const storesClosed = await Promise.allSettled(stores.map(store => store.close()));
    process.off('SIGINT', cancel); process.off('SIGTERM', cancel);
    if ([...closed, ...storesClosed].some(result => result.status === 'rejected')) {
      report.passed = false; report.failure = { code: 'SHUTDOWN_FAILED' };
    }
  }
  return report;
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  const report = process.argv.length === 2 ? await runAcceptance() : { passed: false, failure: { code: 'CLI_ARGUMENTS_REFUSED' } };
  process.stdout.write(`${JSON.stringify(report)}\n`);
  if (!report.passed) process.exitCode = 1;
}
