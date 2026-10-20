import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { HumChat, HumChatError } from '../../dist/index.js';

const org = 'org-mock-1';
const actor = 'user-mock-123';
const channelId = `${org}:${org}-home:sdk-test`;
const marker = 'SYNTHETIC_CREDENTIAL_MUST_NOT_ESCAPE';
const identity = { user_uuid: actor, id: 1, org, email: 'sdk@example.test', roles: ['member'],
  scopes: [], expires_at: null, app_uuid: null };
const capabilities = { sdk_protocol_version: 1, authentication: { user_sessions: true,
  service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
  messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
    durable_replay: false, websocket_scope: 'channel' } };
const channels = { org_uuid: org, team_uuid: `${org}-home`, team_name: 'SDK Test', channels: [
  { channel_id: channelId, org_uuid: org, team_uuid: `${org}-home`, slug: 'sdk-test', display_name: 'SDK Test', is_private: false },
] };
const json = (value, status = 200, headers = {}) => new Response(JSON.stringify(value), {
  status, headers: { 'content-type': 'application/json', ...headers },
});
const code = expected => error => {
  assert.ok(error instanceof HumChatError);
  assert.equal(error.code, expected);
  return true;
};
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}
function makeClient(t, handler = () => json(channels), overrides = {}) {
  const requests = [];
  const fetch = async (input, init) => {
    const url = new URL(input);
    const entry = { path: url.pathname, url, ...init, headers: new Headers(init.headers) };
    requests.push(entry);
    if (url.pathname === '/me') return json(identity);
    if (url.pathname === '/api/sdk/capabilities') return json(capabilities);
    return handler(entry);
  };
  const client = new HumChat({ orgId: org, baseUrl: 'http://127.0.0.1:43123',
    tokenProvider: async () => ({ token: 'synthetic-token-a' }), fetch,
    timeout: 500, retry: { maxRetries: 0, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 },
    ...overrides });
  t.after(() => client.close());
  return { client, requests };
}

test('concurrent connect uses one provider call and one verified handshake', async t => {
  const gate = deferred();
  let calls = 0;
  const { client, requests } = makeClient(t, undefined, { tokenProvider: async context => {
    calls++;
    assert.equal(context.reason, 'initial');
    assert.equal(context.previous, undefined);
    await gate.promise;
    return { token: 'synthetic-token-a' };
  } });
  const first = client.connect();
  const second = client.connect();
  await sleep(10);
  assert.equal(calls, 1);
  gate.resolve();
  const [one, two] = await Promise.all([first, second]);
  assert.equal(one.actorId, actor);
  assert.deepEqual(one, two);
  assert.equal(requests.filter(request => request.path === '/me').length, 1);
  one.roles.push('changed-by-caller');
  assert.deepEqual((await client.connect()).roles, ['member']);
  const cap = await client.getCapabilities();
  cap.messaging.historyVersions.length = 0;
  assert.deepEqual((await client.getCapabilities()).messaging.historyVersions, [2]);
});

test('one cancelled connect waiter does not cancel another shared handshake', async t => {
  const gate = deferred();
  const started = deferred();
  let providerSignal;
  const { client } = makeClient(t, undefined, { tokenProvider: async context => {
    providerSignal = context.signal;
    started.resolve();
    await gate.promise;
    return { token: 'synthetic-token-a' };
  } });
  const cancel = new AbortController();
  const cancelled = client.connect({ signal: cancel.signal });
  const survivor = client.connect();
  await started.promise;
  cancel.abort();
  await assert.rejects(cancelled, code('ABORT'));
  assert.equal(providerSignal.aborted, false);
  gate.resolve();
  assert.equal((await survivor).actorId, actor);
});

test('concurrent 401 responses refresh once without exposing the previous token', async t => {
  const contexts = [];
  const { client, requests } = makeClient(t, request =>
    request.headers.get('authorization') === 'Bearer synthetic-token-a'
      ? json({ error: marker }, 401) : json(channels), {
    tokenProvider: async context => {
      contexts.push(context);
      if (contexts.length > 1) await sleep(15);
      return { token: contexts.length === 1 ? 'synthetic-token-a' : 'synthetic-token-b' };
    },
  });
  await client.connect();
  await Promise.all([client.channels.list(), client.channels.list()]);
  assert.equal(contexts.length, 2);
  assert.equal(contexts[1].reason, 'unauthorized');
  assert.deepEqual(contexts[1].previous, { expiresAt: null });
  assert.equal('token' in contexts[1].previous, false);
  assert.equal(requests.filter(request => request.path === '/me').length, 2);
  assert.equal(requests.filter(request => request.path === '/api/workspace/channels').length, 4);
});

test('a second 401 fails the operation instead of refreshing forever', async t => {
  let calls = 0;
  const { client, requests } = makeClient(t, () => json({ error: marker }, 401), {
    tokenProvider: async () => ({ token: `synthetic-token-${++calls}` }),
  });
  await assert.rejects(client.channels.list(), code('AUTHENTICATION'));
  assert.equal(calls, 2);
  assert.equal(requests.filter(request => request.path === '/api/workspace/channels').length, 2);
});

test('an approaching expiry refreshes proactively with metadata-only previous context', async t => {
  let now = 2_000_000_000_000;
  t.mock.method(Date, 'now', () => now);
  const contexts = [];
  const { client } = makeClient(t, undefined, { tokenProvider: async context => {
    contexts.push(context);
    return { token: `synthetic-token-${contexts.length}`, expiresAt: now / 1000 + 120 };
  } });
  await client.connect();
  now += 61_000;
  await client.channels.list();
  assert.equal(contexts.length, 2);
  assert.equal(contexts[1].reason, 'expiring');
  assert.deepEqual(contexts[1].previous, { expiresAt: 2_000_000_120 });
});

test('a refreshed actor cannot receive a replay intended for the original actor', async t => {
  let providerCalls = 0;
  let dataRequests = 0;
  const { client } = makeClient(t, undefined, {
    tokenProvider: async () => ({ token: `synthetic-token-${++providerCalls}` }),
    fetch: async (input, init) => {
      const path = new URL(input).pathname;
      if (path === '/me') return json({ ...identity, user_uuid: providerCalls === 1 ? actor : 'different-actor' });
      if (path === '/api/sdk/capabilities') return json(capabilities);
      dataRequests++;
      return json({}, 401);
    },
  });
  await assert.rejects(client.channels.list(), code('AUTHENTICATION'));
  await assert.rejects(client.channels.list(), code('AUTHENTICATION'));
  assert.equal(dataRequests, 1);
  assert.equal(providerCalls, 2);
});

test('a different organization is rejected before a data request', async t => {
  const paths = [];
  const { client } = makeClient(t, undefined, { fetch: async input => {
    paths.push(new URL(input).pathname);
    return json({ ...identity, org: 'other-organization' });
  } });
  await assert.rejects(client.channels.list(), code('AUTHENTICATION'));
  assert.deepEqual(paths, ['/me']);
});

test('shared authentication failures do not overwrite another operation\'s intent', async t => {
  const { client } = makeClient(t, undefined, { fetch: async () => json({ ...identity, org: 'other-organization' }) });
  let first;
  let second;
  await assert.rejects(client.messages.send({ channelId, id: 'first-message', content: 'Synthetic first' },
    { idempotencyKey: 'first-key' }), error => { first = error; return code('AUTHENTICATION')(error); });
  await assert.rejects(client.messages.send({ channelId, id: 'second-message', content: 'Synthetic second' },
    { idempotencyKey: 'second-key' }), error => { second = error; return code('AUTHENTICATION')(error); });
  assert.notEqual(first, second);
  assert.equal(first.messageId, 'first-message');
  assert.equal(first.idempotencyKey, 'first-key');
  assert.equal(second.messageId, 'second-message');
  assert.equal(second.idempotencyKey, 'second-key');
});

test('provider failures are sanitized and bounded even when the provider ignores cancellation', async t => {
  const { client: throwing } = makeClient(t, undefined, { tokenProvider: async () => { throw new Error(marker); } });
  await assert.rejects(throwing.connect(), error => {
    code('AUTHENTICATION')(error);
    assert.equal(error.cause, undefined);
    assert.ok(!String(error.stack).includes(marker));
    return true;
  });
  const { client: hanging } = makeClient(t, undefined, {
    tokenProvider: async () => new Promise(() => {}),
    retry: { maxRetries: 0, totalTimeoutMs: 30 },
  });
  const start = performance.now();
  await assert.rejects(hanging.connect(), code('TIMEOUT'));
  assert.ok(performance.now() - start < 500);
});

test('request abort cancels a fetch that ignores its signal', async t => {
  const started = deferred();
  const { client } = makeClient(t, () => { started.resolve(); return new Promise(() => {}); });
  await client.connect();
  const cancel = new AbortController();
  const work = client.channels.list({}, { signal: cancel.signal });
  await started.promise;
  cancel.abort();
  await assert.rejects(work, code('ABORT'));
});

test('attempt timeout includes a stalled response body and cancels its reader', async t => {
  let cancelled = false;
  const { client } = makeClient(t, () => new Response(new ReadableStream({
    cancel() { cancelled = true; },
  })), { timeout: 25 });
  await client.connect();
  await assert.rejects(client.channels.list(), code('TIMEOUT'));
  assert.equal(cancelled, true);
});

test('total deadline interrupts a long Retry-After instead of scheduling more requests', async t => {
  let calls = 0;
  const { client } = makeClient(t, () => { calls++; return json({}, 429, { 'retry-after': '10' }); }, {
    retry: { maxRetries: 3, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 50 },
  });
  await client.connect();
  const start = performance.now();
  await assert.rejects(client.channels.list(), code('TIMEOUT'));
  assert.equal(calls, 1);
  assert.ok(performance.now() - start < 500);
});

test('408, 429, 502, 503 and 504 responses retry a safe read', async t => {
  for (const status of [408, 429, 502, 503, 504]) {
    let attempts = 0;
    const { client } = makeClient(t, () => ++attempts === 1 ? json({}, status) : json(channels), {
      retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 },
    });
    await client.channels.list();
    assert.equal(attempts, 2, `status ${status}`);
  }
});

test('403, 404, 422 and 500 fail without implicit retries', async t => {
  for (const [status, expected] of [[403, 'PERMISSION'], [404, 'NOT_FOUND'], [422, 'VALIDATION'], [500, 'SERVER']]) {
    let attempts = 0;
    const { client } = makeClient(t, () => { attempts++; return json({}, status); }, {
      retry: { maxRetries: 3, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 },
    });
    await assert.rejects(client.channels.list(), code(expected));
    assert.equal(attempts, 1, `status ${status}`);
  }
});

test('Retry-After is honored before a rate-limited request is retried', async t => {
  const times = [];
  const { client } = makeClient(t, () => {
    times.push(performance.now());
    return times.length === 1 ? json({}, 429, { 'retry-after': '0.04' }) : json(channels);
  }, { retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  await client.channels.list();
  assert.equal(times.length, 2);
  assert.ok(times[1] - times[0] >= 30, 'Retry must honor the server cooldown');
});

test('idempotent send retries preserve the complete body and intent headers', async t => {
  const sends = [];
  const { client } = makeClient(t, request => {
    sends.push(request);
    if (sends.length === 1) throw new Error(marker);
    const body = JSON.parse(request.body);
    return json({ status: 'ACK', message_id: body.id, created_at: 1792370346, attachment_count: 0,
      attachment_manifest_blake3: 'd53d18c23212ea7b6300594bb89bce60218f6eff2b9d628b8cc42d3e79bbd5ab' });
  }, { retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  const receipt = await client.messages.send({ channelId, content: 'Synthetic test message' });
  assert.equal(sends.length, 2);
  assert.equal(sends[0].body, sends[1].body);
  assert.ok(sends[0].headers.get('x-idempotency-key'));
  assert.equal(sends[0].headers.get('x-idempotency-key'), sends[1].headers.get('x-idempotency-key'));
  assert.equal(sends[0].headers.get('x-zlack-message-receipt'), 'v1');
  assert.equal(receipt.id, JSON.parse(sends[0].body).id);
});

test('a 401 send replay preserves its message ID, body and idempotency key after refresh', async t => {
  const sends = [];
  let providerCalls = 0;
  const { client } = makeClient(t, request => {
    sends.push(request);
    if (sends.length === 1) return json({}, 401);
    const body = JSON.parse(request.body);
    return json({ status: 'ACK', message_id: body.id, created_at: 1792370346, attachment_count: 0,
      attachment_manifest_blake3: 'd53d18c23212ea7b6300594bb89bce60218f6eff2b9d628b8cc42d3e79bbd5ab' });
  }, { tokenProvider: async () => ({ token: `synthetic-token-${++providerCalls}` }) });
  await client.messages.send({ channelId, content: 'Synthetic reply' });
  assert.equal(providerCalls, 2);
  assert.equal(sends.length, 2);
  assert.equal(sends[0].body, sends[1].body);
  assert.equal(sends[0].headers.get('x-idempotency-key'), sends[1].headers.get('x-idempotency-key'));
  assert.notEqual(sends[0].headers.get('authorization'), sends[1].headers.get('authorization'));
});

test('exhausted network and timeout sends retain exact recoverable intent metadata', async t => {
  for (const kind of ['NETWORK', 'TIMEOUT']) {
    const sends = [];
    const { client } = makeClient(t, request => {
      sends.push(request);
      if (kind === 'NETWORK') throw new Error(marker);
      return new Promise(() => {});
    }, { timeout: 20, retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
    await assert.rejects(client.messages.send({ channelId, content: 'Synthetic send', id: 'failure-intent' },
      { idempotencyKey: 'failure-key' }), error => {
      code(kind)(error);
      assert.equal(error.messageId, 'failure-intent');
      assert.equal(error.channelId, channelId);
      assert.equal(error.idempotencyKey, 'failure-key');
      assert.equal(error.cause, undefined);
      assert.ok(!String(error.stack).includes(marker));
      return true;
    });
    assert.equal(sends.length, 2);
    assert.equal(sends[0].body, sends[1].body);
  }
});

test('reaction retries preserve the operation ID and exact set intent', async t => {
  const operations = [];
  const { client } = makeClient(t, request => {
    operations.push(request);
    if (operations.length === 1) return json({}, 503);
    const body = JSON.parse(request.body);
    return json({ schema: 1, operation_id: body.operation_id, actor_id: actor,
      request_digest: 'd53d18c23212ea7b6300594bb89bce60218f6eff2b9d628b8cc42d3e79bbd5ab',
      state: { schema: 1, org_uuid: org, channel_id: channelId, message_id: 'react-message',
        sender_id: actor, created_at: 1792370346, thread_root_id: null, priority: 'standard',
        original_body: 'Synthetic root', body: 'Synthetic root', revision: 1, updated_at: 1792370346,
        deleted: false, reaction_revision: 1, reactions: { '👍': [actor] }, attachments: [] } });
  }, { retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  const receipt = await client.messages.setReaction({ channelId, messageId: 'react-message', emoji: '👍', present: true });
  assert.equal(operations.length, 2);
  assert.equal(operations[0].body, operations[1].body);
  const body = JSON.parse(operations[0].body);
  assert.ok(body.operation_id);
  assert.deepEqual(body.mutation, { kind: 'set_reaction', emoji: '👍', present: true });
  assert.equal(receipt.operationId, body.operation_id);
});

test('mutation acquisition and response share one total operation deadline', async t => {
  // Move the operation clock explicitly: a loaded host may spend the entire real
  // deadline before a sleeping provider resumes, never attempting the mutation.
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const providerStarted = deferred(), providerGate = deferred();
  let mutationRequests = 0;
  let mutationSignal;
  const { client } = makeClient(t, request => {
    mutationRequests++;
    mutationSignal = request.signal;
    return new Promise(() => {}); // The response deliberately ignores cancellation.
  }, {
    tokenProvider: async () => { providerStarted.resolve(); await providerGate.promise; return { token: 'synthetic-token-a' }; },
    retry: { maxRetries: 0, totalTimeoutMs: 200 },
  });
  let settled = false;
  const operation = client.messages.setReaction({ channelId, messageId: 'deadline-message',
    emoji: '👍', present: true, operationId: 'deadline-operation' });
  void operation.then(() => { settled = true; }, () => { settled = true; });
  const rejected = assert.rejects(operation, error => {
    code('TIMEOUT')(error);
    assert.equal(error.operationId, 'deadline-operation');
    return true;
  });
  // HttpTransport uses zero-delay admission timers between promise-based stages.
  // Flush those stages without advancing the operation's virtual clock.
  const drain = async () => {
    for (let turn = 0; turn < 10; turn++) { t.mock.timers.tick(0); await new Promise(setImmediate); }
  };
  await providerStarted.promise;
  t.mock.timers.tick(120);
  providerGate.resolve();
  await drain();
  assert.equal(mutationRequests, 1, 'Deadline must span both authentication and the attempted mutation');
  assert.equal(settled, false);
  t.mock.timers.tick(79); await drain();
  assert.equal(settled, false, 'The operation must remain pending before its original deadline');
  assert.equal(mutationSignal.aborted, false);
  t.mock.timers.tick(1); await drain();
  assert.equal(settled, true, 'Authentication must consume the same 200 ms deadline as the mutation response');
  assert.equal(mutationSignal.aborted, true, 'The original deadline must cancel the active mutation request');
  await rejected;
});

test('409 is not retried and failures preserve caller intent without server body leakage', async t => {
  let attempts = 0;
  const { client } = makeClient(t, () => { attempts++; return json({ error: marker }, 409, { 'x-request-id': 'safe-request-1' }); }, {
    retry: { maxRetries: 3, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 },
  });
  await assert.rejects(client.messages.send({ channelId, id: 'intent-message-1', content: 'Synthetic test message' },
    { idempotencyKey: 'intent-key-1' }), error => {
    code('CONFLICT')(error);
    assert.equal(error.status, 409);
    assert.equal(error.messageId, 'intent-message-1');
    assert.equal(error.idempotencyKey, 'intent-key-1');
    assert.equal(error.requestId, 'safe-request-1');
    assert.ok(!JSON.stringify(error).includes(marker));
    assert.ok(!String(error.stack).includes(marker));
    return true;
  });
  assert.equal(attempts, 1);
});

test('malformed JSON and unsafe request IDs never copy response text into errors', async t => {
  const { client: malformed } = makeClient(t, () => new Response('not-json-' + marker));
  await assert.rejects(malformed.channels.list(), error => {
    code('PROTOCOL')(error);
    assert.equal(error.cause, undefined);
    assert.ok(!String(error.stack).includes(marker));
    return true;
  });
  const { client: unsafeId } = makeClient(t, () => json({ error: marker }, 500, { 'x-request-id': 'Bearer ' + marker }));
  await assert.rejects(unsafeId.channels.list(), error => {
    code('SERVER')(error);
    assert.equal(error.requestId, undefined);
    assert.ok(!JSON.stringify(error).includes(marker));
    return true;
  });
});

test('custom fetch and response stream errors are sanitized even when they use SDK error classes', async t => {
  for (const boundary of ['fetch', 'body']) {
    const { client } = makeClient(t, () => {
      const sensitive = new HumChatError(marker, 'NETWORK', { cause: new Error(marker), retryable: true });
      if (boundary === 'fetch') throw sensitive;
      return new Response(new ReadableStream({ start(controller) { controller.error(sensitive); } }));
    });
    await assert.rejects(client.channels.list(), error => {
      code('NETWORK')(error);
      assert.equal(error.cause, undefined);
      assert.ok(!String(error.stack).includes(marker));
      assert.ok(!JSON.stringify(error).includes(marker));
      return true;
    });
  }
});

test('an error request ID cannot expose the active short opaque token', async t => {
  const { client } = makeClient(t, () => json({}, 500, { 'x-request-id': 'prefix-synthetic-token-a-suffix' }));
  await assert.rejects(client.channels.list(), error => {
    code('SERVER')(error);
    assert.equal(error.requestId, undefined);
    assert.ok(!JSON.stringify(error).includes('synthetic-token-a'));
    return true;
  });
});

test('all requests disable redirects and credentials stay out of URLs and safe config', async t => {
  const { client, requests } = makeClient(t);
  await client.channels.list();
  for (const request of requests) {
    assert.equal(request.redirect, 'error');
    assert.equal(request.headers.get('authorization'), 'Bearer synthetic-token-a');
    assert.ok(!request.url.href.includes('synthetic-token'));
  }
  const config = client.getConfig();
  assert.equal('tokenProvider' in config, false);
  assert.equal('fetch' in config, false);
  assert.ok(!JSON.stringify(config).includes('synthetic-token'));
});

test('close cancels outstanding fetch and refuses later work without another request', async t => {
  const started = deferred();
  const { client, requests } = makeClient(t, () => { started.resolve(); return new Promise(() => {}); });
  await client.connect();
  const pending = client.channels.list();
  await started.promise;
  await client.close();
  await assert.rejects(pending, code('ABORT'));
  const count = requests.length;
  await assert.rejects(client.channels.list(), code('ABORT'));
  assert.equal(requests.length, count);
});
