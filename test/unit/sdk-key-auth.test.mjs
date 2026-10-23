import test from 'node:test';
import assert from 'node:assert/strict';
import { inspect } from 'node:util';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { HumChat, HumChatError } from '../../dist/index.js';

// Synthetic secrets: no production credentials or customer identities belong here.
const orgId = '01234567-89ab-cdef-0123-456789abcdef';
const actorId = 'synthetic-sdk-key-actor';
const sdkKey = 'hsk_SYNTHETIC_MINT_SECRET_NEVER_EXPOSE';
const actAs = 'synthetic.actor@example.invalid';
const token = 'SYNTHETIC_MINTED_TOKEN_NEVER_EXPOSE';
const marker = 'SYNTHETIC_UNTRUSTED_BODY_NEVER_EXPOSE';
const mintPath = '/api/sdk/auth/user-token';
const keyCapability = { endpoint: mintPath, key_header: 'X-HumChat-Sdk-Key', configured: true };
const identity = { user_uuid: actorId, org: orgId, email: actAs, roles: ['member'], expires_at: null };
const capabilities = { sdk_protocol_version: 1, authentication: {
  user_sessions: true, service_identities: false, api_key_exchange: false,
  app_binding_required: false, mock_authentication: false, sdk_key_user_tokens: keyCapability,
}, messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
  durable_replay: false, websocket_scope: 'channel' } };
const mintResult = { access_token: token, token_type: 'Bearer', expires_in: 900 };
const json = (value, status = 200, headers = {}) => Response.json(value, { status, headers });
const deferred = () => {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
};
function secretFree(value) {
  for (const secret of [sdkKey, token, marker]) {
    assert.equal(value.includes(secret), false, 'Credentials and untrusted response text must stay private');
  }
}
function errorCode(expected, status) {
  return error => {
    assert.ok(error instanceof HumChatError);
    assert.equal(error.code, expected);
    if (status !== undefined) assert.equal(error.status, status);
    assert.equal(error.cause, undefined);
    secretFree(inspect(error, { showHidden: true, depth: null }));
    secretFree(JSON.stringify(error));
    return true;
  };
}
function fixture(t, handlers = {}, options = {}) {
  const requests = [];
  const fetch = async (input, init) => {
    const url = new URL(input);
    const request = { ...init, url, path: url.pathname, headers: new Headers(init.headers) };
    requests.push(request);
    if (handlers[request.path]) return handlers[request.path](request, requests);
    if (request.path === mintPath) return json(mintResult);
    if (request.path === '/me') return json(identity);
    if (request.path === '/api/sdk/capabilities') return json(capabilities);
    assert.equal(request.path, '/api/org/members', 'Unexpected authenticated path');
    return json([]);
  };
  const client = new HumChat({ orgId, baseUrl: 'https://humchat.example.invalid', auth: { sdkKey, actAs }, fetch,
    timeout: 500, retry: { maxRetries: 0, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1_000 }, ...options });
  t.after(() => client.close());
  return { client, requests };
}

test('SDK-key exchange uses only HumChat, then verifies identity and capabilities before a resource call', async t => {
  const { client, requests } = fixture(t);
  assert.deepEqual(requests, []);
  assert.deepEqual(await client.members.list(), []);
  assert.deepEqual(requests.map(request => request.path), [mintPath, '/me', '/api/sdk/capabilities', '/api/org/members']);
  const mint = requests[0];
  assert.equal(mint.method, 'POST');
  assert.equal(mint.headers.get('x-humchat-sdk-key'), sdkKey);
  assert.equal(mint.headers.has('authorization'), false);
  assert.match(mint.headers.get('content-type'), /^application\/json/);
  assert.deepEqual(JSON.parse(mint.body), { org_uuid: orgId, email: actAs, expires_in: 900 });
  for (const request of requests) {
    assert.equal(request.url.origin, 'https://humchat.example.invalid');
    assert.equal(request.url.search, '');
    assert.equal(request.redirect, 'error');
    if (request !== mint) {
      assert.equal(request.headers.get('authorization'), `Bearer ${token}`);
      assert.equal(request.headers.has('x-humchat-sdk-key'), false);
      assert.equal(request.body, undefined);
    }
  }
  assert.equal((await client.connect()).actorId, actorId);
  const caps = await client.getCapabilities();
  assert.deepEqual(caps.authentication.sdkKeyUserTokens, { endpoint: mintPath, keyHeader: 'X-HumChat-Sdk-Key', configured: true });
  caps.authentication.sdkKeyUserTokens.configured = false;
  assert.equal((await client.getCapabilities()).authentication.sdkKeyUserTokens.configured, true);
  assert.equal(requests.length, 4, 'Verified sessions must be reused');
  secretFree(inspect(client, { showHidden: true, depth: null, maxStringLength: null }));
  secretFree(JSON.stringify(client));
  secretFree(JSON.stringify(client.getConfig()));
});

test('SDK-key credentials are snapshotted and normalized without mutating the caller object', async t => {
  const auth = { sdkKey, actAs: ' SYNTHETIC.Actor@EXAMPLE.INVALID ' };
  const original = { ...auth };
  const { client, requests } = fixture(t, {}, { auth, orgId: orgId.toUpperCase() });
  assert.deepEqual(auth, original);
  auth.sdkKey = 'hsk_mutated_after_construction';
  auth.actAs = 'someone.else@example.invalid';
  await client.connect();
  assert.equal(requests[0].headers.get('x-humchat-sdk-key'), sdkKey);
  assert.deepEqual(JSON.parse(requests[0].body), { org_uuid: orgId, email: actAs, expires_in: 900 });
});

test('SDK-key credential getters are read once so validation and use cannot disagree', async t => {
  let keyReads = 0, emailReads = 0;
  const auth = {
    get sdkKey() { return ++keyReads === 1 ? sdkKey : 'hsk_changed_after_validation'; },
    get actAs() { return ++emailReads === 1 ? actAs : 'changed@example.invalid'; },
  };
  const { client, requests } = fixture(t, {}, { auth });
  await client.connect();
  assert.equal(keyReads, 1);
  assert.equal(emailReads, 1);
  assert.equal(requests[0].headers.get('x-humchat-sdk-key'), sdkKey);
  assert.equal(JSON.parse(requests[0].body).email, actAs);
});

test('ASCII email normalization preserves non-ASCII local-part letters', async t => {
  const { client, requests } = fixture(t, { '/me': () => json({ ...identity, email: '\u00c4ctor@example.invalid' }) },
    { auth: { sdkKey, actAs: ' \u00c4CTOR@EXAMPLE.INVALID ' } });
  await client.connect();
  assert.equal(JSON.parse(requests[0].body).email, '\u00c4ctor@example.invalid');
});

test('concurrent SDK-key connect and resource callers share one mint and verified handshake', async t => {
  const started = deferred(), release = deferred();
  const { client, requests } = fixture(t, { [mintPath]: async () => {
    started.resolve(); await release.promise; return json(mintResult);
  } });
  const operations = [client.connect(), client.connect(), client.getCapabilities(), client.members.list(), client.members.list()];
  await started.promise;
  assert.deepEqual(requests.map(request => request.path), [mintPath]);
  release.resolve();
  await Promise.all(operations);
  assert.equal(requests.filter(request => request.path === mintPath).length, 1);
  assert.equal(requests.filter(request => request.path === '/me').length, 1);
  assert.equal(requests.filter(request => request.path === '/api/sdk/capabilities').length, 1);
  assert.equal(requests.filter(request => request.path === '/api/org/members').length, 2);
});

test('cancelling one SDK-key waiter preserves the shared mint for another caller', async t => {
  const started = deferred(), release = deferred();
  let mintSignal;
  const { client } = fixture(t, { [mintPath]: async request => {
    mintSignal = request.signal; started.resolve(); await release.promise; return json(mintResult);
  } });
  const controller = new AbortController();
  const cancelled = client.connect({ signal: controller.signal });
  const survivor = client.connect();
  await started.promise;
  controller.abort(new Error(`${sdkKey}:${token}`));
  await assert.rejects(cancelled, errorCode('ABORT'));
  assert.equal(mintSignal.aborted, false);
  release.resolve();
  assert.equal((await survivor).actorId, actorId);
});

test('close aborts a mint that ignores cancellation and prevents later verification or use', async t => {
  const started = deferred(), release = deferred();
  let mintSignal;
  const { client, requests } = fixture(t, { [mintPath]: async request => {
    mintSignal = request.signal; started.resolve(); await release.promise; return json(mintResult);
  } });
  const pending = client.connect();
  const rejection = assert.rejects(pending, errorCode('ABORT'));
  await started.promise;
  await client.close();
  assert.equal(mintSignal.aborted, true);
  release.resolve();
  await rejection;
  await new Promise(setImmediate);
  await assert.rejects(client.members.list(), errorCode('ABORT'));
  assert.deepEqual(requests.map(request => request.path), [mintPath]);
});

test('a pre-aborted connect does not transmit an SDK key', async t => {
  const { client, requests } = fixture(t);
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(client.connect({ signal: controller.signal }), errorCode('ABORT'));
  assert.deepEqual(requests, []);
});

for (const [status, expected] of [[400, 'VALIDATION'], [401, 'AUTHENTICATION'], [403, 'PERMISSION'],
  [404, 'UNSUPPORTED_FEATURE'], [405, 'UNSUPPORTED_FEATURE'], [413, 'VALIDATION'], [415, 'VALIDATION'],
  [422, 'VALIDATION'], [500, 'SERVER']]) {
  test(`SDK-key mint ${status} is sanitized, typed, and never retried`, async t => {
    const { client, requests } = fixture(t, { [mintPath]: () => new Response(`${sdkKey}:${token}:${marker}`, {
      status, headers: { 'x-request-id': `prefix-${sdkKey}-suffix` },
    }) }, { retry: { maxRetries: 3, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
    await assert.rejects(client.connect(), error => {
      errorCode(expected, status)(error);
      assert.equal(error.retryable, false);
      assert.equal(error.requestId, undefined);
      return true;
    });
    assert.deepEqual(requests.map(request => request.path), [mintPath]);
  });
}

for (const [status, expected] of [[429, 'RATE_LIMIT'], [502, 'SERVER'], [503, 'SERVER']]) {
  test(`SDK-key mint ${status} has bounded retries and retains its native error category`, async t => {
    const { client, requests } = fixture(t, { [mintPath]: () => json({ detail: `${sdkKey}:${token}:${marker}` }, status) },
      { retry: { maxRetries: 2, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
    await assert.rejects(client.connect(), error => {
      errorCode(expected, status)(error);
      assert.equal(error.retryable, true);
      return true;
    });
    assert.equal(requests.length, 3);
    for (const request of requests) {
      assert.equal(request.path, mintPath);
      assert.equal(request.body, requests[0].body);
      assert.equal(request.headers.has('authorization'), false);
    }
  });
}

test('transient SDK-key mint failure retries successfully and keeps the key out of normal calls', async t => {
  let attempts = 0;
  const { client, requests } = fixture(t, { [mintPath]: () => ++attempts === 1 ? json({}, 502) : json(mintResult) },
    { retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  await client.members.list();
  assert.equal(attempts, 2);
  assert.equal(requests.at(-1).headers.has('x-humchat-sdk-key'), false);
});

test('SDK-key mint honors Retry-After within the shared acquisition deadline', async t => {
  const times = [];
  const { client } = fixture(t, { [mintPath]: () => {
    times.push(performance.now());
    return times.length === 1 ? json({}, 429, { 'retry-after': '0.04' }) : json(mintResult);
  } }, { retry: { maxRetries: 1, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  await client.connect();
  assert.equal(times.length, 2);
  assert.ok(times[1] - times[0] >= 30);
});

test('a long mint Retry-After cannot outlive the acquisition deadline or trigger an extra attempt', async t => {
  const { client, requests } = fixture(t, { [mintPath]: () => json({}, 429, { 'retry-after': '10' }) },
    { retry: { maxRetries: 2, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 50 } });
  await assert.rejects(client.connect(), errorCode('TIMEOUT'));
  assert.equal(requests.length, 1);
});

test('SDK-key mint timeout includes a stalled response body and cancels its reader', async t => {
  let cancelled = false;
  const { client } = fixture(t, { [mintPath]: () => new Response(new ReadableStream({ cancel() { cancelled = true; } })) },
    { timeout: 25 });
  await assert.rejects(client.connect(), errorCode('TIMEOUT'));
  assert.equal(cancelled, true);
});

test('oversized mint responses are bounded and discarded before identity verification', async t => {
  let cancelled = false;
  const { client, requests } = fixture(t, { [mintPath]: () => new Response(new ReadableStream({
    start(controller) { controller.enqueue(new Uint8Array(10 * 1024 * 1024 + 1)); },
    cancel() { cancelled = true; },
  })) });
  await assert.rejects(client.connect(), errorCode('PROTOCOL'));
  assert.equal(cancelled, true);
  assert.deepEqual(requests.map(request => request.path), [mintPath]);
});

test('custom mint fetch and body failures cannot smuggle secrets through SDK errors or causes', async t => {
  for (const boundary of ['fetch', 'body']) {
    const { client } = fixture(t, { [mintPath]: () => {
      const sensitive = new HumChatError(`${sdkKey}:${token}:${marker}`, 'AUTHENTICATION', { cause: new Error(sdkKey) });
      if (boundary === 'fetch') throw sensitive;
      return new Response(new ReadableStream({ start(controller) { controller.error(sensitive); } }));
    } });
    await assert.rejects(client.connect(), errorCode('NETWORK'));
  }
});

test('native fetch refuses redirects from the mint endpoint without transmitting the key to the target', async t => {
  const paths = [];
  const server = createServer((request, response) => {
    paths.push(request.url);
    response.writeHead(302, { location: '/must-not-receive-key' });
    response.end();
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const { client } = fixture(t, {}, { baseUrl: `http://127.0.0.1:${server.address().port}`, fetch: globalThis.fetch });
  await assert.rejects(client.connect(), errorCode('NETWORK'));
  assert.deepEqual(paths, [mintPath]);
});

test('malformed mint success bodies fail before identity lookup without copying response data', async t => {
  const cases = [null, {}, [], { ...mintResult, access_token: '' }, { ...mintResult, access_token: `Bearer ${token}` },
    { ...mintResult, access_token: `${token}\n` }, { ...mintResult, access_token: `${token}\u0085` },
    { ...mintResult, access_token: `${token}\u00e9` }, { ...mintResult, access_token: 42 },
    { ...mintResult, token_type: 'Basic' }, { ...mintResult, token_type: null },
    { ...mintResult, expires_in: 0 }, { ...mintResult, expires_in: -1 }, { ...mintResult, expires_in: 3601 },
    { ...mintResult, expires_in: 1.5 }, { ...mintResult, expires_in: '900' }, { ...mintResult, expires_in: null },
    { ...mintResult, expires_in: Number.MAX_SAFE_INTEGER + 1 }];
  for (const result of cases) {
    const { client, requests } = fixture(t, { [mintPath]: () => json(result) });
    await assert.rejects(client.connect(), errorCode('PROTOCOL'));
    assert.deepEqual(requests.map(request => request.path), [mintPath]);
  }
  const { client } = fixture(t, { [mintPath]: () => new Response(`not-json:${sdkKey}:${token}:${marker}`) });
  await assert.rejects(client.connect(), errorCode('PROTOCOL'));
});

test('mint token type accepts case-insensitive Bearer scheme spelling', async t => {
  for (const token_type of ['bearer', 'BEARER', 'bEaReR']) {
    const { client } = fixture(t, { [mintPath]: () => json({ ...mintResult, token_type }) });
    assert.equal((await client.connect()).actorId, actorId);
  }
});

for (const [label, returnedToken] of [
  ['the configured SDK key', sdkKey],
  ['another SDK-shaped key', 'hsk_synthetic_unrelated_response_key'],
  ['a value containing the configured SDK key', `prefix-${sdkKey}-suffix`],
]) {
  test(`mint response cannot promote ${label} into a Bearer credential`, async t => {
    const { client, requests } = fixture(t, {
      [mintPath]: () => json({ ...mintResult, access_token: returnedToken }),
    });
    await assert.rejects(client.members.list(), error => {
      errorCode('PROTOCOL')(error);
      assert.equal(inspect(error, { showHidden: true, depth: null }).includes(returnedToken), false);
      assert.equal(JSON.stringify(error).includes(returnedToken), false);
      return true;
    });
    assert.deepEqual(requests.map(request => request.path), [mintPath],
      'An SDK key returned as a token must be rejected before identity or resource requests');
    assert.ok(requests.every(request => !request.headers.has('authorization')));
  });
}

test('mint lifetimes accept the inclusive supported boundaries', async t => {
  for (const expires_in of [1, 3600]) {
    const { client } = fixture(t, { [mintPath]: () => json({ ...mintResult, expires_in }) });
    assert.equal((await client.connect()).actorId, actorId);
  }
});

test('mint expiry is based on request start rather than response completion', async t => {
  let now = 2_000_000_000_000;
  t.mock.method(Date, 'now', () => now);
  let mints = 0;
  const { client } = fixture(t, { [mintPath]: () => {
    if (++mints === 1) now += 850_000;
    return json(mintResult);
  } });
  await client.connect();
  now += 30_000;
  await client.members.list();
  assert.equal(mints, 2, 'Slow mint responses must not extend the advertised token lifetime');
});

test('a shorter verified identity expiry controls renewal while null identity expiry keeps the mint limit', async t => {
  let now = 2_000_000_000_000;
  t.mock.method(Date, 'now', () => now);
  for (const verifiedExpiry of [120, null]) {
    let mints = 0;
    const { client } = fixture(t, { [mintPath]: () => { mints++; return json(mintResult); },
      '/me': () => json({ ...identity, expires_at: verifiedExpiry === null ? null : now / 1000 + verifiedExpiry }) });
    await client.connect();
    now += (verifiedExpiry === null ? 841 : 61) * 1000;
    await client.members.list();
    assert.equal(mints, 2);
  }
});

test('already-expired verified identity cannot expose a usable SDK-key session', async t => {
  const { client, requests } = fixture(t, { '/me': () => json({ ...identity, expires_at: Math.floor(Date.now() / 1000) - 1 }) });
  await assert.rejects(client.members.list(), errorCode('AUTHENTICATION'));
  assert.equal(requests.some(request => request.path === '/api/org/members'), false);
});

test('wrong organization or normalized email is terminal before capabilities or resource access', async t => {
  for (const wrong of [{ org: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' }, { email: 'different@example.invalid' }]) {
    const { client, requests } = fixture(t, { '/me': () => json({ ...identity, ...wrong }) });
    await assert.rejects(client.members.list(), errorCode('AUTHENTICATION'));
    await assert.rejects(client.members.list(), errorCode('AUTHENTICATION'));
    assert.deepEqual(requests.map(request => request.path), [mintPath, '/me']);
  }
});

test('verified actor email uses the same ASCII case normalization as the requested actor', async t => {
  const { client } = fixture(t, { '/me': () => json({ ...identity, email: ' SYNTHETIC.Actor@EXAMPLE.INVALID ' }) });
  assert.equal((await client.connect()).actorId, actorId);
});

test('unconfigured or absent SDK-key capability prevents exposing the minted session', async t => {
  for (const value of [undefined, { ...keyCapability, configured: false }]) {
    const auth = { ...capabilities.authentication };
    if (value === undefined) delete auth.sdk_key_user_tokens;
    else auth.sdk_key_user_tokens = value;
    const { client, requests } = fixture(t, { '/api/sdk/capabilities': () => json({ ...capabilities, authentication: auth }) });
    await assert.rejects(client.members.list(), errorCode('UNSUPPORTED_FEATURE'));
    assert.equal(requests.some(request => request.path === '/api/org/members'), false);
  }
});

test('malformed and unsafe advertised SDK-key contracts cannot redirect credentials', async t => {
  for (const value of [null, {}, { ...keyCapability, configured: 'true' },
    { ...keyCapability, endpoint: 'https://attacker.example.invalid/mint' },
    { ...keyCapability, endpoint: '//attacker.example.invalid/mint' },
    { ...keyCapability, endpoint: '/api/unrelated' },
    { ...keyCapability, key_header: 'Authorization' },
    { ...keyCapability, key_header: 'X-HumChat-Sdk-Key\r\nAuthorization' }]) {
    const { client, requests } = fixture(t, { '/api/sdk/capabilities': () => json({ ...capabilities,
      authentication: { ...capabilities.authentication, sdk_key_user_tokens: value } }) });
    await assert.rejects(client.members.list(), errorCode('PROTOCOL'));
    assert.deepEqual(requests.map(request => request.path), [mintPath, '/me', '/api/sdk/capabilities']);
    assert.ok(requests.every(request => request.url.origin === 'https://humchat.example.invalid'));
  }
});

test('capability header matching is case insensitive', async t => {
  const { client } = fixture(t, { '/api/sdk/capabilities': () => json({ ...capabilities,
    authentication: { ...capabilities.authentication, sdk_key_user_tokens: { ...keyCapability, key_header: 'x-humchat-sdk-key' } } }) });
  assert.equal((await client.connect()).actorId, actorId);
});

test('concurrent normal-call 401s share one SDK-key remint and retry with the replacement token', async t => {
  const secondToken = 'synthetic-sdk-key-replacement-token';
  let mints = 0;
  const release = deferred(), started = deferred();
  const { client, requests } = fixture(t, {
    [mintPath]: async () => {
      if (++mints > 1) { started.resolve(); await release.promise; }
      return json({ ...mintResult, access_token: mints === 1 ? token : secondToken });
    },
    '/api/org/members': request => request.headers.get('authorization') === `Bearer ${token}` ? json({}, 401) : json([]),
  });
  await client.connect();
  const operations = [client.members.list(), client.members.list()];
  await started.promise;
  release.resolve();
  await Promise.all(operations);
  assert.equal(mints, 2);
  const resourceRequests = requests.filter(request => request.path === '/api/org/members');
  assert.equal(resourceRequests.length, 4);
  assert.ok(resourceRequests.every(request => !request.headers.has('x-humchat-sdk-key')));
});

test('a second normal-call 401 surfaces the error without reminting forever', async t => {
  let mints = 0;
  const { client, requests } = fixture(t, { [mintPath]: () => { mints++; return json(mintResult); },
    '/api/org/members': () => new Response(`${sdkKey}:${token}:${marker}`, { status: 401 }) });
  await assert.rejects(client.members.list(), errorCode('AUTHENTICATION', 401));
  assert.equal(mints, 2);
  assert.equal(requests.filter(request => request.path === '/api/org/members').length, 2);
});

test('SDK-key mode preserves the no-replay rule for conversation creation after 401', async t => {
  let mints = 0;
  const { client, requests } = fixture(t, { [mintPath]: () => { mints++; return json(mintResult); },
    '/api/conversations': () => json({}, 401) },
    { retry: { maxRetries: 3, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  await assert.rejects(client.conversations.create({ memberIds: ['synthetic-peer'] }), errorCode('AUTHENTICATION', 401));
  assert.equal(mints, 1);
  assert.equal(requests.filter(request => request.path === '/api/conversations').length, 1);
});

test('a revoked key during normal-call renewal fails once and does not replay the resource', async t => {
  let mints = 0;
  const { client, requests } = fixture(t, { [mintPath]: () => ++mints === 1 ? json(mintResult) : json({}, 401),
    '/api/org/members': () => json({}, 401) },
    { retry: { maxRetries: 3, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 1000 } });
  await assert.rejects(client.members.list(), errorCode('AUTHENTICATION', 401));
  assert.equal(mints, 2);
  assert.equal(requests.filter(request => request.path === '/api/org/members').length, 1);
});

test('renewal cannot change actor even when the returned email and organization still match', async t => {
  let mints = 0;
  const { client, requests } = fixture(t, { [mintPath]: () => { mints++; return json(mintResult); },
    '/me': () => json({ ...identity, user_uuid: mints === 1 ? actorId : 'different-synthetic-actor' }),
    '/api/org/members': () => json({}, 401) });
  await assert.rejects(client.members.list(), errorCode('AUTHENTICATION'));
  await assert.rejects(client.members.list(), errorCode('AUTHENTICATION'));
  assert.equal(mints, 2);
  assert.equal(requests.filter(request => request.path === '/api/org/members').length, 1);
});

test('normal authenticated error request IDs cannot expose either the SDK key or minted token', async t => {
  for (const secret of [sdkKey, token]) {
    const { client } = fixture(t, { '/api/org/members': () => json({ detail: marker }, 500,
      { 'x-request-id': `prefix-${secret}-suffix` }) });
    await assert.rejects(client.members.list(), error => {
      errorCode('SERVER', 500)(error);
      assert.equal(error.requestId, undefined);
      return true;
    });
  }
});

test('sanitized server request IDs remain available for diagnosing mint failures', async t => {
  const { client } = fixture(t, { [mintPath]: () => json({}, 403, { 'x-request-id': 'safe-mint-request-42' }) });
  await assert.rejects(client.connect(), error => {
    errorCode('PERMISSION', 403)(error);
    assert.equal(error.requestId, 'safe-mint-request-42');
    return true;
  });
});

test('failed SDK-key renewal cannot expose the prior Bearer token in a mint request ID', async t => {
  let mints = 0;
  const { client, requests } = fixture(t, {
    [mintPath]: () => ++mints === 1 ? json(mintResult)
      : json({ detail: marker }, 500, { 'x-request-id': `prefix-${token}-suffix` }),
    '/api/org/members': () => json({}, 401),
  });
  await client.connect();
  await assert.rejects(client.members.list(), error => {
    errorCode('SERVER', 500)(error);
    assert.equal(error.requestId, undefined);
    return true;
  });
  assert.equal(mints, 2);
  assert.equal(requests.filter(request => request.path === '/api/org/members').length, 1,
    'A failed renewal must not replay the resource');
});

test('a token from a rejected mint response remains redacted from later mint request IDs', async t => {
  let mints = 0;
  const { client, requests } = fixture(t, {
    [mintPath]: () => ++mints === 1 ? json({ ...mintResult, expires_in: 0 })
      : json({}, 500, { 'x-request-id': `prefix-${token}-suffix` }),
  });
  await assert.rejects(client.connect(), errorCode('PROTOCOL'));
  await assert.rejects(client.connect(), error => {
    errorCode('SERVER', 500)(error);
    assert.equal(error.requestId, undefined);
    return true;
  });
  assert.equal(mints, 2);
  assert.deepEqual(requests.map(request => request.path), [mintPath, mintPath]);
  assert.ok(requests.every(request => !request.headers.has('authorization')));
});

test('successful SDK-key rotation continues redacting a retired token from resource request IDs', async t => {
  const replacement = 'synthetic-request-id-replacement-token';
  let mints = 0;
  const { client, requests } = fixture(t, {
    [mintPath]: () => json({ ...mintResult, access_token: ++mints === 1 ? token : replacement }),
    '/api/org/members': request => request.headers.get('authorization') === `Bearer ${token}`
      ? json({}, 401) : json({ detail: marker }, 500, { 'x-request-id': `prefix-${token}-suffix` }),
  });
  await client.connect();
  await assert.rejects(client.members.list(), error => {
    errorCode('SERVER', 500)(error);
    assert.equal(error.requestId, undefined);
    assert.equal(inspect(error, { showHidden: true, depth: null }).includes(replacement), false);
    return true;
  });
  assert.equal(mints, 2);
  const resources = requests.filter(request => request.path === '/api/org/members');
  assert.equal(resources.length, 2);
  assert.equal(resources[1].headers.get('authorization'), `Bearer ${replacement}`);
});

test('SDK-key request-ID redaction stays private after bounded token history is saturated', { timeout: 15000 }, async t => {
  let mints = 0, renew = false, errorRequestId;
  const issued = [];
  const { client } = fixture(t, {
    [mintPath]: () => {
      const access_token = `synthetic-history-token-${String(++mints).padStart(4, '0')}`;
      issued.push(access_token);
      return json({ ...mintResult, access_token });
    },
    '/api/org/members': () => {
      if (renew) { renew = false; return json({}, 401); }
      if (errorRequestId !== undefined) return json({}, 500, { 'x-request-id': errorRequestId });
      return json([]);
    },
  });
  await client.connect();
  // More than the 128 remembered short secrets, without depending on whether
  // the configured SDK key itself occupies one of those history slots.
  for (let rotation = 0; rotation < 130; rotation++) {
    renew = true;
    assert.deepEqual(await client.members.list(), []);
  }
  assert.equal(mints, 131);
  for (const requestId of [`prefix-${issued[0]}-suffix`, `prefix-${issued.at(-1)}-suffix`,
    'otherwise-safe-request-after-history-saturation']) {
    errorRequestId = requestId;
    await assert.rejects(client.members.list(), error => {
      errorCode('SERVER', 500)(error);
      assert.equal(error.requestId, undefined,
        'After the bounded history fills, remote request IDs must stay suppressed');
      const exposed = inspect(error, { showHidden: true, depth: null }) + JSON.stringify(error);
      for (const secret of issued) assert.equal(exposed.includes(secret), false);
      return true;
    });
  }
});
