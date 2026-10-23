import test from 'node:test';
import assert from 'node:assert/strict';
import { HumChat } from '../../dist/index.js';

// Synthetic identities only. Virtual time keeps outage tests independent of host load.
const orgId = '01234567-89ab-cdef-0123-456789abcdef';
const actorId = 'synthetic-cooldown-actor';
const actAs = 'synthetic.cooldown@example.invalid';
const mintPath = '/api/sdk/auth/user-token';
const identity = { user_uuid: actorId, org: orgId, email: actAs, roles: ['member'], expires_at: null };
const capabilities = { sdk_protocol_version: 1, authentication: {
  user_sessions: true, service_identities: false, api_key_exchange: false,
  app_binding_required: false, mock_authentication: false,
  sdk_key_user_tokens: { endpoint: mintPath, key_header: 'X-HumChat-Sdk-Key', configured: true },
}, messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
  durable_replay: false, websocket_scope: 'channel' } };
const mintResult = { access_token: 'synthetic-cooldown-token', token_type: 'Bearer', expires_in: 900 };
const json = (value, status = 200, headers = {}) => Response.json(value, { status, headers });

function fixture(t, handlers = {}, options = {}, random = 0) {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 1_800_000_000_000 });
  t.mock.method(Math, 'random', () => random);
  const requests = [];
  const fetch = async (input, init) => {
    const path = new URL(input).pathname;
    const request = { path, at: Date.now(), signal: init.signal };
    requests.push(request);
    if (handlers[path]) return handlers[path](request, requests);
    if (path === mintPath) return json(mintResult);
    if (path === '/me') return json(identity);
    if (path === '/api/sdk/capabilities') return json(capabilities);
    assert.equal(path, '/api/org/members');
    return json([]);
  };
  const config = { orgId, baseUrl: 'https://humchat.example.invalid',
    auth: { sdkKey: 'hsk_synthetic_cooldown_key', actAs }, fetch, timeout: 120_000,
    retry: { maxRetries: 0, totalTimeoutMs: 120_000 }, ...options };
  if ('tokenProvider' in options) delete config.auth;
  const client = new HumChat(config);
  t.after(() => client.close());
  return { client, requests, mints: () => requests.filter(request => request.path === mintPath) };
}

function observe(promise) {
  const result = { state: 'pending', value: undefined, error: undefined };
  result.done = promise.then(value => { result.state = 'fulfilled'; result.value = value; },
    error => { result.state = 'rejected'; result.error = error; });
  return result;
}

async function drain(t) {
  // Fetch admission, JSON streams, and session verification each cross promise boundaries.
  for (let turn = 0; turn < 12; turn++) {
    t.mock.timers.tick(0);
    await new Promise(setImmediate);
  }
}

async function advance(t, milliseconds) {
  t.mock.timers.tick(milliseconds);
  await drain(t);
}

async function rejected(t, result, code) {
  await drain(t);
  assert.equal(result.state, 'rejected');
  assert.equal(result.error.code, code);
  return result.error;
}

test('successive exhausted SDK-key acquisitions back off across flights and cap the shared delay', async t => {
  const { client, mints } = fixture(t, { [mintPath]: () => json({}, 503) });
  await rejected(t, observe(client.connect()), 'SERVER');
  const minimumDelays = [1_000, 2_000, 4_000, 8_000, 15_000, 15_000];
  for (const delay of minimumDelays) {
    const count = mints().length;
    const next = observe(client.connect());
    await drain(t);
    assert.equal(mints().length, count, 'Clearing a failed flight must not permit an immediate new mint');
    await advance(t, delay - 1);
    assert.equal(mints().length, count, 'Cooldown must protect its entire lower-bound interval');
    assert.equal(next.state, 'pending');
    await advance(t, 1);
    await rejected(t, next, 'SERVER');
    assert.equal(mints().length, count + 1);
  }
});

test('SDK-key acquisition cooldown includes jitter up to its upper bound', async t => {
  const { client, mints } = fixture(t, { [mintPath]: () => json({}, 502) }, {}, 0.999999);
  await rejected(t, observe(client.connect()), 'SERVER');
  const next = observe(client.connect());
  await advance(t, 1_998);
  assert.equal(mints().length, 1);
  assert.equal(next.state, 'pending');
  await advance(t, 2);
  await rejected(t, next, 'SERVER');
  assert.equal(mints().length, 2);
  assert.ok(mints()[1].at - mints()[0].at >= 1_999);
  assert.ok(mints()[1].at - mints()[0].at <= 2_000);
});

test('a retryable network failure cannot trigger immediate SDK-key minting from the next caller', async t => {
  let calls = 0;
  const { client, mints } = fixture(t, { [mintPath]: () => {
    if (++calls === 1) throw new Error('synthetic network failure');
    return json(mintResult);
  } });
  await rejected(t, observe(client.connect()), 'NETWORK');
  const next = observe(client.connect());
  await advance(t, 999);
  assert.equal(mints().length, 1);
  await advance(t, 1);
  assert.equal(next.state, 'fulfilled');
  assert.equal(mints().length, 2);
});

test('an attempted mint timeout starts cooldown at failure time and can recover afterward', async t => {
  let calls = 0;
  const { client, mints } = fixture(t, { [mintPath]: () => ++calls === 1 ? new Promise(() => {}) : json(mintResult) },
    { timeout: 200 });
  const initial = observe(client.connect());
  await drain(t);
  assert.equal(mints().length, 1);
  await advance(t, 200);
  await rejected(t, initial, 'TIMEOUT');
  assert.equal(mints()[0].signal.aborted, true);
  const next = observe(client.connect());
  await advance(t, 999);
  assert.equal(mints().length, 1);
  await advance(t, 1);
  assert.equal(next.state, 'fulfilled');
  assert.equal(mints().length, 2);
  assert.equal(mints()[1].at - mints()[0].at, 1_200);
});

for (const status of [429, 502, 503]) {
  test(`SDK-key ${status} Retry-After remains effective after the failed flight is cleared`, async t => {
    const { client, mints } = fixture(t, { [mintPath]: () => json({}, status, { 'retry-after': '7' }) });
    const code = status === 429 ? 'RATE_LIMIT' : 'SERVER';
    await rejected(t, observe(client.connect()), code);
    const next = observe(client.connect());
    await advance(t, 6_999);
    assert.equal(mints().length, 1);
    assert.equal(next.state, 'pending');
    await advance(t, 1);
    await rejected(t, next, code);
    assert.equal(mints().length, 2);
  });
}

for (const path of ['/me', '/api/sdk/capabilities']) {
  test(`transient ${path} failure also limits subsequent SDK-key minting`, async t => {
    const { client, requests, mints } = fixture(t, { [path]: () => json({}, 503) });
    await rejected(t, observe(client.connect()), 'SERVER');
    assert.equal(mints().length, 1);
    const next = observe(client.connect());
    await advance(t, 999);
    assert.equal(mints().length, 1);
    await advance(t, 1);
    await rejected(t, next, 'SERVER');
    assert.equal(mints().length, 2);
    assert.equal(requests.filter(request => request.path === path).length, 2);
  });
}

test('concurrent HTTP and connect callers share one cooldown and one recovered acquisition', async t => {
  let calls = 0;
  const { client, mints } = fixture(t, { [mintPath]: () => ++calls === 1 ? json({}, 503) : json(mintResult) });
  await rejected(t, observe(client.connect()), 'SERVER');
  const pending = [observe(client.connect()), observe(client.getCapabilities()), observe(client.members.list()), observe(client.connect())];
  await advance(t, 999);
  assert.equal(mints().length, 1);
  assert.ok(pending.every(result => result.state === 'pending'));
  await advance(t, 1);
  assert.ok(pending.every(result => result.state === 'fulfilled'));
  assert.equal(mints().length, 2);
});

test('aborting one cooldown waiter leaves the shared acquisition available to another caller', async t => {
  let calls = 0;
  const { client, mints } = fixture(t, { [mintPath]: () => ++calls === 1 ? json({}, 503) : json(mintResult) });
  await rejected(t, observe(client.connect()), 'SERVER');
  const controller = new AbortController();
  const cancelled = observe(client.connect({ signal: controller.signal }));
  const survivor = observe(client.connect());
  controller.abort(new Error('synthetic caller cancellation'));
  await rejected(t, cancelled, 'ABORT');
  await advance(t, 999);
  assert.equal(mints().length, 1);
  assert.equal(survivor.state, 'pending');
  await advance(t, 1);
  assert.equal(survivor.state, 'fulfilled');
  assert.equal(mints().length, 2);
});

for (const action of ['close', 'revoke']) {
  test(`${action} cancels a pending SDK-key cooldown without transmitting another key`, async t => {
    const { client, mints } = fixture(t, { [mintPath]: () => json({}, 503) });
    await rejected(t, observe(client.connect()), 'SERVER');
    const waiting = observe(client.connect());
    await drain(t);
    if (action === 'close') await client.close();
    else client.realtime.revoke('synthetic terminal revocation');
    await rejected(t, waiting, action === 'close' ? 'ABORT' : 'SESSION_REVOKED');
    await advance(t, 30_000);
    assert.equal(mints().length, 1);
    await rejected(t, observe(client.connect()), action === 'close' ? 'ABORT' : 'SESSION_REVOKED');
    assert.equal(mints().length, 1);
  });
}

test('cooldown consumes the acquisition deadline and a delay-only timeout does not extend it', async t => {
  let calls = 0;
  const { client, mints } = fixture(t, { [mintPath]: () => ++calls === 1 ? json({}, 503) : json(mintResult) },
    { retry: { maxRetries: 0, totalTimeoutMs: 600 } });
  await rejected(t, observe(client.connect()), 'SERVER');
  const timedOut = observe(client.connect());
  await advance(t, 599);
  assert.equal(timedOut.state, 'pending');
  assert.equal(mints().length, 1);
  await advance(t, 1);
  await rejected(t, timedOut, 'TIMEOUT');
  assert.equal(mints().length, 1, 'A timeout spent waiting must not attempt minting');
  const recovered = observe(client.connect());
  await advance(t, 399);
  assert.equal(recovered.state, 'pending');
  assert.equal(mints().length, 1);
  await advance(t, 1);
  assert.equal(recovered.state, 'fulfilled', 'The original cooldown deadline must survive a waiting-only timeout');
  assert.equal(mints().length, 2);
});

test('a successful verified session resets accumulated SDK-key acquisition backoff', async t => {
  let fail = true;
  const { client, mints } = fixture(t, { [mintPath]: () => fail ? json({}, 503) : json(mintResult) });
  await rejected(t, observe(client.connect()), 'SERVER');
  let next = observe(client.connect());
  await advance(t, 1_000);
  await rejected(t, next, 'SERVER');
  fail = false;
  next = observe(client.connect());
  await advance(t, 2_000);
  assert.equal(next.state, 'fulfilled');
  const session = await client.realtime.sessions.get(new AbortController().signal);
  client.realtime.sessions.expire(session);
  fail = true;
  await rejected(t, observe(client.connect()), 'SERVER');
  const count = mints().length;
  next = observe(client.connect());
  await advance(t, 999);
  assert.equal(mints().length, count);
  await advance(t, 1);
  await rejected(t, next, 'SERVER');
  assert.equal(mints().length, count + 1, 'A new outage must restart at the initial cooldown');
});

for (const [status, code] of [[400, 'VALIDATION'], [401, 'AUTHENTICATION'], [403, 'PERMISSION']]) {
  test(`SDK-key ${status} stays terminal for its request without a transient retry loop`, async t => {
    const { client, mints } = fixture(t, { [mintPath]: () => json({}, status) },
      { retry: { maxRetries: 3, totalTimeoutMs: 120_000 } });
    await rejected(t, observe(client.connect()), code);
    assert.equal(mints().length, 1);
    await rejected(t, observe(client.connect()), code);
    // Bad input may be requested again explicitly; denied immutable credentials remain blocked.
    assert.equal(mints().length, status === 400 ? 2 : 1);
    await advance(t, 30_000);
    assert.equal(mints().length, status === 400 ? 2 : 1);
  });
}

test('legacy tokenProvider acquisition failures do not acquire SDK-key cooldown behavior', async t => {
  let providers = 0;
  const { client, requests, mints } = fixture(t, { '/me': () => json({}, 503) }, {
    tokenProvider: async () => { providers++; return { token: mintResult.access_token }; },
  });
  await rejected(t, observe(client.connect()), 'SERVER');
  await rejected(t, observe(client.connect()), 'SERVER');
  assert.equal(providers, 2);
  assert.equal(mints().length, 0);
  assert.equal(requests.length, 2);
  assert.equal(requests[0].at, requests[1].at, 'Legacy caller-controlled acquisition timing must be preserved');
});
