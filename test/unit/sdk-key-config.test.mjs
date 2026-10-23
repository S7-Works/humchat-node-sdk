import test from 'node:test';
import assert from 'node:assert/strict';
import { inspect } from 'node:util';
import { HumChat, ConfigurationError, ValidationError, UnsupportedFeatureError } from '../../dist/index.js';

// These are synthetic fixtures only; configuration must never perform I/O.
const orgId = '01234567-89ab-cdef-0123-456789abcdef';
const sdkKey = 'hsk_synthetic_phase2_config_secret';
const actAs = 'synthetic.actor@example.invalid';
const token = 'synthetic-phase2-provider-token';
const tokenProvider = async () => ({ token });
const auth = (extra = {}) => ({ sdkKey, actAs, ...extra });
const options = (extra = {}) => ({ orgId, baseUrl: 'https://humchat.example.invalid', auth: auth(), ...extra });

function assertNoCredentials(value, credentials = [sdkKey, actAs]) {
  for (const credential of credentials) {
    assert.equal(value.includes(credential), false, 'Credential must not appear in an observable representation');
  }
}

// Each override is synchronous and restored even when an assertion fails.
function withGlobals(values, run) {
  const previous = new Map(Object.keys(values).map(name => [name, Object.getOwnPropertyDescriptor(globalThis, name)]));
  try {
    for (const [name, value] of Object.entries(values)) {
      Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
    }
    return run();
  } finally {
    for (const [name, descriptor] of previous) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor);
      else delete globalThis[name];
    }
  }
}

test('SDK-key auth and tokenProvider are mutually exclusive, including explicit undefined fields', () => {
  for (const extra of [
    { tokenProvider },
    { tokenProvider: undefined },
    { tokenProvider, auth: undefined },
    { tokenProvider, auth: null },
  ]) assert.throws(() => new HumChat(options(extra)), ValidationError);

  assert.throws(() => new HumChat({ orgId, baseUrl: 'https://humchat.example.invalid' }), ConfigurationError);
  assert.throws(() => new HumChat({ orgId, baseUrl: 'https://humchat.example.invalid', tokenProvider: null }), ConfigurationError);
});

test('top-level apiKey remains explicitly unsupported in either authentication mode', () => {
  for (const apiKey of ['synthetic-unsupported-api-key', undefined]) {
    assert.throws(() => new HumChat(options({ apiKey })), UnsupportedFeatureError);
    assert.throws(() => new HumChat({ orgId: 'legacy-org', baseUrl: 'http://example.invalid', tokenProvider, apiKey }), UnsupportedFeatureError);
  }
});

test('SDK-key auth rejects malformed nested configuration and unknown fields without echoing credentials', () => {
  for (const nested of [
    null, false, 42, 'synthetic-invalid-auth', [], {},
    { sdkKey }, { actAs },
    auth({ sdkKey: undefined }), auth({ actAs: undefined }),
    auth({ apiKey: 'synthetic-wrong-field' }), auth({ tokenProvider }), auth({ misspelled: true }),
  ]) {
    assert.throws(() => new HumChat(options({ auth: nested })), error => {
      assert.ok(error instanceof ValidationError);
      assertNoCredentials(inspect(error, { showHidden: true, depth: null }));
      assertNoCredentials(JSON.stringify(error));
      return true;
    });
  }
});

test('SDK keys require a nonempty header-safe base64url suffix and at most 128 total characters', async t => {
  for (const key of ['hsk_a', 'hsk_Aa0_-', `hsk_${'a'.repeat(124)}`]) {
    const client = new HumChat(options({ auth: auth({ sdkKey: key }) }));
    t.after(() => client.close());
    assert.equal(client.getConfig().authentication, 'sdkKey');
  }
  for (const key of [
    undefined, null, 17, '', 'hsk_', 'HSK_synthetic', 'synthetic',
    `hsk_${'a'.repeat(125)}`, ` ${sdkKey}`, `${sdkKey} `, 'hsk_one two',
    'hsk_one\ttwo', 'hsk_one\r\ntwo', 'hsk_one\u0000two', 'hsk_one\u007ftwo',
    'hsk_one+two', 'hsk_one/two', 'hsk_one=two', 'hsk_one.two', 'hsk_\u00e9',
  ]) {
    assert.throws(() => new HumChat(options({ auth: auth({ sdkKey: key }) })), error => {
      assert.ok(error instanceof ValidationError);
      if (typeof key === 'string' && key.length > 10) assertNoCredentials(inspect(error, { showHidden: true, depth: null }), [key, actAs]);
      return true;
    });
  }
});

test('SDK-key actor emails accept server normalization and enforce shape and a 254-byte limit', async t => {
  const maximumEmail = `${'a'.repeat(242)}@example.com`;
  assert.equal(Buffer.byteLength(maximumEmail, 'utf8'), 254);
  for (const email of [actAs, ' \tSYNTHETIC.Actor@EXAMPLE.INVALID\r\n', maximumEmail, '\u00c4CTOR@example.invalid']) {
    const client = new HumChat(options({ auth: auth({ actAs: email }) }));
    t.after(() => client.close());
    assert.equal(client.getConfig().authentication, 'sdkKey');
    assertNoCredentials(inspect(client, { showHidden: true, depth: null }), [email, email.trim().replace(/[A-Z]/g, character => character.toLowerCase())]);
  }
  for (const email of [
    undefined, null, 42, '', ' \t ', 'missing-at.example.invalid', '@example.invalid', 'actor@',
    'actor@@example.invalid', 'actor@localhost', 'actor@.invalid', 'actor@example.', 'actor@example..invalid',
    'actor name@example.invalid', 'actor@exam ple.invalid', 'actor\nname@example.invalid',
    'actor\u0000@example.invalid', 'actor\u007f@example.invalid', 'actor\u2003name@example.invalid',
    'actor\u0085name@example.invalid', 'actor\u009fname@example.invalid',
    `a${maximumEmail}`, `${'\u00e9'.repeat(122)}@example.com`,
  ]) {
    assert.throws(() => new HumChat(options({ auth: auth({ actAs: email }) })), error => {
      assert.ok(error instanceof ValidationError);
      assertNoCredentials(inspect(error, { showHidden: true, depth: null }), [sdkKey]);
      if (typeof email === 'string' && email.length > 10) assertNoCredentials(JSON.stringify(error), [email]);
      return true;
    });
  }
});

test('SDK-key organizations require canonical UUID structure, normalize case, and allow any UUID version', async t => {
  for (const value of [orgId, orgId.toUpperCase(), '00000000-0000-0000-0000-000000000000']) {
    const client = new HumChat(options({ orgId: value }));
    t.after(() => client.close());
    assert.equal(client.getConfig().orgId, value.toLowerCase());
  }
  for (const value of [undefined, null, 123, '', 'legacy-org', ` ${orgId}`, `${orgId} `,
    orgId.replaceAll('-', ''), `{${orgId}}`, `urn:uuid:${orgId}`, 'g1234567-89ab-cdef-0123-456789abcdef']) {
    assert.throws(() => new HumChat(options({ orgId: value })), ValidationError);
  }
});

test('SDK-key auth requires HTTPS except the three literal loopback hosts', async t => {
  for (const baseUrl of [
    'https://humchat.example.invalid', 'http://localhost', 'http://localhost:43123/',
    'http://127.0.0.1:43123', 'http://[::1]:43123/',
  ]) {
    const client = new HumChat(options({ baseUrl }));
    t.after(() => client.close());
    assert.equal(client.getConfig().baseUrl, new URL(baseUrl).origin);
  }
  for (const baseUrl of [
    'http://example.invalid', 'http://192.168.1.2', 'http://0.0.0.0', 'http://127.0.0.2',
    'http://localhost.example.invalid', 'http://localhost.', 'http://127.1', 'http://2130706433',
    'http://0x7f000001', 'http://0177.0.0.1', 'http://[0:0:0:0:0:0:0:1]',
    'http://[::ffff:127.0.0.1]',
  ]) assert.throws(() => new HumChat(options({ baseUrl })), ConfigurationError);
});

test('SDK-key auth retains the existing origin-only URL restrictions', () => {
  for (const baseUrl of [
    undefined, 'not-a-url', 'file:///synthetic', 'https://user:synthetic@example.invalid',
    'https://example.invalid/api', 'https://example.invalid/?key=synthetic', 'https://example.invalid/#synthetic',
  ]) assert.throws(() => new HumChat(options({ baseUrl })), ConfigurationError);
});

test('SDK-key auth rejects window, document, and worker browser contexts before I/O', () => {
  let calls = 0;
  const fetch = async () => { calls++; throw new Error('Unexpected browser fetch'); };
  for (const globals of [
    { window: {} },
    { document: {} },
    { WorkerGlobalScope: class WorkerGlobalScope {} },
    { self: globalThis, importScripts() {} },
  ]) {
    withGlobals(globals, () => assert.throws(() => new HumChat(options({ fetch })), UnsupportedFeatureError));
  }
  assert.equal(calls, 0);
});

test('a Node navigator does not turn the Node runtime into a browser', async t => {
  const client = withGlobals({ navigator: { userAgent: 'Node.js/22' } }, () => new HumChat(options()));
  t.after(() => client.close());
  assert.equal(client.getConfig().authentication, 'sdkKey');
});

test('SDK-key construction is lazy and credentials are absent from public config and inspection', async t => {
  let calls = 0;
  const inputAuth = auth({ actAs: ' SYNTHETIC.Actor@EXAMPLE.INVALID ' });
  const originalAuth = { ...inputAuth };
  const client = new HumChat(options({ auth: inputAuth, fetch: async () => { calls++; throw new Error('Unexpected configuration fetch'); } }));
  t.after(() => client.close());
  const safe = client.getConfig();
  assert.equal(safe.authentication, 'sdkKey');
  assert.equal(safe.orgId, orgId);
  assert.equal(safe.timeout, 10_000);
  assert.equal(safe.retry.maxRetries, 3);
  assert.deepEqual(inputAuth, originalAuth, 'Normalization must not modify caller-owned options');
  for (const field of ['auth', 'sdkKey', 'actAs', 'tokenProvider']) {
    assert.equal(field in safe, false);
    assert.equal(field in client.realtime.config, false);
  }
  const secrets = [sdkKey, actAs, originalAuth.actAs, originalAuth.actAs.trim()];
  for (const value of [safe, client, client.realtime.config]) {
    assertNoCredentials(JSON.stringify(value), secrets);
    assertNoCredentials(inspect(value, { showHidden: true, depth: null, maxStringLength: null }), secrets);
  }
  safe.retry.maxRetries = 8;
  safe.realtime.maxChannels = 1;
  safe.recovery.maxPages = 1;
  assert.equal(client.getConfig().retry.maxRetries, 3);
  assert.equal(client.getConfig().realtime.maxChannels, 50);
  assert.equal(client.getConfig().recovery.maxPages, 3);
  assert.equal(calls, 0);
  await client.close();
  assert.equal(calls, 0);
});

test('SDK-key configuration stays lazy until the first operation attempts the HumChat exchange', async t => {
  let calls = 0;
  const client = new HumChat(options({ retry: { maxRetries: 0 }, fetch: async (url, init) => {
    calls++;
    assert.equal(new URL(url).pathname, '/api/sdk/auth/user-token');
    assert.equal(new Headers(init.headers).get('X-HumChat-Sdk-Key'), sdkKey);
    assert.equal(new Headers(init.headers).has('Authorization'), false);
    return new Response('Synthetic unavailable endpoint', { status: 404 });
  } }));
  t.after(() => client.close());
  assert.equal(calls, 0);
  await assert.rejects(client.connect(), error => {
    assert.ok(error instanceof UnsupportedFeatureError);
    assertNoCredentials(inspect(error, { showHidden: true, depth: null }));
    return true;
  });
  assert.equal(calls, 1);
});

test('tokenProvider preserves opaque organization IDs, HTTP origins, laziness, and normal authenticated requests', async t => {
  const legacyOrg = 'org-mock-1';
  let acquisitions = 0;
  const paths = [];
  const client = new HumChat({
    orgId: legacyOrg,
    baseUrl: 'http://humchat.example.invalid',
    tokenProvider: async context => { acquisitions++; assert.equal(context.reason, 'initial'); return { token }; },
    fetch: async (url, init) => {
      const path = new URL(url).pathname;
      paths.push(path);
      assert.equal(init.headers.get('Authorization'), `Bearer ${token}`);
      assert.equal(init.headers.has('X-HumChat-Sdk-Key'), false);
      if (path === '/me') return Response.json({ user_uuid: 'synthetic-actor', org: legacyOrg, email: actAs, roles: ['member'] });
      if (path === '/api/sdk/capabilities') return Response.json({
        sdk_protocol_version: 1,
        authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
        messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: false, websocket_scope: 'channel' },
      });
      assert.equal(path, '/api/org/members');
      return Response.json([]);
    },
  });
  t.after(() => client.close());
  assert.equal(client.getConfig().authentication, 'tokenProvider');
  assert.equal(client.getConfig().orgId, legacyOrg);
  assert.equal(acquisitions, 0);
  assert.deepEqual(paths, []);
  assert.equal((await client.connect()).actorId, 'synthetic-actor');
  assert.equal((await client.getCapabilities()).protocolVersion, 1);
  assert.deepEqual(await client.members.list(), []);
  assert.equal(acquisitions, 1);
  assert.deepEqual(paths, ['/me', '/api/sdk/capabilities', '/api/org/members']);
});
