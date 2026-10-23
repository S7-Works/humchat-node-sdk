import test from 'node:test';
import assert from 'node:assert/strict';
import { HumChat, NotFoundError, ProtocolError } from '../../dist/index.js';

const orgId = '01234567-89ab-cdef-0123-456789abcdef';
const teamId = 'team:Operations.v2';
const actorId = 'synthetic-channel-reader';
const email = 'synthetic.channel.reader@example.invalid';
const token = 'synthetic-channel-reader-token';
const sdkKey = 'hsk_synthetic_channel_reader_key';
const capabilities = {
  sdk_protocol_version: 1,
  authentication: {
    user_sessions: true, service_identities: false, api_key_exchange: false,
    app_binding_required: false, mock_authentication: false,
    sdk_key_user_tokens: {
      endpoint: '/api/sdk/auth/user-token', key_header: 'X-HumChat-Sdk-Key', configured: true,
    },
  },
  messaging: {
    send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
    durable_replay: false, websocket_scope: 'channel',
  },
};

function responseForTeam(selectedTeam = teamId) {
  return {
    org_uuid: orgId, team_uuid: selectedTeam, team_name: 'Synthetic team', channels: [{
      channel_id: `${orgId}:${selectedTeam}:general`, org_uuid: orgId,
      team_uuid: selectedTeam, slug: 'general', display_name: 'General', is_private: false,
    }],
  };
}

const json = value => new Response(JSON.stringify(value), {
  headers: { 'content-type': 'application/json' },
});

function fixture(t, authentication, handleChannels) {
  const requests = [];
  const client = new HumChat({
    orgId, baseUrl: 'https://humchat.example.invalid',
    ...(authentication === 'sdkKey'
      ? { auth: { sdkKey, actAs: email } }
      : { tokenProvider: async () => ({ token }) }),
    retry: { maxRetries: 2, initialDelayMs: 0, maxDelayMs: 0, totalTimeoutMs: 5_000 },
    fetch: async (input, init) => {
      const url = new URL(input);
      const method = init.method ?? 'GET';
      requests.push({ path: url.pathname, search: url.search, method });
      const headers = new Headers(init.headers);
      if (url.pathname === '/api/sdk/auth/user-token') {
        assert.equal(authentication, 'sdkKey');
        assert.equal(method, 'POST');
        assert.equal(headers.get('x-humchat-sdk-key'), sdkKey);
        assert.equal(headers.has('authorization'), false);
        return json({ access_token: token, token_type: 'Bearer', expires_in: 900 });
      }
      assert.equal(method, 'GET');
      assert.equal(headers.get('authorization'), `Bearer ${token}`);
      assert.equal(headers.has('x-humchat-sdk-key'), false);
      if (url.pathname === '/me') return json({
        user_uuid: actorId, id: 1, org: orgId, email, roles: ['member'], scopes: [],
        expires_at: Math.floor(Date.now() / 1_000) + 900, app_uuid: null,
      });
      if (url.pathname === '/api/sdk/capabilities') return json(capabilities);
      assert.equal(url.pathname, '/api/workspace/channels', 'Channel lookup must not call fallback routes');
      return handleChannels(url);
    },
  });
  t.after(() => client.close());
  return { client, requests };
}

for (const authentication of ['tokenProvider', 'sdkKey']) {
  test(`channel lookup sends the exact selected team as one encoded query (${authentication})`, async t => {
    const { client, requests } = fixture(t, authentication, url => {
      assert.deepEqual([...url.searchParams], [['team_uuid', teamId]]);
      assert.equal(url.search, '?team_uuid=team%3AOperations.v2');
      return json(responseForTeam());
    });
    const result = await client.channels.list({ teamId });
    assert.equal(result.orgId, orgId);
    assert.equal(result.teamId, teamId);
    assert.equal(result.channels[0].teamId, teamId);
    assert.equal(result.channels[0].id, `${orgId}:${teamId}:general`);
    assert.equal(requests.filter(request => request.path === '/api/workspace/channels').length, 1);
  });

  test(`omitted team remains server-selected without client discovery (${authentication})`, async t => {
    const serverSelectedTeam = `${orgId}-home`;
    const { client, requests } = fixture(t, authentication, url => {
      assert.equal(url.search, '');
      return json(responseForTeam(serverSelectedTeam));
    });
    assert.equal((await client.channels.list()).teamId, serverSelectedTeam);
    assert.equal(requests.filter(request => request.path === '/api/workspace/channels').length, 1);
  });

  test(`missing workspace remains NotFoundError with no retry or fallback writes (${authentication})`, async t => {
    const privateBody = 'Synthetic workspace lookup detail must stay private';
    const { client, requests } = fixture(t, authentication, url => {
      assert.equal(url.search, '');
      return new Response(privateBody, { status: 404 });
    });
    await assert.rejects(client.channels.list(), error => {
      assert.ok(error instanceof NotFoundError);
      assert.equal(error.status, 404);
      assert.equal(error.retryable, false);
      assert.equal(error.message.includes(privateBody), false);
      return true;
    });
    assert.deepEqual(requests.map(({ method, path }) => `${method} ${path}`), [
      ...(authentication === 'sdkKey' ? ['POST /api/sdk/auth/user-token'] : []),
      'GET /me', 'GET /api/sdk/capabilities', 'GET /api/workspace/channels',
    ]);
  });
}

for (const [description, change] of [
  ['top-level organization differs', value => { value.org_uuid = 'other-org'; }],
  ['selected team differs', value => { value.team_uuid = 'other-team'; value.channels = []; }],
  ['channel organization differs', value => { value.channels[0].org_uuid = 'other-org'; }],
  ['channel team differs', value => { value.channels[0].team_uuid = 'other-team'; }],
]) {
  test(`channel lookup rejects scope drift when ${description}`, async t => {
    const invalid = responseForTeam();
    change(invalid);
    const { client, requests } = fixture(t, 'sdkKey', () => json(invalid));
    await assert.rejects(client.channels.list({ teamId }), ProtocolError);
    assert.equal(requests.filter(request => request.path === '/api/workspace/channels').length, 1);
  });
}
