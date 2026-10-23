import test from 'node:test';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createServer } from 'node:http';
import { gunzipSync } from 'node:zlib';
import { mkdir, mkdtemp, readFile, writeFile, lstat, readdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve, relative } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { WebSocket, WebSocketServer } from 'ws';
import { org as webhookOrg, channel as webhookChannel, subscriptionId as webhookSubscription, secret as webhookSecret,
  previousSecret as webhookPreviousSecret, envelope as webhookEnvelope, signature as webhookSignature } from '../unit/webhooks.fixtures.mjs';

const root = fileURLToPath(new URL('../../', import.meta.url));
const artifacts = process.env.HUMCHAT_SDK_ARTIFACTS_DIR ? resolve(process.env.HUMCHAT_SDK_ARTIFACTS_DIR)
  : fileURLToPath(new URL('../../target/package/', import.meta.url));
const npm = process.env.npm_execpath ?? join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js');
const fixtureToken = 'synthetic-installed-example-token';
const sanitized = value => value.replaceAll(fixtureToken, '[REDACTED]').replaceAll(webhookSecret, '[REDACTED]')
  .replaceAll(webhookPreviousSecret, '[REDACTED]').replace(/Bearer\s+[^\s"']+/g, 'Bearer [REDACTED]');
function run(args, cwd) {
  const result = spawnSync(process.execPath, args, { cwd, encoding: 'utf8', windowsHide: true, timeout: 120_000 });
  assert.equal(result.error, undefined, 'Consumer command could not run');
  assert.equal(result.status, 0, sanitized((result.stdout ?? '') + (result.stderr ?? '')));
  return result.stdout;
}

// npm emits regular ustar files. This reader lists only; it never extracts paths.
async function archiveFiles(path) {
  const stat = await lstat(path);
  assert.ok(stat.isFile() && path.endsWith('.tgz') && stat.size <= 16 * 1024 * 1024, 'Expected a bounded regular .tgz');
  const compressed = await readFile(path);
  const archive = gunzipSync(compressed, { maxOutputLength: 64 * 1024 * 1024 });
  const files = [];
  const entries = [];
  const seen = new Set();
  let offset = 0;
  let ended = false;
  const text = (header, start, length) => header.subarray(start, start + length).toString('utf8').replace(/\0.*$/s, '');
  while (offset + 512 <= archive.length) {
    const header = archive.subarray(offset, offset + 512);
    if (header.every(byte => byte === 0)) {
      assert.ok(archive.length - offset >= 1024 && archive.subarray(offset).every(byte => byte === 0), 'Trailing tar content is not padding');
      ended = true;
      break;
    }
    assert.equal(text(header, 257, 5), 'ustar', 'Only npm ustar archives are supported');
    let checksum = 0;
    for (let index = 0; index < 512; index++) checksum += index >= 148 && index < 156 ? 32 : header[index];
    assert.equal(checksum, Number.parseInt(text(header, 148, 8).trim(), 8), 'Tar header checksum mismatch');
    const kind = text(header, 156, 1);
    assert.ok(kind === '' || kind === '0', 'Tar links, directories and extended headers are not accepted');
    const prefix = text(header, 345, 155);
    const name = (prefix ? prefix + '/' : '') + text(header, 0, 100);
    assert.ok(name.startsWith('package/') && /^[A-Za-z0-9_./-]+$/.test(name) && !name.split('/').some(part => !part || part === '.' || part === '..'), 'Unsafe archive path');
    assert.ok(!seen.has(name), 'Duplicate archive path'); seen.add(name);
    const sizeText = text(header, 124, 12).trim();
    assert.match(sizeText, /^[0-7]+$/);
    const size = Number.parseInt(sizeText, 8);
    assert.ok(Number.isSafeInteger(size) && size >= 0 && offset + 512 + size <= archive.length, 'Truncated tar entry');
    files.push(name.slice('package/'.length));
    entries.push({ path: name.slice('package/'.length), size,
      sha256: createHash('sha256').update(archive.subarray(offset + 512, offset + 512 + size)).digest('hex') });
    assert.ok(files.length <= 1000, 'Unexpected archive file count');
    offset += 512 + Math.ceil(size / 512) * 512;
  }
  assert.ok(ended && files.length > 0, 'Truncated or empty tar archive');
  return { files: files.sort(), entries: entries.sort((a, b) => a.path.localeCompare(b.path)), sha256: createHash('sha256').update(compressed).digest('hex') };
}

async function installedFiles(directory, prefix = '') {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    assert.ok(!entry.isSymbolicLink(), 'Installed package contains a symbolic link');
    if (entry.isDirectory()) files.push(...await installedFiles(join(directory, entry.name), prefix + entry.name + '/'));
    else files.push(prefix + entry.name);
  }
  return files.sort();
}

async function until(predicate, label, timeout = 8000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) { if (await predicate()) return; await sleep(10); }
  assert.fail('Timed out: ' + label);
}

function example(t, consumer, filename, environment, args = []) {
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([name]) => /^(?:path|systemroot|comspec|temp|tmp|home)$/i.test(name)));
  const child = spawn(process.execPath, [join(consumer, 'node_modules/@humchat/node/examples', filename), ...args],
    { cwd: consumer, env: { ...inherited, ...environment }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  let output = '';
  const append = chunk => { output = (output + chunk.toString()).slice(-64 * 1024); };
  child.stdout.on('data', append); child.stderr.on('data', append);
  const timer = setTimeout(() => child.kill('SIGKILL'), 20_000);
  const done = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => { clearTimeout(timer); resolve({ code, signal, output: sanitized(output),
      credentialExposed: [fixtureToken, webhookSecret, webhookPreviousSecret].some(value => output.includes(value)) }); });
  });
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await done; });
  return { done, get output() { return sanitized(output); } };
}

async function examplePeer(t) {
  const org = 'package-org', actor = 'package-bot', user = 'package-user', team = 'team';
  const channels = [org + ':' + team + ':general', org + ':' + team + ':second'];
  const epoch = 'package-epoch', timestamp = 1_792_370_346;
  const streams = new Map(channels.map(channel => [channel, []]));
  const states = new Map(), sent = new Map(), sends = [], mutations = [], peers = [], failures = [], held = [];
  const cursor = (channel, sequence) => Buffer.from(JSON.stringify({ schema: 1, org_uuid: org, channel_id: channel, epoch, sequence: String(sequence) })).toString('base64url');
  const sequence = value => Number(JSON.parse(Buffer.from(value, 'base64url').toString()).sequence);
  const state = (channel, id, content, sender = user, thread = null) => ({ schema: 1, org_uuid: org, channel_id: channel,
    message_id: id, sender_id: sender, created_at: timestamp, thread_root_id: thread, priority: 'standard',
    original_body: content, body: content, revision: 1, updated_at: timestamp, deleted: false, reaction_revision: 0, reactions: {}, attachments: [] });
  const live = state => ({ id: state.message_id, org_uuid: org, channel_id: state.channel_id, sender_id: state.sender_id,
    content: state.body, thread_root_id: state.thread_root_id, created_at: timestamp, is_long: false, priority: 'standard' });
  function publish(channel, id, content, sender = user, thread = null) {
    const value = state(channel, id, content, sender, thread); states.set(id, value);
    const stream = streams.get(channel), seq = stream.length + 1;
    const event = { type: 'event', schema: 1, org_uuid: org, channel_id: channel, epoch, sequence: String(seq),
      event_id: epoch + ':' + seq, cursor: cursor(channel, seq), actor_id: sender, kind: 'message', state: value };
    stream.push(event);
    for (const peer of peers) if (peer.socket.readyState === WebSocket.OPEN) {
      if (peer.multiplex) {
        for (const [subscriptionId, subscribed] of peer.subscriptions) if (subscribed === channel) peer.socket.send(JSON.stringify({ type: 'watermark', schema: 1,
          org_uuid: org, channel_id: channel, subscription_id: subscriptionId, epoch, cursor: event.cursor, sequence: event.sequence, earliest_cursor: cursor(channel, 0) }));
      } else if (peer.channel === channel) peer.socket.send(JSON.stringify(peer.durable ? event : live(value)));
    }
    return event;
  }
  const respond = (response, value) => { response.writeHead(200, { 'Content-Type': 'application/json' }); response.end(JSON.stringify(value)); };
  const fixture = { holdSends: false };
  const wss = new WebSocketServer({ noServer: true });
  const server = createServer(async (request, response) => {
    try {
      assert.equal(request.headers.authorization, 'Bearer ' + fixtureToken);
      const url = new URL(request.url, 'http://127.0.0.1');
      if (url.pathname === '/me') return respond(response, { user_uuid: actor, org, id: 1, email: 'package@example.invalid', roles: ['member'], scopes: [] });
      if (url.pathname === '/api/sdk/capabilities') return respond(response, { sdk_protocol_version: 1,
        authentication: { user_sessions: true, service_identities: false, api_key_exchange: false, app_binding_required: false, mock_authentication: true },
        messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2], durable_replay: true,
          event_protocol_versions: [1], websocket_scope: 'channel', multiplexed_events: { protocol_versions: [1], max_channels: 100 } } });
      if (url.pathname === '/api/workspace/channels') {
        if (url.searchParams.has('team_uuid')) assert.equal(url.searchParams.get('team_uuid'), team);
        return respond(response, { org_uuid: org, team_uuid: team, team_name: 'Package team', channels: channels.map((channel, index) => ({
          channel_id: channel, org_uuid: org, team_uuid: team, slug: index ? 'second' : 'general', display_name: index ? 'Second' : 'General', is_private: false })) });
      }
      if (url.pathname === '/api/messages/sync') return respond(response, { schema: 2, org_uuid: org, channel_id: url.searchParams.get('channel_id'),
        thread_root_id: url.searchParams.get('thread_root_id'), messages: [], next_cursor: null });
      if (url.pathname === '/api/events/sync') {
        const channel = url.searchParams.get('channel_id'), stream = streams.get(channel), after = url.searchParams.get('after');
        const through = url.searchParams.get('through') ?? cursor(channel, stream.length);
        const rows = stream.slice(sequence(after), sequence(through)).slice(0, Number(url.searchParams.get('limit') ?? 200));
        const last = rows.at(-1)?.cursor ?? after;
        return respond(response, { schema: 1, org_uuid: org, channel_id: channel, epoch, events: rows, after_cursor: after,
          through_cursor: through, through_sequence: String(sequence(through)), next_cursor: sequence(last) === sequence(through) ? null : last });
      }
      if (url.pathname.endsWith('/state')) {
        const id = decodeURIComponent(url.pathname.split('/').at(-2));
        return respond(response, states.get(id));
      }
      let body = ''; for await (const chunk of request) body += chunk; const input = body ? JSON.parse(body) : {};
      if (url.pathname === '/api/messages/send') {
        const key = request.headers['x-idempotency-key']; sends.push({ key, body: input });
        assert.ok(channels.includes(input.channel_id)); assert.ok(key);
        if (sent.has(key)) assert.deepEqual(input, sent.get(key));
        else { sent.set(key, input); publish(input.channel_id, input.id, input.content, actor, input.thread_root_id); }
        const receipt = { status: 'ACK', message_id: input.id, created_at: timestamp, attachment_count: 0, attachment_manifest_blake3: '0'.repeat(64) };
        if (fixture.holdSends) { held.push({ response, receipt }); return; }
        return respond(response, receipt);
      }
      if (url.pathname === '/api/messages/mutate') {
        mutations.push(input); assert.equal(input.mutation.kind, 'set_reaction');
        const original = states.get(input.message_id) ?? state(input.channel_id, input.message_id, 'Existing message');
        const result = { ...original, reaction_revision: 1, reactions: { [input.mutation.emoji]: [actor] } };
        return respond(response, { schema: 1, operation_id: input.operation_id, actor_id: actor, request_digest: '0'.repeat(64), state: result });
      }
      response.writeHead(404); response.end();
    } catch (error) { failures.push(error); if (!response.headersSent) response.writeHead(500); response.end(); }
  });
  server.on('upgrade', (request, socket, head) => {
    try {
      assert.equal(request.headers.authorization, 'Bearer ' + fixtureToken);
      const url = new URL(request.url, 'http://127.0.0.1'); assert.equal(url.searchParams.has('token'), false);
      const multiplex = url.pathname === '/ws/events';
      const channel = url.searchParams.get('channel_id'); if (!multiplex) assert.ok(channels.includes(channel));
      if (multiplex) { assert.equal(url.search, ''); assert.equal(request.headers['x-humchat-stream-protocol'], 'v1'); }
      wss.handleUpgrade(request, socket, head, connected => {
        const durable = request.headers['x-humchat-event-protocol'] === 'v1';
        const peer = { socket: connected, channel, durable, multiplex, subscriptions: new Map() };
        peers.push(peer); connected.on('error', () => {});
        if (multiplex) {
          connected.send(JSON.stringify({ type: 'stream_ready', schema: 1, org_uuid: org, max_channels: 100 }));
          connected.on('message', bytes => {
            try {
              const raw = JSON.parse(bytes.toString());
              if (raw.type === 'subscribe') {
                assert.ok(channels.includes(raw.channel_id)); peer.subscriptions.set(raw.subscription_id, raw.channel_id);
                connected.send(JSON.stringify({ type: 'ready', schema: 1, org_uuid: org, channel_id: raw.channel_id,
                  subscription_id: raw.subscription_id, epoch, cursor: cursor(raw.channel_id, streams.get(raw.channel_id).length),
                  sequence: String(streams.get(raw.channel_id).length), earliest_cursor: cursor(raw.channel_id, 0) }));
              } else if (raw.type === 'unsubscribe') {
                peer.subscriptions.delete(raw.subscription_id);
                connected.send(JSON.stringify({ type: 'unsubscribed', schema: 1, subscription_id: raw.subscription_id }));
              } else assert.fail('Unexpected installed multiplex command');
            } catch (error) { failures.push(error); connected.terminate(); }
          });
        }
        if (durable) connected.send(JSON.stringify({ type: 'ready', schema: 1, org_uuid: org, channel_id: channel, epoch,
          cursor: cursor(channel, streams.get(channel).length), sequence: String(streams.get(channel).length), earliest_cursor: cursor(channel, 0) }));
      });
    } catch (error) { failures.push(error); socket.destroy(); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    for (const peer of peers) peer.socket.terminate();
    for (const pending of held) pending.response.destroy();
    server.closeAllConnections();
    await Promise.all([new Promise(resolve => server.close(resolve)), new Promise(resolve => wss.close(resolve))]);
    assert.deepEqual(failures, [], 'Controlled example peer failed');
  });
  return Object.assign(fixture, { org, actor, user, channels, sends, mutations, sent, peers, streams, publish, cursor,
    environment: { HUMCHAT_BASE_URL: 'http://127.0.0.1:' + server.address().port, HUMCHAT_ORG_ID: org,
      HUMCHAT_TOKEN: fixtureToken, HUMCHAT_CHANNEL_ID: channels[0], HUMCHAT_TEAM_ID: team },
    revoke() { for (const peer of peers) if (peer.socket.readyState === WebSocket.OPEN) peer.socket.send(JSON.stringify({ type: 'session_revoked', reason: 'fixture complete' })); },
    repeat(event) { for (const peer of peers) if (peer.channel === event.channel_id && peer.socket.readyState === WebSocket.OPEN) peer.socket.send(JSON.stringify(peer.durable ? event : live(event.state))); },
  });
}

test('packed package works for independent ESM, CJS and strict TypeScript consumers', { timeout: 180_000 }, async t => {
  await mkdir(artifacts, { recursive: true });
  const consumer = await mkdtemp(join(artifacts, 'consumer-'));
  const packed = process.env.HUMCHAT_SDK_PACKAGE_TARBALL ? undefined
    : JSON.parse(run([npm, 'pack', '--json', '--ignore-scripts', '--pack-destination', consumer], root))[0];
  const tarball = process.env.HUMCHAT_SDK_PACKAGE_TARBALL ? resolve(process.env.HUMCHAT_SDK_PACKAGE_TARBALL) : join(consumer, packed.filename);
  const archive = await archiveFiles(tarball);
  if (packed) assert.deepEqual(archive.files, packed.files.map(file => file.path).sort(), 'Archive reader must agree with npm pack metadata');
  const paths = archive.files;
  const report = { schema: 1, passed: false, runtime: { node: process.version, platform: process.platform, arch: process.arch },
    tarball: { path: tarball, relativePath: relative(artifacts, tarball), sha256: archive.sha256 },
    files: archive.entries.map(({ path, size, sha256 }) => ({ path, bytes: size, sha256 })), consumer, examples: [], checks: [] };
  await writeFile(join(artifacts, 'consumer-result.json'), JSON.stringify(report, null, 2));
  t.after(() => writeFile(join(artifacts, 'consumer-result.json'), JSON.stringify(report, null, 2)));
  for (const file of ['package.json', 'dist/index.js', 'dist/index.cjs', 'dist/index.d.ts', 'dist/index.d.cts', 'README.md', 'LICENSE',
    'examples/send.mjs', 'examples/list-channels.mjs', 'examples/react.mjs', 'examples/thread-responder.mjs', 'examples/durable-responder.mjs',
    'examples/send-and-list.cjs', 'examples/multi-channel-worker.mjs', 'examples/durable-worker.mjs', 'examples/README.md', 'examples/multiplex-worker.mjs',
    'examples/webhook-receiver.mjs', 'examples/webhook-inbox.mjs', 'docs/WEBHOOKS.md', 'examples/dm-responder.mjs', 'docs/DIRECT_MESSAGES.md']) {
    assert.ok(paths.includes(file), 'Required package file missing: ' + file);
  }
  assert.ok(!paths.some(path => /^(?:src|test|scripts|contract|node_modules)\//.test(path)), 'Package must exclude development files');
  assert.ok(!paths.some(path => /(?:^|\/)\.env(?:\.|$)/.test(path)), 'Package must exclude environment files');
  await writeFile(join(consumer, 'package.json'), JSON.stringify({ name: 'humchat-installed-consumer', version: '1.0.0', private: true, type: 'module' }));
  const ownPackage = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
  run([npm, 'install', '--ignore-scripts', '--no-audit', '--no-fund', '--package-lock=false',
    tarball, '@types/node@' + ownPackage.devDependencies['@types/node']], consumer);
  const installedRoot = join(consumer, 'node_modules/@humchat/node');
  assert.deepEqual(await installedFiles(installedRoot), archive.files, 'Installed file names must match the exact archive');
  for (const entry of archive.entries) {
    for (const location of [root, installedRoot]) {
      const path = join(location, entry.path);
      assert.ok((await lstat(path)).isFile(), 'Archive source/installed entry is not a regular file');
      const contents = await readFile(path);
      assert.equal(contents.length, entry.size, 'Archive size differs from current source or installation: ' + entry.path);
      assert.equal(createHash('sha256').update(contents).digest('hex'), entry.sha256, 'Archive content differs from current source or installation: ' + entry.path);
    }
  }
  const installed = JSON.parse(await readFile(join(consumer, 'node_modules/@humchat/node/package.json'), 'utf8'));
  assert.equal(installed.private, true);
  assert.equal(installed.license, 'UNLICENSED');
  assert.equal(installed.engines.node, '>=22');
  assert.equal(installed.exports['.'].import.types, './dist/index.d.ts');
  assert.equal(installed.exports['.'].require.types, './dist/index.d.cts');
  report.installedVersion = installed.version;
  async function check(name, options, callback) {
    if (typeof options === 'function') { callback = options; options = {}; }
    const result = { name, passed: false };
    report.checks.push(result);
    await t.test(name, options, async child => { await callback(child); result.passed = true; });
  }
  async function successfulExample(name, execution) {
    const result = await execution.done;
    const logPath = join(consumer, name.replace(/[^A-Za-z0-9._-]+/g, '-') + '.log');
    await writeFile(logPath, result.output);
    report.examples.push({ name, passed: result.code === 0 && !result.credentialExposed, exitCode: result.code, logPath });
    assert.equal(result.code, 0, result.output);
    assert.equal(result.credentialExposed, false, 'Example logged its bearer credential');
    return result.output;
  }

  const runtimeBody = `
assert.equal(typeof HumChat, 'function');
assert.equal(typeof ConflictError, 'function');
assert.equal(typeof WebhookVerificationError, 'function');
const signedBody = ${JSON.stringify(JSON.stringify(webhookEnvelope()))};
const signedHeader = ${JSON.stringify(webhookSignature(JSON.stringify(webhookEnvelope())))};
const verified = verifyWebhook(signedBody, signedHeader, ${JSON.stringify(webhookSecret)}, {now: ${webhookEnvelope().sent_at}, expectedOrgId: ${JSON.stringify(webhookOrg)}});
assert.equal(verified.event.state.body, 'Hello webhook');
assert.throws(() => verifyWebhook(signedBody + ' ', signedHeader, ${JSON.stringify(webhookSecret)}), WebhookVerificationError);
const client = new HumChat({orgId: 'fixture-org', baseUrl: 'http://127.0.0.1:43125', tokenProvider: async () => ({token: 'fixture-token'})});
assert.equal(client.getConfig().orgId, 'fixture-org');
assert.equal(typeof client.messages.send, 'function');
assert.equal(typeof client.messages.iterateHistory, 'function');
assert.equal(typeof client.channels.list, 'function');
assert.equal(typeof client.conversations.list, 'function');
assert.equal(typeof client.realtime.subscribe, 'function');
assert.equal(typeof client.realtime.subscribeMany, 'function');
assert.equal(typeof client.events.sync, 'function');
assert.equal(typeof client.outbox.send, 'function');
assert.equal(typeof client.outbox.flush, 'function');
assert.equal(typeof client.webhooks.create, 'function');
assert.equal(typeof client.webhooks.deliveries, 'function');
assert.ok(!JSON.stringify(client.getConfig()).includes('fixture-token'));
async function verifyMultiplexCapabilities() {
  const multiplex = new HumChat({orgId: 'fixture-org', baseUrl: 'http://127.0.0.1:43125', realtime: {transport: 'multiplex'},
    tokenProvider: async () => ({token: 'fixture-token'}), fetch: async url => Response.json(new URL(url).pathname === '/me'
      ? {user_uuid: 'fixture-actor', org: 'fixture-org', email: 'fixture@example.invalid', roles: []}
      : {sdk_protocol_version: 1, authentication: {user_sessions: true, service_identities: false, api_key_exchange: false,
          app_binding_required: false, mock_authentication: true}, messaging: {send_receipt_versions: [1], message_state_versions: [1],
          history_versions: [2], durable_replay: true, websocket_scope: 'channel', event_protocol_versions: [1],
          multiplexed_events: {protocol_versions: [1], max_channels: 100},
          outgoing_webhooks: {schema_versions: [1], max_channels: 50, workspace_channels_only: true}}})});
  try {
    assert.equal(multiplex.getConfig().realtime.transport, 'multiplex');
    const capabilities = await multiplex.getCapabilities();
    assert.deepEqual(capabilities.messaging.multiplexedEvents, {protocolVersions: [1], maxChannels: 100});
    assert.deepEqual(capabilities.messaging.outgoingWebhooks, {schemaVersions: [1], maxChannels: 50, workspaceChannelsOnly: true});
    capabilities.messaging.multiplexedEvents.protocolVersions.push(99);
    assert.deepEqual((await multiplex.getCapabilities()).messaging.multiplexedEvents.protocolVersions, [1]);
  } finally { await multiplex.close(); }
}
`;
  await check('ESM runtime resolves its exported entry point', async () => {
    await writeFile(join(consumer, 'esm.mjs'), "import assert from 'node:assert/strict';\nimport { HumChat, ConflictError, verifyWebhook, WebhookVerificationError } from '@humchat/node';\n" + runtimeBody + '\nawait verifyMultiplexCapabilities();\nawait client.close();\n');
    run([join(consumer, 'esm.mjs')], consumer);
  });
  await check('CJS runtime resolves its exported entry point', async () => {
    await writeFile(join(consumer, 'commonjs.cjs'), "const assert = require('node:assert/strict');\nconst { HumChat, ConflictError, verifyWebhook, WebhookVerificationError } = require('@humchat/node');\n" + runtimeBody + '\nverifyMultiplexCapabilities().then(() => client.close()).catch(() => { process.exitCode = 1; });\n');
    run([join(consumer, 'commonjs.cjs')], consumer);
  });
  await check('ESM and CJS type consumers accept valid calls and reject invalid calls', async () => {
    const typedBody = `
const client = new HumChat({orgId: 'fixture-org', baseUrl: 'http://127.0.0.1:43125', tokenProvider: async ({reason, previous}) => {
  const allowed: 'initial' | 'expiring' | 'unauthorized' = reason;
  void allowed; void previous;
  return {token: 'fixture-token', expiresAt: 2_000_000_000};
}, recovery: {mode: 'bestEffort'}, realtime: {maxChannels: 5, connectConcurrency: 2}});
async function validCalls() {
  const identity = await client.connect();
  const capabilities: import('@humchat/node').SdkCapabilities = await client.getCapabilities();
  const multiplexed: {protocolVersions: number[]; maxChannels: number} | null = capabilities.messaging.multiplexedEvents;
  if (multiplexed) {
    const supported: boolean = multiplexed.protocolVersions.includes(1);
    const limit: number = multiplexed.maxChannels;
    // @ts-expect-error parsed channel limits are numbers
    const invalidLimit: string = multiplexed.maxChannels;
    void supported; void limit; void invalidLimit;
  }
  // @ts-expect-error absent multiplex support must be handled explicitly
  const unguarded: number = capabilities.messaging.multiplexedEvents.maxChannels;
  const transport: 'channel' | 'multiplex' = client.getConfig().realtime.transport;
  void unguarded; void transport;
  const actor: string = identity.actorId;
  const listing = await client.channels.list({teamId: 'team'});
  const channel: string = listing.channels[0]!.id;
  const sent = await client.messages.send({channelId: channel, content: 'hello'}, {idempotencyKey: 'key'});
  const accepted: 'accepted' = sent.status;
  const state = await client.messages.getState({channelId: channel, messageId: sent.id});
  await client.messages.reply(state, {content: 'reply'});
  await client.messages.edit({channelId: channel, messageId: sent.id, content: 'edited', expectedRevision: state.revision});
  await client.messages.setReaction({channelId: channel, messageId: sent.id, emoji: 'ok', present: true});
  await client.messages.delete({channelId: channel, messageId: sent.id, expectedRevision: state.revision});
  for await (const page of client.messages.iterateHistory({channelId: channel, limit: 10})) {
    const cursor: string | null = page.nextCursor;
    void cursor;
  }
  void actor; void accepted;
}
void validCalls;
async function validSubscriptions() {
  const subscription = client.realtime.subscribe('fixture-org:team:general');
  subscription.on('message', async ({message, source, replayed}) => {
    const messageId: string = message.id;
    const replayFlag: boolean = replayed;
    void messageId; void replayFlag; void source;
    await client.messages.reply(message, {content: 'response'});
  });
  subscription.on('messageUpdated', ({state}) => { const revision: number = state.revision; void revision; });
  subscription.on('error', error => { const code: string = error.code; void code; });
  await subscription.ready;
  subscription.typing();
  subscription.markRead({through: 2_000_000_000});
  await subscription.resync();
  await subscription.close();
  const active: number = client.realtime.getStats().activeSockets;
  for (const admitted of client.realtime.subscribeMany(['fixture-org:team:general'])) {
    const channelId: string = admitted.channelId;
    if (admitted.subscription) await admitted.subscription.close();
    if (admitted.error) { const code: string = admitted.error.code; void code; }
    void channelId;
  }
  void active;
}
void validSubscriptions;
async function durableConsumer(store: import('@humchat/node').CheckpointStore, outbox: import('@humchat/node').OutboxStore) {
  const sub = client.realtime.subscribe('fixture-org:team:general', {start: 'earliest'});
  sub.consume(async (event, {signal}) => {
    const sequence: string = event.sequence;
    await client.outbox.send({consumerId: 'worker', key: event.eventId, store: outbox,
      message: {channelId: event.channelId, content: sequence}}, {signal});
  }, {consumerId: 'worker', checkpointStore: store});
  const page = await client.events.sync({channelId: sub.channelId, after: 'opaque-cursor'});
  const cursor: string | null = page.nextCursor;
  void cursor;
}
void durableConsumer;
async function webhookManagement() {
  const created = await client.webhooks.create({id: '11111111-1111-4111-8111-111111111111', url: 'https://receiver.example.invalid/humchat', channelIds: ['fixture-org:team:general']});
  const secret: string = created.signingSecret;
  const subscription: import('@humchat/node').WebhookSubscription = await client.webhooks.get(created.subscription.id);
  await client.webhooks.pause(subscription.id);
  await client.webhooks.resume(subscription.id);
  await client.webhooks.rotateSecret(subscription.id);
  const page = await client.webhooks.deliveries(subscription.id, {status: 'dead', limit: 10});
  if (page.deliveries[0]) await client.webhooks.replay(subscription.id, page.deliveries[0].id);
  const list: import('@humchat/node').WebhookSubscription[] = await client.webhooks.list();
  const deleted: true = (await client.webhooks.delete(subscription.id)).deleted;
  const event: import('@humchat/node').WebhookEnvelope = verifyWebhook(Buffer.from('{}'), 't=1,v1=...', [secret],
    {expectedOrgId: 'fixture-org', expectedSubscriptionId: subscription.id, expectedChannelId: 'fixture-org:team:general', toleranceSeconds: 300});
  const schemaVersions: number[] | undefined = (await client.getCapabilities()).messaging.outgoingWebhooks?.schemaVersions;
  void list; void deleted; void event; void schemaVersions;
}
void webhookManagement;
async function directMessaging(store: import('@humchat/node').CheckpointStore) {
  const roster: import('@humchat/node').OrgMember[] = await client.members.list();
  const conversation: import('@humchat/node').Conversation = await client.conversations.openDirect({memberId: 'peer'});
  await client.conversations.create({memberIds: ['peer'], name: 'Group'});
  const result: import('@humchat/node').DirectSendReceipt = await client.messages.sendDirect({recipientId: 'peer', content: 'hello'});
  const inbox: import('@humchat/node').InboxWatch = client.inbox.watch(async (event, context) => {
    const id: string = event.state.id; const signal: AbortSignal = context.signal; void id; void signal;
  }, {consumerId: 'consumer', checkpointStore: store, pollIntervalMs: 5000, maxConversations: 20});
  const status: import('@humchat/node').InboxStatus = inbox.status;
  const stats: import('@humchat/node').InboxStats = inbox.getStats();
  await inbox.ready; await inbox.refresh(); await inbox.close(); await inbox.done;
  void roster; void conversation; void result; void status; void stats;
}
void directMessaging;
// @ts-expect-error inbox consumers require a checkpoint store
client.inbox.watch(async () => {}, {consumerId: 'consumer'});
// @ts-expect-error direct sending addresses an actor ID, not an email field
client.messages.sendDirect({email: 'peer@example.invalid', content: 'hello'});
// @ts-expect-error create requires a stable caller UUID for reconciliation
client.webhooks.create({url: 'https://receiver.example.invalid/humchat', channelIds: ['fixture-org:team:general']});
// @ts-expect-error filter status is a known delivery state
client.webhooks.deliveries('id', {status: 'everything'});
// @ts-expect-error receiver requires raw bytes or text, never a parsed body object
verifyWebhook({schema: 1}, 'header', 'secret');
// @ts-expect-error timestamp tolerance is a number of seconds
verifyWebhook('', 'header', 'secret', {toleranceSeconds: '300'});
// @ts-expect-error a durable consumer must explicitly bind its checkpoint store and consumer ID
client.realtime.subscribe('fixture-org:team:general').consume(async () => {});
// @ts-expect-error resource transport context is an internal implementation type
type InternalContext = import('@humchat/node').ResourceContext;
// @ts-expect-error orgId is mandatory
new HumChat({baseUrl: 'http://127.0.0.1', tokenProvider: async () => ({token: 'x'})});
// @ts-expect-error API-key exchange is not an implemented authentication mode
new HumChat({orgId: 'org', baseUrl: 'http://127.0.0.1', apiKey: 'x'});
// @ts-expect-error content must be text
client.messages.send({channelId: 'channel', content: 12});
// @ts-expect-error edits require the observed revision
client.messages.edit({channelId: 'channel', messageId: 'message', content: 'edit'});
// @ts-expect-error reactions require desired membership, not a toggle
client.messages.setReaction({channelId: 'channel', messageId: 'message', emoji: 'ok'});
// @ts-expect-error exact optional properties reject explicit undefined
client.channels.list({teamId: undefined});
// @ts-expect-error unknown event names are rejected
client.realtime.subscribe('fixture-org:team:general').on('notAnEvent', () => {});
// @ts-expect-error read horizons are Unix-second numbers
client.realtime.subscribe('fixture-org:team:general').markRead({through: 'yesterday'});
new HumChat({orgId: 'fixture-org', baseUrl: 'http://127.0.0.1', tokenProvider: async () => ({token: 'x'}), realtime: {transport: 'multiplex'}});
// @ts-expect-error transport has no automatic fallback mode
new HumChat({orgId: 'fixture-org', baseUrl: 'http://127.0.0.1', tokenProvider: async () => ({token: 'x'}), realtime: {transport: 'auto'}});
`;
    await writeFile(join(consumer, 'consumer.mts'), "import { HumChat, verifyWebhook } from '@humchat/node';\n" + typedBody);
    await writeFile(join(consumer, 'consumer.cts'), "import SDK = require('@humchat/node');\nconst { HumChat, verifyWebhook } = SDK;\n" + typedBody);
    const exampleReadme = await readFile(join(installedRoot, 'examples/README.md'), 'utf8');
    const snippets = [...exampleReadme.matchAll(/```ts\r?\n([\s\S]*?)```/g)].map(match => match[1]);
    assert.ok(snippets.length > 0, 'Examples README must contain a checked TypeScript quickstart');
    for (let index = 0; index < snippets.length; index++) await writeFile(join(consumer, 'quickstart-' + index + '.mts'), snippets[index]);
    report.quickstartSnippets = snippets.length;
    const webhookGuide = await readFile(join(installedRoot, 'docs/WEBHOOKS.md'), 'utf8');
    const webhookSnippets = [...webhookGuide.matchAll(/```ts\r?\n([\s\S]*?)```/g)].map(match => match[1]);
    assert.ok(webhookSnippets.length >= 2, 'Webhook guide must contain management and verification snippets');
    const webhookPrelude = `declare const orgId: string, baseUrl: string, channelId: string, subscriptionId: string, signatureHeader: string;
declare const tokenProvider: import('@humchat/node').TokenProvider;
declare const rawBody: Buffer;
declare const signingSecrets: readonly string[];
`;
    for (let index = 0; index < webhookSnippets.length; index++) await writeFile(join(consumer, 'quickstart-webhook-' + index + '.mts'), webhookPrelude + webhookSnippets[index]);
    report.webhookGuideSnippets = webhookSnippets.length;
    await writeFile(join(consumer, 'tsconfig.json'), JSON.stringify({ compilerOptions: { target: 'ES2023', lib: ['ES2023'],
      module: 'NodeNext', moduleResolution: 'NodeNext', strict: true, exactOptionalPropertyTypes: true,
      noUncheckedIndexedAccess: true, noEmit: true, skipLibCheck: false, types: ['node'] }, include: ['consumer.mts', 'consumer.cts', 'quickstart-*.mts'] }));
    run([join(root, 'node_modules/typescript/bin/tsc'), '-p', join(consumer, 'tsconfig.json')], consumer);
  });

  await check('installed ESM and CommonJS examples perform real local list, send and reaction requests', { timeout: 30000 }, async child => {
    const fixture = await examplePeer(child);
    const listed = await successfulExample('list-channels.mjs', example(child, consumer, 'list-channels.mjs', fixture.environment));
    assert.ok(listed.includes('General') && listed.includes(fixture.channels[0]));
    const sentOutput = await successfulExample('send.mjs', example(child, consumer, 'send.mjs', fixture.environment, ['Installed ESM send']));
    assert.equal(fixture.sends.length, 1); assert.equal(fixture.sends[0].body.content, 'Installed ESM send');
    assert.ok(sentOutput.includes(fixture.sends[0].body.id) && sentOutput.includes('accepted'));
    await successfulExample('react.mjs', example(child, consumer, 'react.mjs', { ...fixture.environment, HUMCHAT_MESSAGE_ID: fixture.sends[0].body.id }, ['ok']));
    assert.equal(fixture.mutations.length, 1); assert.deepEqual(fixture.mutations[0].mutation, { kind: 'set_reaction', emoji: 'ok', present: true });
    const before = fixture.sends.length;
    await successfulExample('send-and-list.cjs-list-only', example(child, consumer, 'send-and-list.cjs', fixture.environment));
    assert.equal(fixture.sends.length, before, 'CommonJS smoke must not send without an explicit message environment variable');
    const cjsOutput = await successfulExample('send-and-list.cjs-send', example(child, consumer, 'send-and-list.cjs',
      { ...fixture.environment, HUMCHAT_SEND_MESSAGE: 'Explicit installed CJS send' }));
    assert.equal(fixture.sends.length, before + 1); assert.equal(fixture.sends.at(-1).body.content, 'Explicit installed CJS send');
    assert.ok(cjsOutput.includes('"event":"channels"') && cjsOutput.includes('"event":"sent"'));
  });

  await check('installed best-effort responder ignores itself, deduplicates live input and preserves its thread', { timeout: 15000 }, async child => {
    const fixture = await examplePeer(child);
    const running = example(child, consumer, 'thread-responder.mjs', fixture.environment);
    await until(() => running.output.includes('Listening for !status'), 'installed best-effort readiness');
    fixture.publish(fixture.channels[0], 'self-command', '!status', fixture.actor);
    const command = fixture.publish(fixture.channels[0], 'thread-command', '!status', fixture.user, 'existing-thread-root');
    fixture.repeat(command);
    await until(() => fixture.sends.length === 1, 'installed thread reply');
    assert.equal(fixture.sends[0].body.thread_root_id, 'existing-thread-root');
    assert.equal(fixture.sends[0].body.content, 'All systems operational.');
    await sleep(75); assert.equal(fixture.sends.length, 1);
    fixture.revoke();
    await successfulExample('thread-responder.mjs', running);
  });

  await check('installed durable single-channel entry point replies and releases its state locks', { timeout: 15000 }, async child => {
    const fixture = await examplePeer(child);
    const directory = await mkdtemp(join(consumer, 'single-state-'));
    const running = example(child, consumer, 'durable-responder.mjs', { ...fixture.environment,
      HUMCHAT_STATE_DIR: directory, HUMCHAT_RUN_FOR_MS: '800' });
    await until(() => running.output.includes('"event":"ready"'), 'single-channel durable readiness');
    fixture.publish(fixture.channels[0], 'single-command', '!status');
    await until(() => fixture.sends.length === 1, 'single-channel durable reply');
    assert.equal(fixture.sends[0].body.thread_root_id, 'single-command');
    const output = await successfulExample('durable-responder.mjs', running);
    assert.ok(output.includes('"event":"stopped"'));
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
  });

  await check('installed multi-channel worker resumes checkpoints and retries cancelled outbox work without duplicate intent', { timeout: 30000 }, async child => {
    const fixture = await examplePeer(child);
    const directory = await mkdtemp(join(consumer, 'multi-state-'));
    const environment = { ...fixture.environment, HUMCHAT_STATE_DIR: directory, HUMCHAT_RUN_FOR_MS: '1000',
      HUMCHAT_CHANNEL_IDS: [...fixture.channels, fixture.channels[0]].join(','), HUMCHAT_CONSUMER_ID: 'installed-worker' };
    const runWorker = overrides => example(child, consumer, 'multi-channel-worker.mjs', { ...environment, ...overrides });
    const documents = async () => ({ checkpoints: JSON.parse(await readFile(join(directory, 'checkpoints.json'), 'utf8')),
      outbox: JSON.parse(await readFile(join(directory, 'outbox.json'), 'utf8')) });
    const first = runWorker();
    await until(() => first.output.includes('"event":"ready"'), 'multi-channel readiness');
    assert.equal(fixture.peers.filter(peer => peer.socket.readyState === WebSocket.OPEN).length, 2);
    fixture.publish(fixture.channels[0], 'ignored-self-command', '!status', fixture.actor);
    fixture.publish(fixture.channels[1], 'ignored-normal-message', 'not a command');
    fixture.publish(fixture.channels[0], 'multi-first', '!status');
    fixture.publish(fixture.channels[1], 'multi-second', '!status', fixture.user, 'existing-root');
    await until(() => fixture.sends.length === 2, 'replies in both channels');
    await successfulExample('multi-channel-worker.mjs-first', first);
    assert.deepEqual(fixture.sends.map(send => send.body.thread_root_id).sort(), ['existing-root', 'multi-first']);
    const saved = await documents();
    assert.equal(Object.keys(saved.checkpoints.records).length, 2);
    assert.equal(Object.values(saved.outbox.records).filter(record => record.entry.status === 'sent').length, 2);
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
    fixture.publish(fixture.channels[0], 'offline-command', '!status');
    const resumed = runWorker({ HUMCHAT_RUN_FOR_MS: '500' });
    await successfulExample('multi-channel-worker.mjs-restart', resumed);
    assert.equal(fixture.sends.length, 3, 'Saved checkpoints must process the offline command once without replaying prior replies');
    assert.equal(fixture.sends.at(-1).body.thread_root_id, 'offline-command');

    fixture.holdSends = true;
    const cancelled = runWorker({ HUMCHAT_RUN_FOR_MS: '250' });
    await until(() => cancelled.output.includes('"event":"ready"'), 'cancellable worker readiness');
    fixture.publish(fixture.channels[0], 'cancelled-command', '!status');
    await until(() => fixture.sends.length === 4, 'outbox send is in flight at shutdown');
    await successfulExample('multi-channel-worker.mjs-cancel', cancelled);
    const pending = await documents();
    assert.equal(Object.values(pending.outbox.records).filter(record => record.entry.status === 'pending').length, 1);
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
    fixture.holdSends = false;
    const recovered = runWorker({ HUMCHAT_RUN_FOR_MS: '500' });
    await successfulExample('multi-channel-worker.mjs-recover-pending', recovered);
    assert.equal(fixture.sends.length, 5);
    assert.deepEqual(fixture.sends[4], fixture.sends[3], 'The ambiguous committed send must reuse its exact ID, key and body');
    assert.equal(fixture.sent.size, 4);
    const completed = await documents();
    assert.equal(Object.values(completed.outbox.records).filter(record => record.entry.status === 'pending').length, 0);
    for (const record of Object.values(completed.checkpoints.records)) assert.equal(record.cursor,
      fixture.cursor(record.scope.channelId, fixture.streams.get(record.scope.channelId).length));
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
  });
  await check('installed multiplex worker serves two channels over one physical socket and cleans up durable state', { timeout: 15000 }, async child => {
    const fixture = await examplePeer(child), directory = await mkdtemp(join(consumer, 'multiplex-state-'));
    const environment = { ...fixture.environment, HUMCHAT_STATE_DIR: directory, HUMCHAT_RUN_FOR_MS: '1000',
      HUMCHAT_CHANNEL_IDS: fixture.channels.join(','), HUMCHAT_CONSUMER_ID: 'installed-multiplex' };
    const running = example(child, consumer, 'multiplex-worker.mjs', environment);
    await until(() => running.output.includes('"event":"ready"'), 'installed multiplex readiness');
    const active = fixture.peers.filter(peer => peer.socket.readyState === WebSocket.OPEN);
    assert.equal(active.length, 1); assert.equal(active[0].multiplex, true); assert.equal(active[0].subscriptions.size, 2);
    fixture.publish(fixture.channels[0], 'multiplex-self', '!status', fixture.actor);
    fixture.publish(fixture.channels[0], 'multiplex-first', '!status');
    fixture.publish(fixture.channels[1], 'multiplex-second', '!status', fixture.user, 'multiplex-thread');
    await until(() => fixture.sends.length === 2, 'installed multiplex replies');
    await successfulExample('multiplex-worker.mjs', running);
    assert.deepEqual(fixture.sends.map(send => send.body.thread_root_id).sort(), ['multiplex-first', 'multiplex-thread']);
    assert.equal(fixture.peers.length, 1);
    const checkpoint = JSON.parse(await readFile(join(directory, 'checkpoints.json'), 'utf8'));
    assert.equal(Object.keys(checkpoint.records).length, 2);
    for (const record of Object.values(checkpoint.records)) assert.equal(record.cursor,
      fixture.cursor(record.scope.channelId, fixture.streams.get(record.scope.channelId).length));
    const outbox = JSON.parse(await readFile(join(directory, 'outbox.json'), 'utf8'));
    assert.equal(Object.values(outbox.records).filter(record => record.entry.status === 'sent').length, 2);
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
    await until(() => fixture.peers.every(peer => peer.socket.readyState === WebSocket.CLOSED), 'installed multiplex socket shutdown');
  });
  await check('installed webhook receiver verifies raw requests and durably deduplicates retries across restart', { timeout: 20000 }, async child => {
    const directory = await mkdtemp(join(consumer, 'webhook-state-'));
    const environment = { HUMCHAT_ORG_ID: webhookOrg, HUMCHAT_WEBHOOK_SUBSCRIPTION_ID: webhookSubscription,
      HUMCHAT_CHANNEL_IDS: webhookChannel, HUMCHAT_WEBHOOK_SECRET: webhookSecret, HUMCHAT_WEBHOOK_PREVIOUS_SECRET: webhookPreviousSecret,
      HUMCHAT_WEBHOOK_STATE_DIR: directory, HUMCHAT_WEBHOOK_PORT: '0', HUMCHAT_RUN_FOR_MS: '2000' };
    async function launch(name) {
      const running = example(child, consumer, 'webhook-receiver.mjs', environment);
      await until(() => running.output.includes('"event":"ready"'), name + ' receiver readiness');
      const ready = running.output.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)).find(value => value.event === 'ready');
      assert.equal(ready.host, '127.0.0.1'); assert.ok(ready.port > 0); assert.equal(ready.path, '/humchat/webhook');
      return { running, url: 'http://127.0.0.1:' + ready.port + ready.path };
    }
    async function send(url, value, options = {}) {
      const body = JSON.stringify(value);
      const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json',
        'X-HumChat-Signature': options.header ?? webhookSignature(body, options.secrets ?? [webhookSecret], value.sent_at) },
        body: options.body ?? body, signal: AbortSignal.timeout(3000) });
      await response.arrayBuffer(); return response.status;
    }
    const first = await launch('initial');
    const value = webhookEnvelope({ sent_at: Math.floor(Date.now() / 1000) });
    assert.equal(await send(first.url, value, { body: JSON.stringify(value) + ' ' }), 401, 'Raw body tampering must be rejected');
    assert.equal(await send(first.url, { ...value, subscription_id: '99999999-9999-4999-8999-999999999999' }), 401, 'Signed requests for another installation must be rejected');
    assert.equal(await send(first.url, value, { header: 't=1,v1=invalid' }), 401);
    assert.deepEqual(await Promise.all(Array.from({ length: 4 }, (_, index) => send(first.url,
      { ...value, attempt: index + 1 }, { secrets: index % 2 ? [webhookPreviousSecret] : [webhookSecret] }))), [204, 204, 204, 204]);
    const saved = JSON.parse(await readFile(join(directory, 'inbox.json'), 'utf8'));
    assert.equal(Object.keys(saved.records).length, 1, 'The 2xx response must follow persistence of exactly one scoped event');
    assert.equal(Object.values(saved.records)[0].event.state.body, 'Hello webhook');
    const conflicting = structuredClone(value); conflicting.event.state.body = conflicting.event.state.original_body = 'Conflicting signed event';
    assert.equal(await send(first.url, conflicting), 503, 'A conflicting payload for the same scoped event must not replace the saved event');
    const output = await successfulExample('webhook-receiver.mjs-first', first.running);
    assert.ok(output.includes('"event":"stopped"')); assert.ok(output.includes('"duplicate":true'));
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
    const restarted = await launch('restarted');
    const retried = { ...value, sent_at: Math.floor(Date.now() / 1000), attempt: 5,
      delivery_id: '88888888-8888-4888-8888-888888888888' };
    assert.equal(await send(restarted.url, retried), 204, 'Event identity must deduplicate across process restart and delivery IDs');
    const restartOutput = await successfulExample('webhook-receiver.mjs-restart', restarted.running);
    assert.ok(restartOutput.includes('"duplicate":true'));
    assert.deepEqual(JSON.parse(await readFile(join(directory, 'inbox.json'), 'utf8')), saved);
    assert.ok(!(await readdir(directory)).some(name => name.endsWith('.lock')));
  });
  report.passed = report.checks.every(check => check.passed);
});
