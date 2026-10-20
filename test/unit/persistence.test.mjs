import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, readdir, rename, rmdir, unlink, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { FileCheckpointStore, FileOutboxStore, MemoryCheckpointStore, MemoryOutboxStore,
  ConflictError, PersistenceError, ValidationError } from '../../dist/index.js';

const artifactRoot = fileURLToPath(new URL('../../target/persistence/', import.meta.url));
const scope = { serverOrigin: 'https://chat.example.invalid', orgId: 'org', actorId: 'actor',
  consumerId: 'consumer', channelId: 'org:team:general' };
const input = { channelId: scope.channelId, content: 'One stable outgoing intent' };
const receipt = entry => ({ id: entry.intent.id, channelId: entry.intent.channelId, status: 'accepted',
  createdAt: 123, idempotencyKey: entry.idempotencyKey, attachmentCount: 0, attachmentManifestBlake3: '0'.repeat(64) });
async function directory() { await mkdir(artifactRoot, { recursive: true }); return mkdtemp(join(artifactRoot, 'persistence-')); }
function cleanup(t, store) { t.after(async () => { await store.close(); }); return store; }

test('checkpoint CAS admits one worker and all identity components isolate progress', async t => {
  const store = cleanup(t, new MemoryCheckpointStore());
  assert.equal(await store.load(scope), null);
  const results = await Promise.all([store.compareAndSet(scope, null, 'cursor-a'), store.compareAndSet(scope, null, 'cursor-b')]);
  assert.deepEqual(results, [true, false]);
  assert.equal(await store.compareAndSet(scope, 'stale', 'cursor-c'), false);
  assert.equal(await store.load({ ...scope, serverOrigin: scope.serverOrigin + '/' }), 'cursor-a');
  for (const other of [{ ...scope, serverOrigin: 'https://another.example.invalid' }, { ...scope, actorId: 'another-actor' },
    { ...scope, consumerId: 'another-consumer' }, { ...scope, channelId: 'org:team:other' },
    { ...scope, orgId: 'other-org', channelId: 'other-org:team:general' }]) assert.equal(await store.load(other), null);
  await assert.rejects(store.load({ ...scope, orgId: 'other-org' }), ValidationError);
  await assert.rejects(store.compareAndSet(scope, 'cursor-a', ''), ValidationError);
  const longActor = { ...scope, actorId: 'a'.repeat(256) };
  assert.equal(await store.compareAndSet(longActor, null, 'long-actor-cursor'), true);
  assert.equal(await store.load(longActor), 'long-actor-cursor');
  await assert.rejects(store.load({ ...scope, actorId: 'a'.repeat(257) }), ValidationError);
});

test('file checkpoint survives restart, serializes CAS and enforces one live owner', async t => {
  const path = join(await directory(), 'checkpoints.json');
  const first = cleanup(t, await FileCheckpointStore.open(path));
  await assert.rejects(FileCheckpointStore.open(path), PersistenceError);
  assert.deepEqual(await Promise.all([first.compareAndSet(scope, null, 'cursor-1'), first.compareAndSet(scope, null, 'cursor-2')]), [true, false]);
  await first.close();
  await assert.rejects(first.load(scope), PersistenceError);
  const reopened = cleanup(t, await FileCheckpointStore.open(path));
  assert.equal(await reopened.load(scope), 'cursor-1');
  assert.equal(await reopened.compareAndSet(scope, 'cursor-1', 'cursor-3'), true);
  assert.equal(await reopened.compareAndSet(scope, 'cursor-1', 'cursor-4'), false);
});

test('outbox binds normalized intent and preserves generated IDs across retries', async t => {
  const store = cleanup(t, new MemoryOutboxStore());
  const mutable = { ...input };
  const first = await store.prepare(scope, 'job-1', mutable);
  assert.match(first.intent.id, /^[0-9A-HJKMNP-TV-Z]{26}$/);
  assert.equal(first.idempotencyKey, first.intent.id);
  mutable.content = 'caller mutated its object';
  assert.deepEqual(await store.prepare(scope, 'job-1', { ...input, priority: 'standard', threadRootId: null }), first);
  const callerCopy = await store.listPending(scope);
  callerCopy[0].intent.content = 'mutated returned value';
  assert.equal((await store.listPending(scope))[0].intent.content, input.content);
  await assert.rejects(store.prepare(scope, 'job-1', { ...input, content: 'different intent' }), ConflictError);
  await assert.rejects(store.prepare(scope, 'job-1', { ...input, threadRootId: 'different-thread' }), ConflictError);
  await assert.rejects(store.prepare(scope, 'job-1', input, { idempotencyKey: 'different-key' }), ConflictError);
  await assert.rejects(store.prepare(scope, 'job-2', { ...input, channelId: 'org:team:other' }), ValidationError);
  const explicit = await store.prepare(scope, 'job-2', { ...input, id: 'caller-message-id' }, { idempotencyKey: 'caller-idempotency-key' });
  assert.equal(explicit.intent.id, 'caller-message-id');
  assert.equal(explicit.idempotencyKey, 'caller-idempotency-key');
});

test('outbox completion rejects unrelated receipts and keeps exactly the recorded result', async t => {
  const store = cleanup(t, new MemoryOutboxStore());
  const first = await store.prepare(scope, 'job', input);
  for (const bad of [{ id: 'other-message' }, { channelId: 'org:team:other' }, { idempotencyKey: 'other-key' }]) {
    await assert.rejects(store.markSent(scope, 'job', { ...receipt(first), ...bad }), ConflictError);
  }
  assert.equal((await store.listPending(scope)).length, 1);
  const completed = await store.markSent(scope, 'job', receipt(first));
  assert.equal(completed.status, 'sent');
  assert.equal((await store.listPending(scope)).length, 0);
  assert.deepEqual(await store.markSent(scope, 'job', receipt(first)), completed);
  assert.deepEqual(await store.prepare(scope, 'job', input), completed);
  await assert.rejects(store.markSent(scope, 'job', { ...receipt(first), createdAt: 124 }), ConflictError);
  await assert.rejects(store.markSent({ ...scope, actorId: 'different-actor' }, 'job', receipt(first)), ConflictError);
});

test('file outbox survives pending and completed restarts without minting new intent', async t => {
  const path = join(await directory(), 'outbox.json');
  const first = cleanup(t, await FileOutboxStore.open(path));
  const prepared = await first.prepare(scope, 'job', input);
  await first.close();
  const second = cleanup(t, await FileOutboxStore.open(path));
  assert.deepEqual(await second.listPending(scope), [prepared]);
  assert.deepEqual(await second.prepare(scope, 'job', input), prepared);
  assert.deepEqual(await second.listPending({ ...scope, consumerId: 'different-consumer' }), []);
  await second.markSent(scope, 'job', receipt(prepared));
  await second.close();
  const third = cleanup(t, await FileOutboxStore.open(path));
  assert.deepEqual(await third.listPending(scope), []);
  assert.equal((await third.prepare(scope, 'job', input)).status, 'sent');
  assert.equal((await third.prepare(scope, 'job', input)).intent.id, prepared.intent.id);
});

test('a failed atomic replacement leaves checkpoint memory and existing file unchanged', async t => {
  const dir = await directory();
  const path = join(dir, 'checkpoints.json');
  const backup = join(dir, 'saved-checkpoints.json');
  const store = cleanup(t, await FileCheckpointStore.open(path));
  await store.compareAndSet(scope, null, 'before');
  const contents = await readFile(path, 'utf8');
  // Both targets are fixed names inside this test's freshly created artifact directory.
  await rename(path, backup);
  await mkdir(path);
  try {
    await assert.rejects(store.compareAndSet(scope, 'before', 'after'), PersistenceError);
    assert.equal(await store.load(scope), 'before');
    assert.equal(await readFile(backup, 'utf8'), contents);
    assert.ok(!(await readdir(dir)).some(name => name.includes('.tmp-')));
  } finally { await rmdir(path); await rename(backup, path); }
  assert.equal(await store.compareAndSet(scope, 'before', 'recovered'), true);
});

test('bounded persistence writes cannot create an unpersisted pending outbox entry', async t => {
  const path = join(await directory(), 'small-outbox.json');
  const store = cleanup(t, await FileOutboxStore.open(path, { maxBytes: 1024 }));
  await assert.rejects(store.prepare(scope, 'too-large', { ...input, content: 'x'.repeat(2000) }), PersistenceError);
  assert.deepEqual(await store.listPending(scope), []);
  const short = await store.prepare(scope, 'small', { ...input, content: 'ok' });
  assert.equal(short.status, 'pending');
  await store.close();
  const reopened = cleanup(t, await FileOutboxStore.open(path, { maxBytes: 1024 }));
  assert.deepEqual(await reopened.listPending(scope), [short]);
});

test('corrupt, oversized, wrong-kind and identity-tampered files fail closed', async t => {
  const dir = await directory();
  const path = join(dir, 'checkpoint.json');
  const store = cleanup(t, await FileCheckpointStore.open(path));
  await store.compareAndSet(scope, null, 'original');
  await store.close();
  const original = await readFile(path, 'utf8');
  await assert.rejects(FileOutboxStore.open(path), PersistenceError);
  const tampered = JSON.parse(original);
  Object.values(tampered.records)[0].scope.actorId = 'changed-actor';
  await writeFile(path, JSON.stringify(tampered));
  await assert.rejects(FileCheckpointStore.open(path), PersistenceError);
  await writeFile(path, '{not JSON');
  await assert.rejects(FileCheckpointStore.open(path), PersistenceError);
  await writeFile(path, Buffer.from([0xff, 0xfe, 0xfd]));
  await assert.rejects(FileCheckpointStore.open(path), PersistenceError);
  await writeFile(path, ' '.repeat(2048));
  await assert.rejects(FileCheckpointStore.open(path, { maxBytes: 1024 }), PersistenceError);
  await writeFile(path, original);
  const recovered = cleanup(t, await FileCheckpointStore.open(path));
  assert.equal(await recovered.load(scope), 'original');
});

test('stale lock files are never automatically expired or removed', async () => {
  const path = join(await directory(), 'checkpoint.json');
  const stale = JSON.stringify({ schema: 1, owner: 'stale-test-owner', pid: 99999999, createdAt: '2000-01-01T00:00:00.000Z' });
  await writeFile(path + '.lock', stale);
  await assert.rejects(FileCheckpointStore.open(path), PersistenceError);
  assert.equal(await readFile(path + '.lock', 'utf8'), stale);
});

test('a killed process preserves committed checkpoint and pending outbox intent behind stale locks', { timeout: 10000 }, async t => {
  const root = fileURLToPath(new URL('../../target/persistence/', import.meta.url));
  await mkdir(root, { recursive: true });
  const dir = await mkdtemp(join(root, 'process-crash-'));
  const checkpointPath = join(dir, 'checkpoint.json');
  const outboxPath = join(dir, 'outbox.json');
  const sdkUrl = new URL('../../dist/index.js', import.meta.url).href;
  // Child performs actual durable writes and holds its locks without registering graceful exit handlers.
  const source = `
    const { FileCheckpointStore, FileOutboxStore } = await import(${JSON.stringify(sdkUrl)});
    const [checkpointPath, outboxPath, scopeJson, inputJson] = process.argv.slice(1);
    const scope = JSON.parse(scopeJson);
    const checkpoint = await FileCheckpointStore.open(checkpointPath);
    const outbox = await FileOutboxStore.open(outboxPath);
    await checkpoint.compareAndSet(scope, null, 'committed-before-process-crash');
    const entry = await outbox.prepare(scope, 'crash-job', JSON.parse(inputJson));
    // Module execution ends below; retain live lock owners until the forced crash.
    globalThis.crashFixtureStores = [checkpoint, outbox];
    process.send({ kind: 'committed', entry });
    setInterval(() => {}, 1000);
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', source,
    checkpointPath, outboxPath, JSON.stringify(scope), JSON.stringify(input)], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'], windowsHide: true });
  let stderr = '';
  child.stderr.setEncoding('utf8'); child.stderr.on('data', chunk => { stderr += chunk; });
  const exited = once(child, 'exit');
  t.after(async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await exited; });
  const [message] = await Promise.race([once(child, 'message'), exited.then(() => { assert.fail('Crash fixture exited before committing: ' + stderr); })]);
  assert.equal(message.kind, 'committed');
  const checkpointLock = await readFile(checkpointPath + '.lock', 'utf8');
  const outboxLock = await readFile(outboxPath + '.lock', 'utf8');
  assert.equal(JSON.parse(checkpointLock).pid, child.pid);
  assert.equal(JSON.parse(outboxLock).pid, child.pid);
  await assert.rejects(FileCheckpointStore.open(checkpointPath), PersistenceError);
  assert.equal(child.kill('SIGKILL'), true);
  await exited;
  assert.equal(stderr, '');
  await assert.rejects(FileCheckpointStore.open(checkpointPath), PersistenceError);
  await assert.rejects(FileOutboxStore.open(outboxPath), PersistenceError);
  assert.equal(await readFile(checkpointPath + '.lock', 'utf8'), checkpointLock);
  assert.equal(await readFile(outboxPath + '.lock', 'utf8'), outboxLock);
  // Explicit operator recovery only after this exact child process has exited.
  await unlink(checkpointPath + '.lock');
  await unlink(outboxPath + '.lock');
  const checkpoint = cleanup(t, await FileCheckpointStore.open(checkpointPath));
  const outbox = cleanup(t, await FileOutboxStore.open(outboxPath));
  assert.equal(await checkpoint.load(scope), 'committed-before-process-crash');
  assert.deepEqual(await outbox.listPending(scope), [message.entry]);
  assert.deepEqual(await outbox.prepare(scope, 'crash-job', input), message.entry);
  assert.equal((await outbox.prepare(scope, 'crash-job', input)).idempotencyKey, message.entry.idempotencyKey);
});
