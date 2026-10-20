// Small single-process inbox example. Replace with an application database/queue at scale.
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, open, readFile, rename, unlink } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const limit = 64 * 1024 * 1024;
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const failure = () => Object.assign(new Error('Webhook inbox unavailable'), { code: 'WEBHOOK_INBOX' });
async function boundedRead(path) {
  const file = await open(path, 'r');
  try {
    const chunks = []; let total = 0;
    for await (const chunk of file.createReadStream({ autoClose: false, highWaterMark: 64 * 1024 })) {
      total += chunk.length; if (total > limit) throw failure(); chunks.push(chunk);
    }
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, total)));
  } finally { await file.close(); }
}

export class WebhookInbox {
  #queue = Promise.resolve();
  #closing = false;
  #closed;
  constructor(path, lock, owner, document) { this.path = path; this.lock = lock; this.owner = owner; this.document = document; }
  static async open(directory, orgId, subscriptionId) {
    const path = join(resolve(directory), 'inbox.json'), owner = randomUUID();
    await mkdir(resolve(directory), { recursive: true, mode: 0o700 });
    let lock;
    try { lock = await open(path + '.lock', 'wx', 0o600); }
    catch { throw failure(); } // Stale locks require manual inspection; never expire another owner.
    try {
      await lock.writeFile(JSON.stringify({ owner, pid: process.pid })); await lock.sync();
      let document = { schema: 1, orgId, subscriptionId, records: {} };
      try { document = await boundedRead(path); } catch (error) { if (error.code !== 'ENOENT') throw error; }
      if (document?.schema !== 1 || document.orgId !== orgId || document.subscriptionId !== subscriptionId ||
        !document.records || typeof document.records !== 'object' || Array.isArray(document.records) || Object.keys(document.records).length > 10000) throw failure();
      for (const [key, row] of Object.entries(document.records)) {
        if (!/^[a-f0-9]{64}$/.test(key) || !row || !Array.isArray(row.scope) || row.scope.length !== 4 ||
          row.scope[0] !== orgId || row.scope[1] !== subscriptionId || typeof row.scope[2] !== 'string' ||
          !row.scope[2].startsWith(orgId + ':') || row.scope[3] !== row.event?.eventId ||
          row.event?.orgId !== orgId || row.event?.channelId !== row.scope[2] ||
          digest(row.scope) !== key || digest(row.event) !== row.eventDigest) throw failure();
      }
      return new WebhookInbox(path, lock, owner, document);
    } catch {
      await lock.close().catch(() => {}); await unlink(path + '.lock').catch(() => {}); throw failure();
    }
  }
  accept(envelope) {
    if (this.#closing) return Promise.reject(failure());
    const value = structuredClone(envelope);
    const work = this.#queue.then(async () => {
      if (value.orgId !== this.document.orgId || value.subscriptionId !== this.document.subscriptionId) throw failure();
      const scope = [value.orgId, value.subscriptionId, value.channelId, value.eventId], key = digest(scope), eventDigest = digest(value.event);
      const prior = this.document.records[key];
      if (prior) { if (prior.eventDigest !== eventDigest) throw failure(); return false; }
      if (Object.keys(this.document.records).length >= 10000) throw failure();
      const next = { ...this.document, records: { ...this.document.records, [key]: { scope, eventDigest, event: value.event, receivedAt: Math.floor(Date.now() / 1000) } } };
      const contents = JSON.stringify(next); if (Buffer.byteLength(contents) > limit) throw failure();
      const temporary = this.path + '.tmp-' + randomUUID(); let handle;
      try {
        handle = await open(temporary, 'wx', 0o600); await handle.writeFile(contents); await handle.sync();
        await handle.close(); handle = undefined; await rename(temporary, this.path);
        this.document = next; return true;
      } catch {
        await handle?.close().catch(() => {}); await unlink(temporary).catch(() => {}); throw failure();
      }
    });
    this.#queue = work.catch(() => {}); return work;
  }
  close() {
    if (this.#closed) return this.#closed;
    this.#closing = true;
    this.#closed = this.#queue.then(async () => {
      await this.lock.close();
      const marker = JSON.parse(await readFile(this.path + '.lock', 'utf8'));
      if (marker.owner !== this.owner) throw failure();
      await unlink(this.path + '.lock');
    });
    return this.#closed;
  }
}
