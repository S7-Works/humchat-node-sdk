import { mkdir, open, readFile, realpath, rename, unlink, lstat, type FileHandle } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { ConfigurationError } from '../errors.js';
import { emptyDocument, parseDocument, type StoreDocument } from './model.js';
import { PersistenceError, type FileStoreOptions } from './types.js';

function code(error: unknown): string | undefined { return (error as NodeJS.ErrnoException)?.code; }
async function boundedRead(path: string, maxBytes: number): Promise<string> {
  const handle = await open(path, 'r');
  try {
    if ((await handle.stat()).size > maxBytes) throw new PersistenceError('Persistence file exceeds maxBytes');
    const chunks: Buffer[] = [];
    let total = 0;
    // Read through the same descriptor, checking every chunk if the file grows.
    for await (const raw of handle.createReadStream({ autoClose: false, highWaterMark: 64 * 1024 })) {
      const chunk = raw as Buffer;
      total += chunk.length;
      if (total > maxBytes) throw new PersistenceError('Persistence file exceeds maxBytes');
      chunks.push(chunk);
    }
    return new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks, total));
  } finally { await handle.close(); }
}
/**
 * Internal serialized document engine; a sibling exclusive lock is held for its full lifetime.
 * Intended for a local filesystem, not a distributed lease. File contents are fsynced before
 * replacement; OS/filesystem power-loss durability of the directory entry is not guaranteed.
 */
export class FileDocument {
  #queue: Promise<unknown> = Promise.resolve();
  #closing = false;
  #closePromise: Promise<void> | undefined;
  private constructor(private readonly path: string, private readonly lock: FileHandle, private readonly owner: string,
    private readonly maxBytes: number, private document: StoreDocument) {}
  static async open(path: string, kind: StoreDocument['kind'], options: FileStoreOptions = {}): Promise<FileDocument> {
    if (typeof path !== 'string' || !path.trim() || path.includes('\0')) throw new ConfigurationError('A persistence file path is required');
    if (!options || typeof options !== 'object' || Array.isArray(options)) throw new ConfigurationError('Persistence options must be an object');
    const maxBytes = options.maxBytes ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1024 || maxBytes > 1024 * 1024 * 1024 || Object.keys(options).some(key => key !== 'maxBytes')) {
      throw new ConfigurationError('Persistence maxBytes must be an integer between 1024 and 1073741824');
    }
    let canonical: string;
    try {
      const absolute = resolve(path);
      await mkdir(dirname(absolute), { recursive: true, mode: 0o700 });
      canonical = join(await realpath(dirname(absolute)), basename(absolute));
      try { if ((await lstat(canonical)).isSymbolicLink()) throw new PersistenceError('Persistence files must not be symbolic links'); }
      catch (error) { if (code(error) !== 'ENOENT') throw error; }
    } catch { throw new PersistenceError('Could not prepare the persistence file location'); }
    let lock: FileHandle;
    try { lock = await open(canonical + '.lock', 'wx', 0o600); }
    catch (error) {
      if (code(error) === 'EEXIST') throw new PersistenceError('Persistence store is locked; close its owner or manually inspect a stale lock');
      throw new PersistenceError('Could not acquire the persistence file lock');
    }
    const owner = randomUUID();
    try {
      await lock.writeFile(JSON.stringify({ schema: 1, owner, pid: process.pid, createdAt: new Date().toISOString() }));
      await lock.sync();
      let document: StoreDocument = kind === 'checkpoints' ? emptyDocument('checkpoints') : emptyDocument('outbox');
      try {
        const raw = await boundedRead(canonical, maxBytes);
        document = parseDocument(JSON.parse(raw) as unknown, kind);
      } catch (error) { if (code(error) !== 'ENOENT') throw error; }
      return new FileDocument(canonical, lock, owner, maxBytes, document);
    } catch (error) {
      await lock.close().catch(() => {});
      await unlink(canonical + '.lock').catch(() => {});
      if (error instanceof PersistenceError) throw error;
      throw new PersistenceError('Could not open a valid persistence file');
    }
  }
  async read<T>(operation: (document: StoreDocument) => T): Promise<T> { return this.enqueue(() => operation(this.document)); }
  async update<T>(operation: (document: StoreDocument) => T): Promise<T> {
    return this.enqueue(async () => {
      const next = structuredClone(this.document);
      const result = operation(next);
      // Read-only repeats/CAS failures do not require another disk write.
      if (JSON.stringify(next) !== JSON.stringify(this.document)) {
        await this.write(next);
        this.document = next;
      }
      return result;
    });
  }
  close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closing = true;
    this.#closePromise = this.#queue.then(async () => {
      await this.lock.close();
      try {
        const marker = JSON.parse(await readFile(this.path + '.lock', 'utf8')) as { owner?: string };
        if (marker.owner !== this.owner) throw new Error('different owner');
        await unlink(this.path + '.lock');
      } catch { throw new PersistenceError('Persistence lock ownership changed; manual inspection is required'); }
    }).catch(error => { if (error instanceof PersistenceError) throw error; throw new PersistenceError('Could not close the persistence store'); });
    return this.#closePromise;
  }
  private enqueue<T>(operation: () => T | Promise<T>): Promise<T> {
    if (this.#closing) return Promise.reject(new PersistenceError('Persistence store is closed'));
    const next = this.#queue.then(operation);
    this.#queue = next.catch(() => {});
    return next;
  }
  private async write(document: StoreDocument): Promise<void> {
    const contents = JSON.stringify(document);
    if (Buffer.byteLength(contents) > this.maxBytes) throw new PersistenceError('Persistence write exceeds maxBytes');
    const temporary = this.path + '.tmp-' + randomUUID();
    let handle: FileHandle | undefined;
    try {
      handle = await open(temporary, 'wx', 0o600);
      await handle.writeFile(contents);
      await handle.sync();
      await handle.close(); handle = undefined;
      // Node/libuv replaces the destination atomically on supported local filesystems,
      // including Windows. No delete-then-rename window is introduced.
      await rename(temporary, this.path);
    } catch {
      await handle?.close().catch(() => {});
      await unlink(temporary).catch(() => {});
      throw new PersistenceError('Persistence write failed; the in-memory transaction was not committed');
    }
  }
}
