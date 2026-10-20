import { cursorValue, emptyDocument, scopeKey, scopeValue, type CheckpointDocument } from './model.js';
import { FileDocument } from './file.js';
import { PersistenceError, type CheckpointStore, type FileStoreOptions, type PersistenceScope } from './types.js';

/** Process-local checkpoints. Restarting loses all progress. */
export class MemoryCheckpointStore implements CheckpointStore {
  #document: CheckpointDocument = emptyDocument('checkpoints');
  #closed = false;
  async load(scope: PersistenceScope): Promise<string | null> {
    if (this.#closed) throw new PersistenceError('Persistence store is closed');
    return this.#document.records[scopeKey(scopeValue(scope))]?.cursor ?? null;
  }
  async compareAndSet(scope: PersistenceScope, expectedCursor: string | null, nextCursor: string): Promise<boolean> {
    if (this.#closed) throw new PersistenceError('Persistence store is closed');
    const normalized = scopeValue(scope);
    if (expectedCursor !== null) cursorValue(expectedCursor);
    cursorValue(nextCursor);
    const key = scopeKey(normalized);
    if ((this.#document.records[key]?.cursor ?? null) !== expectedCursor) return false;
    this.#document.records[key] = { scope: normalized, cursor: nextCursor };
    return true;
  }
  async close(): Promise<void> { this.#closed = true; }
}

/** Single-process file store. An abandoned .lock requires manual inspection/removal. */
export class FileCheckpointStore implements CheckpointStore {
  private constructor(private readonly document: FileDocument) {}
  static async open(path: string, options?: FileStoreOptions): Promise<FileCheckpointStore> {
    return new FileCheckpointStore(await FileDocument.open(path, 'checkpoints', options));
  }
  async load(scope: PersistenceScope): Promise<string | null> {
    const normalized = scopeValue(scope);
    return this.document.read(document => (document as CheckpointDocument).records[scopeKey(normalized)]?.cursor ?? null);
  }
  async compareAndSet(scope: PersistenceScope, expectedCursor: string | null, nextCursor: string): Promise<boolean> {
    const normalized = scopeValue(scope);
    if (expectedCursor !== null) cursorValue(expectedCursor);
    cursorValue(nextCursor);
    return this.document.update(document => {
      const records = (document as CheckpointDocument).records;
      const key = scopeKey(normalized);
      if ((records[key]?.cursor ?? null) !== expectedCursor) return false;
      records[key] = { scope: normalized, cursor: nextCursor };
      return true;
    });
  }
  close(): Promise<void> { return this.document.close(); }
}
