import { emptyDocument, keyValue, markEntrySent, normalizedRequest, prepareEntry, scopeValue, type OutboxDocument } from './model.js';
import { FileDocument } from './file.js';
import { PersistenceError, type FileStoreOptions, type OutboxEntry, type OutboxPrepareOptions, type OutboxStore, type PersistenceScope } from './types.js';
import type { SendMessageInput, SendReceipt } from '../types.js';

function pending(document: OutboxDocument, scope: PersistenceScope): OutboxEntry[] {
  const key = JSON.stringify(scope);
  return Object.values(document.records).filter(record => JSON.stringify(record.scope) === key && record.entry.status === 'pending')
    .map(record => structuredClone(record.entry));
}
/** Process-local outbox. Use a durable adapter when sends must survive a restart. */
export class MemoryOutboxStore implements OutboxStore {
  #document: OutboxDocument = emptyDocument('outbox');
  #closed = false;
  private checkOpen(): void { if (this.#closed) throw new PersistenceError('Persistence store is closed'); }
  async prepare(scope: PersistenceScope, key: string, input: SendMessageInput, options: OutboxPrepareOptions = {}): Promise<OutboxEntry> {
    this.checkOpen();
    const normalized = scopeValue(scope);
    return prepareEntry(this.#document, normalized, keyValue(key), normalizedRequest(normalized, input, options));
  }
  async markSent(scope: PersistenceScope, key: string, receipt: SendReceipt): Promise<OutboxEntry> {
    this.checkOpen();
    return markEntrySent(this.#document, scopeValue(scope), keyValue(key), receipt);
  }
  async listPending(scope: PersistenceScope): Promise<OutboxEntry[]> { this.checkOpen(); return pending(this.#document, scopeValue(scope)); }
  async close(): Promise<void> { this.#closed = true; }
}

/** Durable intent and receipt file; no network send happens inside this adapter. */
export class FileOutboxStore implements OutboxStore {
  private constructor(private readonly document: FileDocument) {}
  static async open(path: string, options?: FileStoreOptions): Promise<FileOutboxStore> {
    return new FileOutboxStore(await FileDocument.open(path, 'outbox', options));
  }
  async prepare(scope: PersistenceScope, key: string, input: SendMessageInput, options: OutboxPrepareOptions = {}): Promise<OutboxEntry> {
    const normalized = scopeValue(scope);
    const checkedKey = keyValue(key);
    const request = normalizedRequest(normalized, input, options);
    return this.document.update(document => prepareEntry(document as OutboxDocument, normalized, checkedKey, request));
  }
  async markSent(scope: PersistenceScope, key: string, receipt: SendReceipt): Promise<OutboxEntry> {
    const normalized = scopeValue(scope);
    const checkedKey = keyValue(key);
    const copied = structuredClone(receipt);
    return this.document.update(document => markEntrySent(document as OutboxDocument, normalized, checkedKey, copied));
  }
  async listPending(scope: PersistenceScope): Promise<OutboxEntry[]> {
    const normalized = scopeValue(scope);
    return this.document.read(document => pending(document as OutboxDocument, normalized));
  }
  close(): Promise<void> { return this.document.close(); }
}
