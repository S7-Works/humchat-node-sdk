import { HumChatError } from '../errors.js';
import type { MessagePriority, SendMessageInput, SendReceipt } from '../types.js';

/** All components participate in storage identity. Never reuse a consumer ID for unrelated work. */
export interface PersistenceScope {
  serverOrigin: string;
  orgId: string;
  actorId: string;
  consumerId: string;
  channelId: string;
}
export interface CheckpointStore {
  load(scope: PersistenceScope): Promise<string | null>;
  /** False means another worker changed this checkpoint. Do not acknowledge its work. */
  compareAndSet(scope: PersistenceScope, expectedCursor: string | null, nextCursor: string): Promise<boolean>;
  close(): Promise<void>;
}
export interface OutboxIntent {
  id: string;
  channelId: string;
  content: string;
  threadRootId: string | null;
  priority: MessagePriority;
}
export interface OutboxEntry {
  key: string;
  intent: OutboxIntent;
  idempotencyKey: string;
  status: 'pending' | 'sent';
  receipt?: SendReceipt;
}
export interface OutboxPrepareOptions { idempotencyKey?: string }
export interface OutboxStore {
  /** A key binds one normalized caller intent; omitted ID/key are allocated and persisted once. */
  prepare(scope: PersistenceScope, key: string, input: SendMessageInput, options?: OutboxPrepareOptions): Promise<OutboxEntry>;
  markSent(scope: PersistenceScope, key: string, receipt: SendReceipt): Promise<OutboxEntry>;
  listPending(scope: PersistenceScope): Promise<OutboxEntry[]>;
  close(): Promise<void>;
}
export interface FileStoreOptions {
  /** Maximum serialized state, default 64 MiB. Bounds reads and writes. */
  maxBytes?: number;
}
export class PersistenceError extends HumChatError {
  constructor(message: string) { super(message, 'PERSISTENCE'); }
}
