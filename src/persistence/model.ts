import { createHash } from 'node:crypto';
import { monotonicFactory } from 'ulid';
import { z } from 'zod';
import { ConflictError, ValidationError } from '../errors.js';
import { bodySchema, identitySchema, prioritySchema, validateChannel } from '../protocol/wire.js';
import type { SendMessageInput, SendReceipt } from '../types.js';
import { PersistenceError, type OutboxEntry, type OutboxPrepareOptions, type PersistenceScope } from './types.js';

const nextId = monotonicFactory();
const scopeSchema = z.object({ serverOrigin: z.string().max(2048), orgId: identitySchema(), actorId: identitySchema(256),
  consumerId: identitySchema(), channelId: identitySchema(1024) }).strict();
const cursorSchema = z.string().min(1).max(16_384).refine(value => !/[\x00-\x20\x7f]/.test(value));
const requestSchema = z.object({ channelId: identitySchema(1024), content: bodySchema,
  id: identitySchema().nullable(), threadRootId: identitySchema().nullable(), priority: prioritySchema,
  idempotencyKey: identitySchema().nullable() }).strict();
const intentSchema = z.object({ channelId: identitySchema(1024), content: bodySchema, id: identitySchema(),
  threadRootId: identitySchema().nullable(), priority: prioritySchema }).strict();
const safeInteger = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const receiptSchema = z.object({ id: identitySchema(), channelId: identitySchema(1024), status: z.literal('accepted'),
  createdAt: safeInteger, idempotencyKey: identitySchema(), attachmentCount: safeInteger,
  attachmentManifestBlake3: z.string().regex(/^[0-9a-f]{64}$/) }).strict();
const entrySchema = z.object({ key: identitySchema(), intent: intentSchema, idempotencyKey: identitySchema(),
  status: z.enum(['pending', 'sent']), receipt: receiptSchema.optional() }).strict()
  .transform(({ receipt, ...entry }): OutboxEntry => ({ ...entry, ...(receipt === undefined ? {} : { receipt }) }));
const checkpointRecord = z.object({ scope: scopeSchema, cursor: cursorSchema }).strict();
const outboxRecord = z.object({ scope: scopeSchema, request: requestSchema, entry: entrySchema }).strict();
const documentSchema = z.discriminatedUnion('kind', [
  z.object({ schema: z.literal(1), kind: z.literal('checkpoints'), records: z.record(z.string(), checkpointRecord) }).strict(),
  z.object({ schema: z.literal(1), kind: z.literal('outbox'), records: z.record(z.string(), outboxRecord) }).strict(),
]);
export type StoreDocument = z.infer<typeof documentSchema>;
export type CheckpointDocument = Extract<StoreDocument, { kind: 'checkpoints' }>;
export type OutboxDocument = Extract<StoreDocument, { kind: 'outbox' }>;
export type NormalizedRequest = z.infer<typeof requestSchema>;
export function scopeValue(value: PersistenceScope): PersistenceScope {
  const result = scopeSchema.safeParse(value);
  if (!result.success) throw new ValidationError('Invalid persistence scope');
  let origin: URL;
  try { origin = new URL(result.data.serverOrigin); } catch { throw new ValidationError('Invalid persistence server origin'); }
  if (!['http:', 'https:'].includes(origin.protocol) || origin.username || origin.password || origin.pathname !== '/' || origin.search || origin.hash) {
    throw new ValidationError('Persistence server origin must be an HTTP(S) origin');
  }
  validateChannel(result.data.channelId, result.data.orgId);
  return { ...result.data, serverOrigin: origin.origin };
}
export function cursorValue(value: string): string {
  const result = cursorSchema.safeParse(value);
  if (!result.success) throw new ValidationError('Invalid checkpoint cursor');
  return result.data;
}
export function keyValue(value: string): string {
  const result = identitySchema().safeParse(value);
  if (!result.success) throw new ValidationError('Invalid outbox key');
  return result.data;
}
export function scopeKey(scope: PersistenceScope, key?: string): string {
  return createHash('sha256').update(JSON.stringify([scope.serverOrigin, scope.orgId, scope.actorId,
    scope.consumerId, scope.channelId, key ?? null])).digest('hex');
}
export function normalizedRequest(scope: PersistenceScope, input: SendMessageInput, options: OutboxPrepareOptions): NormalizedRequest {
  const parsed = z.object({ channelId: identitySchema(1024), content: bodySchema, id: identitySchema().optional(),
    threadRootId: identitySchema().nullable().optional(), priority: prioritySchema.optional() }).strict().safeParse(input);
  const parsedOptions = z.object({ idempotencyKey: identitySchema().optional() }).strict().safeParse(options);
  if (!parsed.success || !parsedOptions.success) throw new ValidationError('Invalid outbox send intent');
  if (parsed.data.channelId !== scope.channelId) throw new ValidationError('Outbox intent channel does not match its scope');
  return { channelId: parsed.data.channelId, content: parsed.data.content, id: parsed.data.id ?? null,
    threadRootId: parsed.data.threadRootId ?? null, priority: parsed.data.priority ?? 'standard',
    idempotencyKey: parsedOptions.data.idempotencyKey ?? null };
}
export function prepareEntry(document: OutboxDocument, scope: PersistenceScope, key: string, request: NormalizedRequest): OutboxEntry {
  const hash = scopeKey(scope, key);
  const previous = document.records[hash];
  if (previous) {
    if (JSON.stringify(previous.request) !== JSON.stringify(request)) throw new ConflictError('Outbox key is already bound to another send intent');
    return structuredClone(previous.entry);
  }
  const id = request.id ?? nextId();
  const entry: OutboxEntry = { key, intent: { id, channelId: request.channelId, content: request.content,
    threadRootId: request.threadRootId, priority: request.priority }, idempotencyKey: request.idempotencyKey ?? id, status: 'pending' };
  document.records[hash] = { scope, request, entry };
  return structuredClone(entry);
}
export function markEntrySent(document: OutboxDocument, scope: PersistenceScope, key: string, receipt: SendReceipt): OutboxEntry {
  const parsed = receiptSchema.safeParse(receipt);
  if (!parsed.success) throw new ValidationError('Invalid outbox send receipt');
  const record = document.records[scopeKey(scope, key)];
  if (!record) throw new ConflictError('Outbox intent does not exist');
  validateReceipt(record.entry, parsed.data);
  if (record.entry.status === 'sent' && JSON.stringify(record.entry.receipt) !== JSON.stringify(parsed.data)) {
    throw new ConflictError('Outbox receipt conflicts with the recorded send');
  }
  record.entry.status = 'sent';
  record.entry.receipt = parsed.data;
  return structuredClone(record.entry);
}
function validateReceipt(entry: OutboxEntry, receipt: SendReceipt): void {
  if (receipt.id !== entry.intent.id || receipt.channelId !== entry.intent.channelId || receipt.idempotencyKey !== entry.idempotencyKey) {
    throw new ConflictError('Outbox receipt does not match its send intent');
  }
}
export function parseDocument(value: unknown, kind: StoreDocument['kind']): StoreDocument {
  try {
    const document = documentSchema.parse(value);
    if (document.kind !== kind) throw new Error('wrong kind');
    for (const [hash, record] of Object.entries(document.records)) {
      const scope = scopeValue(record.scope);
      if (JSON.stringify(scope) !== JSON.stringify(record.scope)) throw new Error('noncanonical scope');
      if ('entry' in record) {
        const { entry, request } = record;
        if (hash !== scopeKey(scope, entry.key) || request.channelId !== scope.channelId || entry.intent.channelId !== scope.channelId
            || entry.intent.content !== request.content || entry.intent.threadRootId !== request.threadRootId || entry.intent.priority !== request.priority
            || (request.id !== null && request.id !== entry.intent.id) || (request.idempotencyKey !== null && request.idempotencyKey !== entry.idempotencyKey)
            || (request.idempotencyKey === null && entry.idempotencyKey !== entry.intent.id)
            || (entry.status === 'sent') !== (entry.receipt !== undefined)) throw new Error('invalid stored intent');
        if (entry.receipt) validateReceipt(entry, entry.receipt);
      } else if (hash !== scopeKey(scope)) throw new Error('invalid checkpoint scope');
    }
    return document;
  } catch { throw new PersistenceError('Persistence file is invalid or belongs to another store format'); }
}
export function emptyDocument(kind: 'checkpoints'): CheckpointDocument;
export function emptyDocument(kind: 'outbox'): OutboxDocument;
export function emptyDocument(kind: StoreDocument['kind']): StoreDocument {
  return kind === 'checkpoints' ? { schema: 1, kind, records: {} } : { schema: 1, kind, records: {} };
}
