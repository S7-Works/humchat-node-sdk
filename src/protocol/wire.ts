import { z } from 'zod';
import { ProtocolError, ValidationError } from '../errors.js';
import type { ChannelList, Conversation, HistoryPage, MessageState, MutationReceipt, SendReceipt } from '../types.js';

// Bounds and invariants mirror zlack_common/src/chat_state.rs. Additive response
// fields are allowed; known fields and tenant/channel identity are always checked.
export const identitySchema = (maximum = 128) => z.string().min(1).max(maximum)
  .regex(/^[A-Za-z0-9._:-]+$/).refine(v => v !== '.' && v !== '..');
export const safeInteger = z.number().int().refine(Number.isSafeInteger);
export const revisionSchema = safeInteger.min(1);
const timestamp = safeInteger.min(1).max(99_999_999_999);
export const prioritySchema = z.enum(['standard', 'important', 'urgent']);
const boundedText = (maximum: number) => z.string().refine(v => Buffer.byteLength(v, 'utf8') <= maximum);
export const bodySchema = boundedText(1_048_576).refine(v => v.trim().length > 0);
export const emojiSchema = boundedText(32).refine(v => v.length > 0 && v.trim() === v && !/[\u0000-\u001f\u007f-\u009f]/u.test(v));
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const attachmentSchema = z.object({
  ordinal: safeInteger.min(0).max(63), upload_id: identitySchema().nullable(),
  content_hash: digest.nullable(), file_name: boundedText(1024).min(1).nullable(),
  media_type: boundedText(256).min(1).nullable(), byte_len: safeInteger.min(0).nullable(),
  attachment_variants: boundedText(65_536).nullable(),
}).passthrough();
const stateSchema = z.object({
  schema: z.literal(1), org_uuid: identitySchema(), channel_id: identitySchema(1024),
  message_id: identitySchema(), sender_id: identitySchema(256), created_at: timestamp,
  thread_root_id: identitySchema().nullable(), priority: prioritySchema,
  original_body: boundedText(1_048_576), body: boundedText(1_048_576), revision: revisionSchema,
  updated_at: timestamp, deleted: z.boolean(), reaction_revision: safeInteger.min(0),
  reactions: z.record(emojiSchema, z.array(identitySchema(256)).min(1).max(16_384)),
  attachments: z.array(attachmentSchema).max(64),
}).passthrough().refine(v => {
  if (v.thread_root_id === v.message_id || (v.revision === 1 && (v.deleted || v.body !== v.original_body))) return false;
  const reactions = Object.values(v.reactions);
  if (reactions.length > 256 || (v.reaction_revision === 0 && reactions.length > 0)) return false;
  if (reactions.reduce((n, actors) => n + actors.length, 0) > 16_384) return false;
  if (reactions.some(actors => actors.some((actor, i) => i > 0 && actor <= actors[i - 1]!))) return false;
  return v.attachments.every((a, i) => a.ordinal === i &&
    !/[\u0000-\u001f\u007f-\u009f]/u.test(`${a.file_name ?? ''}${a.media_type ?? ''}`));
});
const cursorSchema = z.object({
  schema: z.literal(2), org_uuid: identitySchema(), channel_id: identitySchema(1024),
  thread_root_id: identitySchema().nullable(), created_at: timestamp, message_id: identitySchema(),
}).passthrough();
export const cursorWireSchema = z.string().min(2).max(4096).regex(/^(?:[a-f0-9]{2})+$/);
const historySchema = z.object({
  schema: z.literal(2), org_uuid: identitySchema(), channel_id: identitySchema(1024),
  thread_root_id: identitySchema().nullable(), messages: z.array(stateSchema).max(200),
  next_cursor: cursorWireSchema.nullable(),
}).passthrough();

export function input<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new ValidationError(`Invalid ${label}`);
  return result.data;
}
function wire<T>(schema: z.ZodType<T>, value: unknown, label: string): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new ProtocolError(`Invalid ${label} response`);
  return result.data;
}
export function assertScope(found: boolean, label: string): asserts found {
  if (!found) throw new ProtocolError(`${label} identity or scope mismatch`);
}
export function validateChannel(channelId: string, orgId: string): void {
  input(identitySchema(1024), channelId, 'channelId');
  if (!channelId.startsWith(`${orgId}:`) || channelId.length === orgId.length + 1) {
    throw new ValidationError('channelId must be a canonical routing key in the configured organization', { channelId });
  }
}
function normalizeState(v: z.infer<typeof stateSchema>): MessageState {
  return {
    schema: 1, orgId: v.org_uuid, channelId: v.channel_id, id: v.message_id, senderId: v.sender_id,
    createdAt: v.created_at, threadRootId: v.thread_root_id, priority: v.priority,
    originalBody: v.original_body, body: v.body, revision: v.revision, updatedAt: v.updated_at,
    deleted: v.deleted, reactionRevision: v.reaction_revision, reactions: v.reactions,
    attachments: v.attachments.map(a => ({ ordinal: a.ordinal, uploadId: a.upload_id,
      contentHash: a.content_hash, fileName: a.file_name, mediaType: a.media_type,
      byteLength: a.byte_len, attachmentVariants: a.attachment_variants })),
  };
}
export function parseState(value: unknown, orgId: string, channelId: string, messageId?: string): MessageState {
  const v = wire(stateSchema, value, 'message state');
  assertScope(v.org_uuid === orgId && v.channel_id === channelId && (messageId === undefined || v.message_id === messageId), 'Message state');
  return normalizeState(v);
}
export function parseSendReceipt(value: unknown, channelId: string, messageId: string, idempotencyKey: string): SendReceipt {
  const v = wire(z.object({ status: z.literal('ACK'), message_id: identitySchema(), created_at: timestamp,
    attachment_count: safeInteger.min(0).max(64), attachment_manifest_blake3: digest }).passthrough(), value, 'send receipt');
  assertScope(v.message_id === messageId && v.attachment_count === 0, 'Send receipt');
  return { id: v.message_id, channelId, status: 'accepted', createdAt: v.created_at,
    idempotencyKey, attachmentCount: v.attachment_count, attachmentManifestBlake3: v.attachment_manifest_blake3 };
}
export function parseMutation(value: unknown, orgId: string, channelId: string, messageId: string, operationId: string, actorId: string): MutationReceipt {
  const v = wire(z.object({ schema: z.literal(1), operation_id: identitySchema(), actor_id: identitySchema(256),
    request_digest: digest, state: stateSchema }).passthrough(), value, 'mutation receipt');
  assertScope(v.operation_id === operationId && v.actor_id === actorId, 'Mutation receipt');
  return { schema: 1, operationId: v.operation_id, actorId: v.actor_id, requestDigest: v.request_digest,
    state: parseState(v.state, orgId, channelId, messageId) };
}
export function parseCursor(value: string, orgId: string, channelId: string, threadRootId: string | null, isInput = false): z.infer<typeof cursorSchema> {
  try {
    const encoded = wire(cursorWireSchema, value, 'history cursor');
    const decoded = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.from(encoded, 'hex'))) as unknown;
    const cursor = wire(cursorSchema, decoded, 'history cursor');
    assertScope(cursor.org_uuid === orgId && cursor.channel_id === channelId && cursor.thread_root_id === threadRootId, 'History cursor');
    return cursor;
  } catch {
    if (isInput) throw new ValidationError('Invalid history cursor or cursor scope', { channelId });
    throw new ProtocolError('Invalid history cursor or cursor scope', { channelId });
  }
}
function before(a: {createdAt: number; id: string}, b: {createdAt: number; id: string}): boolean {
  return a.createdAt < b.createdAt || (a.createdAt === b.createdAt && a.id < b.id);
}
export function parseHistory(value: unknown, orgId: string, channelId: string, threadRootId: string | null, requestCursor?: string): HistoryPage {
  const v = wire(historySchema, value, 'history');
  assertScope(v.org_uuid === orgId && v.channel_id === channelId && v.thread_root_id === threadRootId, 'History');
  const messages = v.messages.map(s => parseState(s, orgId, channelId));
  const ids = new Set<string>();
  for (let i = 0; i < messages.length; i++) {
    const current = messages[i]!;
    assertScope(current.threadRootId === threadRootId && !ids.has(current.id), 'History message');
    ids.add(current.id);
    if (i > 0 && !before(messages[i - 1]!, current)) throw new ProtocolError('History page is not strictly ascending', { channelId });
  }
  if (requestCursor && messages.length) {
    const bound = parseCursor(requestCursor, orgId, channelId, threadRootId);
    assertScope(before(messages[messages.length - 1]!, {createdAt: bound.created_at, id: bound.message_id}), 'History continuation');
  }
  if (v.next_cursor !== null) {
    const cursor = parseCursor(v.next_cursor, orgId, channelId, threadRootId);
    const oldest = messages[0];
    assertScope(oldest !== undefined && cursor.created_at === oldest.createdAt && cursor.message_id === oldest.id, 'History boundary');
  }
  return { schema: 2, orgId, channelId, threadRootId, messages, nextCursor: v.next_cursor };
}
export function parseChannels(value: unknown, orgId: string, teamId?: string): ChannelList {
  const channel = z.object({ channel_id: identitySchema(1024), org_uuid: identitySchema(), team_uuid: identitySchema(),
    slug: z.string().min(1), display_name: z.string(), is_private: z.boolean() }).passthrough();
  const v = wire(z.object({ org_uuid: identitySchema(), team_uuid: identitySchema(), team_name: z.string(), channels: z.array(channel) }).passthrough(), value, 'channels');
  assertScope(v.org_uuid === orgId && (teamId === undefined || v.team_uuid === teamId), 'Channel list');
  const ids = new Set<string>();
  const channels = v.channels.map(c => {
    assertScope(c.org_uuid === orgId && c.team_uuid === v.team_uuid && c.channel_id.startsWith(`${orgId}:`) && !ids.has(c.channel_id), 'Channel');
    ids.add(c.channel_id);
    return { id: c.channel_id, orgId: c.org_uuid, teamId: c.team_uuid, slug: c.slug, displayName: c.display_name, isPrivate: c.is_private };
  });
  return { orgId, teamId: v.team_uuid, teamName: v.team_name, channels };
}
export function parseConversations(value: unknown, orgId: string): Conversation[] {
  const v = wire(z.array(z.object({ id: identitySchema(), channel_key: identitySchema(1024), name: z.string().nullable(),
    kind: z.string().min(1), member_count: safeInteger.min(0), my_role: z.string(),
    members: z.array(z.object({ user_uuid: identitySchema(256), role: z.string() }).passthrough()),
    last_message_at: timestamp.nullable() }).passthrough()), value, 'conversations');
  const ids = new Set<string>();
  return v.map(c => {
    assertScope(c.channel_key === `${orgId}:conv:${c.id}` && !ids.has(c.id), 'Conversation');
    ids.add(c.id);
    return { id: c.id, channelId: c.channel_key, name: c.name, kind: c.kind, memberCount: c.member_count,
      myRole: c.my_role, members: c.members.map(m => ({actorId: m.user_uuid, role: m.role})), lastMessageAt: c.last_message_at };
  });
}
