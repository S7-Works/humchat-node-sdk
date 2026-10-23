import { z } from 'zod';
import { ProtocolError } from '../errors.js';
import { assertScope, emojiSchema, identitySchema, parseState, prioritySchema, safeInteger } from '../protocol/wire.js';
import type { MessageState } from '../types.js';
import type { LiveAttachment, Message, MutationEventName, ParsedFrame } from './types.js';

const time = safeInteger.min(1).max(99_999_999_999);
const text = (limit: number) => z.string().refine(v => Buffer.byteLength(v, 'utf8') <= limit);
const digest = z.string().regex(/^[a-f0-9]{64}$/);
const nullableText = (limit: number) => text(limit).nullable().optional();
const scope = { org_uuid: identitySchema(), channel_id: identitySchema(1024) };
const actor = { ...scope, user_uuid: identitySchema(256) };
const attachment = z.object({ upload_id: identitySchema(), content_hash: digest,
  file_name: text(1024), media_type: text(256), byte_len: safeInteger.min(-1),
  attachment_variants: nullableText(65_536), url: nullableText(16_384), thumb_url: nullableText(16_384),
}).passthrough();
const messageSchema = z.object({ ...scope, id: identitySchema(), sender_id: identitySchema(256),
  content: text(1_048_576), thread_root_id: identitySchema().nullable(), created_at: time,
  is_long: z.boolean(), priority: prioritySchema, body_blake3: digest.nullable().optional(),
  attachments: z.array(attachment).max(64).optional(), attachment_hash: digest.nullable().optional(),
  attachment_name: nullableText(1024), attachment_variants: nullableText(65_536),
  attachment_url: nullableText(16_384), attachment_thumb_url: nullableText(16_384),
}).passthrough().refine(v => v.thread_root_id !== v.id);
const mutationNames: Record<string, MutationEventName> = { edit: 'messageUpdated', delete: 'messageDeleted', reaction: 'reactionUpdated' };
function read<T>(schema: z.ZodType<T>, value: unknown): T {
  const parsed = schema.safeParse(value);
  if (!parsed.success) throw new ProtocolError('Invalid realtime frame');
  return parsed.data;
}
function bound(value: Record<string, unknown>, orgId: string, channelId: string, required = true): void {
  if (required || 'org_uuid' in value) assertScope(value.org_uuid === orgId, 'Realtime organization');
  if (required || 'channel_id' in value) assertScope(value.channel_id === channelId, 'Realtime channel');
}
function message(value: unknown, orgId: string, channelId: string): Message {
  const v = read(messageSchema, value);
  bound(v, orgId, channelId);
  const attachments: LiveAttachment[] = (v.attachments ?? []).map((a, ordinal) => ({
    ordinal, uploadId: a.upload_id, contentHash: a.content_hash, fileName: a.file_name || null,
    mediaType: a.media_type || null, byteLength: a.byte_len < 0 ? null : a.byte_len, attachmentVariants: a.attachment_variants ?? null,
    url: a.url ?? null, thumbnailUrl: a.thumb_url ?? null,
  }));
  // Legacy singular attachment projection is still emitted by ServerMessageResponse.
  // canonical_attachments_from_message uses these legacy placeholders; versioned
  // state converts the empty name and negative unknown length into null values.
  if (!attachments.length && v.attachment_hash != null) attachments.push({ ordinal: 0,
    uploadId: 'legacy:0', contentHash: v.attachment_hash, fileName: v.attachment_name || null,
    mediaType: 'application/octet-stream', byteLength: null, attachmentVariants: v.attachment_variants ?? null,
    url: v.attachment_url ?? null, thumbnailUrl: v.attachment_thumb_url ?? null,
  });
  if (attachments.length && v.attachment_hash != null) {
    assertScope(attachments[0]!.contentHash === v.attachment_hash, 'Realtime attachment projection');
  }
  return { id: v.id, orgId, channelId, senderId: v.sender_id, content: v.content,
    threadRootId: v.thread_root_id, createdAt: v.created_at, isLong: v.is_long, priority: v.priority,
    ...(v.body_blake3 == null ? {} : {bodyDigest: v.body_blake3}), attachments };
}

/** Parse already-decoded JSON. The socket's verified identity supplies missing control-frame scope. */
export function parseFrame(raw: unknown, orgId: string, channelId: string): ParsedFrame {
  const value = read(z.record(z.string(), z.unknown()), raw);
  if ('type' in value && typeof value.type !== 'string') throw new ProtocolError('Invalid realtime discriminator');
  if (!('type' in value)) {
    if ('state' in value) throw new ProtocolError('Unexpected state in a message creation frame');
    return {kind: 'message', message: message(value, orgId, channelId)};
  }
  const kind = value.type as string;
  if (Object.hasOwn(mutationNames, kind)) {
    const event = mutationNames[kind]!;
    const envelope = read(z.object({...scope, message_id: identitySchema()}).passthrough(), value);
    bound(envelope, orgId, channelId);
    if ('state' in value) {
      // Presence, not truthiness: a malformed/null state must never become a legacy hint.
      const state = parseState(value.state, orgId, channelId, envelope.message_id);
      if (kind === 'delete' && !state.deleted) throw new ProtocolError('Delete frame has no tombstone');
      if (kind === 'edit' && state.deleted) throw new ProtocolError('Edit frame contains a tombstone');
      return {kind: 'state', event, state};
    }
    if (kind === 'edit') read(z.object({content: text(1_048_576), edited_at: time}).passthrough(), value);
    if (kind === 'delete') read(z.object({deleted_at: time}).passthrough(), value);
    if (kind === 'reaction') read(z.object({reactions: z.record(emojiSchema, z.array(identitySchema(256)).max(16_384))}).passthrough(), value);
    return {kind: 'legacyMutation', event, messageId: envelope.message_id};
  }
  if (kind === 'typing') {
    const v = read(z.object({...actor, ts: time, ttl_ms: safeInteger.min(0).max(4_294_967_295)}).passthrough(), value);
    bound(v, orgId, channelId);
    return {kind: 'typing', payload: {orgId, channelId, actorId: v.user_uuid, timestamp: v.ts, ttlMs: v.ttl_ms}};
  }
  if (kind === 'presence') {
    const v = read(z.object({...actor, status: z.enum(['online', 'away', 'busy', 'offline']), last_seen: time}).passthrough(), value);
    bound(v, orgId, channelId);
    return {kind: 'presence', payload: {orgId, channelId, actorId: v.user_uuid, status: v.status, lastSeen: v.last_seen}};
  }
  if (kind === 'receipt') {
    const v = read(z.object({...actor, ts: safeInteger.min(0).max(99_999_999_999)}).passthrough(), value);
    bound(v, orgId, channelId);
    return {kind: 'receipt', payload: {orgId, channelId, actorId: v.user_uuid, through: v.ts}};
  }
  if (kind === 'call_invite') {
    const v = read(z.object({...scope, meeting_id: identitySchema(), inviter_id: identitySchema(256), invitee_id: identitySchema(256), created_at: time}).passthrough(), value);
    bound(v, orgId, channelId);
    return {kind: 'callInvite', payload: {orgId, channelId, meetingId: v.meeting_id, inviterId: v.inviter_id, inviteeId: v.invitee_id, createdAt: v.created_at}};
  }
  if (kind === 'resync') {
    const v = read(z.object({channel_id: identitySchema(1024)}).passthrough(), value);
    bound(v, orgId, channelId, false);
    return {kind: 'resync'};
  }
  if (kind === 'session_revoked') {
    read(z.object({reason: text(1024)}).passthrough(), value);
    bound(value, orgId, channelId, false);
    if (value.code === 'TOKEN_EXPIRED') return { kind: 'sessionExpired' };
    // Do not turn arbitrary server strings into logs; the known wire reasons are benign constants.
    const reason = value.reason === 'session deprovisioned' || value.reason === 'session no longer valid' || value.reason === 'session expired'
      ? value.reason : 'session revoked';
    return {kind: 'sessionRevoked', reason};
  }
  bound(value, orgId, channelId, false);
  return {kind: 'unknown', type: /^[a-zA-Z][a-zA-Z0-9_.:-]{0,63}$/.test(kind) ? kind : 'unknown'};
}

/** Snapshot conversion is not an event-history reconstruction; callers choose whether to emit it. */
export function messageFromState(state: MessageState): Message {
  return { id: state.id, orgId: state.orgId, channelId: state.channelId, senderId: state.senderId,
    content: state.body, threadRootId: state.threadRootId, createdAt: state.createdAt,
    priority: state.priority, isLong: false, attachments: state.attachments.map(a => ({...a, url: null, thumbnailUrl: null})) };
}
