import { createHash } from 'node:crypto';
import { BackpressureError, ConfigurationError, ProtocolError } from '../errors.js';
import type { MessageState } from '../types.js';
import type { Message } from './types.js';

export interface StateCacheOptions { maxEntries: number; maxBytes: number }
export interface StateMergeResult { state: MessageState; changed: boolean; contentChanged: boolean; reactionsChanged: boolean }
interface Creation { fingerprint: string; identity: string; contentHash: string; isLong: boolean; attachmentsHash: string; bodyDigest?: string }
interface Entry { state?: MessageState; creation?: Creation; bytes: number }
function hash(value: string): string { return createHash('sha256').update(value).digest('hex'); }
function identity(value: Message | MessageState): string {
  return JSON.stringify([value.orgId, value.channelId, value.id, value.senderId, value.createdAt, value.threadRootId, value.priority]);
}
function attachments(value: Message | MessageState): string {
  // Asset URLs expire and are not part of attachment identity. Enumerating fields
  // also makes identity independent of JavaScript object property insertion order.
  return JSON.stringify(value.attachments.map(a => [a.ordinal, a.uploadId, a.contentHash,
    a.fileName, a.mediaType, a.byteLength, a.attachmentVariants]));
}
function immutable(value: MessageState): string {
  return JSON.stringify([identity(value), value.originalBody, attachments(value)]);
}
function content(value: MessageState): string { return JSON.stringify([value.body, value.deleted, value.updatedAt]); }
function reactions(value: MessageState): string {
  return JSON.stringify(Object.entries(value.reactions).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0));
}
function creation(value: Message): Creation {
  const attachmentIdentity = attachments(value);
  return { identity: identity(value),
    fingerprint: hash(JSON.stringify([identity(value), value.content, value.isLong, value.bodyDigest ?? null, attachmentIdentity])),
    contentHash: hash(value.content), isLong: value.isLong, attachmentsHash: hash(attachmentIdentity),
    ...(value.bodyDigest === undefined ? {} : {bodyDigest: value.bodyDigest}) };
}
function compareCreationToState(created: Creation, state: MessageState): void {
  // messages.rs::make_snippet uses the first 900 Unicode scalar values plus an ellipsis.
  const originalProjection = created.isLong ? Array.from(state.originalBody).slice(0, 900).join('') + '…' : state.originalBody;
  if (created.identity !== identity(state) || created.contentHash !== hash(originalProjection) || created.attachmentsHash !== hash(attachments(state))) {
    throw new ProtocolError('Message creation conflicts with immutable message state', {channelId: state.channelId, messageId: state.id});
  }
}

/** Per-channel LRU. Eviction bounds memory and also bounds process-local duplicate detection. */
export class StateCache {
  #entries = new Map<string, Entry>();
  #bytes = 0;
  constructor(private readonly limits: StateCacheOptions = {maxEntries: 1000, maxBytes: 8 * 1024 * 1024}) {
    if (!Number.isSafeInteger(limits.maxEntries) || limits.maxEntries < 1 || !Number.isSafeInteger(limits.maxBytes) || limits.maxBytes < 1) {
      throw new ConfigurationError('State cache limits must be positive safe integers');
    }
    this.limits = {...limits};
  }
  get size(): number { return this.#entries.size; }
  get bytes(): number { return this.#bytes; }
  ids(): string[] { return [...this.#entries.keys()]; }
  clear(): void { this.#entries.clear(); this.#bytes = 0; }
  get(messageId: string): MessageState | undefined {
    const value = this.#entries.get(messageId);
    if (!value) return undefined;
    this.touch(messageId, value);
    return value.state === undefined ? undefined : structuredClone(value.state);
  }
  /** True only for a previously unobserved creation; snapshots at revision 1 do not consume it. */
  observeMessage(message: Message): boolean {
    const created = creation(message);
    const old = this.#entries.get(message.id);
    if (old?.creation && old.creation.fingerprint !== created.fingerprint) {
      const previous = old.creation;
      // A recovery snapshot supplies full original text; the later live frame may
      // carry its snippet. Accept that representational difference only when the
      // cached authoritative state proves both bodies, and attachments still match.
      if (!old.state || previous.attachmentsHash !== created.attachmentsHash ||
          (previous.bodyDigest !== undefined && created.bodyDigest !== undefined && previous.bodyDigest !== created.bodyDigest)) {
        throw new ProtocolError('Conflicting duplicate message creation', {channelId: message.channelId, messageId: message.id});
      }
      compareCreationToState(previous, old.state);
    }
    if (old?.state) compareCreationToState(created, old.state);
    // A recovery snapshot does not carry the live-frame BLAKE3 digest. Keep any
    // digest already observed so later contradictory live frames still fail.
    if (created.bodyDigest === undefined && old?.creation?.bodyDigest !== undefined) created.bodyDigest = old.creation.bodyDigest;
    const isNew = old?.creation === undefined && !(old?.state && (old.state.deleted || old.state.revision > 1));
    this.put(message.id, {...old, creation: created, bytes: 0}, message.channelId);
    return isNew;
  }
  /** Merge content and reactions independently; equal revisions cannot disagree. */
  merge(incoming: MessageState): StateMergeResult {
    const oldEntry = this.#entries.get(incoming.id);
    if (oldEntry?.creation) compareCreationToState(oldEntry.creation, incoming);
    const old = oldEntry?.state;
    let contentChanged = !old;
    let reactionsChanged = !old;
    let state = structuredClone(incoming);
    if (old) {
      if (immutable(old) !== immutable(incoming)) throw new ProtocolError('Immutable message identity changed', {channelId: incoming.channelId, messageId: incoming.id});
      if (old.revision === incoming.revision && content(old) !== content(incoming)) {
        throw new ProtocolError('Conflicting message content at the same revision', {channelId: incoming.channelId, messageId: incoming.id});
      }
      if (old.reactionRevision === incoming.reactionRevision && reactions(old) !== reactions(incoming)) {
        throw new ProtocolError('Conflicting reactions at the same revision', {channelId: incoming.channelId, messageId: incoming.id});
      }
      contentChanged = incoming.revision > old.revision;
      reactionsChanged = incoming.reactionRevision > old.reactionRevision;
      if (contentChanged && incoming.updatedAt < old.updatedAt) {
        throw new ProtocolError('Content revision moved its timestamp backwards', {channelId: incoming.channelId, messageId: incoming.id});
      }
      if (!contentChanged) state = {...state, body: old.body, deleted: old.deleted, revision: old.revision, updatedAt: old.updatedAt};
      if (!reactionsChanged) state = {...state, reactions: structuredClone(old.reactions), reactionRevision: old.reactionRevision};
    }
    this.put(incoming.id, {...oldEntry, state, bytes: 0}, incoming.channelId);
    return { state: structuredClone(state), changed: contentChanged || reactionsChanged, contentChanged, reactionsChanged };
  }
  private touch(id: string, value: Entry): void { this.#entries.delete(id); this.#entries.set(id, value); }
  private put(id: string, entry: Entry, channelId: string): void {
    entry.bytes = Buffer.byteLength(JSON.stringify([id, entry.state ?? null, entry.creation ?? null]), 'utf8');
    if (entry.bytes > this.limits.maxBytes) throw new BackpressureError('One message exceeds the realtime state cache budget', {channelId, messageId: id});
    const old = this.#entries.get(id);
    if (old) { this.#bytes -= old.bytes; this.#entries.delete(id); }
    while (this.#entries.size >= this.limits.maxEntries || this.#bytes + entry.bytes > this.limits.maxBytes) {
      const oldest = this.#entries.entries().next().value as [string, Entry] | undefined;
      if (!oldest) break;
      this.#entries.delete(oldest[0]);
      this.#bytes -= oldest[1].bytes;
    }
    this.#entries.set(id, entry);
    this.#bytes += entry.bytes;
  }
}
