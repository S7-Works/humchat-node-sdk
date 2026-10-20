import { monotonicFactory } from 'ulid';
import { z } from 'zod';
import { ProtocolError } from '../errors.js';
import { withIntent } from '../http/transport.js';
import { assertScope, bodySchema, cursorWireSchema, emojiSchema, identitySchema, input, parseCursor,
  parseHistory, parseMutation, parseSendReceipt, parseState, prioritySchema, revisionSchema, validateChannel } from '../protocol/wire.js';
import type { DeleteMessageInput, EditMessageInput, HistoryInput, HistoryPage, MessageLocator,
  MessageReference, MessageState, MutationReceipt, ReplyInput, RequestOptions, ResourceContext,
  SendMessageInput, SendOptions, SendReceipt, SetReactionInput } from '../types.js';

const nextId = monotonicFactory();
const locator = { channelId: identitySchema(1024), messageId: identitySchema() };
const mutationBase = { ...locator, operationId: identitySchema().optional() };
const expectedRevision = revisionSchema.max(Number.MAX_SAFE_INTEGER - 1);
type Mutation = { kind: 'edit'; body: string; expected_revision: number }
  | { kind: 'delete'; expected_revision: number }
  | { kind: 'set_reaction'; emoji: string; present: boolean };

export class MessagesResource {
  constructor(private readonly context: ResourceContext) {}

  async send(params: SendMessageInput, options: SendOptions = {}): Promise<SendReceipt> {
    const value = input(z.object({ channelId: identitySchema(1024), content: bodySchema,
      id: identitySchema().optional(), threadRootId: identitySchema().nullable().optional(),
      priority: prioritySchema.optional() }).strict(), params, 'send input');
    validateChannel(value.channelId, this.context.orgId);
    const id = value.id ?? nextId();
    const key = input(identitySchema(), options.idempotencyKey ?? id, 'idempotencyKey');
    const intent = { channelId: value.channelId, messageId: id, idempotencyKey: key };
    const body = { id, channel_id: value.channelId, content: value.content,
      thread_root_id: value.threadRootId ?? null, priority: value.priority ?? 'standard' };
    try {
      const response = await this.context.request('/api/messages/send', {
        ...options, ...intent, method: 'POST', body, replaySafe: true,
        headers: { 'X-Idempotency-Key': key, 'X-Zlack-Message-Receipt': 'v1' },
      });
      return parseSendReceipt(response, value.channelId, id, key);
    } catch (error) { throw withIntent(error, intent); }
  }

  async reply(message: MessageReference, params: ReplyInput, options: SendOptions = {}): Promise<SendReceipt> {
    // A full MessageState is a valid reference, so allow its additional fields.
    const target = input(z.object({ id: identitySchema(), channelId: identitySchema(1024),
      threadRootId: identitySchema().nullable().optional() }).passthrough(), message, 'reply target');
    const value = input(z.object({ content: bodySchema, id: identitySchema().optional(),
      priority: prioritySchema.optional() }).strict(), params, 'reply input');
    return this.send({ channelId: target.channelId, content: value.content,
      threadRootId: target.threadRootId ?? target.id,
      ...(value.id === undefined ? {} : { id: value.id }),
      ...(value.priority === undefined ? {} : { priority: value.priority }) }, options);
  }

  async getState(params: MessageLocator, options: RequestOptions = {}): Promise<MessageState> {
    const value = input(z.object(locator).strict(), params, 'message locator');
    validateChannel(value.channelId, this.context.orgId);
    const response = await this.context.request(`/api/messages/${encodeURIComponent(value.messageId)}/state`, { ...options, ...value });
    return parseState(response, this.context.orgId, value.channelId, value.messageId);
  }

  async history(params: HistoryInput, options: RequestOptions = {}): Promise<HistoryPage> {
    const value = input(z.object({ channelId: identitySchema(1024), threadRootId: identitySchema().nullable().optional(),
      cursor: cursorWireSchema.optional(), limit: z.number().int().min(1).max(200).optional() }).strict(), params, 'history input');
    validateChannel(value.channelId, this.context.orgId);
    const thread = value.threadRootId ?? null;
    if (value.cursor !== undefined) parseCursor(value.cursor, this.context.orgId, value.channelId, thread, true);
    const query = new URLSearchParams({ channel_id: value.channelId });
    if (thread !== null) query.set('thread_root_id', thread);
    if (value.cursor !== undefined) query.set('native_cursor', value.cursor);
    if (value.limit !== undefined) query.set('limit', String(value.limit));
    const response = await this.context.request(`/api/messages/sync?${query}`, {
      ...options, channelId: value.channelId, headers: { 'X-Zlack-Message-State': 'v2' },
    });
    const page = parseHistory(response, this.context.orgId, value.channelId, thread, value.cursor);
    if (value.limit !== undefined && page.messages.length > value.limit) throw new ProtocolError('History exceeded requested limit', {channelId: value.channelId});
    return page;
  }

  /** Yield successively older pages; each page's messages are in ascending order. */
  async *iterateHistory(params: HistoryInput, options: RequestOptions = {}): AsyncGenerator<HistoryPage, void, undefined> {
    // Copy before awaiting so later caller mutations cannot change a traversal's scope.
    const original = { ...params };
    const seen = new Set<string>();
    let cursor = original.cursor;
    while (true) {
      if (cursor !== undefined) {
        if (seen.has(cursor)) throw new ProtocolError('History cursor repeated', { channelId: original.channelId });
        seen.add(cursor);
      }
      const page = await this.history({ ...original, ...(cursor === undefined ? {} : {cursor}) }, options);
      yield page;
      if (page.nextCursor === null) return;
      cursor = page.nextCursor;
    }
  }

  async edit(params: EditMessageInput, options: RequestOptions = {}): Promise<MutationReceipt> {
    const value = input(z.object({ ...mutationBase, content: bodySchema, expectedRevision }).strict(), params, 'edit input');
    return this.mutate(value, { kind: 'edit', body: value.content, expected_revision: value.expectedRevision }, options);
  }
  async delete(params: DeleteMessageInput, options: RequestOptions = {}): Promise<MutationReceipt> {
    const value = input(z.object({ ...mutationBase, expectedRevision }).strict(), params, 'delete input');
    return this.mutate(value, { kind: 'delete', expected_revision: value.expectedRevision }, options);
  }
  async setReaction(params: SetReactionInput, options: RequestOptions = {}): Promise<MutationReceipt> {
    const value = input(z.object({ ...mutationBase, emoji: emojiSchema, present: z.boolean() }).strict(), params, 'reaction input');
    return this.mutate(value, { kind: 'set_reaction', emoji: value.emoji, present: value.present }, options);
  }
  private async mutate(params: {channelId: string; messageId: string; operationId?: string | undefined}, mutation: Mutation, options: RequestOptions): Promise<MutationReceipt> {
    validateChannel(params.channelId, this.context.orgId);
    const intent = { channelId: params.channelId, messageId: params.messageId, operationId: params.operationId ?? nextId() };
    try {
      const response = await this.context.request('/api/messages/mutate', { ...options, ...intent,
        method: 'POST', replaySafe: true, body: { operation_id: intent.operationId,
          channel_id: params.channelId, message_id: params.messageId, mutation } });
      const actorId = this.context.getActorId();
      const receipt = parseMutation(response, this.context.orgId, params.channelId, params.messageId, intent.operationId, actorId);
      if (mutation.kind === 'edit') {
        assertScope(receipt.state.senderId === actorId && !receipt.state.deleted && receipt.state.body === mutation.body &&
          (receipt.state.revision === mutation.expected_revision || receipt.state.revision === mutation.expected_revision + 1), 'Edit result');
      } else if (mutation.kind === 'delete') {
        assertScope(receipt.state.senderId === actorId && receipt.state.deleted && receipt.state.revision === mutation.expected_revision + 1, 'Delete result');
      } else {
        assertScope(!receipt.state.deleted && (receipt.state.reactions[mutation.emoji]?.includes(actorId) ?? false) === mutation.present, 'Reaction result');
      }
      return receipt;
    } catch (error) { throw withIntent(error, intent); }
  }
}
