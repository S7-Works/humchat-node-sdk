import { z } from 'zod';
import { ProtocolError, ValidationError } from '../errors.js';
import { budget } from '../http/transport.js';
import { identitySchema, input, parseConversations } from '../protocol/wire.js';
import type { Conversation, CreateConversationInput, OpenDirectInput, RequestOptions, ResourceContext } from '../types.js';

export class ConversationsResource {
  constructor(private readonly context: ResourceContext, private readonly totalTimeoutMs: number) {}

  async list(options: RequestOptions = {}): Promise<Conversation[]> {
    return parseConversations(await this.context.request('/api/conversations', options), this.context.orgId);
  }

  /** Creation is not automatically replayed: the current server has no creation idempotency key. */
  async create(params: CreateConversationInput, options: RequestOptions = {}): Promise<Conversation> {
    const value = input(z.object({ memberIds: z.array(identitySchema(256)).min(1).max(1000),
      name: z.string().max(4096).optional() }).strict(), params, 'conversation create input');
    if (new Set(value.memberIds).size !== value.memberIds.length) throw new ValidationError('Conversation members must be distinct');
    const name = value.name?.trim() || null;
    const raw = await this.context.request('/api/conversations', { ...options, method: 'POST', replaySafe: false, authRetry: false,
      body: { member_uuids: value.memberIds, name } });
    const conversation = parseConversations([raw], this.context.orgId)[0]!;
    const expected = new Set([...value.memberIds, this.context.getActorId()]);
    const actual = new Set(conversation.members.map(member => member.actorId));
    if (conversation.name !== name || conversation.memberCount !== expected.size || actual.size !== conversation.members.length ||
      actual.size !== expected.size || [...actual].some(id => !expected.has(id))) {
      throw new ProtocolError('Created conversation does not match the requested participants');
    }
    return conversation;
  }

  /** Find an unnamed exact two-person conversation, otherwise ask the server to create/reuse one. */
  async openDirect(params: OpenDirectInput, options: RequestOptions = {}): Promise<Conversation> {
    const value = input(z.object({ memberId: identitySchema(256) }).strict(), params, 'direct conversation input');
    const operation = budget(this.totalTimeoutMs, options.signal ? [options.signal] : []);
    const request = { ...options, signal: operation.signal };
    try {
      const identity = await this.context.connect(request);
      if (value.memberId === identity.actorId) throw new ValidationError('A direct conversation requires another organization member');
      const conversations = await this.list(request);
      const existing = conversations.find(conversation => !conversation.name?.trim() && conversation.memberCount === 2 &&
        conversation.members.length === 2 && new Set(conversation.members.map(member => member.actorId)).size === 2 &&
        conversation.members.some(member => member.actorId === identity.actorId) &&
        conversation.members.some(member => member.actorId === value.memberId));
      return existing ?? await this.create({ memberIds: [value.memberId] }, request);
    } finally { operation.dispose(); }
  }
}
