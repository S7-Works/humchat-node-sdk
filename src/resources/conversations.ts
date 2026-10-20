import { parseConversations } from '../protocol/wire.js';
import type { Conversation, RequestOptions, ResourceContext } from '../types.js';

export class ConversationsResource {
  constructor(private readonly context: ResourceContext) {}

  async list(options: RequestOptions = {}): Promise<Conversation[]> {
    return parseConversations(await this.context.request('/api/conversations', options), this.context.orgId);
  }
}
