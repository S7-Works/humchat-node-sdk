/** Dates are Unix seconds. IDs are opaque, case-sensitive routing identities. */
export interface RequestOptions { signal?: AbortSignal; timeout?: number }
export interface SendOptions extends RequestOptions { idempotencyKey?: string }
export interface Identity {
  actorId: string; orgId: string; email: string; roles: string[]; scopes: string[];
  expiresAt: number | null; appId: string | null;
}
export interface ResourceContext {
  orgId: string;
  request(path: string, options?: RequestOptions & {
    method?: 'GET' | 'POST' | 'DELETE'; body?: unknown; headers?: Record<string, string>;
    replaySafe?: boolean; authRetry?: boolean; channelId?: string; operationId?: string;
    messageId?: string; idempotencyKey?: string;
  }): Promise<unknown>;
  connect(options?: RequestOptions): Promise<Identity>;
  /** Internal: available only after an authenticated request succeeds. */
  getActorId(): string;
}
export type MessagePriority = 'standard' | 'important' | 'urgent';
export interface Channel {
  id: string; orgId: string; teamId: string; slug: string; displayName: string; isPrivate: boolean;
}
export interface ChannelList { orgId: string; teamId: string; teamName: string; channels: Channel[] }
export interface ChannelListInput { teamId?: string }
export interface Conversation {
  /** Management identifier; use channelId for messaging. */
  id: string; channelId: string; name: string | null; kind: string; memberCount: number;
  myRole: string; members: Array<{ actorId: string; role: string }>; lastMessageAt: number | null;
}
export interface OrgMember { actorId: string; name: string; email: string; role: string; status: string }
export interface CreateConversationInput { memberIds: string[]; name?: string }
export interface OpenDirectInput { memberId: string }
export interface SendDirectInput { recipientId: string; content: string; id?: string; threadRootId?: string | null; priority?: MessagePriority }
export interface DirectSendReceipt { conversation: Conversation; receipt: SendReceipt }
export interface MessageAttachment {
  ordinal: number; uploadId: string | null; contentHash: string | null;
  fileName: string | null; mediaType: string | null; byteLength: number | null;
  /** Opaque server metadata; never evaluate this string. */
  attachmentVariants: string | null;
}
export interface MessageReference { id: string; channelId: string; threadRootId?: string | null }
export interface MessageState extends MessageReference {
  schema: 1; orgId: string; senderId: string; createdAt: number; threadRootId: string | null;
  priority: MessagePriority; originalBody: string; body: string; revision: number;
  updatedAt: number; deleted: boolean; reactionRevision: number;
  reactions: Record<string, string[]>; attachments: MessageAttachment[];
}
export interface SendMessageInput {
  channelId: string; content: string; id?: string; threadRootId?: string | null; priority?: MessagePriority;
}
export interface ReplyInput { content: string; id?: string; priority?: MessagePriority }
export interface MessageLocator { channelId: string; messageId: string }
export interface SendReceipt {
  id: string; channelId: string; status: 'accepted'; createdAt: number; idempotencyKey: string;
  attachmentCount: number; attachmentManifestBlake3: string;
}
export interface HistoryInput {
  channelId: string; threadRootId?: string | null; cursor?: string; limit?: number;
}
export interface HistoryPage {
  schema: 2; orgId: string; channelId: string; threadRootId: string | null;
  messages: MessageState[]; nextCursor: string | null;
}
export interface EditMessageInput extends MessageLocator { content: string; expectedRevision: number; operationId?: string }
export interface DeleteMessageInput extends MessageLocator { expectedRevision: number; operationId?: string }
export interface SetReactionInput extends MessageLocator { emoji: string; present: boolean; operationId?: string }
export interface MutationReceipt {
  schema: 1; operationId: string; actorId: string; requestDigest: string; state: MessageState;
}
