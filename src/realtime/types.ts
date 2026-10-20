import type { HumChatError } from '../errors.js';
import type { MessageAttachment, MessagePriority, MessageState } from '../types.js';
import type { DurableEvent } from './durable-types.js';

export type EventSource = 'live' | 'recovery';
export interface LiveAttachment extends MessageAttachment {
  /** Delivery-time asset locations; they are not attachment identity. */
  url: string | null;
  thumbnailUrl: string | null;
}
export interface Message {
  id: string; orgId: string; channelId: string; senderId: string; content: string;
  threadRootId: string | null; createdAt: number; priority: MessagePriority;
  /** content is only a snippet when true; fetch messages.getState() for full current text. */
  isLong: boolean;
  /** Digest of the immutable original full body, never of an edited body. */
  bodyDigest?: string;
  attachments: LiveAttachment[];
}
export interface TypingEvent { orgId: string; channelId: string; actorId: string; timestamp: number; ttlMs: number }
export interface PresenceEvent { orgId: string; channelId: string; actorId: string; status: 'online' | 'away' | 'busy' | 'offline'; lastSeen: number }
export interface ReceiptEvent { orgId: string; channelId: string; actorId: string; through: number }
export interface CallInviteEvent { orgId: string; channelId: string; meetingId: string; inviterId: string; inviteeId: string; createdAt: number }
export type ConnectionStatus = 'idle' | 'connecting' | 'syncing' | 'live' | 'backoff' | 'blocked' | 'closed';
export type MutationEventName = 'messageUpdated' | 'messageDeleted' | 'reactionUpdated';
export interface RecoveryCoverage {
  channelId: string; mode: 'bestEffort'; reason: string;
  pages: number; states: number; complete: false;
}
export interface RealtimeEvents {
  event: DurableEvent;
  checkpoint: { channelId: string; consumerId: string; cursor: string; sequence: string };
  message: { message: Message; source: EventSource; replayed: boolean };
  messageUpdated: { state: MessageState; source: EventSource };
  messageDeleted: { state: MessageState; source: EventSource };
  reactionUpdated: { state: MessageState; source: EventSource };
  stateReconciled: { state: MessageState; source: 'recovery' };
  typing: TypingEvent;
  presence: PresenceEvent;
  receipt: ReceiptEvent;
  callInvite: CallInviteEvent;
  connection: { channelId: string; state: ConnectionStatus; attempt: number; nextRetryAt?: number };
  resync: { channelId: string; reason: string };
  recoveryIncomplete: RecoveryCoverage & { limitations: string[] };
  recoveryComplete: RecoveryCoverage | { channelId: string; mode: 'durable'; reason: string; pages: number; states: number; complete: true; cursor: string };
  sessionRevoked: { channelId: string; reason: string };
  authRequired: { channelId: string; error: HumChatError };
  error: HumChatError;
  handlerError: { event: keyof RealtimeEvents | 'consume'; error: HumChatError };
  unknownEvent: { channelId: string; type: string };
}
export type ParsedFrame =
  | { kind: 'message'; message: Message }
  | { kind: 'state'; event: MutationEventName; state: MessageState }
  | { kind: 'legacyMutation'; event: MutationEventName; messageId: string }
  | { kind: 'typing'; payload: TypingEvent }
  | { kind: 'presence'; payload: PresenceEvent }
  | { kind: 'receipt'; payload: ReceiptEvent }
  | { kind: 'callInvite'; payload: CallInviteEvent }
  | { kind: 'resync' }
  | { kind: 'sessionRevoked'; reason: string }
  | { kind: 'unknown'; type: string };
