import type { MessageState, RequestOptions } from '../types.js';
import type { CheckpointStore } from '../persistence/types.js';

export interface DurableEvent {
  schema: 1; orgId: string; channelId: string; epoch: string; eventId: string;
  actorId: string; sequence: string; cursor: string;
  kind: 'message' | 'edit' | 'delete' | 'reaction'; state: MessageState;
}
export interface EventPage {
  schema: 1; orgId: string; channelId: string; epoch: string; events: DurableEvent[];
  afterCursor: string; throughCursor: string; throughSequence: string; nextCursor: string | null;
}
export interface EventSyncInput { channelId: string; after: string; through?: string; limit?: number }
export interface SubscribeOptions { start?: 'now' | 'earliest' | { cursor: string } }
export interface ConsumerOptions { consumerId: string; checkpointStore: CheckpointStore }
export interface ConsumerContext { signal: AbortSignal; source: 'live' | 'recovery' }
export type DurableConsumer = (event: DurableEvent, context: ConsumerContext) => void | Promise<void>;
export interface EventReader { sync(input: EventSyncInput, options?: RequestOptions): Promise<EventPage> }
