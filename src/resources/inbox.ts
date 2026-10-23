import { z } from 'zod';
import { AbortError, BackpressureError, HumChatError, ProtocolError, UnsupportedFeatureError, ValidationError } from '../errors.js';
import { withIntent } from '../http/transport.js';
import { input } from '../protocol/wire.js';
import type { SdkCapabilities } from '../auth/session.js';
import type { CheckpointStore } from '../persistence/types.js';
import type { DurableConsumer } from '../realtime/durable-types.js';
import type { RealtimeClient } from '../realtime/manager.js';
import type { ChannelSubscription } from '../realtime/subscription.js';
import type { ConnectionStatus } from '../realtime/types.js';
import type { RequestOptions, ResourceContext } from '../types.js';
import type { ConversationsResource } from './conversations.js';

export interface InboxWatchOptions {
  consumerId: string;
  /** Caller-owned; the watcher never closes this store. */
  checkpointStore: CheckpointStore;
  /** Delay after each completed discovery request, default 5000, minimum 100 milliseconds. */
  pollIntervalMs?: number;
  /** Maximum conversations admitted by this watcher; defaults to realtime.maxChannels. */
  maxConversations?: number;
}
export type InboxStatus = 'starting' | 'watching' | 'blocked' | 'closed';
export interface InboxStats { conversations: number; channels: Array<{ channelId: string; status: ConnectionStatus }> }

/** One managed consumer over the authenticated identity's conversations, including group conversations. */
export class InboxWatch {
  /** Initial discovery and consumer registration completed; channel replay can still be running. */
  readonly ready: Promise<void>;
  /** Rejects on a terminal discovery/channel/handler error; resolves when explicitly closed. */
  readonly done: Promise<void>;
  #status: InboxStatus = 'starting';
  #error: HumChatError | null = null;
  #lastChannelError: HumChatError | null = null;
  #cancel = new AbortController();
  #handles = new Map<string, ChannelSubscription>();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #refresh: Promise<void> | undefined;
  #resolveReady!: () => void;
  #rejectReady!: (error: HumChatError) => void;
  #resolveDone!: () => void;
  #rejectDone!: (error: HumChatError) => void;
  constructor(private readonly context: ResourceContext, private readonly conversations: ConversationsResource,
    private readonly realtime: RealtimeClient, private readonly capabilities: (options: RequestOptions) => Promise<SdkCapabilities>,
    private readonly handler: DurableConsumer, private readonly options: Required<InboxWatchOptions>) {
    this.ready = new Promise((resolve, reject) => { this.#resolveReady = resolve; this.#rejectReady = reject; });
    this.done = new Promise((resolve, reject) => { this.#resolveDone = resolve; this.#rejectDone = reject; });
    void this.ready.catch(() => {}); void this.done.catch(() => {});
    queueMicrotask(() => { if (!this.#cancel.signal.aborted) void this.refresh().catch(() => {}); });
  }
  get status(): InboxStatus { return this.#status; }
  get error(): HumChatError | null { return this.#error ? withIntent(this.#error, {}) : null; }
  /** Most recent channel error, including transient errors handled by the existing reconnect policy. */
  get lastChannelError(): HumChatError | null { return this.#lastChannelError ? withIntent(this.#lastChannelError, {}) : null; }
  getStats(): InboxStats {
    return { conversations: this.#handles.size,
      channels: [...this.#handles].map(([channelId, handle]) => ({ channelId, status: handle.status })) };
  }
  /** Coalesces concurrent refreshes. Periodic requests never overlap. */
  refresh(): Promise<void> {
    if (this.#status === 'closed') return Promise.reject(new AbortError('Inbox watcher is closed'));
    if (this.#status === 'blocked') return Promise.reject(this.#error!);
    if (this.#refresh) return this.#refresh;
    clearTimeout(this.#timer);
    this.#refresh = this.discover().then(() => {
      if (this.#cancel.signal.aborted) throw new AbortError('Inbox watcher is closed');
      this.#status = 'watching';
    }).catch(error => { this.fail(error); throw this.#error ?? new AbortError('Inbox watcher is closed'); }).finally(() => {
      this.#refresh = undefined;
      if (this.#status === 'watching') this.#resolveReady();
      if (!this.#cancel.signal.aborted) this.#timer = setTimeout(() => { void this.refresh().catch(() => {}); }, this.options.pollIntervalMs);
    });
    return this.#refresh;
  }
  async close(): Promise<void> {
    if (this.#status === 'closed') return;
    this.#status = 'closed';
    const error = new AbortError('Inbox watcher is closed');
    this.#cancel.abort(error); clearTimeout(this.#timer); this.#rejectReady(error);
    await this.closeHandles();
    this.#resolveDone();
  }
  private async closeHandles(): Promise<void> {
    const handles = [...this.#handles.values()]; this.#handles.clear();
    await Promise.all(handles.map(handle => handle.close()));
  }
  private fail(error: unknown): void {
    if (this.#cancel.signal.aborted) return;
    this.#error = error instanceof HumChatError ? withIntent(error, {}) : new ProtocolError('Inbox operation failed');
    this.#status = 'blocked'; this.#cancel.abort(this.#error); clearTimeout(this.#timer);
    this.#rejectReady(this.#error); this.#rejectDone(this.#error);
    void this.closeHandles();
  }
  private async discover(): Promise<void> {
    const request = { signal: this.#cancel.signal };
    const capabilities = await this.capabilities(request);
    if (!capabilities.messaging.durableReplay || !capabilities.messaging.eventProtocolVersions.includes(1)) {
      throw new UnsupportedFeatureError('Inbox watching requires durable event replay');
    }
    const identity = await this.context.connect(request);
    const conversations = await this.conversations.list(request);
    if (this.#cancel.signal.aborted) throw new AbortError('Inbox watcher is closed');
    if (conversations.length > this.options.maxConversations) throw new BackpressureError('Inbox conversation limit reached');
    for (const conversation of conversations) {
      if (!conversation.members.some(member => member.actorId === identity.actorId) ||
        conversation.memberCount !== conversation.members.length ||
        new Set(conversation.members.map(member => member.actorId)).size !== conversation.members.length) {
        throw new ProtocolError('Inbox conversation membership mismatch', { channelId: conversation.channelId });
      }
    }
    const present = new Set(conversations.map(conversation => conversation.channelId));
    const retiring: Promise<void>[] = [];
    for (const [channelId, handle] of this.#handles) {
      if (!present.has(channelId)) {
        this.#handles.delete(channelId);
        retiring.push(this.realtime.retire(handle, this.#cancel.signal));
      }
    }
    await Promise.all(retiring);
    for (const conversation of conversations) {
      if (this.#cancel.signal.aborted) throw new AbortError('Inbox watcher is closed');
      if (this.#handles.has(conversation.channelId)) continue;
      const handle = this.realtime.subscribe(conversation.channelId, { start: 'earliest' });
      this.#handles.set(conversation.channelId, handle);
      const owned = () => !this.#cancel.signal.aborted && this.#handles.get(conversation.channelId) === handle;
      handle.on('error', error => {
        if (!owned()) return;
        this.#lastChannelError = withIntent(error, {});
        if (handle.status === 'blocked') this.fail(error);
      });
      handle.on('handlerError', ({ error }) => { if (owned()) this.fail(error); });
      handle.consume((event, context) => {
        if (!owned()) throw new AbortError('Inbox conversation is no longer watched');
        return this.handler(event, { ...context, signal: AbortSignal.any([context.signal, this.#cancel.signal]) });
      }, { consumerId: this.options.consumerId, checkpointStore: this.options.checkpointStore });
      void handle.ready.catch(error => { if (owned()) this.fail(error); });
    }
  }
}

export class InboxResource {
  #watch: InboxWatch | undefined;
  #closed = false;
  constructor(private readonly context: ResourceContext, private readonly conversations: ConversationsResource,
    private readonly realtime: RealtimeClient, private readonly capabilities: (options: RequestOptions) => Promise<SdkCapabilities>) {}
  watch(handler: DurableConsumer, options: InboxWatchOptions): InboxWatch {
    if (this.#closed) throw new AbortError('Client is closed');
    if (this.realtime.config.recovery.mode !== 'durable') throw new UnsupportedFeatureError('Inbox watching requires durable recovery mode');
    if (this.#watch && this.#watch.status !== 'closed') throw new ValidationError('Close the existing inbox watcher before starting another');
    if (typeof handler !== 'function' || !options || typeof options.checkpointStore?.load !== 'function' ||
      typeof options.checkpointStore.compareAndSet !== 'function') throw new ValidationError('Invalid inbox consumer');
    const parsed = input(z.object({ consumerId: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/),
      checkpointStore: z.unknown(), pollIntervalMs: z.number().int().min(100).max(2_147_483_647).optional(),
      maxConversations: z.number().int().min(1).max(this.realtime.config.realtime.maxChannels).optional() }).strict(), options, 'inbox options');
    this.#watch = new InboxWatch(this.context, this.conversations, this.realtime, this.capabilities, handler,
      { consumerId: parsed.consumerId, checkpointStore: options.checkpointStore, pollIntervalMs: parsed.pollIntervalMs ?? 5000,
        maxConversations: parsed.maxConversations ?? this.realtime.config.realtime.maxChannels });
    return this.#watch;
  }
  async close(): Promise<void> { this.#closed = true; await this.#watch?.close(); }
}
