import { AbortError, HumChatError, ProtocolError, UnsupportedFeatureError } from '../errors.js';
import type { ConsumerOptions, DurableConsumer } from './durable-types.js';
import { budget, raceSignal, withIntent } from '../http/transport.js';
import type { ConnectionStatus, RealtimeEvents } from './types.js';

export type EventListener<K extends keyof RealtimeEvents> = (event: RealtimeEvents[K]) => void | Promise<void>;
export interface SubscriptionOwner {
  status: ConnectionStatus;
  detach(handle: ChannelSubscription): void;
  resync(): Promise<void>;
  sendSignal(type: 'typing' | 'read', through?: number): Promise<void>;
  consume?(handle: ChannelSubscription, handler: DurableConsumer, options: ConsumerOptions): void;
}

/** One observer handle; multiple handles can share a channel's physical socket. */
export class ChannelSubscription {
  readonly ready: Promise<void>;
  #resolve!: () => void;
  #reject!: (error: HumChatError) => void;
  #closed = new AbortController();
  #listeners = new Map<keyof RealtimeEvents, Set<(event: never) => void | Promise<void>>>();
  #notifications = new Map<keyof RealtimeEvents, RealtimeEvents[keyof RealtimeEvents]>();
  #notifying = new Set<keyof RealtimeEvents>();
  constructor(readonly channelId: string, private readonly owner: SubscriptionOwner, private readonly handlerTimeoutMs: number) {
    this.ready = new Promise<void>((resolve, reject) => { this.#resolve = resolve; this.#reject = reject; });
    void this.ready.catch(() => {}); // A rejected readiness promise is also observable through error events.
  }
  get status(): ConnectionStatus { return this.#closed.signal.aborted ? 'closed' : this.owner.status; }
  on<K extends keyof RealtimeEvents>(event: K, listener: EventListener<K>): this {
    this.checkOpen();
    let listeners = this.#listeners.get(event);
    if (!listeners) { listeners = new Set(); this.#listeners.set(event, listeners); }
    listeners.add(listener as (event: never) => void | Promise<void>);
    return this;
  }
  off<K extends keyof RealtimeEvents>(event: K, listener: EventListener<K>): this {
    this.#listeners.get(event)?.delete(listener as (event: never) => void | Promise<void>);
    return this;
  }
  async typing(): Promise<void> { this.checkOpen(); await this.owner.sendSignal('typing'); }
  consume(handler: DurableConsumer, options: ConsumerOptions): this {
    this.checkOpen();
    if (!this.owner.consume) throw new UnsupportedFeatureError('Acknowledged consumption requires durable recovery');
    this.owner.consume(this, handler, options);
    return this;
  }
  async markRead(input: { through: number }): Promise<void> { this.checkOpen(); await this.owner.sendSignal('read', input.through); }
  async resync(): Promise<void> { this.checkOpen(); await this.owner.resync(); }
  async close(): Promise<void> {
    if (this.#closed.signal.aborted) return;
    const error = new AbortError('Subscription is closed');
    this.#closed.abort(error);
    this.#reject(error);
    this.owner.detach(this);
    this.#listeners.clear();
    this.#notifications.clear();
  }
  private checkOpen(): void { if (this.#closed.signal.aborted || this.owner.status === 'closed') throw new AbortError('Subscription is closed'); }
  /** @internal */ resolveReady(): void { if (!this.#closed.signal.aborted) this.#resolve(); }
  /** @internal */ rejectReady(error: HumChatError): void { this.#reject(withIntent(error, { channelId: this.channelId })); }
  /** @internal Control notifications never become an unhandled EventEmitter error. */
  notify<K extends keyof RealtimeEvents>(event: K, payload: RealtimeEvents[K]): void {
    if (this.#closed.signal.aborted) return;
    // Observational notifications retain at most one active and one latest value per kind.
    this.#notifications.set(event, payload);
    if (this.#notifying.has(event)) return;
    this.#notifying.add(event);
    void (async () => {
      try {
        while (!this.#closed.signal.aborted && this.#notifications.has(event)) {
          const latest = this.#notifications.get(event)! as RealtimeEvents[K];
          this.#notifications.delete(event);
          await this.dispatch(event, latest, this.#closed.signal, false);
        }
      } finally { this.#notifying.delete(event); this.#notifications.delete(event); }
    })().catch(() => {});
  }
  /** @internal Application delivery is serialized and awaited by the channel worker. */
  async dispatch<K extends keyof RealtimeEvents>(event: K, payload: RealtimeEvents[K], signal: AbortSignal, failOnHandler = true): Promise<void> {
    for (const listener of [...(this.#listeners.get(event) ?? [])]) {
      if (this.#closed.signal.aborted || signal.aborted) return;
      const lifetime = budget(this.handlerTimeoutMs, [signal, this.#closed.signal]);
      try {
        const copy = payload instanceof HumChatError ? withIntent(payload, {})
          : event === 'handlerError' || event === 'authRequired' ? { ...payload, error: withIntent((payload as { error: HumChatError }).error, {}) }
            : structuredClone(payload);
        await raceSignal(Promise.resolve().then(() => {
          if (this.#closed.signal.aborted || lifetime.signal.aborted) return;
          return listener(copy as never);
        }), lifetime.signal);
      } catch {
        if (this.#closed.signal.aborted || signal.aborted) return;
        const error = new ProtocolError('Event handler failed or exceeded its deadline', { channelId: this.channelId });
        if (event !== 'handlerError' && event !== 'error') this.notify('handlerError', { event, error });
        if (failOnHandler) throw error;
      } finally { lifetime.dispose(); }
    }
  }
}
