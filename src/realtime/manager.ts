import WebSocket from 'ws';
import type { ResolvedConfig } from '../config.js';
import type { SessionManager, VerifiedSession } from '../auth/session.js';
import type { MessagesResource } from '../resources/messages.js';
import type { MessageState } from '../types.js';
import { AbortError, AuthenticationError, BackpressureError, HumChatError, NetworkError, NotFoundError,
  PermissionError, ProtocolError, RateLimitError, ServerError, SessionRevokedError, TimeoutError, UnsupportedFeatureError, ValidationError } from '../errors.js';
import { aborted, budget, raceSignal, withIntent } from '../http/transport.js';
import { validateChannel } from '../protocol/wire.js';
import { parseFrame, messageFromState } from './protocol.js';
import { StateCache } from './state-cache.js';
import { ChannelSubscription, type SubscriptionOwner } from './subscription.js';
import type { ConnectionStatus, Message, ParsedFrame, RealtimeEvents, RecoveryCoverage } from './types.js';
import { DurableChannelWorker } from './durable-worker.js';
import type { SubscribeOptions } from './durable-types.js';
import type { EventsResource } from '../resources/events.js';
import { MultiplexTransport } from './multiplex-transport.js';

export interface RealtimeStats {
  activeSockets: number; channels: number; subscriptions: number; queuedEvents: number; queuedBytes: number;
  reconnectCount: number; recoveryCount: number; authRefreshFailures: number; droppedEphemeralCount: number;
}
export type SubscriptionResult = { channelId: string; subscription: ChannelSubscription; error?: never }
  | { channelId: string; error: HumChatError; subscription?: never };
type Position = { createdAt: number; id: string };
const newer = (a: Position, b: Position) => a.createdAt > b.createdAt || (a.createdAt === b.createdAt && a.id > b.id);
const ephemeral = (frame: ParsedFrame) => ['typing', 'presence', 'receipt', 'callInvite', 'unknown'].includes(frame.kind);

/** Registry, shared admission budget, and one socket worker per canonical channel. */
export class RealtimeClient {
  /** @internal */ readonly multiplex: MultiplexTransport | undefined;
  #channels = new Map<string, ChannelWorker | DurableChannelWorker>();
  #starts = new Map<string, string>();
  #slots = 0;
  #waiters: Array<() => void> = [];
  #ended = false;
  #revoked = false;
  #removeSessionListener: () => void;
  readonly counters = { queuedEvents: 0, queuedBytes: 0, reconnectCount: 0, recoveryCount: 0, authRefreshFailures: 0, droppedEphemeralCount: 0 };
  constructor(readonly config: ResolvedConfig, readonly sessions: SessionManager, readonly messages: MessagesResource, readonly events: EventsResource) {
    this.multiplex = config.realtime.transport === 'multiplex' ? new MultiplexTransport(this) : undefined;
    this.#removeSessionListener = sessions.onChange(session => {
      this.multiplex?.rotate(session);
      for (const worker of this.#channels.values()) worker.rotate(session);
    });
  }
  subscribe(channelId: string, options: SubscribeOptions = {}): ChannelSubscription {
    if (this.#ended) throw new AbortError('Client is closed');
    if (this.#revoked) throw new SessionRevokedError('Session ended; create a new client');
    if (!options || typeof options !== 'object' || Object.keys(options).some(key => key !== 'start')) throw new ValidationError('Invalid subscription options');
    const start = options.start ?? 'now';
    if (start !== 'now' && start !== 'earliest' && !(start && typeof start === 'object' && Object.keys(start).length === 1 && typeof start.cursor === 'string')) throw new ValidationError('Invalid subscription start');
    if (this.config.recovery.mode === 'bestEffort' && options.start !== undefined) throw new UnsupportedFeatureError('Explicit event positions require durable recovery');
    const startKey = JSON.stringify(start);
    validateChannel(channelId, this.config.orgId);
    let worker = this.#channels.get(channelId);
    if (worker && this.#starts.get(channelId) !== startKey) throw new ValidationError('Shared channel handles must use the same start option');
    if (!worker) {
      if (this.#channels.size >= this.config.realtime.maxChannels) throw new BackpressureError('Channel subscription limit reached', { channelId });
      worker = this.config.recovery.mode === 'bestEffort' ? new ChannelWorker(channelId, this)
        : new DurableChannelWorker(channelId, this, structuredClone(options));
      this.#channels.set(channelId, worker);
      this.#starts.set(channelId, startKey);
      queueMicrotask(() => worker!.start());
    }
    return worker.attach();
  }
  subscribeMany(channelIds: readonly string[]): SubscriptionResult[] {
    return channelIds.map(channelId => {
      try { return { channelId, subscription: this.subscribe(channelId) }; }
      catch (error) { return { channelId, error: withIntent(error, { channelId }) }; }
    });
  }
  getStats(): RealtimeStats {
    const workers = [...this.#channels.values()];
    return { ...this.counters, channels: workers.length, subscriptions: workers.reduce((n, c) => n + c.handles.size, 0),
      activeSockets: this.multiplex ? Number(this.multiplex.socketOpen) : workers.filter(c => c.socketOpen).length };
  }
  async close(): Promise<void> {
    if (this.#ended) return;
    this.#ended = true;
    this.#removeSessionListener();
    for (const worker of [...this.#channels.values()]) worker.stop();
    this.multiplex?.close();
    this.#channels.clear();
    this.#starts.clear();
  }
  /** @internal */ remove(channelId: string, worker: ChannelWorker | DurableChannelWorker): void {
    if (this.#channels.get(channelId) === worker) { this.#channels.delete(channelId); this.#starts.delete(channelId); }
  }
  /** @internal */ revoke(reason: string): void {
    this.#revoked = true;
    this.sessions.revoke();
    for (const worker of this.#channels.values()) {
      worker.notify('sessionRevoked', { channelId: worker.channelId, reason });
      worker.block(new SessionRevokedError('Session ended; automatic reconnect is disabled', { channelId: worker.channelId }));
    }
    this.multiplex?.block(new SessionRevokedError('Session ended; automatic reconnect is disabled'));
  }
  /** @internal Returns true only when an explicitly negotiated expiry permits reconnect. */
  endSession(frame: Extract<ParsedFrame, { kind: 'sessionRevoked' | 'sessionExpired' }>, session: VerifiedSession): boolean {
    if (frame.kind === 'sessionExpired' && session.capabilities.authentication.socketSessionEndCodes.includes('TOKEN_EXPIRED')) {
      this.sessions.expire(session);
      return true;
    }
    this.revoke(frame.kind === 'sessionRevoked' ? frame.reason : 'session no longer valid');
    return false;
  }
  /** @internal */ async acquire(signal: AbortSignal): Promise<() => void> {
    if (signal.aborted) throw aborted(signal);
    if (this.#slots >= this.config.realtime.connectConcurrency) {
      await new Promise<void>((resolve, reject) => {
        const cancel = () => { this.#waiters = this.#waiters.filter(fn => fn !== wake); signal.removeEventListener('abort', cancel); reject(aborted(signal)); };
        const wake = () => { signal.removeEventListener('abort', cancel); resolve(); };
        this.#waiters.push(wake);
        signal.addEventListener('abort', cancel, { once: true });
      });
    } else this.#slots++;
    if (signal.aborted) { this.release(); throw aborted(signal); }
    let released = false;
    return () => { if (!released) { released = true; this.release(); } };
  }
  private release(): void { const next = this.#waiters.shift(); if (next) next(); else this.#slots--; }
}

class ChannelWorker implements SubscriptionOwner {
  readonly handles = new Set<ChannelSubscription>();
  status: ConnectionStatus = 'idle';
  #cache: StateCache;
  #threads = new Map<string, true>();
  #watermarks = new Map<string, Position | null>();
  #socket: WebSocket | undefined;
  #session: VerifiedSession | undefined;
  #cycle = new AbortController();
  #generation = 0;
  #attempt = 0;
  #baseline = false;
  #ready = false;
  #queue: Array<{ frame: ParsedFrame; bytes: number }> = [];
  #draining: Promise<void> | undefined;
  #recovering: Promise<void> | undefined;
  #resyncAgain = false;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;
  #pingTimer: ReturnType<typeof setInterval> | undefined;
  #pongTimer: ReturnType<typeof setTimeout> | undefined;
  #expiryTimer: ReturnType<typeof setTimeout> | undefined;
  #stableTimer: ReturnType<typeof setTimeout> | undefined;
  constructor(readonly channelId: string, private readonly manager: RealtimeClient) {
    const c = manager.config.realtime;
    this.#cache = new StateCache({ maxEntries: c.maxCachedMessages, maxBytes: c.maxCachedBytes });
  }
  get socketOpen(): boolean { return this.#socket?.readyState === WebSocket.OPEN; }
  attach(): ChannelSubscription {
    const handle = new ChannelSubscription(this.channelId, this, this.manager.config.realtime.handlerTimeoutMs);
    this.handles.add(handle);
    if (this.status === 'live' && this.#ready) queueMicrotask(() => {
      if (this.status === 'live' && this.#ready) handle.resolveReady();
    });
    if (this.status === 'blocked') queueMicrotask(() => handle.rejectReady(new AuthenticationError('Channel is blocked; close its handles before subscribing again')));
    return handle;
  }
  detach(handle: ChannelSubscription): void {
    this.handles.delete(handle);
    if (!this.handles.size) { this.stop(); this.manager.remove(this.channelId, this); }
  }
  start(): void {
    if (this.status !== 'idle') return;
    void this.connect();
  }
  stop(): void {
    if (this.status === 'closed') return;
    this.invalidate();
    this.setStatus('closed');
    const error = new AbortError('Subscription is closed', { channelId: this.channelId });
    for (const handle of [...this.handles]) {
      handle.rejectReady(error);
      void handle.close();
    }
    this.#cache.clear(); this.#threads.clear(); this.#watermarks.clear();
  }
  block(error: HumChatError): void {
    if (this.status === 'closed' || this.status === 'blocked') return;
    this.invalidate();
    this.setStatus('blocked');
    this.notify('error', error);
    if (error instanceof AuthenticationError || error instanceof SessionRevokedError) this.notify('authRequired', { channelId: this.channelId, error });
    for (const handle of this.handles) handle.rejectReady(error);
  }
  notify<K extends keyof RealtimeEvents>(event: K, payload: RealtimeEvents[K]): void {
    for (const handle of this.handles) handle.notify(event, payload);
  }
  private async emit<K extends keyof RealtimeEvents>(event: K, payload: RealtimeEvents[K], generation: number, signal = this.#cycle.signal): Promise<void> {
    for (const handle of [...this.handles]) {
      if (!this.current(generation)) return;
      if (signal.aborted) throw aborted(signal);
      await handle.dispatch(event, payload, signal);
      if (signal.aborted) throw aborted(signal);
    }
  }
  private setStatus(state: ConnectionStatus, nextRetryAt?: number): void {
    this.status = state;
    this.notify('connection', { channelId: this.channelId, state, attempt: this.#attempt,
      ...(nextRetryAt === undefined ? {} : { nextRetryAt }) });
  }
  private current(generation: number): boolean {
    return generation === this.#generation && !this.#cycle.signal.aborted && this.status !== 'closed' && this.status !== 'blocked';
  }
  private invalidate(): void {
    this.#ready = false;
    this.#generation++;
    this.#cycle.abort(new AbortError('Connection generation ended'));
    clearTimeout(this.#retryTimer); clearInterval(this.#pingTimer); clearTimeout(this.#pongTimer);
    clearTimeout(this.#expiryTimer); clearTimeout(this.#stableTimer);
    this.#retryTimer = undefined; this.#pingTimer = undefined; this.#pongTimer = undefined;
    this.#expiryTimer = undefined; this.#stableTimer = undefined;
    const socket = this.#socket; this.#socket = undefined;
    if (socket) { socket.on('error', () => {}); socket.terminate(); }
    for (const item of this.#queue) this.release(item.bytes);
    this.#queue = [];
  }
  private async connect(): Promise<void> {
    if (!this.handles.size || this.status === 'closed' || this.status === 'blocked') return;
    this.#cycle = new AbortController();
    const generation = ++this.#generation;
    this.setStatus('connecting');
    let release: (() => void) | undefined;
    try {
      release = await this.manager.acquire(this.#cycle.signal);
      const sessionBudget = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
      let session: VerifiedSession;
      try { session = await this.manager.sessions.get(sessionBudget.signal); }
      finally { sessionBudget.dispose(); }
      if (!this.current(generation)) return;
      this.#session = session;
      try { await this.open(session, generation); }
      catch (error) {
        if (!(error instanceof AuthenticationError) || error.status !== 401 || !this.current(generation)) throw error;
        const refreshBudget = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
        try { session = await this.manager.sessions.unauthorized(session, refreshBudget.signal); }
        catch (error) { this.manager.counters.authRefreshFailures++; throw error; }
        finally { refreshBudget.dispose(); }
        this.#session = session;
        await this.open(session, generation);
      }
      if (!this.current(generation)) return;
      // Refresh may have completed through HTTP while this handshake was pending.
      const checkBudget = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
      let latest: VerifiedSession;
      try { latest = await this.manager.sessions.get(checkBudget.signal); } finally { checkBudget.dispose(); }
      if (!this.current(generation)) return;
      if (latest.token !== session.token) throw new NetworkError('Credential changed during handshake; reconnecting channel', { retryable: true });
      this.#session = latest;
      this.startLiveness(generation);
      await this.recover(this.#baseline ? 'reconnect' : 'initial', generation);
      if (!this.current(generation)) return;
      this.#baseline = true;
      this.setStatus('live');
      await this.drain(generation);
      if (!this.current(generation)) return;
      this.#ready = true;
      for (const handle of this.handles) handle.resolveReady();
      this.#stableTimer = setTimeout(() => { if (this.current(generation)) this.#attempt = 0; }, 30_000);
      if (this.#resyncAgain) { this.#resyncAgain = false; void this.resync(); }
    } catch (error) {
      if (this.current(generation)) this.fail(withIntent(error, { channelId: this.channelId }));
    } finally { release?.(); }
  }
  private open(session: VerifiedSession, generation: number): Promise<void> {
    const url = new URL('/ws/chat', this.manager.config.baseUrl);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    url.searchParams.set('channel_id', this.channelId);
    return new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(url, { headers: { Authorization: `Bearer ${session.token}` },
        followRedirects: false, handshakeTimeout: this.manager.config.timeout,
        maxPayload: this.manager.config.realtime.maxPayloadBytes, perMessageDeflate: false });
      this.#socket = socket;
      let opened = false;
      let rejected = false;
      const fail = (error: HumChatError) => { if (rejected) return; rejected = true; reject(error); socket.terminate(); };
      socket.on('unexpected-response', (_request, response) => {
        const status = response.statusCode ?? 0;
        const retryAfter = response.headers['retry-after'];
        const retryMilliseconds = typeof retryAfter === 'string' ? (/^\d+(\.\d+)?$/.test(retryAfter) ? Number(retryAfter) * 1_000 : Date.parse(retryAfter) - Date.now()) : NaN;
        const retry = { status, retryable: true, ...(Number.isFinite(retryMilliseconds) ? { retryAfterMs: Math.max(0, Math.min(2_147_483_647, retryMilliseconds)) } : {}) };
        response.destroy();
        fail(status === 401 ? new AuthenticationError('WebSocket authentication rejected', { status })
          : status === 403 ? new PermissionError('WebSocket channel permission denied', { status })
            : status === 404 ? new NotFoundError('WebSocket channel not found', { status })
              : status === 429 ? new RateLimitError('WebSocket rate limit exceeded', retry)
                : status === 408 ? new TimeoutError('WebSocket upgrade timed out', retry)
                  : [502, 503, 504].includes(status) ? new ServerError('WebSocket service temporarily unavailable', retry)
                    : new ProtocolError('WebSocket upgrade rejected', { status }));
      });
      socket.on('open', () => {
        if (!this.current(generation)) { socket.terminate(); return; }
        opened = true; resolve();
      });
      socket.on('message', (data, binary) => {
        if (!this.current(generation) || socket !== this.#socket) return;
        if (binary) { this.block(new ProtocolError('Unexpected binary chat frame', { channelId: this.channelId })); return; }
        try {
          const bytes = Buffer.isBuffer(data) ? data : Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data);
          const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
          const frame = parseFrame(raw, this.manager.config.orgId, this.channelId);
          if (frame.kind === 'sessionRevoked' || frame.kind === 'sessionExpired') {
            if (this.manager.endSession(frame, session)) this.fail(new NetworkError('Socket token expired; reconnecting with a fresh session', { retryable: true }));
            return;
          }
          if (frame.kind === 'resync') { void this.resync(); return; }
          this.enqueue(frame, bytes.byteLength, generation);
        } catch (error) {
          this.block(error instanceof HumChatError ? error : new ProtocolError('Invalid JSON chat frame', { channelId: this.channelId }));
        }
      });
      socket.on('pong', () => { if (this.current(generation)) { clearTimeout(this.#pongTimer); this.#pongTimer = undefined; } });
      socket.on('error', () => {
        if (!opened) fail(new NetworkError('WebSocket handshake failed', { retryable: true }));
        else if (this.current(generation) && socket === this.#socket) this.fail(new NetworkError('WebSocket transport failed', { retryable: true }));
      });
      socket.on('close', () => {
        if (!opened) fail(new NetworkError('WebSocket closed during handshake', { retryable: true }));
        else if (this.current(generation) && socket === this.#socket) this.fail(new NetworkError('WebSocket connection closed', { retryable: true }));
      });
    });
  }
  private fail(error: HumChatError): void {
    if (!error.retryable) { this.block(error); return; }
    this.notify('error', error);
    this.invalidate();
    const options = this.manager.config.realtime;
    const ceiling = Math.min(options.reconnectMaxDelayMs, options.reconnectInitialDelayMs * 2 ** Math.min(this.#attempt, 20));
    const delay = Math.max(1, Math.floor(Math.random() * ceiling), error.retryAfterMs ?? 0);
    this.#attempt++; this.manager.counters.reconnectCount++;
    this.setStatus('backoff', Date.now() + delay);
    this.#retryTimer = setTimeout(() => { this.#retryTimer = undefined; void this.connect(); }, delay);
  }
  rotate(session: VerifiedSession): void {
    if (this.status !== 'live' && this.status !== 'syncing') return;
    if (!this.#session || this.#session.token === session.token) return;
    this.fail(new NetworkError('Credential refreshed; reconnecting channel', { retryable: true }));
  }
  private startLiveness(generation: number): void {
    const options = this.manager.config.realtime;
    this.#pingTimer = setInterval(() => {
      if (!this.current(generation) || !this.socketOpen || this.#pongTimer) return;
      this.#socket!.ping(undefined, undefined, error => { if (error && this.current(generation)) this.fail(new NetworkError('WebSocket ping failed', { retryable: true })); });
      this.#pongTimer = setTimeout(() => { if (this.current(generation)) this.fail(new TimeoutError('WebSocket pong deadline exceeded', { retryable: true })); }, options.pongTimeoutMs);
    }, options.pingIntervalMs);
    this.scheduleRefresh(generation);
  }
  private scheduleRefresh(generation: number): void {
    if (!this.current(generation) || this.#session?.refreshAt === null || this.#session?.refreshAt === undefined) return;
    const delay = Math.min(60_000, Math.max(1, this.#session.refreshAt * 1_000 - Date.now()));
    this.#expiryTimer = setTimeout(() => { void (async () => {
      try {
        const lifetime = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
        let session: VerifiedSession;
        try { session = await this.manager.sessions.get(lifetime.signal); } finally { lifetime.dispose(); }
        if (!this.current(generation)) return;
        if (session.token !== this.#session?.token) { this.rotate(session); return; }
        this.#session = session;
        this.scheduleRefresh(generation);
      } catch (error) {
        if (this.current(generation)) { this.manager.counters.authRefreshFailures++; this.fail(withIntent(error, { channelId: this.channelId })); }
      }
    })(); }, delay);
  }
  private enqueue(frame: ParsedFrame, bytes: number, generation: number): void {
    const total = this.manager.counters, limit = this.manager.config.realtime;
    if (total.queuedEvents + 1 > limit.maxBufferedEvents || total.queuedBytes + bytes > limit.maxBufferedBytes) {
      if (ephemeral(frame)) { total.droppedEphemeralCount++; return; }
      this.notify('recoveryIncomplete', { channelId: this.channelId, mode: 'bestEffort', reason: 'overflow', pages: 0, states: 0, complete: false, limitations: ['Incoming event buffer exceeded its budget; delivery is blocked'] });
      this.block(new BackpressureError('Incoming event buffer exceeded its budget', { channelId: this.channelId }));
      return;
    }
    total.queuedEvents++; total.queuedBytes += bytes;
    this.#queue.push({ frame, bytes });
    if (this.status === 'live') void this.drain(generation);
  }
  private release(bytes: number): void { this.manager.counters.queuedEvents--; this.manager.counters.queuedBytes -= bytes; }
  private drain(generation: number): Promise<void> {
    if (this.#draining) return this.#draining;
    const work = (async () => {
      while (this.current(generation) && this.status === 'live' && this.#queue.length) {
        const item = this.#queue.shift()!;
        try { await this.deliver(item.frame, generation); }
        catch (error) { if (this.current(generation)) this.block(withIntent(error, { channelId: this.channelId })); }
        finally { this.release(item.bytes); }
      }
    })();
    this.#draining = work;
    void work.finally(() => { if (this.#draining === work) this.#draining = undefined; });
    return work;
  }
  private remember(value: Position & { threadRootId: string | null }): void {
    const stream = value.threadRootId ?? '';
    const old = this.#watermarks.get(stream);
    if (old === undefined || old === null || newer(value, old)) this.#watermarks.set(stream, { createdAt: value.createdAt, id: value.id });
    const root = value.threadRootId ?? value.id;
    this.#threads.delete(root); this.#threads.set(root, true);
    while (this.#threads.size > this.manager.config.recovery.maxThreads) {
      const oldest = this.#threads.keys().next().value!;
      this.#threads.delete(oldest); this.#watermarks.delete(oldest);
    }
  }
  private async deliver(frame: ParsedFrame, generation: number): Promise<void> {
    switch (frame.kind) {
      case 'message':
        this.remember(frame.message);
        if (this.#cache.observeMessage(frame.message)) await this.emit('message', { message: frame.message, source: 'live', replayed: false }, generation);
        break;
      case 'state': {
        this.remember(frame.state);
        const merged = this.#cache.merge(frame.state);
        if ((frame.event === 'reactionUpdated' && merged.reactionsChanged) || (frame.event !== 'reactionUpdated' && merged.contentChanged)) await this.emit(frame.event, { state: merged.state, source: 'live' }, generation);
        break;
      }
      case 'legacyMutation': {
        const state = await this.manager.messages.getState({ channelId: this.channelId, messageId: frame.messageId }, { signal: this.#cycle.signal });
        if (!this.current(generation)) return;
        const merged = this.#cache.merge(state);
        // A hydrated current snapshot cannot certify which historical mutation it represents.
        if (merged.changed) await this.emit('stateReconciled', { state: merged.state, source: 'recovery' }, generation);
        break;
      }
      case 'typing': await this.emit('typing', frame.payload, generation); break;
      case 'presence': await this.emit('presence', frame.payload, generation); break;
      case 'receipt': await this.emit('receipt', frame.payload, generation); break;
      case 'callInvite': await this.emit('callInvite', frame.payload, generation); break;
      case 'unknown': await this.emit('unknownEvent', { channelId: this.channelId, type: frame.type }, generation); break;
    }
  }
  async sendSignal(type: 'typing' | 'read', through?: number): Promise<void> {
    if (this.status !== 'live' || !this.socketOpen) throw new NetworkError('Channel is not live', { channelId: this.channelId });
    if (type === 'read' && (typeof through !== 'number' || !Number.isSafeInteger(through) || through < 0)) throw new ValidationError('through must be non-negative Unix seconds');
    if (this.#socket!.bufferedAmount > this.manager.config.realtime.maxBufferedBytes) throw new BackpressureError('Outgoing signal buffer exceeded its budget', { channelId: this.channelId });
    const body = JSON.stringify({ type, channel_id: this.channelId, ...(type === 'read' ? { ts: through } : {}) });
    const lifetime = budget(this.manager.config.timeout, [this.#cycle.signal]);
    try {
      await raceSignal(new Promise<void>((resolve, reject) => this.#socket!.send(body, error => error ? reject(new NetworkError('WebSocket signal failed')) : resolve())), lifetime.signal);
    } finally { lifetime.dispose(); }
  }
  /** Schedule recovery. Observe recoveryComplete for completion, including from inside handlers. */
  async resync(): Promise<void> {
    if (this.status === 'closed') throw new AbortError('Subscription is closed');
    if (this.status === 'blocked') throw new ProtocolError('Channel is blocked; create a new subscription');
    this.notify('resync', { channelId: this.channelId, reason: 'requested' });
    if (this.status !== 'live' || this.#recovering) { this.#resyncAgain = true; return; }
    const generation = this.#generation;
    this.#ready = false;
    this.setStatus('syncing');
    const work = (async () => {
      await this.#draining;
      if (!this.current(generation)) return;
      do {
        this.#resyncAgain = false;
        await this.recover('resync', generation);
      } while (this.current(generation) && this.#resyncAgain);
      if (this.current(generation)) {
        this.setStatus('live');
        await this.drain(generation);
        if (this.current(generation)) {
          this.#ready = true;
          for (const handle of this.handles) handle.resolveReady();
        }
      }
    })();
    this.#recovering = work;
    void work.catch(error => { if (this.current(generation)) this.fail(withIntent(error, { channelId: this.channelId })); })
      .finally(() => {
        if (this.#recovering === work) this.#recovering = undefined;
        // A resync can arrive from a buffered handler after the scan loop ended.
        if (this.current(generation) && this.#resyncAgain) { this.#resyncAgain = false; void this.resync(); }
      });
  }
  private async recover(reason: string, generation: number): Promise<void> {
    if (!this.current(generation)) return;
    const lifetime = budget(this.manager.config.recovery.timeoutMs, [this.#cycle.signal]);
    try { await this.recoverWithin(reason, generation, lifetime.signal); }
    finally { lifetime.dispose(); }
  }
  private async recoverWithin(reason: string, generation: number, signal: AbortSignal): Promise<void> {
    if (!this.current(generation)) return;
    if (signal.aborted) throw aborted(signal);
    this.setStatus('syncing');
    this.manager.counters.recoveryCount++;
    const coverage: RecoveryCoverage = { channelId: this.channelId, mode: 'bestEffort', reason, pages: 0, states: 0, complete: false };
    const limitations = new Set(['Current server history is not a durable event log', 'Replies in unknown or unscanned threads and older mutations may be missing']);
    const seen = new Set<string>();
    let bytes = 0;
    const scan = async (root: string | null) => {
      const stream = root ?? '';
      const boundary = this.#watermarks.get(stream);
      const pages: MessageState[][] = [];
      let cursor: string | undefined;
      for (let count = 0; count < this.manager.config.recovery.maxPages; count++) {
        const page = await this.manager.messages.history({ channelId: this.channelId, ...(root === null ? {} : { threadRootId: root }), ...(cursor === undefined ? {} : { cursor }) }, { signal });
        if (!this.current(generation)) return;
        coverage.pages++;
        bytes += Buffer.byteLength(JSON.stringify(page));
        if (bytes > this.manager.config.recovery.maxBytes) { limitations.add('Recovery byte budget reached'); break; }
        pages.push(page.messages);
        if (page.nextCursor === null) break;
        const oldest = page.messages[0];
        if (boundary && oldest && !newer(oldest, boundary)) break;
        cursor = page.nextCursor;
        if (count + 1 === this.manager.config.recovery.maxPages) limitations.add('Recovery page budget reached');
      }
      if (!this.#watermarks.has(stream)) this.#watermarks.set(stream, null);
      for (const state of pages.reverse().flat()) {
        if (!this.current(generation)) return;
        seen.add(state.id); coverage.states++;
        const prior = this.#cache.get(state.id);
        const recoverCreation = this.#baseline && boundary !== undefined && !prior && !state.deleted && state.revision === 1 && (boundary === null || newer(state, boundary));
        if (recoverCreation) {
          const message = messageFromState(state);
          if (this.#cache.observeMessage(message)) await this.emit('message', { message, source: 'recovery', replayed: true }, generation, signal);
        }
        const merged = this.#cache.merge(state);
        this.remember(state);
        if (merged.changed) await this.emit('stateReconciled', { state: merged.state, source: 'recovery' }, generation, signal);
      }
    };
    await scan(null);
    for (const root of [...this.#threads.keys()].reverse()) {
      if (!this.current(generation) || bytes > this.manager.config.recovery.maxBytes) break;
      await scan(root);
    }
    if (this.#baseline && this.current(generation)) {
      const targets = this.#cache.ids().filter(id => !seen.has(id)).reverse();
      if (targets.length > this.manager.config.recovery.maxStateRefresh) limitations.add('Known-state refresh budget reached');
      for (const id of targets.slice(0, this.manager.config.recovery.maxStateRefresh)) {
        if (!this.current(generation) || bytes > this.manager.config.recovery.maxBytes) break;
        try {
          const state = await this.manager.messages.getState({ channelId: this.channelId, messageId: id }, { signal });
          if (!this.current(generation)) return;
          bytes += Buffer.byteLength(JSON.stringify(state));
          if (bytes > this.manager.config.recovery.maxBytes) { limitations.add('Recovery byte budget reached'); break; }
          coverage.states++;
          const merged = this.#cache.merge(state);
          if (merged.changed) await this.emit('stateReconciled', { state: merged.state, source: 'recovery' }, generation, signal);
        } catch (error) {
          if (error instanceof NotFoundError) limitations.add('A previously observed message is no longer readable');
          else throw error;
        }
      }
    }
    if (!this.current(generation)) return;
    await this.emit('recoveryIncomplete', { ...coverage, limitations: [...limitations] }, generation, signal);
    await this.emit('recoveryComplete', coverage, generation, signal);
  }
}
