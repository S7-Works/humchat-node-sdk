import WebSocket from 'ws';
import { AbortError, AuthenticationError, BackpressureError, ConflictError, HumChatError, NetworkError,
  NotFoundError, PermissionError, ProtocolError, RateLimitError, RecoveryGapError, ServerError,
  SessionRevokedError, TimeoutError, UnsupportedFeatureError, ValidationError } from '../errors.js';
import { aborted, budget, raceSignal, withIntent } from '../http/transport.js';
import type { VerifiedSession } from '../auth/session.js';
import { PersistenceError, type PersistenceScope } from '../persistence/types.js';
import type { RealtimeClient } from './manager.js';
import { ChannelSubscription, type SubscriptionOwner } from './subscription.js';
import type { ConnectionStatus, RealtimeEvents } from './types.js';
import type { ConsumerOptions, DurableConsumer, DurableEvent, SubscribeOptions } from './durable-types.js';
import { cursorPosition, parseEvent, parseGap, parseReady, sameStream, type ReadyFrame } from './durable-protocol.js';
import { parseFrame, messageFromState } from './protocol.js';
import type { MultiplexLease } from './multiplex-transport.js';

/** Ordered log consumption. WS watermarks wake HTTP replay; frames never advance an acknowledgement. */
export class DurableChannelWorker implements SubscriptionOwner {
  readonly handles = new Set<ChannelSubscription>();
  status: ConnectionStatus = 'idle';
  #socket: WebSocket | undefined;
  #lease: MultiplexLease | undefined;
  #session: VerifiedSession | undefined;
  #cycle = new AbortController();
  #generation = 0;
  #attempt = 0;
  #ready = false;
  #cursor: string | undefined;
  #target: string | undefined;
  #pump: Promise<void> | undefined;
  #storeWork: Promise<unknown> | undefined;
  #consumerWork: Promise<void> | undefined;
  #deliveryEnded = new AbortController();
  #refreshRequested = false;
  #resyncRequested = false;
  #consumer: { handle: ChannelSubscription; handler: DurableConsumer; options: ConsumerOptions; scope?: PersistenceScope } | undefined;
  #retryTimer: ReturnType<typeof setTimeout> | undefined;
  #heartbeat: ReturnType<typeof setInterval> | undefined;
  #pong: ReturnType<typeof setTimeout> | undefined;
  #refresh: ReturnType<typeof setTimeout> | undefined;
  #stable: ReturnType<typeof setTimeout> | undefined;
  #poll: ReturnType<typeof setInterval> | undefined;
  constructor(readonly channelId: string, private readonly manager: RealtimeClient, private readonly options: SubscribeOptions) {
    this.#lease = manager.multiplex?.create(channelId);
  }
  get socketOpen(): boolean { return this.#lease ? this.#lease.socketOpen : this.#socket?.readyState === WebSocket.OPEN; }
  attach(): ChannelSubscription {
    const handle = new ChannelSubscription(this.channelId, this, this.manager.config.realtime.handlerTimeoutMs);
    this.handles.add(handle);
    if (this.#ready && this.status === 'live') queueMicrotask(() => { if (this.#ready) handle.resolveReady(); });
    if (this.status === 'blocked') queueMicrotask(() => handle.rejectReady(new ProtocolError('Channel is blocked; close its handles before subscribing again')));
    return handle;
  }
  consume(handle: ChannelSubscription, handler: DurableConsumer, options: ConsumerOptions): void {
    if (this.status !== 'idle' || this.#consumer) throw new ValidationError('Register one durable consumer per channel before startup');
    if (typeof handler !== 'function' || !options || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(options.consumerId) ||
      typeof options.checkpointStore?.load !== 'function' || typeof options.checkpointStore.compareAndSet !== 'function') throw new ValidationError('Invalid durable consumer options');
    this.#consumer = { handle, handler, options: { ...options } };
  }
  detach(handle: ChannelSubscription): void {
    this.handles.delete(handle);
    if (!this.handles.size) { this.stop(); this.manager.remove(this.channelId, this); }
    else if (this.#consumer?.handle === handle && this.status !== 'closed') this.block(new AbortError('Acknowledged consumer closed; replace the channel subscription'));
  }
  start(): void { if (this.status === 'idle') void this.connect(); }
  stop(): void {
    if (this.status === 'closed') return;
    this.invalidate(); this.setStatus('closed');
    this.#lease?.close();
    this.#deliveryEnded.abort(new AbortError('Subscription is closed'));
    for (const handle of [...this.handles]) { handle.rejectReady(new AbortError('Subscription is closed')); void handle.close(); }
  }
  block(error: HumChatError): void {
    if (this.status === 'closed' || this.status === 'blocked') return;
    this.invalidate(); this.setStatus('blocked'); this.#deliveryEnded.abort(error); this.notify('error', error);
    this.#lease?.close();
    if (error instanceof AuthenticationError || error instanceof SessionRevokedError) this.notify('authRequired', { channelId: this.channelId, error });
    for (const handle of this.handles) handle.rejectReady(error);
  }
  notify<K extends keyof RealtimeEvents>(event: K, value: RealtimeEvents[K]): void { for (const h of this.handles) h.notify(event, value); }
  private async emit<K extends keyof RealtimeEvents>(event: K, value: RealtimeEvents[K], generation: number): Promise<void> {
    for (const h of [...this.handles]) {
      if (!this.current(generation)) throw aborted(this.#cycle.signal);
      await h.dispatch(event, value, this.#cycle.signal);
    }
    if (!this.current(generation)) throw new AbortError('Event delivery generation ended');
  }
  private setStatus(state: ConnectionStatus, nextRetryAt?: number): void {
    this.status = state;
    this.notify('connection', { channelId: this.channelId, state, attempt: this.#attempt, ...(nextRetryAt === undefined ? {} : { nextRetryAt }) });
  }
  private current(generation: number): boolean {
    return generation === this.#generation && !this.#cycle.signal.aborted && this.status !== 'blocked' && this.status !== 'closed';
  }
  private invalidate(): void {
    this.#generation++; this.#ready = false;
    this.#cycle.abort(new AbortError('Connection generation ended'));
    clearTimeout(this.#retryTimer); clearInterval(this.#heartbeat); clearTimeout(this.#pong);
    clearTimeout(this.#refresh); clearTimeout(this.#stable); clearInterval(this.#poll);
    this.#retryTimer = undefined; this.#heartbeat = undefined; this.#pong = undefined;
    this.#refresh = undefined; this.#stable = undefined; this.#poll = undefined;
    this.#lease?.cancelAttempt();
    const socket = this.#socket; this.#socket = undefined;
    if (socket) { socket.on('error', () => {}); socket.terminate(); }
  }
  private fail(error: HumChatError): void {
    if (!error.retryable) { this.block(error); return; }
    this.notify('error', error); this.invalidate();
    const config = this.manager.config.realtime;
    const ceiling = Math.min(config.reconnectMaxDelayMs, config.reconnectInitialDelayMs * 2 ** Math.min(this.#attempt, 20));
    const delay = Math.max(1, Math.floor(Math.random() * ceiling), error.retryAfterMs ?? 0);
    this.#attempt++; if (!this.#lease) this.manager.counters.reconnectCount++;
    this.setStatus('backoff', Date.now() + delay);
    this.#retryTimer = setTimeout(() => { void this.connect(); }, delay);
  }
  rotate(session: VerifiedSession): void {
    if (this.#lease) return; // The shared transport rotates every logical lease together.
    if ((this.status === 'live' || this.status === 'syncing') && this.#session?.token !== session.token) this.fail(new NetworkError('Credential changed; reconnecting event stream', { retryable: true }));
  }
  private async connect(): Promise<void> {
    if (!this.handles.size || this.status === 'closed' || this.status === 'blocked') return;
    this.#cycle = new AbortController(); const generation = ++this.#generation;
    this.setStatus('connecting');
    let release: (() => void) | undefined;
    try {
      // Cancellation cannot stop an application promise. Never overlap it with replay,
      // and let an in-flight checkpoint write settle before another generation loads it.
      // A prior generation's replay is expected to reject when its socket disappears.
      // Wait for settlement, but do not turn that cancellation into a permanent error
      // for the replacement generation. Consumer/store barriers below retain failures.
      await this.#pump?.catch(() => {});
      await this.#consumerWork;
      await this.#storeWork;
      if (!this.current(generation)) return;
      release = await this.manager.acquire(this.#cycle.signal);
      const lifetime = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
      let session: VerifiedSession;
      try { session = await this.manager.sessions.get(lifetime.signal); } finally { lifetime.dispose(); }
      if (!this.current(generation)) return;
      if (!session.capabilities.messaging.durableReplay || !session.capabilities.messaging.eventProtocolVersions.includes(1)) throw new UnsupportedFeatureError('Server lacks durable event protocol v1; choose bestEffort explicitly for an older server');
      this.#session = session;
      let ready: ReadyFrame;
      try { ready = await this.open(session, generation); }
      catch (error) {
        if (this.#lease) throw error; // Shared transport already performed its single 401 refresh.
        if (!(error instanceof AuthenticationError) || error.status !== 401 || !this.current(generation)) throw error;
        const refresh = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
        try { session = await this.manager.sessions.unauthorized(session, refresh.signal); }
        catch (error) { this.manager.counters.authRefreshFailures++; throw error; }
        finally { refresh.dispose(); }
        this.#session = session; ready = await this.open(session, generation);
      }
      if (!this.current(generation)) return;
      const latestBudget = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
      let latest: VerifiedSession;
      try { latest = await this.manager.sessions.get(latestBudget.signal); } finally { latestBudget.dispose(); }
      if (latest.token !== session.token && !this.#lease) throw new NetworkError('Credential changed during handshake', { retryable: true });
      // The hub may refresh a rejected upgrade once for all waiting channels. Its
      // generation guards already invalidate leases if that new credential rotates.
      if (this.#lease) { session = latest; this.#session = latest; }
      if (!this.current(generation)) return;
      // Admission bounds handshakes, not application work or an arbitrarily large
      // durable backlog. Shared delivery budgets still bound replay across channels.
      release(); release = undefined;
      this.setStatus('syncing'); this.startTimers(generation);
      const resumed = this.#cursor !== undefined || this.#consumer !== undefined;
      this.#target = this.higher(this.#target, ready.cursor);
      const consumer = this.#consumer;
      if (consumer) {
        consumer.scope = { serverOrigin: this.manager.config.baseUrl, orgId: session.identity.orgId, actorId: session.identity.actorId,
          consumerId: consumer.options.consumerId, channelId: this.channelId };
        this.#cursor = (await this.checkpoint(() => consumer.options.checkpointStore.load(consumer.scope!))) ?? undefined;
      }
      if (!this.current(generation)) return;
      if (this.#cursor === undefined) {
        const start = this.options.start ?? 'now';
        this.#cursor = typeof start === 'object' ? start.cursor : start === 'earliest' ? ready.earliestCursor : ready.cursor;
        this.checkBoundary(this.#cursor, ready);
        if (consumer && !await this.checkpoint(() => consumer.options.checkpointStore.compareAndSet(consumer.scope!, null, this.#cursor!))) throw new ConflictError('Checkpoint changed concurrently; only one owner may consume this stream');
      } else this.checkBoundary(this.#cursor, ready);
      if (!this.current(generation)) return;
      this.manager.counters.recoveryCount++;
      await this.startPump(generation, resumed ? 'reconnect' : 'initial', ready.cursor);
      if (!this.current(generation)) return;
      this.#ready = true; this.setStatus('live');
      for (const handle of this.handles) handle.resolveReady();
      if (!this.#lease) this.#stable = setTimeout(() => { if (this.current(generation)) this.#attempt = 0; }, 30_000);
      this.wake(generation);
    } catch (error) { if (this.current(generation)) this.fail(withIntent(error, { channelId: this.channelId })); }
    finally { release?.(); }
  }
  private checkBoundary(cursor: string, ready: ReadyFrame): void {
    const position = cursorPosition(cursor, this.manager.config.orgId, this.channelId);
    const floor = cursorPosition(ready.earliestCursor, this.manager.config.orgId, this.channelId);
    const head = cursorPosition(ready.cursor, this.manager.config.orgId, this.channelId);
    if (position.epoch !== head.epoch || position.sequence < floor.sequence) throw new RecoveryGapError('Checkpoint has expired or its stream was replaced', {
      channelId: this.channelId, earliestCursor: ready.earliestCursor, earliestSequence: floor.sequence.toString() });
    if (position.sequence > head.sequence) throw new ProtocolError('Checkpoint is ahead of the committed stream');
  }
  private async checkpoint<T>(operation: () => Promise<T>): Promise<T> {
    const work = Promise.resolve().then(operation);
    this.#storeWork = work;
    try { return await work; }
    catch { throw new PersistenceError('Checkpoint storage failed; event acknowledgement stopped'); }
    finally { if (this.#storeWork === work) this.#storeWork = undefined; }
  }
  private higher(current: string | undefined, candidate: string): string {
    if (!current) return candidate;
    const a = cursorPosition(current, this.manager.config.orgId, this.channelId), b = cursorPosition(candidate, this.manager.config.orgId, this.channelId);
    sameStream(a, b); return b.sequence > a.sequence ? candidate : current;
  }
  private open(session: VerifiedSession, generation: number): Promise<ReadyFrame> {
    if (this.#lease) {
      this.#target = undefined;
      return this.#lease.open(this.#cycle.signal, raw => {
        if (!this.current(generation)) return;
        try { this.receive(raw, generation); }
        catch (error) { this.fail(error instanceof HumChatError ? error : new ProtocolError('Invalid multiplex channel frame')); }
      }, error => {
        if (!this.current(generation)) return;
        if (!error.retryable) { this.block(error); return; }
        this.notify('error', error); this.invalidate(); this.setStatus('backoff');
        // Physical reconnect and delay belong to the shared transport. Logical workers
        // only wait for it, retaining their application/store settlement barriers.
        queueMicrotask(() => { void this.connect(); });
      });
    }
    const url = new URL('/ws/chat', this.manager.config.baseUrl);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'; url.searchParams.set('channel_id', this.channelId);
    this.#target = undefined;
    return new Promise((resolve, reject) => {
      let gotReady = false, settled = false, failed = false;
      const timer = setTimeout(() => fail(new TimeoutError('Durable ready handshake timed out', { retryable: true })), this.manager.config.timeout);
      const socket = new WebSocket(url, { headers: { Authorization: `Bearer ${session.token}`, 'X-HumChat-Event-Protocol': 'v1' },
        followRedirects: false, handshakeTimeout: this.manager.config.timeout, maxPayload: this.manager.config.realtime.maxPayloadBytes, perMessageDeflate: false });
      this.#socket = socket;
      const fail = (error: HumChatError) => {
        // terminate() can emit error/close after an HTTP rejection. Preserve the first
        // handshake error so a 403 cannot become a retryable network failure.
        if (failed) return;
        failed = true; clearTimeout(timer);
        if (!settled) { settled = true; reject(error); }
        else if (gotReady && this.current(generation) && socket === this.#socket) this.fail(error);
        socket.terminate();
      };
      const cancel = () => fail(new AbortError('Durable handshake cancelled'));
      this.#cycle.signal.addEventListener('abort', cancel, { once: true });
      const cleanup = () => { clearTimeout(timer); this.#cycle.signal.removeEventListener('abort', cancel); };
      socket.on('unexpected-response', (_request, response) => {
        const status = response.statusCode ?? 0, rawDelay = response.headers['retry-after'];
        const milliseconds = typeof rawDelay === 'string' ? (/^\d+(\.\d+)?$/.test(rawDelay) ? Number(rawDelay) * 1000 : Date.parse(rawDelay) - Date.now()) : NaN;
        const retry = { status, retryable: true, ...(Number.isFinite(milliseconds) ? { retryAfterMs: Math.max(0, Math.min(2_147_483_647, milliseconds)) } : {}) };
        response.destroy();
        fail(status === 401 ? new AuthenticationError('WebSocket authentication rejected', { status })
          : status === 403 ? new PermissionError('Channel permission denied', { status })
            : status === 404 ? new NotFoundError('Channel not found', { status })
              : status === 429 ? new RateLimitError('WebSocket rate limit exceeded', retry)
                : status === 408 ? new TimeoutError('WebSocket upgrade timed out', retry)
                  : [502, 503, 504].includes(status) ? new ServerError('WebSocket temporarily unavailable', retry)
                    : new ProtocolError('Durable WebSocket negotiation rejected', { status }));
      });
      socket.on('message', (data, binary) => {
        if (!this.current(generation) || socket !== this.#socket) return;
        try {
          if (binary) throw new ProtocolError('Unexpected binary event frame');
          const bytes = Buffer.isBuffer(data) ? data : Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data);
          const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
          const kind = (raw as { type?: unknown })?.type;
          if (kind === 'session_revoked') { const frame = parseFrame(raw, this.manager.config.orgId, this.channelId); if (frame.kind === 'sessionRevoked') this.manager.revoke(frame.reason); return; }
          if (!gotReady) {
            const ready = parseReady(raw, this.manager.config.orgId, this.channelId);
            gotReady = true; settled = true; this.#target = ready.cursor; cleanup(); resolve(ready); return;
          }
          this.receive(raw, generation);
        } catch (error) { fail(error instanceof HumChatError ? error : new ProtocolError('Invalid event JSON')); }
      });
      socket.on('pong', () => { if (this.current(generation)) { clearTimeout(this.#pong); this.#pong = undefined; } });
      socket.on('error', () => fail(new NetworkError('Durable WebSocket transport failed', { retryable: true })));
      socket.on('close', () => { cleanup(); if (this.current(generation) && socket === this.#socket) fail(new NetworkError('Durable WebSocket closed', { retryable: true })); });
    });
  }
  private receive(raw: unknown, generation: number): void {
    const kind = (raw as { type?: unknown })?.type;
    if (kind === 'event') {
      const event = parseEvent(raw, this.manager.config.orgId, this.channelId);
      this.#target = this.higher(this.#target, event.cursor); this.wake(generation); return;
    }
    if (kind === 'watermark' && this.#lease) {
      const cursor = (raw as { cursor: string }).cursor;
      this.#target = this.higher(this.#target, cursor); this.wake(generation); return;
    }
    if (kind === 'ready') throw new ProtocolError('Duplicate ready frame');
    if (kind === 'event_gap') throw parseGap((raw as { gap?: unknown }).gap, this.manager.config.orgId, this.channelId);
    if (kind === 'gap' || (raw as { code?: unknown })?.code === 'EVENT_GAP') throw parseGap(raw, this.manager.config.orgId, this.channelId);
    const frame = parseFrame(raw, this.manager.config.orgId, this.channelId);
    if (frame.kind === 'resync') { this.#refreshRequested = true; this.wake(generation); }
    else if (frame.kind === 'typing' || frame.kind === 'presence' || frame.kind === 'receipt' || frame.kind === 'callInvite') this.notify(frame.kind, frame.payload as never);
    else if (frame.kind === 'unknown') this.notify('unknownEvent', { channelId: this.channelId, type: frame.type });
    else throw new ProtocolError('Unversioned durable message frame');
  }
  private startTimers(generation: number): void {
    if (this.#lease) return; // Heartbeat, expiry and missed-hint polling are shared.
    const options = this.manager.config.realtime;
    this.#heartbeat = setInterval(() => {
      if (!this.current(generation) || !this.socketOpen || this.#pong) return;
      this.#socket!.ping(undefined, undefined, error => { if (error && this.current(generation)) this.fail(new NetworkError('Ping failed', { retryable: true })); });
      this.#pong = setTimeout(() => { if (this.current(generation)) this.fail(new TimeoutError('Pong deadline exceeded', { retryable: true })); }, options.pongTimeoutMs);
    }, options.pingIntervalMs);
    // A second source of progress detection protects against a missed final live hint.
    this.#poll = setInterval(() => { if (this.current(generation)) { this.#refreshRequested = true; this.wake(generation); } }, 5000);
    this.scheduleRefresh(generation);
  }
  private scheduleRefresh(generation: number): void {
    if (!this.current(generation) || this.#session?.refreshAt == null) return;
    this.#refresh = setTimeout(() => { void (async () => {
      const lifetime = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
      try {
        const session = await this.manager.sessions.get(lifetime.signal);
        if (!this.current(generation)) return;
        if (session.token !== this.#session?.token) this.rotate(session);
        else { this.#session = session; this.scheduleRefresh(generation); }
      } catch (error) { if (this.current(generation)) { this.manager.counters.authRefreshFailures++; this.block(withIntent(error, { channelId: this.channelId })); } }
      finally { lifetime.dispose(); }
    })(); }, Math.min(60_000, Math.max(1, this.#session.refreshAt * 1000 - Date.now())));
  }
  private wake(generation: number): void {
    if (this.status !== 'live' || this.#pump || !this.current(generation)) return;
    const reason = this.#resyncRequested ? 'resync' : 'live';
    this.#resyncRequested = false;
    if (reason === 'resync') this.manager.counters.recoveryCount++;
    void this.startPump(generation, reason).catch(error => { if (this.current(generation)) this.fail(withIntent(error, { channelId: this.channelId })); });
  }
  private startPump(generation: number, reason: string, through?: string): Promise<void> {
    if (this.#pump) return this.#pump;
    const work = this.pump(generation, reason, through);
    this.#pump = work;
    void work.then(() => {
      if (this.#pump === work) this.#pump = undefined;
      if (this.current(generation) && this.status === 'live' && (this.#refreshRequested || (this.#target && this.#cursor !== this.#target))) this.wake(generation);
    }, () => { if (this.#pump === work) this.#pump = undefined; });
    return work;
  }
  private async pump(generation: number, reason: string, through?: string): Promise<void> {
    let pages = 0, states = 0;
    // The attempt's captured watermark is fixed; later live hints wait for the next pass.
    let target = through ?? (this.#refreshRequested ? undefined : this.#target);
    this.#refreshRequested = false;
    while (this.current(generation) && this.#cursor !== undefined && (target === undefined || this.#cursor !== target)) {
      const page = await this.manager.events.sync({ channelId: this.channelId, after: this.#cursor,
        ...(target === undefined ? {} : { through: target }), limit: Math.min(200, this.manager.config.realtime.maxBufferedEvents) }, { signal: this.#cycle.signal });
      if (!this.current(generation)) return;
      pages++; target = page.throughCursor; this.#target = this.higher(this.#target, target);
      const bytes = Buffer.byteLength(JSON.stringify(page));
      const counters = this.manager.counters, options = this.manager.config.realtime;
      if (bytes > this.manager.config.recovery.maxBytes || bytes > options.maxBufferedBytes || page.events.length > options.maxBufferedEvents) {
        throw new BackpressureError('Durable replay page exceeds available delivery budget; checkpoint was not advanced', { channelId: this.channelId });
      }
      if (counters.queuedBytes + bytes > options.maxBufferedBytes || counters.queuedEvents + page.events.length > options.maxBufferedEvents) {
        throw new BackpressureError('Shared delivery budget is busy; replay will resume from the checkpoint', { channelId: this.channelId, retryable: true });
      }
      counters.queuedBytes += bytes; counters.queuedEvents += page.events.length;
      try {
        for (const event of page.events) {
          if (!this.current(generation)) return;
          await this.deliver(event, reason === 'live' ? 'live' : 'recovery', generation);
          states++;
        }
      } finally { counters.queuedBytes -= bytes; counters.queuedEvents -= page.events.length; }
      if (!page.nextCursor) break;
    }
    if (this.current(generation) && reason !== 'live' && this.#cursor !== undefined) await this.emit('recoveryComplete', {
      channelId: this.channelId, mode: 'durable', reason, pages, states, complete: true, cursor: this.#cursor,
    }, generation);
  }
  private async deliver(event: DurableEvent, source: 'live' | 'recovery', generation: number): Promise<void> {
    const prior = this.#cursor!;
    // Replay remains in committed channel order. No snapshot cache suppresses a historical mutation.
    await this.emit('event', event, generation);
    if (event.kind === 'message') await this.emit('message', { message: messageFromState(event.state), source, replayed: source === 'recovery' }, generation);
    else await this.emit(event.kind === 'edit' ? 'messageUpdated' : event.kind === 'delete' ? 'messageDeleted' : 'reactionUpdated', { state: event.state, source }, generation);
    const consumer = this.#consumer;
    if (consumer) {
      // Keep the original deadline alive after a disconnect: user work may ignore its
      // cancellation signal. A new generation waits for settlement or blocks at that
      // deadline instead of running the same consumer concurrently.
      const lifetime = budget(this.manager.config.realtime.handlerTimeoutMs, [this.#deliveryEnded.signal]);
      const signal = AbortSignal.any([lifetime.signal, this.#cycle.signal]);
      const work = Promise.resolve().then(() => {
        if (signal.aborted) throw aborted(signal);
        return consumer.handler(structuredClone(event), { signal, source });
      });
      const barrier = raceSignal(work.then(() => {}, () => {}), lifetime.signal).catch(() => {
        const failure = new ProtocolError('Acknowledged consumer did not settle before its deadline; checkpoint not advanced', { channelId: this.channelId });
        if (this.status !== 'blocked' && this.status !== 'closed') {
          this.notify('handlerError', { event: 'consume', error: failure });
          this.block(failure);
        }
        throw failure;
      });
      this.#consumerWork = barrier;
      const settled = () => { lifetime.dispose(); if (this.#consumerWork === barrier) this.#consumerWork = undefined; };
      void barrier.then(settled, settled);
      try {
        await raceSignal(work, signal);
      } catch (error) {
        if (!this.current(generation)) throw error;
        const failure = new ProtocolError('Acknowledged consumer failed or exceeded its deadline; checkpoint not advanced', { channelId: this.channelId });
        this.notify('handlerError', { event: 'consume', error: failure }); throw failure;
      }
      if (!this.current(generation)) return;
      // Await durable write even if the socket disconnects; the next generation reloads the store.
      if (!await this.checkpoint(() => consumer.options.checkpointStore.compareAndSet(consumer.scope!, prior, event.cursor))) throw new ConflictError('Checkpoint changed concurrently; channel ownership was lost');
      this.#cursor = event.cursor;
      if (this.current(generation)) this.notify('checkpoint', { channelId: this.channelId, consumerId: consumer.options.consumerId, cursor: event.cursor, sequence: event.sequence });
    } else this.#cursor = event.cursor;
  }
  async resync(): Promise<void> {
    if (this.status === 'closed') throw new AbortError('Subscription is closed');
    if (this.status === 'blocked') throw new ProtocolError('Channel is blocked; create a replacement subscription');
    this.notify('resync', { channelId: this.channelId, reason: 'requested' });
    this.#refreshRequested = true;
    this.#resyncRequested = true;
    this.wake(this.#generation);
  }
  async sendSignal(type: 'typing' | 'read', through?: number): Promise<void> {
    if (this.status !== 'live' || !this.socketOpen) throw new NetworkError('Channel is not live');
    if (type === 'read' && (!Number.isSafeInteger(through) || through! < 0)) throw new ValidationError('through must be non-negative Unix seconds');
    if (this.#lease) {
      const lifetime = budget(this.manager.config.timeout, [this.#cycle.signal]);
      try { await this.#lease.send(type, through, lifetime.signal); } finally { lifetime.dispose(); }
      return;
    }
    if (this.#socket!.bufferedAmount > this.manager.config.realtime.maxBufferedBytes) throw new BackpressureError('Outgoing signal buffer exceeded');
    const lifetime = budget(this.manager.config.timeout, [this.#cycle.signal]);
    try { await raceSignal(new Promise<void>((resolve, reject) => this.#socket!.send(JSON.stringify({ type, channel_id: this.channelId,
      ...(type === 'read' ? { ts: through } : {}) }), error => error ? reject(new NetworkError('Signal send failed')) : resolve())), lifetime.signal); }
    finally { lifetime.dispose(); }
  }
}
