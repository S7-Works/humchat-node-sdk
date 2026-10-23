import WebSocket from 'ws';
import type { VerifiedSession } from '../auth/session.js';
import { AbortError, AuthenticationError, BackpressureError, HumChatError, NetworkError, NotFoundError,
  PermissionError, ProtocolError, RateLimitError, ServerError, TimeoutError, UnsupportedFeatureError, ValidationError } from '../errors.js';
import { aborted, budget, raceSignal } from '../http/transport.js';
import { parseReady, type ReadyFrame } from './durable-protocol.js';
import { parseFrame } from './protocol.js';
import type { RealtimeClient } from './manager.js';

type Frame = Record<string, unknown>;
type Waiting = { resolve: (ready: ReadyFrame) => void; reject: (error: HumChatError) => void; cleanup: () => void };

/** A logical subscription. Cancellation removes its correlation ID before sending unsubscribe. */
export class MultiplexLease {
  id: string | undefined;
  deadline: number | undefined;
  ready = false;
  #waiting: Waiting | undefined;
  #frame: ((raw: Frame) => void) | undefined;
  #interrupt: ((error: HumChatError) => void) | undefined;
  #closed = false;
  constructor(readonly channelId: string, private readonly owner: MultiplexTransport) {}
  get socketOpen(): boolean { return this.ready && this.owner.socketOpen; }
  open(signal: AbortSignal, frame: (raw: Frame) => void, interrupt: (error: HumChatError) => void): Promise<ReadyFrame> {
    this.cancelAttempt();
    if (this.#closed) return Promise.reject(new AbortError('Multiplex channel is closed'));
    if (signal.aborted) return Promise.reject(aborted(signal));
    this.#frame = frame; this.#interrupt = interrupt;
    return new Promise((resolve, reject) => {
      const cancel = () => this.cancelAttempt(aborted(signal));
      this.#waiting = { resolve, reject, cleanup: () => signal.removeEventListener('abort', cancel) };
      signal.addEventListener('abort', cancel, { once: true });
      this.owner.request(this);
    });
  }
  get pending(): boolean { return this.#waiting !== undefined; }
  accept(raw: Frame): void {
    if (!this.#waiting || this.ready) throw new ProtocolError('Unexpected multiplex ready frame');
    const ready = parseReady(raw, this.owner.orgId, this.channelId);
    this.ready = true; this.deadline = undefined;
    const waiting = this.#waiting; this.#waiting = undefined;
    waiting.cleanup(); waiting.resolve(ready);
  }
  frame(raw: Frame): void { if (this.ready) this.#frame?.(raw); }
  poll(): void { if (this.ready) this.frame({ type: 'resync', channel_id: this.channelId }); }
  disconnect(error: HumChatError): void {
    this.id = undefined; this.deadline = undefined;
    if (!this.ready) return; // Pending opens await the single shared reconnect loop.
    this.ready = false;
    this.#interrupt?.(error);
  }
  reject(error: HumChatError): void {
    const waiting = this.#waiting, interrupt = this.#interrupt;
    this.cancelAttempt(error);
    if (!waiting) interrupt?.(error);
  }
  cancelAttempt(error: HumChatError = new AbortError('Logical subscription attempt ended')): void {
    this.owner.forget(this);
    this.ready = false; this.deadline = undefined;
    const waiting = this.#waiting; this.#waiting = undefined;
    this.#frame = undefined; this.#interrupt = undefined;
    if (waiting) { waiting.cleanup(); waiting.reject(error); }
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true; this.cancelAttempt(); this.owner.remove(this);
  }
  async send(type: 'typing' | 'read', through: number | undefined, signal: AbortSignal): Promise<void> {
    if (!this.socketOpen || !this.id) throw new NetworkError('Channel is not live');
    await this.owner.send({ type, subscription_id: this.id, channel_id: this.channelId,
      ...(type === 'read' ? { ts: through } : {}) }, signal);
  }
}

/** One physical connection, retry loop, refresh lifecycle and heartbeat for all durable channels. */
export class MultiplexTransport {
  readonly orgId: string;
  #leases = new Set<MultiplexLease>();
  #ids = new Map<string, MultiplexLease>();
  #queue = new Set<MultiplexLease>();
  #socket: WebSocket | undefined;
  #session: VerifiedSession | undefined;
  #ready = false;
  #connecting = false;
  #closed = false;
  #blocked: HumChatError | undefined;
  #generation = 0;
  #serial = 0;
  #attempt = 0;
  #limit = 100;
  #cycle = new AbortController();
  #retry: ReturnType<typeof setTimeout> | undefined;
  #heartbeat: ReturnType<typeof setInterval> | undefined;
  #pong: ReturnType<typeof setTimeout> | undefined;
  #refresh: ReturnType<typeof setTimeout> | undefined;
  #stable: ReturnType<typeof setTimeout> | undefined;
  #poll: ReturnType<typeof setInterval> | undefined;
  #supervisor: ReturnType<typeof setInterval> | undefined;
  constructor(private readonly manager: RealtimeClient) { this.orgId = manager.config.orgId; }
  get socketOpen(): boolean { return this.#socket?.readyState === WebSocket.OPEN; }
  create(channelId: string): MultiplexLease {
    if (this.#closed) throw new AbortError('Client is closed');
    if (this.#blocked) throw this.#blocked;
    const lease = new MultiplexLease(channelId, this); this.#leases.add(lease); return lease;
  }
  request(lease: MultiplexLease): void {
    if (this.#blocked || this.#closed) { lease.reject(this.#blocked ?? new AbortError('Client is closed')); return; }
    this.#queue.add(lease);
    if (this.#ready) this.flush(); else if (!this.#connecting && !this.#retry) void this.connect();
  }
  forget(lease: MultiplexLease): void {
    this.#queue.delete(lease);
    const id = lease.id; lease.id = undefined;
    if (!id) return;
    this.#ids.delete(id);
    if (this.#ready && this.socketOpen) this.write({ type: 'unsubscribe', subscription_id: id });
  }
  remove(lease: MultiplexLease): void {
    this.#leases.delete(lease);
    if (!this.#leases.size) { this.reset(); this.#attempt = 0; }
  }
  rotate(session: VerifiedSession): void {
    if (this.#session && this.#session.token !== session.token && this.#ready) this.fail(new NetworkError('Credential changed; reconnecting multiplex stream', { retryable: true }));
  }
  close(): void { this.#closed = true; this.reset(); for (const lease of [...this.#leases]) lease.close(); }
  block(error: HumChatError): void {
    if (this.#closed || this.#blocked) return;
    this.#blocked = error; this.reset();
    for (const lease of [...this.#leases]) lease.reject(error);
  }
  private reset(): void {
    this.#generation++; this.#ready = false; this.#connecting = false;
    this.#cycle.abort(new AbortError('Multiplex connection generation ended'));
    clearTimeout(this.#retry); clearInterval(this.#heartbeat); clearTimeout(this.#pong);
    clearTimeout(this.#refresh); clearTimeout(this.#stable); clearInterval(this.#poll); clearInterval(this.#supervisor);
    this.#retry = undefined; this.#heartbeat = undefined; this.#pong = undefined;
    this.#refresh = undefined; this.#stable = undefined; this.#poll = undefined; this.#supervisor = undefined;
    const socket = this.#socket; this.#socket = undefined;
    if (socket) { socket.on('error', () => {}); socket.terminate(); }
    this.#ids.clear(); this.#queue.clear();
  }
  private current(generation: number): boolean { return generation === this.#generation && !this.#closed && !this.#blocked && !this.#cycle.signal.aborted; }
  private fail(error: HumChatError): void {
    if (this.#closed || this.#blocked) return;
    if (!error.retryable) { this.block(error); return; }
    this.reset();
    if (!this.#leases.size) return;
    const config = this.manager.config.realtime;
    const ceiling = Math.min(config.reconnectMaxDelayMs, config.reconnectInitialDelayMs * 2 ** Math.min(this.#attempt, 20));
    const delay = Math.max(1, Math.floor(Math.random() * ceiling), error.retryAfterMs ?? 0);
    this.#attempt++; this.manager.counters.reconnectCount++;
    // Install the shared timer before interrupting workers that will immediately request new leases.
    this.#retry = setTimeout(() => { this.#retry = undefined; void this.connect(); }, delay);
    for (const lease of [...this.#leases]) {
      lease.disconnect(error);
      if (lease.pending) this.#queue.add(lease);
    }
  }
  private async connect(): Promise<void> {
    if (this.#closed || this.#blocked || this.#connecting || !this.#leases.size) return;
    this.#cycle = new AbortController(); const generation = ++this.#generation;
    this.#connecting = true;
    const lifetime = budget(this.manager.config.retry.totalTimeoutMs, [this.#cycle.signal]);
    try {
      let session = await this.manager.sessions.get(lifetime.signal);
      this.validateCapabilities(session); this.#session = session;
      try { await this.open(session, generation); }
      catch (error) {
        if (!(error instanceof AuthenticationError) || error.status !== 401 || !this.current(generation)) throw error;
        try { session = await this.manager.sessions.unauthorized(session, lifetime.signal); }
        catch (error) { this.manager.counters.authRefreshFailures++; throw error; }
        this.validateCapabilities(session); this.#session = session;
        await this.open(session, generation);
      }
      if (!this.current(generation)) return;
      const latest = await this.manager.sessions.get(lifetime.signal);
      if (latest.token !== session.token) throw new NetworkError('Credential changed during multiplex handshake', { retryable: true });
      if (!this.current(generation)) return;
      this.#connecting = false; this.#ready = true;
      this.startTimers(generation); this.flush();
    } catch (error) { if (this.current(generation)) this.fail(error instanceof HumChatError ? error : new ProtocolError('Multiplex connection failed')); }
    finally { lifetime.dispose(); }
  }
  private validateCapabilities(session: VerifiedSession): void {
    const cap = session.capabilities.messaging;
    if (!cap.durableReplay || !cap.eventProtocolVersions.includes(1) || !cap.multiplexedEvents?.protocolVersions.includes(1)) {
      throw new UnsupportedFeatureError('Server does not advertise multiplex event protocol v1');
    }
    this.#limit = cap.multiplexedEvents.maxChannels;
  }
  private open(session: VerifiedSession, generation: number): Promise<void> {
    const url = new URL('/ws/events', this.manager.config.baseUrl); url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    return new Promise((resolve, reject) => {
      let ready = false, failed = false;
      const timer = setTimeout(() => fail(new TimeoutError('Multiplex ready handshake timed out', { retryable: true })), this.manager.config.timeout);
      const socket = new WebSocket(url, { headers: { Authorization: `Bearer ${session.token}`, 'X-HumChat-Stream-Protocol': 'v1' },
        followRedirects: false, handshakeTimeout: this.manager.config.timeout, maxPayload: this.manager.config.realtime.maxPayloadBytes, perMessageDeflate: false });
      this.#socket = socket;
      const cleanup = () => { clearTimeout(timer); this.#cycle.signal.removeEventListener('abort', cancel); };
      const fail = (error: HumChatError) => {
        if (failed) return;
        failed = true; cleanup();
        if (!ready) reject(error); else if (this.current(generation) && this.#socket === socket) this.fail(error);
        socket.terminate();
      };
      const cancel = () => fail(new AbortError('Multiplex handshake cancelled'));
      this.#cycle.signal.addEventListener('abort', cancel, { once: true });
      socket.on('unexpected-response', (_request, response) => {
        const status = response.statusCode ?? 0, raw = response.headers['retry-after'];
        const milliseconds = typeof raw === 'string' ? (/^\d+(\.\d+)?$/.test(raw) ? Number(raw) * 1000 : Date.parse(raw) - Date.now()) : NaN;
        const retry = { status, retryable: true, ...(Number.isFinite(milliseconds) ? { retryAfterMs: Math.max(0, Math.min(2_147_483_647, milliseconds)) } : {}) };
        response.destroy();
        fail(status === 401 ? new AuthenticationError('Multiplex authentication rejected', { status })
          : status === 403 ? new PermissionError('Multiplex permission denied', { status })
            : status === 404 ? new UnsupportedFeatureError('Multiplex endpoint is unavailable', { status })
              : status === 429 ? new RateLimitError('Multiplex rate limit exceeded', retry)
                : status === 408 ? new TimeoutError('Multiplex upgrade timed out', retry)
                  : [502, 503, 504].includes(status) ? new ServerError('Multiplex service unavailable', retry)
                    : new ProtocolError('Multiplex negotiation rejected', { status }));
      });
      socket.on('message', (data, binary) => {
        if (!this.current(generation) || this.#socket !== socket) return;
        try {
          if (binary) throw new ProtocolError('Unexpected binary multiplex frame');
          const bytes = Buffer.isBuffer(data) ? data : Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data);
          const raw: unknown = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
          if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new ProtocolError('Invalid multiplex frame');
          const frame = raw as Frame;
          if (frame.type === 'session_revoked') {
            const parsed = parseFrame(frame, this.orgId, '');
            if ((parsed.kind === 'sessionRevoked' || parsed.kind === 'sessionExpired') && this.manager.endSession(parsed, session)) {
              fail(new NetworkError('Socket token expired; reconnecting with a fresh session', { retryable: true }));
            }
            return;
          }
          if (frame.type === 'stream_error') throw new ProtocolError('Multiplex stream rejected by server');
          if (!ready) {
            if (frame.type !== 'stream_ready' || frame.schema !== 1 || frame.org_uuid !== this.orgId ||
              !Number.isSafeInteger(frame.max_channels) || Number(frame.max_channels) < 1 || Number(frame.max_channels) > this.#limit) throw new ProtocolError('Invalid multiplex stream ready');
            this.#limit = Number(frame.max_channels); ready = true; cleanup(); resolve(); return;
          }
          this.frame(frame);
        } catch (error) { fail(error instanceof HumChatError ? error : new ProtocolError('Invalid multiplex JSON')); }
      });
      socket.on('pong', () => { if (this.current(generation) && socket === this.#socket) { clearTimeout(this.#pong); this.#pong = undefined; } });
      socket.on('error', error => fail((error as Error & { code?: string }).code === 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'
        ? new BackpressureError('Multiplex frame exceeds the configured payload limit')
        : new NetworkError('Multiplex transport failed', { retryable: true })));
      socket.on('close', () => { cleanup(); if (this.current(generation) && socket === this.#socket) fail(new NetworkError('Multiplex socket closed', { retryable: true })); });
    });
  }
  private frame(raw: Frame): void {
    if (raw.type === 'stream_ready') throw new ProtocolError('Duplicate multiplex stream ready');
    if (typeof raw.subscription_id !== 'string' || !/^[\x21-\x7e]{1,128}$/.test(raw.subscription_id)) throw new ProtocolError('Missing multiplex correlation ID');
    const lease = this.#ids.get(raw.subscription_id);
    if (!lease) return; // Late frames cannot reach a replacement lease for the same channel.
    if (raw.type === 'unsubscribed') throw new ProtocolError('Unexpected unsubscribe acknowledgement for an active lease');
    if (raw.channel_id !== lease.channelId || ('org_uuid' in raw && raw.org_uuid !== this.orgId)) throw new ProtocolError('Multiplex channel scope mismatch');
    if (raw.type === 'channel_error') {
      if (raw.schema !== 1 || raw.retryable !== false || !Number.isSafeInteger(raw.status) || Number(raw.status) < 400 || Number(raw.status) > 599) throw new ProtocolError('Invalid multiplex channel error');
      const options = { status: Number(raw.status), channelId: lease.channelId };
      const error = raw.code === 'FORBIDDEN' ? new PermissionError('Channel permission denied', options)
        : raw.code === 'NOT_FOUND' ? new NotFoundError('Channel not found', options)
          : raw.code === 'LIMIT_EXCEEDED' ? new BackpressureError('Multiplex channel limit reached', options)
            : raw.code === 'INVALID_REQUEST' ? new ValidationError('Multiplex channel request rejected', options)
              : raw.code === 'SERVER_ERROR' ? new ServerError('Multiplex channel rejected', options)
              : undefined;
      if (!error) throw new ProtocolError('Unknown multiplex channel error');
      lease.reject(error); return;
    }
    if (raw.type === 'ready') { lease.accept(raw); return; }
    if (!lease.ready) throw new ProtocolError('Multiplex channel data arrived before ready');
    if (raw.type === 'watermark') {
      const watermark = parseReady({ ...raw, type: 'ready' }, this.orgId, lease.channelId);
      lease.frame({ type: 'watermark', cursor: watermark.cursor }); return;
    }
    lease.frame(raw);
  }
  private flush(): void {
    if (!this.#ready || !this.socketOpen) return;
    for (const lease of [...this.#queue]) {
      this.#queue.delete(lease);
      if (!lease.pending || lease.id) continue;
      if (this.#ids.size >= this.#limit) { lease.reject(new BackpressureError('Multiplex server channel limit reached', { channelId: lease.channelId })); continue; }
      const id = `s${++this.#serial}`;
      lease.id = id; lease.deadline = Date.now() + this.manager.config.timeout; this.#ids.set(id, lease);
      if (!this.write({ type: 'subscribe', subscription_id: id, channel_id: lease.channelId })) return;
    }
  }
  private write(frame: Frame): boolean {
    if (!this.#ready || !this.socketOpen) return false;
    const bytes = Buffer.byteLength(JSON.stringify(frame));
    if (bytes > this.manager.config.realtime.maxBufferedBytes) {
      this.block(new BackpressureError('Multiplex command exceeds the configured buffer limit')); return false;
    }
    if (this.#socket!.bufferedAmount + bytes > this.manager.config.realtime.maxBufferedBytes) {
      this.fail(new BackpressureError('Multiplex signal buffer exceeded', { retryable: true })); return false;
    }
    const socket = this.#socket!;
    socket.send(JSON.stringify(frame), error => { if (error && socket === this.#socket) this.fail(new NetworkError('Multiplex send failed', { retryable: true })); });
    return true;
  }
  async send(frame: Frame, signal: AbortSignal): Promise<void> {
    if (!this.#ready || !this.socketOpen) throw new NetworkError('Multiplex stream is not live');
    const socket = this.#socket!, data = JSON.stringify(frame);
    if (socket.bufferedAmount + Buffer.byteLength(data) > this.manager.config.realtime.maxBufferedBytes) throw new BackpressureError('Multiplex signal buffer exceeded');
    await raceSignal(new Promise<void>((resolve, reject) => socket.send(data, error => error ? reject(new NetworkError('Multiplex signal send failed')) : resolve())), signal);
  }
  private startTimers(generation: number): void {
    const config = this.manager.config.realtime;
    this.#heartbeat = setInterval(() => {
      if (!this.current(generation) || !this.socketOpen || this.#pong) return;
      const socket = this.#socket!;
      socket.ping(undefined, undefined, error => { if (error && this.current(generation)) this.fail(new NetworkError('Multiplex ping failed', { retryable: true })); });
      this.#pong = setTimeout(() => { if (this.current(generation)) this.fail(new TimeoutError('Multiplex pong deadline exceeded', { retryable: true })); }, config.pongTimeoutMs);
    }, config.pingIntervalMs);
    this.#poll = setInterval(() => { if (this.current(generation)) for (const lease of this.#leases) lease.poll(); }, 5000);
    this.#supervisor = setInterval(() => {
      if (!this.current(generation)) return;
      for (const lease of [...this.#ids.values()]) if (lease.deadline !== undefined && lease.deadline <= Date.now()) lease.reject(new TimeoutError('Multiplex channel ready deadline exceeded'));
    }, Math.min(1000, this.manager.config.timeout));
    this.#stable = setTimeout(() => { if (this.current(generation)) this.#attempt = 0; }, 30_000);
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
      } catch (error) {
        if (this.current(generation)) { this.manager.counters.authRefreshFailures++; this.fail(error instanceof HumChatError ? error : new AuthenticationError('Multiplex credential refresh failed')); }
      } finally { lifetime.dispose(); }
    })(); }, Math.min(60_000, Math.max(1, this.#session.refreshAt * 1000 - Date.now())));
  }
}
