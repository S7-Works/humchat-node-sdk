import { z } from 'zod';
import type { ResolvedConfig, TokenProviderContext, TokenResult } from '../config.js';
import type { Identity } from '../types.js';
import { AuthenticationError, NotFoundError, ProtocolError, SessionRevokedError, UnsupportedFeatureError } from '../errors.js';
import { aborted, budget, HttpTransport, raceSignal } from '../http/transport.js';

const safeInteger = z.number().int().min(0).max(Number.MAX_SAFE_INTEGER);
const identitySchema = z.object({
  user_uuid: z.string().min(1), org: z.string().min(1), email: z.string(), roles: z.array(z.string()),
  scopes: z.array(z.string()).optional(), expires_at: safeInteger.nullable().optional(),
  app_uuid: z.string().min(1).nullable().optional(),
});
const capabilitiesSchema = z.object({
  sdk_protocol_version: z.literal(1),
  authentication: z.object({
    user_sessions: z.boolean(), service_identities: z.boolean(), api_key_exchange: z.boolean(),
    app_binding_required: z.boolean(), mock_authentication: z.boolean(),
  }),
  messaging: z.object({
    send_receipt_versions: z.array(safeInteger), message_state_versions: z.array(safeInteger),
    history_versions: z.array(safeInteger), durable_replay: z.boolean(), websocket_scope: z.literal('channel'),
    event_protocol_versions: z.array(safeInteger).optional(),
    multiplexed_events: z.object({ protocol_versions: z.array(safeInteger), max_channels: safeInteger.min(1).max(100) }).optional(),
    outgoing_webhooks: z.object({ schema_versions: z.array(safeInteger), max_channels: safeInteger.min(1).max(50), workspace_channels_only: z.boolean() }).optional(),
  }),
});
export interface SdkCapabilities {
  protocolVersion: 1;
  authentication: {
    userSessions: boolean; serviceIdentities: boolean; apiKeyExchange: boolean;
    appBindingRequired: boolean; mockAuthentication: boolean;
  };
  messaging: {
    sendReceiptVersions: number[]; messageStateVersions: number[]; historyVersions: number[];
    durableReplay: boolean; websocketScope: 'channel';
    eventProtocolVersions: number[];
    multiplexedEvents: { protocolVersions: number[]; maxChannels: number } | null;
    outgoingWebhooks: { schemaVersions: number[]; maxChannels: number; workspaceChannelsOnly: boolean } | null;
  };
}
export interface VerifiedSession {
  token: string;
  identity: Identity;
  expiresAt: number | null;
  refreshAt: number | null;
  capabilities: SdkCapabilities;
}

/** One acquisition/refresh in flight; only verified, same-principal sessions become visible. */
export class SessionManager {
  #current: VerifiedSession | undefined;
  #pending: Promise<VerifiedSession> | undefined;
  #bound: { actorId: string; orgId: string } | undefined;
  #blocked: AuthenticationError | SessionRevokedError | undefined;
  #listeners = new Set<(session: VerifiedSession) => void>();
  constructor(private readonly config: ResolvedConfig, private readonly http: HttpTransport, private readonly closed: AbortSignal) {}

  getActorId(): string {
    if (!this.#bound) throw new ProtocolError('No verified actor is bound to this client');
    return this.#bound.actorId;
  }
  onChange(listener: (session: VerifiedSession) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }
  revoke(): void {
    this.#blocked = new SessionRevokedError('Session ended; create a new client with a new credential lifecycle');
    this.#current = undefined;
  }

  async get(signal: AbortSignal): Promise<VerifiedSession> {
    if (signal.aborted) throw aborted(signal);
    if (this.#blocked) throw this.#blocked;
    if (this.#pending) return raceSignal(this.#pending, signal);
    if (!this.#current) return this.refresh('initial', signal);
    if (this.#current.refreshAt !== null && Date.now() / 1_000 >= this.#current.refreshAt) return this.refresh('expiring', signal);
    return this.#current;
  }

  async unauthorized(previous: VerifiedSession, signal: AbortSignal): Promise<VerifiedSession> {
    if (signal.aborted) throw aborted(signal);
    if (this.#blocked) throw this.#blocked;
    if (this.#pending) return raceSignal(this.#pending, signal);
    if (this.#current && this.#current !== previous) return this.get(signal);
    return this.refresh('unauthorized', signal);
  }

  private refresh(reason: TokenProviderContext['reason'], signal: AbortSignal): Promise<VerifiedSession> {
    if (!this.#pending) {
      const work = this.acquire(reason);
      this.#pending = work;
      void work.then(() => { this.#pending = undefined; }, () => { this.#pending = undefined; });
    }
    return raceSignal(this.#pending, signal);
  }

  private async acquire(reason: TokenProviderContext['reason']): Promise<VerifiedSession> {
    const lifetime = budget(this.config.retry.totalTimeoutMs, [this.closed]);
    try {
      const context: TokenProviderContext = { reason, signal: lifetime.signal };
      if (this.#current) context.previous = { expiresAt: this.#current.expiresAt };
      let supplied: TokenResult;
      try { supplied = await raceSignal(Promise.resolve().then(() => this.config.tokenProvider(context)), lifetime.signal); }
      catch (error) {
        if (lifetime.signal.aborted) throw aborted(lifetime.signal);
        throw new AuthenticationError('Token provider failed');
      }
      if (!supplied || typeof supplied.token !== 'string' || !supplied.token || /\s|[\x00-\x1f\x7f]/.test(supplied.token)) {
        throw new AuthenticationError('Token provider returned an invalid token');
      }
      if (supplied.expiresAt !== undefined && (!Number.isSafeInteger(supplied.expiresAt) || supplied.expiresAt <= Date.now() / 1_000)) {
        throw new AuthenticationError('Token provider returned an invalid or expired expiry');
      }
      const me = identitySchema.safeParse(await this.http.request('/me', supplied.token, {}, lifetime.signal));
      if (!me.success) throw new ProtocolError('Invalid identity response');
      const raw = me.data;
      if (raw.org !== this.config.orgId || (this.#bound && (this.#bound.actorId !== raw.user_uuid || this.#bound.orgId !== raw.org))) {
        this.#blocked = new AuthenticationError('Identity changed or does not match the configured organization; create a new client');
        this.#current = undefined;
        throw this.#blocked;
      }
      let capabilitiesRaw: unknown;
      try { capabilitiesRaw = await this.http.request('/api/sdk/capabilities', supplied.token, {}, lifetime.signal); }
      catch (error) {
        if (error instanceof NotFoundError) throw new UnsupportedFeatureError('Server SDK capability discovery is required by this preview');
        throw error;
      }
      const parsed = capabilitiesSchema.safeParse(capabilitiesRaw);
      if (!parsed.success) throw new ProtocolError('Invalid SDK capability response');
      const cap = parsed.data;
      if (!cap.messaging.send_receipt_versions.includes(1) || !cap.messaging.message_state_versions.includes(1) || !cap.messaging.history_versions.includes(2)) {
        throw new UnsupportedFeatureError('Server lacks the required send v1, state v1, or history v2 protocol');
      }
      const expiryCandidates = [supplied.expiresAt, raw.expires_at].filter((value): value is number => typeof value === 'number');
      const expiresAt = expiryCandidates.length ? Math.min(...expiryCandidates) : null;
      const now = Date.now() / 1_000;
      if (this.#blocked) throw this.#blocked;
      if (expiresAt !== null && expiresAt <= now) throw new AuthenticationError('Verified session is expired');
      const identity: Identity = {
        actorId: raw.user_uuid, orgId: raw.org, email: raw.email, roles: raw.roles,
        scopes: raw.scopes ?? [], expiresAt: raw.expires_at ?? null, appId: raw.app_uuid ?? null,
      };
      const capabilities: SdkCapabilities = {
        protocolVersion: 1,
        authentication: {
          userSessions: cap.authentication.user_sessions, serviceIdentities: cap.authentication.service_identities,
          apiKeyExchange: cap.authentication.api_key_exchange, appBindingRequired: cap.authentication.app_binding_required,
          mockAuthentication: cap.authentication.mock_authentication,
        },
        messaging: {
          sendReceiptVersions: cap.messaging.send_receipt_versions, messageStateVersions: cap.messaging.message_state_versions,
          historyVersions: cap.messaging.history_versions, durableReplay: cap.messaging.durable_replay, websocketScope: cap.messaging.websocket_scope,
          eventProtocolVersions: cap.messaging.event_protocol_versions ?? [],
          multiplexedEvents: cap.messaging.multiplexed_events ? { protocolVersions: cap.messaging.multiplexed_events.protocol_versions,
            maxChannels: cap.messaging.multiplexed_events.max_channels } : null,
          outgoingWebhooks: cap.messaging.outgoing_webhooks ? { schemaVersions: cap.messaging.outgoing_webhooks.schema_versions,
            maxChannels: cap.messaging.outgoing_webhooks.max_channels, workspaceChannelsOnly: cap.messaging.outgoing_webhooks.workspace_channels_only } : null,
        },
      };
      this.#bound = { actorId: identity.actorId, orgId: identity.orgId };
      this.#current = { token: supplied.token, identity, expiresAt,
        refreshAt: expiresAt === null ? null : expiresAt - Math.min(60, (expiresAt - now) / 2), capabilities };
      for (const listener of this.#listeners) listener(this.#current);
      return this.#current;
    } finally { lifetime.dispose(); }
  }
}
