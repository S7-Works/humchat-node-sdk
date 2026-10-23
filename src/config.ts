import { ConfigurationError, UnsupportedFeatureError, ValidationError } from './errors.js';

export interface TokenResult {
  token: string;
  /** Unix seconds, never milliseconds. Prefer returning this when known. */
  expiresAt?: number;
}
export interface TokenProviderContext {
  reason: 'initial' | 'expiring' | 'unauthorized';
  previous?: { expiresAt: number | null };
  /** Shared acquisition deadline. A single caller's abort does not cancel other waiters. */
  signal: AbortSignal;
}
export type TokenProvider = (context: TokenProviderContext) => Promise<TokenResult>;
export interface RetryOptions {
  maxRetries?: number;
  initialDelayMs?: number;
  maxDelayMs?: number;
  totalTimeoutMs?: number;
}
export interface RealtimeOptions {
  /** Multiplex requires durable event protocol support; it never falls back silently. */
  transport?: 'channel' | 'multiplex';
  maxChannels?: number;
  connectConcurrency?: number;
  maxBufferedEvents?: number;
  maxBufferedBytes?: number;
  maxPayloadBytes?: number;
  maxCachedMessages?: number;
  maxCachedBytes?: number;
  reconnectInitialDelayMs?: number;
  reconnectMaxDelayMs?: number;
  pingIntervalMs?: number;
  pongTimeoutMs?: number;
  handlerTimeoutMs?: number;
}
export interface RecoveryOptions {
  /** Durable is the default; older servers require explicit bestEffort opt-in. */
  mode: 'bestEffort' | 'durable';
  maxPages?: number;
  maxThreads?: number;
  maxBytes?: number;
  maxStateRefresh?: number;
  timeoutMs?: number;
}
/** Delegated member login through the configured HumChat server. */
export interface SdkKeyAuthOptions {
  /** Secret backend credential. Never pass it as a Bearer token. */
  sdkKey: string;
  /** Organization member whose identity the server should verify. */
  actAs: string;
}
interface HumChatBaseOptions {
  orgId: string;
  /** Explicit HumChat HTTP(S) origin. SDK-key mode requires HTTPS outside loopback. */
  baseUrl: string;
  timeout?: number;
  retry?: RetryOptions;
  realtime?: RealtimeOptions;
  recovery?: RecoveryOptions;
  /** Optional fetch-compatible transport for testing or an application-managed proxy. */
  fetch?: typeof globalThis.fetch;
}
export type HumChatOptions = HumChatBaseOptions & (
  | { tokenProvider: TokenProvider; auth?: never }
  | { auth: SdkKeyAuthOptions; tokenProvider?: never }
);
export interface SafeConfig {
  orgId: string;
  baseUrl: string;
  timeout: number;
  retry: Required<RetryOptions>;
  realtime: Required<RealtimeOptions>;
  recovery: Required<RecoveryOptions>;
  authentication: 'tokenProvider' | 'sdkKey';
}
type ResolvedAuthentication =
  | { authentication: 'tokenProvider'; tokenProvider: TokenProvider }
  | { authentication: 'sdkKey'; tokenProvider?: never };
export type ResolvedConfig = Omit<SafeConfig, 'authentication'> & ResolvedAuthentication & {
  fetch: typeof globalThis.fetch;
};

function validateSdkKeyAuth(value: unknown): Readonly<SdkKeyAuthOptions> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new ValidationError('auth must contain sdkKey and actAs');
  }
  const auth = value as Record<string, unknown>;
  if (Object.keys(auth).some(key => key !== 'sdkKey' && key !== 'actAs')) {
    throw new ValidationError('Unknown auth field');
  }
  const key = auth.sdkKey;
  if (typeof key !== 'string' || key.length > 128 || !/^hsk_[A-Za-z0-9_-]+$/.test(key)) {
    throw new ValidationError('auth.sdkKey must be an hsk_ credential without whitespace or invalid header characters');
  }
  const actAs = auth.actAs;
  if (typeof actAs !== 'string') throw new ValidationError('auth.actAs must be an email address');
  // Snapshot once; later caller changes must not change the delegated principal.
  const email = normalizeEmail(actAs);
  const parts = email.split('@');
  if (Buffer.byteLength(email, 'utf8') > 254 || /[\s\p{Cc}]/u.test(email) || parts.length !== 2 ||
      !parts[0] || !parts[1]?.includes('.') || parts[1].split('.').some(label => !label)) {
    throw new ValidationError('auth.actAs must be an email address');
  }
  return Object.freeze({ sdkKey: key, actAs: email });
}

export function normalizeEmail(email: string): string {
  return email.trim().replace(/[A-Z]/g, letter => letter.toLowerCase());
}

function refuseBrowserSdkKey(): void {
  const runtime = globalThis as typeof globalThis & {
    window?: unknown; document?: unknown; WorkerGlobalScope?: unknown; self?: unknown; importScripts?: unknown;
  };
  if (runtime.window !== undefined || runtime.document !== undefined || runtime.WorkerGlobalScope !== undefined ||
      (runtime.self === globalThis && typeof runtime.importScripts === 'function')) {
    throw new UnsupportedFeatureError('SDK-key authentication is available only in a backend Node.js process');
  }
}

function integer(value: unknown, name: string, min: number, max = 2_147_483_647): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new ConfigurationError(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

export function resolveConfig(options: HumChatOptions): { config: ResolvedConfig; sdkKeyAuth?: Readonly<SdkKeyAuthOptions> } {
  if (!options || typeof options !== 'object') throw new ConfigurationError('A configuration object is required');
  const raw = options as unknown as Record<string, unknown>;
  if ('apiKey' in raw) throw new UnsupportedFeatureError('apiKey bot authentication is not supported; use tokenProvider or delegated auth configuration');
  const sdkKeyMode = 'auth' in raw;
  if (sdkKeyMode && 'tokenProvider' in raw) throw new ValidationError('Configure either auth or tokenProvider, not both');
  for (const key of Object.keys(raw)) {
    if (!['orgId', 'baseUrl', 'tokenProvider', 'auth', 'timeout', 'retry', 'fetch', 'realtime', 'recovery'].includes(key)) {
      throw new ConfigurationError('Unknown configuration field');
    }
  }
  let orgId = options.orgId;
  let authentication: ResolvedAuthentication;
  let sdkKeyAuth: Readonly<SdkKeyAuthOptions> | undefined;
  if (sdkKeyMode) {
    refuseBrowserSdkKey();
    sdkKeyAuth = validateSdkKeyAuth(raw.auth);
    if (typeof orgId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(orgId)) {
      throw new ValidationError('orgId must be a canonical UUID for SDK-key authentication');
    }
    orgId = orgId.toLowerCase();
    // Secrets travel separately and never enter publicly inspectable resolved config.
    authentication = { authentication: 'sdkKey' };
  } else {
    if (typeof options.tokenProvider !== 'function') throw new ConfigurationError('tokenProvider must be an async function');
    authentication = { authentication: 'tokenProvider', tokenProvider: options.tokenProvider };
  }
  if (typeof orgId !== 'string' || !orgId.trim() || orgId !== orgId.trim()) {
    throw new ConfigurationError('orgId must be a non-empty string without surrounding whitespace');
  }
  if (typeof options.baseUrl !== 'string') throw new ConfigurationError('baseUrl is required during the preview');
  let url: URL;
  try { url = new URL(options.baseUrl); } catch { throw new ConfigurationError('baseUrl must be an absolute HTTP(S) origin'); }
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
    throw new ConfigurationError('baseUrl must be an HTTP(S) origin without credentials, path, query, or fragment');
  }
  if (sdkKeyMode && url.protocol !== 'https:' &&
      !/^http:\/\/(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?\/?$/i.test(options.baseUrl)) {
    throw new ConfigurationError('SDK-key authentication requires HTTPS outside literal loopback development origins');
  }
  if (options.fetch !== undefined && typeof options.fetch !== 'function') throw new ConfigurationError('fetch must be a function');
  if (options.retry !== undefined && (!options.retry || typeof options.retry !== 'object' || Array.isArray(options.retry))) {
    throw new ConfigurationError('retry must be an object');
  }
  for (const key of Object.keys(options.retry ?? {})) {
    if (!['maxRetries', 'initialDelayMs', 'maxDelayMs', 'totalTimeoutMs'].includes(key)) throw new ConfigurationError('Unknown retry field');
  }
  const retry = {
    maxRetries: integer(options.retry?.maxRetries ?? 3, 'retry.maxRetries', 0, 10),
    initialDelayMs: integer(options.retry?.initialDelayMs ?? 250, 'retry.initialDelayMs', 0),
    maxDelayMs: integer(options.retry?.maxDelayMs ?? 2_000, 'retry.maxDelayMs', 0),
    totalTimeoutMs: integer(options.retry?.totalTimeoutMs ?? 30_000, 'retry.totalTimeoutMs', 1),
  };
  const realtimeDefaults: Required<RealtimeOptions> = {
    transport: 'channel',
    maxChannels: 50, connectConcurrency: 5, maxBufferedEvents: 1_000, maxBufferedBytes: 16 * 1024 * 1024,
    maxPayloadBytes: 10 * 1024 * 1024, maxCachedMessages: 1_000, maxCachedBytes: 8 * 1024 * 1024,
    reconnectInitialDelayMs: 500, reconnectMaxDelayMs: 30_000, pingIntervalMs: 30_000,
    pongTimeoutMs: 10_000, handlerTimeoutMs: 30_000,
  };
  const recoveryDefaults: Required<RecoveryOptions> = { mode: 'durable', maxPages: 3, maxThreads: 20, maxBytes: 16 * 1024 * 1024, maxStateRefresh: 50, timeoutMs: 30_000 };
  for (const [name, value, defaults] of [['realtime', options.realtime, realtimeDefaults], ['recovery', options.recovery, recoveryDefaults]] as const) {
    if (value !== undefined && (!value || typeof value !== 'object' || Array.isArray(value))) throw new ConfigurationError(`${name} must be an object`);
    for (const key of Object.keys(value ?? {})) if (!(key in defaults)) throw new ConfigurationError(`Unknown ${name} field`);
  }
  const realtime = { ...realtimeDefaults };
  for (const key of Object.keys(realtimeDefaults) as Array<keyof RealtimeOptions>) {
    if (key === 'transport') continue;
    realtime[key] = integer(options.realtime?.[key] ?? realtimeDefaults[key], `realtime.${key}`, 1);
  }
  realtime.transport = options.realtime?.transport ?? 'channel';
  if (realtime.transport !== 'channel' && realtime.transport !== 'multiplex') throw new ConfigurationError('realtime.transport must be channel or multiplex');
  const mode = options.recovery?.mode ?? 'durable';
  if (mode !== 'bestEffort' && mode !== 'durable') throw new ConfigurationError('recovery.mode must be bestEffort or durable');
  if (realtime.transport === 'multiplex' && mode !== 'durable') throw new UnsupportedFeatureError('Multiplex transport requires durable recovery');
  const recovery: Required<RecoveryOptions> = { mode,
    maxPages: integer(options.recovery?.maxPages ?? recoveryDefaults.maxPages, 'recovery.maxPages', 1, 100),
    maxThreads: integer(options.recovery?.maxThreads ?? recoveryDefaults.maxThreads, 'recovery.maxThreads', 0, 1_000),
    maxBytes: integer(options.recovery?.maxBytes ?? recoveryDefaults.maxBytes, 'recovery.maxBytes', 1),
    maxStateRefresh: integer(options.recovery?.maxStateRefresh ?? recoveryDefaults.maxStateRefresh, 'recovery.maxStateRefresh', 0, 10_000),
    timeoutMs: integer(options.recovery?.timeoutMs ?? recoveryDefaults.timeoutMs, 'recovery.timeoutMs', 1),
  };
  const config: ResolvedConfig = {
    orgId, baseUrl: url.origin, timeout: integer(options.timeout ?? 10_000, 'timeout', 1),
    retry, realtime, recovery, ...authentication,
    fetch: options.fetch ?? globalThis.fetch,
  };
  return { config, ...(sdkKeyAuth ? { sdkKeyAuth } : {}) };
}
