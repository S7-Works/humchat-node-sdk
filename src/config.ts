import { ConfigurationError, UnsupportedFeatureError } from './errors.js';

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
export interface HumChatOptions {
  orgId: string;
  /** Required during the preview: the official production origin is unverified. */
  baseUrl: string;
  tokenProvider: TokenProvider;
  timeout?: number;
  retry?: RetryOptions;
  realtime?: RealtimeOptions;
  recovery?: RecoveryOptions;
  /** Optional fetch-compatible transport for testing or an application-managed proxy. */
  fetch?: typeof globalThis.fetch;
}
export interface SafeConfig {
  orgId: string;
  baseUrl: string;
  timeout: number;
  retry: Required<RetryOptions>;
  realtime: Required<RealtimeOptions>;
  recovery: Required<RecoveryOptions>;
  authentication: 'tokenProvider';
}
export interface ResolvedConfig extends SafeConfig {
  tokenProvider: TokenProvider;
  fetch: typeof globalThis.fetch;
}

function integer(value: unknown, name: string, min: number, max = 2_147_483_647): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new ConfigurationError(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
}

export function resolveConfig(options: HumChatOptions): ResolvedConfig {
  if (!options || typeof options !== 'object') throw new ConfigurationError('A configuration object is required');
  const raw = options as unknown as Record<string, unknown>;
  if ('apiKey' in raw) throw new UnsupportedFeatureError('Machine credential exchange is not supported in this preview; configure tokenProvider');
  for (const key of Object.keys(raw)) {
    if (!['orgId', 'baseUrl', 'tokenProvider', 'timeout', 'retry', 'fetch', 'realtime', 'recovery'].includes(key)) {
      throw new ConfigurationError('Unknown configuration field');
    }
  }
  if (typeof options.orgId !== 'string' || !options.orgId.trim() || options.orgId !== options.orgId.trim()) {
    throw new ConfigurationError('orgId must be a non-empty string without surrounding whitespace');
  }
  if (typeof options.baseUrl !== 'string') throw new ConfigurationError('baseUrl is required during the preview');
  let url: URL;
  try { url = new URL(options.baseUrl); } catch { throw new ConfigurationError('baseUrl must be an absolute HTTP(S) origin'); }
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
    throw new ConfigurationError('baseUrl must be an HTTP(S) origin without credentials, path, query, or fragment');
  }
  if (typeof options.tokenProvider !== 'function') throw new ConfigurationError('tokenProvider must be an async function');
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
  return {
    orgId: options.orgId, baseUrl: url.origin, timeout: integer(options.timeout ?? 10_000, 'timeout', 1),
    retry, realtime, recovery, authentication: 'tokenProvider', tokenProvider: options.tokenProvider,
    fetch: options.fetch ?? globalThis.fetch,
  };
}
