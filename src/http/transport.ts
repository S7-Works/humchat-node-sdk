import type { ResolvedConfig, SdkKeyAuthOptions, TokenResult } from '../config.js';
import { parseGap } from '../realtime/durable-protocol.js';
import {
  AbortError, AuthenticationError, ConflictError, HumChatError, NetworkError, NotFoundError,
  PermissionError, ProtocolError, RateLimitError, ServerError, TimeoutError, UnsupportedFeatureError, ValidationError,
  type HumChatErrorOptions,
} from '../errors.js';

export interface HttpOptions extends HumChatErrorOptions {
  method?: 'GET' | 'POST' | 'DELETE';
  body?: unknown;
  headers?: Record<string, string>;
  signal?: AbortSignal;
  timeout?: number;
  replaySafe?: boolean;
  /** Internal: one-time secret operations must not replay after a 401. */
  authRetry?: boolean;
}
export function aborted(signal: AbortSignal): HumChatError {
  return signal.reason instanceof HumChatError ? signal.reason : new AbortError('Operation cancelled');
}
export function raceSignal<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) { void promise.catch(() => {}); return Promise.reject(aborted(signal)); }
  return new Promise<T>((resolve, reject) => {
    const cancel = () => { signal.removeEventListener('abort', cancel); reject(aborted(signal)); };
    signal.addEventListener('abort', cancel, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', cancel));
  });
}
export function budget(milliseconds: number, signals: AbortSignal[] = []): { signal: AbortSignal; dispose(): void } {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new TimeoutError('Operation deadline exceeded', { retryable: true })), milliseconds);
  return { signal: AbortSignal.any([...signals, controller.signal]), dispose: () => clearTimeout(timer) };
}
export function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(aborted(signal));
  return new Promise((resolve, reject) => {
    const cancel = () => { clearTimeout(timer); signal.removeEventListener('abort', cancel); reject(aborted(signal)); };
    const timer = setTimeout(() => { signal.removeEventListener('abort', cancel); resolve(); }, milliseconds);
    signal.addEventListener('abort', cancel, { once: true });
  });
}
export function requestTimeout(options: HttpOptions, config: ResolvedConfig): number {
  const value = options.timeout ?? config.timeout;
  if (!Number.isSafeInteger(value) || value <= 0 || value > 2_147_483_647) throw new ValidationError('timeout must be a positive bounded integer');
  return value;
}
export function withIntent(error: unknown, options: HttpOptions): HumChatError {
  const source = error instanceof HumChatError ? error : new NetworkError('Transport failed', { retryable: true });
  // Shared auth failures and AbortSignal reasons must not acquire another call's IDs.
  const result = new Error(source.message) as HumChatError;
  Object.setPrototypeOf(result, Object.getPrototypeOf(source));
  Object.defineProperties(result, Object.getOwnPropertyDescriptors(source));
  // These IDs are caller-owned intent metadata, never response bodies or credentials.
  for (const key of ['channelId', 'operationId', 'messageId', 'idempotencyKey'] as const) {
    if (options[key] !== undefined) Object.defineProperty(result, key, { value: options[key], configurable: true, enumerable: true });
  }
  return result;
}
const transient = new Set([408, 429, 502, 503, 504]);
function statusError(response: Response, isSensitive: (value: string) => boolean, mint: boolean): HumChatError {
  const status = response.status;
  const requestId = response.headers.get('x-request-id');
  const options: HumChatErrorOptions = { status, retryable: transient.has(status) };
  if (requestId && /^[\w.:-]{1,128}$/.test(requestId) && !isSensitive(requestId)) options.requestId = requestId;
  const retryAfter = response.headers.get('retry-after');
  if (retryAfter !== null) {
    const numeric = /^\d+(?:\.\d+)?$/.test(retryAfter.trim()) ? Number(retryAfter) * 1_000 : Date.parse(retryAfter) - Date.now();
    if (Number.isFinite(numeric)) options.retryAfterMs = Math.max(0, Math.min(2_147_483_647, Math.ceil(numeric)));
  }
  if (status === 401) return new AuthenticationError('Authentication rejected', options);
  if (status === 403) return new PermissionError('Permission denied', options);
  if (mint && (status === 404 || status === 405)) return new UnsupportedFeatureError('Server does not support SDK-key login', options);
  if (status === 404) return new NotFoundError('Resource not found', options);
  if (status === 409) return new ConflictError('Operation conflicts with existing state or intent', options);
  if (status === 429) return new RateLimitError('Rate limit exceeded', options);
  if (status === 400 || status === 422 || (mint && (status === 413 || status === 415))) return new ValidationError('Server rejected request input', options);
  if (status === 408) return new TimeoutError('Server request timeout', options);
  if (status >= 500) return new ServerError('Server request failed', options);
  return new ProtocolError('Unexpected HTTP status', options);
}

/** Fetch-only transport. Credentials are supplied by the verified session manager. */
export class HttpTransport {
  #cooldownUntil = 0;
  #isSensitive: (value: string) => boolean;
  #credentials = new Set<string>();
  #omitRequestIds = false;
  constructor(private readonly config: ResolvedConfig, isSensitive: (value: string) => boolean = () => false) {
    this.#isSensitive = isSensitive;
  }

  private rememberCredential(value: string): void {
    // Only credentials this short can fit in the bounded request-ID header.
    if (this.#omitRequestIds || !value || value.length > 128 || this.#credentials.has(value)) return;
    // Never forget an old credential while continuing to expose remote IDs.
    // Long-lived clients fail closed once the bounded redaction history fills.
    if (this.#credentials.size >= 128) {
      this.#credentials.clear();
      this.#omitRequestIds = true;
    } else this.#credentials.add(value);
  }

  private sensitiveRequestId(value: string): boolean {
    return this.#omitRequestIds || this.#isSensitive(value) ||
      [...this.#credentials].some(credential => value.includes(credential));
  }

  async request(path: string, token: string, options: HttpOptions, signal: AbortSignal): Promise<unknown> {
    return (await this.execute(path, { token }, options, signal)).value;
  }

  /** Bootstrap only at this fixed same-origin endpoint; never follow a credential redirect. */
  async mintSdkKey(auth: Readonly<SdkKeyAuthOptions>, signal: AbortSignal): Promise<TokenResult> {
    const result = await this.execute('/api/sdk/auth/user-token', { sdkKey: auth.sdkKey }, {
      method: 'POST', body: { org_uuid: this.config.orgId, email: auth.actAs, expires_in: 900 },
      // Retrying login can issue another short-lived token; it does not mutate messaging state.
      replaySafe: true,
    }, signal);
    const value = result.value as Record<string, unknown> | null;
    // Remember even unusable/expired results before verification can reject them.
    if (value && typeof value.access_token === 'string') this.rememberCredential(value.access_token);
    if (!value || typeof value !== 'object' || Array.isArray(value) ||
        typeof value.access_token !== 'string' || !/^[\x21-\x7e]+$/.test(value.access_token) ||
        value.access_token.startsWith('hsk_') || value.access_token.includes(auth.sdkKey) ||
        typeof value.token_type !== 'string' || value.token_type.toLowerCase() !== 'bearer' ||
        typeof value.expires_in !== 'number' || !Number.isSafeInteger(value.expires_in) || value.expires_in < 1 || value.expires_in > 3600) {
      throw new ProtocolError('Invalid SDK-key token response');
    }
    // Count latency against validity; never extend a token's lifetime by response duration.
    return { token: value.access_token, expiresAt: Math.floor(result.requestedAt / 1000) + value.expires_in };
  }

  private async execute(path: string, credential: { token: string } | { sdkKey: string }, options: HttpOptions,
    signal: AbortSignal): Promise<{ value: unknown; requestedAt: number }> {
    const timeout = requestTimeout(options, this.config);
    const method = options.method ?? 'GET';
    const replaySafe = method === 'GET' || options.replaySafe === true;
    const body = options.body === undefined ? undefined : JSON.stringify(options.body);
    this.rememberCredential('sdkKey' in credential ? credential.sdkKey : credential.token);
    for (let attempt = 0; ; attempt++) {
      try {
        await delay(Math.max(0, this.#cooldownUntil - Date.now()), signal);
        if (signal.aborted) throw aborted(signal);
        const attemptBudget = budget(timeout, [signal]);
        try {
          const headers = new Headers(options.headers);
          const mint = 'sdkKey' in credential;
          if (mint) {
            headers.delete('Authorization');
            headers.set('X-HumChat-Sdk-Key', credential.sdkKey);
          } else {
            headers.set('Authorization', `Bearer ${credential.token}`);
          }
          headers.set('Accept', 'application/json');
          if (body !== undefined) headers.set('Content-Type', 'application/json');
          let response: Response;
          const requestedAt = Date.now();
          try {
            response = await raceSignal(this.config.fetch(new URL(path, this.config.baseUrl), {
              method, headers, ...(body === undefined ? {} : { body }), signal: attemptBudget.signal, redirect: 'error',
            }), attemptBudget.signal);
          } catch {
            if (attemptBudget.signal.aborted) throw aborted(attemptBudget.signal);
            throw new NetworkError('Transport failed', { retryable: true });
          }
          if (!response.ok) {
            if (response.status === 410 && path.startsWith('/api/events/sync?') && options.channelId) {
              throw parseGap(await this.readJson(response, attemptBudget.signal), this.config.orgId, options.channelId);
            }
            void response.body?.cancel().catch(() => {});
            const error = statusError(response, value => this.sensitiveRequestId(value), mint);
            if (error instanceof RateLimitError && error.retryAfterMs !== undefined) {
              this.#cooldownUntil = Math.max(this.#cooldownUntil, Date.now() + error.retryAfterMs);
            }
            throw error;
          }
          return { value: await this.readJson(response, attemptBudget.signal), requestedAt };
        } finally { attemptBudget.dispose(); }
      } catch (failure) {
        const error = withIntent(signal.aborted ? aborted(signal) : failure, options);
        if (signal.aborted || error instanceof AbortError || !replaySafe || !error.retryable || attempt >= this.config.retry.maxRetries) throw error;
        const ceiling = Math.min(this.config.retry.maxDelayMs, this.config.retry.initialDelayMs * 2 ** attempt);
        const wait = Math.max(error.retryAfterMs ?? 0, Math.floor(Math.random() * (ceiling + 1)));
        try { await delay(wait, signal); } catch (failure) { throw withIntent(failure, options); }
      }
    }
  }

  private async readJson(response: Response, signal: AbortSignal): Promise<unknown> {
    if (!response.body) throw new ProtocolError('Expected a JSON response body');
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      while (true) {
        let chunk: Awaited<ReturnType<typeof reader.read>>;
        try { chunk = await raceSignal(reader.read(), signal); }
        catch {
          if (signal.aborted) throw aborted(signal);
          throw new NetworkError('Response stream failed', { retryable: true });
        }
        const { value, done } = chunk;
        if (done) break;
        length += value.byteLength;
        if (length > 10 * 1024 * 1024) throw new ProtocolError('Response exceeds the 10 MiB SDK limit');
        chunks.push(value);
      }
      try { return JSON.parse(Buffer.concat(chunks, length).toString('utf8')) as unknown; }
      catch { throw new ProtocolError('Response is not valid JSON'); }
    } finally { void reader.cancel().catch(() => {}); }
  }
}
