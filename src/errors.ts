export interface HumChatErrorOptions {
  status?: number;
  requestId?: string;
  retryable?: boolean;
  channelId?: string;
  operationId?: string;
  messageId?: string;
  idempotencyKey?: string;
  retryAfterMs?: number;
  earliestCursor?: string;
  earliestSequence?: string;
  cause?: unknown;
}

/** Stable categories; server bodies and credential-provider errors are never copied here. */
export class HumChatError extends Error {
  readonly code: string;
  readonly kind: string;
  readonly status?: number;
  readonly requestId?: string;
  readonly retryable: boolean;
  readonly channelId?: string;
  readonly operationId?: string;
  readonly messageId?: string;
  readonly idempotencyKey?: string;
  readonly retryAfterMs?: number;
  readonly earliestCursor?: string;
  readonly earliestSequence?: string;

  constructor(message: string, code = 'HUMCHAT', options: HumChatErrorOptions = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = new.target.name;
    this.kind = this.name;
    this.code = code;
    this.retryable = options.retryable ?? false;
    if (options.status !== undefined) this.status = options.status;
    if (options.requestId !== undefined) this.requestId = options.requestId;
    if (options.channelId !== undefined) this.channelId = options.channelId;
    if (options.operationId !== undefined) this.operationId = options.operationId;
    if (options.messageId !== undefined) this.messageId = options.messageId;
    if (options.idempotencyKey !== undefined) this.idempotencyKey = options.idempotencyKey;
    if (options.retryAfterMs !== undefined) this.retryAfterMs = options.retryAfterMs;
    if (options.earliestCursor !== undefined) this.earliestCursor = options.earliestCursor;
    if (options.earliestSequence !== undefined) this.earliestSequence = options.earliestSequence;
  }
}

export class ConfigurationError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'CONFIGURATION', options); }
}
export class ValidationError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'VALIDATION', options); }
}
export class AuthenticationError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'AUTHENTICATION', options); }
}
export class SessionRevokedError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'SESSION_REVOKED', options); }
}
export class PermissionError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'PERMISSION', options); }
}
export class NotFoundError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'NOT_FOUND', options); }
}
export class ConflictError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'CONFLICT', options); }
}
export class RateLimitError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'RATE_LIMIT', options); }
}
export class NetworkError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'NETWORK', options); }
}
export class TimeoutError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'TIMEOUT', options); }
}
export class AbortError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'ABORT', options); }
}
export class ServerError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'SERVER', options); }
}
export class ProtocolError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'PROTOCOL', options); }
}
export class UnsupportedFeatureError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'UNSUPPORTED_FEATURE', options); }
}
export class BackpressureError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'BACKPRESSURE', options); }
}
export class RecoveryGapError extends HumChatError {
  constructor(message: string, options?: HumChatErrorOptions) { super(message, 'RECOVERY_GAP', options); }
}
/** Authentication, timestamp and schema failures share a generic receiver error. */
export class WebhookVerificationError extends HumChatError {
  constructor() { super('Webhook verification failed', 'WEBHOOK_VERIFICATION'); }
}
