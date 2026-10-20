import { resolveConfig, type HumChatOptions, type ResolvedConfig, type SafeConfig } from './config.js';
import { AbortError, AuthenticationError } from './errors.js';
import { SessionManager, type SdkCapabilities } from './auth/session.js';
import { budget, HttpTransport, requestTimeout, withIntent, type HttpOptions } from './http/transport.js';
import { ChannelsResource } from './resources/channels.js';
import { ConversationsResource } from './resources/conversations.js';
import { MessagesResource } from './resources/messages.js';
import { RealtimeClient } from './realtime/manager.js';
import { EventsResource } from './resources/events.js';
import { OutboxResource } from './resources/outbox.js';
import { WebhooksResource } from './resources/webhooks.js';
import type { Identity, RequestOptions, ResourceContext } from './types.js';

/** Backend HTTP and channel WebSocket preview. Construction performs no network IO. */
export class HumChat {
  readonly channels: ChannelsResource;
  readonly conversations: ConversationsResource;
  readonly messages: MessagesResource;
  readonly realtime: RealtimeClient;
  readonly events: EventsResource;
  readonly outbox: OutboxResource;
  readonly webhooks: WebhooksResource;
  #config: ResolvedConfig;
  #closed = new AbortController();
  #http: HttpTransport;
  #sessions: SessionManager;

  constructor(options: HumChatOptions) {
    this.#config = resolveConfig(options);
    this.#http = new HttpTransport(this.#config);
    this.#sessions = new SessionManager(this.#config, this.#http, this.#closed.signal);
    const context: ResourceContext = {
      orgId: this.#config.orgId,
      connect: options => this.connect(options),
      getActorId: () => this.#sessions.getActorId(),
      request: (path, options) => this.request(path, options),
    };
    this.channels = new ChannelsResource(context);
    this.conversations = new ConversationsResource(context);
    this.messages = new MessagesResource(context);
    this.events = new EventsResource(context, options => this.getCapabilities(options), this.#config.retry.totalTimeoutMs);
    this.realtime = new RealtimeClient(this.#config, this.#sessions, this.messages, this.events);
    this.outbox = new OutboxResource(this.#config.baseUrl, options => this.connect(options), this.messages);
    this.webhooks = new WebhooksResource(context, options => this.getCapabilities(options), this.#config.retry.totalTimeoutMs);
  }

  getConfig(): SafeConfig {
    const { orgId, baseUrl, timeout, retry, realtime, recovery, authentication } = this.#config;
    return { orgId, baseUrl, timeout, retry: { ...retry }, realtime: { ...realtime }, recovery: { ...recovery }, authentication };
  }

  async connect(options: RequestOptions = {}): Promise<Identity> {
    requestTimeout(options, this.#config);
    const operation = budget(Math.min(options.timeout ?? this.#config.retry.totalTimeoutMs, this.#config.retry.totalTimeoutMs), [this.#closed.signal, ...(options.signal ? [options.signal] : [])]);
    try { return structuredClone((await this.#sessions.get(operation.signal)).identity); }
    finally { operation.dispose(); }
  }

  async getCapabilities(options: RequestOptions = {}): Promise<SdkCapabilities> {
    requestTimeout(options, this.#config);
    const operation = budget(Math.min(options.timeout ?? this.#config.retry.totalTimeoutMs, this.#config.retry.totalTimeoutMs), [this.#closed.signal, ...(options.signal ? [options.signal] : [])]);
    try { return structuredClone((await this.#sessions.get(operation.signal)).capabilities); }
    finally { operation.dispose(); }
  }

  /** Cancel this client's work. Does not log out or revoke the caller's shared token. */
  async close(): Promise<void> {
    this.#closed.abort(new AbortError('Client is closed'));
    await this.realtime.close();
  }

  private async request(path: string, options: HttpOptions = {}): Promise<unknown> {
    requestTimeout(options, this.#config);
    const operation = budget(this.#config.retry.totalTimeoutMs, [this.#closed.signal, ...(options.signal ? [options.signal] : [])]);
    try {
      let session = await this.#sessions.get(operation.signal);
      try { return await this.#http.request(path, session.token, options, operation.signal); }
      catch (error) {
        if (!(error instanceof AuthenticationError) || error.status !== 401 || options.authRetry === false) throw error;
        session = await this.#sessions.unauthorized(session, operation.signal);
        return await this.#http.request(path, session.token, options, operation.signal);
      }
    } catch (error) { throw withIntent(error, options); }
    finally { operation.dispose(); }
  }
}
