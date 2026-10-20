import { z } from 'zod';
import type { SdkCapabilities } from '../auth/session.js';
import { ProtocolError, UnsupportedFeatureError, ValidationError } from '../errors.js';
import { budget, type HttpOptions } from '../http/transport.js';
import { identitySchema, input, safeInteger, validateChannel } from '../protocol/wire.js';
import type { RequestOptions, ResourceContext } from '../types.js';
import { webhookSecret, webhookUuid } from '../webhooks/verify.js';
import type { WebhookCreateInput, WebhookDelivery, WebhookDeliveryListInput, WebhookDeliveryPage,
  WebhookSecretResult, WebhookSubscription } from '../webhooks/types.js';

const time = safeInteger.min(0).max(99_999_999_999);
const sequence = z.string().regex(/^(0|[1-9][0-9]{0,18})$/).refine(value => BigInt(value) <= 9_223_372_036_854_775_807n);
const status = z.enum(['pending', 'retry', 'delivered', 'dead']);
const lastError = z.string().max(1024).nullable();
const urlSchema = z.string().min(1).max(2048).refine(value => {
  if (Buffer.byteLength(value, 'utf8') > 2048 || /[\x00-\x20\x7f]/.test(value)) return false;
  try { const url = new URL(value); return ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.hash; }
  catch { return false; }
});
const subscriptionSchema = z.object({ id: webhookUuid, org_uuid: identitySchema(), url: urlSchema,
  status: z.enum(['active', 'paused', 'deleted']), secret_version: safeInteger.min(1), previous_secret_expires_at: time.nullable(),
  created_by: identitySchema(256), created_at: time, updated_at: time,
  channels: z.array(z.object({ channel_id: identitySchema(1024), epoch: webhookUuid, sequence,
    status: z.enum(['active', 'gap', 'dead']), last_error: lastError })).min(1).max(50) });
const deliverySchema = z.object({ id: webhookUuid, subscription_id: webhookUuid, channel_id: identitySchema(1024),
  epoch: webhookUuid, sequence, event_id: z.string().min(1).max(160), status, attempts: safeInteger.min(0).max(8),
  next_attempt_at: time, created_at: time, updated_at: time, last_status: safeInteger.min(100).max(599).nullable(), last_error: lastError });
function read<T>(schema: z.ZodType<T>, raw: unknown): T {
  const parsed = schema.safeParse(raw); if (!parsed.success) throw new ProtocolError('Invalid webhook management response'); return parsed.data;
}

/** Administrative workspace grants. Mutations never automatically repeat, including on 401. */
export class WebhooksResource {
  constructor(private readonly context: ResourceContext, private readonly capabilities: (options?: RequestOptions) => Promise<SdkCapabilities>, private readonly totalTimeoutMs: number) {}
  private async request(path: string, options: HttpOptions, channels?: number): Promise<unknown> {
    const lifetime = budget(this.totalTimeoutMs, options.signal ? [options.signal] : []);
    try {
      const cap = (await this.capabilities({ ...options, signal: lifetime.signal })).messaging.outgoingWebhooks;
      if (!cap?.schemaVersions.includes(1) || !cap.workspaceChannelsOnly) throw new UnsupportedFeatureError('Server does not advertise outgoing webhook schema v1');
      if (channels !== undefined && channels > cap.maxChannels) throw new ValidationError('Webhook channel grant exceeds the server limit');
      return await this.context.request(path, { ...options, signal: lifetime.signal });
    } finally { lifetime.dispose(); }
  }
  private path(id: string): string { return '/api/webhooks/' + input(webhookUuid, id, 'webhook id'); }
  private subscription(raw: unknown, expectedId?: string): WebhookSubscription {
    const value = read(subscriptionSchema, raw);
    if (value.org_uuid !== this.context.orgId || (expectedId !== undefined && value.id !== expectedId) ||
      new Set(value.channels.map(channel => channel.channel_id)).size !== value.channels.length) throw new ProtocolError('Webhook subscription identity mismatch');
    for (const channel of value.channels) if (!channel.channel_id.startsWith(this.context.orgId + ':')) throw new ProtocolError('Webhook grant organization mismatch');
    return { id: value.id, orgId: value.org_uuid, url: value.url, status: value.status, secretVersion: value.secret_version,
      previousSecretExpiresAt: value.previous_secret_expires_at, createdBy: value.created_by, createdAt: value.created_at, updatedAt: value.updated_at,
      channels: value.channels.map(channel => ({ channelId: channel.channel_id, epoch: channel.epoch, sequence: channel.sequence, status: channel.status, lastError: channel.last_error })) };
  }
  private result(raw: unknown, expectedId: string): WebhookSubscription {
    const value = read(z.object({ schema: z.literal(1), subscription: z.unknown() }), raw);
    return this.subscription(value.subscription, expectedId);
  }
  private secretResult(raw: unknown, expectedId: string): WebhookSecretResult {
    const value = read(z.object({ schema: z.literal(1), subscription: z.unknown(), signing_secret: webhookSecret }), raw);
    return { subscription: this.subscription(value.subscription, expectedId), signingSecret: value.signing_secret };
  }
  private delivery(raw: unknown, subscriptionId: string, deliveryId?: string): WebhookDelivery {
    const value = read(deliverySchema, raw);
    if (value.subscription_id !== subscriptionId || (deliveryId !== undefined && value.id !== deliveryId) ||
      !value.channel_id.startsWith(this.context.orgId + ':') || value.sequence === '0' || value.event_id !== value.epoch + ':' + value.sequence) throw new ProtocolError('Webhook delivery identity mismatch');
    return { id: value.id, subscriptionId: value.subscription_id, channelId: value.channel_id, epoch: value.epoch, sequence: value.sequence,
      eventId: value.event_id, status: value.status, attempts: value.attempts, nextAttemptAt: value.next_attempt_at, createdAt: value.created_at,
      updatedAt: value.updated_at, lastStatus: value.last_status, lastError: value.last_error };
  }
  async create(params: WebhookCreateInput, options: RequestOptions = {}): Promise<WebhookSecretResult> {
    const value = input(z.object({ id: webhookUuid, url: urlSchema, channelIds: z.array(identitySchema(1024)).min(1).max(50) }).strict(), params, 'webhook create input');
    if (new Set(value.channelIds).size !== value.channelIds.length) throw new ValidationError('Webhook channels must be distinct');
    for (const channel of value.channelIds) validateChannel(channel, this.context.orgId);
    const raw = await this.request('/api/webhooks', { ...options, method: 'POST', replaySafe: false, authRetry: false,
      body: { id: value.id, url: value.url, channel_ids: value.channelIds } }, value.channelIds.length);
    const result = this.secretResult(raw, value.id);
    if (new URL(result.subscription.url).href !== new URL(value.url).href || result.subscription.status !== 'paused' ||
      JSON.stringify(result.subscription.channels.map(channel => channel.channelId).sort()) !== JSON.stringify([...value.channelIds].sort())) {
      throw new ProtocolError('Created webhook does not match the requested grant');
    }
    return result;
  }
  async list(options: RequestOptions = {}): Promise<WebhookSubscription[]> {
    const raw = await this.request('/api/webhooks', options);
    const result = read(z.object({ schema: z.literal(1), org_uuid: identitySchema(), subscriptions: z.array(z.unknown()).max(100) }), raw);
    if (result.org_uuid !== this.context.orgId) throw new ProtocolError('Webhook list organization mismatch');
    const subscriptions = result.subscriptions.map(value => this.subscription(value));
    if (new Set(subscriptions.map(value => value.id)).size !== subscriptions.length) throw new ProtocolError('Duplicate webhook subscription');
    return subscriptions;
  }
  async get(id: string, options: RequestOptions = {}): Promise<WebhookSubscription> { return this.result(await this.request(this.path(id), options), id); }
  async pause(id: string, options: RequestOptions = {}): Promise<WebhookSubscription> { return this.change(id, 'pause', options); }
  async resume(id: string, options: RequestOptions = {}): Promise<WebhookSubscription> { return this.change(id, 'resume', options); }
  private async change(id: string, action: 'pause' | 'resume', options: RequestOptions): Promise<WebhookSubscription> {
    const result = this.result(await this.request(this.path(id) + '/' + action, { ...options, method: 'POST', replaySafe: false, authRetry: false }), id);
    if (result.status !== (action === 'pause' ? 'paused' : 'active')) throw new ProtocolError('Webhook status does not match the requested action');
    return result;
  }
  async rotateSecret(id: string, options: RequestOptions = {}): Promise<WebhookSecretResult> {
    return this.secretResult(await this.request(this.path(id) + '/rotate-secret', { ...options, method: 'POST', replaySafe: false, authRetry: false }), id);
  }
  async delete(id: string, options: RequestOptions = {}): Promise<{ id: string; deleted: true }> {
    const result = read(z.object({ schema: z.literal(1), id: webhookUuid, deleted: z.literal(true) }),
      await this.request(this.path(id), { ...options, method: 'DELETE', replaySafe: false, authRetry: false }));
    if (result.id !== id) throw new ProtocolError('Deleted webhook identity mismatch');
    return { id, deleted: true };
  }
  async deliveries(id: string, params: WebhookDeliveryListInput = {}, options: RequestOptions = {}): Promise<WebhookDeliveryPage> {
    const path = this.path(id);
    const value = input(z.object({ status: status.optional(), limit: safeInteger.min(1).max(100).optional(), before: webhookUuid.optional() }).strict(), params, 'webhook delivery list input');
    const query = new URLSearchParams();
    if (value.status !== undefined) query.set('status', value.status);
    if (value.limit !== undefined) query.set('limit', String(value.limit));
    if (value.before !== undefined) query.set('before', value.before);
    const raw = await this.request(path + '/deliveries' + (query.size ? '?' + query : ''), options);
    const result = read(z.object({ schema: z.literal(1), org_uuid: identitySchema(), deliveries: z.array(z.unknown()).max(100), next_before: webhookUuid.nullable() }), raw);
    if (result.org_uuid !== this.context.orgId) throw new ProtocolError('Webhook deliveries organization mismatch');
    const deliveries = result.deliveries.map(delivery => this.delivery(delivery, id));
    if (new Set(deliveries.map(delivery => delivery.id)).size !== deliveries.length || (value.limit !== undefined && deliveries.length > value.limit) ||
      deliveries.some(delivery => value.status !== undefined && delivery.status !== value.status) ||
      (result.next_before !== null && result.next_before !== deliveries.at(-1)?.id)) throw new ProtocolError('Invalid webhook delivery page');
    return { deliveries, nextBefore: result.next_before };
  }
  async replay(id: string, deliveryId: string, options: RequestOptions = {}): Promise<WebhookDelivery> {
    const path = this.path(id) + '/deliveries/' + input(webhookUuid, deliveryId, 'webhook delivery id') + '/replay';
    const raw = await this.request(path, { ...options, method: 'POST', replaySafe: false, authRetry: false });
    const result = read(z.object({ schema: z.literal(1), delivery: z.unknown() }), raw);
    return this.delivery(result.delivery, id, deliveryId);
  }
}
