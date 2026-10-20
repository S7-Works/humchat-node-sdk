import type { DurableEvent } from '../realtime/durable-types.js';

export interface WebhookCreateInput { id: string; url: string; channelIds: string[] }
export type WebhookStatus = 'active' | 'paused' | 'deleted';
export type WebhookDeliveryStatus = 'pending' | 'retry' | 'delivered' | 'dead';
export interface WebhookChannel {
  channelId: string; epoch: string; sequence: string; status: 'active' | 'gap' | 'dead'; lastError: string | null;
}
export interface WebhookSubscription {
  id: string; orgId: string; url: string; status: WebhookStatus; secretVersion: number;
  previousSecretExpiresAt: number | null; createdBy: string; createdAt: number; updatedAt: number; channels: WebhookChannel[];
}
/** signingSecret is returned only once by create/rotate; store it before serving requests. */
export interface WebhookSecretResult { subscription: WebhookSubscription; signingSecret: string }
export interface WebhookDelivery {
  id: string; subscriptionId: string; channelId: string; epoch: string; sequence: string; eventId: string;
  status: WebhookDeliveryStatus; attempts: number; nextAttemptAt: number; createdAt: number; updatedAt: number;
  lastStatus: number | null; lastError: string | null;
}
export interface WebhookDeliveryListInput { status?: WebhookDeliveryStatus; limit?: number; before?: string }
export interface WebhookDeliveryPage { deliveries: WebhookDelivery[]; nextBefore: string | null }
export interface WebhookEnvelope {
  schema: 1; subscriptionId: string; deliveryId: string; orgId: string; channelId: string;
  eventId: string; attempt: number; sentAt: number; event: DurableEvent;
}
export interface VerifyWebhookOptions {
  expectedOrgId?: string; expectedSubscriptionId?: string; expectedChannelId?: string;
  /** Allowed absolute clock skew, in seconds; defaults to 300, maximum 3600. */
  toleranceSeconds?: number;
  /** Current Unix seconds for deterministic tests; defaults to Math.floor(Date.now()/1000). */
  now?: number;
  /** May lower the hard 8 MiB body limit, never raise it. */
  maxBodyBytes?: number;
}
