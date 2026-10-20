import { createHmac, timingSafeEqual } from 'node:crypto';
import { z } from 'zod';
import { WebhookVerificationError } from '../errors.js';
import { identitySchema, safeInteger } from '../protocol/wire.js';
import { parseEvent } from '../realtime/durable-protocol.js';
import type { VerifyWebhookOptions, WebhookEnvelope } from './types.js';

export const webhookUuid = z.string().regex(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
export const webhookSecret = z.string().regex(/^whsec_[0-9a-f]{64}$/);
const time = safeInteger.min(0).max(99_999_999_999);
const optionsSchema = z.object({ expectedOrgId: identitySchema().optional(), expectedSubscriptionId: webhookUuid.optional(),
  expectedChannelId: identitySchema(1024).optional(), toleranceSeconds: safeInteger.min(0).max(3600).optional(),
  now: time.optional(), maxBodyBytes: safeInteger.min(1).max(8 * 1024 * 1024).optional() }).strict();
const envelopeSchema = z.object({ schema: z.literal(1), subscription_id: webhookUuid, delivery_id: webhookUuid,
  org_uuid: identitySchema(), channel_id: identitySchema(1024), event_id: z.string().min(1).max(160),
  attempt: safeInteger.min(1).max(8), sent_at: time, event: z.unknown() });

/** Authenticate the exact received bytes before parsing; never pass a JSON-parsed object. */
export function verifyWebhook(rawBody: Uint8Array | string, signatureHeader: string, secrets: string | readonly string[], options: VerifyWebhookOptions = {}): WebhookEnvelope {
  try {
    const settings = optionsSchema.parse(options);
    const keys = z.array(webhookSecret).min(1).max(2).parse(typeof secrets === 'string' ? [secrets] : secrets);
    if (new Set(keys).size !== keys.length || typeof signatureHeader !== 'string' || signatureHeader.length > 256) throw new Error();
    const signature = /^t=(0|[1-9][0-9]{0,10}),v1=([0-9a-f]{64})(?:,v1=([0-9a-f]{64}))?$/.exec(signatureHeader);
    if (!signature) throw new Error();
    const timestamp = Number(signature[1]), now = settings.now ?? Math.floor(Date.now() / 1000);
    if (!Number.isSafeInteger(now) || now < 0 || now > 99_999_999_999 || Math.abs(now - timestamp) > (settings.toleranceSeconds ?? 300)) throw new Error();
    if (!(typeof rawBody === 'string' || rawBody instanceof Uint8Array)) throw new Error();
    const bytes = typeof rawBody === 'string' ? Buffer.byteLength(rawBody, 'utf8') : rawBody.byteLength;
    if (bytes < 1 || bytes > (settings.maxBodyBytes ?? 8 * 1024 * 1024)) throw new Error();
    const body = Buffer.from(rawBody); // Own the bytes used by both HMAC and parsing.
    const signatures = [signature[2]!, ...(signature[3] ? [signature[3]] : [])].map(value => Buffer.from(value, 'hex'));
    let matched = 0;
    for (const key of keys) {
      const digest = createHmac('sha256', Buffer.from(key.slice(6), 'hex')).update(signature[1]! + '.', 'utf8').update(body).digest();
      for (const candidate of signatures) matched |= Number(timingSafeEqual(digest, candidate));
    }
    if (!matched) throw new Error();
    const envelope = envelopeSchema.parse(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(body)) as unknown);
    if (envelope.sent_at !== timestamp || !envelope.channel_id.startsWith(envelope.org_uuid + ':') ||
      (settings.expectedOrgId !== undefined && settings.expectedOrgId !== envelope.org_uuid) ||
      (settings.expectedSubscriptionId !== undefined && settings.expectedSubscriptionId !== envelope.subscription_id) ||
      (settings.expectedChannelId !== undefined && settings.expectedChannelId !== envelope.channel_id)) throw new Error();
    const event = parseEvent(envelope.event, envelope.org_uuid, envelope.channel_id);
    if (event.eventId !== envelope.event_id) throw new Error();
    return { schema: 1, subscriptionId: envelope.subscription_id, deliveryId: envelope.delivery_id, orgId: envelope.org_uuid,
      channelId: envelope.channel_id, eventId: envelope.event_id, attempt: envelope.attempt, sentAt: envelope.sent_at, event };
  } catch { throw new WebhookVerificationError(); }
}
