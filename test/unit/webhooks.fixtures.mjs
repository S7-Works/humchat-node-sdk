import { createHmac } from 'node:crypto';
export const org = 'webhook-org', channel = org + ':team:general', actor = 'webhook-actor';
export const subscriptionId = '11111111-1111-4111-8111-111111111111';
export const deliveryId = '22222222-2222-4222-8222-222222222222';
export const secret = 'whsec_' + '11'.repeat(32), previousSecret = 'whsec_' + '22'.repeat(32);
export const timestamp = 1_792_370_346, epoch = '33333333-3333-4333-8333-333333333333';
export function envelope(overrides = {}) {
  const cursor = Buffer.from(JSON.stringify({ schema: 1, org_uuid: org, channel_id: channel, epoch, sequence: '1' })).toString('base64url');
  return { schema: 1, subscription_id: subscriptionId, delivery_id: deliveryId, org_uuid: org, channel_id: channel,
    event_id: epoch + ':1', attempt: 1, sent_at: timestamp, event: { type: 'event', schema: 1, org_uuid: org, channel_id: channel,
      epoch, sequence: '1', event_id: epoch + ':1', cursor, actor_id: actor, kind: 'message', state: { schema: 1, org_uuid: org,
        channel_id: channel, message_id: 'webhook-message', sender_id: actor, created_at: timestamp, thread_root_id: null,
        priority: 'standard', original_body: 'Hello webhook', body: 'Hello webhook', revision: 1, updated_at: timestamp,
        deleted: false, reaction_revision: 0, reactions: {}, attachments: [] } }, ...overrides };
}
export function signature(raw, keys = [secret], sentAt = timestamp) {
  return 't=' + sentAt + keys.map(key => ',v1=' + createHmac('sha256', Buffer.from(key.slice(6), 'hex')).update(String(sentAt) + '.').update(raw).digest('hex')).join('');
}
export function subscription(overrides = {}) {
  return { id: subscriptionId, org_uuid: org, url: 'https://receiver.example.invalid/humchat', status: 'active', secret_version: 1,
    previous_secret_expires_at: null, created_by: actor, created_at: timestamp, updated_at: timestamp,
    channels: [{ channel_id: channel, epoch, sequence: '1', status: 'active', last_error: null }], ...overrides };
}
export function delivery(overrides = {}) {
  return { id: deliveryId, subscription_id: subscriptionId, channel_id: channel, epoch, sequence: '1', event_id: epoch + ':1',
    status: 'dead', attempts: 8, next_attempt_at: timestamp, created_at: timestamp, updated_at: timestamp,
    last_status: 503, last_error: 'HTTP_REJECTED', ...overrides };
}
export const capabilities = { sdk_protocol_version: 1, authentication: { user_sessions: true, service_identities: false, api_key_exchange: false,
  app_binding_required: false, mock_authentication: true }, messaging: { send_receipt_versions: [1], message_state_versions: [1], history_versions: [2],
  durable_replay: true, event_protocol_versions: [1], websocket_scope: 'channel',
  outgoing_webhooks: { schema_versions: [1], max_channels: 50, workspace_channels_only: true } } };
