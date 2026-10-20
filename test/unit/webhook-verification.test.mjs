import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { verifyWebhook, WebhookVerificationError } from '../../dist/index.js';
import { org, channel, subscriptionId, deliveryId, secret, previousSecret, timestamp, envelope, signature } from './webhooks.fixtures.mjs';

const options = { now: timestamp, expectedOrgId: org, expectedChannelId: channel, expectedSubscriptionId: subscriptionId };
function invalid(work) {
  assert.throws(work, error => {
    assert.ok(error instanceof WebhookVerificationError); assert.equal(error.code, 'WEBHOOK_VERIFICATION');
    assert.equal(error.message, 'Webhook verification failed'); assert.equal(error.cause, undefined);
    assert.ok(!String(error.stack).includes(secret)); return true;
  });
}
test('webhook verification authenticates exact bytes and returns normalized, scoped durable data', () => {
  const value = envelope(); value.event.state.body = value.event.state.original_body = 'Unicode webhook: café 🌍';
  const raw = JSON.stringify(value), header = signature(raw);
  for (const body of [raw, Buffer.from(raw), new Uint8Array(Buffer.from(raw))]) {
    const verified = verifyWebhook(body, header, secret, options);
    assert.equal(verified.subscriptionId, subscriptionId); assert.equal(verified.deliveryId, deliveryId);
    assert.equal(verified.orgId, org); assert.equal(verified.channelId, channel); assert.equal(verified.sentAt, timestamp);
    assert.equal(verified.event.state.body, value.event.state.body); assert.equal(verified.event.state.id, 'webhook-message');
    assert.equal(verified.event.eventId, verified.eventId); assert.equal(verified.event.sequence, '1');
  }
  const padded = Buffer.concat([Buffer.from('padding'), Buffer.from(raw), Buffer.from('tail')]);
  assert.equal(verifyWebhook(padded.subarray(7, padded.length - 4), header, secret, options).eventId, value.event_id);
});

test('rotation verifies either of two signatures with either accepted current/previous key', () => {
  const raw = JSON.stringify(envelope()), dual = signature(raw, [secret, previousSecret]);
  for (const keys of [secret, previousSecret, [secret, previousSecret], [previousSecret, secret]]) assert.equal(verifyWebhook(raw, dual, keys, options).attempt, 1);
  assert.equal(verifyWebhook(raw, signature(raw, [previousSecret]), [secret, previousSecret], options).attempt, 1);
  invalid(() => verifyWebhook(raw, signature(raw, ['whsec_' + '33'.repeat(32)]), [secret, previousSecret], options));
  const literalKey = 't=' + timestamp + ',v1=' + createHmac('sha256', secret).update(timestamp + '.' + raw).digest('hex');
  invalid(() => verifyWebhook(raw, literalKey, secret, options));
});

test('raw-body tampering, normalization and invalid UTF-8 fail before becoming events', () => {
  const raw = JSON.stringify(envelope()), header = signature(raw);
  for (const body of [raw + ' ', raw.replace('Hello', 'Changed'), JSON.stringify(envelope(), null, 2)]) invalid(() => verifyWebhook(body, header, secret, options));
  for (const body of [Buffer.from([0xff, 0xfe]), '{bad json', 'null', '[]']) invalid(() => verifyWebhook(body, signature(body), secret, options));
  invalid(() => verifyWebhook(envelope(), header, secret, options));
});

test('timestamp tolerance is symmetric and strict at the configured boundary', () => {
  for (const delta of [-300, 300]) {
    const raw = JSON.stringify(envelope({ sent_at: timestamp + delta }));
    assert.equal(verifyWebhook(raw, signature(raw, [secret], timestamp + delta), secret, options).sentAt, timestamp + delta);
  }
  for (const delta of [-301, 301]) {
    const raw = JSON.stringify(envelope({ sent_at: timestamp + delta }));
    invalid(() => verifyWebhook(raw, signature(raw, [secret], timestamp + delta), secret, options));
  }
  const raw = JSON.stringify(envelope());
  assert.equal(verifyWebhook(raw, signature(raw), secret, { ...options, toleranceSeconds: 0 }).attempt, 1);
  invalid(() => verifyWebhook(raw, signature(raw), secret, { ...options, now: timestamp + 1, toleranceSeconds: 0 }));
});

test('duplicate timestamps, malformed or excessive signatures, invalid secrets and options fail generically', () => {
  const raw = JSON.stringify(envelope()), header = signature(raw), digest = header.split('v1=')[1];
  for (const invalidHeader of [undefined, '', header + ',t=' + timestamp, header + ',v1=' + digest + ',v1=' + digest,
    't=0' + timestamp + ',v1=' + digest, 't=+123,v1=' + digest, 't=NaN,v1=' + digest,
    't=' + timestamp + ',v1=' + digest.toUpperCase(), 't=' + timestamp + ',v1=' + digest.slice(1),
    't=' + timestamp + ',v2=' + digest, header + ',other=value', ' ' + header]) invalid(() => verifyWebhook(raw, invalidHeader, secret, options));
  for (const keys of ['', 'whsec_' + 'GG'.repeat(32), 'whsec_' + '11'.repeat(31), [], [secret, secret], [secret, previousSecret, secret], {}]) invalid(() => verifyWebhook(raw, header, keys, options));
  for (const extra of [{ now: new Date() }, { now: Infinity }, { now: '1792370346' }, { now: -1 }, { toleranceSeconds: -1 }, { toleranceSeconds: 3601 },
    { maxBodyBytes: 0 }, { maxBodyBytes: 8 * 1024 * 1024 + 1 }, { unknown: true }]) invalid(() => verifyWebhook(raw, header, secret, { ...options, ...extra }));
});

test('body size limits are measured in bytes before allocation/parsing', () => {
  const raw = JSON.stringify(envelope()), header = signature(raw), bytes = Buffer.byteLength(raw);
  assert.equal(verifyWebhook(raw, header, secret, { ...options, maxBodyBytes: bytes }).attempt, 1);
  invalid(() => verifyWebhook(raw, header, secret, { ...options, maxBodyBytes: bytes - 1 }));
  invalid(() => verifyWebhook(Buffer.alloc(8 * 1024 * 1024 + 1), header, secret, options));
  invalid(() => verifyWebhook('', header, secret, options));
});

test('valid signatures cannot cross expected installation scope or contradict their nested event', () => {
  const raw = JSON.stringify(envelope()), header = signature(raw);
  for (const extra of [{ expectedOrgId: 'other-org' }, { expectedSubscriptionId: deliveryId }, { expectedChannelId: org + ':team:other' }]) invalid(() => verifyWebhook(raw, header, secret, { ...options, ...extra }));
  const corruptions = [value => { value.schema = 2; }, value => { value.attempt = 0; }, value => { value.attempt = 9; },
    value => { value.sent_at++; }, value => { value.subscription_id = 'not-a-uuid'; }, value => { value.delivery_id = 'not-a-uuid'; },
    value => { value.org_uuid = 'other-org'; }, value => { value.event_id = 'other:1'; }, value => { value.event.org_uuid = 'other-org'; },
    value => { value.event.channel_id = org + ':team:other'; }, value => { value.event.state.channel_id = org + ':team:other'; },
    value => { value.event.sequence = '2'; }, value => { value.event.state.revision = 2; }];
  for (const corrupt of corruptions) { const value = envelope(); corrupt(value); const body = JSON.stringify(value); invalid(() => verifyWebhook(body, signature(body), secret, { now: timestamp })); }
});
