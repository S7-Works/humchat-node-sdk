import { ValidationError } from '../errors.js';
import type { Identity, RequestOptions, SendMessageInput, SendReceipt } from '../types.js';
import type { OutboxStore, PersistenceScope } from '../persistence/types.js';
import type { MessagesResource } from './messages.js';

export interface OutboxSendInput { consumerId: string; key: string; store: OutboxStore; message: SendMessageInput; idempotencyKey?: string }
export interface OutboxFlushInput { consumerId: string; channelId: string; store: OutboxStore }
/** Persist exact send intent before network work; application checkpoints follow confirmed delivery. */
export class OutboxResource {
  constructor(private readonly origin: string, private readonly connect: (options?: RequestOptions) => Promise<Identity>, private readonly messages: MessagesResource) {}
  private async scope(consumerId: string, channelId: string, options: RequestOptions): Promise<PersistenceScope> {
    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(consumerId)) throw new ValidationError('Invalid consumerId');
    const identity = await this.connect(options);
    return { serverOrigin: this.origin, orgId: identity.orgId, actorId: identity.actorId, consumerId, channelId };
  }
  async send(input: OutboxSendInput, options: RequestOptions = {}): Promise<SendReceipt> {
    const scope = await this.scope(input.consumerId, input.message.channelId, options);
    const entry = await input.store.prepare(scope, input.key, input.message,
      input.idempotencyKey === undefined ? {} : { idempotencyKey: input.idempotencyKey });
    if (entry.status === 'sent' && entry.receipt) return structuredClone(entry.receipt);
    const receipt = await this.messages.send(entry.intent, { ...options, idempotencyKey: entry.idempotencyKey });
    await input.store.markSent(scope, input.key, receipt);
    return receipt;
  }
  async flush(input: OutboxFlushInput, options: RequestOptions = {}): Promise<SendReceipt[]> {
    const scope = await this.scope(input.consumerId, input.channelId, options);
    const results: SendReceipt[] = [];
    for (const entry of await input.store.listPending(scope)) {
      const receipt = await this.messages.send(entry.intent, { ...options, idempotencyKey: entry.idempotencyKey });
      await input.store.markSent(scope, entry.key, receipt); results.push(receipt);
    }
    return results;
  }
}
