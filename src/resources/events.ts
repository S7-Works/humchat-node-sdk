import { z } from 'zod';
import { UnsupportedFeatureError } from '../errors.js';
import { input, identitySchema, validateChannel } from '../protocol/wire.js';
import { cursorPosition, parsePage } from '../realtime/durable-protocol.js';
import type { EventPage, EventSyncInput } from '../realtime/durable-types.js';
import type { RequestOptions, ResourceContext } from '../types.js';
import type { SdkCapabilities } from '../auth/session.js';
import { budget } from '../http/transport.js';

export class EventsResource {
  constructor(private readonly context: ResourceContext, private readonly capabilities: (options?: RequestOptions) => Promise<SdkCapabilities>, private readonly totalTimeoutMs: number) {}
  async sync(params: EventSyncInput, options: RequestOptions = {}): Promise<EventPage> {
    const value = input(z.object({ channelId: identitySchema(1024), after: z.string().min(1).max(8192),
      through: z.string().min(1).max(8192).optional(), limit: z.number().int().min(1).max(200).optional() }).strict(), params, 'event sync input');
    validateChannel(value.channelId, this.context.orgId);
    cursorPosition(value.after, this.context.orgId, value.channelId);
    if (value.through !== undefined) cursorPosition(value.through, this.context.orgId, value.channelId);
    const lifetime = budget(this.totalTimeoutMs, options.signal ? [options.signal] : []);
    const bounded = { ...options, signal: lifetime.signal };
    try {
      const caps = await this.capabilities(bounded);
      if (!caps.messaging.durableReplay || !caps.messaging.eventProtocolVersions.includes(1)) throw new UnsupportedFeatureError('Server lacks durable event protocol v1');
      const query = new URLSearchParams({ channel_id: value.channelId, after: value.after });
      if (value.through !== undefined) query.set('through', value.through);
      if (value.limit !== undefined) query.set('limit', String(value.limit));
      const raw = await this.context.request(`/api/events/sync?${query}`, { ...bounded, channelId: value.channelId });
      return parsePage(raw, this.context.orgId, value.channelId, value.after, value.through);
    } finally { lifetime.dispose(); }
  }
}
