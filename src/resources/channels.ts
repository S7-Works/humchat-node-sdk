import { z } from 'zod';
import { identitySchema, input, parseChannels } from '../protocol/wire.js';
import type { ChannelList, ChannelListInput, RequestOptions, ResourceContext } from '../types.js';

export class ChannelsResource {
  constructor(private readonly context: ResourceContext) {}

  async list(params: ChannelListInput = {}, options: RequestOptions = {}): Promise<ChannelList> {
    const value = input(z.object({ teamId: identitySchema().optional() }).strict(), params, 'channel list input');
    const query = value.teamId === undefined ? '' : `?${new URLSearchParams({ team_uuid: value.teamId })}`;
    const response = await this.context.request(`/api/workspace/channels${query}`, options);
    return parseChannels(response, this.context.orgId, value.teamId);
  }
}
