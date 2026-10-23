import { z } from 'zod';
import { ProtocolError } from '../errors.js';
import { identitySchema } from '../protocol/wire.js';
import type { OrgMember, RequestOptions, ResourceContext } from '../types.js';

/** The authenticated organization's roster; visibility is enforced by the server. */
export class MembersResource {
  constructor(private readonly context: ResourceContext) {}
  async list(options: RequestOptions = {}): Promise<OrgMember[]> {
    const result = z.array(z.object({ user_uuid: identitySchema(256), name: z.string().max(4096),
      email: z.string().max(4096), role: z.string().min(1).max(256), status: z.string().min(1).max(256),
      org_uuid: identitySchema().optional() }).passthrough()).max(100_000)
      .safeParse(await this.context.request('/api/org/members', options));
    if (!result.success) throw new ProtocolError('Invalid organization members response');
    const seen = new Set<string>();
    return result.data.map(member => {
      if (seen.has(member.user_uuid) || (member.org_uuid !== undefined && member.org_uuid !== this.context.orgId)) {
        throw new ProtocolError('Organization member identity or scope mismatch');
      }
      seen.add(member.user_uuid);
      return { actorId: member.user_uuid, name: member.name, email: member.email, role: member.role, status: member.status };
    });
  }
}
