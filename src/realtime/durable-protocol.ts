import { z } from 'zod';
import { ProtocolError, RecoveryGapError } from '../errors.js';
import { identitySchema, parseState } from '../protocol/wire.js';
import type { DurableEvent, EventPage } from './durable-types.js';

const sequence = z.string().regex(/^(0|[1-9][0-9]{0,18})$/).refine(v => BigInt(v) <= 9_223_372_036_854_775_807n);
const cursorText = z.string().min(1).max(8192).regex(/^[A-Za-z0-9_-]+$/);
const scope = { schema: z.literal(1), org_uuid: identitySchema(), channel_id: identitySchema(1024), epoch: identitySchema() };
const cursorSchema = z.object({ ...scope, sequence });
export interface CursorPosition { epoch: string; sequence: bigint }
function read<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success) throw new ProtocolError('Invalid durable event protocol');
  return result.data;
}
function bound(value: { org_uuid: string; channel_id: string }, org: string, channel: string): void {
  if (value.org_uuid !== org || value.channel_id !== channel) throw new ProtocolError('Durable event scope mismatch');
}
export function cursorPosition(cursor: string, org: string, channel: string): CursorPosition {
  read(cursorText, cursor);
  let raw: unknown;
  try {
    const decoded = Buffer.from(cursor, 'base64url');
    if (decoded.toString('base64url') !== cursor) throw new Error();
    raw = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(decoded));
  } catch { throw new ProtocolError('Invalid durable cursor encoding'); }
  const value = read(cursorSchema, raw); bound(value, org, channel);
  return { epoch: value.epoch, sequence: BigInt(value.sequence) };
}
export function sameStream(a: CursorPosition, b: CursorPosition): void {
  if (a.epoch !== b.epoch) throw new RecoveryGapError('Event stream epoch changed; explicit recovery is required');
}
export function parseEvent(raw: unknown, org: string, channel: string): DurableEvent {
  const value = read(z.object({ ...scope, type: z.literal('event'), event_id: z.string().max(160), actor_id: identitySchema(256),
    sequence, cursor: cursorText, kind: z.enum(['message', 'edit', 'delete', 'reaction']), state: z.unknown() }), raw);
  bound(value, org, channel);
  const position = cursorPosition(value.cursor, org, channel);
  if (position.epoch !== value.epoch || position.sequence !== BigInt(value.sequence) || position.sequence === 0n || value.event_id !== `${value.epoch}:${value.sequence}`) {
    throw new ProtocolError('Durable event identity does not match its cursor');
  }
  const state = parseState(value.state, org, channel);
  if ((value.kind === 'message' && (state.revision !== 1 || state.deleted || state.reactionRevision !== 0)) ||
      (value.kind === 'delete' && !state.deleted) || (value.kind === 'edit' && state.deleted)) {
    throw new ProtocolError('Durable event kind contradicts its state');
  }
  return { schema: 1, orgId: org, channelId: channel, epoch: value.epoch, eventId: value.event_id,
    actorId: value.actor_id, sequence: value.sequence, cursor: value.cursor, kind: value.kind, state };
}
export interface ReadyFrame { cursor: string; earliestCursor: string; epoch: string; sequence: string }
export function parseReady(raw: unknown, org: string, channel: string): ReadyFrame {
  const value = read(z.object({ ...scope, type: z.literal('ready'), cursor: cursorText, sequence, earliest_cursor: cursorText }), raw);
  bound(value, org, channel);
  const head = cursorPosition(value.cursor, org, channel), floor = cursorPosition(value.earliest_cursor, org, channel);
  if (head.epoch !== value.epoch || floor.epoch !== head.epoch || head.sequence !== BigInt(value.sequence) || floor.sequence > head.sequence) {
    throw new ProtocolError('Invalid durable ready watermark');
  }
  return { cursor: value.cursor, earliestCursor: value.earliest_cursor, epoch: value.epoch, sequence: value.sequence };
}
export function parsePage(raw: unknown, org: string, channel: string, after: string, through?: string): EventPage {
  const value = read(z.object({ ...scope, events: z.array(z.unknown()).max(200), after_cursor: cursorText,
    through_cursor: cursorText, through_sequence: sequence, next_cursor: cursorText.nullable() }), raw);
  bound(value, org, channel);
  if (value.after_cursor !== after || (through !== undefined && value.through_cursor !== through)) throw new ProtocolError('Event page does not match requested boundaries');
  let position = cursorPosition(after, org, channel);
  const head = cursorPosition(value.through_cursor, org, channel);
  sameStream(position, head);
  if (head.epoch !== value.epoch || head.sequence !== BigInt(value.through_sequence) || position.sequence > head.sequence) throw new ProtocolError('Invalid event page watermark');
  const events = value.events.map(rawEvent => parseEvent(rawEvent, org, channel));
  for (const event of events) {
    if (event.epoch !== head.epoch || BigInt(event.sequence) !== position.sequence + 1n || BigInt(event.sequence) > head.sequence) throw new ProtocolError('Durable event page is not contiguous');
    position = { epoch: event.epoch, sequence: BigInt(event.sequence) };
  }
  const last = events.at(-1)?.cursor ?? after;
  if ((position.sequence === head.sequence && value.next_cursor !== null) ||
      (position.sequence < head.sequence && (!events.length || value.next_cursor !== last))) throw new ProtocolError('Durable page skips retained events');
  return { schema: 1, orgId: org, channelId: channel, epoch: value.epoch, events,
    afterCursor: after, throughCursor: value.through_cursor, throughSequence: value.through_sequence, nextCursor: value.next_cursor };
}
export function parseGap(raw: unknown, org: string, channel: string): RecoveryGapError {
  const value = read(z.object({ code: z.literal('EVENT_GAP'), org_uuid: identitySchema(), channel_id: identitySchema(1024),
    epoch: identitySchema(), earliest_cursor: cursorText, earliest_sequence: sequence }), raw);
  bound(value, org, channel);
  const floor = cursorPosition(value.earliest_cursor, org, channel);
  if (floor.epoch !== value.epoch || floor.sequence !== BigInt(value.earliest_sequence)) throw new ProtocolError('Invalid recovery gap boundary');
  return new RecoveryGapError('Checkpoint is outside the retained event stream; explicit recovery is required',
    { status: 410, channelId: channel, earliestCursor: value.earliest_cursor, earliestSequence: value.earliest_sequence });
}
