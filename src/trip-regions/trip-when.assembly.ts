/**
 * The `/when` route's steps from a validated query to the search's input.
 * Pure: `TripWhenService` feeds it from Redis and the database, the dev shim
 * from production, and both get the same answer. Contract:
 * docs/frontend/trip-regions-when.md.
 */
import { BadRequestException } from "@nestjs/common";
import { addIsoDays, isRealIsoDate } from "../common/utils/date.util";
import type { RegionConfig } from "./regions";
import { normalizePlanQuery, planCacheQuery } from "./trip-plan.assembly";
import {
  WHEN_DEFAULT_DAYS,
  WHEN_DEFAULT_GAP,
  WHEN_DEFAULT_LIMIT,
  WHEN_MAX_RANGE_DAYS,
  type WhenQuery,
  type WhenWithin,
} from "./trip-when";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripWhenQueryDto } from "./dto/trip-when.dto";

/** The park whose climate normals stand for the region's months. */
export const CLIMATE_REFERENCE_PARK = "magic-kingdom-park";

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

/**
 * A search range: real dates, from today to a year ahead (today + 365), at
 * most 366 days; `to` defaults to a year after `from`, cut at the last day.
 * 400 otherwise.
 */
export function whenRange(
  fromQuery: string | undefined,
  toQuery: string | undefined,
  today: string,
): { from: string; to: string } {
  const last = addIsoDays(today, WHEN_MAX_RANGE_DAYS - 1);
  const from = fromQuery ?? today;
  const fallback = addIsoDays(from, WHEN_MAX_RANGE_DAYS - 1);
  const to = toQuery ?? (fallback < last ? fallback : last);
  for (const [name, value] of [
    ["from", from],
    ["to", to],
  ] as const) {
    if (!isRealIsoDate(value)) {
      throw new BadRequestException(`${name} is not a date the calendar has`);
    }
  }
  if (from < today) {
    throw new BadRequestException(`from must not be before today (${today})`);
  }
  if (to < from) throw new BadRequestException("to must not be before from");
  if (to > last) throw new BadRequestException(`to must not be after ${last}`);
  if (daysBetween(from, to) + 1 > WHEN_MAX_RANGE_DAYS) {
    throw new BadRequestException(
      `The search range is at most ${WHEN_MAX_RANGE_DAYS} days`,
    );
  }
  return { from, to };
}

/**
 * The plan's query for one window. `parties` is the old name of `fireworks`:
 * `avoid` kept the party nights out, which is `want`; `fireworks` wins.
 */
function planQueryOf(
  query: TripWhenQueryDto,
  from: string,
  to: string,
): TripPlanQueryDto {
  const fireworks =
    query.fireworks ??
    (query.parties === undefined
      ? undefined
      : query.parties === "avoid"
        ? "want"
        : "any");
  return {
    from,
    to,
    parks: query.parks,
    hotel: query.hotel,
    area: query.area,
    arrival: query.arrival,
    departure: query.departure,
    blocks: query.blocks,
    fireworks,
    split: query.split,
    rest: query.rest,
    hop: query.hop,
    // `hop` only counts with a hopper ticket, so the ticket has to travel with it.
    ticket: query.ticket,
    hopper: query.hopper,
    tickets: query.tickets,
  };
}

/**
 * Validates a `/when` query; 400 with the reason otherwise. The trip options
 * go through the plan's own validation and defaults, on the range's first
 * window. `within` is resolved by the caller (`resolveWithin`) and passed in.
 */
export function normalizeWhenQuery(
  region: RegionConfig,
  known: string[],
  query: TripWhenQueryDto,
  today: string,
  within?: WhenWithin,
): WhenQuery {
  const { from, to } = whenRange(query.from, query.to, today);
  const days = query.days ?? WHEN_DEFAULT_DAYS;
  if (daysBetween(from, to) + 1 < days) {
    throw new BadRequestException(
      `The search range is shorter than a ${days}-day trip`,
    );
  }
  // Owned tickets are read against the whole range, and without dates: each
  // window searches their starts.
  const plan = normalizePlanQuery(
    region,
    known,
    planQueryOf(query, from, addIsoDays(from, days - 1)),
    today,
    { from, to, starts: false },
  );
  return {
    from,
    to,
    days,
    limit: query.limit ?? WHEN_DEFAULT_LIMIT,
    gap: query.gap ?? WHEN_DEFAULT_GAP,
    plan,
    suggest: query.suggest === "1" || query.suggest === "true",
    ...(within && { within }),
  };
}

/** The canonical query string a `/when` answer is cached by. */
export function whenCacheQuery(q: WhenQuery): string {
  return [
    q.from,
    q.to,
    q.days,
    q.limit,
    q.gap,
    q.within?.code ?? "-",
    planCacheQuery(q.plan),
    ...(q.suggest ? ["suggest"] : []),
  ].join("|");
}
