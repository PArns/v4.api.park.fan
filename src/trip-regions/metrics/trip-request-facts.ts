import { HttpException } from "@nestjs/common";
import { findRegion, type RegionConfig } from "../regions";
import type { TicketProduct } from "../tickets";
import { productOf, type OwnedTicket } from "../trip-tickets";
import type { NormalizedPlanQuery } from "../trip-plan.assembly";
import type { TripWhen, WhenQuery } from "../trip-when";
import type { TripPlanDto } from "../dto/trip-region-response.dto";
import type { TripRequestShape, TripRequestTrace } from "./trip-request-trace";
import type { TripRequestMetric } from "./trip-request-metric.entity";

/**
 * The facts a trip-region answer leaves in its metric row, read from the
 * normalized query, the answer and the error: counts, flags and the API's own
 * codes. Pure, so the specs pin every one. Nothing here keeps a date, a park
 * or ticket name, a message or anything else a visitor wrote.
 * docs/frontend/trip-regions.md#what-the-server-records.
 */

/** The routes a metric names. */
export const TRIP_METRIC_ROUTES = ["plan", "when", "best_days"] as const;
export type TripMetricRoute = (typeof TRIP_METRIC_ROUTES)[number];

/** The region of a request whose slug names none: the slug itself is visitor text. */
export const UNKNOWN_REGION = "(unknown)";

/** What an owned ticket's product is, for the counts by kind. */
export const TICKET_KINDS = [
  "pass",
  "evening",
  "reserved_day",
  "one_day",
  "multi_day",
  "unknown",
] as const;
export type TicketKind = (typeof TICKET_KINDS)[number];

/**
 * Why a request was refused or failed. A 400 the services throw is told by
 * its message, which names the parameter at fault ({@link BAD_REQUEST_CODES});
 * a 400 of the query's own validation is `invalid_query`.
 */
export const TRIP_ERROR_CODES = [
  "unknown_region",
  "invalid_query",
  "bad_date",
  "window_in_past",
  "window_reversed",
  "window_past_horizon",
  "window_too_long",
  "range_shorter_than_trip",
  "unknown_park",
  "park_listed_twice",
  "visits_per_park",
  "too_many_visits",
  "visits_exceed_days",
  "bad_hopper",
  "unknown_area",
  "bad_hotel",
  "bad_lock",
  "bad_tickets",
  "bad_within",
  "bad_bases",
  "no_bases_in_region",
  "area_needs_base",
  "stay_nights_exceed_trip",
  "bad_request",
  "forbidden",
  "unavailable",
  "internal",
] as const;
export type TripErrorCode = (typeof TRIP_ERROR_CODES)[number];

/**
 * A service's 400 by its message, first match wins. The messages carry the
 * visitor's own values (a slug, a date), so only the code is kept.
 */
const BAD_REQUEST_CODES: ReadonlyArray<readonly [RegExp, TripErrorCode]> = [
  [/^tickets\b/, "bad_tickets"],
  // Before the area and twice patterns below, which a base's messages share.
  [/^(bases|startBase|endBase):/, "bad_bases"],
  [/ has no bases: /, "no_bases_in_region"],
  [/hotel area goes with its base/, "area_needs_base"],
  [/^Your stays add up to/, "stay_nights_exceed_trip"],
  [/^within:/, "bad_within"],
  [/^hopper:/, "bad_hopper"],
  [/^lock /, "bad_lock"],
  [/is not a date the calendar has/, "bad_date"],
  [/must not be before today/, "window_in_past"],
  [/^to must not be before from/, "window_reversed"],
  [/must not be after/, "window_past_horizon"],
  [/(window|search range) is at most/, "window_too_long"],
  [/search range is shorter than/, "range_shorter_than_trip"],
  [/is not a park of/, "unknown_park"],
  [/is listed twice/, "park_listed_twice"],
  [/^A park takes 1 to/, "visits_per_park"],
  [/at most \d+ park visits/, "too_many_visits"],
  [/^More park visits than/, "visits_exceed_days"],
  [/is not a hotel area of/, "unknown_area"],
  [/^hotel must be/, "bad_hotel"],
];

/** The status and code of what a route threw. */
export function errorOf(error: unknown): {
  status: number;
  code: TripErrorCode;
} {
  if (!(error instanceof HttpException)) {
    return { status: 500, code: "internal" };
  }
  const status = error.getStatus();
  if (status === 404) return { status, code: "unknown_region" };
  if (status === 403) return { status, code: "forbidden" };
  if (status === 503) return { status, code: "unavailable" };
  if (status !== 400) {
    return { status, code: status >= 500 ? "internal" : "bad_request" };
  }
  const body = error.getResponse();
  const message =
    typeof body === "string"
      ? body
      : (body as { message?: unknown } | null)?.message;
  // The query's own validation answers a list of the constraints that failed.
  if (Array.isArray(message)) return { status, code: "invalid_query" };
  const text = typeof message === "string" ? message : error.message;
  const match = BAD_REQUEST_CODES.find(([pattern]) => pattern.test(text));
  return { status, code: match?.[1] ?? "bad_request" };
}

/** A region slug as a metric may keep it: one `regions.ts` has, else {@link UNKNOWN_REGION}. */
export function regionOf(slug: unknown): string {
  return typeof slug === "string" && findRegion(slug) ? slug : UNKNOWN_REGION;
}

/** What an owned ticket is: a pass, an evening ticket, a reserved day, one day or several. */
export function ticketKindOf(
  product: TicketProduct | undefined,
  owned: Pick<OwnedTicket, "days">,
): TicketKind {
  if (!product) return "unknown";
  if (product.pass || owned.days === "unlimited") return "pass";
  if (product.entryFrom) return "evening";
  if (product.reservedDayParks?.length) return "reserved_day";
  return owned.days === 1 ? "one_day" : "multi_day";
}

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

/** The shape of a valid `/plan` query. `parksNamed` comes from the raw query. */
export function planShapeOf(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  parksNamed: boolean,
): TripRequestShape {
  return {
    tripDays: daysBetween(q.from, q.to) + 1,
    parks: q.parks.length,
    parksNamed,
    visits: q.parks.reduce((sum, p) => sum + p.visits, 0),
    locks: q.locks.length,
    ticketKinds: q.tickets.map((t) =>
      ticketKindOf(productOf(region, t.product), t),
    ),
    hopper:
      q.hopperResorts.length > 0 || q.tickets.some((t) => t.hopper === true),
  };
}

/** The shape of a valid `/when` query: its trip's, with the range for the dates. */
export function whenShapeOf(
  region: RegionConfig,
  q: WhenQuery,
  parksNamed: boolean,
): TripRequestShape {
  return {
    ...planShapeOf(region, q.plan, parksNamed),
    tripDays: q.days,
    locks: 0,
    rangeDays: daysBetween(q.from, q.to) + 1,
    within: Boolean(q.within),
  };
}

/** What an answer says about its search, for the row. */
export interface TripAnswerFacts {
  exact: boolean | null;
  relaxed: string[];
  unplacedReasons: string[];
  bases: number | null;
  moves: number | null;
  windows: number | null;
  windowsPlanned: number | null;
}

/** A code of the API's own, as an answer carries it: anything else is not kept. */
const CODE = /^[a-z][a-z0-9_]{0,39}$/;
const codes = (values: unknown): string[] =>
  Array.isArray(values)
    ? values.filter((v): v is string => typeof v === "string" && CODE.test(v))
    : [];
const count = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

/** Facts of a `/plan` answer: its search, what it loosened and left out, its stays. */
export function planAnswerFacts(body: TripPlanDto): TripAnswerFacts {
  const meta = body.meta ?? {};
  const totals = body.totals ?? {};
  const days = Array.isArray(body.days) ? body.days : [];
  const stays = Array.isArray(meta.stays) ? meta.stays : null;
  const withBase = days.some((d) => typeof d.base === "string");
  return {
    exact: typeof meta.exact === "boolean" ? meta.exact : null,
    relaxed: codes(meta.relaxed).sort(),
    unplacedReasons: codes(
      (Array.isArray(body.unplaced) ? body.unplaced : []).map((u) => u.reason),
    ).sort(),
    bases: stays ? stays.length : null,
    moves:
      count(totals.moves) ??
      (stays || withBase ? days.filter((d) => d.move).length : null),
    windows: null,
    windowsPlanned: null,
  };
}

/**
 * Facts of a `/when` answer: whether the search was proven, how much it
 * planned, the windows found, the parks the best window left out and its
 * stays, and a loosened ticket window on any window.
 */
export function whenAnswerFacts(body: TripWhen): TripAnswerFacts {
  const search = body.meta?.search;
  const windows = Array.isArray(body.windows) ? body.windows : [];
  const best = windows[0];
  return {
    exact: typeof search?.exact === "boolean" ? search.exact : null,
    relaxed: windows.some((w) => w.warnings?.includes("ticket_window_relaxed"))
      ? ["ticket_window_relaxed"]
      : [],
    // A window's plan names the parks it left out, not why.
    unplacedReasons: (best?.plan?.unplaced ?? []).map(() => "unknown"),
    // A window names its stays but not its moves.
    bases: Array.isArray(best?.plan?.stays) ? best.plan.stays.length : null,
    moves: null,
    windows: windows.length,
    windowsPlanned: count(search?.planned),
  };
}

/** A metric row before it is written: the entity without its id. */
export type TripRequestMetricRow = Omit<TripRequestMetric, "id">;

/** The UTC hour of an instant: the only time a row keeps. */
export function hourOf(at: Date): Date {
  const hour = new Date(at.getTime());
  hour.setUTCMinutes(0, 0, 0);
  return hour;
}

const EMPTY_FACTS: TripAnswerFacts = {
  exact: null,
  relaxed: [],
  unplacedReasons: [],
  bases: null,
  moves: null,
  windows: null,
  windowsPlanned: null,
};

/** One answer as its row: the route's facts, the trace's caches and shape, the error's code. */
export function metricRowOf(input: {
  route: TripMetricRoute;
  region: string;
  at: Date;
  latencyMs: number;
  trace: TripRequestTrace;
  status: number;
  errorCode?: TripErrorCode | null;
  facts?: TripAnswerFacts;
}): TripRequestMetricRow {
  const { trace } = input;
  const shape = trace.shape ?? {};
  const facts = input.facts ?? EMPTY_FACTS;
  const small = (value: number | null | undefined) =>
    value == null ? null : Math.max(0, Math.min(32767, Math.round(value)));
  return {
    bucket: hourOf(input.at),
    region: input.region,
    route: input.route,
    status: input.status,
    errorCode: input.errorCode ?? null,
    latencyMs: Math.max(0, Math.round(input.latencyMs)),
    // `/best-days` has no cache of its own: the calendar's is the answer's.
    cache:
      (input.route === "best_days" ? trace.calendarCache : trace.cache) ?? null,
    calendarCache: trace.calendarCache ?? null,
    exact: facts.exact,
    relaxed: facts.relaxed,
    unplacedReasons: facts.unplacedReasons,
    tripDays: small(shape.tripDays),
    parks: small(shape.parks),
    parksNamed: shape.parksNamed ?? null,
    visits: small(shape.visits),
    locks: small(shape.locks),
    ticketKinds: shape.ticketKinds ?? [],
    hopper: shape.hopper ?? null,
    bases: small(facts.bases),
    moves: small(facts.moves),
    rangeDays: small(shape.rangeDays),
    windowsPlanned: small(facts.windowsPlanned),
    windows: small(facts.windows),
    within: shape.within ?? null,
  };
}
