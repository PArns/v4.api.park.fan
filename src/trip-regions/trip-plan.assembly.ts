/**
 * The plan route's steps, from a validated query and the region calendar to
 * the answer. Pure: `TripPlanService` feeds it from Redis and the database,
 * the dev shim from production, and both get the same plan. Contract:
 * docs/frontend/trip-regions.md.
 */
import { BadRequestException } from "@nestjs/common";
import { HOTEL_KINDS, type HotelKind, type RegionConfig } from "./regions";
import {
  extraVisitRoom,
  MAX_PARK_DAYS,
  MAX_TRIP_DAYS,
  MAX_VISITS_PER_PARK,
  optimizeTrip,
  type FireworksOption,
  type OptimizedPlan,
  type PlanCell,
  type PlanInput,
  type PlanLock,
  type PlannedDay,
  type RestOption,
} from "./trip-optimizer";
import {
  buildLegs,
  findLeg,
  hopPairs,
  legFromHotel,
  type TripLeg,
} from "./travel";
import {
  planCellOf,
  validateWindow,
  type InternalRegionCalendar,
} from "./trip-region.assembly";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripPlanDto } from "./dto/trip-region-response.dto";

/** The plan's options after validation, in one canonical form. */
export interface NormalizedPlanQuery {
  from: string;
  to: string;
  /** `explicit`: the visitor wrote `slug:n`, so `split=auto` leaves it alone. */
  parks: Array<{ slug: string; visits: number; explicit: boolean }>;
  hotel: HotelKind;
  area?: string;
  arrival: "free" | "park";
  departure: "free" | "park";
  blocks: boolean;
  fireworks: FireworksOption;
  split: "auto" | "fixed";
  rest: RestOption;
  hop: boolean;
  locks: PlanLock[];
}

/** A plan before its answer is written: the optimizer's result and what split added. */
export interface OptimizedTrip {
  plan: OptimizedPlan;
  split: Array<{ slug: string; visits: number; auto: true }>;
}

const flag = (value: string | undefined, fallback: boolean): boolean =>
  value === undefined ? fallback : value === "1" || value === "true";

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

/**
 * Validates the query against the region and today; 400 with the reason
 * otherwise. `known` are the region parks the database has.
 */
export function normalizePlanQuery(
  region: RegionConfig,
  known: string[],
  query: TripPlanQueryDto,
  today: string,
): NormalizedPlanQuery {
  validateWindow(query.from, query.to, MAX_TRIP_DAYS, today);
  const tripDays = daysBetween(query.from, query.to) + 1;
  const knownSet = new Set(known);

  const parks: NormalizedPlanQuery["parks"] = [];
  if (query.parks) {
    for (const part of query.parks.split(",")) {
      const [slug, n] = part.split(":");
      const visits = n ? Number(n) : 1;
      if (!knownSet.has(slug)) {
        throw new BadRequestException(
          `${slug} is not a park of ${region.name}`,
        );
      }
      if (parks.some((p) => p.slug === slug)) {
        throw new BadRequestException(`${slug} is listed twice`);
      }
      if (visits < 1 || visits > MAX_VISITS_PER_PARK) {
        throw new BadRequestException(
          `A park takes 1 to ${MAX_VISITS_PER_PARK} visits`,
        );
      }
      parks.push({ slug, visits, explicit: n !== undefined });
    }
  } else {
    for (const config of region.parks) {
      if (config.headliner && knownSet.has(config.slug)) {
        parks.push({ slug: config.slug, visits: 1, explicit: false });
      }
    }
  }
  parks.sort((a, b) => a.slug.localeCompare(b.slug));

  const hop = flag(query.hop, false);
  const parkDays = parks.reduce((s, p) => s + p.visits, 0);
  if (parkDays > MAX_PARK_DAYS) {
    throw new BadRequestException(
      `A plan takes at most ${MAX_PARK_DAYS} park visits`,
    );
  }
  if (parkDays > tripDays * (hop ? 2 : 1)) {
    throw new BadRequestException(
      hop
        ? "More park visits than two a day"
        : "More park visits than trip days: set hop=1 or choose fewer",
    );
  }

  const area = query.area;
  const areaConfig = area
    ? region.hotelAreas.find((h) => h.key === area)
    : undefined;
  if (area && !areaConfig) {
    throw new BadRequestException(
      `${area} is not a hotel area of ${region.name}`,
    );
  }
  const hotel = (query.hotel ?? areaConfig?.hotel ?? "offsite") as HotelKind;
  if (!HOTEL_KINDS.includes(hotel)) {
    throw new BadRequestException(
      `hotel must be one of ${HOTEL_KINDS.join(", ")}`,
    );
  }

  const rest: RestOption =
    !query.rest || query.rest === "auto"
      ? { mode: "auto" }
      : query.rest === "none"
        ? { mode: "none" }
        : { mode: "max", n: Number(query.rest.slice(4)) };

  const locks: PlanLock[] = [];
  for (const part of query.lock ? query.lock.split(",") : []) {
    const [target, date] = part.split("@");
    if (target !== "rest" && !parks.some((p) => p.slug === target)) {
      throw new BadRequestException(
        `lock ${part}: ${target} is not one of the plan's parks`,
      );
    }
    if (date < query.from || date > query.to) {
      throw new BadRequestException(
        `lock ${part}: the date is outside the trip`,
      );
    }
    locks.push({ target, date });
  }
  locks.sort((a, b) =>
    `${a.date}${a.target}`.localeCompare(`${b.date}${b.target}`),
  );

  return {
    from: query.from,
    to: query.to,
    parks,
    hotel,
    ...(area && { area }),
    arrival: query.arrival === "park" ? "park" : "free",
    departure: query.departure === "park" ? "park" : "free",
    blocks: flag(query.blocks, true),
    fireworks: query.fireworks === "want" ? "want" : "any",
    split: query.split === "fixed" ? "fixed" : "auto",
    rest,
    hop,
    locks,
  };
}

/** The canonical query string a plan is cached by. */
export function planCacheQuery(q: NormalizedPlanQuery): string {
  return [
    q.from,
    q.to,
    q.parks
      .map((p) => `${p.slug}${p.explicit ? `:${p.visits}` : ""}`)
      .join(","),
    q.hotel,
    q.area ?? "-",
    q.arrival,
    q.departure,
    q.blocks ? 1 : 0,
    q.fireworks,
    q.split,
    q.rest.mode === "max" ? `max:${q.rest.n}` : q.rest.mode,
    q.hop ? 1 : 0,
    q.locks.map((l) => `${l.target}@${l.date}`).join(","),
  ].join("|");
}

/** The optimizer's input for a query and a calendar of exactly the trip. */
export function planInput(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  visits: Map<string, number>,
): PlanInput {
  const configs = new Map(region.parks.map((p) => [p.slug, p]));
  const legs = buildLegs(region);
  const slugs = q.parks.map((p) => p.slug);
  const cells: PlanInput["cells"] = {};
  for (const s of slugs) {
    cells[s] = {};
    for (const day of calendar.days) {
      const c = day.parks[s];
      if (!c) continue;
      const cell: PlanCell = planCellOf(c);
      cells[s][day.date] = cell;
    }
  }
  return {
    days: calendar.days.map((d) => ({
      date: d.date,
      weekday: new Date(`${d.date}T12:00:00Z`).getUTCDay(),
      isHoliday: d.isHoliday,
      isSchoolVacation: d.isSchoolVacation,
    })),
    parks: q.parks.map((p) => {
      const config = configs.get(p.slug);
      return {
        slug: p.slug,
        visits: visits.get(p.slug) ?? p.visits,
        group: config?.group ?? "",
        type: config?.type ?? "theme_park",
      };
    }),
    cells,
    options: {
      arrival: q.arrival,
      departure: q.departure,
      blocks: q.blocks,
      blockGroups: region.groups
        .filter((g) => g.kind === "resort")
        .map((g) => g.key),
      fireworks: q.fireworks,
      rest: q.rest,
      hop: q.hop,
      hopPairs: hopPairs(region, legs, slugs),
      perkRules: region.perks[q.hotel],
      locks: q.locks,
    },
  };
}

/**
 * `split=auto`: second visits for parks listed without a count, in the
 * region's `secondVisitRank` order, while the trip has park days to spare
 * after its rest days and the park has a second open day. Explicit counts
 * never change.
 */
export function autoSplit(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
): Map<string, number> {
  const visits = new Map(q.parks.map((p) => [p.slug, p.visits]));
  if (q.split !== "auto") return visits;
  const ranked = region.parks
    .filter((c) => c.secondVisitRank != null)
    .sort((a, b) => (a.secondVisitRank ?? 0) - (b.secondVisitRank ?? 0))
    .filter((c) => q.parks.some((p) => p.slug === c.slug && !p.explicit));
  let room = extraVisitRoom(planInput(region, q, calendar, visits));
  let total = [...visits.values()].reduce((s, v) => s + v, 0);
  for (const config of ranked) {
    if (room <= 0 || total >= MAX_PARK_DAYS) break;
    const openDays = calendar.days.filter(
      (d) => d.parks[config.slug] && d.parks[config.slug].status !== "CLOSED",
    ).length;
    if (openDays < 2) continue;
    visits.set(config.slug, 2);
    room--;
    total++;
  }
  return visits;
}

/** Runs the optimizer for a query on its calendar, second visits included. */
export function optimizePlan(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
): OptimizedTrip {
  const visits = autoSplit(region, q, calendar);
  const plan = optimizeTrip(planInput(region, q, calendar, visits));
  const split = q.parks
    .filter((p) => (visits.get(p.slug) ?? p.visits) !== p.visits)
    .map((p) => ({
      slug: p.slug,
      visits: visits.get(p.slug) as number,
      auto: true as const,
    }));
  return { plan, split };
}

/**
 * The lead days of the planned visits that carry a forecast: the distances
 * whose measured error `expectedError` reads (`leadTimeMae`).
 */
export function plannedLeads(trip: OptimizedTrip, today: string): number[] {
  const leads = new Set<number>();
  for (const day of trip.plan.days) {
    for (const visit of visitsOf(day)) {
      if (visit.basis === "forecast" || visit.basis === "long_range") {
        leads.add(daysBetween(today, day.date));
      }
    }
  }
  return [...leads].sort((a, b) => a - b);
}

/** A day's visits with their figures, on a park day and a hop day alike. */
function visitsOf(day: PlannedDay) {
  if (day.kind === "park") {
    return [{ slug: day.parks[0].slug, basis: day.basis }];
  }
  if (day.kind === "hop") return day.parks;
  return [];
}

/** A leg as a plan day shows it. */
function travelOf(leg: TripLeg | undefined) {
  if (!leg) return undefined;
  return {
    from: leg.from,
    to: leg.to,
    ...(leg.km != null && { km: leg.km }),
    minutes: leg.minutes,
    mode: leg.mode,
    ...(leg.via && { via: leg.via }),
    ...(leg.transferMinutes != null && {
      transferMinutes: leg.transferMinutes,
    }),
  };
}

/** A park day's travel from the hotel area; a hop day's leg between its parks. */
function withTravel(day: PlannedDay, legs: TripLeg[], area?: string) {
  if (day.parks.length === 0) return day;
  const first = day.parks[0].slug;
  const travel = area ? travelOf(legFromHotel(legs, area, first)) : undefined;
  let between;
  if (day.parks.length > 1) {
    const leg = findLeg(legs, first, day.parks[1].slug);
    between = travelOf(leg && { ...leg, from: first, to: day.parks[1].slug });
  }
  return {
    ...day,
    ...(travel && { travel }),
    ...(between && { between }),
  };
}

export interface PlanResponseContext {
  computedAt: string;
  today: string;
  /**
   * The measured error of the served daily forecast at a lead, in minutes per
   * headliner ride (`leadTimeMae`, docs/ml/reader-facing-accuracy.md); null
   * where nothing is measured at that distance.
   */
  leadMae: Map<number, number | null>;
}

/** The plan route's answer. */
export function planResponse(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  _calendar: InternalRegionCalendar,
  trip: OptimizedTrip,
  ctx: PlanResponseContext,
): TripPlanDto {
  const { plan } = trip;
  const legs = buildLegs(region);
  const errors = plan.days.flatMap((day) =>
    visitsOf(day).flatMap((visit) => {
      if (visit.basis !== "forecast" && visit.basis !== "long_range") return [];
      const mae = ctx.leadMae.get(daysBetween(ctx.today, day.date));
      return mae != null ? [mae] : [];
    }),
  );
  const expectedError =
    errors.length > 0
      ? Math.round(errors.reduce((s, e) => s + e, 0) / errors.length)
      : undefined;
  return {
    meta: {
      region: region.slug,
      from: q.from,
      to: q.to,
      computedAt: ctx.computedAt,
      objective: "headliner_wait",
      exact: plan.exact,
      ...(plan.relaxed.length > 0 && { relaxed: plan.relaxed }),
      ...(trip.split.length > 0 && { split: trip.split }),
    },
    days: plan.days.map((day) =>
      withTravel(day, legs, q.area),
    ) as unknown as Array<Record<string, unknown>>,
    parks: plan.parks as unknown as Array<Record<string, unknown>>,
    totals: {
      headlinerWait: plan.totals.headlinerWait,
      averageDayWait: plan.totals.averageDayWait,
      savedMinutesPerRide: plan.totals.savedMinutesPerRide,
      ...(expectedError != null && { expectedError }),
      orderMatters: plan.totals.orderMatters,
      parkDays: plan.totals.parkDays,
      restDays: plan.totals.restDays,
      visits: plan.totals.visits,
    },
    unplaced: plan.unplaced,
    ...(plan.ignoredLocks.length > 0 && { ignoredLocks: plan.ignoredLocks }),
  };
}
