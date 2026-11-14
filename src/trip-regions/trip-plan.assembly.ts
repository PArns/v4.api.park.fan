/**
 * The plan route's steps, from a validated query and the region calendar to
 * the answer. Pure: `TripPlanService` feeds it from Redis and the database,
 * the dev shim from production, and both get the same plan. Contract:
 * docs/frontend/trip-regions.md.
 */
import { BadRequestException } from "@nestjs/common";
import {
  hotelKinds,
  stayRulesOf,
  type HotelKind,
  type RegionConfig,
} from "./regions";
import type { StaysOption } from "./trip-stays";
import { minuteOfClock } from "./tickets";
import { isoAt } from "./trip-evening";
import {
  extraVisitRoom,
  ORDER_MATTERS_MINUTES,
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
  type UnplacedReason,
} from "./trip-optimizer";
import {
  buildLegs,
  doorToDoorMinutes,
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
import {
  planEvenings,
  type EveningShows,
  type TicketKind,
} from "./trip-evening";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripPlanDto } from "./dto/trip-region-response.dto";
import {
  canonicalTickets,
  ownedHops,
  ownedHopSets,
  ownedVerdict,
  parseTickets,
  planTicketsOf,
  productOf,
  suggestTickets,
  ticketSummaries,
  type DayTicket,
  type HopperVerdict,
  type OwnedTicket,
  type ReplanChange,
  type SuggestionInput,
  type TicketSuggestion,
} from "./trip-tickets";
import { parksOf } from "./tickets";
import { catalogVersionOf } from "./ticket-catalog";

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
  ticket: TicketKind;
  /** `hop=1` with a hopper ticket; without one it is dropped with a warning. */
  hop: boolean;
  evenings: boolean;
  locks: PlanLock[];
  /** `hop_needs_hopper`: `hop=1` came without a hopper and was ignored. */
  warnings: Array<"hop_needs_hopper">;
  /** The visitor's own tickets (`tickets=`); empty: plan first throughout. */
  tickets: OwnedTicket[];
  /** Plan first's resorts planned with the hopper (`hopper=`, or all with `ticket=hopper`). */
  hopperResorts: string[];
  /** `blocks` was written in the query: it then holds for owned tickets' resorts too. */
  blocksWritten: boolean;
  /** The ticket catalog's version the plan is made with: part of every cache key. */
  catalogVersion: string;
  /**
   * A region with bases: the stays the visitor fixed (by base key, in key
   * order) and the trip's ends. Absent in a region without bases.
   */
  stays?: {
    bases: Array<{ base: string; nights?: number; area?: string }>;
    startBase?: string;
    endBase?: string;
    /** `moveBuffer=`: the visitor's own minutes for a hotel change, over the region's. */
    moveBuffer?: number;
  };
}

/** A plan before its answer is written: the optimizer's result and what split added. */
export interface OptimizedTrip {
  plan: OptimizedPlan;
  split: Array<{ slug: string; visits: number; auto: true; ticket?: string }>;
  /** The visits each park got, second visits included: what a second plan reuses. */
  visits?: Map<string, number>;
  /** Owned tickets whose second visits stopped for the trip's rest days. */
  restKept?: Set<string>;
  /** Owned tickets' resorts kept in blocks (`blockOwned`), as the plan was made. */
  blockOwned?: boolean;
  /** With owned tickets: whether their resorts mix, and what mixing saved per ride. */
  mixing?: { used: boolean; savedMinutesPerRide?: number };
}

/**
 * Owned tickets' resorts mix only where that saves at least this many minutes
 * per ride over the same plan in blocks: below it the order says nothing a
 * forecast's error does not, and a trip that switches resorts every day for
 * nothing is worse to live.
 */
export const MIX_MIN_PER_RIDE = ORDER_MATTERS_MINUTES;

/** What a second plan changes: the hopper of one resort or one owned ticket. */
export type PlanOverrides = ReplanChange;

const flag = (value: string | undefined, fallback: boolean): boolean =>
  value === undefined ? fallback : value === "1" || value === "true";

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

/**
 * The visits a query pins: the counts of two or more it writes. A park
 * without a count, or with one, may be left out where the trip has no day
 * for it ({@link parksLeftOut}); pinned visits that exceed the trip's days
 * are a 400.
 */
export function pinnedVisits(
  parks: ReadonlyArray<{ visits: number; explicit: boolean }>,
): number {
  return parks
    .filter((p) => p.explicit && p.visits > 1)
    .reduce((sum, p) => sum + p.visits, 0);
}

/** A chosen park the plan has no visit of, and why. */
export interface ParkLeftOut {
  park: string;
  reason: UnplacedReason;
}

/**
 * The chosen parks a plan has no visit of, in the region's order, each with
 * the reason its visit found no day (`unplaced`): `no_day_left` where the
 * trip's park days went to the others. A dropped second visit is not one:
 * its park is in the plan. The same field on `/plan` and on each `/when`
 * window, so a client can say "EPCOT and Epic Universe don't fit into 5
 * park days".
 */
export function parksLeftOut(
  region: Pick<RegionConfig, "parks">,
  chosen: readonly string[],
  plan: Pick<OptimizedPlan, "days" | "unplaced">,
): ParkLeftOut[] {
  const planned = new Set(plan.days.flatMap((d) => d.parks.map((v) => v.slug)));
  const order = new Map(region.parks.map((p, i) => [p.slug, i]));
  return [...new Set(chosen)]
    .filter((slug) => !planned.has(slug))
    .sort(
      (a, b) =>
        (order.get(a) ?? Infinity) - (order.get(b) ?? Infinity) ||
        a.localeCompare(b),
    )
    .map((park) => ({
      park,
      reason:
        plan.unplaced.find((u) => u.slug === park)?.reason ?? "no_day_left",
    }));
}

/**
 * Validates the query against the region and today; 400 with the reason
 * otherwise. `known` are the region parks the database has.
 */
export function normalizePlanQuery(
  region: RegionConfig,
  known: string[],
  query: TripPlanQueryDto,
  today: string,
  ticketScope?: { from: string; to: string; starts: boolean },
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

  const ticket: TicketKind = query.ticket === "hopper" ? "hopper" : "base";
  const resorts = region.groups
    .filter((g) => g.kind === "resort")
    .map((g) => g.key);
  const hopperResorts =
    ticket === "hopper"
      ? resorts
      : [...new Set(query.hopper ? query.hopper.split(",") : [])].sort();
  for (const key of hopperResorts) {
    if (!resorts.includes(key)) {
      throw new BadRequestException(
        `hopper: ${key} is not a resort of ${region.name} (${resorts.join(", ")})`,
      );
    }
  }
  const tickets = parseTickets(
    region,
    query.tickets,
    ticketScope ?? { from: query.from, to: query.to },
    ticketScope?.starts ?? true,
  );
  const hopAsked = flag(query.hop, false);
  const hop =
    hopAsked &&
    (hopperResorts.length > 0 || tickets.some((t) => ownedHops(region, t)));
  const parkDays = parks.reduce((s, p) => s + p.visits, 0);
  if (parkDays > MAX_PARK_DAYS) {
    throw new BadRequestException(
      `A plan takes at most ${MAX_PARK_DAYS} park visits`,
    );
  }
  // Only visits the visitor pinned (a count of two or three) can make a trip
  // impossible; a park without a count, or with one, is planned where it
  // fits and named in `parksLeftOut` where it does not.
  if (pinnedVisits(parks) > tripDays * (hop ? 2 : 1)) {
    throw new BadRequestException(
      hop
        ? "More park visits than two a day"
        : "More park visits than trip days: set ticket=hopper&hop=1 or choose fewer",
    );
  }

  const stays = normalizeStays(region, query, tripDays - 1);
  const area = query.area;
  const areaConfig = area
    ? region.hotelAreas.find((h) => h.key === area)
    : undefined;
  if (area && !areaConfig) {
    throw new BadRequestException(
      `${area} is not a hotel area of ${region.name}`,
    );
  }
  const hotel: HotelKind = query.hotel ?? areaConfig?.hotel ?? "offsite";
  const hotels = hotelKinds(region);
  if (!hotels.includes(hotel)) {
    throw new BadRequestException(`hotel must be one of ${hotels.join(", ")}`);
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
    ticket,
    hop,
    evenings: flag(query.evenings, true),
    locks,
    warnings: hopAsked && !hop ? ["hop_needs_hopper"] : [],
    tickets,
    hopperResorts,
    blocksWritten: query.blocks !== undefined,
    catalogVersion: catalogVersionOf(region),
    ...(stays && { stays }),
  };
}

/**
 * `bases`, `startBase`, `endBase` and `moveBuffer`: only in a region with bases, where they
 * replace `area` and `hotel`; every base known, its area its own, its nights
 * within the trip's. 400 otherwise.
 */
function normalizeStays(
  region: RegionConfig,
  query: TripPlanQueryDto,
  nights: number,
): NormalizedPlanQuery["stays"] {
  const asked =
    query.bases !== undefined ||
    query.startBase !== undefined ||
    query.endBase !== undefined ||
    query.moveBuffer !== undefined;
  if (!region.bases) {
    if (asked) {
      throw new BadRequestException(
        `${region.name} has no bases: bases, startBase, endBase and moveBuffer are for a trip across several cities`,
      );
    }
    return undefined;
  }
  if (query.area !== undefined || query.hotel !== undefined) {
    throw new BadRequestException(
      `In ${region.name} a stay's hotel area goes with its base: bases=<base>@<area>`,
    );
  }
  const known = region.bases.map((b) => b.key);
  const baseOf = (key: string, name: string) => {
    const base = region.bases?.find((b) => b.key === key);
    if (!base) {
      throw new BadRequestException(
        `${name}: ${key} is not a base of ${region.name} (${known.join(", ")})`,
      );
    }
    return base;
  };
  const bases: NonNullable<NormalizedPlanQuery["stays"]>["bases"] = [];
  for (const part of query.bases ? query.bases.split(",") : []) {
    const m = /^([a-z0-9-]+)(?::(\d+))?(?:@([a-z0-9_-]+))?$/.exec(part);
    if (!m)
      throw new BadRequestException(
        `bases: ${part} is not base[:nights][@area]`,
      );
    const base = baseOf(m[1], "bases");
    if (bases.some((b) => b.base === base.key)) {
      throw new BadRequestException(`bases: ${base.key} is listed twice`);
    }
    const n = m[2] === undefined ? undefined : Number(m[2]);
    if (n !== undefined && n > nights) {
      throw new BadRequestException(
        `bases: ${part} has more nights than the trip (${nights})`,
      );
    }
    if (m[3] !== undefined && !base.hotelAreas.includes(m[3])) {
      throw new BadRequestException(
        `bases: ${m[3]} is not a hotel area of ${base.name} (${base.hotelAreas.join(", ")})`,
      );
    }
    if (m[3] !== undefined && n === 0) {
      throw new BadRequestException(
        `bases: ${part} has a hotel area but no night`,
      );
    }
    bases.push({
      base: base.key,
      ...(n !== undefined && { nights: n }),
      ...(m[3] !== undefined && { area: m[3] }),
    });
  }
  const fixed = bases.reduce((sum, b) => sum + (b.nights ?? 0), 0);
  if (fixed > nights) {
    throw new BadRequestException(
      `Your stays add up to ${fixed} nights; the trip has ${nights}`,
    );
  }
  const startBase = query.startBase
    ? baseOf(query.startBase, "startBase").key
    : undefined;
  const endBase = query.endBase
    ? baseOf(query.endBase, "endBase").key
    : undefined;
  bases.sort((a, b) => a.base.localeCompare(b.base));
  return {
    bases,
    ...(startBase && { startBase }),
    ...(endBase && { endBase }),
    ...(query.moveBuffer !== undefined && { moveBuffer: query.moveBuffer }),
  };
}

/** The stays' canonical query, part of the cache key. */
function staysCacheQuery(
  stays: NonNullable<NormalizedPlanQuery["stays"]>,
): string {
  return [
    stays.bases
      .map(
        (b) =>
          `${b.base}${b.nights !== undefined ? `:${b.nights}` : ""}${b.area ? `@${b.area}` : ""}`,
      )
      .join(","),
    stays.startBase ?? "-",
    stays.endBase ?? "-",
    stays.moveBuffer ?? "-",
  ].join(";");
}

/** The optimizer's stays for a query in a region with bases. */
export function staysOption(
  region: RegionConfig,
  stays: NonNullable<NormalizedPlanQuery["stays"]>,
  legs: TripLeg[],
): StaysOption {
  const rules = stayRulesOf(region);
  return {
    bases: (region.bases ?? []).map((b) => {
      const area = stays.bases.find((s) => s.base === b.key)?.area;
      const hotel = area
        ? region.hotelAreas.find((h) => h.key === area)?.hotel
        : undefined;
      const reach = new Map(
        b.parks.map((p) => {
          // The stay's own area refines the base's curated reach.
          const leg = area ? legFromHotel(legs, area, p.slug) : undefined;
          return [
            p.slug,
            leg
              ? { minutes: doorToDoorMinutes(leg), mode: leg.mode }
              : { minutes: p.minutes, mode: p.mode },
          ] as const;
        }),
      );
      return {
        key: b.key,
        ...(b.order !== undefined && { order: b.order }),
        reach,
        perkRules: hotel ? (region.perks[hotel] ?? []) : [],
        ...(area && { area }),
      };
    }),
    legs: (region.baseLegs ?? []).map((l) => ({
      from: l.from,
      to: l.to,
      minutes: l.minutes,
      mode: l.mode,
      ...(l.name && { name: l.name }),
      parkDay: l.parkDay ?? l.minutes <= rules.parkDayMaxLegMinutes,
    })),
    fixed: new Map(stays.bases.map((b) => [b.base, b.nights])),
    ...(stays.startBase && { startBase: stays.startBase }),
    ...(stays.endBase && { endBase: stays.endBase }),
    rules: {
      dayTripFreeMinutes: rules.dayTripFreeMinutes,
      moveOverheadMinutes: stays.moveBuffer ?? rules.moveOverheadMinutes,
      moveCost: rules.moveCost,
      moveStartMinute: minuteOfClock(rules.moveStartsAt) ?? 8 * 60,
      lastArrivalMinute: minuteOfClock(rules.lastArrivalAt) ?? 23 * 60,
    },
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
    q.ticket,
    q.evenings ? 1 : 0,
    // Tickets changed nothing above; what they add goes last.
    ...(q.tickets.length > 0 || q.hopperResorts.length > 0
      ? [
          canonicalTickets(q.tickets),
          q.hopperResorts.join(","),
          q.blocksWritten ? "b" : "-",
        ]
      : []),
    // An edited ticket rule never serves a plan made under the old one.
    `catalog:${q.catalogVersion}`,
    ...(q.stays ? [`stays:${staysCacheQuery(q.stays)}`] : []),
  ].join("|");
}

/** The optimizer's input for a query and a calendar of exactly the trip. */
export function planInput(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  visits: Map<string, number>,
  change?: PlanOverrides,
  blockOwned = q.blocks && q.blocksWritten,
): PlanInput {
  const configs = new Map(region.parks.map((p) => [p.slug, p]));
  const legs = buildLegs(region);
  const slugs = q.parks.map((p) => p.slug);
  // The parks left unchosen are priced only on the plan's spare days.
  const candidates = region.parks.filter((p) => !slugs.includes(p.slug));
  const cells: PlanInput["cells"] = {};
  for (const s of [...slugs, ...candidates.map((p) => p.slug)]) {
    cells[s] = {};
    for (const day of calendar.days) {
      const c = day.parks[s];
      if (!c) continue;
      const cell: PlanCell = planCellOf(c);
      cells[s][day.date] = cell;
    }
  }
  // A second plan may switch one resort's or one ticket's hopper.
  let hopperResorts = q.hopperResorts;
  let hopperTicket: string | undefined;
  if (change && "resort" in change) {
    hopperResorts = change.hopper
      ? [...new Set([...hopperResorts, change.resort])]
      : hopperResorts.filter((r) => r !== change.resort);
  } else if (change?.hopper) {
    hopperTicket = change.ticket;
  }
  const groupOf = new Map(region.parks.map((p) => [p.slug, p.group]));
  const planFirstPairs = hopPairs(region, legs, slugs).filter(([a]) =>
    hopperResorts.includes(groupOf.get(a) ?? ""),
  );
  const dates = calendar.days.map((d) => d.date);
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
    ...(candidates.length > 0 && {
      candidates: candidates.map((p) => ({
        slug: p.slug,
        visits: 1,
        group: p.group,
        type: p.type,
      })),
    }),
    cells,
    options: {
      arrival: q.arrival,
      departure: q.departure,
      blocks: q.blocks,
      blockGroups: region.groups
        .filter((g) => g.kind === "resort")
        .map((g) => g.key),
      blockOwned,
      fireworks: q.fireworks,
      rest: q.rest,
      // A second plan's switched hopper may hop where the visitor did not ask.
      hop: q.hop || (change?.hopper ?? false),
      hopPairs: planFirstPairs,
      // A region with bases: each stay's hotel brings its own perks.
      perkRules: q.stays ? [] : region.perks[q.hotel],
      locks: q.locks,
      weather: region.weather,
      ...(q.stays && { stays: staysOption(region, q.stays, legs) }),
    },
    ...(q.tickets.length > 0 && {
      tickets: planTicketsOf(region, q.tickets, dates, legs, hopperTicket),
    }),
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
  return splitVisits(region, q, calendar).visits;
}

/**
 * {@link autoSplit} with what the plan route says about it: each owned
 * ticket with a day count first gets second visits for its parks until its
 * days are used (the ticket named), then the other parks as before. A park
 * of such a ticket gets none beyond it. `restKept`: the tickets whose filling
 * stopped for the trip's rest days.
 */
export function splitVisits(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
): {
  visits: Map<string, number>;
  byTicket: Map<string, string>;
  restKept: Set<string>;
} {
  const visits = new Map(q.parks.map((p) => [p.slug, p.visits]));
  const byTicket = new Map<string, string>();
  const restKept = new Set<string>();
  if (q.split !== "auto") return { visits, byTicket, restKept };
  const ranked = region.parks
    .filter((c) => c.secondVisitRank != null)
    .sort((a, b) => (a.secondVisitRank ?? 0) - (b.secondVisitRank ?? 0))
    .filter((c) => q.parks.some((p) => p.slug === c.slug && !p.explicit));
  let room = extraVisitRoom(planInput(region, q, calendar, visits));
  let total = [...visits.values()].reduce((s, v) => s + v, 0);
  const openDays = (slug: string) =>
    calendar.days.filter(
      (d) => d.parks[slug] && d.parks[slug].status !== "CLOSED",
    ).length;
  const counted = new Set<string>();
  for (const o of q.tickets) {
    const product = productOf(region, o.product);
    if (!product || o.days === "unlimited") continue;
    const covered = parksOf(product, o.days);
    covered.forEach((slug) => counted.add(slug));
    let ticketRoom =
      o.days -
      q.parks
        .filter((p) => covered.includes(p.slug))
        .reduce((sum, p) => sum + (visits.get(p.slug) ?? p.visits), 0);
    for (const config of ranked) {
      if (ticketRoom <= 0 || total >= MAX_PARK_DAYS) break;
      if (!covered.includes(config.slug) || visits.get(config.slug) !== 1) {
        continue;
      }
      if (room <= 0) {
        restKept.add(o.id);
        break;
      }
      if (openDays(config.slug) < 2) continue;
      visits.set(config.slug, 2);
      byTicket.set(config.slug, o.id);
      room--;
      total++;
      ticketRoom--;
    }
  }
  for (const config of ranked) {
    if (room <= 0 || total >= MAX_PARK_DAYS) break;
    if (counted.has(config.slug) || visits.get(config.slug) !== 1) continue;
    if (openDays(config.slug) < 2) continue;
    visits.set(config.slug, 2);
    room--;
    total++;
  }
  return { visits, byTicket, restKept };
}

/** Runs the optimizer for a query on its calendar, second visits included. */
export function optimizePlan(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
): OptimizedTrip {
  const { visits, byTicket, restKept } = splitVisits(region, q, calendar);
  const { plan, blockOwned, mixing } = planMixing(region, q, calendar, visits);
  const split = q.parks
    .filter((p) => (visits.get(p.slug) ?? p.visits) !== p.visits)
    .map((p) => ({
      slug: p.slug,
      visits: visits.get(p.slug) as number,
      auto: true as const,
      ...(byTicket.has(p.slug) && { ticket: byTicket.get(p.slug) }),
    }));
  return {
    plan,
    split,
    visits,
    restKept,
    blockOwned,
    ...(mixing && { mixing }),
  };
}

/**
 * The plan, and with owned tickets whether their resorts mix. `blocks=1`
 * written keeps them in blocks, `blocks=0` lets them mix; by default the trip
 * is planned both ways and mixes only where that places more visits, or as
 * many saving at least {@link MIX_MIN_PER_RIDE} minutes per ride.
 */
function planMixing(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  visits: Map<string, number>,
): Pick<OptimizedTrip, "plan" | "blockOwned" | "mixing"> {
  if (q.tickets.length === 0 || q.blocksWritten || !q.blocks) {
    const blockOwned = q.blocks && q.blocksWritten;
    return {
      plan: optimizeTrip(
        planInput(region, q, calendar, visits, undefined, blockOwned),
      ),
      blockOwned,
      ...(q.tickets.length > 0 && { mixing: { used: !blockOwned } }),
    };
  }
  const blocked = optimizeTrip(
    planInput(region, q, calendar, visits, undefined, true),
  );
  const free = optimizeTrip(
    planInput(region, q, calendar, visits, undefined, false),
  );
  const saved = (blocked.totals.exactWait ?? 0) - (free.totals.exactWait ?? 0);
  const mix =
    free.totals.visits > blocked.totals.visits ||
    (free.totals.visits === blocked.totals.visits && saved >= MIX_MIN_PER_RIDE);
  return {
    plan: mix ? free : blocked,
    blockOwned: !mix,
    mixing: {
      used: mix,
      savedMinutesPerRide:
        free.totals.visits === blocked.totals.visits
          ? Math.max(0, Math.round(saved))
          : 0,
    },
  };
}

/** The plan again with one resort's or one ticket's hopper switched, on the same visits. */
export function replanTrip(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  trip: OptimizedTrip,
  change: PlanOverrides,
): OptimizedPlan {
  const visits = trip.visits ?? new Map(q.parks.map((p) => [p.slug, p.visits]));
  return optimizeTrip(
    planInput(region, q, calendar, visits, change, trip.blockOwned),
  );
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

/**
 * A plan day as the route serves it in a region with bases: the move's times
 * and the guest's way into the park after a morning move on the park's clock,
 * the hotel area the day's travel starts from (the stay's).
 */
function withStay(
  day: PlannedDay,
  q: NormalizedPlanQuery,
  timezone: string,
): { day: PlannedDay; area?: string } {
  if (!q.stays || !day.base) return { day };
  const { startMinute, move, ...rest } = day;
  const from = move?.when === "evening" ? move.from : (move?.to ?? day.base);
  const area = q.stays.bases.find((b) => b.base === from)?.area;
  return {
    day: {
      ...rest,
      ...(move && {
        move: {
          from: move.from,
          to: move.to,
          minutes: move.minutes,
          mode: move.mode,
          ...(move.name && { name: move.name }),
          when: move.when,
          departAt: isoAt(day.date, move.departMinute, timezone),
          arriveAt: isoAt(day.date, move.arriveMinute, timezone),
        },
      }),
      ...(startMinute !== undefined && {
        startAt: isoAt(day.date, startMinute, timezone),
      }),
    } as PlannedDay,
    ...(area && { area }),
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
  /**
   * The night shows the evenings read, by `showsKey(park, date)`: what
   * `eveningShowsWanted` asked for, from the shows data. Absent: no show is
   * named and no fireworks view is suggested.
   */
  shows?: EveningShows;
}

/**
 * The hop sets an evening on a date may use: the owned ticket's the day uses,
 * else the region's hopper sets where plan first has the hopper for the
 * day's resort.
 */
export function dayHopSets(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  plan: OptimizedPlan,
): (date: string) => string[][] {
  const groupOf = new Map(region.parks.map((p) => [p.slug, p.group]));
  const owned = new Map(q.tickets.map((t) => [t.id, t]));
  const byDate = new Map(plan.days.map((d) => [d.date, d]));
  return (date) => {
    const day = byDate.get(date);
    if (!day || day.parks.length === 0) return [];
    const ticket = day.ticket ? owned.get(day.ticket) : undefined;
    if (ticket) return ownedHopSets(region, ticket);
    const resort = groupOf.get(day.parks[0].slug) ?? "";
    return q.hopperResorts.includes(resort) ? region.hopperSets : [];
  };
}

/** The ticket parts of an answer: the owned tickets, plan first's suggestion and its days. */
export interface PlanTicketParts {
  summaries: ReturnType<typeof ticketSummaries>;
  suggestions: TicketSuggestion[];
  days: Map<string, DayTicket>;
  warnings: Array<"hop_needs_hopper" | "blockouts_unchecked">;
}

/** The answer's ticket parts for a planned trip. */
export function planTickets(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  trip: OptimizedTrip,
  eveningsWith: SuggestionInput["eveningsWith"],
): PlanTicketParts {
  const { plan } = trip;
  const input: SuggestionInput = {
    region,
    plan,
    trip: { from: q.from, to: q.to },
    owned: q.tickets,
    hopperResorts: q.hopperResorts,
    eveningsWith,
    replan: (change) => replanTrip(region, q, calendar, trip, change),
  };
  const { suggestions, days } = suggestTickets(input);
  const verdicts = new Map<string, HopperVerdict>();
  for (const t of q.tickets) {
    if (t.hopper !== "unknown") continue;
    const verdict = ownedVerdict(input, t);
    if (verdict) verdicts.set(t.id, verdict);
  }
  const dates = calendar.days.map((d) => d.date);
  const available = dates.filter(
    (_, i) =>
      !(i === 0 && q.arrival === "free") &&
      !(i === dates.length - 1 && dates.length > 1 && q.departure === "free"),
  );
  const summaries =
    q.tickets.length > 0
      ? ticketSummaries({
          region,
          owned: q.tickets,
          planned: plan.tickets,
          parks: q.parks.map((p) => p.slug),
          available,
          restKept: trip.restKept ?? new Set(),
          visits:
            trip.visits ?? new Map(q.parks.map((p) => [p.slug, p.visits])),
          verdicts,
        })
      : [];
  const warnings: PlanTicketParts["warnings"] = [...q.warnings];
  if (summaries.some((t) => t.warnings.includes("blockouts_unchecked"))) {
    warnings.push("blockouts_unchecked");
  }
  return { summaries, suggestions, days, warnings };
}

/**
 * An every-day ticket's day is the day of its window ("day 10 of 14"), not
 * the count of its days used so far.
 */
function windowDayOf(
  day: PlannedDay,
  summaries: PlanTicketParts["summaries"],
): Partial<PlannedDay> {
  const t = summaries.find((s) => s.id === day.ticket);
  if (!t?.windowDays || !t.start || day.ticketDay === undefined) return {};
  return { ticketDay: daysBetween(t.start, day.date) + 1 };
}

/** A one-night change to the visitor's fixed stays that saves waiting (`meta.staySuggestion`). */
export interface StaySuggestion {
  /** The nights to write into `bases` instead: the changed stays only. */
  nights: Record<string, number>;
  savedMinutesPerRide: number;
  /** The park whose day moved to the cheaper one, and that day. */
  park?: string;
  date?: string;
}

/**
 * With nights the visitor fixed: each fixed stay one night shorter or longer
 * (the others' nights as they were, the plan's own ones searched again), the
 * plan made once more, and the change that saves most per ride, at least
 * {@link ORDER_MATTERS_MINUTES} and placing every visit the plan placed.
 * Undefined without fixed nights or without such a change.
 */
export function staySuggestionOf(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  trip: OptimizedTrip,
): StaySuggestion | undefined {
  const stays = q.stays;
  const planStays = trip.plan.stays;
  if (!stays || !planStays) return undefined;
  const fixed = stays.bases.filter((b) => (b.nights ?? 0) > 0);
  if (fixed.length === 0) return undefined;
  const now = trip.plan.totals.exactWait;
  if (now === undefined) return undefined;
  let best: StaySuggestion | undefined;
  for (const f of fixed) {
    for (const delta of [-1, 1]) {
      const nights = (f.nights as number) + delta;
      if (nights < 1) continue;
      const variant: NormalizedPlanQuery = {
        ...q,
        stays: {
          ...stays,
          bases: stays.bases.map((b) =>
            b.base === f.base ? { ...b, nights } : b,
          ),
        },
      };
      if (
        variant.stays &&
        variant.stays.bases.reduce((sum, b) => sum + (b.nights ?? 0), 0) >
          daysBetween(q.from, q.to)
      ) {
        continue;
      }
      const other = optimizePlan(region, variant, calendar);
      const then = other.plan.totals.exactWait;
      if (then === undefined) continue;
      if (other.plan.totals.visits < trip.plan.totals.visits) continue;
      const saved = Math.round(now - then);
      if (saved < ORDER_MATTERS_MINUTES) continue;
      if (best && saved <= best.savedMinutesPerRide) continue;
      const moved = movedVisit(trip.plan, other.plan);
      best = {
        nights: { [f.base]: nights },
        savedMinutesPerRide: saved,
        ...moved,
      };
    }
  }
  return best;
}

/** The park whose planned day changed most for the better between two plans. */
function movedVisit(
  before: OptimizedPlan,
  after: OptimizedPlan,
): { park?: string; date?: string } {
  let best: { park: string; date: string; gain: number } | undefined;
  for (const p of after.parks) {
    const was = before.parks.find((x) => x.slug === p.slug);
    for (const date of p.dates) {
      if (was?.dates.includes(date)) continue;
      const wait = after.days.find((d) => d.date === date)?.headlinerWait;
      const old = was?.dates
        .map((d) => before.days.find((x) => x.date === d)?.headlinerWait)
        .find((w) => w !== undefined);
      if (wait === undefined || old === undefined) continue;
      if (!best || old - wait > best.gain) {
        best = { park: p.slug, date, gain: old - wait };
      }
    }
  }
  return best ? { park: best.park, date: best.date } : {};
}

/** The plan route's answer. */
export function planResponse(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  trip: OptimizedTrip,
  ctx: PlanResponseContext,
): TripPlanDto {
  const { plan } = trip;
  const legs = buildLegs(region);
  const hopSetsOn = dayHopSets(region, q, plan);
  const eveningsWith = (sets: (date: string) => string[][]) =>
    planEvenings({
      region,
      calendar,
      legs,
      days: plan.days,
      trip: { from: q.from, to: q.to },
      ticket: q.ticket,
      hopSetsOn: sets,
      shows: ctx.shows ?? new Map(),
    });
  const evenings = q.evenings ? eveningsWith(hopSetsOn) : new Map();
  const tickets = planTickets(region, q, calendar, trip, eveningsWith);
  const suggestion = staySuggestionOf(region, q, calendar, trip);
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
      ticket: q.ticket,
      ...(q.hopperResorts.length > 0 && { hopper: q.hopperResorts }),
      ...(tickets.summaries.length > 0 && { tickets: tickets.summaries }),
      ...(trip.mixing && { mixing: trip.mixing }),
      ticketCatalog: q.catalogVersion,
      ...(tickets.suggestions.length > 0 && {
        ticketSuggestion: tickets.suggestions,
      }),
      ...(tickets.warnings.length > 0 && { warnings: tickets.warnings }),
      ...(plan.stays && {
        stays: plan.stays.map((st) => {
          const area = q.stays?.bases.find((b) => b.base === st.base)?.area;
          return { ...st, ...(area && { area }) };
        }),
      }),
      ...(suggestion && { staySuggestion: suggestion }),
      ...(q.stays && {
        moveBuffer:
          q.stays.moveBuffer ?? stayRulesOf(region).moveOverheadMinutes,
      }),
    },
    days: plan.days.map((raw) => {
      const { day, area } = withStay(raw, q, region.timezone);
      const evening = evenings.get(day.date);
      return {
        ...withTravel(day, legs, q.stays ? area : q.area),
        ...(day.ticket
          ? windowDayOf(day, tickets.summaries)
          : tickets.days.get(day.date)),
        ...(evening && { evening }),
      };
    }) as unknown as Array<Record<string, unknown>>,
    parks: plan.parks as unknown as Array<Record<string, unknown>>,
    totals: {
      headlinerWait: plan.totals.headlinerWait,
      averageDayWait: plan.totals.averageDayWait,
      savedMinutesPerRide: plan.totals.savedMinutesPerRide,
      ...(expectedError != null && { expectedError }),
      orderMatters: plan.totals.orderMatters,
      parkDays: plan.totals.parkDays,
      restDays: plan.totals.restDays,
      spareDays: plan.totals.spareDays,
      visits: plan.totals.visits,
      ...(plan.totals.moves !== undefined && {
        moves: plan.totals.moves,
        travelDays: plan.totals.travelDays,
      }),
    },
    unplaced: plan.unplaced,
    parksLeftOut: parksLeftOut(
      region,
      q.parks.map((p) => p.slug),
      plan,
    ),
    ...(plan.ignoredLocks.length > 0 && { ignoredLocks: plan.ignoredLocks }),
    ...(plan.spareDayParks && { spareDayParks: plan.spareDayParks }),
  };
}
