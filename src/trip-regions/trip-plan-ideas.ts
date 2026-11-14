/**
 * The plan's ideas (`GET /v1/trip-regions/:region/plan/ideas`): the best
 * single edits of a plan, each planned again by the optimizer and priced in
 * the figures the plan prints. Pure: `TripPlanService` feeds it the query and
 * the region calendar the plan reads, and every idea is `optimizePlan` on the
 * query with one change, never a second way of planning. Contract:
 * docs/frontend/trip-regions.md#ideas-get-v1trip-regionsorlandoplanideas.
 */
import { BadRequestException } from "@nestjs/common";
import type { RegionConfig } from "./regions";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import type { OptimizedPlan, PlannedDay } from "./trip-optimizer";
import {
  normalizePlanQuery,
  optimizePlan,
  parksLeftOut,
  type NormalizedPlanQuery,
  type OptimizedTrip,
} from "./trip-plan.assembly";
import { savedOnCommonVisits } from "./trip-tickets";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";

export type PlanIdeaKind =
  "move" | "swap" | "add" | "travel_day" | "hopper" | "avoid_party";

/** An idea is worth showing from this many minutes saved over the trip's visits. */
export const MIN_IDEA_MINUTES = 5;
/** Ideas in an answer: the owner's default, three. */
export const MAX_IDEAS = 3;
/**
 * Plans made per answer beyond the plan itself: the candidates are ranked by
 * what the calendar alone says they save and only the first are planned, so
 * an answer costs at most this many plans whatever the trip.
 */
export const MAX_IDEA_PLANS = 10;
/** Of those, moves and swaps: the ones the calendar ranks highest. */
const MAX_MOVES = 3;
const MAX_SWAPS = 2;

/** One idea: the change, and the plan it makes against the plan as it is. */
export interface PlanIdea {
  kind: PlanIdeaKind;
  /**
   * The change as query parameters, each replacing the current value whole
   * (`lock=epcot@2026-12-02,rest@2026-12-04`, the visitor's pins kept):
   * applied as is, it asks `/plan` for the plan this idea priced.
   */
  query: string;
  /** The park the idea is about. */
  park?: string;
  /** Its day in the plan as it is; absent where it has none. */
  from?: string;
  /** Its day in the plan with the idea. */
  to?: string;
  /** `swap`: the park that takes `from`'s place. */
  with?: string;
  /** The park's `headlinerWait` on its day, before and after: the day's figure. */
  headlinerWait: { before?: number; after?: number };
  /** `totals.headlinerWait` of the plan as it is and with the idea: the trip's figure. */
  totals: { before?: number; after?: number };
  /**
   * Headliner minutes saved over the visits both plans place, a park's
   * visits paired in date order; negative where the idea waits longer.
   */
  savedMinutes: number;
  /** The parks only the idea's plan places: left out now, or added. */
  places?: string[];
}

export interface PlanIdeasAnswer {
  meta: {
    region: string;
    from: string;
    to: string;
    computedAt: string;
    /** The plans made for the answer beyond the plan itself, at most {@link MAX_IDEA_PLANS}. */
    plans: number;
  };
  ideas: PlanIdea[];
}

/** The `/plan` parameters an idea may set: never one that removes a pin. */
type IdeaParam =
  "parks" | "lock" | "arrival" | "departure" | "fireworks" | "hopper" | "hop";

/** A change to try, before it is planned. */
interface Candidate {
  kind: PlanIdeaKind;
  /** The query's parameters it sets. */
  set: Partial<Record<IdeaParam, string>>;
  park?: string;
  from?: string;
  to?: string;
  with?: string;
  /** `travel_day`: the day it opens to a park. */
  opens?: string;
  /** What the calendar alone says it saves; ranks moves and swaps. */
  estimate: number;
}

const QUERY_ORDER: IdeaParam[] = [
  "parks",
  "lock",
  "arrival",
  "departure",
  "fireworks",
  "hopper",
  "hop",
];

/** Rank of a kind among ideas that save the same. */
const KIND_ORDER: PlanIdeaKind[] = [
  "travel_day",
  "hopper",
  "move",
  "swap",
  "add",
  "avoid_party",
];

const lockList = (q: NormalizedPlanQuery, extra: string[]): string =>
  [...q.locks.map((l) => `${l.target}@${l.date}`), ...extra].join(",");

const parksParam = (q: NormalizedPlanQuery, extra?: string): string =>
  [
    ...q.parks.map((p) => (p.explicit ? `${p.slug}:${p.visits}` : p.slug)),
    ...(extra ? [extra] : []),
  ].join(",");

/** A planned day's wait for one of its parks: a hop day's per park. */
function waitOn(day: PlannedDay | undefined, slug: string): number | undefined {
  if (!day) return undefined;
  if (day.kind === "hop") {
    return day.parks.find((p) => p.slug === slug)?.headlinerWait;
  }
  return day.parks[0]?.slug === slug ? day.headlinerWait : undefined;
}

/** The unrounded wait of a park on a date, from the calendar. */
function exactOn(
  calendar: InternalRegionCalendar,
  slug: string,
  date: string,
): number | undefined {
  const c = calendar.days.find((d) => d.date === date)?.parks[slug];
  if (!c || c.status === "CLOSED") return undefined;
  return c.waitExact ?? c.headlinerWait;
}

/** The changes worth planning for a plan, each ranked by the calendar's estimate. */
function candidates(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
  trip: OptimizedTrip,
): Candidate[] {
  const { plan } = trip;
  const pinned = new Set(q.locks.map((l) => `${l.target}@${l.date}`));
  const singles = plan.days.filter(
    (d) => d.kind === "park" && !pinned.has(`${d.parks[0].slug}@${d.date}`),
  );
  const out: Candidate[] = [];

  // A park the plan leaves out comes in with a park on the arrival or departure day.
  const leftOut = parksLeftOut(
    region,
    q.parks.map((p) => p.slug),
    plan,
  );
  const n = calendar.days.length;
  if (n > 1) {
    if (q.arrival === "free") {
      out.push({
        kind: "travel_day",
        set: { arrival: "park" },
        opens: q.from,
        estimate: 0,
      });
    }
    if (q.departure === "free") {
      out.push({
        kind: "travel_day",
        set: { departure: "park" },
        opens: q.to,
        estimate: 0,
      });
    }
  }

  // Plan first's hopper, where a left-out park shares a resort with a planned
  // one and the visitor's tickets do not decide it.
  if (q.tickets.length === 0 && region.hopperSets.length > 0) {
    const groupOf = new Map(region.parks.map((p) => [p.slug, p.group]));
    const resorts = new Set(
      leftOut
        .filter((p) => p.reason === "no_day_left")
        .map((p) => groupOf.get(p.park) ?? ""),
    );
    for (const resort of [...resorts].sort()) {
      if (!resort || q.hopperResorts.includes(resort)) continue;
      if (!region.groups.some((g) => g.key === resort && g.kind === "resort")) {
        continue;
      }
      out.push({
        kind: "hopper",
        set: {
          hopper: [...q.hopperResorts, resort].sort().join(","),
          hop: "1",
        },
        estimate: 0,
      });
    }
  }

  // A party night the day guests lose the evening to: plan with the evening kept.
  if (q.fireworks === "any") {
    const party = plan.days.find((d) =>
      d.warnings.includes("party_night_short_hours"),
    );
    if (party) {
      out.push({
        kind: "avoid_party",
        set: { fireworks: "want" },
        park: party.parks[0]?.slug,
        from: party.date,
        estimate: 0,
      });
    }
  }

  // A park on a spare day: the first of `spareDayParks`, on its cheapest.
  const spare = plan.spareDayParks?.[0];
  if (spare && spare.days[0]) {
    out.push({
      kind: "add",
      set: {
        parks: parksParam(q, spare.park),
        lock: lockList(q, [`${spare.park}@${spare.days[0].date}`]),
      },
      park: spare.park,
      to: spare.days[0].date,
      estimate: 0,
    });
  }

  // A visit pinned to one of its park's cheapest other days the plan may use:
  // a free arrival or departure day is a travel-day idea, a pinned rest day
  // the visitor's.
  const closedToParks = new Set([
    ...(q.arrival === "free" ? [q.from] : []),
    ...(q.departure === "free" ? [q.to] : []),
    ...q.locks.filter((l) => l.target === "rest").map((l) => l.date),
  ]);
  const moves: Candidate[] = [];
  for (const day of singles) {
    const slug = day.parks[0].slug;
    const now = exactOn(calendar, slug, day.date);
    const summary = plan.parks.find((p) => p.slug === slug);
    if (now === undefined || !summary) continue;
    for (const alt of summary.alternatives) {
      if (closedToParks.has(alt.date)) continue;
      const then = exactOn(calendar, slug, alt.date);
      if (then === undefined || now - then < MIN_IDEA_MINUTES) continue;
      moves.push({
        kind: "move",
        set: { lock: lockList(q, [`${slug}@${alt.date}`]) },
        park: slug,
        from: day.date,
        to: alt.date,
        estimate: now - then,
      });
    }
  }
  out.push(...rankByEstimate(moves).slice(0, MAX_MOVES));

  // Two visits trading days, both pinned.
  const swaps: Candidate[] = [];
  for (let i = 0; i < singles.length; i++) {
    for (let j = i + 1; j < singles.length; j++) {
      const a = singles[i];
      const b = singles[j];
      const pa = a.parks[0].slug;
      const pb = b.parks[0].slug;
      if (pa === pb) continue;
      const before =
        (exactOn(calendar, pa, a.date) ?? NaN) +
        (exactOn(calendar, pb, b.date) ?? NaN);
      const after =
        (exactOn(calendar, pa, b.date) ?? NaN) +
        (exactOn(calendar, pb, a.date) ?? NaN);
      if (!(before - after >= MIN_IDEA_MINUTES)) continue;
      swaps.push({
        kind: "swap",
        set: { lock: lockList(q, [`${pa}@${b.date}`, `${pb}@${a.date}`]) },
        park: pa,
        from: a.date,
        to: b.date,
        with: pb,
        estimate: before - after,
      });
    }
  }
  out.push(...rankByEstimate(swaps).slice(0, MAX_SWAPS));
  return out.slice(0, MAX_IDEA_PLANS);
}

const queryOf = (set: Candidate["set"]): string =>
  QUERY_ORDER.filter((k) => set[k] !== undefined)
    .map((k) => `${k}=${set[k]}`)
    .join("&");

function rankByEstimate(list: Candidate[]): Candidate[] {
  return [...list].sort(
    (a, b) =>
      b.estimate - a.estimate || queryOf(a.set).localeCompare(queryOf(b.set)),
  );
}

/** Each planned park's dates, for telling two plans apart. */
const signature = (plan: OptimizedPlan): string =>
  plan.days.map((d) => d.parks.map((p) => p.slug).join("+")).join("|");

/** The idea a candidate makes, or null where it is not worth one. */
function ideaOf(
  c: Candidate,
  base: OptimizedTrip,
  next: OptimizedTrip,
  chosen: { before: string[]; after: string[] },
  region: RegionConfig,
): PlanIdea | null {
  const before = base.plan;
  const after = next.plan;
  // An idea never costs a visit or a park the plan has.
  if (after.totals.visits < before.totals.visits) return null;
  const placedBefore = new Set(
    before.parks.filter((p) => p.dates.length > 0).map((p) => p.slug),
  );
  const placedAfter = new Set(
    after.parks.filter((p) => p.dates.length > 0).map((p) => p.slug),
  );
  if ([...placedBefore].some((slug) => !placedAfter.has(slug))) return null;
  // A pin the plan could not keep is not the idea it was meant to be.
  if (after.ignoredLocks.length > before.ignoredLocks.length) return null;
  const parks = new Set(chosen.after);
  const { saved } = savedOnCommonVisits(after, before, parks);
  const leftNow = new Set(
    parksLeftOut(region, chosen.before, before).map((p) => p.park),
  );
  const places = [...placedAfter]
    .filter(
      (slug) =>
        !placedBefore.has(slug) && (leftNow.has(slug) || slug === c.park),
    )
    .sort();
  const savedMinutes = Math.round(saved);
  const worth =
    places.length > 0 ||
    savedMinutes >= MIN_IDEA_MINUTES ||
    (c.kind === "avoid_party" &&
      !after.days.some(
        (d) =>
          d.warnings.includes("party_night_short_hours") ||
          d.warnings.includes("fireworks_missed"),
      ) &&
      savedMinutes > -MIN_IDEA_MINUTES);
  if (!worth) return null;
  const dayOf = (plan: OptimizedPlan, date: string | undefined) =>
    date ? plan.days.find((d) => d.date === date) : undefined;
  // The park the idea is about: its own, a park it places, or the park the
  // day it opens gets.
  const park = c.park ?? places[0] ?? dayOf(after, c.opens)?.parks[0]?.slug;
  const datesIn = (plan: OptimizedPlan) =>
    plan.parks.find((p) => p.slug === park)?.dates ?? [];
  const from =
    c.from ?? datesIn(before).find((d) => !datesIn(after).includes(d));
  const to = c.to ?? datesIn(after).find((d) => !datesIn(before).includes(d));
  return {
    kind: c.kind,
    query: queryOf(c.set),
    ...(park && { park }),
    ...(from && { from }),
    ...(to && { to }),
    ...(c.with && { with: c.with }),
    headlinerWait: {
      ...(park && from && { before: waitOn(dayOf(before, from), park) }),
      ...(park && to && { after: waitOn(dayOf(after, to), park) }),
    },
    totals: {
      before: before.totals.headlinerWait,
      after: after.totals.headlinerWait,
    },
    savedMinutes,
    ...(places.length > 0 && { places }),
  };
}

/** Parks placed first, then minutes saved, then the kind's rank. */
function rankIdeas(ideas: PlanIdea[]): PlanIdea[] {
  return [...ideas].sort(
    (a, b) =>
      (b.places?.length ?? 0) - (a.places?.length ?? 0) ||
      b.savedMinutes - a.savedMinutes ||
      KIND_ORDER.indexOf(a.kind) - KIND_ORDER.indexOf(b.kind) ||
      a.query.localeCompare(b.query),
  );
}

export interface PlanIdeasInput {
  region: RegionConfig;
  /** The region parks the database has, as `/plan` validates against. */
  known: string[];
  query: TripPlanQueryDto;
  today: string;
  /** The region calendar for exactly the trip, as `/plan` reads it. */
  calendar: InternalRegionCalendar;
  computedAt: string;
  /** The plan as `/plan` made it; made here when absent. */
  base?: { q: NormalizedPlanQuery; trip: OptimizedTrip };
}

/**
 * The plan's best single edits: travel days and the hopper where a chosen
 * park is left out, a party night kept for the evening, a park on a spare
 * day, a visit moved or two swapped. Each is planned again on the same
 * calendar and kept where it places a park the plan leaves out or saves at
 * least {@link MIN_IDEA_MINUTES} over the visits both place, never at the
 * cost of a visit; ranked so, at most {@link MAX_IDEAS}, one per plan they
 * make. No idea removes a visitor's pin.
 */
export function planIdeas(input: PlanIdeasInput): PlanIdeasAnswer {
  const { region, known, query, today, calendar } = input;
  const q = input.base?.q ?? normalizePlanQuery(region, known, query, today);
  const trip = input.base?.trip ?? optimizePlan(region, q, calendar);
  const ideas: PlanIdea[] = [];
  const seen = new Set<string>([signature(trip.plan)]);
  let plans = 0;
  for (const c of candidates(region, q, calendar, trip)) {
    let nq: NormalizedPlanQuery;
    try {
      nq = normalizePlanQuery(region, known, { ...query, ...c.set }, today);
    } catch (err) {
      if (err instanceof BadRequestException) continue;
      throw err;
    }
    plans++;
    const next = optimizePlan(region, nq, calendar);
    const sig = signature(next.plan);
    if (seen.has(sig)) continue;
    const idea = ideaOf(
      c,
      trip,
      next,
      {
        before: q.parks.map((p) => p.slug),
        after: nq.parks.map((p) => p.slug),
      },
      region,
    );
    if (!idea) continue;
    seen.add(sig);
    ideas.push(idea);
  }
  return {
    meta: {
      region: region.slug,
      from: q.from,
      to: q.to,
      computedAt: input.computedAt,
      plans,
    },
    ideas: rankIdeas(ideas).slice(0, MAX_IDEAS),
  };
}
