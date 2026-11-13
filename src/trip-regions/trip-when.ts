/**
 * "When should we go?": every trip window of a given length in a search range,
 * ranked by the plan it would get, plus the year strip (`weeks`, `months`) the
 * best-time pages show. Contract: docs/frontend/trip-regions-when.md.
 *
 * Pure: no Nest, no clock, no I/O. `TripWhenService` reads the region calendar
 * for the whole range once and passes it in; the dev shim can do the same from
 * production. Days are priced by the plan's own {@link dayCost} through
 * {@link priceDay}, and each window is planned by the plan route's own
 * {@link optimizePlan}, so a window's figure is the figure `/plan` would give.
 *
 * The search: a cheap lower bound ranks every start date (each visit on its
 * park's cheapest days of the window, ignoring that two parks cannot share a
 * day), and the full optimizer plans candidates in bound order until the next
 * bound cannot beat the `limit`-th window kept after the `gap` rule. That is
 * exact for the ranking the contract defines: every window left unplanned has
 * a bound, and so a score, behind the last one returned.
 */
import type { ClimateNormals } from "../external-apis/weather/climate-normals";
import { CLIMATE_NORMAL_PERIOD } from "../external-apis/weather/climate-normals";
import type { RegionConfig, PerkRule } from "./regions";
import {
  dayCostParts,
  hurricaneWeight,
  isHurricanePeak,
  medianDayHours,
  penaltiesOf,
  PERK_SHARE,
  PlanCostModel,
  type DayCostContext,
  type DayCostParts,
  type OptimizedPlan,
  type PlanPenalties,
  type PlanCell,
  type PlannedDay,
} from "./trip-optimizer";
import {
  autoSplit,
  optimizePlan,
  type NormalizedPlanQuery,
} from "./trip-plan.assembly";
import {
  planCellOf,
  type InternalCalendarDay,
  type InternalRegionCalendar,
} from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import { solveAssignment } from "./hungarian";

/** The shortest and longest trip `/when` ranks. */
export const WHEN_MIN_DAYS = 3;
export const WHEN_MAX_DAYS = 21;
/** A search range covers at most this many days. */
export const WHEN_MAX_RANGE_DAYS = 366;
export const WHEN_DEFAULT_DAYS = 10;
export const WHEN_DEFAULT_LIMIT = 5;
export const WHEN_MAX_LIMIT = 10;
export const WHEN_DEFAULT_GAP = 7;
/**
 * Windows whose cost per visit lies this close count as equal and are ordered
 * by comfort instead: a forecast's error is about 17 minutes per ride, so a
 * 3-minute lead says nothing a forecast-versus-last-year difference does not.
 */
export const WHEN_TIE_MINUTES = 3;

/**
 * What a visit the window cannot hold adds to its score: the scale of the
 * plan's "only when nothing else fits" penalties, so a window that drops a
 * chosen park ranks behind every window that keeps it.
 */
export const UNPLACED_VISIT_COST = 1000;

/**
 * `rangeAverageWait` plans every `stride`-th window: about 26 over any range,
 * every other week of a year. The mean of all windows would need every
 * window planned; the sample is within a minute of it on the measured year
 * (docs/frontend/trip-regions-when.md).
 */
export const RANGE_AVERAGE_SAMPLES = 26;

/**
 * Planning time a request may spend: the search, then the range average. A
 * 10-day year search needs about a tenth of it; a 21-day trip with second
 * visits costs the optimizer about 0.3 s a window, and there the budget ends
 * the search with `meta.search.exact: false` and thins the average's sample.
 */
export const SEARCH_BUDGET_MS = 2000;
export const AVERAGE_BUDGET_MS = 800;

/** `long_days`: the window's day-guest hours beat the range's by this much. */
export const LONG_DAYS_HOURS = 1;
/** `mild_weather`: the climate normal's mean high in this band, and dry enough. */
export const MILD_TEMP_MAX = { min: 20, max: 29 } as const;
export const MILD_MAX_DAILY_RAIN_MM = 3;
/** `after_holidays`: a school break ended within this many days before the start. */
export const AFTER_HOLIDAYS_DAYS = 7;
/** `before_spring_break`: the next school break starts within this many days after the end. */
export const BEFORE_BREAK_DAYS = 14;
/** The months a school break that starts then counts as spring break. */
const SPRING_BREAK_MONTHS = new Set([2, 3, 4]);
/** Seasons that make a window worth naming: festivals and the holiday seasons. */
const FESTIVAL_KINDS = new Set([
  "special_event",
  "halloween",
  "christmas",
  "summer_nights",
]);

export type WhenBasis = "forecast" | "mixed" | "last_year" | "none";

export type WhenReason =
  | "no_school_break"
  | "after_holidays"
  | "before_spring_break"
  | "festival"
  | "long_days"
  | "mild_weather";

export type WhenWarning =
  | "school_break"
  | "hurricane_season"
  | "hurricane_peak"
  | "extreme_heat"
  | "party_nights"
  | "fireworks_missed"
  | "hours_not_published"
  | "estimate_last_year"
  | "water_park_closed";

/** One school break of a holiday region, weekends at its edges included. */
export interface SchoolBreakPeriod {
  from: string;
  to: string;
  name: string;
}

export interface WhenWithin {
  code: string;
  name: string;
  country: string;
  /** The last day the holiday data covers for the region; later breaks are unknown. */
  through?: string | null;
  periods: SchoolBreakPeriod[];
}

export interface WhenQuery {
  from: string;
  to: string;
  days: number;
  limit: number;
  gap: number;
  /** The trip each window is planned for; its `from`/`to` are replaced per window. */
  plan: NormalizedPlanQuery;
  within?: WhenWithin;
}

export interface WhenInput {
  region: RegionConfig;
  /** The region calendar of exactly [from, to]. */
  calendar: InternalRegionCalendar;
  query: WhenQuery;
  /** The reference park's climate normals (`months[].climate`, `mild_weather`). */
  climate?: ClimateNormals | null;
  /** The reference park the climate is read at, named in `meta`. */
  climatePark?: string;
  /**
   * Milliseconds, for the time budget. Without it the search runs to the end,
   * which is what the specs rely on.
   */
  clock?: () => number;
}

export interface TripWhenWindow {
  rank: number;
  from: string;
  to: string;
  /** The plan's mean over its park days, without `penalties`. */
  headlinerWait?: number;
  /** Heat and hurricane minutes per park day the ranking added. */
  penalties?: PlanPenalties;
  rangeAverageWait?: number;
  basis: WhenBasis;
  reasons: WhenReason[];
  warnings: WhenWarning[];
  seasons: string[];
  closesEarlyDays: number;
  plan: {
    days: Array<{ date: string; kind: PlannedDay["kind"]; parks: string[] }>;
    unplaced?: string[];
  };
}

export interface TripWhenWeek {
  start: string;
  headlinerWait?: number;
  basis: WhenBasis;
}

export interface TripWhenClimate {
  tempMax: number;
  tempMin: number;
  precipitationMm: number;
  rainDays?: number;
  basis: "climate_normal";
  period: string;
}

export interface TripWhenMonth {
  month: string;
  headlinerWait?: number;
  basis: WhenBasis;
  climate?: TripWhenClimate;
  usSchoolBreakDays: Array<{ date: string; name: string }>;
  seasons: string[];
  partyNights: number;
  dayGuestHours?: number;
  hurricaneSeason: boolean;
}

export interface TripWhen {
  meta: {
    region: string;
    from: string;
    to: string;
    days: number;
    /** Set by the service; the search reads no clock for it. */
    computedAt?: string;
    forecastUntil?: string;
    within?: WhenWithin;
    /** Windows the range holds, how many were planned, and the range average's sample. */
    search: {
      /** False when the time budget stopped the search before it was proven. */
      exact: boolean;
      windows: number;
      candidates: number;
      planned: number;
      averageStride: number;
      averageSample: number;
    };
    climate?: { park: string; basis: "climate_normal"; period: string };
  };
  windows: TripWhenWindow[];
  weeks: TripWhenWeek[];
  months: TripWhenMonth[];
  seasons: TripCalendarSeason[];
}

type TripCalendarSeason = InternalRegionCalendar["seasons"][number];

// ── adapters to the plan ────────────────────────────────────────────────────
// The two places `/when` touches the plan's pricing and search. Everything
// below them goes through these, so a change to the plan lands here only.

/** One park on one day in minutes, exactly as the plan prices it, with its heat and hurricane terms. */
export function priceDay(cell: PlanCell, ctx: DayCostContext): DayCostParts {
  return dayCostParts(cell, ctx);
}

/** A window planned by the `/plan` route's own steps. */
export function planWindow(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
): OptimizedPlan {
  return optimizePlan(region, q, calendar).plan;
}

/** The visits the plan gives each park in a window (`split=auto` may add some). */
function windowVisits(
  region: RegionConfig,
  q: NormalizedPlanQuery,
  calendar: InternalRegionCalendar,
): Map<string, number> {
  return autoSplit(region, q, calendar);
}

// ── pricing helpers ─────────────────────────────────────────────────────────

const mean = (values: number[]): number | undefined =>
  values.length === 0
    ? undefined
    : values.reduce((s, v) => s + v, 0) / values.length;

const median = (values: number[]): number | undefined => {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

const round1 = (v: number): number => Math.round(v * 10) / 10;

const waitOf = (c: PlanCell): number | undefined =>
  c.exactWait ?? c.headlinerWait;

/** The hotel's perk factor for a park on a day, as the plan applies it. */
function perkFactor(slug: string, cell: PlanCell, rules: PerkRule[]): number {
  if (cell.status === "CLOSED") return 1;
  let factor = 1;
  for (const rule of rules) {
    if (!rule.parks.includes(slug)) continue;
    const applies =
      rule.needs === "always" ||
      (rule.needs === "extended_evening"
        ? cell.evening?.kind === "extended_evening"
        : !!cell.early);
    if (applies) factor *= 1 - PERK_SHARE[rule.perk];
  }
  return factor;
}

/** How one park is priced over a stretch of days: the plan's window context. */
interface ParkPricing {
  slug: string;
  type: DayCostContext["type"];
  medianHours?: number;
  neutralWait: number;
}

function pricingOver(
  region: RegionConfig,
  slug: string,
  days: readonly InternalCalendarDay[],
): ParkPricing {
  const cells = days
    .map((d) => d.parks[slug])
    .filter((c): c is TripParkDayInternal => !!c)
    .map(planCellOf);
  const waits = cells
    .filter((c) => c.status !== "CLOSED" && waitOf(c) != null)
    .map((c) => waitOf(c) as number);
  return {
    slug,
    type: region.parks.find((p) => p.slug === slug)?.type ?? "theme_park",
    medianHours: medianDayHours(cells),
    neutralWait: median(waits) ?? PlanCostModel.defaultNeutralWait,
  };
}

/** A park's day as `/when` reads it: what ranks it, what is shown, and the penalties in it. */
interface DayPrice {
  /** The plan's cost: ranks windows and bounds them. */
  rank: number;
  /** The cost without the `fireworks=want`, heat and hurricane terms: the shown figure. */
  plain: number;
  heat: number;
  hurricane: number;
}

const NO_DAY: DayPrice = {
  rank: Infinity,
  plain: Infinity,
  heat: 0,
  hurricane: 0,
};

/**
 * A park's day across windows. An UNKNOWN day (hours not published) is priced
 * as if open: the plan's penalty keeps it off a trip that has a published day,
 * but between windows it would only say "too far ahead", and a year's search
 * is mostly too far ahead. The shown figure leaves out the `fireworks=want`
 * penalty, which ranks a window but is no number of minutes, and the heat and
 * hurricane terms, which `penalties` reports beside it.
 */
function priceOf(
  q: NormalizedPlanQuery,
  rules: PerkRule[],
  pricing: ParkPricing,
  day: InternalCalendarDay,
): DayPrice {
  const raw = day.parks[pricing.slug];
  if (!raw) return NO_DAY;
  const cell = planCellOf(raw);
  const open: PlanCell =
    cell.status === "UNKNOWN" ? { ...cell, status: "OPERATING" } : cell;
  const ctx: DayCostContext = {
    type: pricing.type,
    medianHours: pricing.medianHours,
    neutralWait: pricing.neutralWait,
    fireworks: q.fireworks,
    perkFactor: perkFactor(pricing.slug, cell, rules),
    date: day.date,
  };
  const rank = priceDay(open, ctx);
  const any = priceDay(open, { ...ctx, fireworks: "any" });
  return {
    rank: rank.cost,
    plain: any.cost - any.heat - any.hurricane,
    heat: rank.heat,
    hurricane: rank.hurricane,
  };
}

/** A window's basis, best first: the tie-break between equal windows. */
const BASIS_RANK: Record<WhenBasis, number> = {
  forecast: 0,
  mixed: 1,
  last_year: 2,
  none: 3,
};

/** forecast | last_year | mixed over the cells a figure read; none without one. */
function basisOf(bases: Array<string | undefined>): WhenBasis {
  const known = bases
    .map((b) =>
      b === "forecast" || b === "long_range"
        ? "forecast"
        : b === "last_year"
          ? "last_year"
          : b === "mixed"
            ? "mixed"
            : null,
    )
    .filter((b): b is "forecast" | "last_year" | "mixed" => b !== null);
  if (known.length === 0) return "none";
  if (known.every((b) => b === "forecast")) return "forecast";
  if (known.every((b) => b === "last_year")) return "last_year";
  return "mixed";
}

/** Monday of a date's ISO week. */
export function isoWeekStart(date: string): string {
  const d = new Date(`${date}T12:00:00Z`);
  const back = (d.getUTCDay() + 6) % 7;
  d.setUTCDate(d.getUTCDate() - back);
  return d.toISOString().slice(0, 10);
}

const monthNumber = (date: string): number => Number(date.slice(5, 7));

const daysApart = (a: string, b: string): number =>
  Math.round(
    (Date.parse(`${b}T00:00:00Z`) - Date.parse(`${a}T00:00:00Z`)) / 86_400_000,
  );

// ── the search ──────────────────────────────────────────────────────────────

interface Evaluated {
  index: number;
  score: number;
  /** The score per visit: windows within {@link WHEN_TIE_MINUTES} of it are equal. */
  perVisit: number;
  basis: WhenBasis;
  basisRank: number;
  /** Heat and hurricane minutes per park day: comfort, between equal windows. */
  penaltyTotal: number;
  /** The plan's mean over its park days, without its penalties. */
  figure?: number;
  penalties?: PlanPenalties;
  plan: OptimizedPlan;
}

/** Start indexes whose window lies inside one of the school breaks (all without `within`). */
function allowedStarts(
  dates: readonly string[],
  length: number,
  within: WhenWithin | undefined,
): number[] {
  const out: number[] = [];
  for (let i = 0; i + length <= dates.length; i++) {
    const from = dates[i];
    const to = dates[i + length - 1];
    if (within && !within.periods.some((p) => p.from <= from && to <= p.to)) {
      continue;
    }
    out.push(i);
  }
  return out;
}

/**
 * The windows in score order, each at least `gap` days from every better one
 * kept; at most `limit`.
 */
export function selectWindows<T extends Rankable>(
  evaluated: readonly T[],
  limit: number,
  gap: number,
): T[] {
  const kept: T[] = [];
  for (const e of rankWindows(evaluated)) {
    if (kept.length >= limit) break;
    if (kept.every((k) => Math.abs(k.index - e.index) >= gap)) kept.push(e);
  }
  return kept;
}

interface Rankable {
  index: number;
  score: number;
  perVisit?: number;
  basisRank?: number;
  penaltyTotal?: number;
}

/**
 * The ranking's tiers. From the cheapest window left (by cost per visit), it
 * and every window within {@link WHEN_TIE_MINUTES} of it count as equal and
 * form a tier, ordered by comfort: forecast before mixed before last year,
 * then the lower heat and hurricane penalty, then the exact cost and the
 * earlier start. Then the next tier from the cheapest window left.
 */
function tiersOf<T extends Rankable>(
  evaluated: readonly T[],
): Array<{ start: number; items: T[] }> {
  const cost = (e: T) => e.perVisit ?? e.score;
  const left = [...evaluated].sort(
    (a, b) => cost(a) - cost(b) || a.index - b.index,
  );
  const tiers: Array<{ start: number; items: T[] }> = [];
  while (left.length > 0) {
    const start = cost(left[0]);
    const items = left.filter((e) => cost(e) <= start + WHEN_TIE_MINUTES);
    items.sort(
      (a, b) =>
        (a.basisRank ?? 0) - (b.basisRank ?? 0) ||
        (a.penaltyTotal ?? 0) - (b.penaltyTotal ?? 0) ||
        a.score - b.score ||
        a.index - b.index,
    );
    tiers.push({ start, items });
    left.splice(0, items.length);
  }
  return tiers;
}

/** The ranking order: the tiers one after the other. */
export function rankWindows<T extends Rankable>(evaluated: readonly T[]): T[] {
  return tiersOf(evaluated).flatMap((t) => t.items);
}

/** The cost per visit the tier holding `e` starts from. */
function tierStartOf<T extends Rankable>(
  evaluated: readonly T[],
  e: T,
): number {
  const tier = tiersOf(evaluated).find((t) => t.items.includes(e));
  return tier ? tier.start : (e.perVisit ?? e.score);
}

/** The search over every window; see the module comment. */
export function findTripDates(input: WhenInput): TripWhen {
  const { region, calendar, query } = input;
  const q = query.plan;
  const rules = region.perks[q.hotel] ?? [];
  const days = calendar.days;
  const dates = days.map((d) => d.date);
  const length = query.days;
  const slugs = [...new Set(q.parks.map((p) => p.slug))];

  const windowCalendar = (i: number): InternalRegionCalendar => ({
    ...calendar,
    meta: { ...calendar.meta, from: dates[i], to: dates[i + length - 1] },
    days: days.slice(i, i + length),
  });
  const windowQuery = (i: number): NormalizedPlanQuery => ({
    ...q,
    from: dates[i],
    to: dates[i + length - 1],
  });

  // Per window and park: the plan's context, priced once.
  const pricingCache = new Map<number, Map<string, ParkPricing>>();
  const pricingAt = (i: number): Map<string, ParkPricing> => {
    let hit = pricingCache.get(i);
    if (!hit) {
      const slice = days.slice(i, i + length);
      hit = new Map(slugs.map((s) => [s, pricingOver(region, s, slice)]));
      pricingCache.set(i, hit);
    }
    return hit;
  };

  /**
   * A score no plan of the window can beat: the plan with its rules dropped
   * (rest days, blocks, repeats apart), i.e. the cheapest assignment of the
   * visits to distinct days, a visit left out at {@link UNPLACED_VISIT_COST}.
   * Where the plan may pair two parks on a hop day, each visit on its park's
   * cheapest days at the hop day's half price instead.
   */
  const visitsCache = new Map<number, Map<string, number>>();
  const visitsAt = (i: number): Map<string, number> => {
    let hit = visitsCache.get(i);
    if (!hit) {
      hit = windowVisits(region, windowQuery(i), windowCalendar(i));
      visitsCache.set(i, hit);
    }
    return hit;
  };
  const visitCount = (i: number): number =>
    Math.max(
      1,
      [...visitsAt(i).values()].reduce((s, v) => s + v, 0),
    );

  const lowerBound = (i: number): number => {
    const pricing = pricingAt(i);
    const visits = visitsAt(i);
    const open: number[] = [];
    for (let k = 0; k < length; k++) {
      const free =
        (k === 0 && q.arrival === "free") ||
        (k === length - 1 && length > 1 && q.departure === "free");
      if (!free) open.push(i + k);
    }
    const total = [...visits.values()].reduce((s, v) => s + v, 0);
    const costsOf = (slug: string) =>
      open.map(
        (d) =>
          priceOf(q, rules, pricing.get(slug) as ParkPricing, days[d]).rank,
      );
    // The plan pairs parks on a hop day only when the visits outnumber the days.
    if (q.hop && total > open.length) {
      let bound = 0;
      for (const slug of slugs) {
        const costs = costsOf(slug)
          .map((c) =>
            Math.min(c, 0.5 * c + PlanCostModel.hopTransferPenalty / 2),
          )
          .sort((a, b) => a - b);
        for (let v = 0; v < (visits.get(slug) ?? 1); v++) {
          bound += Math.min(costs[v] ?? Infinity, UNPLACED_VISIT_COST);
        }
      }
      return bound;
    }
    const matrix: number[][] = [];
    for (const slug of slugs) {
      const row = [
        ...costsOf(slug),
        ...new Array<number>(total).fill(UNPLACED_VISIT_COST),
      ];
      for (let v = 0; v < (visits.get(slug) ?? 1); v++) matrix.push(row);
    }
    return solveAssignment(matrix).cost;
  };

  const planned = new Map<number, Evaluated>();
  const evaluate = (i: number): Evaluated => {
    const hit = planned.get(i);
    if (hit) return hit;
    const plan = planWindow(region, windowQuery(i), windowCalendar(i));
    const pricing = pricingAt(i);
    let score = 0;
    let shown = 0;
    let heat = 0;
    let hurricane = 0;
    let parkDays = 0;
    const bases: Array<string | undefined> = [];
    for (const day of plan.days) {
      if (day.parks.length === 0) continue;
      const calendarDay = days[i + daysApart(dates[i], day.date)];
      const prices = day.parks.map((v) => {
        bases.push(calendarDay.parks[v.slug]?.basis);
        return priceOf(
          q,
          rules,
          pricing.get(v.slug) as ParkPricing,
          calendarDay,
        );
      });
      const hop = prices.length > 1;
      const share = hop ? 0.5 : 1;
      const move = hop ? PlanCostModel.hopTransferPenalty : 0;
      score += prices.reduce((sum, p) => sum + share * p.rank, move);
      shown += prices.reduce((sum, p) => sum + share * p.plain, move);
      heat += prices.reduce((sum, p) => sum + share * p.heat, 0);
      hurricane += prices.reduce((sum, p) => sum + share * p.hurricane, 0);
      parkDays++;
    }
    score += UNPLACED_VISIT_COST * plan.unplaced.length;
    const basis = basisOf(bases);
    const penalties = penaltiesOf(heat, hurricane, parkDays);
    const evaluated: Evaluated = {
      index: i,
      score,
      perVisit: score / visitCount(i),
      basis,
      basisRank: BASIS_RANK[basis],
      penaltyTotal: (penalties?.heat ?? 0) + (penalties?.hurricane ?? 0),
      ...(parkDays > 0 && { figure: shown / parkDays }),
      ...(penalties && { penalties }),
      plan,
    };
    planned.set(i, evaluated);
    return evaluated;
  };

  const allStarts = allowedStarts(dates, length, undefined);
  const starts = query.within
    ? allowedStarts(dates, length, query.within)
    : allStarts;
  // In order of the bound per visit, the unit the ranking's tiers use.
  const candidates = starts
    .map((index) => ({
      index,
      perVisit: lowerBound(index) / visitCount(index),
    }))
    .sort((a, b) => a.perVisit - b.perVisit || a.index - b.index);

  const clock = input.clock;
  const started = clock?.() ?? 0;
  const over = (ms: number) => !!clock && clock() - started > ms;

  const searchedWindows: Evaluated[] = [];
  let kept: Evaluated[] = [];
  let exact = true;
  for (const candidate of candidates) {
    if (kept.length >= query.limit && over(SEARCH_BUDGET_MS)) {
      exact = false;
      break;
    }
    if (kept.length >= query.limit) {
      const last = kept[kept.length - 1];
      // A candidate past the last kept's tier by its bound cannot enter that
      // tier or any before it, nor start one: neither can any after it.
      const start = tierStartOf(searchedWindows, last);
      if (candidate.perVisit > start + WHEN_TIE_MINUTES) break;
    }
    searchedWindows.push(evaluate(candidate.index));
    kept = selectWindows(searchedWindows, query.limit, query.gap);
  }
  const searched = planned.size;

  // The range average: every `stride`-th window of the whole range.
  const stride = Math.max(
    1,
    Math.ceil(allStarts.length / RANGE_AVERAGE_SAMPLES),
  );
  const sample: number[] = [];
  for (let k = 0; k < allStarts.length; k += stride) {
    if (sample.length > 0 && over(SEARCH_BUDGET_MS + AVERAGE_BUDGET_MS)) break;
    const figure = evaluate(allStarts[k]).figure;
    if (figure != null) sample.push(figure);
  }
  const rangeAverage = mean(sample);

  const rangeHours = mean(
    days.flatMap((d) => headlinerHours(region, slugs, d)),
  );
  const windows = kept.map((e, rank) =>
    describeWindow(input, e, rank + 1, rangeAverage, rangeHours),
  );
  const weeks = weekStrip(input, rules);
  const months = monthStrip(input, weeks);

  const named = new Set([
    ...windows.flatMap((w) => w.seasons),
    ...months.flatMap((m) => m.seasons),
  ]);
  return {
    meta: {
      region: region.slug,
      from: query.from,
      to: query.to,
      days: length,
      ...(calendar.meta.forecastUntil && {
        forecastUntil: calendar.meta.forecastUntil,
      }),
      ...(query.within && { within: query.within }),
      search: {
        exact,
        windows: allStarts.length,
        candidates: starts.length,
        planned: searched,
        averageStride: stride,
        averageSample: sample.length,
      },
      ...(input.climate &&
        input.climatePark && {
          climate: {
            park: input.climatePark,
            basis: "climate_normal" as const,
            period: CLIMATE_NORMAL_PERIOD,
          },
        }),
    },
    windows,
    weeks,
    months,
    seasons: calendar.seasons.filter((s) => named.has(String(s.id))),
  };
}

// ── a window's answer ───────────────────────────────────────────────────────

/** The day-guest hours of the chosen headliner parks on a day. */
function headlinerHours(
  region: RegionConfig,
  slugs: readonly string[],
  day: InternalCalendarDay,
): number[] {
  return slugs
    .filter((s) => region.parks.find((p) => p.slug === s)?.headliner)
    .map((s) => day.parks[s])
    .filter((c) => c?.status === "OPERATING" && c.dayHours)
    .map((c) => c.dayHours as number);
}

/** The ids of the seasons the chosen parks' days carry. */
function seasonsOn(
  days: readonly InternalCalendarDay[],
  slugs: readonly string[],
): string[] {
  const ids = new Set<string>();
  for (const day of days) {
    for (const slug of slugs) {
      for (const id of day.parks[slug]?.seasons ?? []) ids.add(id);
    }
  }
  return [...ids].sort();
}

/** The climate normals of the given dates, from the `MM-DD` table. */
function normalsOf(
  climate: ClimateNormals | null | undefined,
  dates: readonly string[],
) {
  if (!climate) return [];
  return dates
    .map((d) => climate[d.slice(5, 10)])
    .filter((n): n is NonNullable<typeof n> => !!n);
}

function describeWindow(
  input: WhenInput,
  e: Evaluated,
  rank: number,
  rangeAverage: number | undefined,
  rangeHours: number | undefined,
): TripWhenWindow {
  const { region, calendar, query } = input;
  const days = calendar.days;
  const slugs = [...new Set(query.plan.parks.map((p) => p.slug))];
  const window = days.slice(e.index, e.index + query.days);
  const from = window[0].date;
  const to = window[window.length - 1].date;
  const byDate = new Map(window.map((d) => [d.date, d]));
  const visits = e.plan.days.flatMap((d) =>
    d.parks.map((v) => byDate.get(d.date)?.parks[v.slug]),
  );

  const reasons: WhenReason[] = [];
  const schoolDays = window.filter((d) => d.isSchoolVacation);
  const unknownSchool = window.some((d) => d.schoolVacationUnknown);
  if (schoolDays.length === 0 && !unknownSchool) {
    reasons.push("no_school_break");
    const before = days.slice(
      Math.max(0, e.index - AFTER_HOLIDAYS_DAYS),
      e.index,
    );
    if (before.some((d) => d.isSchoolVacation)) reasons.push("after_holidays");
    const after = days.slice(
      e.index + query.days,
      e.index + query.days + BEFORE_BREAK_DAYS,
    );
    const next = after.find((d) => d.isSchoolVacation);
    if (next && SPRING_BREAK_MONTHS.has(monthNumber(next.date))) {
      reasons.push("before_spring_break");
    }
  }
  const seasons = seasonsOn(window, slugs);
  const seasonById = new Map(calendar.seasons.map((s) => [String(s.id), s]));
  if (
    seasons.some((id) => {
      const s = seasonById.get(id);
      return (
        !!s && FESTIVAL_KINDS.has(String(s.kind)) && s.separateTicket !== true
      );
    })
  ) {
    reasons.push("festival");
  }
  const hours = mean(window.flatMap((d) => headlinerHours(region, slugs, d)));
  if (
    hours != null &&
    rangeHours != null &&
    hours >= rangeHours + LONG_DAYS_HOURS
  ) {
    reasons.push("long_days");
  }
  const normals = normalsOf(
    input.climate,
    window.map((d) => d.date),
  );
  const highs = normals
    .map((n) => n.temperatureMax)
    .filter((v): v is number => v != null);
  const rain = normals
    .map((n) => n.precipitationSum)
    .filter((v): v is number => v != null);
  const high = mean(highs);
  const wet = mean(rain);
  if (
    high != null &&
    wet != null &&
    high >= MILD_TEMP_MAX.min &&
    high <= MILD_TEMP_MAX.max &&
    wet <= MILD_MAX_DAILY_RAIN_MM
  ) {
    reasons.push("mild_weather");
  }

  const warnings: WhenWarning[] = [];
  if (schoolDays.length > 0) warnings.push("school_break");
  if (window.some((d) => hurricaneWeight(d.date) > 0)) {
    warnings.push("hurricane_season");
  }
  if (window.some((d) => isHurricanePeak(d.date))) {
    warnings.push("hurricane_peak");
  }
  const closesEarlyDays = visits.filter((c) => c?.closesEarly).length;
  if (
    visits.some((c) => c?.closesEarly && c.evening?.kind === "ticketed_event")
  ) {
    warnings.push("party_nights");
  }
  if (e.plan.days.some((d) => d.warnings.includes("extreme_heat"))) {
    warnings.push("extreme_heat");
  }
  if (e.plan.days.some((d) => d.warnings.includes("fireworks_missed"))) {
    warnings.push("fireworks_missed");
  }
  if (visits.some((c) => c?.status === "UNKNOWN")) {
    warnings.push("hours_not_published");
  }
  if (visits.some((c) => c?.basis === "last_year")) {
    warnings.push("estimate_last_year");
  }
  const waterParks = slugs.filter(
    (s) => region.parks.find((p) => p.slug === s)?.type === "water_park",
  );
  if (
    waterParks.some((s) =>
      window.some(
        (d) => d.parks[s]?.status === "CLOSED" && !!d.parks[s]?.closedBy,
      ),
    ) ||
    e.plan.unplaced.some((u) => waterParks.includes(u.slug))
  ) {
    warnings.push("water_park_closed");
  }

  const unplaced = e.plan.unplaced.map((u) => u.slug);
  return {
    rank,
    from,
    to,
    ...(e.figure != null && { headlinerWait: Math.round(e.figure) }),
    ...(e.penalties && { penalties: e.penalties }),
    ...(rangeAverage != null && {
      rangeAverageWait: Math.round(rangeAverage),
    }),
    basis: e.basis,
    reasons,
    warnings,
    seasons,
    closesEarlyDays,
    plan: {
      days: e.plan.days.map((d) => ({
        date: d.date,
        kind: d.kind,
        parks: d.parks.map((v) => v.slug),
      })),
      ...(unplaced.length > 0 && { unplaced }),
    },
  };
}

// ── the year strip ──────────────────────────────────────────────────────────

/** Unrounded weekly figures, kept for the months. */
interface WeekFigure extends TripWhenWeek {
  exact?: number;
  dates: string[];
}

/**
 * Per ISO week in the range: each chosen park on its cheapest day of the week
 * (the week priced as a window of its own), meaned over the parks open that
 * week. An overview of how the year runs, not a plan.
 */
function weekStrip(input: WhenInput, rules: PerkRule[]): WeekFigure[] {
  const { region, calendar, query } = input;
  const q = query.plan;
  const slugs = [...new Set(q.parks.map((p) => p.slug))];
  const byWeek = new Map<string, InternalCalendarDay[]>();
  for (const day of calendar.days) {
    const start = isoWeekStart(day.date);
    byWeek.set(start, [...(byWeek.get(start) ?? []), day]);
  }
  return [...byWeek].map(([start, weekDays]) => {
    const costs: number[] = [];
    const bases: Array<string | undefined> = [];
    for (const slug of slugs) {
      const pricing = pricingOver(region, slug, weekDays);
      let best = Infinity;
      let basis: string | undefined;
      for (const day of weekDays) {
        const c = priceOf(q, rules, pricing, day).plain;
        if (c < best) {
          best = c;
          basis = day.parks[slug]?.basis;
        }
      }
      if (Number.isFinite(best)) {
        costs.push(best);
        bases.push(basis);
      }
    }
    const exact = mean(costs);
    return {
      start,
      ...(exact != null && { headlinerWait: Math.round(exact) }),
      basis: basisOf(bases),
      exact,
      dates: weekDays.map((d) => d.date),
    };
  });
}

/** Per calendar month the range touches: the strip's figures and the month's facts. */
function monthStrip(input: WhenInput, weeks: WeekFigure[]): TripWhenMonth[] {
  const { region, calendar, query } = input;
  const slugs = [...new Set(query.plan.parks.map((p) => p.slug))];
  const byMonth = new Map<string, InternalCalendarDay[]>();
  for (const day of calendar.days) {
    const ym = day.date.slice(0, 7);
    byMonth.set(ym, [...(byMonth.get(ym) ?? []), day]);
  }
  return [...byMonth].map(([month, monthDays]) => {
    // The weeks' figures, each weighted by its days in this month.
    let weighted = 0;
    let weight = 0;
    const bases: WhenBasis[] = [];
    for (const week of weeks) {
      const inMonth = week.dates.filter((d) => d.startsWith(month)).length;
      if (inMonth === 0) continue;
      if (week.exact != null) {
        weighted += week.exact * inMonth;
        weight += inMonth;
      }
      bases.push(week.basis);
    }
    const breaks = monthDays
      .filter((d) => d.isSchoolVacation || d.isHoliday)
      .map((d) => ({ date: d.date, name: (d.holidays ?? []).join(", ") }));
    const partyNights = monthDays.reduce(
      (n, d) =>
        n +
        slugs.filter(
          (s) =>
            d.parks[s]?.closesEarly &&
            d.parks[s]?.evening?.kind === "ticketed_event",
        ).length,
      0,
    );
    const hours = mean(
      monthDays.flatMap((d) => headlinerHours(region, slugs, d)),
    );
    const climate = monthClimate(input.climate, month);
    return {
      month,
      ...(weight > 0 && { headlinerWait: Math.round(weighted / weight) }),
      basis: basisOf(bases),
      ...(climate && { climate }),
      usSchoolBreakDays: breaks,
      seasons: seasonsOn(monthDays, slugs),
      partyNights,
      ...(hours != null && { dayGuestHours: round1(hours) }),
      hurricaneSeason: hurricaneWeight(`${month}-15`) > 0,
    };
  });
}

/**
 * A calendar month's normals: the mean high and low and the month's total
 * precipitation (the mean daily normal times the month's days, which is their
 * sum when every day has one). No `rainDays`: a normal is a mean per calendar
 * day, and a mean of 3 mm does not say on how many days it rains.
 */
function monthClimate(
  climate: ClimateNormals | null | undefined,
  month: string,
): TripWhenClimate | undefined {
  if (!climate) return undefined;
  const mm = month.slice(5, 7);
  const entries = Object.entries(climate)
    .filter(([md]) => md.startsWith(`${mm}-`) && md !== "02-29")
    .map(([, n]) => n);
  const highs = entries
    .map((n) => n.temperatureMax)
    .filter((v): v is number => v != null);
  const lows = entries
    .map((n) => n.temperatureMin)
    .filter((v): v is number => v != null);
  const rain = entries
    .map((n) => n.precipitationSum)
    .filter((v): v is number => v != null);
  const high = mean(highs);
  const low = mean(lows);
  if (high == null || low == null || rain.length === 0) return undefined;
  return {
    tempMax: round1(high),
    tempMin: round1(low),
    precipitationMm: Math.round(
      (mean(rain) as number) *
        new Date(Date.UTC(2027, Number(mm), 0)).getUTCDate(),
    ),
    basis: "climate_normal",
    period: CLIMATE_NORMAL_PERIOD,
  };
}
