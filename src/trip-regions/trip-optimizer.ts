/**
 * The trip optimizer: which park goes on which day of a vacation so that the
 * headliners queue least, under the visitor's rules (blocks per resort, rest
 * days, park hopping, fireworks, hotel perks, pinned days).
 *
 * Pure: no Nest, no clock, no I/O. `TripPlanService` reads the region calendar
 * and hands this module plain cells; everything here is a function of its input,
 * which is what lets the specs check it against brute force.
 *
 * The objective is the sum over planned park visits of a cost per (park, day),
 * see {@link PlanCostModel}. The search is a min-cost assignment of visits to
 * days (`hungarian.ts`) inside every "structure" the options allow — a pairing
 * of parks for hop days, an order and boundaries of resort blocks — searched in
 * order of the structure's lower bound, so a structure that cannot beat the best
 * plan so far is never solved twice. Rest-day patterns are enumerated exactly
 * while there are at most {@link REST_EXACT_LIMIT} of them and searched locally
 * past that. A plan is reported `exact` only when nothing on the way was cut
 * short; the contract (docs/frontend/trip-regions.md) says how to read it.
 */
import { addIsoDays } from "../common/utils/date.util";
import { solveAssignment } from "./hungarian";
import type { PerkKind, PerkRule, TripParkType } from "./regions";

/** A trip runs at most three weeks, arrival and departure included. */
export const MAX_TRIP_DAYS = 21;
/** Park visits per plan, repeats included. */
export const MAX_PARK_DAYS = 16;
export const MAX_VISITS_PER_PARK = 3;
/** `rest=auto`: at most this many park days in a row before a rest day. */
export const DEFAULT_MAX_STREAK = 4;

/** Rest patterns up to this count are searched exhaustively. */
export const REST_EXACT_LIMIT = 5000;
/** Assignment solves per plan; past it the search stops and says so. */
const SOLVE_BUDGET = 20000;
/** Pairings of parks for hop days tried per plan. */
const HOP_COMBO_LIMIT = 200;
/** Local-search evaluations of rest patterns per structure. */
const REST_LOCAL_SEARCH_EVALS = 600;
/** Combinations of the owned tickets' free starts tried per pairing. */
const START_COMBO_LIMIT = 3000;

/**
 * The share a hotel perk takes off a park's cost on the day it applies. Fixed
 * shares, not measurements: an early hour at a Disney park is worth a ride or
 * two before the day guests arrive, an extended evening more, and Express
 * Unlimited at the two Universal parks most of the queue.
 */
export const PERK_SHARE: Record<PerkKind, number> = {
  early_entry: 0.05,
  early_park_admission: 0.05,
  extended_evening: 0.1,
  express_included: 0.6,
};

/**
 * The cost model, in minutes of headliner standby (the calendar's
 * `headlinerWait`), so that every constant below reads as "worth so many
 * minutes".
 */
export const PlanCostModel = {
  /** An UNKNOWN day (hours not published) is used only when nothing else fits. */
  unknownDayPenalty: 1000,
  /** An open day without a figure costs the park's median day, plus this. */
  noFigurePenalty: 5,
  /** The median when a park has no figure in the whole window. */
  defaultNeutralWait: 30,
  /** Water parks and Discovery Cove: no queue figure, a flat day. */
  flatDayCost: 30,
  /** A short day (party night) costs up to this much more. */
  hoursFactorCap: 1.5,
  /**
   * `fireworks=want`: a night the day guests lose the evening show to a
   * ticketed event is used only when nothing else fits.
   */
  fireworksMissedPenalty: 500,
  /**
   * Two visits of one park on consecutive days. The search keeps repeats a day
   * apart as a rule; this is what a plan pays where no placement can.
   */
  adjacentRepeatPenalty: 5000,
  /** A hop day: half of each park plus the move between them. */
  hopTransferPenalty: 10,
  /** Flat days: warm and dry is cheaper, cool or wet dearer. */
  warmDryFactor: 0.8,
  coolWetFactor: 1.25,
  /** `runner_up_close`: another day within this many minutes. */
  runnerUpMinutes: 5,
} as const;

/**
 * Extreme heat at a theme park: the day's cost rises by `perDegree` for each
 * degree of the day's high above the threshold, up to `cap`. A forecast high
 * counts from 35 °C, a day clearly hotter than Orlando's usual summer day: Magic
 * Kingdom's 2015–2024 daily normals (ERA5) top out at 32.1 °C in late June and
 * run 30.9 °C at the June–October median. Past the forecast the climate normal
 * stands in, from 31.5 °C (the top fifth of those June–October normals, "typically
 * very hot") at half the weight: a normal is a 70-day mean, so it is flatter
 * than any one day and says less about the day the visitor gets. Water parks
 * and Discovery Cove take no heat term: a hot dry day is already their good
 * one (`weatherFactor`). Contract: docs/frontend/trip-regions.md.
 */
export const HeatModel = {
  forecastFromC: 35,
  normalFromC: 31.5,
  perDegree: 0.08,
  normalWeight: 0.5,
  cap: 1.4,
} as const;

/**
 * The Atlantic hurricane season, 1 June to 30 November, weighted by half
 * month to the shape of the National Hurricane Center's climatology
 * (nhc.noaa.gov/climo, 1991–2020): the first named storm usually forms in
 * mid to late June, the first hurricane in early to mid August; the peak is
 * 10 September, with most activity between mid-August and mid-October. The
 * weights are curated relative to that peak, not NHC figures, and never a
 * forecast. `[month, firstHalf, secondHalf]`, the halves split after the 15th
 * (the 14th in August, so the peak starts on 15 August).
 */
export const HURRICANE_WEIGHTS: ReadonlyArray<[number, number, number]> = [
  [6, 0.05, 0.1],
  [7, 0.15, 0.2],
  [8, 0.4, 0.8],
  [9, 1, 1],
  [10, 0.8, 0.4],
  [11, 0.15, 0.05],
];

/** What a park day at the season's peak costs more, in minutes of headliner wait. */
export const HURRICANE_PEAK_MINUTES = 8;

/** The hurricane season's weight on a date: 0 outside 1 June … 30 November. */
export function hurricaneWeight(date: string): number {
  const month = Number(date.slice(5, 7));
  const day = Number(date.slice(8, 10));
  const row = HURRICANE_WEIGHTS.find(([m]) => m === month);
  if (!row) return 0;
  const split = month === 8 ? 14 : 15;
  return day <= split ? row[1] : row[2];
}

/** The season's peak, mid-August to mid-October (15 August … 15 October). */
export function isHurricanePeak(date: string): boolean {
  const md = date.slice(5, 10);
  return md >= "08-15" && md <= "10-15";
}

/** The hurricane season's minutes on a date, the same for every park that day. */
export function hurricaneMinutes(date: string): number {
  return HURRICANE_PEAK_MINUTES * hurricaneWeight(date);
}

/**
 * How much hotter than its threshold a day's high is, and the factor it puts
 * on a theme park's cost; null when it is not extreme or nothing is known.
 * The forecast high where the calendar has one, else the day's climate normal.
 */
export function heatOf(weather: PlanCell["weather"]): {
  excess: number;
  factor: number;
  basis: "forecast" | "climate_normal";
} | null {
  if (!weather || weather.tempMax == null) return null;
  const basis =
    weather.basis === "climate_normal" ? "climate_normal" : "forecast";
  const from =
    basis === "forecast" ? HeatModel.forecastFromC : HeatModel.normalFromC;
  const excess = weather.tempMax - from;
  if (excess <= 0) return null;
  const weight = basis === "forecast" ? 1 : HeatModel.normalWeight;
  return {
    excess,
    factor: Math.min(HeatModel.cap, 1 + HeatModel.perDegree * weight * excess),
    basis,
  };
}

export type PlanDayStatus = "OPERATING" | "CLOSED" | "UNKNOWN";
export type PlanBasis = "forecast" | "long_range" | "last_year" | "none";

/** One park on one day, as the optimizer needs it. */
export interface PlanCell {
  status: PlanDayStatus;
  headlinerWait?: number;
  /** `headlinerWait` before rounding; the cost and every comparison read it. */
  exactWait?: number;
  crowdLevel?: string;
  basis: PlanBasis;
  /** Hours the day guest has: the OPERATING row's open to close. */
  dayHours?: number;
  /**
   * The park's usual day-guest hours around this date, from the calendar
   * (independent of the window asked for); preferred over a window median.
   */
  usualHours?: number;
  early?: { kind: string };
  evening?: { kind: "ticketed_event" | "extended_evening" };
  weather?: {
    tempMax?: number;
    precipitationMm?: number;
    icon?: number;
    /** `climate_normal` past the forecast's reach; a forecast otherwise. */
    basis?: "forecast" | "climate_normal";
  };
}

export interface PlanDayInfo {
  date: string;
  /** 0 = Sunday … 6 = Saturday. */
  weekday: number;
  isHoliday: boolean;
  isSchoolVacation: boolean;
}

export interface PlanPark {
  slug: string;
  group: string;
  type: TripParkType;
  visits: number;
}

/** `slug@date` pins a visit, `rest@date` a rest day. */
export interface PlanLock {
  target: string;
  date: string;
}

export type RestOption =
  { mode: "auto" } | { mode: "none" } | { mode: "max"; n: number };

/** `want`: keep the evening show (no party nights); `any`: price the hours only. */
export type FireworksOption = "want" | "any";

export interface PlanOptions {
  arrival: "free" | "park";
  departure: "free" | "park";
  blocks: boolean;
  /** The groups `blocks` keeps together (the resorts; day trips stay free). */
  blockGroups: string[];
  /**
   * `blocks` holds for the resorts of owned tickets too (written in the query);
   * otherwise their windows replace it and they may mix.
   */
  blockOwned?: boolean;
  fireworks: FireworksOption;
  rest: RestOption;
  hop: boolean;
  /** Parks that may share a hop day: same resort, a short leg. */
  hopPairs: Array<[string, string]>;
  perkRules: PerkRule[];
  locks: PlanLock[];
}

/**
 * A ticket the visitor already has, as the optimizer keeps it: the parks a
 * day of it covers, its days and window, its caps and hop pairs. Resolved
 * from the catalog by `trip-tickets.ts`; contract:
 * docs/frontend/trip-regions.md#tickets.
 */
export interface PlanTicket {
  id: string;
  /** Parks a ticket day covers. */
  parks: string[];
  /** Visits that use no ticket day (UK Magic water parks), at most `visits`. */
  allowance?: { parks: string[]; visits: number };
  /** Days it may be used on; Infinity for a pass. */
  days: number;
  /** Calendar days its use may spread over from its start; Infinity: no window. */
  window: number;
  /** A fixed start date, which may lie before the trip; absent: the plan picks the start. */
  start?: string;
  /** The dates a picked start may be (on sale); absent: any day of the trip. */
  startOptions?: string[];
  /** Parks used only on `reservedDay` (Discovery Cove); the window must hold that day. */
  reservedDayParks?: string[];
  reservedDay?: string;
  /** At most `max` days on which any of `parks` is visited (a hop day between two is one). */
  caps: Array<{ parks: string[]; max: number }>;
  /** Parks one hop day of this ticket may combine. */
  hopPairs: Array<[string, string]>;
}

export interface PlanInput {
  days: PlanDayInfo[];
  parks: PlanPark[];
  /** cells[slug][date]; a missing cell reads as UNKNOWN. */
  cells: Record<string, Record<string, PlanCell>>;
  options: PlanOptions;
  /**
   * The visitor's own tickets. A park one of them covers is planned inside
   * its rules; the others are planned as before ("plan first").
   */
  tickets?: PlanTicket[];
}

export type PlanReason =
  | "quietest_in_window"
  | "runner_up_close"
  | "weekday"
  | "holiday_ends"
  | PerkKind
  | "water_park_weather"
  | "busiest_day_spared"
  | "rest_after_streak"
  | "locked";

export type PlanWarning =
  | "party_night_short_hours"
  | "fireworks_missed"
  | "hours_not_published"
  | "estimate_last_year"
  | "long_range"
  | "no_forecast"
  | "adjacent_repeat"
  | "long_streak"
  | "extreme_heat"
  | "hurricane_season"
  | "hurricane_peak"
  | "outside_ticket_window";

export interface PlannedParkVisit {
  slug: string;
  part: "full" | "half";
  /** On a hop day each park carries its own figures. */
  headlinerWait?: number;
  crowdLevel?: string;
  basis?: PlanBasis;
  perks?: PerkKind[];
}

export interface PlannedDay {
  date: string;
  kind: "arrival" | "park" | "hop" | "rest" | "departure";
  parks: PlannedParkVisit[];
  headlinerWait?: number;
  crowdLevel?: string;
  basis?: PlanBasis;
  perks?: PerkKind[];
  reasons: PlanReason[];
  warnings: PlanWarning[];
  /** The owned ticket the day uses (its id); plan first's days get theirs later. */
  ticket?: string;
  /** The day's number among the ticket's days. */
  ticketDay?: number;
  /** A visit on the ticket's allowance, which uses no ticket day. */
  ticketAllowance?: true;
}

export interface PlanAlternative {
  date: string;
  headlinerWait?: number;
  crowdLevel?: string;
}

export interface PlannedParkSummary {
  slug: string;
  dates: string[];
  alternatives: PlanAlternative[];
}

/** A plan whose order saves less than this per ride is "any order will do". */
export const ORDER_MATTERS_MINUTES = 3;

export interface PlanTotals {
  headlinerWait?: number;
  averageDayWait?: number;
  savedMinutesPerRide?: number;
  /** The typical error of the planned days' forecasts, per ride (`trip-plan.assembly.ts`). */
  expectedError?: number;
  /** The planned order saves at least {@link ORDER_MATTERS_MINUTES} per ride. */
  orderMatters: boolean;
  /**
   * What heat and the hurricane season added to the plan's cost, in minutes
   * per park day, beside `headlinerWait`, which they leave alone.
   */
  penalties?: PlanPenalties;
  parkDays: number;
  visits: number;
  restDays: number;
}

/** Minutes of cost per park day that are not waiting: what ranked a plan lower. */
export interface PlanPenalties {
  heat?: number;
  hurricane?: number;
}

/** An owned ticket as the plan used it. */
export interface PlannedTicket {
  id: string;
  /** The window's first day: the given start, or the first day the plan uses it. */
  start?: string;
  /** The plan picked `start`. */
  startChosen?: true;
  /** The window's last day. */
  end?: string;
  usedDays: number;
  allowanceVisits: number;
}

export type UnplacedReason = "no_open_day" | "no_day_left" | "no_ticket_day";

export interface OptimizedPlan {
  exact: boolean;
  /**
   * Rules the plan had to loosen: `blocks_relaxed`, `streak_relaxed`,
   * `ticket_window_relaxed`.
   */
  relaxed: string[];
  days: PlannedDay[];
  parks: PlannedParkSummary[];
  totals: PlanTotals;
  unplaced: Array<{ slug: string; reason: UnplacedReason }>;
  /** The owned tickets in input order; empty without any. */
  tickets: PlannedTicket[];
  ignoredLocks: Array<{
    lock: string;
    reason: "outside_trip" | "closed" | "day_taken" | "no_visit_left";
  }>;
}

// ── internals ───────────────────────────────────────────────────────────────

interface Unit {
  slug: string;
  visit: number;
  /** The owned ticket (its index) the visit uses; absent: plan first. */
  ticket?: number;
  /** On that ticket's allowance: it uses no ticket day. */
  allowance?: boolean;
}

interface Row {
  units: Unit[];
  /** Trip-day index a lock pins this row to. */
  lockedDay?: number;
}

interface Solution {
  /** Trip-day index of each row. */
  dayOf: number[];
  base: number;
  objective: number;
}

type DayKind = "break" | "forced" | "choice";

interface Structure {
  rows: Row[];
  /** `true` where a row may not go (outside its resort's block or its ticket's window). */
  mask: (row: Row, day: number) => boolean;
  /** Minutes a cell costs on top of its day: a day outside a relaxed ticket window. */
  penalty?: (row: Row, day: number) => number;
  /** The trip-day index each owned ticket's window starts on; NaN without a window. */
  starts: number[];
  /** Visits this structure leaves out for lack of days. */
  dropped: Unit[];
}

interface SearchResult {
  rows: Row[];
  solution: Solution;
  dropped: Unit[];
  exact: boolean;
  starts: number[];
}

/**
 * What a day outside an owned ticket's window costs once the windows are
 * relaxed: more than any day inside, so the plan leaves a window only where
 * nothing else fits.
 */
export const TICKET_WINDOW_PENALTY = 2000;

interface StructureResult {
  solution: Solution | null;
  /** The solution is the best this structure has. */
  provable: boolean;
}

/** The wait a cell is costed and compared by: unrounded where known. */
const waitOf = (c: PlanCell): number | undefined =>
  c.exactWait ?? c.headlinerWait;

const median = (values: number[]): number | undefined => {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};

const mean = (values: number[]): number | undefined =>
  values.length === 0
    ? undefined
    : values.reduce((s, v) => s + v, 0) / values.length;

/** Weather codes (WMO) of rain, showers and storms. */
const WET_CODES = new Set([
  61, 63, 65, 66, 67, 80, 81, 82, 95, 96, 99, 51, 53, 55,
]);

/** A flat-cost day's weather: warm and dry is the reason to go. */
export function weatherFactor(weather: PlanCell["weather"]): {
  factor: number;
  warmDry: boolean;
} {
  if (!weather || weather.tempMax == null) return { factor: 1, warmDry: false };
  const rain = weather.precipitationMm ?? 0;
  const wet =
    rain >= 5 || (weather.icon != null && WET_CODES.has(weather.icon));
  if (weather.tempMax >= 27 && rain < 2 && !wet) {
    return { factor: PlanCostModel.warmDryFactor, warmDry: true };
  }
  if (weather.tempMax < 22 || wet) {
    return { factor: PlanCostModel.coolWetFactor, warmDry: false };
  }
  return { factor: 1, warmDry: false };
}

/**
 * What a day's cost is read against: the park and the window it is compared
 * in. The plan, the calendar's `best` and `/best-days` all price a day with
 * {@link dayCost}; the one model is described in docs/frontend/trip-regions.md
 * ("How a day is priced").
 */
export interface DayCostContext {
  type: TripParkType;
  /** The park's median day-guest hours over the window's OPERATING days. */
  medianHours?: number;
  /** The park's median wait over the window, for an open day without a figure. */
  neutralWait?: number;
  /** Default `any`. */
  fireworks?: FireworksOption;
  /** The product of (1 − share) of the hotel perks that day; 1 without a hotel. */
  perkFactor?: number;
  /** The date priced, for the hurricane season's term; without it there is none. */
  date?: string;
}

/**
 * How much shorter the day guest's day is than the park's usual one: the
 * median hours ÷ that day's hours, at least 1, at most
 * {@link PlanCostModel.hoursFactorCap}. 1 when either is unknown.
 */
export function hoursFactor(
  cell: Pick<PlanCell, "dayHours" | "usualHours">,
  medianHours: number | undefined,
): number {
  const usual = cell.usualHours ?? medianHours;
  if (!usual || !cell.dayHours || cell.dayHours <= 0) return 1;
  return Math.min(
    PlanCostModel.hoursFactorCap,
    Math.max(1, usual / cell.dayHours),
  );
}

/**
 * The day guests lose the evening to a ticketed event: a party night that
 * closes the park early. An After Hours night after a normal close does not.
 */
export function losesEvening(
  cell: Pick<PlanCell, "dayHours" | "evening" | "usualHours">,
  medianHours: number | undefined,
): boolean {
  return (
    cell.evening?.kind === "ticketed_event" &&
    hoursFactor(cell, medianHours) > 1
  );
}

/** The median of a park's day-guest hours over its OPERATING days. */
export function medianDayHours(
  cells: Array<Pick<PlanCell, "status" | "dayHours">>,
): number | undefined {
  return median(
    cells
      .filter((c) => c.status === "OPERATING" && c.dayHours)
      .map((c) => c.dayHours as number),
  );
}

/** A day's cost and the two terms in it that are not waiting. */
export interface DayCostParts {
  cost: number;
  /** What extreme heat added (theme parks only). */
  heat: number;
  /** What the hurricane season added (every park alike that date). */
  hurricane: number;
}

/**
 * A park's cost on one day, in minutes of headliner wait: the unrounded wait,
 * multiplied by how much shorter the day guest's day is than usual (a party
 * night at 18:00 costs more than its waits), by the hotel's perks and by
 * extreme heat ({@link HeatModel}), plus a penalty for a day without published
 * hours, with `fireworks=want` one for a night that costs the evening show,
 * and the hurricane season's minutes for the date ({@link hurricaneMinutes}).
 * A water park or reservation park is a flat day the weather moves. Infinity
 * when the park is closed.
 */
export function dayCostParts(
  cell: PlanCell,
  ctx: DayCostContext,
): DayCostParts {
  if (cell.status === "CLOSED") {
    return { cost: Infinity, heat: 0, hurricane: 0 };
  }
  const unknown =
    cell.status === "UNKNOWN" ? PlanCostModel.unknownDayPenalty : 0;
  const hurricane = ctx.date ? hurricaneMinutes(ctx.date) : 0;
  if (ctx.type !== "theme_park") {
    return {
      cost:
        PlanCostModel.flatDayCost * weatherFactor(cell.weather).factor +
        unknown +
        hurricane,
      heat: 0,
      hurricane,
    };
  }
  const base =
    waitOf(cell) ??
    (ctx.neutralWait ?? PlanCostModel.defaultNeutralWait) +
      PlanCostModel.noFigurePenalty;
  const missed =
    ctx.fireworks === "want" && losesEvening(cell, ctx.medianHours)
      ? PlanCostModel.fireworksMissedPenalty
      : 0;
  const core =
    base * hoursFactor(cell, ctx.medianHours) * (ctx.perkFactor ?? 1);
  const heat = core * ((heatOf(cell.weather)?.factor ?? 1) - 1);
  return {
    cost: core + heat + unknown + missed + hurricane,
    heat,
    hurricane,
  };
}

/** {@link dayCostParts}' cost alone: what every ranking reads. */
export function dayCost(cell: PlanCell, ctx: DayCostContext): number {
  return dayCostParts(cell, ctx).cost;
}

/** The longest run of `true` in a list. */
function longestRun(flags: boolean[]): number {
  let best = 0;
  let run = 0;
  for (const f of flags) {
    run = f ? run + 1 : 0;
    if (run > best) best = run;
  }
  return best;
}

/** Every k-permutation would be overkill: groups are at most three. */
function permutations<T>(items: T[]): T[][] {
  if (items.length <= 1) return [items];
  const out: T[][] = [];
  items.forEach((item, i) => {
    const rest = [...items.slice(0, i), ...items.slice(i + 1)];
    for (const tail of permutations(rest)) out.push([item, ...tail]);
  });
  return out;
}

/** Every split of [0, n) into k non-empty consecutive segments, as start indices. */
function compositions(n: number, k: number): number[][] {
  const out: number[][] = [];
  const walk = (starts: number[]) => {
    if (starts.length === k) {
      out.push(starts);
      return;
    }
    const last = starts[starts.length - 1];
    const remaining = k - starts.length;
    for (let s = last + 1; s <= n - remaining; s++) walk([...starts, s]);
  };
  if (k >= 1 && k <= n) walk([0]);
  return out;
}

class TripOptimizer {
  private readonly n: number;
  private readonly dates: string[];
  private readonly dateIndex = new Map<string, number>();
  private readonly parkBySlug = new Map<string, PlanPark>();
  private readonly neutral = new Map<string, number>();
  private readonly medianHours = new Map<string, number>();
  private readonly costCache = new Map<string, number>();
  private solves = 0;
  private cutShort = false;
  private readonly relaxed = new Set<string>();
  private readonly maxStreak: number;
  private readonly tickets: PlanTicket[];
  /** Days outside an owned ticket's window cost a penalty instead of being forbidden. */
  private relaxWindows = false;
  /** The penalty of the structure being solved, which every objective adds. */
  private activePenalty: Structure["penalty"] | null = null;

  constructor(private readonly input: PlanInput) {
    this.tickets = input.tickets ?? [];
    this.n = input.days.length;
    this.dates = input.days.map((d) => d.date);
    this.dates.forEach((d, i) => this.dateIndex.set(d, i));
    for (const park of input.parks) {
      this.parkBySlug.set(park.slug, park);
      const cells = this.dates.map((d) => this.cell(park.slug, d));
      const waits = cells
        .filter((c) => c.status !== "CLOSED" && waitOf(c) != null)
        .map((c) => waitOf(c) as number);
      this.neutral.set(
        park.slug,
        median(waits) ?? PlanCostModel.defaultNeutralWait,
      );
      const mh = medianDayHours(cells);
      if (mh) this.medianHours.set(park.slug, mh);
    }
    const rest = input.options.rest;
    this.maxStreak =
      rest.mode === "auto"
        ? DEFAULT_MAX_STREAK
        : rest.mode === "max"
          ? rest.n
          : Infinity;
  }

  cell(slug: string, date: string): PlanCell {
    return (
      this.input.cells[slug]?.[date] ?? { status: "UNKNOWN", basis: "none" }
    );
  }

  /** The hotel perks that count for a park on a day: listed, and the cost factor. */
  perks(slug: string, date: string): { listed: PerkKind[]; factor: number } {
    const c = this.cell(slug, date);
    const listed: PerkKind[] = [];
    let factor = 1;
    if (c.status === "CLOSED") return { listed, factor };
    for (const rule of this.input.options.perkRules) {
      if (!rule.parks.includes(slug)) continue;
      let applies = false;
      if (rule.needs === "always") applies = true;
      else if (rule.needs === "extended_evening") {
        applies = c.evening?.kind === "extended_evening";
      } else if (c.early) {
        applies = true;
      }
      if (!applies) continue;
      factor *= 1 - PERK_SHARE[rule.perk];
      if (!listed.includes(rule.perk)) listed.push(rule.perk);
    }
    return { listed, factor };
  }

  /**
   * The day guests lose the evening to a ticketed event: a party night that
   * closes the park early (an After Hours night after a normal close does not).
   */
  losesEvening(slug: string, c: PlanCell): boolean {
    return losesEvening(c, this.medianHours.get(slug));
  }

  /** The hours factor: how much shorter the day guest's day is than usual. */
  hoursFactor(slug: string, c: PlanCell): number {
    return hoursFactor(c, this.medianHours.get(slug));
  }

  /** The cost of one park on one trip day; Infinity when it is closed. */
  cost(slug: string, day: number): number {
    const key = `${slug}|${day}`;
    const cached = this.costCache.get(key);
    if (cached !== undefined) return cached;
    const value = this.computeCost(slug, this.dates[day]);
    this.costCache.set(key, value);
    return value;
  }

  private computeCost(slug: string, date: string): number {
    return this.costParts(slug, date).cost;
  }

  /** One park's cost on a date with its heat and hurricane terms. */
  costParts(slug: string, date: string): DayCostParts {
    const park = this.parkBySlug.get(slug);
    if (!park) return { cost: Infinity, heat: 0, hurricane: 0 };
    return dayCostParts(this.cell(slug, date), {
      type: park.type,
      medianHours: this.medianHours.get(slug),
      neutralWait: this.neutral.get(slug),
      fireworks: this.input.options.fireworks,
      perkFactor: this.perks(slug, date).factor,
      date,
    });
  }

  rowCost(row: Row, day: number): number {
    if (row.lockedDay !== undefined && row.lockedDay !== day) return Infinity;
    if (row.units.length === 1) return this.cost(row.units[0].slug, day);
    const [a, b] = row.units;
    return (
      0.5 * this.cost(a.slug, day) +
      0.5 * this.cost(b.slug, day) +
      PlanCostModel.hopTransferPenalty
    );
  }

  rowGroup(row: Row): string {
    return this.parkBySlug.get(row.units[0].slug)?.group ?? "";
  }

  /** Visits of one park on consecutive days, over a placement. */
  adjacency(rows: Row[], dayOf: number[]): number {
    const daysBySlug = new Map<string, Set<number>>();
    rows.forEach((row, r) => {
      for (const unit of row.units) {
        const set = daysBySlug.get(unit.slug) ?? new Set<number>();
        set.add(dayOf[r]);
        daysBySlug.set(unit.slug, set);
      }
    });
    let count = 0;
    for (const set of daysBySlug.values()) {
      for (const d of set) if (set.has(d + 1)) count++;
    }
    return count;
  }

  solve(cost: number[][]) {
    this.solves++;
    return solveAssignment(cost);
  }

  budgetLeft(): boolean {
    if (this.solves < SOLVE_BUDGET) return true;
    this.cutShort = true;
    return false;
  }

  // ── the plan ──────────────────────────────────────────────────────────────

  run(): OptimizedPlan {
    const { options } = this.input;
    const n = this.n;
    const ignoredLocks: OptimizedPlan["ignoredLocks"] = [];
    const unplaced: OptimizedPlan["unplaced"] = [];

    const units: Unit[] = [];
    for (const park of this.input.parks) {
      for (let v = 1; v <= park.visits; v++) {
        units.push({ slug: park.slug, visit: v });
      }
    }

    // Locks first: they take their day before anything is searched.
    const restLocked = new Set<number>();
    const lockedUnitDay = new Map<Unit, number>();
    const takenDays = new Set<number>();
    for (const lock of options.locks) {
      const label = `${lock.target}@${lock.date}`;
      const day = this.dateIndex.get(lock.date);
      if (day === undefined) {
        ignoredLocks.push({ lock: label, reason: "outside_trip" });
        continue;
      }
      if (takenDays.has(day)) {
        ignoredLocks.push({ lock: label, reason: "day_taken" });
        continue;
      }
      if (lock.target === "rest") {
        restLocked.add(day);
        takenDays.add(day);
        continue;
      }
      const unit = units.find(
        (u) => u.slug === lock.target && !lockedUnitDay.has(u),
      );
      if (!unit) {
        ignoredLocks.push({ lock: label, reason: "no_visit_left" });
        continue;
      }
      if (this.cell(unit.slug, lock.date).status === "CLOSED") {
        ignoredLocks.push({ lock: label, reason: "closed" });
        continue;
      }
      lockedUnitDay.set(unit, day);
      takenDays.add(day);
    }

    const lockedDays = new Set(lockedUnitDay.values());
    const isFree = (i: number) =>
      (i === 0 && options.arrival === "free") ||
      (i === n - 1 && n > 1 && options.departure === "free");
    const available: number[] = [];
    for (let i = 0; i < n; i++) {
      if (restLocked.has(i)) continue;
      if (isFree(i) && !lockedDays.has(i)) continue;
      available.push(i);
    }

    // A park with no open day left in the trip cannot be placed at all, and a
    // park with fewer open days than visits loses the visits that do not fit.
    const placeable: Unit[] = [];
    for (const park of this.input.parks) {
      const open = available.filter(
        (i) => this.cell(park.slug, this.dates[i]).status !== "CLOSED",
      ).length;
      const own = units.filter((u) => u.slug === park.slug);
      own.forEach((unit) => {
        if (lockedUnitDay.has(unit) || unit.visit <= open) {
          placeable.push(unit);
        } else {
          unplaced.push({ slug: unit.slug, reason: "no_open_day" });
        }
      });
    }

    // Owned tickets: each visit takes the ticket that covers it, and a visit
    // beyond a ticket's days or caps leaves the plan at once.
    const overTicket = this.assignTickets(placeable, lockedUnitDay, available);
    for (const u of overTicket) {
      unplaced.push({ slug: u.slug, reason: "no_ticket_day" });
    }

    let pool = placeable.filter((u) => !overTicket.includes(u));
    let useBlocks = options.blocks;
    let best: SearchResult | null = null;
    const windowed = this.tickets.some(
      (t) => Number.isFinite(t.window) || (t.reservedDayParks?.length ?? 0) > 0,
    );
    for (let attempt = 0; attempt <= units.length + 3; attempt++) {
      // A failed attempt's loosened rules are not the plan's.
      const relaxedBefore = new Set(this.relaxed);
      best = this.searchAll(pool, lockedUnitDay, available, useBlocks);
      if (best) break;
      this.relaxed.clear();
      relaxedBefore.forEach((r) => this.relaxed.add(r));
      if (useBlocks) {
        // No order of the resorts fits (closed days, locks): plan without blocks.
        this.relaxed.add("blocks_relaxed");
        useBlocks = false;
        continue;
      }
      if (windowed && !this.relaxWindows) {
        // No placement keeps every ticket's window: a day may leave it, at a
        // price. The blocks come back: it may have been the windows alone.
        this.relaxWindows = true;
        if (options.blocks) {
          this.relaxed.delete("blocks_relaxed");
          useBlocks = true;
        }
        continue;
      }
      const stuck = this.stuckUnits(pool, lockedUnitDay, available);
      if (stuck.length === 0) break;
      pool = pool.filter((u) => !stuck.includes(u));
      for (const u of stuck)
        unplaced.push({ slug: u.slug, reason: "no_day_left" });
    }
    for (const u of best?.dropped ?? []) {
      unplaced.push({ slug: u.slug, reason: "no_day_left" });
    }

    return this.describe(
      best?.rows ?? [],
      best?.solution.dayOf ?? [],
      available,
      restLocked,
      lockedDays,
      unplaced,
      ignoredLocks,
      best?.exact ?? true,
      best?.starts ?? this.tickets.map(() => NaN),
    );
  }

  // ── owned tickets ─────────────────────────────────────────────────────────

  /** The order visits leave a plan in: repeats first, then the park whose best day is dearest. */
  private dropOrder(units: Unit[], available: number[]): Unit[] {
    const best = new Map<string, number>();
    const bestCost = (slug: string): number => {
      let v = best.get(slug);
      if (v === undefined) {
        v = Math.min(...available.map((d) => this.cost(slug, d)));
        best.set(slug, v);
      }
      return v;
    };
    return [...units].sort(
      (a, b) =>
        b.visit - a.visit ||
        bestCost(b.slug) - bestCost(a.slug) ||
        a.slug.localeCompare(b.slug),
    );
  }

  /**
   * Gives each visit the owned ticket that covers it: the park with the fewest
   * covering tickets first, each visit to the first ticket in input order with
   * room for it (a day, a cap, an allowance visit), counting a ticket that may
   * hop as holding two visits a day only once none has room otherwise. Then
   * trims each ticket to what its days, caps and hop pairs can hold. Returns
   * the visits no ticket has room for; a park no ticket covers stays plan first.
   */
  private assignTickets(
    units: Unit[],
    lockedUnitDay: Map<Unit, number>,
    available: number[],
  ): Unit[] {
    const tickets = this.tickets;
    if (tickets.length === 0) return [];
    const covers = (t: PlanTicket, slug: string) =>
      t.parks.includes(slug) || !!t.allowance?.parks.includes(slug);
    const coverCount = (u: Unit) =>
      tickets.filter((t) => covers(t, u.slug)).length;
    const parkIndex = new Map(this.input.parks.map((p, i) => [p.slug, i]));
    const order = units
      .filter((u) => coverCount(u) > 0)
      .sort(
        (a, b) =>
          coverCount(a) - coverCount(b) ||
          a.visit - b.visit ||
          (parkIndex.get(a.slug) ?? 0) - (parkIndex.get(b.slug) ?? 0),
      );
    const dayUse = tickets.map(() => 0);
    const allowanceUse = tickets.map(() => 0);
    const capUse = tickets.map((t) => t.caps.map(() => 0));
    const pairable = (t: PlanTicket, set?: string[]) =>
      this.input.options.hop &&
      t.hopPairs.some(([a, b]) => !set || (set.includes(a) && set.includes(b)));
    const assign = (u: Unit, loose: boolean): boolean => {
      for (let i = 0; i < tickets.length; i++) {
        const t = tickets[i];
        if (!covers(t, u.slug)) continue;
        if (!t.parks.includes(u.slug)) {
          if (allowanceUse[i] >= (t.allowance?.visits ?? 0)) continue;
          allowanceUse[i]++;
          u.ticket = i;
          u.allowance = true;
          return true;
        }
        if (dayUse[i] >= t.days * (loose && pairable(t) ? 2 : 1)) continue;
        const hit = t.caps
          .map((c, k) => (c.parks.includes(u.slug) ? k : -1))
          .filter((k) => k >= 0);
        const full = hit.some(
          (k) =>
            capUse[i][k] >=
            t.caps[k].max * (loose && pairable(t, t.caps[k].parks) ? 2 : 1),
        );
        if (full) continue;
        dayUse[i]++;
        hit.forEach((k) => capUse[i][k]++);
        u.ticket = i;
        return true;
      }
      return false;
    };
    const waiting = order.filter((u) => !assign(u, false));
    const over = waiting.filter((u) => !assign(u, true));
    return [
      ...over,
      ...this.trimTickets(
        units.filter((u) => !over.includes(u)),
        lockedUnitDay,
        available,
      ),
    ];
  }

  /**
   * Leaves out the visits a ticket's days, caps and hop pairs cannot hold
   * (`no_ticket_day`), the drop order's first each time, until every ticket
   * has a pairing that keeps them.
   */
  private trimTickets(
    units: Unit[],
    lockedUnitDay: Map<Unit, number>,
    available: number[],
  ): Unit[] {
    const dropped: Unit[] = [];
    for (let ti = 0; ti < this.tickets.length; ti++) {
      for (;;) {
        const mine = units.filter(
          (u) => u.ticket === ti && !u.allowance && !dropped.includes(u),
        );
        const free = mine.filter((u) => !lockedUnitDay.has(u));
        const lockedRows: Row[] = mine
          .filter((u) => lockedUnitDay.has(u))
          .map((u) => ({ units: [u], lockedDay: lockedUnitDay.get(u) }));
        const need = this.pairsNeeded(ti, free, lockedRows);
        const kept =
          need === 0
            ? this.ticketKept(ti, [], free, lockedRows)
            : this.input.options.hop &&
              this.pairCombos(free, need, this.tickets[ti].hopPairs).some(
                (combo) => this.ticketKept(ti, combo, free, lockedRows),
              );
        if (kept || free.length === 0) break;
        dropped.push(this.dropOrder(free, available)[0]);
      }
    }
    return dropped;
  }

  /**
   * The hop days a ticket's free visits need: enough to fit its days, and to
   * fit each cap (a pair inside a cap's parks is one of its days).
   */
  private pairsNeeded(ti: number, free: Unit[], lockedRows: Row[]): number {
    const t = this.tickets[ti];
    const own = lockedRows.filter(
      (r) => r.units[0].ticket === ti && !r.units[0].allowance,
    );
    const days = Math.max(0, own.length + free.length - t.days);
    let caps = 0;
    for (const c of t.caps) {
      const k =
        free.filter((u) => c.parks.includes(u.slug)).length +
        own.filter((r) => c.parks.includes(r.units[0].slug)).length;
      caps += Math.max(0, k - c.max);
    }
    return Math.max(days, caps);
  }

  /** A ticket's rows under a pairing keep its days and its caps. */
  private ticketKept(
    ti: number,
    pairs: Row[],
    free: Unit[],
    lockedRows: Row[],
  ): boolean {
    const t = this.tickets[ti];
    const inPairs = new Set(pairs.flatMap((p) => p.units));
    const rows: Row[] = [
      ...lockedRows.filter(
        (r) => r.units[0].ticket === ti && !r.units[0].allowance,
      ),
      ...pairs,
      ...free.filter((u) => !inPairs.has(u)).map((u) => ({ units: [u] })),
    ];
    if (rows.length > t.days) return false;
    return t.caps.every(
      (c) =>
        rows.filter((r) => r.units.some((u) => c.parks.includes(u.slug)))
          .length <= c.max,
    );
  }

  /** The index of a trip date, for a ticket's start or reserved day. */
  private dayIndexOf(date: string | undefined): number | undefined {
    return date === undefined ? undefined : this.dateIndex.get(date);
  }

  /** Days from the trip's first day to a date, negative before it. */
  private offsetOf(date: string): number {
    return Math.round(
      (Date.parse(`${date}T00:00:00Z`) -
        Date.parse(`${this.dates[0]}T00:00:00Z`)) /
        86_400_000,
    );
  }

  /** Whether a row on a day lies outside its owned ticket's window (or off its reserved day). */
  private outsideTicket(row: Row, day: number, starts: number[]): boolean {
    const ti = row.units[0].ticket;
    if (ti === undefined) return false;
    const t = this.tickets[ti];
    const reserved = this.dayIndexOf(t.reservedDay);
    if (
      reserved !== undefined &&
      row.units.some((u) => t.reservedDayParks?.includes(u.slug)) &&
      day !== reserved
    ) {
      return true;
    }
    if (!Number.isFinite(t.window)) return false;
    const start = starts[ti];
    return day < start || day > start + t.window - 1;
  }

  /**
   * The starts a ticket's window may take in this structure. A fixed start is
   * itself; a free one any trip day on sale (holding the reserved day), less
   * those another start's window covers (every window that reaches the trip's
   * end is beaten by the earliest of them) and, while the windows are kept,
   * those that cannot hold the ticket's visits on open days. NaN: no window.
   */
  private startCandidates(
    ti: number,
    rows: Row[],
    available: number[],
  ): number[] {
    const t = this.tickets[ti];
    const n = this.n;
    if (!Number.isFinite(t.window)) return [NaN];
    let starts: number[];
    if (t.start !== undefined) {
      starts = [this.offsetOf(t.start)];
    } else {
      const all = Array.from({ length: n }, (_, i) => i);
      const allowed = t.startOptions ? new Set(t.startOptions) : null;
      starts = allowed ? all.filter((i) => allowed.has(this.dates[i])) : all;
      const reserved = this.dayIndexOf(t.reservedDay);
      if (reserved !== undefined) {
        starts = starts.filter(
          (s0) => s0 <= reserved && reserved <= s0 + t.window - 1,
        );
      }
      if (starts.length === 0 && this.relaxWindows) starts = all;
      const last = n - t.window;
      const late = starts.filter((s0) => s0 > last);
      starts = starts.filter((s0) => s0 <= last);
      if (late.length > 0 && !starts.includes(last)) starts.push(late[0]);
    }
    if (this.relaxWindows) return starts;
    const own = rows.filter((r) => r.units[0].ticket === ti);
    return starts.filter((s0) => {
      const lo = Math.max(0, s0);
      const hi = Math.min(n - 1, s0 + t.window - 1);
      const days = available.filter((d) => d >= lo && d <= hi);
      if (days.length < own.length) return false;
      const need = new Map<string, number>();
      for (const r of own) {
        if (r.lockedDay !== undefined && (r.lockedDay < lo || r.lockedDay > hi))
          return false;
        for (const u of r.units) need.set(u.slug, (need.get(u.slug) ?? 0) + 1);
      }
      for (const [slug, k] of need) {
        const open = days.filter(
          (d) => this.cell(slug, this.dates[d]).status !== "CLOSED",
        ).length;
        if (open < k) return false;
      }
      return true;
    });
  }

  /**
   * One mask (or, with the windows relaxed, one penalty) per combination of
   * the owned tickets' starts.
   */
  private ticketStructures(
    rows: Row[],
    available: number[],
  ): Array<Pick<Structure, "mask" | "penalty" | "starts">> {
    if (this.tickets.length === 0) return [{ mask: () => false, starts: [] }];
    const options = this.tickets.map((_, ti) =>
      this.startCandidates(ti, rows, available),
    );
    if (options.some((o) => o.length === 0)) return [];
    const combos: number[][] = [];
    const walk = (i: number, chosen: number[]) => {
      if (combos.length >= START_COMBO_LIMIT) {
        this.cutShort = true;
        return;
      }
      if (i === options.length) {
        combos.push([...chosen]);
        return;
      }
      for (const s0 of options[i]) {
        chosen.push(s0);
        walk(i + 1, chosen);
        chosen.pop();
      }
    };
    walk(0, []);
    return combos.map((starts) =>
      this.relaxWindows
        ? {
            mask: () => false,
            penalty: (row: Row, day: number) =>
              this.outsideTicket(row, day, starts) ? TICKET_WINDOW_PENALTY : 0,
            starts,
          }
        : {
            mask: (row: Row, day: number) =>
              this.outsideTicket(row, day, starts),
            starts,
          },
    );
  }

  /**
   * The visits a plain assignment (no blocks, no rest rule) can only put on a
   * closed day: two parks open on the same single day, say. They leave the plan.
   */
  private stuckUnits(
    pool: Unit[],
    lockedUnitDay: Map<Unit, number>,
    available: number[],
  ): Unit[] {
    const rows: Row[] = pool.map((u) => ({
      units: [u],
      lockedDay: lockedUnitDay.get(u),
    }));
    if (rows.length === 0 || rows.length > available.length) return [];
    const result = this.solve(this.matrix(rows, available, () => false));
    const stuck = result.forbiddenRows
      .map((r) => rows[r])
      .filter((row) => row.lockedDay === undefined)
      .map((row) => row.units[0]);
    return stuck.length > 0
      ? stuck
      : result.forbiddenRows.map((r) => rows[r].units[0]);
  }

  /**
   * Builds the rows (single visits, hop pairs, locks), drops the visits that
   * cannot fit, and searches every structure cheapest bound first. Null when
   * no structure is feasible.
   */
  private searchAll(
    pool: Unit[],
    lockedUnitDay: Map<Unit, number>,
    available: number[],
    useBlocks: boolean,
  ): SearchResult | null {
    const lockedRows: Row[] = pool
      .filter((u) => lockedUnitDay.has(u))
      .map((u) => ({ units: [u], lockedDay: lockedUnitDay.get(u) }));
    const free = pool.filter((u) => !lockedUnitDay.has(u));
    const capacity = available.length - lockedRows.length;

    // Hop days are only for a trip, or a ticket, shorter than its visits.
    const pairings = this.pairings(free, lockedRows, capacity);

    // A repeat visit is dropped first, then the park whose best day is dearest.
    const dropOrder = this.dropOrder(free, available);

    const structures: Structure[] = [];
    for (const pairs of pairings) {
      const inPairs = new Set(pairs.flatMap((p) => p.units));
      const singles = free.filter((u) => !inPairs.has(u));
      const excess =
        lockedRows.length + pairs.length + singles.length - available.length;
      const dropped =
        excess > 0
          ? dropOrder.filter((u) => !inPairs.has(u)).slice(0, excess)
          : [];
      const rows: Row[] = [
        ...lockedRows,
        ...pairs,
        ...singles
          .filter((u) => !dropped.includes(u))
          .map((u) => ({ units: [u] })),
      ];
      if (rows.length > available.length) continue;
      const blocks = this.blockMasks(rows, useBlocks);
      for (const ticket of this.ticketStructures(rows, available)) {
        for (const block of blocks) {
          structures.push({
            rows,
            mask: (row, day) => ticket.mask(row, day) || block(row, day),
            ...(ticket.penalty && { penalty: ticket.penalty }),
            starts: ticket.starts,
            dropped,
          });
        }
      }
    }
    if (structures.length === 0) return null;

    // The streak rule, loosened as little as possible when the visits leave
    // too few spare days to keep it. Every structure has as many rows.
    const lockedDays = new Set(lockedRows.map((r) => r.lockedDay as number));
    const kinds = this.dayKinds(available, lockedDays);
    const spares = available.length - structures[0].rows.length;
    let limit = this.maxStreak;
    while (
      Number.isFinite(limit) &&
      limit < this.n &&
      this.countPatterns(kinds, spares, limit, 1) === 0
    ) {
      limit++;
      this.relaxed.add("streak_relaxed");
    }

    // Lower bound of each structure: its assignment without the rest rule and
    // without the repeat penalty.
    const bounded: Array<
      Structure & { matrix: number[][]; rowToCol: number[]; bound: number }
    > = [];
    for (const s of structures) {
      if (!this.budgetLeft()) break;
      const matrix = this.matrix(s.rows, available, s.mask, s.penalty);
      const result = this.solve(matrix);
      if (result.feasible) {
        bounded.push({
          ...s,
          matrix,
          rowToCol: result.rowToCol,
          bound: result.cost,
        });
      }
    }
    bounded.sort((a, b) => a.bound - b.bound);

    // Structures whose visits cannot keep the streak rule are skipped; when
    // none can, the rule is loosened one day at a time.
    for (;;) {
      const best = this.evaluate(bounded, available, kinds, spares, limit);
      if (best || !Number.isFinite(limit)) return best;
      limit = limit + 1 >= this.n ? Infinity : limit + 1;
      this.relaxed.add("streak_relaxed");
    }
  }

  /** Branch and bound over the structures, cheapest bound first. */
  private evaluate(
    bounded: Array<
      Structure & { matrix: number[][]; rowToCol: number[]; bound: number }
    >,
    available: number[],
    kinds: DayKind[],
    spares: number,
    limit: number,
  ): SearchResult | null {
    let best: SearchResult | null = null;
    const unproven: number[] = [];
    for (const s of bounded) {
      if (best && s.bound >= best.solution.objective) break;
      if (!this.budgetLeft()) break;
      this.activePenalty = s.penalty ?? null;
      const outcome = this.solveStructure(
        s.rows,
        available,
        s.matrix,
        s.rowToCol,
        kinds,
        spares,
        limit,
      );
      if (!outcome.solution) continue;
      if (!outcome.provable) unproven.push(s.bound);
      if (!best || outcome.solution.objective < best.solution.objective) {
        best = {
          rows: s.rows,
          solution: outcome.solution,
          dropped: s.dropped,
          exact: true,
          starts: s.starts,
        };
      }
    }
    if (!best && this.cutShort && bounded.length > 0) {
      // Out of budget before anything kept the rule: the cheapest structure's
      // plain assignment is still a plan, and the answer says it is not exact.
      const s = bounded[0];
      this.activePenalty = s.penalty ?? null;
      if (
        longestRun(this.parkDayFlags(s.rowToCol.map((c) => available[c]))) >
        limit
      ) {
        this.relaxed.add("streak_relaxed");
      }
      return {
        rows: s.rows,
        solution: this.objective(
          s.rows,
          s.rowToCol.map((c) => available[c]),
        ),
        dropped: s.dropped,
        exact: false,
        starts: s.starts,
      };
    }
    if (!best) return null;
    const objective = best.solution.objective;
    best.exact =
      !this.cutShort && unproven.every((bound) => bound >= objective - 1e-9);
    return best;
  }

  /**
   * The pairings a structure may use. Each owned ticket pairs at least as many
   * of its visits as its days and caps need, and the trip pairs as many as it
   * has visits over days, in the group a pair's two visits share (a ticket's,
   * or plan first's with `ticket=hopper`); fewer trip pairs when that many
   * cannot be made. `[[]]` when none is needed.
   */
  private pairings(free: Unit[], lockedRows: Row[], capacity: number): Row[][] {
    const { options } = this.input;
    const byGroup = new Map<number, Unit[]>();
    for (const u of free) {
      // -1: plan first; -2: allowance visits, which never pair.
      const g = u.ticket === undefined ? -1 : u.allowance ? -2 : u.ticket;
      byGroup.set(g, [...(byGroup.get(g) ?? []), u]);
    }
    const groups = [...byGroup].map(([g, units]) => ({
      g,
      units,
      pairs:
        !options.hop || g === -2
          ? []
          : g === -1
            ? options.hopPairs
            : this.tickets[g].hopPairs,
      min: g >= 0 ? this.pairsNeeded(g, units, lockedRows) : 0,
    }));
    const minimum = groups.reduce((sum, g) => sum + g.min, 0);
    const total = Math.max(minimum, free.length - capacity);
    if (total <= 0) return [[]];
    const cache = new Map<string, Row[][]>();
    const combosOf = (k: number, size: number): Row[][] => {
      const key = `${k}|${size}`;
      const hit = cache.get(key);
      if (hit) return hit;
      const g = groups[k];
      const list = (
        size === 0 ? [[]] : this.pairCombos(g.units, size, g.pairs)
      ).filter(
        (combo) => g.g < 0 || this.ticketKept(g.g, combo, g.units, lockedRows),
      );
      cache.set(key, list);
      return list;
    };
    for (let size = total; size >= minimum; size--) {
      const out: Row[][] = [];
      const walk = (k: number, left: number, chosen: Row[][]) => {
        if (out.length >= HOP_COMBO_LIMIT) {
          this.cutShort = true;
          return;
        }
        if (k === groups.length) {
          if (left === 0) out.push(chosen.flat());
          return;
        }
        const g = groups[k];
        const most = Math.min(left, Math.floor(g.units.length / 2));
        for (let take = g.min; take <= most; take++) {
          for (const combo of combosOf(k, take)) {
            chosen.push(combo);
            walk(k + 1, left - take, chosen);
            chosen.pop();
            if (out.length >= HOP_COMBO_LIMIT) return;
          }
        }
      };
      walk(0, size, []);
      if (out.length > 0) return out;
    }
    return [[]];
  }

  /** The ways to make exactly `size` hop pairs of the given visits from the eligible park pairs. */
  private pairCombos(
    units: Unit[],
    size: number,
    eligiblePairs: Array<[string, string]>,
  ): Row[][] {
    const eligible = eligiblePairs.filter(
      ([a, b]) =>
        a !== b &&
        units.some((u) => u.slug === a) &&
        units.some((u) => u.slug === b),
    );
    const combos: Row[][] = [];
    const left = new Map<string, Unit[]>();
    for (const u of units) left.set(u.slug, [...(left.get(u.slug) ?? []), u]);
    const walk = (start: number, chosen: Row[]) => {
      if (combos.length >= HOP_COMBO_LIMIT) {
        this.cutShort = true;
        return;
      }
      if (chosen.length === size) {
        combos.push([...chosen]);
        return;
      }
      for (let k = start; k < eligible.length; k++) {
        const [a, b] = eligible[k];
        const ua = left.get(a);
        const ub = left.get(b);
        if (!ua?.length || !ub?.length) continue;
        const unitA = ua.pop() as Unit;
        const unitB = ub.pop() as Unit;
        chosen.push({ units: [unitA, unitB] });
        walk(k, chosen);
        chosen.pop();
        ua.push(unitA);
        ub.push(unitB);
      }
    };
    walk(0, []);
    return combos;
  }

  /**
   * One mask per block structure: `true` where a row may not go. Without
   * `blocks`, or with one resort, a single structure with no mask.
   */
  private blockMasks(
    rows: Row[],
    useBlocks: boolean,
  ): Array<(row: Row, day: number) => boolean> {
    const none = [() => false];
    if (!useBlocks) return none;
    const blockable = new Set(this.input.options.blockGroups);
    // An owned ticket's window replaces its resort's block, unless the query
    // asked for blocks in so many words.
    const planFirst = (row: Row) =>
      this.input.options.blockOwned === true ||
      row.units[0].ticket === undefined;
    const groups = [
      ...new Set(
        rows
          .filter(planFirst)
          .map((r) => this.rowGroup(r))
          .filter((g) => blockable.has(g)),
      ),
    ].sort();
    if (groups.length <= 1) return none;
    const masks: Array<(row: Row, day: number) => boolean> = [];
    for (const order of permutations(groups)) {
      for (const starts of compositions(this.n, order.length)) {
        const segmentOf = new Array<string>(this.n);
        starts.forEach((start, k) => {
          const end = k + 1 < starts.length ? starts[k + 1] : this.n;
          for (let d = start; d < end; d++) segmentOf[d] = order[k];
        });
        masks.push((row, day) => {
          if (!planFirst(row)) return false;
          const group = this.rowGroup(row);
          return blockable.has(group) && segmentOf[day] !== group;
        });
      }
    }
    return masks;
  }

  private matrix(
    rows: Row[],
    days: number[],
    mask: (row: Row, day: number) => boolean,
    penalty?: (row: Row, day: number) => number,
  ): number[][] {
    const lockedDays = new Set(
      rows.filter((r) => r.lockedDay !== undefined).map((r) => r.lockedDay),
    );
    return rows.map((row) =>
      days.map((day) => {
        if (mask(row, day)) return Infinity;
        if (row.lockedDay === undefined && lockedDays.has(day)) return Infinity;
        return this.rowCost(row, day) + (penalty?.(row, day) ?? 0);
      }),
    );
  }

  private objective(rows: Row[], dayOf: number[]): Solution {
    const penalty = this.activePenalty;
    const base = rows.reduce(
      (s, row, r) =>
        s + this.rowCost(row, dayOf[r]) + (penalty?.(row, dayOf[r]) ?? 0),
      0,
    );
    return {
      dayOf,
      base,
      objective:
        base +
        PlanCostModel.adjacentRepeatPenalty * this.adjacency(rows, dayOf),
    };
  }

  /** Which trip days are park days under a placement. */
  private parkDayFlags(dayOf: number[]): boolean[] {
    const flags = new Array<boolean>(this.n).fill(false);
    for (const d of dayOf) flags[d] = true;
    return flags;
  }

  /**
   * The best placement inside one structure, given the unconstrained
   * assignment already solved for its bound.
   */
  private solveStructure(
    rows: Row[],
    available: number[],
    matrix: number[][],
    rowToCol: number[],
    kinds: DayKind[],
    spares: number,
    limit: number,
  ): StructureResult {
    const plain = rowToCol.map((col) => available[col]);
    const keepsRule = (dayOf: number[]) =>
      longestRun(this.parkDayFlags(dayOf)) <= limit;

    if (keepsRule(plain) && this.adjacency(rows, plain) === 0) {
      return { solution: this.objective(rows, plain), provable: true };
    }
    if (keepsRule(plain)) {
      // Only repeats sit back to back: keep them apart over every day first.
      const apart = this.assignApart(rows, available, matrix);
      if (apart.dayOf && keepsRule(apart.dayOf)) {
        return {
          solution: this.objective(rows, apart.dayOf),
          provable: !apart.cutShort,
        };
      }
    }

    const count = this.countPatterns(
      kinds,
      spares,
      limit,
      REST_EXACT_LIMIT + 1,
    );
    const found =
      count <= REST_EXACT_LIMIT
        ? this.exactRest(rows, available, matrix, kinds, spares, limit)
        : this.localRest(rows, available, matrix, kinds, spares, limit);
    let provable = count <= REST_EXACT_LIMIT && !found.cutShort;
    // No spare pattern within the rule leaves every visit an open day here.
    if (!found.dayOf) return { solution: null, provable };

    let solution = this.objective(rows, found.dayOf);
    if (solution.objective > solution.base) {
      // No placement keeps every repeat apart: spread them as far as moves can.
      solution = this.spreadRepeats(rows, available, matrix, solution, limit);
      provable = false;
    }
    return { solution, provable };
  }

  /**
   * The cheapest assignment that keeps every park's visits off consecutive
   * days, by branch and bound on the Hungarian: where park p sits on day d and
   * d + 1, one branch forbids p on d and the other on d + 1, and every
   * placement that keeps the rule is in one of them. A branch whose bound is
   * no better than the best found is cut. `dayOf` is null when no placement
   * keeps the rule.
   */
  private assignApart(
    rows: Row[],
    colDays: number[],
    matrix: number[][],
  ): { dayOf: number[] | null; cost: number; cutShort: boolean } {
    let best: { dayOf: number[]; cost: number } | null = null;
    let cutShort = false;
    const walk = (forbidden: Map<string, Set<number>>) => {
      if (!this.budgetLeft()) {
        cutShort = true;
        return;
      }
      const m = matrix.map((row, r) =>
        row.map((value, c) =>
          rows[r].units.some((u) => forbidden.get(u.slug)?.has(colDays[c]))
            ? Infinity
            : value,
        ),
      );
      const result = this.solve(m);
      if (!result.feasible) return;
      if (best && result.cost >= best.cost - 1e-9) return;
      const dayOf = result.rowToCol.map((c) => colDays[c]);
      const clash = this.firstClash(rows, dayOf);
      if (!clash) {
        best = { dayOf, cost: result.cost };
        return;
      }
      for (const day of [clash.day, clash.day + 1]) {
        const next = new Map(forbidden);
        next.set(
          clash.slug,
          new Set([...(forbidden.get(clash.slug) ?? []), day]),
        );
        walk(next);
      }
    };
    walk(new Map());
    const found = best as { dayOf: number[]; cost: number } | null;
    return {
      dayOf: found ? found.dayOf : null,
      cost: found ? found.cost : Infinity,
      cutShort,
    };
  }

  /** The first park found on two consecutive days, by its earlier day. */
  private firstClash(
    rows: Row[],
    dayOf: number[],
  ): { slug: string; day: number } | null {
    const days = new Map<string, Set<number>>();
    rows.forEach((row, r) => {
      for (const u of row.units) {
        const set = days.get(u.slug) ?? new Set<number>();
        set.add(dayOf[r]);
        days.set(u.slug, set);
      }
    });
    for (const [slug, set] of days) {
      for (const day of [...set].sort((a, b) => a - b)) {
        if (set.has(day + 1)) return { slug, day };
      }
    }
    return null;
  }

  /** Per trip day: "break" (no park possible), "forced" (a lock), or "choice". */
  private dayKinds(available: number[], lockedDays: Set<number>): DayKind[] {
    const avail = new Set(available);
    return Array.from({ length: this.n }, (_, i) =>
      !avail.has(i) ? "break" : lockedDays.has(i) ? "forced" : "choice",
    );
  }

  /**
   * Spare-day patterns with exactly `spares` spare days and no run of park days
   * longer than `limit`, counted up to `cap`.
   */
  private countPatterns(
    kinds: DayKind[],
    spares: number,
    limit: number,
    cap: number,
  ): number {
    let count = 0;
    this.walkPatterns(kinds, spares, limit, () => {
      count++;
      return count < cap;
    });
    return count;
  }

  /** Visits each valid spare set; `visit` returns false to stop. */
  private walkPatterns(
    kinds: DayKind[],
    spares: number,
    limit: number,
    visit: (spare: number[]) => boolean,
  ): void {
    const choicesFrom = new Array<number>(kinds.length + 1).fill(0);
    for (let i = kinds.length - 1; i >= 0; i--) {
      choicesFrom[i] = choicesFrom[i + 1] + (kinds[i] === "choice" ? 1 : 0);
    }
    const chosen: number[] = [];
    let stop = false;
    const step = (i: number, run: number, left: number) => {
      if (stop) return;
      if (left > choicesFrom[i]) return;
      if (i === kinds.length) {
        if (left === 0 && !visit([...chosen])) stop = true;
        return;
      }
      const kind = kinds[i];
      if (kind === "break") return step(i + 1, 0, left);
      if (kind === "choice" && left > 0) {
        chosen.push(i);
        step(i + 1, 0, left - 1);
        chosen.pop();
      }
      if (run + 1 <= limit) step(i + 1, run + 1, left);
    };
    step(0, 0, spares);
  }

  /** The square assignment over the days a spare set leaves. */
  private solveWithSpares(
    rows: Row[],
    available: number[],
    matrix: number[][],
    spare: Set<number>,
  ): { dayOf: number[]; cost: number } | null {
    const cols: number[] = [];
    available.forEach((day, col) => {
      if (!spare.has(day)) cols.push(col);
    });
    const sub = matrix.map((row) => cols.map((c) => row[c]));
    const colDays = cols.map((c) => available[c]);
    const apart = this.assignApart(rows, colDays, sub);
    if (apart.dayOf) return { dayOf: apart.dayOf, cost: apart.cost };
    // No placement on these days keeps the repeats apart: the plain cheapest.
    const result = this.solve(sub);
    if (!result.feasible) return null;
    return {
      dayOf: result.rowToCol.map((k) => colDays[k]),
      cost: result.cost,
    };
  }

  private exactRest(
    rows: Row[],
    available: number[],
    matrix: number[][],
    kinds: DayKind[],
    spares: number,
    limit: number,
  ): { dayOf: number[] | null; cutShort: boolean } {
    let best: Solution | undefined;
    let cutShort = false;
    this.walkPatterns(kinds, spares, limit, (spare) => {
      if (!this.budgetLeft()) {
        cutShort = true;
        return false;
      }
      const found = this.solveWithSpares(
        rows,
        available,
        matrix,
        new Set(spare),
      );
      if (found) {
        const s = this.objective(rows, found.dayOf);
        if (!best || s.objective < best.objective) best = s;
      }
      return true;
    });
    const chosen = best as Solution | undefined;
    return { dayOf: chosen ? chosen.dayOf : null, cutShort };
  }

  /**
   * Past {@link REST_EXACT_LIMIT} patterns: start from the busiest days the
   * streak rule allows to spare, then swap a spare day with a park day while
   * that lowers the cost.
   */
  private localRest(
    rows: Row[],
    available: number[],
    matrix: number[][],
    kinds: DayKind[],
    spares: number,
    limit: number,
  ): { dayOf: number[] | null; cutShort: boolean } {
    const colOf = new Map(available.map((d, c) => [d, c]));
    const busy = (day: number): number => {
      const col = colOf.get(day);
      if (col === undefined) return 0;
      const values = matrix.map((row) =>
        Number.isFinite(row[col]) ? row[col] : 1e4,
      );
      return mean(values) ?? 0;
    };
    const start = this.busiestPattern(kinds, spares, limit, busy);
    if (!start) return { dayOf: null, cutShort: true };

    const valid = (spare: Set<number>) => {
      const flags = kinds.map((k, i) => k !== "break" && !spare.has(i));
      return longestRun(flags) <= limit;
    };
    let spare = new Set(start);
    let current = this.solveWithSpares(rows, available, matrix, spare);
    let currentObjective = current
      ? this.objective(rows, current.dayOf).objective
      : Infinity;
    let evals = 1;
    let improved = true;
    while (improved && evals < REST_LOCAL_SEARCH_EVALS && this.budgetLeft()) {
      improved = false;
      const choices = kinds
        .map((k, i) => (k === "choice" ? i : -1))
        .filter((i) => i >= 0);
      outer: for (const out of [...spare]) {
        for (const into of choices) {
          if (spare.has(into)) continue;
          const next = new Set(spare);
          next.delete(out);
          next.add(into);
          if (!valid(next)) continue;
          evals++;
          const found = this.solveWithSpares(rows, available, matrix, next);
          if (!found) continue;
          const objective = this.objective(rows, found.dayOf).objective;
          if (objective < currentObjective - 1e-9) {
            spare = next;
            current = found;
            currentObjective = objective;
            improved = true;
            break outer;
          }
          if (evals >= REST_LOCAL_SEARCH_EVALS) break outer;
        }
      }
    }
    return { dayOf: current?.dayOf ?? null, cutShort: true };
  }

  /** The spare set of the given size that spares the busiest days the rule allows. */
  private busiestPattern(
    kinds: DayKind[],
    spares: number,
    limit: number,
    busy: (day: number) => number,
  ): number[] | null {
    const n = kinds.length;
    // best[i][run][left]: the most busyness spared from day i on.
    const memo = new Map<string, { value: number; pick: boolean } | null>();
    const go = (i: number, run: number, left: number): number => {
      if (i === n) return left === 0 ? 0 : -Infinity;
      const key = `${i}|${run}|${left}`;
      const hit = memo.get(key);
      if (hit !== undefined) return hit ? hit.value : -Infinity;
      let value = -Infinity;
      let pick = false;
      if (kinds[i] === "break") {
        value = go(i + 1, 0, left);
      } else {
        if (kinds[i] === "choice" && left > 0) {
          const v = busy(i) + go(i + 1, 0, left - 1);
          if (v > value) {
            value = v;
            pick = true;
          }
        }
        if (run + 1 <= limit) {
          const v = go(i + 1, run + 1, left);
          if (v > value) {
            value = v;
            pick = false;
          }
        }
      }
      memo.set(key, Number.isFinite(value) ? { value, pick } : null);
      return value;
    };
    if (!Number.isFinite(go(0, 0, spares))) return null;
    const out: number[] = [];
    let run = 0;
    let left = spares;
    for (let i = 0; i < n; i++) {
      if (kinds[i] === "break") {
        run = 0;
        continue;
      }
      const hit = memo.get(`${i}|${run}|${left}`);
      if (hit?.pick) {
        out.push(i);
        left--;
        run = 0;
      } else {
        run++;
      }
    }
    return out;
  }

  /**
   * Moves and swaps that lower the full objective, the repeat penalty
   * included, keeping every rule: masks, locks and the streak limit.
   */
  private spreadRepeats(
    rows: Row[],
    available: number[],
    matrix: number[][],
    start: Solution,
    limit: number,
  ): Solution {
    let best = start;
    const colOf = new Map(available.map((d, c) => [d, c]));
    const free = (dayOf: number[]) =>
      available.filter((d) => !dayOf.includes(d));
    const fits = (dayOf: number[]) =>
      dayOf.every((d, r) =>
        Number.isFinite(matrix[r][colOf.get(d) as number]),
      ) && longestRun(this.parkDayFlags(dayOf)) <= limit;
    for (let pass = 0; pass < 50; pass++) {
      let improved = false;
      for (let r = 0; r < rows.length && !improved; r++) {
        if (rows[r].lockedDay !== undefined) continue;
        const candidates: number[][] = [];
        for (let q = 0; q < rows.length; q++) {
          if (q === r || rows[q].lockedDay !== undefined) continue;
          const swapped = [...best.dayOf];
          [swapped[r], swapped[q]] = [swapped[q], swapped[r]];
          candidates.push(swapped);
        }
        for (const day of free(best.dayOf)) {
          const moved = [...best.dayOf];
          moved[r] = day;
          candidates.push(moved);
        }
        for (const dayOf of candidates) {
          if (!fits(dayOf)) continue;
          const s = this.objective(rows, dayOf);
          if (s.objective < best.objective - 1e-9) {
            best = s;
            improved = true;
            break;
          }
        }
      }
      if (!improved) break;
    }
    return best;
  }

  // ── the answer ────────────────────────────────────────────────────────────

  private describe(
    rows: Row[],
    dayOf: number[],
    available: number[],
    restLocked: Set<number>,
    lockedDays: Set<number>,
    unplaced: OptimizedPlan["unplaced"],
    ignoredLocks: OptimizedPlan["ignoredLocks"],
    exact: boolean,
    starts: number[],
  ): OptimizedPlan {
    const { options, days: dayInfo } = this.input;
    const n = this.n;
    const rowOnDay = new Map<number, Row>();
    rows.forEach((row, r) => rowOnDay.set(dayOf[r], row));
    const availSet = new Set(available);

    // How busy a day is for this trip: the mean cost of its theme parks.
    const themeParks = this.input.parks.filter((p) => p.type === "theme_park");
    const busyness = (i: number): number | undefined =>
      mean(
        themeParks
          .map((p) => this.cell(p.slug, this.dates[i]))
          .filter((c) => c.status !== "CLOSED" && waitOf(c) != null)
          .map((c) => waitOf(c) as number),
      );
    const availBusy = available
      .map((i) => busyness(i))
      .filter((v): v is number => v != null);
    const busyMedian = median(availBusy);

    const visitDates = new Map<string, string[]>();
    for (const [day, row] of rowOnDay) {
      for (const u of row.units) {
        visitDates.set(u.slug, [
          ...(visitDates.get(u.slug) ?? []),
          this.dates[day],
        ]);
      }
    }

    const defaultLimit =
      options.rest.mode === "max" ? options.rest.n : DEFAULT_MAX_STREAK;

    const out: PlannedDay[] = [];
    const ticketDays = this.tickets.map(() => 0);
    let run = 0;
    for (let i = 0; i < n; i++) {
      const date = this.dates[i];
      const row = rowOnDay.get(i);
      const ti = row?.units[0].ticket;
      let ticket: Pick<PlannedDay, "ticket" | "ticketDay" | "ticketAllowance"> =
        {};
      if (row && ti !== undefined) {
        const id = this.tickets[ti].id;
        if (row.units[0].allowance) {
          ticket = { ticket: id, ticketAllowance: true };
        } else {
          ticketDays[ti]++;
          ticket = { ticket: id, ticketDay: ticketDays[ti] };
        }
      }
      if (!row) {
        const precededBy = run;
        run = 0;
        const isArrival = i === 0;
        const isDeparture = i === n - 1 && n > 1;
        if (restLocked.has(i)) {
          out.push({
            date,
            kind: "rest",
            parks: [],
            reasons: ["locked"],
            warnings: [],
          });
          continue;
        }
        if (isArrival || isDeparture) {
          out.push({
            date,
            kind: isArrival ? "arrival" : "departure",
            parks: [],
            reasons: [],
            warnings: [],
          });
          continue;
        }
        const reasons: PlanReason[] = [];
        if (options.rest.mode !== "none" && availSet.has(i)) {
          const b = busyness(i);
          if (b != null && busyMedian != null && b >= busyMedian) {
            reasons.push("busiest_day_spared");
          }
          if (precededBy >= 3) reasons.push("rest_after_streak");
        }
        out.push({ date, kind: "rest", parks: [], reasons, warnings: [] });
        continue;
      }

      run++;
      const reasons = new Set<PlanReason>();
      const warnings = new Set<PlanWarning>();
      if (lockedDays.has(i) && row.lockedDay === i) reasons.add("locked");
      if (options.rest.mode !== "none" && run > defaultLimit)
        warnings.add("long_streak");
      const info = dayInfo[i];
      if (info.weekday >= 1 && info.weekday <= 5 && !info.isHoliday) {
        reasons.add("weekday");
      }
      const prev = i > 0 ? dayInfo[i - 1] : undefined;
      if (
        prev &&
        (prev.isHoliday || prev.isSchoolVacation) &&
        !info.isHoliday &&
        !info.isSchoolVacation
      ) {
        reasons.add("holiday_ends");
      }

      const visits: PlannedParkVisit[] = row.units.map((u) => {
        const c = this.cell(u.slug, date);
        const park = this.parkBySlug.get(u.slug) as PlanPark;
        const { listed } = this.perks(u.slug, date);
        listed.forEach((p) => reasons.add(p));
        if (c.status === "UNKNOWN") warnings.add("hours_not_published");
        if (isHurricanePeak(date)) warnings.add("hurricane_peak");
        else if (hurricaneWeight(date) > 0) warnings.add("hurricane_season");
        if (c.basis === "last_year") warnings.add("estimate_last_year");
        if (c.basis === "long_range") warnings.add("long_range");
        if (park.type === "theme_park") {
          if (c.headlinerWait == null) warnings.add("no_forecast");
          this.dayChoiceReasons(
            u.slug,
            i,
            visitDates.get(u.slug) ?? [],
          ).forEach((r) => reasons.add(r));
          if (this.losesEvening(u.slug, c)) {
            warnings.add(
              options.fireworks === "want"
                ? "fireworks_missed"
                : "party_night_short_hours",
            );
          }
          if (heatOf(c.weather)) warnings.add("extreme_heat");
        } else if (weatherFactor(c.weather).warmDry) {
          reasons.add("water_park_weather");
        }
        if (
          i > 0 &&
          rowOnDay.get(i - 1)?.units.some((x) => x.slug === u.slug)
        ) {
          warnings.add("adjacent_repeat");
        }
        if (this.relaxWindows && this.outsideTicket(row, i, starts)) {
          warnings.add("outside_ticket_window");
          this.relaxed.add("ticket_window_relaxed");
        }
        return {
          slug: u.slug,
          part: row.units.length > 1 ? "half" : "full",
          ...(c.headlinerWait != null && { headlinerWait: c.headlinerWait }),
          ...(c.crowdLevel && { crowdLevel: c.crowdLevel }),
          basis: c.basis,
          ...(listed.length > 0 && { perks: listed }),
        };
      });

      if (row.units.length === 1) {
        const [{ slug: _slug, part: _part, ...figures }] = visits;
        out.push({
          date,
          kind: "park",
          parks: [{ slug: visits[0].slug, part: "full" }],
          ...figures,
          reasons: [...reasons],
          warnings: [...warnings],
          ...ticket,
        });
      } else {
        out.push({
          date,
          kind: "hop",
          parks: visits,
          reasons: [...reasons],
          warnings: [...warnings],
          ...ticket,
        });
      }
    }

    return {
      exact,
      relaxed: [...this.relaxed],
      days: out,
      parks: this.summaries(visitDates),
      totals: this.totals(rowOnDay, out),
      unplaced,
      ignoredLocks,
      tickets: this.ticketSummaries(out, starts),
    };
  }

  /**
   * Each owned ticket as the plan used it. A start the plan picked is the
   * first day it is used on where that start is on sale (its window then
   * reaches furthest), else the start the search kept.
   */
  private ticketSummaries(
    days: PlannedDay[],
    starts: number[],
  ): PlannedTicket[] {
    return this.tickets.map((t, ti) => {
      const used = days.filter((d) => d.ticket === t.id);
      const usedDays = used.filter((d) => d.ticketDay !== undefined).length;
      const allowanceVisits = used.length - usedDays;
      const summary: PlannedTicket = { id: t.id, usedDays, allowanceVisits };
      if (!Number.isFinite(t.window)) return summary;
      let start = t.start;
      if (start === undefined) {
        const first = used[0]?.date;
        const onSale = (date: string) =>
          !t.startOptions || t.startOptions.includes(date);
        const outside = used.some((d) =>
          d.warnings.includes("outside_ticket_window"),
        );
        start =
          first && onSale(first) && !outside
            ? first
            : Number.isFinite(starts[ti])
              ? this.dates[starts[ti]]
              : first;
      }
      if (start === undefined) return summary;
      return {
        ...summary,
        start,
        ...(t.start === undefined && { startChosen: true as const }),
        end: addIsoDays(start, t.window - 1),
      };
    });
  }

  /** `quietest_in_window` and `runner_up_close` for one visit. */
  private dayChoiceReasons(
    slug: string,
    day: number,
    plannedDates: string[],
  ): PlanReason[] {
    const chosen = waitOf(this.cell(slug, this.dates[day]));
    if (chosen == null) return [];
    const others: number[] = [];
    let quietest = true;
    this.dates.forEach((date, i) => {
      if (i === day) return;
      const c = this.cell(slug, date);
      const wait = waitOf(c);
      if (c.status === "CLOSED" || wait == null) return;
      if (wait < chosen) quietest = false;
      if (!plannedDates.includes(date)) others.push(wait);
    });
    const reasons: PlanReason[] = [];
    if (quietest) reasons.push("quietest_in_window");
    const runnerUp = others.length ? Math.min(...others) : undefined;
    if (
      runnerUp != null &&
      runnerUp - chosen <= PlanCostModel.runnerUpMinutes
    ) {
      reasons.push("runner_up_close");
    }
    return reasons;
  }

  /** Per park: its dates and the three cheapest other open days of the trip. */
  private summaries(visitDates: Map<string, string[]>): PlannedParkSummary[] {
    return this.input.parks.map((park) => {
      const dates = [...(visitDates.get(park.slug) ?? [])].sort();
      const alternatives = this.dates
        .map((date, i) => ({ date, i, c: this.cell(park.slug, date) }))
        .filter(
          ({ date, c }) => !dates.includes(date) && c.status === "OPERATING",
        )
        .sort(
          (a, b) =>
            this.cost(park.slug, a.i) - this.cost(park.slug, b.i) ||
            a.date.localeCompare(b.date),
        )
        .slice(0, 3)
        .map(({ date, c }) => ({
          date,
          ...(c.headlinerWait != null && { headlinerWait: c.headlinerWait }),
          ...(c.crowdLevel && { crowdLevel: c.crowdLevel }),
        }));
      return { slug: park.slug, dates, alternatives };
    });
  }

  /**
   * The planned visits' mean headliner wait against the same parks on an
   * average open day of the trip window: like with like, per visit.
   */
  private totals(rowOnDay: Map<number, Row>, days: PlannedDay[]): PlanTotals {
    const planned: number[] = [];
    const average: number[] = [];
    let visits = 0;
    for (const [day, row] of rowOnDay) {
      for (const u of row.units) {
        visits++;
        const wait = waitOf(this.cell(u.slug, this.dates[day]));
        if (wait == null) continue;
        const windowMean = mean(
          this.dates
            .map((date) => this.cell(u.slug, date))
            .filter((c) => c.status !== "CLOSED" && waitOf(c) != null)
            .map((c) => waitOf(c) as number),
        );
        if (windowMean == null) continue;
        planned.push(wait);
        average.push(windowMean);
      }
    }
    const headlinerWait = mean(planned);
    const averageDayWait = mean(average);
    // Per park day, as a hop day is costed: half of each park.
    let heat = 0;
    let hurricane = 0;
    for (const [day, row] of rowOnDay) {
      const share = row.units.length > 1 ? 0.5 : 1;
      for (const u of row.units) {
        const parts = this.costParts(u.slug, this.dates[day]);
        heat += share * parts.heat;
        hurricane += share * parts.hurricane;
      }
    }
    const parkDays = rowOnDay.size;
    const penalties = penaltiesOf(heat, hurricane, parkDays);
    return {
      ...(headlinerWait != null && {
        headlinerWait: Math.round(headlinerWait),
      }),
      ...(averageDayWait != null && {
        averageDayWait: Math.round(averageDayWait),
      }),
      ...(headlinerWait != null &&
        averageDayWait != null && {
          savedMinutesPerRide: Math.round(averageDayWait - headlinerWait),
        }),
      orderMatters:
        headlinerWait != null &&
        averageDayWait != null &&
        Math.round(averageDayWait) - Math.round(headlinerWait) >=
          ORDER_MATTERS_MINUTES,
      ...(penalties && { penalties }),
      parkDays: days.filter((d) => d.kind === "park" || d.kind === "hop")
        .length,
      visits,
      restDays: days.filter((d) => d.kind === "rest").length,
    };
  }
}

/**
 * Heat and hurricane minutes summed over a plan's park days, as the mean per
 * park day, one decimal; undefined when both are zero.
 */
export function penaltiesOf(
  heat: number,
  hurricane: number,
  parkDays: number,
): PlanPenalties | undefined {
  if (parkDays === 0) return undefined;
  const per = (v: number) => Math.round((v / parkDays) * 10) / 10;
  const out: PlanPenalties = {
    ...(per(heat) > 0 && { heat: per(heat) }),
    ...(per(hurricane) > 0 && { hurricane: per(hurricane) }),
  };
  return Object.keys(out).length > 0 ? out : undefined;
}

/** `rest=auto` on a trip this long or longer keeps a rest day per five days. */
export const LONG_TRIP_DAYS = 8;

/**
 * How many more visits a trip has room for once the rest days the rest rule
 * keeps are set aside: the days a park can go on (not a free arrival or
 * departure day, not a pinned rest day), less the visits, less the rest days.
 * `rest=auto` keeps a run of park days to four and, on a trip of eight days or
 * more, one rest day per five days (pinned rest days count); `rest=max:n` keeps
 * the runs to n; `rest=none` keeps none. `split=auto` gives this room to
 * second visits (`trip-plan.assembly.ts`).
 */
export function extraVisitRoom(input: PlanInput): number {
  const { days, options, parks } = input;
  const n = days.length;
  const lockedDates = new Set(
    options.locks.filter((l) => l.target !== "rest").map((l) => l.date),
  );
  const restDates = new Set(
    options.locks.filter((l) => l.target === "rest").map((l) => l.date),
  );
  const open = days.map((d, i) => {
    if (restDates.has(d.date)) return false;
    const free =
      (i === 0 && options.arrival === "free") ||
      (i === n - 1 && n > 1 && options.departure === "free");
    return !free || lockedDates.has(d.date);
  });
  const available = open.filter(Boolean).length;
  const limit =
    options.rest.mode === "auto"
      ? DEFAULT_MAX_STREAK
      : options.rest.mode === "max"
        ? options.rest.n
        : Infinity;
  let streakRest = 0;
  if (Number.isFinite(limit)) {
    let run = 0;
    for (const isOpen of [...open, false]) {
      if (isOpen) run++;
      else {
        streakRest += Math.floor(run / (limit + 1));
        run = 0;
      }
    }
  }
  const ratioRest =
    options.rest.mode === "auto" && n >= LONG_TRIP_DAYS
      ? Math.max(0, Math.floor(n / 5) - restDates.size)
      : 0;
  const visits = parks.reduce((sum, p) => sum + p.visits, 0);
  return Math.max(0, available - visits - Math.max(streakRest, ratioRest));
}

/** Plans a trip; see the module comment and docs/frontend/trip-regions.md. */
export function optimizeTrip(input: PlanInput): OptimizedPlan {
  return new TripOptimizer(input).run();
}
