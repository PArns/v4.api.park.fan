/**
 * The trip optimizer: which park goes on which day of a vacation so that the
 * headliners queue least, under the visitor's rules (blocks per resort, rest
 * days, park hopping, party nights, hotel perks, pinned days).
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
  /** `parties=avoid`: a shortened party night costs this much more again. */
  partyAvoidFactor: 1.25,
  /** `parties=prefer`: a party night costs this much, the hours left aside. */
  partyPreferFactor: 0.8,
  /** Two visits of one park on consecutive days. */
  adjacentRepeatPenalty: 40,
  /** A hop day: half of each park plus the move between them. */
  hopTransferPenalty: 10,
  /** Flat days: warm and dry is cheaper, cool or wet dearer. */
  warmDryFactor: 0.8,
  coolWetFactor: 1.25,
  /** `runner_up_close`: another day within this many minutes. */
  runnerUpMinutes: 5,
} as const;

export type PlanDayStatus = "OPERATING" | "CLOSED" | "UNKNOWN";
export type PlanBasis = "forecast" | "long_range" | "last_year" | "none";

/** One park on one day, as the optimizer needs it. */
export interface PlanCell {
  status: PlanDayStatus;
  headlinerWait?: number;
  crowdLevel?: string;
  basis: PlanBasis;
  /** Hours the day guest has: the OPERATING row's open to close. */
  dayHours?: number;
  early?: { kind: string };
  /** The feed's early row was lost to the evening one (see the calendar). */
  earlyUnknown?: boolean;
  evening?: { kind: "ticketed_event" | "extended_evening" };
  weather?: { tempMax?: number; precipitationMm?: number; icon?: number };
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

export type PartiesOption = "avoid" | "allow" | "prefer";

export interface PlanOptions {
  arrival: "free" | "park";
  departure: "free" | "park";
  blocks: boolean;
  /** The groups `blocks` keeps together (the resorts; day trips stay free). */
  blockGroups: string[];
  parties: PartiesOption;
  rest: RestOption;
  hop: boolean;
  /** Parks that may share a hop day: same resort, a short leg. */
  hopPairs: Array<[string, string]>;
  perkRules: PerkRule[];
  locks: PlanLock[];
}

export interface PlanInput {
  days: PlanDayInfo[];
  parks: PlanPark[];
  /** cells[slug][date]; a missing cell reads as UNKNOWN. */
  cells: Record<string, Record<string, PlanCell>>;
  options: PlanOptions;
}

export type PlanReason =
  | "quietest_in_window"
  | "runner_up_close"
  | "weekday"
  | "holiday_ends"
  | PerkKind
  | "party_night"
  | "water_park_weather"
  | "busiest_day_spared"
  | "rest_after_streak"
  | "locked";

export type PlanWarning =
  | "party_night_short_hours"
  | "hours_not_published"
  | "estimate_last_year"
  | "long_range"
  | "no_forecast"
  | "adjacent_repeat"
  | "long_streak";

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

export interface PlanTotals {
  headlinerWait?: number;
  averageDayWait?: number;
  savedMinutesPerRide?: number;
  parkDays: number;
  visits: number;
  restDays: number;
}

export interface OptimizedPlan {
  exact: boolean;
  /** Rules the plan had to loosen: `blocks_relaxed`, `streak_relaxed`. */
  relaxed: string[];
  days: PlannedDay[];
  parks: PlannedParkSummary[];
  totals: PlanTotals;
  unplaced: Array<{ slug: string; reason: "no_open_day" | "no_day_left" }>;
  ignoredLocks: Array<{
    lock: string;
    reason: "outside_trip" | "closed" | "day_taken" | "no_visit_left";
  }>;
}

// ── internals ───────────────────────────────────────────────────────────────

interface Unit {
  slug: string;
  visit: number;
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
  /** `true` where a row may not go (outside its resort's block). */
  mask: (row: Row, day: number) => boolean;
  /** Visits this structure leaves out for lack of days. */
  dropped: Unit[];
}

interface SearchResult {
  rows: Row[];
  solution: Solution;
  dropped: Unit[];
  exact: boolean;
}

interface StructureResult {
  solution: Solution | null;
  /** The solution is the best this structure has. */
  provable: boolean;
}

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

  constructor(private readonly input: PlanInput) {
    this.n = input.days.length;
    this.dates = input.days.map((d) => d.date);
    this.dates.forEach((d, i) => this.dateIndex.set(d, i));
    for (const park of input.parks) {
      this.parkBySlug.set(park.slug, park);
      const cells = this.dates.map((d) => this.cell(park.slug, d));
      const waits = cells
        .filter((c) => c.status !== "CLOSED" && c.headlinerWait != null)
        .map((c) => c.headlinerWait as number);
      this.neutral.set(
        park.slug,
        median(waits) ?? PlanCostModel.defaultNeutralWait,
      );
      const hours = cells
        .filter((c) => c.status === "OPERATING" && c.dayHours)
        .map((c) => c.dayHours as number);
      const mh = median(hours);
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
      let shown = true;
      if (rule.needs === "always") applies = true;
      else if (rule.needs === "extended_evening") {
        applies = c.evening?.kind === "extended_evening";
      } else if (c.early) {
        applies = true;
      } else if (c.earlyUnknown) {
        // The early row was overwritten by the evening one in the feed: count
        // the perk so a party night is not charged twice, but do not claim it.
        applies = true;
        shown = false;
      }
      if (!applies) continue;
      factor *= 1 - PERK_SHARE[rule.perk];
      if (shown && !listed.includes(rule.perk)) listed.push(rule.perk);
    }
    return { listed, factor };
  }

  /** The hours factor: how much shorter the day guest's day is than usual. */
  hoursFactor(slug: string, c: PlanCell): number {
    const usual = this.medianHours.get(slug);
    if (!usual || !c.dayHours || c.dayHours <= 0) return 1;
    return Math.min(
      PlanCostModel.hoursFactorCap,
      Math.max(1, usual / c.dayHours),
    );
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
    const park = this.parkBySlug.get(slug);
    const c = this.cell(slug, date);
    if (!park || c.status === "CLOSED") return Infinity;
    const unknown =
      c.status === "UNKNOWN" ? PlanCostModel.unknownDayPenalty : 0;
    if (park.type !== "theme_park") {
      return (
        PlanCostModel.flatDayCost * weatherFactor(c.weather).factor + unknown
      );
    }
    const base =
      c.headlinerWait ??
      (this.neutral.get(slug) ?? PlanCostModel.defaultNeutralWait) +
        PlanCostModel.noFigurePenalty;
    const hours = this.hoursFactor(slug, c);
    const party = c.evening?.kind === "ticketed_event";
    const { parties } = this.input.options;
    let factor: number;
    if (party && parties === "prefer") {
      factor = PlanCostModel.partyPreferFactor;
    } else {
      factor =
        hours *
        (party && hours > 1 && parties === "avoid"
          ? PlanCostModel.partyAvoidFactor
          : 1);
    }
    return base * factor * this.perks(slug, date).factor + unknown;
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

    let pool = placeable;
    let useBlocks = options.blocks;
    let best: SearchResult | null = null;
    for (let attempt = 0; attempt <= units.length; attempt++) {
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
    const { options } = this.input;
    const lockedRows: Row[] = pool
      .filter((u) => lockedUnitDay.has(u))
      .map((u) => ({ units: [u], lockedDay: lockedUnitDay.get(u) }));
    const free = pool.filter((u) => !lockedUnitDay.has(u));
    const capacity = available.length - lockedRows.length;

    // Hop days are only for a trip shorter than its visits.
    let pairings: Row[][] = [[]];
    const overflow = free.length - capacity;
    if (overflow > 0 && options.hop) {
      const found = this.hopPairings(free, overflow);
      if (found.length > 0) pairings = found;
    }

    // A repeat visit is dropped first, then the park whose best day is dearest.
    const bestCost = (slug: string) =>
      Math.min(...available.map((d) => this.cost(slug, d)));
    const dropOrder = [...free].sort(
      (a, b) =>
        b.visit - a.visit ||
        bestCost(b.slug) - bestCost(a.slug) ||
        a.slug.localeCompare(b.slug),
    );

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
      for (const mask of this.blockMasks(rows, useBlocks)) {
        structures.push({ rows, mask, dropped });
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
      const matrix = this.matrix(s.rows, available, s.mask);
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
        };
      }
    }
    if (!best && this.cutShort && bounded.length > 0) {
      // Out of budget before anything kept the rule: the cheapest structure's
      // plain assignment is still a plan, and the answer says it is not exact.
      const s = bounded[0];
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
      };
    }
    if (!best) return null;
    const objective = best.solution.objective;
    best.exact =
      !this.cutShort && unproven.every((bound) => bound >= objective - 1e-9);
    return best;
  }

  /** The hop pairings of `need` pairs, or fewer when that many cannot be made. */
  private hopPairings(free: Unit[], need: number): Row[][] {
    const eligible = this.input.options.hopPairs.filter(
      ([a, b]) =>
        a !== b &&
        free.some((u) => u.slug === a) &&
        free.some((u) => u.slug === b),
    );
    for (let size = need; size >= 1; size--) {
      const combos: Row[][] = [];
      const left = new Map<string, Unit[]>();
      for (const u of free) left.set(u.slug, [...(left.get(u.slug) ?? []), u]);
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
      if (combos.length > 0) return combos;
    }
    return [];
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
    const groups = [
      ...new Set(
        rows.map((r) => this.rowGroup(r)).filter((g) => blockable.has(g)),
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
  ): number[][] {
    const lockedDays = new Set(
      rows.filter((r) => r.lockedDay !== undefined).map((r) => r.lockedDay),
    );
    return rows.map((row) =>
      days.map((day) => {
        if (mask(row, day)) return Infinity;
        if (row.lockedDay === undefined && lockedDays.has(day)) return Infinity;
        return this.rowCost(row, day);
      }),
    );
  }

  private objective(rows: Row[], dayOf: number[]): Solution {
    const base = rows.reduce((s, row, r) => s + this.rowCost(row, dayOf[r]), 0);
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
    let dayOf = rowToCol.map((col) => available[col]);
    let provable = true;

    if (longestRun(this.parkDayFlags(dayOf)) > limit) {
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
      if (count > REST_EXACT_LIMIT || found.cutShort) provable = false;
      // No spare pattern within the rule leaves every visit an open day here.
      if (!found.dayOf) return { solution: null, provable };
      dayOf = found.dayOf;
    }

    let solution = this.objective(rows, dayOf);
    if (solution.objective > solution.base) {
      // The Hungarian does not see the repeat penalty; a local search can.
      solution = this.spreadRepeats(rows, available, matrix, solution, limit);
      provable = false;
    }
    return { solution, provable };
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
    const result = this.solve(sub);
    if (!result.feasible) return null;
    return {
      dayOf: result.rowToCol.map((k) => available[cols[k]]),
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
          .filter((c) => c.status !== "CLOSED" && c.headlinerWait != null)
          .map((c) => c.headlinerWait as number),
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
    let run = 0;
    for (let i = 0; i < n; i++) {
      const date = this.dates[i];
      const row = rowOnDay.get(i);
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
        if (c.basis === "last_year") warnings.add("estimate_last_year");
        if (c.basis === "long_range") warnings.add("long_range");
        if (park.type === "theme_park") {
          if (c.headlinerWait == null) warnings.add("no_forecast");
          this.dayChoiceReasons(
            u.slug,
            i,
            visitDates.get(u.slug) ?? [],
          ).forEach((r) => reasons.add(r));
          if (c.evening?.kind === "ticketed_event") {
            if (options.parties === "prefer") reasons.add("party_night");
            else if (this.hoursFactor(u.slug, c) > 1) {
              warnings.add("party_night_short_hours");
            }
          }
        } else if (weatherFactor(c.weather).warmDry) {
          reasons.add("water_park_weather");
        }
        if (
          i > 0 &&
          rowOnDay.get(i - 1)?.units.some((x) => x.slug === u.slug)
        ) {
          warnings.add("adjacent_repeat");
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
        });
      } else {
        out.push({
          date,
          kind: "hop",
          parks: visits,
          reasons: [...reasons],
          warnings: [...warnings],
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
    };
  }

  /** `quietest_in_window` and `runner_up_close` for one visit. */
  private dayChoiceReasons(
    slug: string,
    day: number,
    plannedDates: string[],
  ): PlanReason[] {
    const chosen = this.cell(slug, this.dates[day]).headlinerWait;
    if (chosen == null) return [];
    const others: number[] = [];
    let quietest = true;
    this.dates.forEach((date, i) => {
      if (i === day) return;
      const c = this.cell(slug, date);
      if (c.status === "CLOSED" || c.headlinerWait == null) return;
      if (c.headlinerWait < chosen) quietest = false;
      if (!plannedDates.includes(date)) others.push(c.headlinerWait);
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
        const wait = this.cell(u.slug, this.dates[day]).headlinerWait;
        if (wait == null) continue;
        const windowMean = mean(
          this.dates
            .map((date) => this.cell(u.slug, date))
            .filter((c) => c.status !== "CLOSED" && c.headlinerWait != null)
            .map((c) => c.headlinerWait as number),
        );
        if (windowMean == null) continue;
        planned.push(wait);
        average.push(windowMean);
      }
    }
    const headlinerWait = mean(planned);
    const averageDayWait = mean(average);
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
      parkDays: days.filter((d) => d.kind === "park" || d.kind === "hop")
        .length,
      visits,
      restDays: days.filter((d) => d.kind === "rest").length,
    };
  }
}

/** Plans a trip; see the module comment and docs/frontend/trip-regions.md. */
export function optimizeTrip(input: PlanInput): OptimizedPlan {
  return new TripOptimizer(input).run();
}
