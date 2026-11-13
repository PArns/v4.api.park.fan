import { wishesWithAdditions } from "./add-ride-fit";
import { gridForDay, withEarlyEntry } from "./day-grid";
import { hasReadableWaitTimes } from "./estimate";
import {
  evaluateFit,
  fitBlocks,
  fitChoiceAll,
  fitLeverView,
  fitOrder,
  fitWishes,
  type FitBlock,
  type FitChoice,
  type FitInput,
  type FitLever,
  type FitOutcome,
} from "./fit";
import {
  MAX_STOPS,
  clashCount,
  headlinersSkipped,
  headlinersToAdd,
  movableEntries,
  optimizeDay,
  scoreCurrent,
  type OptimizeStop,
} from "./optimize";
import { planEntries, type PlannedEntry } from "./plan-entries";
import type {
  DayClock,
  OptimizerDay,
  OptimizerRide,
  PlannerDayPrefs,
  PlannerEntry,
} from "./types";

/**
 * One request to the day optimizer, answered in one pure call: what park.fan's optimise buttons,
 * its fit assistant and Parky asked of the engine in the browser, gathered into one function so
 * every caller gets the same answer. The clock comes in as a value; the day is `/plan/day` plus the
 * rides in works.
 */

/** Which question is asked. */
export type OptimizeMode = "day" | "headliners" | "fit";

/** The visitor's answers in the fit assistant, as wish keys and block ids. */
export interface FitChoiceInput {
  dropped?: readonly string[];
  droppedBlocks?: readonly string[];
  shortBlocks?: readonly string[];
  priority?: readonly string[];
}

/** Everything a caller sends. */
export interface DayOptimizerRequest {
  mode: OptimizeMode;
  /** The day as it stands, free blocks, shows and ticked-off rides included. */
  entries: readonly PlannerEntry[];
  prefs?: PlannerDayPrefs;
  /** `fit` only: slugs of rides being added on top of the entries. */
  add?: readonly string[];
  /** `fit` only: the visitor's answers; absent is everything ticked, nothing pinned. */
  choice?: FitChoiceInput;
  /** `fit` only: also measure what each free block would buy. */
  levers?: boolean;
  /**
   * `day` and `headliners`: answer what a press would do without first asking whether the day
   * holds everything, which is what the button's promise is computed from.
   */
  preview?: boolean;
}

/** Why a day cannot be optimized at all. */
export type OptimizeRefusal =
  | "past-day"
  | "no-plan"
  | "no-hours"
  | "no-wait-times"
  | "no-rides"
  | "unknown-ride";

/** A day scored by the engine's own rules, for a before-and-after. */
export interface PlanScore {
  /** Minutes queued across the stops that happen. */
  totalWaitMinutes: number;
  /** Minutes standing about that the plan decided on. */
  idleMinutes: number;
  /** When the last queue that happens is left, ride included. */
  endMinute: number;
  /** Stops whose queue is joined after closing. */
  overflow: number;
  /** Times the next thing starts before the one before it is over and walked from. */
  clashes: number;
}

/** The plan a press would apply. */
export interface OptimizedPlanView {
  /** In plan order; `entryId: null` is a ride being added. */
  stops: OptimizeStop[];
  totalWaitMinutes: number;
  idleMinutes: number;
  endMinute: number;
  overflow: number;
  /** Rides that never reached the search because {@link MAX_STOPS} was full. */
  capped: number;
}

/** A wish of the fit question, without the ride payload behind it. */
export interface FitWishView {
  key: string;
  entryId: string | null;
  attractionSlug: string;
  attractionName: string;
  headliner: boolean;
  /** Whether the day has a curve for it; a wish without one keeps its minute. */
  replannable: boolean;
}

/** The fit assistant's answer for one choice. */
export interface FitResultView {
  wishes: FitWishView[];
  blocks: FitBlock[];
  /** Wish keys in the order the day is decided in: pinned first, then as offered, laps last. */
  order: string[];
  /** Wishes still ticked. */
  wanted: number;
  /** Wish keys that get a slot before closing, in plan order. */
  fitted: string[];
  /** Ticked wishes that find no minute before closing. */
  missed: string[];
  endMinute: number;
  totalWaitMinutes: number;
  idleMinutes: number;
  /** The entries the day keeps under this choice, before the stops are laid over them. */
  entries: PlannerEntry[];
  /** The plan to lay over them. */
  stops: OptimizeStop[];
  /** What each free block would buy, best first; `null` where not asked for. */
  levers: FitLever[] | null;
  /** Lever keys (`kind:entryId`) already pulled in this choice; `null` where not asked for. */
  applied: string[] | null;
}

/** What the engine answers. */
export interface DayOptimizerResult {
  mode: OptimizeMode;
  /**
   * `optimized`: a plan that beats the day. `unchanged`: the day is as good as this can make it.
   * `needs-fit`: something asked for has no slot before closing, so the assistant opens instead.
   * `fit`: the assistant's answer. `refused`: see {@link refusal}.
   */
  status: "optimized" | "unchanged" | "needs-fit" | "fit" | "refused";
  refusal: OptimizeRefusal | null;
  /** The clock the day was planned against. */
  clock: DayClock;
  maxStops: number;
  /** Slugs of the rides offered on top of the entries. */
  add: string[];
  /** Headliners left out because this party cannot ride them (`headliners` only). */
  skippedHeadliners: number;
  /** Entries the engine may move: not done, not a block, not under way, not in works. */
  movable: number;
  before: PlanScore | null;
  after: PlanScore | null;
  plan: OptimizedPlanView | null;
  /** The day as the answer leaves it, in start order; added rides carry `id: null`. */
  entries: PlannedEntry[] | null;
  fit: FitResultView | null;
}

function choiceFrom(input: FitChoiceInput | undefined): FitChoice {
  return {
    dropped: new Set(input?.dropped ?? []),
    droppedBlocks: new Set(input?.droppedBlocks ?? []),
    shortBlocks: new Set(input?.shortBlocks ?? []),
    priority: [...(input?.priority ?? [])],
  };
}

function fitResult(
  input: FitInput,
  choice: FitChoice,
  withLevers: boolean,
  outcome: FitOutcome = evaluateFit(input, choice),
): FitResultView {
  const view = withLevers ? fitLeverView(input, choice, outcome) : null;
  return {
    wishes: input.wishes.map((wish) => ({
      key: wish.key,
      entryId: wish.entryId,
      attractionSlug: wish.attractionSlug,
      attractionName: wish.attractionName,
      headliner: wish.headliner,
      replannable: wish.ride !== null,
    })),
    blocks: [...input.blocks],
    order: fitOrder(input, choice).map((wish) => wish.key),
    wanted: input.wishes.filter((wish) => !choice.dropped.has(wish.key)).length,
    fitted: outcome.fitted,
    missed: outcome.missed,
    endMinute: outcome.endMinute,
    totalWaitMinutes: outcome.totalWaitMinutes,
    idleMinutes: outcome.idleMinutes,
    entries: outcome.entries,
    stops: outcome.stops,
    levers: view ? view.levers : null,
    applied: view ? [...view.applied] : null,
  };
}

/** The day as a plan leaves it, for counting its clashes; the same mapping the frontend used. */
function withPlan(
  entries: readonly PlannerEntry[],
  stops: readonly { entryId: string | null; startMinute: number }[],
): PlannerEntry[] {
  const moved = new Map<string, number>();
  for (const stop of stops)
    if (stop.entryId) moved.set(stop.entryId, stop.startMinute);
  return entries.map((entry) =>
    moved.has(entry.id)
      ? { ...entry, startMinute: moved.get(entry.id) as number }
      : entry,
  );
}

/**
 * Answers one request. Refusals come first and in a fixed order, so a caller can tell a past day
 * from a park nobody can read; then `fit` answers the assistant, and `day` and `headliners` do what
 * a press did in the browser: ask whether the day holds everything (the fit question, unless
 * `preview`), and otherwise plan it.
 */
export function runDayOptimizer(
  payload: OptimizerDay | null | undefined,
  request: DayOptimizerRequest,
  clock: DayClock,
): DayOptimizerResult {
  const answer: DayOptimizerResult = {
    mode: request.mode,
    status: "refused",
    refusal: null,
    clock,
    maxStops: MAX_STOPS,
    add: [],
    skippedHeadliners: 0,
    movable: 0,
    before: null,
    after: null,
    plan: null,
    entries: null,
    fit: null,
  };
  const refuse = (refusal: OptimizeRefusal): DayOptimizerResult => ({
    ...answer,
    refusal,
  });

  // A walked day is a record, not a plan.
  if (clock.phase === "past") return refuse("past-day");
  const day = withEarlyEntry(payload, request.prefs?.earlyEntry);
  if (!day) return refuse("no-plan");
  const grid = gridForDay(day);
  if (!grid) return refuse("no-hours");
  if (!hasReadableWaitTimes(day.context)) return refuse("no-wait-times");
  if (day.rides.length === 0) return refuse("no-rides");

  const { entries } = request;
  answer.movable = movableEntries(entries, clock, day).length;

  if (request.mode === "fit") {
    const rides: OptimizerRide[] = [];
    for (const slug of request.add ?? []) {
      const ride = day.rides.find((r) => r.attractionSlug === slug);
      if (!ride) return refuse("unknown-ride");
      rides.push(ride);
    }
    const input: FitInput = {
      day,
      grid,
      entries,
      wishes: wishesWithAdditions(day, entries, rides, clock),
      blocks: fitBlocks(entries),
      clock,
    };
    const fit = fitResult(
      input,
      choiceFrom(request.choice),
      request.levers === true,
    );
    return {
      ...answer,
      status: "fit",
      add: rides.map((ride) => ride.attractionSlug),
      fit,
      entries: planEntries(fit.entries, fit.stops),
    };
  }

  const add =
    request.mode === "headliners"
      ? headlinersToAdd(day, entries, request.prefs)
      : [];
  answer.add = add.map((ride) => ride.attractionSlug);
  answer.skippedHeadliners =
    request.mode === "headliners"
      ? headlinersSkipped(day, entries, request.prefs)
      : 0;

  // A dialog in front of a button that would have done the right thing is a dialog people learn
  // to dismiss, so the question is only asked where something does not fit.
  if (!request.preview) {
    const input: FitInput = {
      day,
      grid,
      entries,
      wishes: fitWishes(day, entries, add, clock),
      blocks: fitBlocks(entries),
      clock,
    };
    if (input.wishes.length > 0) {
      const all = fitChoiceAll();
      const outcome = evaluateFit(input, all);
      if (outcome.fitted.length < input.wishes.length) {
        return {
          ...answer,
          status: "needs-fit",
          fit: fitResult(input, all, true, outcome),
        };
      }
    }
  }

  const current = scoreCurrent({ day, grid, entries, clock });
  answer.before = current
    ? {
        totalWaitMinutes: current.totalWaitMinutes,
        idleMinutes: current.idleMinutes,
        endMinute: current.endMinute,
        overflow: current.overflow,
        clashes: clashCount(day, entries, clock),
      }
    : null;

  const plan = optimizeDay({ day, grid, entries, add, clock });
  if (!plan) return { ...answer, status: "unchanged" };

  return {
    ...answer,
    status: "optimized",
    after: {
      totalWaitMinutes: plan.totalWaitMinutes,
      idleMinutes: plan.idleMinutes,
      endMinute: plan.endMinute,
      overflow: plan.overflow,
      clashes: clashCount(day, withPlan(entries, plan.stops), clock),
    },
    plan: {
      stops: plan.stops,
      totalWaitMinutes: plan.totalWaitMinutes,
      idleMinutes: plan.idleMinutes,
      endMinute: plan.endMinute,
      overflow: plan.overflow,
      capped: plan.capped,
    },
    entries: planEntries(entries, plan.stops),
  };
}
