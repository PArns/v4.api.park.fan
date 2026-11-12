import {
  type DayGrid,
  RIDE_DURATION_MIN,
  SNAP_MIN_FINE,
  dayStartMin,
  rideFloor,
} from "./day-grid";
import {
  estimateFor,
  hasReadableWaitTimes,
  plannedMinutes,
  worksPeriodFor,
} from "./estimate";
import { SLOT_MIN } from "./ride-series";
import {
  entryPlace,
  isBlockEntry,
  transferBetween,
  type LegPlace,
} from "./leg";
import { partyFlags } from "./party";
import type {
  DayClock,
  OptimizerDay as PlanDay,
  OptimizerRide as PlanDayRide,
  PlannerDayPrefs,
  PlannerEntry,
} from "./types";

/**
 * Orders a day so it costs the least queueing. Pure, and every decision is written down rather
 * than buried in a weight.
 *
 * What is minimised, in this order:
 *
 * 1. **Rides that do not fit before the park closes**, in the tiers of {@link OVERFLOW_STRIDE}.
 * 2. **What the day costs**: `cost = Σ wait + IDLE_WEIGHT × Σ idle`, where idle is only the
 *    standing about this file decided on (see {@link idleFor}).
 * 3. **The moment the last queue is left**, only to settle a tie.
 *
 * A ride the optimiser adds is never filed past closing; an entry the visitor already has keeps
 * its overflow block, because deleting their own plan is worse. With `clock`, nothing is planned
 * before now, an entry already under way is fixed, and a walked day is refused. Done entries and
 * free blocks are fixed too: only undone ride entries move.
 *
 * The schedule is contiguous, so the order is the only free variable. The one exception is a
 * deliberate delay, which {@link placementsFor} offers as a Pareto front over (cost, clock). Rope
 * drop is not a special case: it is what minimising the sum produces where the curve says so.
 * A park with no readable wait times has no ordering to prefer ({@link canOptimize}).
 *
 * Ported from park.fan's `lib/planner/optimize.ts`, which ran it in the browser; the contract is
 * docs/frontend/plan-day/optimize.md, and the rules it keeps are park.fan's
 * docs/rules/the-planners-day-ends-when-the-park-closes-and-a-headliner-is.md and
 * docs/rules/a-day-that-does-not-fit-opens-an-assistant-not-a-footnote.md.
 */

/**
 * How long the optimiser may leave somebody standing about to let a queue fall.
 *
 * Priced at {@link IDLE_WEIGHT}, a long enough collapse would justify hours of waiting, so the
 * ceiling is a judgement about the visitor: past two hours it stops being a plan anybody follows.
 */
export const MAX_DELAY_MIN = 120;

/**
 * What a minute of standing about costs, in minutes of queueing: a free minute is still worth
 * something to the visitor, and less than one spent in a queue.
 *
 * Two cases in `optimize.vectors.spec.ts` bracket it at 1/6 < k < 14/15 (§17: five queued
 * minutes may not buy thirty idle ones; §11: seventy queued minutes must outweigh seventy-five idle
 * ones), and no lexicographic order over queue and clock gets both right. Exported so the tests
 * read it instead of restating 0.5.
 */
export const IDLE_WEIGHT = 0.5;

/**
 * How a plan's overflow is ranked, in tiers rather than as one count, worst first: an entry the
 * visitor already had, a headliner being added, anything else being added, and then which of them
 * by {@link Candidate.dropWeight}. The first three are disjoint counts.
 *
 * A plain count left which ride falls out to cost, and cost minimisation drops the longest queue,
 * which is the ride most people came for. See
 * park.fan's docs/rules/the-planners-day-ends-when-the-park-closes-and-a-headliner-is.md.
 *
 * Packed into one number by {@link overflowKey} so the Pareto front keeps its dimensions. 32 has to
 * clear {@link MAX_STOPS}.
 */
const OVERFLOW_STRIDE = 32;

/** Clears the largest possible {@link OverflowCounts.dropWeight}, 24 × 24 = 576. */
const DROP_WEIGHT_STRIDE = 1024;

/** Overflow as one comparable number — entries, headliners, count, then which. */
function overflowKey(counts: OverflowCounts): number {
  return (
    ((counts.overflowEntries * OVERFLOW_STRIDE + counts.overflowHeadliners) *
      OVERFLOW_STRIDE +
      counts.overflow) *
      DROP_WEIGHT_STRIDE +
    counts.dropWeight
  );
}

/** The tiers of {@link OVERFLOW_STRIDE}, as every scored shape carries them. */
interface OverflowCounts {
  /** Everything that did not fit, whatever it was. */
  overflow: number;
  /** Of those, the ones the visitor already had. */
  overflowEntries: number;
  /** Of those, the headliners being ADDED. Disjoint from `overflowEntries`. */
  overflowHeadliners: number;
  /** The summed {@link Candidate.dropWeight} of what did not fit, ranked last of the four. */
  dropWeight: number;
}

const NO_OVERFLOW: OverflowCounts = {
  overflow: 0,
  overflowEntries: 0,
  overflowHeadliners: 0,
  dropWeight: 0,
};

/**
 * Which tier a stop that did not fit falls into. Nothing where it fits.
 *
 * One function so the beam, the scheduler and {@link scoreCurrent} cannot count it differently.
 */
function overflowTier(
  fits: boolean,
  candidate: { entryId: string | null; headliner: boolean; dropWeight: number },
): OverflowCounts {
  if (fits) return NO_OVERFLOW;
  if (candidate.entryId !== null)
    return {
      overflow: 1,
      overflowEntries: 1,
      overflowHeadliners: 0,
      dropWeight: 0,
    };
  return {
    overflow: 1,
    overflowEntries: 0,
    overflowHeadliners: candidate.headliner ? 1 : 0,
    dropWeight: candidate.dropWeight,
  };
}

/**
 * Whether the clock has passed this entry's start: at 14:00 somebody is already in the queue a
 * 13:30 block gives, so a started slot is fixed, not only a finished one.
 *
 * Strictly `<`: the floor snaps up to the quarter hour, so a press at 14:00 files the next ride AT
 * 14:00, and with `<=` that ride would count as started the instant it is planned.
 */
function hasStarted(entry: PlannerEntry, clock?: DayClock): boolean {
  return clock?.phase === "today" && entry.startMinute < clock.nowMinute;
}

/**
 * The entries a press may move: not ticked off, not a free block, a real ride, not already under
 * way, and not shut for maintenance that day (`worksPeriodFor`): a ride that does not run is not
 * planned, so no press places, scores or keeps it; its block stays where it is, marked.
 *
 * Exported so the search, `isExecutable`, {@link scoreCurrent}, the fit assistant and the bar share
 * one filter: every bug this rule had was two copies of it disagreeing.
 */
export function movableEntries(
  entries: readonly PlannerEntry[],
  clock?: DayClock,
  day?: PlanDay | null,
): PlannerEntry[] {
  return entries.filter(
    (entry) =>
      !entry.done &&
      !entry.custom &&
      entry.attractionSlug &&
      !hasStarted(entry, clock) &&
      !worksPeriodFor(day, entry.attractionSlug),
  );
}

/**
 * The one number every comparison in this file is made of.
 *
 * Not rounded, and that matters: at {@link IDLE_WEIGHT} a cost lands on halves,
 * and rounding them away would collapse exactly the differences a delay of one
 * SNAP_MIN_FINE step is decided by.
 */
function planCost(waitMinutes: number, idleMinutes: number): number {
  return waitMinutes + IDLE_WEIGHT * idleMinutes;
}

/**
 * How many partial plans the search carries forward at each step.
 *
 * A beam rather than Held–Karp because the cost is time-dependent, so its subproblems do not exist.
 * §16 of `optimize.vectors.spec.ts` checks it against all 40,320 orders of eight rides.
 */
export const BEAM_WIDTH = 192;

/**
 * Labels kept per (visited set, last ride): a small Pareto front (see {@link dominates}) rather
 * than the single best, because a prefix half an hour ahead on the clock and a few minutes worse on
 * queueing is often the one that fits the last headliner before closing.
 */
const LABELS_PER_KEY = 4;

/**
 * Stops past this are dropped. No park has this many headliners.
 *
 * Three things are pinned to it, so raising it past 31 changes all three in one edit: the visited
 * bitmask in `search` (`1 << index`), its `placed * 64` map key, and {@link OVERFLOW_STRIDE}.
 */
export const MAX_STOPS = 24;

/** How many improvement passes the local search may run before it gives up. */
const MAX_IMPROVE_PASSES = 40;

/** One stop of an optimised plan. */
export interface OptimizeStop {
  /** The entry this stop is, or `null` for a ride the optimiser is adding. */
  entryId: string | null;
  attractionSlug: string;
  attractionName: string;
  startMinute: number;
  /** The wait the schedule was built on. `null` where the day has no figure. */
  waitMinutes: number | null;
  /** Whether this stop's queue is joined before the park closes. */
  fits: boolean;
}

/** What {@link optimizeDay} hands back. */
export interface OptimizedPlan {
  stops: OptimizeStop[];
  /** Minutes queued across every stop that has a figure. */
  totalWaitMinutes: number;
  /**
   * Minutes spent standing about between stops. Reported because a plan is chosen on
   * `wait + IDLE_WEIGHT × idle`, and the queue total alone cannot tell a tidy day from one with
   * forty minutes of nothing in it.
   */
  idleMinutes: number;
  /** When the last queue is left, in park-local minutes. */
  endMinute: number;
  /** Stops that do not fit before the park closes. */
  overflow: number;
  /**
   * Rides that never reached the search because {@link MAX_STOPS} was full. Reported so a caller
   * does not announce them as added.
   */
  capped: number;
  /** False where the plan is the one that was already there. */
  changed: boolean;
}

/** What {@link optimizeDay} plans from. */
export interface OptimizeInput {
  day: PlanDay | null | undefined;
  grid: DayGrid;
  entries: readonly PlannerEntry[];
  /** Rides to add on top of what the day already holds. */
  add?: readonly PlanDayRide[];
  /**
   * The visitor's own order over {@link add}, most important first.
   *
   * Only consulted where something has to be left out — it decides WHICH, never
   * the order of the day, which is the schedule's job. Slugs not named here
   * fall in behind the ones that are, ranked by the day's own expected queues.
   * See {@link rankHeadliners}.
   */
  priority?: readonly string[];
  /**
   * Where this day sits against the park's clock. Omitted means `future`, which plans the day as if
   * there were no clock. One value rather than a date plus a minute, so a caller cannot set one and
   * forget the other; passed in because "now" is a render-time value and this module is pure.
   */
  clock?: DayClock;
}

/**
 * Whether ordering this day could mean anything.
 *
 * Three ways it cannot, and they are different: no payload at all, no axis to
 * place anything on, and a park whose wait times nobody can read — where every
 * ride costs the same assumed nothing and any order is as good as any other.
 */
export function canOptimize(
  day: PlanDay | null | undefined,
  grid: DayGrid | null,
): boolean {
  if (!day || !grid) return false;
  if (!hasReadableWaitTimes(day.context)) return false;
  return day.rides.length > 0;
}

/**
 * The park's headliners that this day does not have and this party can ride.
 *
 * The curated `isHeadliner`, never the day's busiest rides. The party filter removes rides here and
 * only here: everywhere else `partyFlags` flags without hiding, but this button says "plan the
 * headliners for us".
 */
export function headlinersToAdd(
  day: PlanDay | null | undefined,
  entries: readonly PlannerEntry[],
  prefs: PlannerDayPrefs | undefined,
): PlanDayRide[] {
  if (!day) return [];
  const planned = new Set(
    entries
      .map((entry) => entry.attractionSlug)
      .filter((slug): slug is string => Boolean(slug)),
  );
  return day.rides.filter((ride) => {
    if (!ride.isHeadliner) return false;
    if (planned.has(ride.attractionSlug)) return false;
    const flags = partyFlags(ride, prefs);
    return !flags.tooShort && !flags.wet;
  });
}

/** Headliners this party cannot ride, so a caller can say how many were skipped. */
export function headlinersSkipped(
  day: PlanDay | null | undefined,
  entries: readonly PlannerEntry[],
  prefs: PlannerDayPrefs | undefined,
): number {
  if (!day) return 0;
  const planned = new Set(
    entries
      .map((entry) => entry.attractionSlug)
      .filter((slug): slug is string => Boolean(slug)),
  );
  return day.rides.filter((ride) => {
    if (!ride.isHeadliner || planned.has(ride.attractionSlug)) return false;
    const flags = partyFlags(ride, prefs);
    return flags.tooShort || flags.wet;
  }).length;
}

/**
 * Whether a ride being added is one somebody is there for: a curated headliner, or a ride the
 * visitor named in {@link OptimizeInput.priority}.
 *
 * The tier and {@link rankHeadliners} have to agree on this, or the tier drops a ride the ranking
 * put first. See park.fan's docs/rules/a-day-that-does-not-fit-opens-an-assistant-not-a-footnote.md.
 */
function isWanted(
  ride: PlanDayRide,
  priority: readonly string[] | undefined,
): boolean {
  return (
    Boolean(ride.isHeadliner) ||
    Boolean(priority?.includes(ride.attractionSlug))
  );
}

/**
 * How much each ride being added costs to lose, hardest first.
 *
 * The visitor's own order in `priority` wins wherever it says anything; otherwise the ride's own
 * expected queue ranks it, the only figure in the payload that measures how much of a draw a ride
 * is. Ties go to the slug, so every run ranks the same way. Ranks rather than minutes, because the
 * gaps between the figures sit inside the model's own error, and a rank bounded by
 * {@link MAX_STOPS} packs into {@link overflowKey}.
 */
function rankHeadliners(
  add: readonly PlanDayRide[],
  priority: readonly string[] | undefined,
): number[] {
  const weights = add.map(() => 0);
  const heads = add
    .map((ride, index) => ({ ride, index }))
    .filter(({ ride }) => isWanted(ride, priority));
  if (heads.length === 0) return weights;

  // Per entry, not per slug: the n-th time a slug appears is matched to its n-th place in
  // `priority`, and a lap ranks behind every first ride, named or not. `LAPS` clears the largest
  // first-ride rank. See
  // park.fan's docs/rules/the-planners-day-ends-when-the-park-closes-and-a-headliner-is.md.
  const places = new Map<string, number[]>();
  priority?.forEach((slug, index) =>
    places.set(slug, [...(places.get(slug) ?? []), index]),
  );
  const seen = new Map<string, number>();
  const unnamed = (priority?.length ?? 0) + heads.length;
  const LAPS = unnamed + 1;
  const ranked = heads.map(({ ride, index }) => {
    const lap = seen.get(ride.attractionSlug) ?? 0;
    seen.set(ride.attractionSlug, lap + 1);
    const said = places.get(ride.attractionSlug)?.[lap] ?? -1;
    const rank = (lap > 0 ? LAPS : 0) + (said >= 0 ? said : unnamed);
    return { ride, index, rank };
  });
  ranked.sort(
    (a, b) =>
      a.rank - b.rank ||
      (b.ride.dayPeak ?? 0) - (a.ride.dayPeak ?? 0) ||
      a.ride.attractionSlug.localeCompare(b.ride.attractionSlug, "en") ||
      a.index - b.index,
  );
  ranked.forEach(({ index }, position) => {
    weights[index] = ranked.length - position;
  });
  return weights;
}

/** A minute range the optimiser may not schedule over. */
interface FixedBlock {
  from: number;
  to: number;
  /**
   * Where the block is, when it is a show with coordinates. A ride filed before
   * it has to walk there and one filed after it has to walk from it, so the
   * block is wider than its minutes by that walk. `null` on everything else,
   * which keeps exactly the width it always had.
   */
  place: LegPlace | null;
  /** A ticked-off or running ride, which {@link fixedPads} leaves unpadded. */
  ride: boolean;
}

/**
 * How far a ride's own minutes have to stay from a fixed block on each side. A block with no place
 * has no walk, but a ride filed before it still pays for leaving the ride. The ceiling is what the
 * search builds against and the floor what {@link isExecutable} judges against, the same split
 * `leg.ts` draws, so a day the search files is never one the grid calls broken.
 */
function fixedPads(
  ride: LegPlace | null,
  block: FixedBlock,
  bound: "floor" | "ceiling",
): { before: number; after: number } {
  if (block.ride) return { before: 0, after: 0 };
  const key = bound === "floor" ? "floorMinutes" : "ceilingMinutes";
  return {
    before: transferBetween(ride, block.place, null, { toBlock: true })[key],
    after: transferBetween(block.place, ride, null, { fromBlock: true })[key],
  };
}

interface Candidate {
  entryId: string | null;
  slug: string;
  name: string;
  ride: PlanDayRide | null;
  /** Where a block for this ride may be filed — `rideFloor().softMin`. */
  floorMin: number;
  /** A curated headliner — see {@link OVERFLOW_STRIDE} for what hangs on it. */
  headliner: boolean;
  /**
   * What it costs to leave this one out: non-zero only on a wanted ride being added (see
   * {@link isWanted}), from `1`, the easiest to lose, upwards. See {@link rankHeadliners}.
   */
  dropWeight: number;
  /** Expected wait per park-local quarter hour, `null` where the day has no figure. */
  waitBySlot: (number | null)[];
  /**
   * What the stop occupies at that quarter hour: the expected wait, band excluded, so the day is
   * not paced off the pessimistic end of every queue. The block is still drawn a band taller.
   */
  occupiedBySlot: number[];
}

interface Context {
  grid: DayGrid;
  candidates: Candidate[];
  fixed: FixedBlock[];
  /** `transfer[i][j]` — minutes from candidate i to candidate j, ceiling. */
  transfer: number[][];
  /** Rides {@link MAX_STOPS} had no room for. Reported, never swallowed. */
  capped: number;
  /**
   * Placement fronts, keyed by candidate and earliest start. A front depends on nothing else, and
   * the local search re-schedules the same orders thousands of times. Per call, so it cannot go
   * stale.
   */
  fronts: Map<number, Placement[]>;
}

/**
 * The cache key is `candidate * this + first`, injective while `first` stays under it. A `first`
 * beyond it skips the cache rather than sharing a key with another ride.
 */
const FRONT_KEY_STRIDE = 1 << 16;

function snapUp(minute: number, step: number): number {
  return Math.ceil(minute / step) * step;
}

/**
 * How far past midnight the tables below reach. `unfoldedCloseHour` lifts a close past midnight (a
 * park open 16:00–01:00 closes in hour 25) and a stop that does not fit is parked later still; 36
 * leaves room for both.
 */
const TABLE_HOURS = 36;

/** The table's rows: one per quarter hour, the width of a `/plan/day` slot and of the snap. */
const TABLE_SLOTS = (TABLE_HOURS * 60) / SLOT_MIN;

/** The quarter hour a minute falls in, as this file's tables are indexed. */
function slotIndex(minute: number): number {
  return Math.min(Math.max(Math.floor(minute / SLOT_MIN), 0), TABLE_SLOTS - 1);
}

/**
 * Every quarter hour's answer, once, before the search starts, from the app's own `estimateFor` so
 * the minutes the optimiser reckons with are the minutes the block draws. Probed per quarter hour
 * because starts snap to {@link SNAP_MIN_FINE} and the API may answer per quarter hour; an hourly
 * day answers the same figure four times and plans exactly as it did.
 *
 * `occupiedBySlot` is the planned length, not the drawn one (see `plannedMinutes`). The table runs
 * to {@link TABLE_HOURS} rather than 24 because `estimateFor` normalises an hour past midnight
 * itself, and clamping at 23 priced a stop at 00:15 with the 23:00 figure.
 */
function tabulate(
  day: PlanDay,
  slug: string,
): Pick<Candidate, "waitBySlot" | "occupiedBySlot"> {
  const waitBySlot: (number | null)[] = [];
  const occupiedBySlot: number[] = [];
  for (let slot = 0; slot < TABLE_SLOTS; slot++) {
    const probe: PlannerEntry = {
      id: "",
      attractionSlug: slug,
      startMinute: slot * SLOT_MIN,
    };
    waitBySlot.push(estimateFor(day, probe).wait);
    occupiedBySlot.push(Math.max(plannedMinutes(day, probe), SNAP_MIN_FINE));
  }
  return { waitBySlot, occupiedBySlot };
}

/**
 * Push a start past every fixed block it would run into.
 *
 * The span is a function of the minute because a push lands in another hour, where the same ride
 * can take longer and reach the next block. So it is re-read every pass and the loop ends only on
 * a pass that moved nothing. It terminates because `at` only moves forward, past a block it
 * overlapped.
 */
function clearFixed(
  start: number,
  spanAt: (minute: number) => number,
  fixed: readonly FixedBlock[],
  ride: LegPlace | null,
): number {
  let at = start;
  for (let pass = 0; pass < fixed.length + 1; pass++) {
    const span = spanAt(at);
    let moved = false;
    for (const block of fixed) {
      const { before, after } = fixedPads(ride, block, "ceiling");
      if (at < block.to + after && at + span + before > block.from) {
        at = snapUp(block.to + after, SNAP_MIN_FINE);
        moved = true;
      }
    }
    if (!moved) break;
  }
  return at;
}

interface Placement {
  startMinute: number;
  waitMinutes: number | null;
  freeAt: number;
  /**
   * Whether this stop happens at all, decided on the start and never on the end: a queue may be
   * joined right up to closing time. See
   * park.fan's docs/rules/the-planners-day-ends-when-the-park-closes-and-a-headliner-is.md.
   */
  fits: boolean;
}

/**
 * Every placement of one ride worth considering, given when the visitor is free: among the starts
 * from the earliest feasible one up to {@link MAX_DELAY_MIN} later, the ones not beaten on both
 * cost and the clock. Sorted by the clock, so the first entry always goes now.
 *
 * A single "free soonest" winner would let the tie-break outrank cost inside the scheduler, where
 * neither the search nor the brute-force test can see it.
 */
function placementsFrom(
  ctx: Context,
  candidate: Candidate,
  first: number,
): Placement[] {
  const { grid } = ctx;
  const spanAt = (minute: number) =>
    candidate.occupiedBySlot[slotIndex(minute)] ?? SNAP_MIN_FINE;

  const options: Placement[] = [];
  for (let delay = 0; delay <= MAX_DELAY_MIN; delay += SNAP_MIN_FINE) {
    const raw = first + delay;
    // Waiting into a closed park is never better, so the delay loop stops at the gate.
    if (raw >= grid.closeMin) break;
    const start = clearFixed(raw, spanAt, ctx.fixed, candidate.ride);
    if (start >= grid.closeMin) break;
    // Read off the minute `clearFixed` chose, which may be in another slot than `raw`.
    const slot = slotIndex(start);
    const freeAt = start + spanAt(start);
    options.push({
      startMinute: start,
      waitMinutes: candidate.waitBySlot[slot] ?? null,
      freeAt,
      // True by construction (both `break`s above test it), written out because it is the rule.
      fits: start < grid.closeMin,
    });
  }

  if (options.length === 0) {
    // No minute before closing is left to join a queue at. The stop is still placed, where the
    // sequence would put it past the gate, because a silently dropped ride is worse than a visible
    // overflow; `growGridForSpans` widens the canvas and those minutes are hatched. It clears the
    // fixed blocks like every other option, or an overflowing ride lies across a dinner and
    // `isExecutable` rejects the day on every press.
    const spanOutside = (minute: number) =>
      candidate.occupiedBySlot[slotIndex(minute)] ?? SNAP_MIN_FINE;
    const start = clearFixed(
      Math.max(first, grid.closeMin),
      spanOutside,
      ctx.fixed,
      candidate.ride,
    );
    const slot = slotIndex(start);
    return [
      {
        startMinute: start,
        // No figure out here, and that is `estimateFor`'s own answer: a queue
        // joined after closing is `outside-hours`, not a wait of zero.
        waitMinutes: candidate.waitBySlot[slot] ?? null,
        freeAt: start + (candidate.occupiedBySlot[slot] ?? SNAP_MIN_FINE),
        fits: false,
      },
    ];
  }

  // A delay is judged on the queue plus its own idle at IDLE_WEIGHT, measured from `first`, which
  // is common to every option and cancels; the callers add the true idle. Ranking on the bare queue
  // would keep an option a minute cheaper and half an hour later.
  const costOf = (option: Placement) =>
    planCost(waitCost(option), option.startMinute - first);
  options.sort(
    (a, b) =>
      a.freeAt - b.freeAt ||
      costOf(a) - costOf(b) ||
      a.startMinute - b.startMinute,
  );
  const front: Placement[] = [];
  let cheapest = Infinity;
  for (const option of options) {
    const cost = costOf(option);
    if (cost >= cheapest) continue;
    cheapest = cost;
    front.push(option);
  }
  return front;
}

/**
 * What a stop costs the queue total, which is what `null` has to mean here.
 *
 * `scheduleOrder` adds nothing for a stop with no figure, so the front has to
 * rank it the same way or the two would disagree about which option is cheaper.
 */
function waitCost(placement: Placement): number {
  return placement.waitMinutes ?? 0;
}

/**
 * The same, for a stop that may not happen at all: a queue nobody joins costs nobody anything.
 * Counting it made two plans that give up different rides cost the same, so the choice fell
 * through to the clock. The idle in front of such a stop goes the same way.
 */
function waitOf(placement: Placement): number {
  return placement.fits ? (placement.waitMinutes ?? 0) : 0;
}

/**
 * The earliest minute a stop could be filed at, walk and opening included.
 *
 * One function because two things need the same answer and must not drift: the
 * placement front starts here, and {@link idleFor} measures the standing about
 * a plan CHOSE against it.
 */
function earliestStart(
  ctx: Context,
  index: number,
  freeBefore: number | null,
  transferMinutes: number,
): number {
  const candidate = ctx.candidates[index];
  // `dayStartMin`: on an early-entry day a headliner's floor sits below the
  // park's opening, and the floor decides. Without early entry it is `openMin`.
  const earliest =
    freeBefore === null ? dayStartMin(ctx.grid) : freeBefore + transferMinutes;
  return snapUp(Math.max(earliest, candidate.floorMin), SNAP_MIN_FINE);
}

/**
 * The standing about one stop adds, the part of it this file decided on. Two things are left out.
 *
 * The way in: the first stop is charged from {@link earliestStart}, not from opening, so the walk
 * from the gates is free but choosing to wait past it is not; otherwise idling the morning costs
 * nothing and a filler moves behind the ride it should precede.
 *
 * The quarter-hour grid: the base is the arrival snapped up, because charging the rounding favours
 * routes that happen to arrive on the quarter hour, such as crossing the park between every ride.
 */
function idleFor(
  ctx: Context,
  index: number,
  freeBefore: number | null,
  transferMinutes: number,
  startMinute: number,
): number {
  const base =
    freeBefore === null
      ? earliestStart(ctx, index, null, 0)
      : snapUp(freeBefore + transferMinutes, SNAP_MIN_FINE);
  return Math.max(0, startMinute - base);
}

/** {@link placementsFrom}, addressed by the state the search is actually in. */
function placementsFor(
  ctx: Context,
  index: number,
  freeBefore: number | null,
  transferMinutes: number,
): Placement[] {
  const candidate = ctx.candidates[index];
  const first = earliestStart(ctx, index, freeBefore, transferMinutes);

  if (first >= FRONT_KEY_STRIDE) return placementsFrom(ctx, candidate, first);

  const key = index * FRONT_KEY_STRIDE + first;
  const cached = ctx.fronts.get(key);
  if (cached) return cached;
  const front = placementsFrom(ctx, candidate, first);
  ctx.fronts.set(key, front);
  return front;
}

interface Scored extends OverflowCounts {
  stops: OptimizeStop[];
  totalWaitMinutes: number;
  idleMinutes: number;
  endMinute: number;
}

/**
 * How many part-built schedules the fixed-order pass carries between stops, as many as
 * {@link LABELS_PER_KEY}. A beam and not an exact DP because a delay is bounded relative to when
 * the visitor gets free, so arriving earlier does not strictly dominate arriving later.
 */
const SCHEDULE_STATES = 4;

/** One order, part-scheduled: the delays chosen so far and what they cost. */
interface PartialSchedule extends OverflowCounts {
  stops: OptimizeStop[];
  totalWaitMinutes: number;
  idleMinutes: number;
  /** When the visitor is free after the last stop. `null` before the first. */
  freeAt: number | null;
  /**
   * When the last queue that actually happens is left; zero before the first stop that fits. Not
   * `freeAt`, which includes stops parked past the gate that `optimizeDay` drops from its plan.
   */
  endMinute: number;
}

function comparePartials(a: PartialSchedule, b: PartialSchedule): number {
  const aOver = overflowKey(a);
  const bOver = overflowKey(b);
  if (aOver !== bOver) return aOver - bOver;
  const aCost = planCost(a.totalWaitMinutes, a.idleMinutes);
  const bCost = planCost(b.totalWaitMinutes, b.idleMinutes);
  if (aCost !== bCost) return aCost - bCost;
  const aFree = a.freeAt ?? 0;
  const bFree = b.freeAt ?? 0;
  if (aFree !== bFree) return aFree - bFree;
  // Two schedules that cost the same and end at the same minute are still two
  // different plans, and which of them is printed may not depend on the order
  // the states happened to be built in.
  return (
    (a.stops[a.stops.length - 1]?.startMinute ?? 0) -
    (b.stops[b.stops.length - 1]?.startMinute ?? 0)
  );
}

function partialDominates(a: PartialSchedule, b: PartialSchedule): boolean {
  return schedulingDominates(
    {
      overflow: overflowKey(a),
      cost: planCost(a.totalWaitMinutes, a.idleMinutes),
      freeAt: a.freeAt ?? 0,
    },
    {
      overflow: overflowKey(b),
      cost: planCost(b.totalWaitMinutes, b.idleMinutes),
      freeAt: b.freeAt ?? 0,
    },
  );
}

/** What dominance is decided on, whichever of the two searches is asking. */
interface Frontier {
  overflow: number;
  cost: number;
  freeAt: number;
}

/**
 * Whether `a` can be kept and `b` thrown away, on overflow, cost and the clock.
 *
 * Cost is not comparable across two clocks: a state free earlier that then schedules the rest where
 * `b` did stands about for the difference. So the cost is compared with that charge netted out,
 * `cost − IDLE_WEIGHT × freeAt`; raw costs would discard a state behind on the clock and ahead on
 * the idle it is about to be excused.
 */
function schedulingDominates(a: Frontier, b: Frontier): boolean {
  const aNet = a.cost - IDLE_WEIGHT * a.freeAt;
  const bNet = b.cost - IDLE_WEIGHT * b.freeAt;
  return (
    a.overflow <= b.overflow &&
    aNet <= bNet &&
    a.freeAt <= b.freeAt &&
    (a.overflow < b.overflow || aNet < bNet || a.freeAt < b.freeAt)
  );
}

/**
 * The clock, the queue total and the overflow that one order produces.
 *
 * A forward pass over the part-built schedules nothing beats outright, because each stop offers a
 * front of delays and the cheapest completion is not made of the cheapest steps. The winner is
 * picked under {@link better}, so the scheduler has no objective of its own.
 */
function scheduleOrder(ctx: Context, order: readonly number[]): Scored {
  let states: PartialSchedule[] = [
    {
      stops: [],
      totalWaitMinutes: 0,
      idleMinutes: 0,
      ...NO_OVERFLOW,
      freeAt: null,
      endMinute: 0,
    },
  ];
  let previous = -1;

  for (const index of order) {
    const candidate = ctx.candidates[index];
    const transfer = previous < 0 ? 0 : ctx.transfer[previous][index];
    const grown: PartialSchedule[] = [];
    for (const state of states) {
      for (const placement of placementsFor(
        ctx,
        index,
        state.freeAt,
        transfer,
      )) {
        const tier = overflowTier(placement.fits, candidate);
        grown.push({
          stops: [
            ...state.stops,
            {
              entryId: candidate.entryId,
              attractionSlug: candidate.slug,
              attractionName: candidate.name,
              startMinute: placement.startMinute,
              waitMinutes: placement.waitMinutes,
              fits: placement.fits,
            },
          ],
          totalWaitMinutes: state.totalWaitMinutes + waitOf(placement),
          idleMinutes:
            state.idleMinutes +
            (placement.fits
              ? idleFor(
                  ctx,
                  index,
                  state.freeAt,
                  transfer,
                  placement.startMinute,
                )
              : 0),
          overflow: state.overflow + tier.overflow,
          overflowEntries: state.overflowEntries + tier.overflowEntries,
          overflowHeadliners:
            state.overflowHeadliners + tier.overflowHeadliners,
          dropWeight: state.dropWeight + tier.dropWeight,
          freeAt: placement.freeAt,
          endMinute: placement.fits
            ? Math.max(state.endMinute, placement.freeAt + RIDE_DURATION_MIN)
            : state.endMinute,
        });
      }
    }

    // Sorted first, so a state can only ever be dropped by one already kept —
    // nothing later in this order can dominate anything earlier in it.
    grown.sort(comparePartials);
    const kept: PartialSchedule[] = [];
    for (const state of grown) {
      if (kept.some((other) => partialDominates(other, state))) continue;
      kept.push(state);
      if (kept.length === SCHEDULE_STATES) break;
    }
    states = kept;
    previous = index;
  }

  const best = states[0];
  if (!best) {
    return {
      stops: [],
      totalWaitMinutes: 0,
      idleMinutes: 0,
      endMinute: ctx.grid.openMin,
      ...NO_OVERFLOW,
    };
  }
  return {
    stops: best.stops,
    totalWaitMinutes: best.totalWaitMinutes,
    idleMinutes: best.idleMinutes,
    endMinute: best.endMinute || ctx.grid.openMin,
    overflow: best.overflow,
    overflowEntries: best.overflowEntries,
    overflowHeadliners: best.overflowHeadliners,
    dropWeight: best.dropWeight,
  };
}

/** Fits first, then {@link planCost}, then the clock to settle a tie. */
function better(a: Scored, b: Scored): boolean {
  const aOver = overflowKey(a);
  const bOver = overflowKey(b);
  if (aOver !== bOver) return aOver < bOver;
  const aCost = planCost(a.totalWaitMinutes, a.idleMinutes);
  const bCost = planCost(b.totalWaitMinutes, b.idleMinutes);
  if (aCost !== bCost) return aCost < bCost;
  return a.endMinute < b.endMinute;
}

interface Label extends OverflowCounts {
  placed: number;
  last: number;
  order: number[];
  freeAt: number;
  totalWait: number;
  totalIdle: number;
}

function dominates(a: Label, b: Label): boolean {
  return schedulingDominates(
    {
      overflow: overflowKey(a),
      cost: planCost(a.totalWait, a.totalIdle),
      freeAt: a.freeAt,
    },
    {
      overflow: overflowKey(b),
      cost: planCost(b.totalWait, b.totalIdle),
      freeAt: b.freeAt,
    },
  );
}

/**
 * The order search: a beam over prefixes with a small Pareto front per (visited set, last ride).
 * Deterministic down to the candidate index, so pressing twice cannot give two plans.
 */
function search(ctx: Context, beamWidth: number): number[] {
  const n = ctx.candidates.length;
  if (n === 0) return [];

  let beam: Label[] = [
    {
      placed: 0,
      last: -1,
      order: [],
      freeAt: 0,
      totalWait: 0,
      totalIdle: 0,
      ...NO_OVERFLOW,
    },
  ];

  for (let step = 0; step < n; step++) {
    const grown: Label[] = [];
    for (const label of beam) {
      for (let index = 0; index < n; index++) {
        if (label.placed & (1 << index)) continue;
        const transfer = label.last < 0 ? 0 : ctx.transfer[label.last][index];
        // Every worthwhile delay for this ride is a branch of its own, so the
        // beam chooses when to queue as well as in which order.
        const freeBefore = label.last < 0 ? null : label.freeAt;
        for (const placement of placementsFor(
          ctx,
          index,
          freeBefore,
          transfer,
        )) {
          const tier = overflowTier(placement.fits, ctx.candidates[index]);
          grown.push({
            placed: label.placed | (1 << index),
            last: index,
            order: [...label.order, index],
            freeAt: placement.freeAt,
            totalWait: label.totalWait + waitOf(placement),
            totalIdle:
              label.totalIdle +
              (placement.fits
                ? idleFor(
                    ctx,
                    index,
                    freeBefore,
                    transfer,
                    placement.startMinute,
                  )
                : 0),
            overflow: label.overflow + tier.overflow,
            overflowEntries: label.overflowEntries + tier.overflowEntries,
            overflowHeadliners:
              label.overflowHeadliners + tier.overflowHeadliners,
            dropWeight: label.dropWeight + tier.dropWeight,
          });
        }
      }
    }

    // Pareto front per state, capped — see LABELS_PER_KEY.
    const byKey = new Map<number, Label[]>();
    for (const label of grown) {
      const key = label.placed * 64 + (label.last + 1);
      const kept = byKey.get(key);
      if (!kept) {
        byKey.set(key, [label]);
        continue;
      }
      if (kept.some((other) => dominates(other, label))) continue;
      const survivors = kept.filter((other) => !dominates(label, other));
      survivors.push(label);
      survivors.sort(compareLabels);
      byKey.set(key, survivors.slice(0, LABELS_PER_KEY));
    }

    beam = [...byKey.values()].flat().sort(compareLabels).slice(0, beamWidth);
  }

  return beam[0]?.order ?? [];
}

function compareLabels(a: Label, b: Label): number {
  // The same order `better` uses, tiers included: a beam that prunes toward one objective while
  // the winner is picked by another throws the winner away.
  const aOver = overflowKey(a);
  const bOver = overflowKey(b);
  if (aOver !== bOver) return aOver - bOver;
  const aCost = planCost(a.totalWait, a.totalIdle);
  const bCost = planCost(b.totalWait, b.totalIdle);
  if (aCost !== bCost) return aCost - bCost;
  if (a.freeAt !== b.freeAt) return a.freeAt - b.freeAt;
  // The last tie-break is the order itself, so two runs cannot disagree.
  for (let i = 0; i < Math.min(a.order.length, b.order.length); i++) {
    if (a.order[i] !== b.order[i]) return a.order[i] - b.order[i];
  }
  return a.order.length - b.order.length;
}

/**
 * Or-opt, exchange and 2-opt until nothing improves. Only a strict improvement under
 * {@link better} is accepted and the sweep order is fixed, so it terminates, at the same place
 * every time.
 *
 * The exchange is there for a day that cannot hold every ride: the tail of the order is what falls
 * out, so changing which ride is dropped needs two stops to trade places at once.
 */
function improve(ctx: Context, order: readonly number[]): number[] {
  let best = [...order];
  let bestScore = scheduleOrder(ctx, best);

  for (let pass = 0; pass < MAX_IMPROVE_PASSES; pass++) {
    let moved = false;

    // Or-opt: take one stop out and put it back somewhere else.
    for (let from = 0; from < best.length && !moved; from++) {
      for (let to = 0; to < best.length && !moved; to++) {
        if (from === to) continue;
        const next = [...best];
        const [stop] = next.splice(from, 1);
        next.splice(to, 0, stop);
        const score = scheduleOrder(ctx, next);
        if (better(score, bestScore)) {
          best = next;
          bestScore = score;
          moved = true;
        }
      }
    }

    // Exchange: two stops trade places, which decides which ride falls out of a full day.
    for (let i = 0; i < best.length - 1 && !moved; i++) {
      for (let j = i + 1; j < best.length && !moved; j++) {
        const next = [...best];
        next[i] = best[j];
        next[j] = best[i];
        const score = scheduleOrder(ctx, next);
        if (better(score, bestScore)) {
          best = next;
          bestScore = score;
          moved = true;
        }
      }
    }

    // 2-opt: reverse a run, which is what untangles a route that crosses itself.
    for (let i = 0; i < best.length - 1 && !moved; i++) {
      for (let j = i + 1; j < best.length && !moved; j++) {
        const next = [
          ...best.slice(0, i),
          ...best.slice(i, j + 1).reverse(),
          ...best.slice(j + 1),
        ];
        const score = scheduleOrder(ctx, next);
        if (better(score, bestScore)) {
          best = next;
          bestScore = score;
          moved = true;
        }
      }
    }

    if (!moved) break;
  }

  return best;
}

/** Everything the search needs, precomputed once. */
function buildContext(input: OptimizeInput): Context | null {
  const { day, grid, entries, add = [], clock } = input;
  if (!canOptimize(day, grid) || !day) return null;
  // A walked day is a record, not a plan. Refused here so every caller answers the same way.
  if (clock?.phase === "past") return null;

  // Fixed: a ticked-off entry happened, a free block is a decision, and a slot
  // the clock has reached is being lived through — see `hasStarted`.
  const fixed: FixedBlock[] = entries
    .filter((entry) => entry.done || entry.custom || hasStarted(entry, clock))
    .map((entry) => ({
      from: entry.startMinute,
      to:
        entry.startMinute + Math.max(plannedMinutes(day, entry), SNAP_MIN_FINE),
      place: entry.showSlug ? entryPlace(day, entry) : null,
      ride: !isBlockEntry(entry),
    }))
    .sort((a, b) => a.from - b.from);

  // Filtered BEFORE the `MAX_STOPS` split below, deliberately: applied after
  // it, `capped` would report rides that were never going to be planned, and
  // `addKept` would refuse headliners the remaining minutes had room for.
  const movable = movableEntries(entries, clock, day);

  const rideOf = (slug: string) =>
    day.rides.find((r) => r.attractionSlug === slug) ?? null;

  /**
   * The {@link MAX_STOPS} budget, split before the cut so `capped` can report it. Existing entries
   * are served first: a movable entry left out is not removed by `applyPlan` and would keep its old
   * minute under the new plan, while a headliner left out is merely not added.
   */
  const movableKept = movable.slice(0, MAX_STOPS);
  const addKept = add.slice(0, MAX_STOPS - movableKept.length);
  const capped =
    movable.length - movableKept.length + (add.length - addKept.length);

  const weights = rankHeadliners(addKept, input.priority);

  const candidates: Candidate[] = [
    ...movableKept.map((entry) => {
      const slug = entry.attractionSlug as string;
      const ride = rideOf(slug);
      return {
        entryId: entry.id,
        slug,
        name: entry.attractionName ?? ride?.attractionName ?? slug,
        ride,
        floorMin: rideFloor(grid, ride, clock).softMin,
        headliner: Boolean(ride?.isHeadliner),
        // An entry the visitor already had is its own tier and is never
        // dropped, so there is nothing here for a weight to decide.
        dropWeight: 0,
        ...tabulate(day, slug),
      };
    }),
    ...addKept.map((ride, index) => ({
      entryId: null,
      slug: ride.attractionSlug,
      name: ride.attractionName,
      ride,
      floorMin: rideFloor(grid, ride, clock).softMin,
      headliner: isWanted(ride, input.priority),
      dropWeight: weights[index] ?? 0,
      ...tabulate(day, ride.attractionSlug),
    })),
  ];

  // One candidate is still a question: it has a floor, fixed blocks to clear and a delay to weigh.
  if (candidates.length === 0) return null;

  const transfer = candidates.map((from) =>
    candidates.map((to) => transferBetween(from.ride, to.ride).ceilingMinutes),
  );

  return { grid, candidates, fixed, transfer, capped, fronts: new Map() };
}

/**
 * How many rounds the peel below may run. Each is a whole extra search, so this is a time budget
 * too; a round beyond the first is only needed where setting one ride aside makes another
 * homeless.
 */
const MAX_PEEL_ROUNDS = 4;

/**
 * The plan, with the rides that cannot fit chosen rather than left over.
 *
 * The beam cannot choose them: overflow only appears on the last stop, so every prefix scores
 * `overflow: 0`, is ranked on cost alone, and the prefix that would keep the expensive ride is
 * pruned early. So each round sets aside the least important added rides by
 * {@link Candidate.dropWeight}, re-runs the search, and scores the result in the original context
 * with them appended, so {@link better} decides between rounds on its usual terms. On a day that
 * holds everything it runs the search once. See
 * park.fan's docs/rules/the-planners-day-ends-when-the-park-closes-and-a-headliner-is.md.
 */
function peeled(input: OptimizeInput, ctx: Context): Scored {
  let best = scheduleOrder(ctx, improve(ctx, search(ctx, BEAM_WIDTH)));
  const add = input.add ?? [];
  if (best.overflowHeadliners === 0) return best;

  // Least important first. By index rather than by slug, so a ride planned three times is not set
  // aside all at once.
  const weights = rankHeadliners(add, input.priority);
  const givable = add
    .map((ride, index) => ({ ride, index, weight: weights[index] ?? 0 }))
    .filter(({ weight }) => weight > 0)
    .sort(
      (a, b) =>
        a.weight - b.weight ||
        a.ride.attractionSlug.localeCompare(b.ride.attractionSlug, "en") ||
        a.index - b.index,
    );

  // Only rides the full search actually holds: `MAX_STOPS` may have cut some
  // of `add` before it ever reached the context, and setting one of THOSE
  // aside would produce an order that does not cover the candidates. The
  // context takes `add` from the front, so what it holds is a prefix.
  const addInContext = ctx.candidates.filter((c) => c.entryId === null).length;
  const inPlay = givable.filter(({ index }) => index < addInContext);

  const from = Math.min(best.overflowHeadliners, inPlay.length);
  for (let round = 0; round < MAX_PEEL_ROUNDS; round++) {
    const asideCount = from + round;
    if (asideCount >= inPlay.length) break;
    const asideRides = inPlay.slice(0, asideCount).map(({ ride }) => ride);
    const aside = new Set(
      inPlay.slice(0, asideCount).map(({ index }) => index),
    );

    const reduced = buildContext({
      ...input,
      add: add.filter((_, index) => !aside.has(index)),
    });
    if (!reduced) break;
    const plan = scheduleOrder(
      reduced,
      improve(reduced, search(reduced, BEAM_WIDTH)),
    );

    // Scored in the full context with the set-aside rides on the end, so the plan knows what it
    // gave up and two rounds compare.
    const full = orderIn(ctx, [
      ...plan.stops.map((stop) => stop.attractionSlug),
      ...asideRides.map((ride) => ride.attractionSlug),
    ]);
    if (!full) break;
    const candidate = scheduleOrder(ctx, full);
    if (better(candidate, best)) best = candidate;
    // Everything that was kept found a minute, so peeling further can only give
    // up a ride the day had room for.
    if (plan.overflowHeadliners === 0) break;
  }

  return best;
}

/**
 * A list of slugs as candidate indices in `ctx`, or `null` where one is missing.
 *
 * Repeats matter — a ride may legitimately appear twice in one day — so each
 * slug takes the first candidate index not already spoken for.
 */
function orderIn(ctx: Context, slugs: readonly string[]): number[] | null {
  const used = new Set<number>();
  const order: number[] = [];
  for (const slug of slugs) {
    const index = ctx.candidates.findIndex(
      (c, i) => c.slug === slug && !used.has(i),
    );
    if (index < 0) return null;
    used.add(index);
    order.push(index);
  }
  return order.length === ctx.candidates.length ? order : null;
}

/**
 * Orders the day for the least queueing, or `null` where there is nothing to order: no day, no
 * axis, no readable wait times, no ride to arrange, or a day already as good as this can make it.
 */
export function optimizeDay(input: OptimizeInput): OptimizedPlan | null {
  const ctx = buildContext(input);
  if (!ctx) return null;
  const add = input.add ?? [];
  const entries = input.entries;

  const scored = peeled(input, ctx);

  /**
   * What the new plan has to beat: the day as it stands, and only where it is executable. Scoring
   * the current sequence would call a day with two-hour holes optimal; scoring an impossible day
   * would beat every real schedule. After one press the day is the schedule, so a second press is a
   * no-op. Not on a press that adds rides, which has no incumbent.
   */
  let changed = true;
  if (add.length === 0 && isExecutable(input, ctx)) {
    // Over the same rides the search saw: past `MAX_STOPS` the incumbent would carry extra rides,
    // score worse than any plan, and every press would reshuffle an unchanged day.
    const seen = new Set(ctx.candidates.map((candidate) => candidate.entryId));
    const current = scoreCurrent({
      ...input,
      entries: entries.filter(
        (entry) => !entry.attractionSlug || seen.has(entry.id),
      ),
    });
    if (current && !better(scored, current)) changed = false;
  }

  if (!changed) return null;

  /**
   * A ride the optimiser adds is never filed past the gate: one that does not fit is not added, but
   * still counted in `overflow`. An entry the visitor already has keeps its overflow block, because
   * dropping it would delete their own plan.
   */
  const stops = scored.stops.filter(
    (stop) => stop.fits || stop.entryId !== null,
  );

  return {
    stops,
    totalWaitMinutes: scored.totalWaitMinutes,
    idleMinutes: scored.idleMinutes,
    endMinute: scored.endMinute,
    overflow: scored.overflow,
    capped: ctx.capped,
    changed: true,
  };
}

/**
 * Whether the day as it stands could actually be walked.
 *
 * Each gap has to cover the transfer's floor, the one verdict `leg.ts` calls certifiable, so a day
 * this rejects is impossible rather than uncomfortable; a block over a fixed one fails too. A block
 * ends at `plannedMinutes`, the span the search files: judged against a longer span, every plan
 * the search makes would read as overlapping and the button would reshuffle on every press.
 */
function isExecutable(input: OptimizeInput, ctx: Context): boolean {
  const { day } = input;
  if (!day) return false;

  // The same set the search works on. The bare filter reaches entries also in `ctx.fixed` (a block
  // overlapping itself), and skipping the `MAX_STOPS` cut reaches entries that keep their old
  // minute under the new blocks; either way no day would ever read as executable.
  const seen = new Set(ctx.candidates.map((candidate) => candidate.entryId));
  const stops = movableEntries(input.entries, input.clock, day)
    .filter((entry) => seen.has(entry.id))
    .sort((a, b) => a.startMinute - b.startMinute);

  for (let i = 0; i < stops.length; i++) {
    const entry = stops[i];
    const span = Math.max(plannedMinutes(day, entry), SNAP_MIN_FINE);
    const there = ctx.candidates.find((c) => c.entryId === entry.id);
    for (const block of ctx.fixed) {
      const { before, after } = fixedPads(there?.ride ?? null, block, "floor");
      if (
        entry.startMinute < block.to + after &&
        entry.startMinute + span + before > block.from
      ) {
        return false;
      }
    }
    if (i === 0) continue;
    const previous = stops[i - 1];
    const from = ctx.candidates.find((c) => c.entryId === previous.id);
    const to = there;
    const occupied = Math.max(plannedMinutes(day, previous), SNAP_MIN_FINE);
    const floor = transferBetween(
      from?.ride ?? null,
      to?.ride ?? null,
    ).floorMinutes;
    if (entry.startMinute - (previous.startMinute + occupied) < floor)
      return false;
  }

  return true;
}

/**
 * What one named sequence would cost, scheduled by the same rules. The brute force in
 * `optimize.vectors.spec.ts` scores every permutation through it, so the search is never
 * checked against itself.
 *
 * The slugs must be the movable set; a slug that is not a candidate answers `null`.
 */
export function scoreOrder(
  input: OptimizeInput,
  slugs: readonly string[],
): {
  totalWaitMinutes: number;
  idleMinutes: number;
  endMinute: number;
  overflow: number;
  overflowKey: number;
} | null {
  const ctx = buildContext(input);
  if (!ctx) return null;
  const order: number[] = [];
  for (const slug of slugs) {
    const index = ctx.candidates.findIndex(
      (candidate) =>
        candidate.slug === slug &&
        !order.includes(ctx.candidates.indexOf(candidate)),
    );
    if (index < 0) return null;
    order.push(index);
  }
  const scored = scheduleOrder(ctx, order);
  return {
    totalWaitMinutes: scored.totalWaitMinutes,
    idleMinutes: scored.idleMinutes,
    endMinute: scored.endMinute,
    overflow: scored.overflow,
    // The tier order as one comparable number, so a check can assert WHICH
    // rides a plan gave up and not only how many. `better` reads exactly this.
    overflowKey: overflowKey(scored),
  };
}

/**
 * How many times, in the part of the day still ahead, the next thing starts before the one before
 * it is over and walked away from.
 *
 * Judged the way the grid judges its legs (`legBetween`'s `broken`); a free block counts with its
 * own length and no walk, and a ride with no wait figure is skipped. {@link scoreCurrent} cannot
 * see these, since two things at the same minute cost it nothing extra.
 */
export function clashCount(
  day: PlanDay,
  entries: readonly PlannerEntry[],
  clock?: DayClock,
): number {
  const ahead = entries
    .filter((entry) => !entry.done && !hasStarted(entry, clock))
    .filter((entry) => entry.custom || entry.attractionSlug)
    .sort((a, b) => a.startMinute - b.startMinute);
  const rideOf = (entry: PlannerEntry) => entryPlace(day, entry);
  let clashes = 0;
  for (let index = 1; index < ahead.length; index++) {
    const from = ahead[index - 1];
    const to = ahead[index];
    if (!from.custom && estimateFor(day, from).wait === null) continue;
    const walk = transferBetween(rideOf(from), rideOf(to), null, {
      fromBlock: isBlockEntry(from),
      toBlock: isBlockEntry(to),
    }).floorMinutes;
    if (to.startMinute < from.startMinute + plannedMinutes(day, from) + walk)
      clashes++;
  }
  return clashes;
}

/**
 * The same day scored as it stands, for a before-and-after a caller can print.
 *
 * Through the same scorer, so both numbers come from one function. The idle total is what makes it
 * comparable: on queued minutes alone, three rides at 10:00, 13:00 and 16:00 score as well as the
 * same three back to back. Holes are counted against the same walk and block heights the search
 * uses, so a second press changes nothing.
 */
export function scoreCurrent(input: OptimizeInput): Scored | null {
  const { day, grid, entries, clock } = input;
  if (!canOptimize(day, grid) || !day) return null;
  if (clock?.phase === "past") return null;

  // The clock decides which rides this covers, never where they sit: this scores the blocks where
  // they are. A before-figure over five rides against an after-figure over two is not a saving.
  const movable = movableEntries(entries, clock, day).sort(
    (a, b) => a.startMinute - b.startMinute,
  );
  if (movable.length === 0) return null;

  let totalWaitMinutes = 0;
  let idleMinutes = 0;
  let overflow = 0;
  let endMinute = grid.openMin;
  let freeBefore: number | null = null;
  let previous: PlanDayRide | null = null;
  const stops: OptimizeStop[] = [];

  for (const entry of movable) {
    const estimate = estimateFor(day, entry);
    const span = Math.max(plannedMinutes(day, entry), SNAP_MIN_FINE);
    const freeAt = entry.startMinute + span;
    const ride =
      day.rides.find((r) => r.attractionSlug === entry.attractionSlug) ?? null;
    const transfer =
      previous === null ? 0 : transferBetween(previous, ride).ceilingMinutes;
    // Decided on the start, like everywhere else. See {@link Placement.fits}.
    const fits = entry.startMinute < grid.closeMin;
    if (!fits) overflow++;
    // A queue nobody joins costs nothing on this side too, or the incumbent and the plan measure
    // different days. See {@link waitOf}.
    if (fits && estimate.wait !== null) totalWaitMinutes += estimate.wait;
    // {@link idleFor}'s rule: the arrival snapped up to the grid, and nothing before the first
    // block, since this file does not know when the visitor came through the gates.
    if (fits && freeBefore !== null) {
      idleMinutes += Math.max(
        0,
        entry.startMinute - snapUp(freeBefore + transfer, SNAP_MIN_FINE),
      );
    }
    // The same rule the scheduler uses: a block the visitor dragged into the
    // night is not when their day ends, it is the thing being fixed.
    if (fits) endMinute = Math.max(endMinute, freeAt + RIDE_DURATION_MIN);
    freeBefore = freeAt;
    previous = ride;
    stops.push({
      entryId: entry.id,
      attractionSlug: entry.attractionSlug as string,
      attractionName: entry.attractionName ?? (entry.attractionSlug as string),
      startMinute: entry.startMinute,
      waitMinutes: estimate.wait,
      fits,
    });
  }

  // Every stop here is an entry the visitor placed, so all overflow is first tier and no weight
  // applies.
  return {
    stops,
    totalWaitMinutes,
    idleMinutes,
    endMinute,
    overflow,
    overflowEntries: overflow,
    overflowHeadliners: 0,
    dropWeight: 0,
  };
}
