import type { DayGrid } from "./day-grid";
import {
  movableEntries,
  optimizeDay,
  scoreCurrent,
  type OptimizeStop,
} from "./optimize";
import type {
  DayClock,
  OptimizerDay as PlanDay,
  OptimizerRide as PlanDayRide,
  PlannerBlockIcon,
  PlannerEntry,
} from "./types";

/**
 * What to give up when the day is too short, measured rather than argued.
 *
 * `optimizeDay` cannot answer "what would I have to change for it to fit": it takes the fixed
 * blocks as given and never deletes an entry. So every lever here is the same engine run again with
 * one thing taken away, and what the assistant prints is a difference between two plans.
 *
 * A ticked wish with a payload row is handed to the engine as an addition, so which one falls out
 * is decided by the visitor's own order (`OptimizeInput.priority`). This is the one place an entry
 * may be removed, and only in front of a list that names it. A wish is fitted when it has a stop
 * that `fits`, never derived from the minute. See
 * park.fan's docs/rules/a-day-that-does-not-fit-opens-an-assistant-not-a-footnote.md.
 */

/** One thing the visitor wants on the day — an entry they have, or a ride to add. */
export interface FitWish {
  /** Stable and unique within one list: `e:<entry id>` or `a:<slug>`. */
  key: string;
  /** The entry this stands for, or `null` where it is a ride being added. */
  entryId: string | null;
  attractionSlug: string;
  attractionName: string;
  /**
   * The day's own row for this ride, which is what lets it be re-planned. `null` where the payload
   * has none; such a wish keeps its minute, and can only be left or taken out.
   */
  ride: PlanDayRide | null;
  /** A curated headliner. Decoration here; the engine reads it off `ride`. */
  headliner: boolean;
}

/** A free block the day holds — the one thing in a plan that is not a queue. */
export interface FitBlock {
  entryId: string;
  label: string;
  icon: PlannerBlockIcon;
  durationMinutes: number;
}

/** Everything the fit assistant reasons over for one day. */
export interface FitInput {
  day: PlanDay;
  grid: DayGrid;
  /** The day exactly as it stands, free blocks and ticked-off entries included. */
  entries: readonly PlannerEntry[];
  /** Everything the visitor is deciding about, in the order they were offered. */
  wishes: readonly FitWish[];
  /** The free blocks the levers below may take away or cut short. */
  blocks: readonly FitBlock[];
  clock?: DayClock;
}

/** The visitor's answers. Everything not named here is planned as offered. */
export interface FitChoice {
  /** Wish keys switched OFF — not planned, and removed where they are entries. */
  dropped: ReadonlySet<string>;
  /** Free blocks taken out of the day. */
  droppedBlocks: ReadonlySet<string>;
  /** Free blocks cut to {@link SHORT_BLOCK_MIN}. */
  shortBlocks: ReadonlySet<string>;
  /**
   * Wish keys, most important first. It decides which wish falls out where they cannot all fit,
   * never the order of the day. Keys not listed fall in behind the named ones.
   */
  priority: readonly string[];
}

/** What a plan for one {@link FitChoice} comes to. */
export interface FitOutcome {
  /** Wish keys that get a slot before the park closes, in plan order. */
  fitted: string[];
  /** Ticked wishes that still find no minute before closing. */
  missed: string[];
  /** When the last queue that actually happens is left, park-local minutes. */
  endMinute: number;
  totalWaitMinutes: number;
  idleMinutes: number;
  /** The entries the day would keep — what a caller writes before the stops. */
  entries: PlannerEntry[];
  /** The plan to lay over them. */
  stops: OptimizeStop[];
}

/** What a shortened free block is cut to. Half an hour is still a meal. */
export const SHORT_BLOCK_MIN = 30;

/** Which lever a {@link FitLever} pulls. */
export type FitLeverKind = "drop-block" | "shorten-block" | "drop-all-blocks";

/**
 * One thing the visitor could change, and what it would buy: `fits` and `fitsNow` are counts of the
 * same wish list under the same engine. A lever that buys nothing is not returned.
 */
export interface FitLever {
  kind: FitLeverKind;
  /** The block it acts on. Absent on `drop-all-blocks`. */
  entryId?: string;
  label?: string;
  icon?: PlannerBlockIcon;
  /** What the block would be cut to. `shorten-block` only. */
  minutes?: number;
  /** How many of the ticked wishes fit once it is pulled. */
  fits: number;
  /** How many fit as things stand. */
  fitsNow: number;
  /** How many the visitor is asking for. */
  wanted: number;
  /** Pulling this one alone makes the whole wish list fit. */
  solves: boolean;
}

const ENTRY_PREFIX = "e:";
const ADD_PREFIX = "a:";

/** The wish key for an entry the visitor already has. */
export function entryWishKey(entryId: string): string {
  return `${ENTRY_PREFIX}${entryId}`;
}

/** The wish key for a ride being added. */
export function addWishKey(slug: string): string {
  return `${ADD_PREFIX}${slug}`;
}

/**
 * Everything the visitor is being asked about, in the order it is offered: their own rides first,
 * then the additions, which is also the default priority. Only the movable entries, read through
 * the engine's own `movableEntries`.
 */
export function fitWishes(
  day: PlanDay,
  entries: readonly PlannerEntry[],
  add: readonly PlanDayRide[],
  clock?: DayClock,
): FitWish[] {
  const rideOf = (slug: string) =>
    day.rides.find((ride) => ride.attractionSlug === slug) ?? null;
  const own: FitWish[] = movableEntries(entries, clock, day).map((entry) => {
    const slug = entry.attractionSlug as string;
    const ride = rideOf(slug);
    return {
      key: entryWishKey(entry.id),
      entryId: entry.id,
      attractionSlug: slug,
      attractionName: entry.attractionName ?? ride?.attractionName ?? slug,
      ride,
      headliner: Boolean(ride?.isHeadliner),
    };
  });
  const planned = new Set(own.map((wish) => wish.attractionSlug));
  const extra: FitWish[] = add
    .filter((ride) => !planned.has(ride.attractionSlug))
    .map((ride) => ({
      key: addWishKey(ride.attractionSlug),
      entryId: null,
      attractionSlug: ride.attractionSlug,
      attractionName: ride.attractionName,
      ride,
      headliner: Boolean(ride.isHeadliner),
    }));
  return [...own, ...extra];
}

/**
 * The free blocks a lever may act on. A ticked-off block is a record of an hour that happened, so
 * it is not on offer.
 */
export function fitBlocks(entries: readonly PlannerEntry[]): FitBlock[] {
  return entries
    .filter(
      (
        entry,
      ): entry is PlannerEntry & {
        custom: NonNullable<PlannerEntry["custom"]>;
      } => Boolean(entry.custom && !entry.showSlug && !entry.done),
    )
    .map((entry) => ({
      entryId: entry.id,
      label: entry.custom.label,
      icon: entry.custom.icon,
      durationMinutes: entry.custom.durationMinutes,
    }));
}

/**
 * Everything ticked, nothing shortened, nothing pinned: how the question opens. `priority` starts
 * empty, meaning the visitor has not spoken, so a pre-filled list does not make every ride look
 * pinned.
 */
export function fitChoiceAll(): FitChoice {
  return {
    dropped: new Set(),
    droppedBlocks: new Set(),
    shortBlocks: new Set(),
    priority: [],
  };
}

/**
 * The wishes in the order the day is decided in, which is what the list draws: pinned first, then
 * the rest as offered. Exported so the list is never a sort of its own and its bottom is what the
 * engine gives up first.
 */
export function fitOrder(input: FitInput, choice: FitChoice): FitWish[] {
  return ordered(input, choice);
}

/**
 * The wishes in the visitor's order, unranked ones behind them, and a second go on a ride behind
 * every first one, because the engine gives a lap up first and "gestrichen wird von unten" has to
 * stay true. A pinned lap stays where it was pinned.
 */
function ordered(input: FitInput, choice: FitChoice): FitWish[] {
  const byKey = new Map(input.wishes.map((wish) => [wish.key, wish]));
  const out: FitWish[] = [];
  for (const key of choice.priority) {
    const wish = byKey.get(key);
    if (wish) {
      out.push(wish);
      byKey.delete(key);
    }
  }
  const seen = new Set(out.map((wish) => wish.attractionSlug));
  const laps: FitWish[] = [];
  for (const wish of input.wishes) {
    if (!byKey.has(wish.key)) continue;
    if (seen.has(wish.attractionSlug)) {
      laps.push(wish);
      continue;
    }
    seen.add(wish.attractionSlug);
    out.push(wish);
  }
  return [...out, ...laps];
}

/**
 * The day as the choice leaves it, before the plan is laid over it: a switched-off wish goes, a
 * re-planned wish goes (it comes back as a stop), everything else stays, shortened where chosen.
 */
function entriesAfter(input: FitInput, choice: FitChoice): PlannerEntry[] {
  const byEntryId = new Map(
    input.wishes
      .filter((wish) => wish.entryId)
      .map((wish) => [wish.entryId as string, wish]),
  );
  const kept: PlannerEntry[] = [];
  for (const entry of input.entries) {
    const wish = byEntryId.get(entry.id);
    if (wish) {
      if (choice.dropped.has(wish.key)) continue;
      // No row in the payload, no curve to plan against: it keeps its minute (see
      // {@link FitWish.ride}).
      if (wish.ride) continue;
      kept.push(entry);
      continue;
    }
    if (!entry.custom) {
      kept.push(entry);
      continue;
    }
    if (choice.droppedBlocks.has(entry.id)) continue;
    if (
      choice.shortBlocks.has(entry.id) &&
      entry.custom.durationMinutes > SHORT_BLOCK_MIN
    ) {
      kept.push({
        ...entry,
        custom: { ...entry.custom, durationMinutes: SHORT_BLOCK_MIN },
      });
      continue;
    }
    kept.push(entry);
  }
  return kept;
}

/**
 * The plan for one choice, and what it leaves out. Where `optimizeDay` answers `null`, the day as
 * it stands is the plan, so it is scored through `scoreCurrent`; otherwise a list that already
 * fits would show an empty result.
 */
export function evaluateFit(input: FitInput, choice: FitChoice): FitOutcome {
  const wishes = ordered(input, choice).filter(
    (wish) => !choice.dropped.has(wish.key),
  );
  const entries = entriesAfter(input, choice);
  const replanned = wishes.filter((wish) => wish.ride);
  const optimizeInput = {
    day: input.day,
    grid: input.grid,
    entries,
    add: replanned.map((wish) => wish.ride as PlanDayRide),
    priority: replanned.map((wish) => wish.attractionSlug),
    clock: input.clock,
  };

  const plan = optimizeDay(optimizeInput);
  const scored = plan ?? scoreCurrent(optimizeInput);
  const stops = scored?.stops ?? [];

  // Paired in plan order, so a ride planned twice matches its two stops.
  const spare = stops.map((stop) => ({ stop, taken: false }));
  const fitted: string[] = [];
  const missed: string[] = [];
  for (const wish of wishes) {
    const hit = spare.find(
      (slot) =>
        !slot.taken &&
        slot.stop.attractionSlug === wish.attractionSlug &&
        (wish.ride
          ? slot.stop.entryId === null
          : slot.stop.entryId === wish.entryId),
    );
    if (hit) hit.taken = true;
    if (hit?.stop.fits) fitted.push(wish.key);
    else missed.push(wish.key);
  }

  return {
    fitted,
    missed,
    endMinute: scored?.endMinute ?? input.grid.openMin,
    totalWaitMinutes: scored?.totalWaitMinutes ?? 0,
    idleMinutes: scored?.idleMinutes ?? 0,
    entries,
    stops: plan ? plan.stops : [],
  };
}

/**
 * The changes worth offering, best first, each the whole engine run again against the current
 * choice, so the levers move as rides are unticked.
 *
 * Shortening is offered before dropping, and dropping not at all where half an hour already solves
 * it; neither for a block not longer than {@link SHORT_BLOCK_MIN}. One search per lever, so call
 * sites memoise it, and pass `base` (`evaluateFit(input, choice)`), which they already hold.
 */
export function fitLevers(
  input: FitInput,
  choice: FitChoice,
  base: FitOutcome = evaluateFit(input, choice),
): FitLever[] {
  const wanted = input.wishes.filter(
    (wish) => !choice.dropped.has(wish.key),
  ).length;
  const fitsNow = base.fitted.length;
  if (fitsNow >= wanted) return [];

  const open = input.blocks.filter(
    (block) => !choice.droppedBlocks.has(block.entryId),
  );
  const levers: FitLever[] = [];

  const probe = (next: FitChoice) => evaluateFit(input, next).fitted.length;
  const withSet = (set: ReadonlySet<string>, id: string) =>
    new Set([...set, id]);
  const withoutSet = (set: ReadonlySet<string>, id: string) =>
    new Set([...set].filter((value) => value !== id));

  for (const block of open) {
    const shortenable =
      block.durationMinutes > SHORT_BLOCK_MIN &&
      !choice.shortBlocks.has(block.entryId);
    const shortFits = shortenable
      ? probe({
          ...choice,
          shortBlocks: withSet(choice.shortBlocks, block.entryId),
        })
      : fitsNow;
    if (shortenable && shortFits > fitsNow) {
      levers.push({
        kind: "shorten-block",
        entryId: block.entryId,
        label: block.label,
        icon: block.icon,
        minutes: SHORT_BLOCK_MIN,
        fits: shortFits,
        fitsNow,
        wanted,
        solves: shortFits >= wanted,
      });
      // Half an hour already bought the whole list; dropping the block buys nothing more.
      if (shortFits >= wanted) continue;
    }
    const dropFits = probe({
      ...choice,
      droppedBlocks: withSet(choice.droppedBlocks, block.entryId),
      shortBlocks: withoutSet(choice.shortBlocks, block.entryId),
    });
    if (dropFits > Math.max(fitsNow, shortFits)) {
      levers.push({
        kind: "drop-block",
        entryId: block.entryId,
        label: block.label,
        icon: block.icon,
        fits: dropFits,
        fitsNow,
        wanted,
        solves: dropFits >= wanted,
      });
    }
  }

  // Only where no single block was enough: giving up every block is the biggest ask, so it comes
  // last.
  if (open.length > 1 && !levers.some((lever) => lever.solves)) {
    const allFits = probe({
      ...choice,
      droppedBlocks: new Set(open.map((block) => block.entryId)),
    });
    if (allFits > Math.max(fitsNow, ...levers.map((lever) => lever.fits))) {
      levers.push({
        kind: "drop-all-blocks",
        fits: allFits,
        fitsNow,
        wanted,
        solves: allFits >= wanted,
      });
    }
  }

  return levers.sort(
    (a, b) =>
      Number(b.solves) - Number(a.solves) ||
      b.fits - a.fits ||
      KIND_RANK[a.kind] - KIND_RANK[b.kind],
  );
}

/** A lever's identity, so a caller can hold a set of the ones already pulled. */
export function leverKey(lever: Pick<FitLever, "kind" | "entryId">): string {
  return `${lever.kind}:${lever.entryId ?? "*"}`;
}

/** The levers to draw, and which of them are on. */
export interface FitLeverView {
  levers: FitLever[];
  applied: Set<string>;
}

/**
 * {@link fitLevers}, plus the ones the visitor has already pulled, folded back in at the end with
 * current counts, so a pressed lever does not vanish and pressing it again undoes it. Here rather
 * than in a component, because both assistants draw this list.
 */
export function fitLeverView(
  input: FitInput,
  choice: FitChoice,
  base: FitOutcome = evaluateFit(input, choice),
): FitLeverView {
  const levers = fitLevers(input, choice, base);
  const applied = new Set<string>();
  for (const id of choice.droppedBlocks) applied.add(`drop-block:${id}`);
  for (const id of choice.shortBlocks) applied.add(`shorten-block:${id}`);
  if (
    input.blocks.length > 1 &&
    input.blocks.every((block) => choice.droppedBlocks.has(block.entryId))
  )
    applied.add("drop-all-blocks:*");

  const fits = base.fitted.length;
  const wanted = input.wishes.filter(
    (wish) => !choice.dropped.has(wish.key),
  ).length;
  const seen = new Set(levers.map(leverKey));
  const back: FitLever[] = [];
  for (const block of input.blocks) {
    const shared = {
      entryId: block.entryId,
      label: block.label,
      icon: block.icon,
      fits,
      fitsNow: fits,
      wanted,
      solves: fits >= wanted,
    };
    if (
      choice.droppedBlocks.has(block.entryId) &&
      !seen.has(`drop-block:${block.entryId}`)
    )
      back.push({ kind: "drop-block", ...shared });
    if (
      choice.shortBlocks.has(block.entryId) &&
      !seen.has(`shorten-block:${block.entryId}`)
    )
      back.push({ kind: "shorten-block", minutes: SHORT_BLOCK_MIN, ...shared });
  }
  return { levers: [...levers, ...back], applied };
}

/**
 * One lever pressed, as a new choice. Two levers on one block are alternatives, so taking one sets
 * the other down, and every press is its own inverse.
 */
export function toggleLever(
  input: FitInput,
  choice: FitChoice,
  lever: FitLever,
): FitChoice {
  const droppedBlocks = new Set(choice.droppedBlocks);
  const shortBlocks = new Set(choice.shortBlocks);

  if (lever.kind === "drop-all-blocks") {
    const all = input.blocks.every((block) => droppedBlocks.has(block.entryId));
    for (const block of input.blocks) {
      if (all) droppedBlocks.delete(block.entryId);
      else droppedBlocks.add(block.entryId);
    }
    return { ...choice, droppedBlocks, shortBlocks };
  }

  const id = lever.entryId as string;
  if (lever.kind === "drop-block") {
    if (droppedBlocks.has(id)) droppedBlocks.delete(id);
    else {
      droppedBlocks.add(id);
      shortBlocks.delete(id);
    }
  } else {
    if (shortBlocks.has(id)) shortBlocks.delete(id);
    else {
      shortBlocks.add(id);
      droppedBlocks.delete(id);
    }
  }
  return { ...choice, droppedBlocks, shortBlocks };
}

/** One wish ticked or unticked. A ride nobody plans is not a ride anybody pinned. */
export function toggleWish(choice: FitChoice, key: string): FitChoice {
  const dropped = new Set(choice.dropped);
  if (dropped.has(key)) dropped.delete(key);
  else dropped.add(key);
  return {
    ...choice,
    dropped,
    priority: dropped.has(key)
      ? choice.priority.filter((value) => value !== key)
      : choice.priority,
  };
}

/** One wish pinned to the top of the order, or unpinned. */
export function togglePin(choice: FitChoice, key: string): FitChoice {
  return {
    ...choice,
    priority: choice.priority.includes(key)
      ? choice.priority.filter((value) => value !== key)
      : [...choice.priority, key],
  };
}

/** Cheapest change first, where two levers buy the same thing. */
const KIND_RANK: Record<FitLeverKind, number> = {
  "shorten-block": 0,
  "drop-block": 1,
  "drop-all-blocks": 2,
};

/**
 * Whether this day is worth asking about at all: only where something the visitor asked for does
 * not fit. A day that holds everything never sees the assistant.
 */
export function needsFitHelp(input: FitInput, choice: FitChoice): boolean {
  const wanted = input.wishes.filter(
    (wish) => !choice.dropped.has(wish.key),
  ).length;
  return evaluateFit(input, choice).fitted.length < wanted;
}
