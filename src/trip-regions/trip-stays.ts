/**
 * The stays of a trip in a region with bases (Japan): which cities it sleeps
 * in, in which order and for how many nights, and what each day's move is.
 * The optimizer plans the parks inside every stay plan this module lists and
 * keeps the cheapest, as it does with the resort blocks. Pure. Contract:
 * docs/frontend/trip-regions.md#bases-a-trip-across-several-cities.
 *
 * The stays follow the region's route line (`RegionBaseConfig.order`) one way
 * or the other and never double back, or, in a region whose bases lie on no
 * line (Paris), come in any order the base legs connect; each base is stayed
 * in at most once.
 * The visitor may pin the first and last base (`startBase`, `endBase`, the
 * cities the flights land in and leave from) and any base's nights (0: no
 * stay there); the rest is searched. Where the trip ends somewhere other than
 * its last stay (a round trip from Tokyo), the departure day moves there, and
 * where the first base is ruled out, the arrival day moves on from it.
 */
import type { PerkRule, TransportMode } from "./regions";

/** How a base reaches a park for a day: door to door, one way. */
export interface StayReach {
  minutes: number;
  mode: "drive" | TransportMode;
}

/** A base as the search reads it, with the stay's own area applied. */
export interface StayBase {
  key: string;
  /** Its place on the route line; absent: the region's bases lie on none. */
  order?: number;
  /** Park slug → its reach from the stay's area (else the base's usual one). */
  reach: Map<string, StayReach>;
  /** The perks of the stay's hotel (its area's kind); none without an area. */
  perkRules: PerkRule[];
  area?: string;
}

/** A move between two bases, valid both ways. */
export interface StayLeg {
  from: string;
  to: string;
  minutes: number;
  mode: "drive" | TransportMode;
  name?: string;
  /** A park fits the day of this move (before or after it). */
  parkDay: boolean;
}

export interface StaysOption {
  bases: StayBase[];
  legs: StayLeg[];
  /**
   * The bases the visitor named: their nights (0: no stay there), or
   * undefined for a base named without nights (a stay, its nights searched).
   */
  fixed: Map<string, number | undefined>;
  startBase?: string;
  endBase?: string;
  rules: {
    dayTripFreeMinutes: number;
    moveOverheadMinutes: number;
    moveCost: number;
    /** When a morning move leaves, in minutes after midnight. */
    moveStartMinute: number;
    /** An evening move must arrive by this, in minutes after midnight. */
    lastArrivalMinute: number;
  };
}

/** One stay of a plan. */
export interface PlanStay {
  base: string;
  nights: number;
  /** Trip-day index of the check-in day and of the check-out day. */
  firstDay: number;
  lastDay: number;
  /** The search chose the base or its nights. */
  auto: boolean;
}

/** A sequence of stays over the trip's days, as the optimizer prices it. */
export interface StayPlan {
  stays: PlanStay[];
  /** Per trip day: the base the guest wakes in and the base they sleep in. */
  morning: string[];
  evening: string[];
  /** Per trip day: the move that day, where the two differ. */
  moveOf: Array<StayLeg | undefined>;
  /** Per trip day: a move to or from an airport's city on the arrival or departure day; no park. */
  endMove: boolean[];
  /** The moves' fixed cost (`moveCost` each). */
  fixedCost: number;
  /** The number of moves, the end moves included. */
  moves: number;
  /** Moves that leave no room for a park that day (a long leg, or an airport's). */
  travelMoves: number;
  /**
   * The trip's parks' reach, summed, each park from the closest of the plan's
   * bases: what decides between two plans that cost the same, so a tie goes
   * to the bases by the parks (Disneyland Paris' hotels for a Disney trip),
   * never to the one listed first.
   */
  reachMinutes: number;
}

/** The leg between two bases, either way round. */
export function stayLeg(
  legs: readonly StayLeg[],
  a: string,
  b: string,
): StayLeg | undefined {
  return legs.find(
    (l) => (l.from === a && l.to === b) || (l.from === b && l.to === a),
  );
}

/** Every split of `nights` over the stays, honouring the fixed ones; each stay at least one night. */
function nightSplits(
  nights: number,
  fixed: Array<number | undefined>,
): number[][] {
  const out: number[][] = [];
  const walk = (i: number, left: number, chosen: number[]) => {
    if (i === fixed.length) {
      if (left === 0) out.push([...chosen]);
      return;
    }
    const rest = fixed.length - i - 1;
    const restFixed = fixed
      .slice(i + 1)
      .reduce<number>((s, f) => s + (f ?? 1), 0);
    const options =
      fixed[i] !== undefined
        ? [fixed[i] as number]
        : Array.from(
            { length: Math.max(0, left - restFixed) },
            (_, k) => k + 1,
          );
    for (const n of options) {
      if (n < 1 || n > left - (rest > 0 ? restFixed : 0)) continue;
      chosen.push(n);
      walk(i + 1, left - n, chosen);
      chosen.pop();
    }
  };
  walk(0, nights, []);
  return out;
}

/** Every order of a list (a region's bases are few). */
function permutations<T>(items: readonly T[]): T[][] {
  if (items.length <= 1) return [[...items]];
  return items.flatMap((item, i) =>
    permutations([...items.slice(0, i), ...items.slice(i + 1)]).map((rest) => [
      item,
      ...rest,
    ]),
  );
}

/** Every subset of a list. */
function subsets<T>(items: readonly T[]): T[][] {
  const out: T[][] = [[]];
  for (const item of items) {
    const n = out.length;
    for (let i = 0; i < n; i++) out.push([...out[i], item]);
  }
  return out;
}

/** The parks of a trip that no base outside the ruled-out ones reaches at all. */
export function unreachableParks(
  opt: StaysOption,
  parks: readonly string[],
): string[] {
  const open = opt.bases.filter((b) => opt.fixed.get(b.key) !== 0);
  return parks.filter((p) => !open.some((b) => b.reach.has(p)));
}

/**
 * Every stay plan of a trip of `days` days for its parks: the bases that
 * reach them (a base counts when it reaches one within the free day-trip
 * time, or is pinned), in route order either way, with every split of the
 * nights. Empty when nothing fits (the plan then leaves the parks out).
 */
export function stayPlans(
  opt: StaysOption,
  days: number,
  parks: readonly string[],
): StayPlan[] {
  const nights = days - 1;
  const free = opt.rules.dayTripFreeMinutes;
  const byKey = new Map(opt.bases.map((b) => [b.key, b]));
  const excluded = (key: string) => opt.fixed.get(key) === 0;
  const coverable = parks.filter((p) =>
    opt.bases.some((b) => !excluded(b.key) && b.reach.has(p)),
  );
  const startStay =
    opt.startBase && !excluded(opt.startBase) ? opt.startBase : undefined;
  const endStay =
    opt.endBase && !excluded(opt.endBase) && opt.endBase !== startStay
      ? opt.endBase
      : undefined;
  const pinned = new Set<string>(
    [...opt.fixed].filter(([, n]) => n === undefined || n > 0).map(([k]) => k),
  );
  if (startStay) pinned.add(startStay);
  if (endStay) pinned.add(endStay);
  const useful = opt.bases.filter(
    (b) =>
      !excluded(b.key) &&
      !pinned.has(b.key) &&
      parks.some((p) => (b.reach.get(p)?.minutes ?? Infinity) <= free),
  );
  const plans: StayPlan[] = [];
  const seen = new Set<string>();
  for (const extra of subsets(useful)) {
    const set = [
      ...[...pinned].map((k) => byKey.get(k)).filter((b): b is StayBase => !!b),
      ...extra,
    ];
    if (set.length === 0) continue;
    if (!coverable.every((p) => set.some((b) => b.reach.has(p)))) continue;
    if (set.length > Math.max(1, nights)) continue;
    const onLine = set.every((b) => b.order !== undefined);
    const ascending = [...set].sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
    const orders = onLine
      ? [ascending, [...ascending].reverse()]
      : permutations(set);
    for (const seq of orders) {
      const keys = seq.map((b) => b.key);
      const signature = keys.join(">");
      if (seen.has(signature)) continue;
      seen.add(signature);
      if (startStay && keys[0] !== startStay) continue;
      if (endStay && keys[keys.length - 1] !== endStay) continue;
      if (keys.some((k, i) => i > 0 && !stayLeg(opt.legs, keys[i - 1], k))) {
        continue;
      }
      const arrivalFrom =
        opt.startBase && opt.startBase !== keys[0] ? opt.startBase : undefined;
      const departureTo =
        opt.endBase && opt.endBase !== keys[keys.length - 1]
          ? opt.endBase
          : undefined;
      if (arrivalFrom && !stayLeg(opt.legs, arrivalFrom, keys[0])) continue;
      if (
        departureTo &&
        !stayLeg(opt.legs, keys[keys.length - 1], departureTo)
      ) {
        continue;
      }
      const fixedNights = keys.map((k) => {
        const n = opt.fixed.get(k);
        return n !== undefined && n > 0 ? n : undefined;
      });
      const splits =
        nights === 0
          ? keys.length === 1
            ? [[0]]
            : []
          : nightSplits(nights, fixedNights);
      const reachMinutes = coverable.reduce(
        (sum, p) =>
          sum +
          Math.min(...seq.map((b) => b.reach.get(p)?.minutes ?? Infinity)),
        0,
      );
      for (const split of splits) {
        plans.push({
          ...planOf(
            opt,
            days,
            keys,
            split,
            fixedNights,
            arrivalFrom,
            departureTo,
          ),
          reachMinutes,
        });
      }
    }
  }
  return plans;
}

function planOf(
  opt: StaysOption,
  days: number,
  keys: string[],
  split: number[],
  fixedNights: Array<number | undefined>,
  arrivalFrom: string | undefined,
  departureTo: string | undefined,
): Omit<StayPlan, "reachMinutes"> {
  const nightBase: string[] = [];
  keys.forEach((k, i) => {
    for (let j = 0; j < split[i]; j++) nightBase.push(k);
  });
  const morning: string[] = [];
  const evening: string[] = [];
  for (let d = 0; d < days; d++) {
    morning.push(d === 0 ? (arrivalFrom ?? keys[0]) : nightBase[d - 1]);
    evening.push(
      d === days - 1
        ? (departureTo ?? nightBase[d - 1] ?? keys[keys.length - 1])
        : nightBase[d],
    );
  }
  const moveOf = morning.map((m, d) =>
    m !== evening[d] ? stayLeg(opt.legs, m, evening[d]) : undefined,
  );
  const endMove = moveOf.map(
    (leg, d) =>
      !!leg &&
      ((d === 0 && !!arrivalFrom) || (d === days - 1 && !!departureTo)),
  );
  const stays: PlanStay[] = [];
  let first = 0;
  keys.forEach((base, i) => {
    const last = first + split[i];
    stays.push({
      base,
      nights: split[i],
      firstDay: first,
      lastDay: Math.min(days - 1, last),
      auto: fixedNights[i] === undefined,
    });
    first = last;
  });
  const moves = moveOf.filter(Boolean).length;
  const travelMoves = moveOf.filter(
    (leg, d) => !!leg && (endMove[d] || !leg.parkDay),
  ).length;
  return {
    stays,
    morning,
    evening,
    moveOf,
    endMove,
    moves,
    travelMoves,
    fixedCost: moves * opt.rules.moveCost,
  };
}

/** The fewest days any stay plan of the trip spends only moving: what `split=auto` keeps free for them. */
export function fewestTravelDays(plans: readonly StayPlan[]): number {
  return plans.length === 0 ? 0 : Math.min(...plans.map((p) => p.travelMoves));
}
