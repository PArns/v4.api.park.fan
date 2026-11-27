/**
 * PAR-1452: the fit assistant on a real day (Phantasialand, 27 Nov 2026, the `/plan/day` payload
 * the app planned on), fourteen rides, one plan in the order the app sent it.
 *
 * - Unticking rides never makes the rest come out worse than the plan for the whole list with
 *   those rides taken out: no fewer rides fitted, and no worse under the engine's own order
 *   (overflow tiers, then the cost).
 * - A pinned ride is not given up for unpinned ones; where the day has no minute for it at all,
 *   `pinnedMissed` names it.
 *
 *     pnpm test -- fit.monotone
 */
import { readFileSync } from "fs";
import { join } from "path";
import { gunzipSync } from "zlib";
import { gridForDay } from "./day-grid";
import { runDayOptimizer } from "./engine";
import {
  evaluateFit,
  fitBlocks,
  fitChoiceAll,
  fitOrder,
  fitWishes,
  type FitChoice,
  type FitInput,
} from "./fit";
import { scoreOrder } from "./optimize";
import type { DayClock, OptimizerDay } from "./types";

const day = JSON.parse(
  gunzipSync(
    readFileSync(
      join(__dirname, "__fixtures__/par-1452-phantasialand-2026-11-27.json.gz"),
    ),
  ).toString(),
) as OptimizerDay;
const grid = gridForDay(day)!;
const slugs = [
  "taron",
  "crazy-bats",
  "winjas-fear",
  "winjas-force",
  "maus-au-chocolat",
  "fly",
  "black-mamba",
  "colorado-adventure",
  "raik",
  "mystery-castle",
  "wuermling-express",
  "das-verrueckte-hotel-tartueff",
  "feng-ju-palace",
  "talocan",
];
const entries = slugs.map((attractionSlug, i) => ({
  id: `r${i}`,
  attractionSlug,
  startMinute: 1035 + i * 15,
}));
const slugOf = (key: string) => slugs[Number(key.slice(3))];
const today = (nowMinute: number): DayClock => ({ phase: "today", nowMinute });
const inputAt = (clock: DayClock): FitInput => ({
  day,
  grid,
  entries,
  wishes: fitWishes(day, entries, [], clock),
  blocks: fitBlocks(entries),
  clock,
});
const choice = (dropped: string[], priority: string[] = []): FitChoice => ({
  ...fitChoiceAll(),
  dropped: new Set(dropped),
  priority,
});

/** An order scored as `evaluateFit` would plan it for `c`: overflow key, then the cost. */
function score(input: FitInput, c: FitChoice, order: string[]): number[] {
  const wishes = fitOrder(input, c).filter((wish) => !c.dropped.has(wish.key));
  const keys = new Set(input.wishes.map((wish) => wish.key));
  const s = scoreOrder(
    {
      day,
      grid,
      entries: entries.filter((entry) => !keys.has(`e:${entry.id}`)),
      add: wishes.map((wish) => wish.ride!),
      priority: wishes.map((wish) => wish.attractionSlug),
      clock: input.clock,
    },
    order,
  )!;
  return [
    s.overflowKey,
    s.totalWaitMinutes + 0.5 * s.idleMinutes + 0.2 * s.walkMinutes,
  ];
}

/** What went wrong with `dropped` unticked, against the whole list's plan; `null` where nothing. */
function worseThanWhole(input: FitInput, dropped: string[]): string | null {
  const whole = evaluateFit(input, choice([]));
  const c = choice(dropped);
  const sub = evaluateFit(input, c);
  const out = dropped.map(slugOf);
  const restricted = [
    ...whole.stops.map((stop) => stop.attractionSlug),
    ...whole.missed.map(slugOf),
  ].filter((slug) => !out.includes(slug));
  const own = [
    ...sub.stops.map((stop) => stop.attractionSlug),
    ...sub.missed.map(slugOf),
  ];
  const kept = whole.fitted.filter((key) => !dropped.includes(key));
  const [wasKey, wasCost] = score(input, c, restricted);
  const [nowKey, nowCost] = score(input, c, own);
  return sub.fitted.length < kept.length ||
    nowKey > wasKey ||
    (nowKey === wasKey && nowCost > wasCost + 1e-9)
    ? `${dropped.join("+")}: ${sub.fitted.length} fitted (whole list kept ${kept.length})`
    : null;
}

/** The sweep below: `PLANNER_EXHAUSTIVE=1` (`pnpm test:planner-exhaustive`), about 45 seconds. */
const EXHAUSTIVE = process.env.PLANNER_EXHAUSTIVE === "1";

describe("fit: a smaller list never fits worse (PAR-1452)", () => {
  // Cases the search alone answered worse than the whole list's plan with the rides taken out.
  it.each([
    [900, ["e:r11", "e:r13"]], // one ride fewer fitted
    [900, ["e:r13"]],
    [750, ["e:r2"]],
    [1050, ["e:r2"]],
  ])("now %i, %j unticked", (now, dropped) => {
    expect(worseThanWhole(inputAt(today(now)), dropped)).toBeNull();
  });

  (EXHAUSTIVE ? it.each : it.skip.each)([660, 750, 900, 990, 1050])(
    "now %i: every one or two rides unticked",
    (now) => {
      const input = inputAt(today(now));
      const failures: string[] = [];
      for (let i = 0; i < slugs.length; i++)
        for (let j = i; j < slugs.length; j++) {
          const failure = worseThanWhole(input, [
            ...new Set([`e:r${i}`, `e:r${j}`]),
          ]);
          if (failure) failures.push(failure);
        }
      expect(failures).toEqual([]);
    },
  );
});

describe("fit: a pin is kept (PAR-1452)", () => {
  // 16:30, the minute of the report: Taron, Winja's Force and Fly do not fit the whole list.
  const clock = today(990);
  const input = inputAt(clock);
  const whole = evaluateFit(input, choice([]));

  it("the report's day leaves rides out", () => {
    expect(whole.missed.length).toBeGreaterThan(0);
    expect(whole.pinnedMissed).toEqual([]);
  });

  it.each(["e:r0", "e:r3", "e:r5"])("pinning %s keeps it", (key) => {
    expect(whole.missed).toContain(key);
    const pinned = evaluateFit(input, choice([], [key]));
    expect(pinned.fitted).toContain(key);
    expect(pinned.pinnedMissed).toEqual([]);
  });

  it("pinning everything that fell out keeps all of them", () => {
    const pinned = evaluateFit(input, choice([], whole.missed));
    for (const key of whole.missed) expect(pinned.fitted).toContain(key);
    expect(pinned.pinnedMissed).toEqual([]);
  });

  it("a pin the day has no minute for is named, through the engine's answer", () => {
    // One minute before closing nothing can be queued for any more.
    const late = runDayOptimizer(
      day,
      {
        mode: "fit",
        entries: entries.map((entry) => ({ ...entry, startMinute: 1205 })),
        choice: { priority: ["e:r0"] },
      },
      today(grid.closeMin - 1),
    );
    expect(late.status).toBe("fit");
    expect(late.fit?.missed).toContain("e:r0");
    expect(late.fit?.pinnedMissed).toEqual(["e:r0"]);
  });
});
