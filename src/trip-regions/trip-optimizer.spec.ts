import {
  DEFAULT_MAX_STREAK,
  optimizeTrip,
  ORDER_MATTERS_MINUTES,
  PlanCostModel,
  type OptimizedPlan,
  type PlanCell,
  type PlanInput,
  type PlanOptions,
  type PlanPark,
} from "./trip-optimizer";
import { ORLANDO } from "./regions";
import { addIsoDays } from "../common/utils/date.util";

/** A small deterministic PRNG, so a failing instance can be reproduced. */
function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

const START = "2026-11-30"; // a Monday

function dates(n: number, start = START): string[] {
  return Array.from({ length: n }, (_, i) => addIsoDays(start, i));
}

const open = (wait: number, extra: Partial<PlanCell> = {}): PlanCell => ({
  status: "OPERATING",
  headlinerWait: wait,
  basis: "forecast",
  dayHours: 12,
  ...extra,
});
const closed: PlanCell = { status: "CLOSED", basis: "none" };

const BASE_OPTIONS: PlanOptions = {
  arrival: "park",
  departure: "park",
  blocks: false,
  blockGroups: ["walt-disney-world", "universal-orlando", "seaworld-orlando"],
  fireworks: "any",
  rest: { mode: "none" },
  hop: false,
  hopPairs: [],
  perkRules: [],
  locks: [],
};

const GROUP: Record<string, string> = Object.fromEntries(
  ORLANDO.parks.map((p) => [p.slug, p.group]),
);
const TYPE: Record<string, PlanPark["type"]> = Object.fromEntries(
  ORLANDO.parks.map((p) => [p.slug, p.type]),
);

function park(slug: string, visits = 1): PlanPark {
  return {
    slug,
    group: GROUP[slug] ?? "test",
    type: TYPE[slug] ?? "theme_park",
    visits,
  };
}

/**
 * A plan input: `waits[slug]` is one figure per day (`null` = CLOSED, `"?"` =
 * UNKNOWN), or a function for richer cells.
 */
function input(
  n: number,
  parks: PlanPark[],
  waits: Record<string, Array<number | null | "?"> | ((i: number) => PlanCell)>,
  options: Partial<PlanOptions> = {},
  start = START,
): PlanInput {
  const ds = dates(n, start);
  const cells: PlanInput["cells"] = {};
  for (const p of parks) {
    const spec = waits[p.slug];
    cells[p.slug] = {};
    ds.forEach((d, i) => {
      if (typeof spec === "function") {
        cells[p.slug][d] = spec(i);
        return;
      }
      const v = spec?.[i];
      cells[p.slug][d] =
        v === null
          ? closed
          : v === "?"
            ? { status: "UNKNOWN", basis: "none" }
            : open(v ?? 50);
    });
  }
  return {
    days: ds.map((date) => ({
      date,
      weekday: new Date(`${date}T12:00:00Z`).getUTCDay(),
      isHoliday: false,
      isSchoolVacation: false,
    })),
    parks,
    cells,
    options: { ...BASE_OPTIONS, ...options },
  };
}

/** Each park's planned dates. */
function placement(plan: OptimizedPlan): Record<string, string[]> {
  return Object.fromEntries(plan.parks.map((p) => [p.slug, p.dates]));
}

function parkDayFlags(plan: OptimizedPlan): boolean[] {
  return plan.days.map((d) => d.kind === "park" || d.kind === "hop");
}

function longestRun(flags: boolean[]): number {
  let best = 0;
  let run = 0;
  for (const f of flags) {
    run = f ? run + 1 : 0;
    best = Math.max(best, run);
  }
  return best;
}

/** Visits of one park on consecutive days. */
function adjacentRepeats(plan: OptimizedPlan): number {
  let count = 0;
  for (const p of plan.parks) {
    for (const d of p.dates) if (p.dates.includes(addIsoDays(d, 1))) count++;
  }
  return count;
}

describe("optimizeTrip", () => {
  describe("the assignment", () => {
    it("puts each park on its quietest day when the days do not collide", () => {
      const plan = optimizeTrip(
        input(5, [park("magic-kingdom-park"), park("epcot")], {
          "magic-kingdom-park": [60, 40, 55, 70, 65],
          epcot: [50, 45, 60, 30, 55],
        }),
      );
      expect(placement(plan)).toEqual({
        "magic-kingdom-park": ["2026-12-01"],
        epcot: ["2026-12-03"],
      });
      const mk = plan.days.find((d) => d.date === "2026-12-01");
      expect(mk).toMatchObject({
        kind: "park",
        parks: [{ slug: "magic-kingdom-park", part: "full" }],
        headlinerWait: 40,
        basis: "forecast",
      });
      expect(mk?.reasons).toEqual(
        expect.arrayContaining(["quietest_in_window", "weekday"]),
      );
      expect(plan.exact).toBe(true);
    });

    it("gives a contested day to the park that gains most from it", () => {
      // Both parks are quietest on day 1; MK loses 30 elsewhere, EPCOT only 5.
      const plan = optimizeTrip(
        input(3, [park("magic-kingdom-park"), park("epcot")], {
          "magic-kingdom-park": [70, 20, 70],
          epcot: [30, 25, 90],
        }),
      );
      expect(placement(plan)).toEqual({
        "magic-kingdom-park": ["2026-12-01"],
        epcot: ["2026-11-30"],
      });
    });

    it("keeps the arrival and departure days free by default", () => {
      const plan = optimizeTrip(
        input(
          4,
          [park("epcot")],
          { epcot: [10, 50, 60, 10] },
          { arrival: "free", departure: "free" },
        ),
      );
      expect(plan.days.map((d) => d.kind)).toEqual([
        "arrival",
        "park",
        "rest",
        "departure",
      ]);
      expect(placement(plan).epcot).toEqual(["2026-12-01"]);
    });

    it("never uses a CLOSED day, and an UNKNOWN one only as a last resort", () => {
      const plan = optimizeTrip(
        input(4, [park("magic-kingdom-park"), park("epcot")], {
          "magic-kingdom-park": [null, 90, "?", null],
          epcot: [null, 20, "?", null],
        }),
      );
      // Both want day 1; the one that must go elsewhere gets the UNKNOWN day.
      expect(placement(plan)).toEqual({
        "magic-kingdom-park": ["2026-12-01"],
        epcot: ["2026-12-02"],
      });
      const unknownDay = plan.days.find((d) => d.date === "2026-12-02");
      expect(unknownDay?.warnings).toContain("hours_not_published");
    });

    it("leaves out a park that is closed on every day of the trip", () => {
      const plan = optimizeTrip(
        input(3, [park("epcot"), park("disneys-typhoon-lagoon-water-park")], {
          epcot: [40, 30, 50],
          "disneys-typhoon-lagoon-water-park": [null, null, null],
        }),
      );
      expect(plan.unplaced).toEqual([
        { slug: "disneys-typhoon-lagoon-water-park", reason: "no_open_day" },
      ]);
      expect(placement(plan).epcot).toEqual(["2026-12-01"]);
    });

    it("drops a repeat visit first when the days run out", () => {
      const plan = optimizeTrip(
        input(2, [park("magic-kingdom-park", 2), park("epcot")], {
          "magic-kingdom-park": [30, 40],
          epcot: [50, 60],
        }),
      );
      expect(plan.unplaced).toEqual([
        { slug: "magic-kingdom-park", reason: "no_day_left" },
      ]);
      expect(plan.totals.visits).toBe(2);
    });

    it("ties the optimum brute force finds, whenever it says exact", () => {
      const random = lcg(42);
      const slugs = [
        "magic-kingdom-park",
        "epcot",
        "disneys-hollywood-studios",
        "universal-studios-florida",
      ];
      let checked = 0;
      for (let k = 0; k < 150; k++) {
        const n = 3 + Math.floor(random() * 5);
        const parks = slugs
          .slice(0, 1 + Math.floor(random() * 4))
          .map((s) => park(s, random() < 0.3 ? 2 : 1));
        const waits: Record<string, Array<number | null>> = {};
        for (const p of parks) {
          waits[p.slug] = Array.from({ length: n }, () =>
            random() < 0.15 ? null : 10 + Math.round(random() * 80),
          );
        }
        const rest =
          random() < 0.5
            ? ({ mode: "none" } as const)
            : ({ mode: "max", n: 1 + Math.floor(random() * 3) } as const);
        const arrival = random() < 0.3 ? "free" : "park";
        const plan = optimizeTrip(input(n, parks, waits, { rest, arrival }));

        const expected = bruteForce(n, parks, waits, rest, arrival === "free");
        if (expected === null || plan.unplaced.length > 0) continue;
        if (!plan.exact || plan.relaxed.length > 0) continue;
        checked++;
        const got =
          plan.parks.reduce(
            (sum, p) =>
              sum +
              p.dates.reduce(
                (s, d) => s + (waits[p.slug][dates(n).indexOf(d)] as number),
                0,
              ),
            0,
          ) +
          PlanCostModel.adjacentRepeatPenalty * adjacentRepeats(plan);
        // The spread may take a placement that ties the cheapest: never
        // below it, less than ORDER_MATTERS_MINUTES per park day above.
        expect(got).toBeGreaterThanOrEqual(expected - 1e-6);
        expect(got).toBeLessThan(
          expected + ORDER_MATTERS_MINUTES * plan.totals.parkDays,
        );
      }
      expect(checked).toBeGreaterThan(50);
    });
  });

  describe("repeat visits", () => {
    it("never puts two visits of one park on the same day", () => {
      const plan = optimizeTrip(
        input(3, [park("magic-kingdom-park", 3)], {
          "magic-kingdom-park": [10, 20, 30],
        }),
      );
      expect(new Set(placement(plan)["magic-kingdom-park"]).size).toBe(3);
    });

    it("keeps two visits a day apart, however much the back-to-back days save", () => {
      // Days 1 and 2 are far the quietest, back to back; the rule wins.
      const plan = optimizeTrip(
        input(5, [park("magic-kingdom-park", 2)], {
          "magic-kingdom-park": [80, 10, 12, 90, 70],
        }),
      );
      // Apart, the cheapest pair is days 1 and 4 (10 + 70), not 1 and 2 (22).
      expect(placement(plan)["magic-kingdom-park"]).toEqual([
        "2026-12-01",
        "2026-12-04",
      ]);
      expect(plan.exact).toBe(true);
      expect(
        plan.days.some((d) => d.warnings.includes("adjacent_repeat")),
      ).toBe(false);
    });

    it("keeps three visits apart in a week, with other parks between", () => {
      const plan = optimizeTrip(
        input(6, [park("magic-kingdom-park", 3), park("epcot", 2)], {
          "magic-kingdom-park": [10, 10, 10, 50, 50, 50],
          epcot: [50, 50, 50, 10, 10, 10],
        }),
      );
      expect(adjacentRepeats(plan)).toBe(0);
      expect(plan.unplaced).toEqual([]);
      expect(plan.exact).toBe(true);
    });

    it("warns when back-to-back visits are the only way", () => {
      const plan = optimizeTrip(
        input(2, [park("magic-kingdom-park", 2)], {
          "magic-kingdom-park": [30, 30],
        }),
      );
      expect(plan.days[1].warnings).toContain("adjacent_repeat");
      expect(plan.days[0].warnings).not.toContain("adjacent_repeat");
    });

    it("plans a 14-day trip with three doubles inside every rule", () => {
      const parks = [
        park("magic-kingdom-park", 2),
        park("epcot", 2),
        park("universal-epic-universe", 2),
        park("disneys-hollywood-studios"),
        park("disneys-animal-kingdom-theme-park"),
        park("universal-studios-florida"),
        park("universal-islands-of-adventure"),
      ];
      const random = lcg(14);
      const waits: Record<string, number[]> = {};
      for (const p of parks) {
        // Weekends busier, as in the real calendar.
        waits[p.slug] = dates(14).map((d) => {
          const dow = new Date(`${d}T12:00:00Z`).getUTCDay();
          return (
            30 + Math.round(random() * 30) + (dow === 0 || dow === 6 ? 25 : 0)
          );
        });
      }
      const plan = optimizeTrip(
        input(14, parks, waits, {
          arrival: "free",
          departure: "free",
          rest: { mode: "auto" },
        }),
      );
      expect(plan.unplaced).toEqual([]);
      expect(plan.totals).toMatchObject({
        visits: 10,
        parkDays: 10,
        restDays: 2,
      });
      expect(plan.days[0].kind).toBe("arrival");
      expect(plan.days[13].kind).toBe("departure");
      expect(longestRun(parkDayFlags(plan))).toBeLessThanOrEqual(
        DEFAULT_MAX_STREAK,
      );
      for (const p of plan.parks) {
        expect(new Set(p.dates).size).toBe(p.dates.length);
      }
      expect(adjacentRepeats(plan)).toBe(0);
      const rests = plan.days.filter((d) => d.kind === "rest");
      expect(rests).toHaveLength(2);
      for (const r of rests) {
        expect(r.reasons.length).toBeGreaterThan(0);
      }
      expect(plan.totals.savedMinutesPerRide).toBeGreaterThanOrEqual(0);
    });

    it("keeps the 14-day trip inside every rule with resort blocks too", () => {
      const parks = [
        park("magic-kingdom-park", 2),
        park("epcot", 2),
        park("universal-epic-universe", 2),
        park("disneys-hollywood-studios"),
        park("disneys-animal-kingdom-theme-park"),
        park("universal-studios-florida"),
        park("universal-islands-of-adventure"),
      ];
      const random = lcg(1414);
      const waits = Object.fromEntries(
        parks.map((p) => [
          p.slug,
          Array.from({ length: 14 }, () => 25 + Math.round(random() * 50)),
        ]),
      );
      const plan = optimizeTrip(
        input(14, parks, waits, {
          arrival: "free",
          departure: "free",
          rest: { mode: "auto" },
          blocks: true,
        }),
      );
      expect(plan.unplaced).toEqual([]);
      expect(plan.relaxed).toEqual([]);
      expect(longestRun(parkDayFlags(plan))).toBeLessThanOrEqual(4);
      expect(adjacentRepeats(plan)).toBe(0);
      const groups = plan.days
        .filter((d) => d.parks.length > 0)
        .map((d) => GROUP[d.parks[0].slug]);
      // One switch of resort: Disney then Universal, or the other way round.
      const switches = groups.filter((g, i) => i > 0 && g !== groups[i - 1]);
      expect(switches).toHaveLength(1);
    });
  });

  describe("rest days", () => {
    it("auto keeps runs to four park days and spares the busiest days", () => {
      // Nine days, seven parks: the two spare days must break the run.
      const parks = [
        "magic-kingdom-park",
        "epcot",
        "disneys-hollywood-studios",
        "disneys-animal-kingdom-theme-park",
        "universal-studios-florida",
        "universal-islands-of-adventure",
        "universal-epic-universe",
      ].map((s) => park(s));
      const waits = Object.fromEntries(
        parks.map((p) => [p.slug, [20, 20, 20, 20, 20, 20, 20, 90, 95]]),
      );
      const plan = optimizeTrip(
        input(9, parks, waits, { rest: { mode: "auto" } }),
      );
      expect(longestRun(parkDayFlags(plan))).toBeLessThanOrEqual(4);
      // Both spare days are free days; one may fall on the last day, which
      // stays the departure day.
      expect(plan.days.filter((d) => d.parks.length === 0)).toHaveLength(2);
      const rests = plan.days.filter((d) => d.kind === "rest");
      expect(rests.some((d) => d.reasons.includes("rest_after_streak"))).toBe(
        true,
      );
      expect(plan.exact).toBe(true);
    });

    it("auto rests on the busiest days when the streak rule leaves room", () => {
      const parks = [
        "magic-kingdom-park",
        "epcot",
        "disneys-hollywood-studios",
      ].map((s) => park(s));
      const waits = Object.fromEntries(
        parks.map((p) => [p.slug, [30, 30, 90, 30, 30]]),
      );
      const plan = optimizeTrip(
        input(5, parks, waits, { rest: { mode: "auto" } }),
      );
      const rest = plan.days.filter((d) => d.kind === "rest");
      expect(rest.map((d) => d.date)).toContain("2026-12-02");
      expect(rest.find((d) => d.date === "2026-12-02")?.reasons).toContain(
        "busiest_day_spared",
      );
    });

    it("loosens the streak only as far as the chosen visits force it", () => {
      const parks = ["a", "b", "c", "d", "e", "f"].map((s) => park(s));
      const waits = Object.fromEntries(
        parks.map((p) => [p.slug, [30, 30, 30, 30, 30, 30]]),
      );
      const plan = optimizeTrip(
        input(6, parks, waits, { rest: { mode: "auto" } }),
      );
      expect(plan.relaxed).toContain("streak_relaxed");
      expect(plan.days[4].warnings).toContain("long_streak");
      expect(plan.days[3].warnings).not.toContain("long_streak");
    });

    it("max:n holds exactly", () => {
      const parks = ["a", "b", "c", "d"].map((s) => park(s));
      const waits = Object.fromEntries(
        parks.map((p) => [p.slug, [10, 10, 10, 10, 80, 80]]),
      );
      const plan = optimizeTrip(
        input(6, parks, waits, { rest: { mode: "max", n: 2 } }),
      );
      expect(longestRun(parkDayFlags(plan))).toBeLessThanOrEqual(2);
      expect(plan.totals.visits).toBe(4);
      expect(plan.exact).toBe(true);
    });

    it("searches locally past the exact limit and says so", () => {
      // 21 days, the eight quietest in one row: the rule must break it up, and
      // there are far more than REST_EXACT_LIMIT spare patterns.
      const parks = ["a", "b", "c", "d", "e", "f", "g", "h"].map((s) =>
        park(s),
      );
      const waits = Object.fromEntries(
        parks.map((p, k) => [
          p.slug,
          Array.from({ length: 21 }, (_, i) =>
            i < 8 ? 10 + k : 60 + ((i * 7 + k) % 13),
          ),
        ]),
      );
      const plan = optimizeTrip(
        input(21, parks, waits, { rest: { mode: "auto" } }),
      );
      expect(plan.exact).toBe(false);
      expect(longestRun(parkDayFlags(plan))).toBeLessThanOrEqual(4);
      expect(plan.totals.visits).toBe(8);
    });

    it("none suggests nothing and lets the parks run on", () => {
      const parks = ["a", "b", "c", "d", "e"].map((s) => park(s));
      const waits = Object.fromEntries(
        parks.map((p) => [p.slug, [10, 10, 10, 10, 10, 90]]),
      );
      const plan = optimizeTrip(
        input(6, parks, waits, { rest: { mode: "none" } }),
      );
      expect(longestRun(parkDayFlags(plan))).toBe(5);
      expect(plan.days[5]).toMatchObject({ kind: "departure", reasons: [] });
      expect(plan.days.every((d) => !d.warnings.includes("long_streak"))).toBe(
        true,
      );
    });

    it("honours a pinned rest day", () => {
      const plan = optimizeTrip(
        input(
          3,
          [park("epcot")],
          { epcot: [50, 10, 50] },
          {
            locks: [{ target: "rest", date: "2026-12-01" }],
          },
        ),
      );
      expect(plan.days[1]).toMatchObject({ kind: "rest", reasons: ["locked"] });
      expect(placement(plan).epcot).not.toContain("2026-12-01");
    });

    it("marks a rest day spare where the rule would hold with a park on it", () => {
      // Six visits on eight days: the day after four park days is the
      // rule's, the other is free for anything.
      const parks = ["a", "b", "c", "d", "e", "f"].map((s) => park(s));
      const waits = Object.fromEntries(
        parks.map((p) => [p.slug, [10, 10, 10, 10, 90, 10, 10, 95, 10]]),
      );
      const auto = optimizeTrip(
        input(9, parks, waits, { rest: { mode: "auto" }, departure: "free" }),
      );
      const rests = auto.days.filter((d) => d.kind === "rest");
      expect(rests.map((d) => [d.date, d.spare ?? false])).toEqual([
        ["2026-12-04", false],
        ["2026-12-07", true],
      ]);
      expect(auto.totals).toMatchObject({ restDays: 2, spareDays: 1 });
      // rest=none keeps no rule: every day without a park is spare.
      const none = optimizeTrip(
        input(9, parks, waits, { rest: { mode: "none" }, departure: "free" }),
      );
      expect(none.days.filter((d) => d.kind === "rest")).toEqual(
        expect.arrayContaining([expect.objectContaining({ spare: true })]),
      );
      expect(none.totals.spareDays).toBe(none.totals.restDays);
      // A pinned rest day is the visitor's own.
      const pinned = optimizeTrip(
        input(
          3,
          [park("epcot")],
          { epcot: [50, 10, 50] },
          { locks: [{ target: "rest", date: "2026-12-01" }] },
        ),
      );
      expect(pinned.days[1].spare).toBeUndefined();
    });
  });

  describe("spreading the park days", () => {
    const flat = (n: number, wait = 60) =>
      Array.from({ length: n }, () => wait);

    /** Runs of days without a park, rest and travel and the ends included. */
    function longestGap(plan: OptimizedPlan): number {
      return longestRun(plan.days.map((d) => d.parks.length === 0));
    }

    it("spreads the visits of a long trip over its flat days", () => {
      // Seventeen days of equal waits, three parks twice: any placement is
      // the cheapest, and the plan takes the one with no rest week.
      const parks = [
        park("magic-kingdom-park", 2),
        park("epcot", 2),
        park("universal-studios-florida", 2),
      ];
      const plan = optimizeTrip(
        input(
          17,
          parks,
          Object.fromEntries(parks.map((p) => [p.slug, flat(17)])),
          { rest: { mode: "auto" }, arrival: "free", departure: "free" },
        ),
      );
      expect(plan.totals.visits).toBe(6);
      expect(plan.totals.headlinerWait).toBe(60);
      expect(longestRun(parkDayFlags(plan))).toBe(1);
      expect(longestGap(plan)).toBeLessThanOrEqual(2);
      // Each park's two visits a few days apart.
      for (const p of plan.parks) {
        const [a, b] = p.dates.map((d) => Date.parse(d) / 86_400_000);
        expect(b - a).toBeGreaterThanOrEqual(3);
      }
    });

    it("moves a visit to a day at most five minutes dearer for it", () => {
      // EPCOT's quietest day sits next to Magic Kingdom's; its runner-up,
      // five minutes dearer, breaks up the cluster and the rest days.
      const plan = optimizeTrip(
        input(
          9,
          [park("magic-kingdom-park"), park("epcot")],
          {
            "magic-kingdom-park": [80, 40, 80, 80, 80, 80, 80, 80, 80],
            epcot: [80, 80, 50, 80, 80, 55, 80, 80, 80],
          },
          { rest: { mode: "auto" }, arrival: "free", departure: "free" },
        ),
      );
      expect(placement(plan)).toEqual({
        "magic-kingdom-park": ["2026-12-01"],
        epcot: ["2026-12-05"],
      });
    });

    it("never gives up a clearly better day for it", () => {
      // EPCOT's next best day is six minutes dearer: the cluster stays.
      const plan = optimizeTrip(
        input(
          9,
          [park("magic-kingdom-park"), park("epcot")],
          {
            "magic-kingdom-park": [80, 40, 80, 80, 80, 80, 80, 80, 80],
            epcot: [80, 80, 50, 80, 80, 56, 80, 80, 80],
          },
          { rest: { mode: "auto" }, arrival: "free", departure: "free" },
        ),
      );
      expect(placement(plan)).toEqual({
        "magic-kingdom-park": ["2026-12-01"],
        epcot: ["2026-12-02"],
      });
    });

    it("names the unchosen parks that fit a spare day, with their cheapest", () => {
      const ds = dates(6);
      const cells = {
        epcot: Object.fromEntries(ds.map((d) => [d, open(50)])),
        "universal-studios-florida": Object.fromEntries(
          ds.map((d, i) => [d, i === 2 ? closed : open(70 - 5 * i)]),
        ),
        "seaworld-orlando": Object.fromEntries(ds.map((d) => [d, closed])),
      };
      const plan = optimizeTrip({
        ...input(6, [park("epcot")], { epcot: flat(6, 50) }),
        candidates: [
          park("universal-studios-florida"),
          park("seaworld-orlando"),
        ],
        cells,
        options: {
          ...BASE_OPTIONS,
          rest: { mode: "auto" },
          arrival: "free",
          departure: "free",
        },
      });
      const spare = plan.days.filter((d) => d.spare).map((d) => d.date);
      expect(spare).toHaveLength(3);
      expect(plan.spareDayParks).toEqual([
        {
          park: "universal-studios-florida",
          days: spare
            .filter((d) => d !== ds[2])
            .sort((a, b) => b.localeCompare(a))
            .map((date) => ({
              date,
              headlinerWait: 70 - 5 * ds.indexOf(date),
            })),
        },
      ]);
      // Never a park the visitor chose, and nothing without a spare day.
      const full = optimizeTrip({
        ...input(2, [park("epcot", 2)], { epcot: flat(2, 50) }),
        candidates: [park("universal-studios-florida")],
      });
      expect(full.spareDayParks).toBeUndefined();
    });
  });

  describe("locks", () => {
    it("pins a visit to its day", () => {
      const plan = optimizeTrip(
        input(
          3,
          [park("epcot")],
          { epcot: [10, 50, 50] },
          {
            locks: [{ target: "epcot", date: "2026-12-02" }],
          },
        ),
      );
      expect(placement(plan).epcot).toEqual(["2026-12-02"]);
      expect(plan.days[2].reasons).toContain("locked");
    });

    it("ignores a lock on a closed day, outside the trip or past the visits", () => {
      const plan = optimizeTrip(
        input(
          3,
          [park("epcot")],
          { epcot: [null, 30, 40] },
          {
            locks: [
              { target: "epcot", date: "2026-11-30" },
              { target: "epcot", date: "2027-01-01" },
              { target: "epcot", date: "2026-12-02" },
              { target: "epcot", date: "2026-12-01" },
            ],
          },
        ),
      );
      expect(plan.ignoredLocks).toEqual([
        { lock: "epcot@2026-11-30", reason: "closed" },
        { lock: "epcot@2027-01-01", reason: "outside_trip" },
        { lock: "epcot@2026-12-01", reason: "no_visit_left" },
      ]);
      expect(placement(plan).epcot).toEqual(["2026-12-02"]);
    });

    it("turns a free arrival day into a park day when a visit is pinned to it", () => {
      const plan = optimizeTrip(
        input(
          3,
          [park("epcot")],
          { epcot: [50, 10, 10] },
          {
            arrival: "free",
            locks: [{ target: "epcot", date: "2026-11-30" }],
          },
        ),
      );
      expect(plan.days[0]).toMatchObject({
        kind: "park",
        parks: [{ slug: "epcot" }],
      });
    });
  });

  describe("blocks", () => {
    it("keeps each resort's days together and lets a day trip go anywhere", () => {
      const parks = [
        park("magic-kingdom-park"),
        park("epcot"),
        park("universal-studios-florida"),
        park("universal-islands-of-adventure"),
        park("legoland-florida"),
      ];
      // Unblocked, the parks would interleave: Disney quiet on even days,
      // Universal on odd ones.
      const waits = {
        "magic-kingdom-park": [10, 60, 12, 60, 14, 60],
        epcot: [11, 60, 13, 60, 15, 60],
        "universal-studios-florida": [60, 10, 60, 12, 60, 14],
        "universal-islands-of-adventure": [60, 11, 60, 13, 60, 15],
        "legoland-florida": [30, 30, 30, 30, 30, 30],
      };
      const unblocked = optimizeTrip(input(6, parks, waits));
      const blocked = optimizeTrip(input(6, parks, waits, { blocks: true }));
      expect(blocked.exact).toBe(true);
      const groupOn = blocked.days.map((d) =>
        d.parks.length ? GROUP[d.parks[0].slug] : "-",
      );
      for (const g of ["walt-disney-world", "universal-orlando"]) {
        const idx = groupOn.flatMap((x, i) => (x === g ? [i] : []));
        for (let i = idx[0]; i <= idx[idx.length - 1]; i++) {
          // Inside a block only that resort, a day trip or a free day.
          expect([g, "day-trips", "-"]).toContain(groupOn[i]);
        }
      }
      const sum = (plan: OptimizedPlan) =>
        plan.days.reduce((s, d) => s + (d.headlinerWait ?? 0), 0);
      expect(sum(blocked)).toBeGreaterThanOrEqual(sum(unblocked));
    });

    it("plans without blocks, and says so, when no order of the resorts fits", () => {
      const waits = {
        "magic-kingdom-park": [10, null, 10],
        "universal-studios-florida": [null, 10, null],
        epcot: [10, null, 10],
      };
      const plan = optimizeTrip(
        input(
          3,
          [
            park("magic-kingdom-park"),
            park("universal-studios-florida"),
            park("epcot"),
          ],
          waits,
          { blocks: true },
        ),
      );
      expect(plan.relaxed).toContain("blocks_relaxed");
      expect(plan.unplaced).toEqual([]);
    });
  });

  describe("hop days", () => {
    const hopPairs: Array<[string, string]> = [
      ["universal-studios-florida", "universal-islands-of-adventure"],
    ];

    it("pairs two parks on one day when the trip is shorter than its parks", () => {
      const plan = optimizeTrip(
        input(
          2,
          [
            park("universal-studios-florida"),
            park("universal-islands-of-adventure"),
            park("universal-epic-universe"),
          ],
          {
            "universal-studios-florida": [30, 40],
            "universal-islands-of-adventure": [35, 45],
            "universal-epic-universe": [60, 50],
          },
          { hop: true, hopPairs },
        ),
      );
      expect(plan.unplaced).toEqual([]);
      const hop = plan.days.find((d) => d.kind === "hop");
      expect(hop?.parks.map((p) => p.slug).sort()).toEqual([
        "universal-islands-of-adventure",
        "universal-studios-florida",
      ]);
      expect(hop?.parks.every((p) => p.part === "half")).toBe(true);
      expect(hop?.date).toBe("2026-11-30");
    });

    it("does not hop when every park has a day of its own", () => {
      const plan = optimizeTrip(
        input(
          3,
          [
            park("universal-studios-florida"),
            park("universal-islands-of-adventure"),
          ],
          {},
          { hop: true, hopPairs },
        ),
      );
      expect(plan.days.some((d) => d.kind === "hop")).toBe(false);
    });

    it("drops a visit rather than hop parks that may not share a day", () => {
      const plan = optimizeTrip(
        input(
          1,
          [park("magic-kingdom-park"), park("universal-epic-universe")],
          {},
          {
            hop: true,
            hopPairs,
          },
        ),
      );
      expect(plan.unplaced).toHaveLength(1);
    });
  });

  describe("fireworks", () => {
    // Day 1 is a party night: the day guests leave at 18:00 (9 h, not 13 h).
    const party = (i: number): PlanCell =>
      i === 1
        ? open(30, { dayHours: 9, evening: { kind: "ticketed_event" } })
        : open(i === 0 ? 38 : 60, { dayHours: 13 });

    it("any: prices a party night by the hours the day guest gets", () => {
      // 30 × 13/9 = 43.3 against 38.
      const plan = optimizeTrip(
        input(3, [park("magic-kingdom-park")], { "magic-kingdom-park": party }),
      );
      expect(placement(plan)["magic-kingdom-park"]).toEqual(["2026-11-30"]);
    });

    it("any: plans a party night that is quiet enough, and says the day is short", () => {
      // 10 × 13/9 = 14.4 against 60.
      const quietParty = (i: number): PlanCell =>
        i === 1
          ? open(10, { dayHours: 9, evening: { kind: "ticketed_event" } })
          : open(60, { dayHours: 13 });
      const plan = optimizeTrip(
        input(3, [park("magic-kingdom-park")], {
          "magic-kingdom-park": quietParty,
        }),
      );
      expect(placement(plan)["magic-kingdom-park"]).toEqual(["2026-12-01"]);
      expect(plan.days[1].warnings).toContain("party_night_short_hours");
    });

    it("want: keeps off a party night however quiet, while another day fits", () => {
      const quietParty = (i: number): PlanCell =>
        i === 1
          ? open(10, { dayHours: 9, evening: { kind: "ticketed_event" } })
          : open(80, { dayHours: 13 });
      const plan = optimizeTrip(
        input(
          3,
          [park("magic-kingdom-park")],
          { "magic-kingdom-park": quietParty },
          { fireworks: "want" },
        ),
      );
      expect(placement(plan)["magic-kingdom-park"]).not.toContain("2026-12-01");
    });

    it("want: uses a party night only when nothing else fits, with fireworks_missed", () => {
      // EPCOT is pinned to day 0, so day 1, the party night, is MK's only day.
      const plan = optimizeTrip(
        input(
          2,
          [park("magic-kingdom-park"), park("epcot")],
          {
            "magic-kingdom-park": (i) =>
              i === 1
                ? open(30, { dayHours: 9, evening: { kind: "ticketed_event" } })
                : open(40, { dayHours: 13 }),
            epcot: [20, 20],
          },
          {
            fireworks: "want",
            locks: [{ target: "epcot", date: "2026-11-30" }],
          },
        ),
      );
      expect(placement(plan)).toEqual({
        "magic-kingdom-park": ["2026-12-01"],
        epcot: ["2026-11-30"],
      });
      expect(plan.days[1].warnings).toContain("fireworks_missed");
      expect(plan.days[1].warnings).not.toContain("party_night_short_hours");
    });

    it("does not charge an After Hours night that leaves the day as long as usual", () => {
      const afterHours = (i: number): PlanCell =>
        i === 1
          ? open(30, { dayHours: 13, evening: { kind: "ticketed_event" } })
          : open(32, { dayHours: 13 });
      for (const fireworks of ["any", "want"] as const) {
        const plan = optimizeTrip(
          input(
            3,
            [park("magic-kingdom-park")],
            { "magic-kingdom-park": afterHours },
            { fireworks },
          ),
        );
        expect(placement(plan)["magic-kingdom-park"]).toEqual(["2026-12-01"]);
        expect(plan.days[1].warnings).toEqual([]);
      }
    });
  });

  describe("hotel perks", () => {
    const early = { kind: "early_entry" };

    it("lists the perk the hotel gets that day and lowers the cost", () => {
      const plan = optimizeTrip(
        input(
          2,
          [park("magic-kingdom-park")],
          {
            "magic-kingdom-park": (i) => (i === 0 ? open(42) : open(40, {})),
          },
          { perkRules: ORLANDO.perks.disney },
        ),
      );
      // Without the perk day 1 wins; nobody has early entry here.
      expect(placement(plan)["magic-kingdom-park"]).toEqual(["2026-12-01"]);

      const withEarly = optimizeTrip(
        input(
          2,
          [park("magic-kingdom-park")],
          {
            "magic-kingdom-park": (i) =>
              i === 0 ? open(41.5, { early }) : open(40),
          },
          { perkRules: ORLANDO.perks.disney },
        ),
      );
      // 41.5 × 0.95 = 39.4, plus 0.4 for the last day of the hurricane
      // season, beats 40 on 1 December.
      expect(placement(withEarly)["magic-kingdom-park"]).toEqual([
        "2026-11-30",
      ]);
      expect(withEarly.days[0].perks).toEqual(["early_entry"]);
      expect(withEarly.days[0].reasons).toContain("early_entry");
    });

    it("gives Express at the premier hotels to Universal Studios and Islands only", () => {
      const plan = optimizeTrip(
        input(
          2,
          [park("universal-studios-florida"), park("universal-epic-universe")],
          {},
          { perkRules: ORLANDO.perks.universal_premier },
        ),
      );
      const usf = plan.days.find(
        (d) => d.parks[0]?.slug === "universal-studios-florida",
      );
      const epic = plan.days.find(
        (d) => d.parks[0]?.slug === "universal-epic-universe",
      );
      expect(usf?.perks).toEqual(["express_included"]);
      expect(epic?.perks).toBeUndefined();
    });

    it("counts an extended evening only for the deluxe hotels", () => {
      const evening = (i: number): PlanCell =>
        i === 0
          ? open(42, { evening: { kind: "extended_evening" } })
          : open(40);
      const deluxe = optimizeTrip(
        input(
          2,
          [park("epcot")],
          { epcot: evening },
          { perkRules: ORLANDO.perks.disney_deluxe },
        ),
      );
      const moderate = optimizeTrip(
        input(
          2,
          [park("epcot")],
          { epcot: evening },
          { perkRules: ORLANDO.perks.disney },
        ),
      );
      expect(placement(deluxe).epcot).toEqual(["2026-11-30"]);
      expect(deluxe.days[0].reasons).toContain("extended_evening");
      expect(placement(moderate).epcot).toEqual(["2026-12-01"]);
    });

    it("gives the Disney Springs Resort Area hotels Early Entry and no extended evening", () => {
      const rules = { perkRules: ORLANDO.perks.disney_springs };
      const withEarly = optimizeTrip(
        input(
          2,
          [park("magic-kingdom-park")],
          {
            "magic-kingdom-park": (i) =>
              i === 0 ? open(41.5, { early }) : open(40),
          },
          rules,
        ),
      );
      expect(placement(withEarly)["magic-kingdom-park"]).toEqual([
        "2026-11-30",
      ]);
      expect(withEarly.days[0].perks).toEqual(["early_entry"]);
      const evening = optimizeTrip(
        input(
          2,
          [park("epcot")],
          {
            epcot: (i) =>
              i === 0
                ? open(42, { evening: { kind: "extended_evening" } })
                : open(40),
          },
          rules,
        ),
      );
      // As for a moderate Disney hotel: the extended evening is the deluxe guests'.
      expect(placement(evening).epcot).toEqual(["2026-12-01"]);
      expect(evening.days.flatMap((d) => d.perks ?? [])).not.toContain(
        "extended_evening",
      );
    });
  });

  describe("water parks and reservation parks", () => {
    it("go on the warm dry day the theme parks leave", () => {
      const plan = optimizeTrip(
        input(3, [park("epcot"), park("disneys-blizzard-beach-water-park")], {
          epcot: [20, 50, 50],
          "disneys-blizzard-beach-water-park": (i) => ({
            status: "OPERATING",
            basis: "none",
            weather:
              i === 2
                ? { tempMax: 30, precipitationMm: 0, icon: 1 }
                : { tempMax: 19, precipitationMm: 8, icon: 63 },
          }),
        }),
      );
      expect(placement(plan)).toEqual({
        epcot: ["2026-11-30"],
        "disneys-blizzard-beach-water-park": ["2026-12-02"],
      });
      expect(plan.days[2].reasons).toContain("water_park_weather");
      expect(plan.days[2].warnings).not.toContain("no_forecast");
    });
  });

  describe("the answer", () => {
    it("names three alternatives per park and compares like with like", () => {
      const plan = optimizeTrip(
        input(6, [park("epcot")], { epcot: [70, 30, 40, 35, null, 50] }),
      );
      expect(plan.parks[0]).toEqual({
        slug: "epcot",
        dates: ["2026-12-01"],
        alternatives: [
          { date: "2026-12-03", headlinerWait: 35 },
          { date: "2026-12-02", headlinerWait: 40 },
          { date: "2026-12-05", headlinerWait: 50 },
        ],
      });
      // Mean of the open days: (70 + 30 + 40 + 35 + 50) / 5 = 45.
      expect(plan.totals).toMatchObject({
        headlinerWait: 30,
        averageDayWait: 45,
        savedMinutesPerRide: 15,
        parkDays: 1,
        visits: 1,
        // The first and last free days stay the arrival and departure days.
        restDays: 3,
      });
      expect(plan.days[1].reasons).toContain("runner_up_close");
    });

    it("labels last-year and long-range days", () => {
      const plan = optimizeTrip(
        input(2, [park("epcot"), park("magic-kingdom-park")], {
          epcot: () => ({ ...open(30), basis: "last_year" }),
          "magic-kingdom-park": () => ({ ...open(30), basis: "long_range" }),
        }),
      );
      const warnings = plan.days.flatMap((d) => d.warnings);
      expect(warnings).toEqual(
        expect.arrayContaining(["estimate_last_year", "long_range"]),
      );
    });

    it("flags the day after a holiday", () => {
      const base = input(3, [park("epcot")], { epcot: [60, 30, 60] });
      base.days[0].isSchoolVacation = true;
      const plan = optimizeTrip(base);
      expect(plan.days[1].reasons).toContain("holiday_ends");
    });
  });
});

/**
 * The best full objective (waits plus the repeat penalty) over every placement
 * of the visits on open days, with the streak rule; null when none exists.
 */
function bruteForce(
  n: number,
  parks: PlanPark[],
  waits: Record<string, Array<number | null>>,
  rest: { mode: "none" } | { mode: "max"; n: number },
  arrivalFree: boolean,
): number | null {
  const units = parks.flatMap((p) =>
    Array.from({ length: p.visits }, () => p.slug),
  );
  const days = Array.from({ length: n }, (_, i) => i).filter(
    (i) => !(arrivalFree && i === 0),
  );
  if (units.length > days.length) return null;
  let best = Infinity;
  const used = new Map<number, string>();
  const walk = (k: number, sum: number) => {
    if (k === units.length) {
      const flags = Array.from({ length: n }, (_, i) => used.has(i));
      if (rest.mode === "max" && longestRun(flags) > rest.n) return;
      let adjacent = 0;
      for (const [d, slug] of used) if (used.get(d + 1) === slug) adjacent++;
      best = Math.min(
        best,
        sum + PlanCostModel.adjacentRepeatPenalty * adjacent,
      );
      return;
    }
    for (const d of days) {
      if (used.has(d)) continue;
      const w = waits[units[k]][d];
      if (w === null) continue;
      used.set(d, units[k]);
      walk(k + 1, sum + w);
      used.delete(d);
    }
  };
  walk(0, 0);
  return Number.isFinite(best) ? best : null;
}
