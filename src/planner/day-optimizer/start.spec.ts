import { runDayOptimizer, type PlanStart } from "./engine";
import type {
  DayClock,
  OptimizerDay,
  OptimizerRide,
  PlannerEntry,
} from "./types";

/**
 * "Umplanen ab hier": the rest of today planned from the ride the visitor stands at. Two clusters
 * of rides about a kilometre apart, every queue the same, so the walk alone decides the order.
 */

const WEST = 6.87;
/** About 1050 m east at this latitude: 26 minutes at park pace with the detour factor. */
const EAST = 6.885;

function ride(slug: string, longitude: number, wait = 20): OptimizerRide {
  const hours = [];
  for (let hour = 9; hour <= 18; hour++) hours.push({ hour, wait });
  return {
    attractionSlug: slug,
    attractionName: slug.toUpperCase(),
    hours,
    dayPeak: wait,
    sampleDays: 90,
    latitude: 50.8,
    longitude,
    land: null,
  };
}

const DAY: OptimizerDay = {
  tier: "measured",
  context: { openHour: 9, closeHour: 18, liveWaitTimes: { available: true } },
  rides: [
    ride("w1", WEST),
    ride("w2", WEST + 0.0003),
    ride("e1", EAST),
    ride("e2", EAST + 0.0003),
    ride("ridden", WEST),
    ride("running", WEST),
  ],
  shows: [],
};

const entry = (
  id: string,
  slug: string,
  startMinute: number,
  extra: Partial<PlannerEntry> = {},
): PlannerEntry => ({ id, attractionSlug: slug, startMinute, ...extra });

const at = (
  slug: string,
  longitude: number,
  extra: Partial<PlanStart> = {},
): PlanStart => ({
  attractionSlug: slug,
  attractionName: slug.toUpperCase(),
  latitude: 50.8,
  longitude,
  land: null,
  ...extra,
});

const NOW = 14 * 60;
const TODAY: DayClock = { phase: "today", nowMinute: NOW };

/** A zigzag across the park, with a ticked-off ride and one under way before it. */
const ENTRIES = [
  entry("done", "ridden", 10 * 60, { done: true, actualWait: 15 }),
  entry("past", "running", 13 * 60 + 45),
  entry("w1", "w1", 14 * 60 + 15),
  entry("e1", "e1", 15 * 60),
  entry("w2", "w2", 15 * 60 + 45),
  entry("e2", "e2", 16 * 60 + 30),
];

const plan = (start?: PlanStart, clock: DayClock = TODAY, entries = ENTRIES) =>
  runDayOptimizer(
    DAY,
    { mode: "day", entries, preview: true, ...(start && { start }) },
    clock,
  );

describe("the day optimizer from a start", () => {
  it("orders the rest of the day by the walk from where the visitor stands", () => {
    const fromEast = plan(at("east-kiosk", EAST));
    const fromWest = plan(at("west-kiosk", WEST));
    expect(fromEast.status).toBe("optimized");
    const sides = (answer: typeof fromEast) =>
      answer.plan?.stops.map((stop) => stop.attractionSlug[0]).join("");
    expect(sides(fromEast)).toBe("eeww");
    // From the west the day is walked the other way round.
    expect(fromWest.status).toBe("optimized");
    expect(sides(fromWest)).toBe("wwee");
  });

  it("holds ticked-off and started entries where they are", () => {
    const answer = plan(at("east-kiosk", EAST));
    const minuteOf = (id: string) =>
      answer.entries?.find((e) => e.id === id)?.startMinute;
    expect(minuteOf("done")).toBe(10 * 60);
    expect(minuteOf("past")).toBe(13 * 60 + 45);
    const moved = answer.plan?.stops.map((stop) => stop.entryId) ?? [];
    expect(moved).not.toContain("done");
    expect(moved).not.toContain("past");
    expect(answer.movable).toBe(4);
  });

  it("starts the first stop no earlier than now plus the walk", () => {
    const entries = [
      entry("w1", "w1", 14 * 60 + 15),
      entry("w2", "w2", 15 * 60),
    ];
    const answer = plan(at("east-kiosk", EAST), TODAY, entries);
    const first = answer.receipt?.first;
    expect(first?.walkMinutes).toBeGreaterThanOrEqual(25);
    expect(first?.startMinute).toBeGreaterThanOrEqual(
      NOW + (first?.walkMinutes ?? 0),
    );
    // The day as sent put W1 at 14:15, which nobody a kilometre away reaches: it moves.
    expect(answer.status).toBe("optimized");
    expect(answer.receipt?.moved).toContainEqual(
      expect.objectContaining({ entryId: "w1", fromMinute: 14 * 60 + 15 }),
    );
    for (const stop of answer.plan?.stops ?? [])
      expect(stop.startMinute).toBeGreaterThanOrEqual(NOW + 25);
    // Without a start the first walk costs nothing and the day begins at once.
    expect(plan(undefined, TODAY, entries).plan?.stops[0].startMinute).toBe(
      NOW,
    );
  });

  it("writes a receipt: the start, the walk, what moved from when to when", () => {
    const receipt = plan(at("east-kiosk", EAST)).receipt;
    expect(receipt?.from).toEqual({
      attractionSlug: "east-kiosk",
      attractionName: "EAST-KIOSK",
    });
    expect(receipt?.nowMinute).toBe(NOW);
    expect(receipt?.first?.attractionSlug).toMatch(/^e/);
    expect(receipt?.first?.walkMinutes).toBeLessThanOrEqual(1);
    expect(receipt?.moved.map((line) => line.entryId)).toEqual(
      expect.arrayContaining(["e1", "w1"]),
    );
    for (const line of receipt?.moved ?? [])
      expect(line.toMinute).not.toBe(line.fromMinute);
    expect(receipt?.pastClosing).toEqual([]);
    // The walk is in the totals too: before and after both start at the kiosk.
    const answer = plan(at("east-kiosk", EAST));
    expect(answer.before?.walkMinutes).toBeGreaterThan(
      answer.after?.walkMinutes ?? Infinity,
    );
  });

  it("flags what no longer fits before closing and drops nothing", () => {
    const late: DayClock = { phase: "today", nowMinute: 17 * 60 + 10 };
    const entries = [
      entry("w1", "w1", 17 * 60 + 15),
      entry("w2", "w2", 17 * 60 + 40),
      entry("e1", "e1", 17 * 60 + 55),
    ];
    const answer = plan(at("east-kiosk", EAST), late, entries);
    expect(answer.receipt?.pastClosing.length).toBeGreaterThan(0);
    for (const line of answer.receipt?.pastClosing ?? [])
      expect(line.toMinute).toBeGreaterThanOrEqual(18 * 60);
    expect((answer.entries ?? entries).map((e) => e.id).sort()).toEqual([
      "e1",
      "w1",
      "w2",
    ]);
    // Asked without `preview`, the same day opens the fit assistant, receipt included.
    const asked = runDayOptimizer(
      DAY,
      { mode: "day", entries, start: at("east-kiosk", EAST) },
      late,
    );
    expect(asked.status).toBe("needs-fit");
    expect(asked.receipt?.from.attractionSlug).toBe("east-kiosk");
  });

  it("walks an assumed three minutes from a start nobody has placed", () => {
    const answer = plan(
      at("unplaced", 0, { latitude: null, longitude: null }),
      TODAY,
      [entry("w1", "w1", 15 * 60)],
    );
    expect(answer.receipt?.first?.walkMinutes).toBe(3);
  });

  it("answers exactly as before without a start, and off today", () => {
    const without = JSON.stringify(plan());
    expect(without).not.toContain("receipt");
    expect(
      JSON.stringify(
        runDayOptimizer(
          DAY,
          { mode: "day", entries: ENTRIES, preview: true, start: undefined },
          TODAY,
        ),
      ),
    ).toBe(without);
    // A start on another day is not read: there is no "now" to leave it at.
    const future: DayClock = { phase: "future" };
    expect(JSON.stringify(plan(at("east-kiosk", EAST), future))).toBe(
      JSON.stringify(plan(undefined, future)),
    );
  });
});
