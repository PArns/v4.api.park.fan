import { runDayOptimizer } from "./engine";
import type { OptimizerDay, OptimizerRide, PlannerEntry } from "./types";

/**
 * The request layer over the ported engine: which refusal wins, what a press answers, and the
 * entries a caller without the browser's store can write back. The engine's own answers are held
 * to the browser's in `browser-parity.spec.ts`.
 */

function ride(
  slug: string,
  wait: number,
  extra: Partial<OptimizerRide> = {},
): OptimizerRide {
  const hours = [];
  for (let hour = 9; hour <= 18; hour++) hours.push({ hour, wait });
  return {
    attractionSlug: slug,
    attractionName: slug.toUpperCase(),
    hours,
    dayPeak: wait,
    sampleDays: 90,
    latitude: 50.8,
    longitude: 6.87,
    land: "A",
    uncertaintyMinutes: null,
    isHeadliner: false,
    ...extra,
  };
}

function day(rides: OptimizerRide[], context = {}): OptimizerDay {
  return {
    tier: "measured",
    context: {
      openHour: 9,
      closeHour: 18,
      liveWaitTimes: { available: true },
      ...context,
    },
    rides,
    shows: [],
  };
}

const entry = (
  id: string,
  slug: string,
  startMinute: number,
  extra: Partial<PlannerEntry> = {},
): PlannerEntry => ({
  id,
  attractionSlug: slug,
  attractionName: slug.toUpperCase(),
  startMinute,
  ...extra,
});

const FUTURE = { phase: "future" } as const;

describe("runDayOptimizer", () => {
  const payload = day([
    ride("morning", 0, {
      hours: Array.from({ length: 10 }, (_, i) => ({
        hour: 9 + i,
        wait: 9 + i < 12 ? 5 : 60,
      })),
    }),
    ride("steady", 20),
    ride("star", 30, { isHeadliner: true, minimumHeight: 120 }),
    ride("splash", 25, { isHeadliner: true, mayGetWet: true }),
  ]);

  it("refuses a walked day before anything else", () => {
    const answer = runDayOptimizer(
      null,
      { mode: "day", entries: [] },
      {
        phase: "past",
      },
    );
    expect(answer.status).toBe("refused");
    expect(answer.refusal).toBe("past-day");
  });

  it.each([
    ["no-plan", null],
    ["no-hours", day([ride("a", 10)], { openHour: null })],
    [
      "no-wait-times",
      day([ride("a", 10)], {
        liveWaitTimes: { available: false, reason: "not_published" },
      }),
    ],
    ["no-rides", day([])],
  ] as const)("refuses with %s", (refusal, input) => {
    const answer = runDayOptimizer(input, { mode: "day", entries: [] }, FUTURE);
    expect(answer).toMatchObject({ status: "refused", refusal });
  });

  it("moves a ride into the hours its queue is short", () => {
    const entries = [
      entry("s", "steady", 9 * 60 + 15),
      entry("m", "morning", 13 * 60),
    ];
    const answer = runDayOptimizer(payload, { mode: "day", entries }, FUTURE);
    expect(answer.status).toBe("optimized");
    const morning = answer.plan?.stops.find((stop) => stop.entryId === "m");
    expect(morning?.startMinute).toBeLessThan(12 * 60);
    expect(answer.before?.totalWaitMinutes).toBe(80);
    expect(answer.after?.totalWaitMinutes).toBe(25);
    // Same ids back with the plan's minutes, in start order.
    const back = answer.entries ?? [];
    expect(back.map((e) => e.id).sort()).toEqual(["m", "s"]);
    expect(back.find((e) => e.id === "m")?.startMinute).toBe(
      morning?.startMinute,
    );
    expect(back[0].startMinute).toBeLessThanOrEqual(back[1].startMinute);
  });

  it("answers unchanged where the day is already the plan", () => {
    const entries = [
      entry("s", "steady", 9 * 60 + 15),
      entry("m", "morning", 13 * 60),
    ];
    const first = runDayOptimizer(payload, { mode: "day", entries }, FUTURE);
    const again = runDayOptimizer(
      payload,
      { mode: "day", entries: first.entries as PlannerEntry[] },
      FUTURE,
    );
    expect(again.status).toBe("unchanged");
    expect(again.plan).toBeNull();
  });

  it("adds the headliners this party can ride, and counts the others", () => {
    const answer = runDayOptimizer(
      payload,
      {
        mode: "headliners",
        entries: [entry("s", "steady", 10 * 60)],
        prefs: { riderHeightCm: 110, avoidWet: true },
      },
      FUTURE,
    );
    expect(answer.add).toEqual([]);
    expect(answer.skippedHeadliners).toBe(2);

    const tall = runDayOptimizer(
      payload,
      { mode: "headliners", entries: [entry("s", "steady", 10 * 60)] },
      FUTURE,
    );
    expect(tall.add).toEqual(["star", "splash"]);
    expect(tall.status).toBe("optimized");
    const added = tall.entries?.filter((e) => e.added) ?? [];
    expect(added.map((e) => e.attractionSlug).sort()).toEqual([
      "splash",
      "star",
    ]);
    expect(added.every((e) => e.id === null)).toBe(true);
  });

  it("asks the fit question where the day cannot hold what was asked", () => {
    const short = day(
      [ride("a", 60), ride("b", 60), ride("c", 60), ride("d", 60)],
      { closeHour: 12 },
    );
    const entries = ["a", "b", "c", "d"].map((slug, i) =>
      entry(slug, slug, 9 * 60 + i * 15),
    );
    const press = runDayOptimizer(short, { mode: "day", entries }, FUTURE);
    expect(press.status).toBe("needs-fit");
    expect(press.fit?.wanted).toBe(4);
    expect(press.fit?.fitted.length).toBeLessThan(4);
    expect(press.fit?.levers).toEqual([]);

    // The button's promise is computed without the question.
    const preview = runDayOptimizer(
      short,
      { mode: "day", entries, preview: true },
      FUTURE,
    );
    expect(preview.status).not.toBe("needs-fit");
    expect(preview.fit).toBeNull();
  });

  it("answers the assistant for one choice", () => {
    const short = day(
      [ride("a", 60), ride("b", 60), ride("c", 60), ride("d", 60)],
      { closeHour: 12 },
    );
    const entries = [
      ...["a", "b", "c", "d"].map((slug, i) =>
        entry(slug, slug, 9 * 60 + i * 15),
      ),
      {
        id: "lunch",
        custom: { label: "Lunch", icon: "food" as const, durationMinutes: 60 },
        startMinute: 10 * 60,
      },
    ];
    const answer = runDayOptimizer(
      short,
      {
        mode: "fit",
        entries,
        choice: { dropped: ["e:d"], droppedBlocks: ["lunch"] },
        levers: true,
      },
      FUTURE,
    );
    expect(answer.status).toBe("fit");
    expect(answer.fit?.wanted).toBe(3);
    expect(answer.fit?.entries.some((e) => e.id === "lunch")).toBe(false);
    expect(answer.fit?.applied).toEqual(["drop-block:lunch"]);
    expect(answer.fit?.blocks.map((b) => b.entryId)).toEqual(["lunch"]);
  });

  it("offers no show as a lever: its minute is the performance's", () => {
    const answer = runDayOptimizer(
      payload,
      {
        mode: "fit",
        entries: [
          entry("s", "steady", 10 * 60),
          {
            id: "parade",
            showSlug: "parade",
            custom: { label: "Parade", icon: "show", durationMinutes: 30 },
            startMinute: 15 * 60,
          },
        ],
        levers: true,
      },
      FUTURE,
    );
    expect(answer.fit?.blocks).toEqual([]);
  });

  it("refuses to add a ride the day does not list", () => {
    const answer = runDayOptimizer(
      payload,
      { mode: "fit", entries: [], add: ["nope"] },
      FUTURE,
    );
    expect(answer).toMatchObject({
      status: "refused",
      refusal: "unknown-ride",
    });
  });

  it("plans nothing before now on today's day", () => {
    const answer = runDayOptimizer(
      payload,
      {
        mode: "day",
        entries: [
          entry("s", "steady", 9 * 60),
          entry("m", "morning", 9 * 60 + 30),
        ],
      },
      { phase: "today", nowMinute: 11 * 60 + 5 },
    );
    expect(answer.movable).toBe(0);
    expect(answer.status).toBe("unchanged");
  });
});
