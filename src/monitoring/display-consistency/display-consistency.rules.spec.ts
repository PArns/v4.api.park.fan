import {
  SURFACE_PAIRS,
  compareSurfaces,
  isKnownValue,
  type SurfacePair,
  type SurfaceValue,
} from "./display-consistency.rules";

const DAY = "2026-11-17";

/** A crowd level one surface shows on {@link DAY}. */
const level = (
  surface: SurfaceValue["surface"],
  value: SurfaceValue["value"],
  extra: Partial<SurfaceValue> = {},
): SurfaceValue => ({
  date: DAY,
  surface,
  measure: "crowd_level",
  value,
  ...extra,
});

describe("isKnownValue", () => {
  it("knows a level, closed and a number, and nothing that says nothing", () => {
    expect(isKnownValue("high")).toBe(true);
    expect(isKnownValue("closed")).toBe(true);
    expect(isKnownValue(0)).toBe(true);
    expect(isKnownValue("unknown")).toBe(false);
    expect(isKnownValue(null)).toBe(false);
    expect(isKnownValue(undefined)).toBe(false);
    expect(isKnownValue("")).toBe(false);
    expect(isKnownValue(Number.NaN)).toBe(false);
  });
});

describe("compareSurfaces", () => {
  it("reports two surfaces of one statistic that disagree, naming both and the statistic", () => {
    const result = compareSurfaces([
      level("calendar", "very_low"),
      level("plan_day", "high", { asOf: "2026-11-17T09:40:00.000Z" }),
    ]);
    expect(result.mismatches).toEqual([
      {
        date: DAY,
        measure: "crowd_level",
        a: { surface: "calendar", value: "very_low" },
        b: {
          surface: "plan_day",
          value: "high",
          asOf: "2026-11-17T09:40:00.000Z",
        },
        statistic: expect.stringContaining("PlanDayService.loadDay"),
      },
    ]);
    expect(result.compared).toBe(1);
  });

  it("counts agreeing surfaces as compared and reports nothing", () => {
    const result = compareSurfaces([
      level("calendar", "high"),
      level("best_days", "high"),
      level("plan_day", "high"),
      level("parky_get_days", "high"),
    ]);
    expect(result.mismatches).toEqual([]);
    expect(result.compared).toBe(3);
  });

  it("never counts an unknown or absent value as a mismatch", () => {
    const result = compareSurfaces([
      level("calendar", "high"),
      level("best_days", "unknown"),
      level("plan_day", null),
      level("parky_get_days", "high"),
    ]);
    expect(result.mismatches).toEqual([]);
    expect(result.compared).toBe(1);
  });

  it("puts a pair of different statistics under differences, not mismatches", () => {
    const result = compareSurfaces([
      level("calendar", "high"),
      level("park_live", "moderate"),
      level("calendar_so_far", "low"),
    ]);
    expect(result.mismatches).toEqual([]);
    expect(
      result.differences.map((d) => [d.a.surface, d.b.surface]).sort(),
    ).toEqual([
      ["calendar", "calendar_so_far"],
      ["calendar", "park_live"],
      ["park_live", "calendar_so_far"],
    ]);
  });

  it("reads closed against a level as a mismatch", () => {
    const result = compareSurfaces([
      level("calendar", "closed"),
      level("trip_region", "high"),
    ]);
    expect(result.mismatches.map((m) => m.b.surface)).toEqual(["trip_region"]);
  });

  it("compares only the same day", () => {
    const result = compareSurfaces([
      level("calendar", "high"),
      level("best_days", "low", { date: "2026-11-18" }),
    ]);
    expect(result.compared).toBe(0);
  });

  it("matches keyed values by key, and an unkeyed one against every key", () => {
    const result = compareSurfaces([
      {
        date: DAY,
        surface: "calendar",
        measure: "ride_wait",
        key: "taron",
        value: 25,
      },
      {
        date: DAY,
        surface: "plan_day",
        measure: "ride_wait",
        key: "taron",
        value: 23,
      },
      {
        date: DAY,
        surface: "plan_day",
        measure: "ride_wait",
        key: "chiapas",
        value: 40,
      },
      {
        date: DAY,
        surface: "calendar",
        measure: "ride_wait",
        key: "chiapas",
        value: 30,
      },
    ]);
    // 23 minutes lies on the calendar's 25 once rounded to its grid; Chiapas's 40 does not.
    expect(result.mismatches).toEqual([
      expect.objectContaining({
        measure: "ride_wait",
        key: "chiapas",
        a: { surface: "calendar", value: 30 },
        b: { surface: "plan_day", value: 40 },
      }),
    ]);
    expect(result.compared).toBe(2);
  });

  it("names the edge copy a relay's value came from", () => {
    const copy = { age: 3300, cloudflare: "HIT", vercel: "HIT" };
    const result = compareSurfaces([
      level("calendar", "very_low"),
      level("calendar_relay", "high", { copy }),
      level("best_days_relay", "high", {
        asOf: "2026-11-16T20:05:00.000Z",
        copy: { age: 120 },
      }),
    ]);
    expect(result.mismatches).toEqual([
      expect.objectContaining({
        a: { surface: "calendar", value: "very_low" },
        b: { surface: "calendar_relay", value: "high", copy },
        statistic: expect.stringContaining("edge copy"),
      }),
    ]);
    // The two relays agree with each other: one stale copy, not two.
    expect(result.compared).toBe(2);
  });

  it("compares minutes exactly where neither side rounds differently", () => {
    const result = compareSurfaces([
      {
        date: DAY,
        surface: "calendar",
        measure: "headliner_wait",
        value: 20,
      },
      {
        date: DAY,
        surface: "parky_get_days",
        measure: "headliner_wait",
        value: 25,
      },
    ]);
    expect(result.mismatches).toHaveLength(1);
  });

  it("takes its pairs as an argument", () => {
    const pairs: SurfacePair[] = [
      {
        a: "calendar",
        b: "best_days",
        measure: "crowd_level",
        rule: "may_differ",
        statistic: "test",
      },
    ];
    const result = compareSurfaces(
      [level("calendar", "high"), level("best_days", "low")],
      pairs,
    );
    expect(result.mismatches).toEqual([]);
    expect(result.differences).toHaveLength(1);
  });
});

describe("SURFACE_PAIRS", () => {
  it("names a statistic for every pair and lists each pair once", () => {
    const seen = new Set<string>();
    for (const pair of SURFACE_PAIRS) {
      expect(pair.statistic.length).toBeGreaterThan(10);
      expect(pair.a).not.toBe(pair.b);
      const id = `${pair.measure}:${[pair.a, pair.b].sort().join("|")}`;
      expect(seen.has(id)).toBe(false);
      seen.add(id);
    }
  });

  it("compares no surface with a copy of itself", () => {
    const surfaces = new Set(SURFACE_PAIRS.flatMap((pair) => [pair.a, pair.b]));
    for (const gone of [
      "calendar_predicted",
      "parky_tip",
      "parky_tip_served",
    ]) {
      expect(surfaces.has(gone as never)).toBe(false);
    }
    expect(SURFACE_PAIRS.map((pair) => pair.measure)).not.toContain("verdict");
  });

  it("checks the relays against the API and against each other, as the page shows them", () => {
    const relayPairs = SURFACE_PAIRS.filter(
      (pair) => pair.a.endsWith("_relay") || pair.b.endsWith("_relay"),
    ).map((pair) => [pair.a, pair.b, pair.measure, pair.rule]);
    expect(relayPairs).toEqual([
      ["calendar", "calendar_relay", "crowd_level", "equal"],
      ["calendar", "today_relay", "crowd_level", "equal"],
      ["calendar_relay", "best_days_relay", "crowd_level", "equal"],
      ["calendar", "calendar_relay", "headliner_wait", "equal"],
      ["calendar", "today_relay", "headliner_wait", "equal"],
    ]);
  });

  it("keeps the live level and the day's forecast apart", () => {
    const live = SURFACE_PAIRS.find(
      (p) =>
        p.measure === "crowd_level" &&
        p.a === "calendar" &&
        p.b === "park_live",
    );
    expect(live?.rule).toBe("may_differ");
  });
});
