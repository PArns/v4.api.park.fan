import { NotFoundException } from "@nestjs/common";
import { ModelComparisonService } from "./model-comparison.service";
import { DayOptionRow } from "../../ml/utils/model-comparison-day.util";

const T = (iso: string) => new Date(iso);
const PARK = "11111111-1111-4111-8111-111111111111";
const RIDE = "22222222-2222-4222-8222-222222222222";

/**
 * A non-UTC park: New York, 2026-10-10 (EDT, UTC−4). Published window
 * 10:00–12:00 local = 14:00–16:00 UTC; the 06:05 origin is 10:05 UTC.
 */
function build(
  opts: {
    rideInPark?: boolean;
    history?: unknown[];
    schedule?: unknown[];
    readings?: unknown[];
    planWaits?: (number | null)[];
    timezones?: string[];
    dates?: string[];
    optionRows?: Record<string, Partial<DayOptionRow>[]>;
  } = {},
) {
  const sql: string[] = [];
  const params: unknown[][] = [];
  const curve = (
    surface: number,
    slotMinutes: number,
    waits: (number | null)[],
  ) => ({
    originAt: T("2026-10-10T10:05:00Z"),
    attractionId: RIDE,
    surface,
    targetDate: "2026-10-10",
    parkId: PARK,
    originKind: 1,
    leadDays: 0,
    slotStart: T("2026-10-10T14:00:00Z"),
    slotMinutes,
    waits,
    sources: "c".repeat(waits.length),
    bands: null,
    dayPeak: 40,
    expectedError: null,
    rideQ90: 40,
    isHeadliner: true,
    liveWait: null,
    peakBand: null,
    levelSource: "tft",
  });
  const route = (q: string, p: unknown[] = []): unknown[] => {
    sql.push(q);
    params.push(p);
    if (q.includes("SELECT DISTINCT timezone FROM parks")) {
      return (opts.timezones ?? ["Europe/Berlin"]).map((timezone) => ({
        timezone,
      }));
    }
    if (q.includes("generate_series")) {
      return (opts.dates ?? []).map((d) => ({ d }));
    }
    if (q.includes("WITH rides AS")) {
      return (opts.optionRows?.[p[0] as string] ?? []).map((r) => ({
        parkId: PARK,
        parkName: "Test Park",
        timezone: "America/New_York",
        name: "Ride",
        isHeadliner: false,
        models: "6",
        coverage: "0",
        ...r,
      }));
    }
    if (q.includes("FROM parks WHERE id = $1")) {
      return [{ id: PARK, name: "Test Park", timezone: "America/New_York" }];
    }
    if (q.includes("FROM attractions WHERE id = $1")) {
      return opts.rideInPark === false ? [] : [{ id: RIDE, name: "Coaster" }];
    }
    if (q.includes("FROM forecast_archive_curves")) {
      return [
        curve(2, 60, opts.planWaits ?? [20, 40]), // plan_day, hourly
        curve(1, 15, [25, 25, 25, 30, 30, 35, 35, 35]), // park_hourly
      ];
    }
    if (q.includes("schedule_entries")) {
      return (
        opts.schedule ?? [
          {
            open: T("2026-10-10T14:00:00Z"),
            close: T("2026-10-10T16:00:00Z"),
          },
        ]
      );
    }
    if (q.includes("FROM queue_data")) {
      return (
        opts.readings ?? [
          { ts: T("2026-10-10T13:50:00Z"), status: "OPERATING", wait: 25 },
          { ts: T("2026-10-10T14:40:00Z"), status: "DOWN", wait: null },
          { ts: T("2026-10-10T15:10:00Z"), status: "OPERATING", wait: 35 },
        ]
      );
    }
    if (q.includes("attraction_hourly_history")) return opts.history ?? [];
    return [];
  };
  const connection = {
    transaction: async (fn: (em: unknown) => unknown) =>
      fn({ query: async (q: string, p?: unknown[]) => route(q, p) }),
  };
  const redis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
  };
  const service = new ModelComparisonService(
    { manager: { connection } } as never,
    redis as never,
  );
  return { service, sql, params, redis };
}

describe("ModelComparisonService.getDay", () => {
  it("lays the day out on park-local 15-min slots of the published window", async () => {
    const { service } = build();
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    expect(day.park.timezone).toBe("America/New_York");
    expect(day.slots).toEqual([
      "10:00",
      "10:15",
      "10:30",
      "10:45",
      "11:00",
      "11:15",
      "11:30",
      "11:45",
    ]);
    expect(day.openingHours).toEqual({ open: "10:00", close: "12:00" });
    expect(day.origin).toEqual({
      id: "morning",
      label: "Am Morgen des Tages",
      at: "2026-10-10T10:05:00.000Z",
      localLabel: "10.10. 06:05 Uhr",
    });
  });

  it("keeps truth gaps where the ride was not operating", async () => {
    const { service } = build();
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    // 14:45 and 15:00 UTC fall into the DOWN reading → null.
    expect(day.truth).toEqual([25, 25, 25, null, null, 35, 35, 35]);
  });

  it("expands hourly surfaces to quarter-hours and scores over slots with truth", async () => {
    const { service } = build();
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    const served = day.series.find((s) => s.model === "served")!;
    expect(served.label).toBe("Tagesplaner heute");
    expect(served.values).toEqual([20, 20, 20, 20, 40, 40, 40, 40]);
    expect(day.series.find((s) => s.model === "pcn")!.values).toHaveLength(8);
    // pcn: |25-25|×3 + |35-35|×3 = 0; served: 5×3 + 5×3 = 30 over 6 slots.
    expect(day.scores).toEqual([
      { model: "pcn", label: "PCN", mae: 0, n: 6 },
      { model: "served", label: "Tagesplaner heute", mae: 5, n: 6 },
    ]);
  });

  it("adds the weekday average from the rollup before the origin", async () => {
    const slots = [{ time_slot: "10:00", avgWait: 30, p90: 0, sampleCount: 1 }];
    const { service, sql } = build({
      history: [
        { d: "2026-10-03", slots },
        { d: "2026-09-26", slots },
      ],
    });
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    const wkd = day.series.find((s) => s.model === "wkd")!;
    expect(wkd.label).toBe("Wochentag-Durchschnitt");
    // 10:00 local held for 3 h → every slot of the window.
    expect(wkd.values).toEqual([30, 30, 30, 30, 30, 30, 30, 30]);
    expect(sql.some((q) => q.includes("attraction_hourly_history"))).toBe(true);
  });

  it("404s a ride that is not in the park", async () => {
    const { service } = build({ rideInPark: false });
    await expect(
      service.getDay(PARK, "2026-10-10", RIDE, "morning"),
    ).rejects.toBeInstanceOf(NotFoundException);
  });
});

describe("ModelComparisonService.getDay — time axis", () => {
  it("reports the share of slots with truth", async () => {
    const { service } = build();
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    // 6 of 8 slots of the published window carry truth.
    expect(day.truthCoverage).toBe(0.75);
  });

  it("without published hours, spans the truth padded by an hour", async () => {
    const { service } = build({
      schedule: [],
      readings: [
        { ts: T("2026-10-10T14:10:00Z"), status: "OPERATING", wait: 25 },
        { ts: T("2026-10-10T15:20:00Z"), status: "DOWN", wait: null },
      ],
    });
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    expect(day.openingHours).toBeNull();
    // Truth 10:15–11:00 EDT (14:15–15:00 UTC) → 09:15 … 12:00, not 06:00–03:00.
    expect(day.slots[0]).toBe("09:15");
    expect(day.slots[day.slots.length - 1]).toBe("12:00");
    expect(day.slots).toHaveLength(12);
    expect(day.truth.filter((t) => t !== null)).toEqual([25, 25, 25, 25]);
    expect(day.truthCoverage).toBe(0.333);
  });

  it("without hours or truth, spans the served curve where it has values", async () => {
    const { service } = build({
      schedule: [],
      readings: [],
      planWaits: [20, 40, null],
    });
    const day = await service.getDay(PARK, "2026-10-10", RIDE, "morning");
    expect(day.openingHours).toBeNull();
    expect(day.slots).toEqual([
      "10:00",
      "10:15",
      "10:30",
      "10:45",
      "11:00",
      "11:15",
      "11:30",
      "11:45",
    ]);
    expect(day.truthCoverage).toBe(0);
    expect(day.scores).toEqual([]);
  });
});

describe("ModelComparisonService.getDayOptions", () => {
  const CHICAGO_PARK = "33333333-3333-4333-8333-333333333333";
  const EP = "44444444-4444-4444-8444-444444444444";
  const row = (r: Partial<DayOptionRow>): Partial<DayOptionRow> => r;

  it("lists only days that are over in every park and suggests a ride with truth", async () => {
    // 03:00 UTC on 11.10.: Berlin is on the 11th, Chicago still on the 10th
    // (22:00) — so the 10th is not complete, the 9th is the newest that is.
    const { service, sql, params } = build({
      timezones: ["Europe/Berlin", "America/Chicago"],
      dates: ["2026-10-09", "2026-10-08"],
      optionRows: {
        "2026-10-09": [
          // Sorts first by name, but no OPERATING reading that day.
          row({
            parkId: CHICAGO_PARK,
            parkName: "Adventureland Resort",
            timezone: "America/Chicago",
            attractionId: "a1",
            name: "Draken Falls",
            coverage: 0,
          }),
          row({
            parkId: EP,
            parkName: "Europa-Park",
            timezone: "Europe/Berlin",
            attractionId: "e2",
            name: "Alpenexpress",
            coverage: 0.95,
          }),
          row({
            parkId: EP,
            parkName: "Europa-Park",
            timezone: "Europe/Berlin",
            attractionId: "e1",
            name: "Blue Fire",
            isHeadliner: true,
            coverage: 0.9,
          }),
          row({
            parkId: EP,
            parkName: "Europa-Park",
            timezone: "Europe/Berlin",
            attractionId: "e3",
            name: "Voltron",
            isHeadliner: true,
            coverage: 0.2,
          }),
          // Fewer models: never the suggestion, however well covered.
          row({
            parkId: PARK,
            parkName: "Aaa Park",
            attractionId: "t1",
            name: "Coaster",
            isHeadliner: true,
            models: 3,
            coverage: 1,
          }),
        ],
      },
    });
    const opts = await service.getDayOptions(
      undefined,
      T("2026-10-11T03:00:00Z"),
    );
    const dateQuery = sql.findIndex((q) => q.includes("generate_series"));
    expect(params[dateQuery][0]).toBe("2026-10-09");
    expect(opts.date).toBe("2026-10-09");
    expect(opts.dates).toEqual(["2026-10-09", "2026-10-08"]);
    expect(opts.suggested).toEqual({
      date: "2026-10-09",
      parkId: EP,
      attractionId: "e1",
    });
    // Richest park first; the ride without truth (and its park) is gone.
    expect(opts.parks.map((p) => p.name)).toEqual(["Europa-Park", "Aaa Park"]);
    expect(opts.parks[0].rides.map((r) => r.name)).toEqual([
      "Blue Fire",
      "Voltron",
      "Alpenexpress",
    ]);
  });

  it("moves on to the next complete day when the newest has no ride with truth", async () => {
    const { service } = build({
      timezones: ["America/Chicago"],
      dates: ["2026-10-10", "2026-10-09"],
      optionRows: {
        "2026-10-10": [row({ attractionId: "x", coverage: 0.1 })],
        "2026-10-09": [row({ attractionId: "y", coverage: 0.8 })],
      },
    });
    const opts = await service.getDayOptions(
      undefined,
      T("2026-10-11T12:00:00Z"),
    );
    expect(opts.date).toBe("2026-10-09");
    expect(opts.suggested).toEqual({
      date: "2026-10-09",
      parkId: PARK,
      attractionId: "y",
    });
  });

  it("describes an explicitly requested date, even an incomplete one", async () => {
    const { service } = build({
      timezones: ["America/Chicago"],
      dates: ["2026-10-09"],
      optionRows: { "2026-10-11": [row({ attractionId: "z", coverage: 0.3 })] },
    });
    const opts = await service.getDayOptions(
      "2026-10-11",
      T("2026-10-11T12:00:00Z"),
    );
    expect(opts.date).toBe("2026-10-11");
    expect(opts.parks[0].rides.map((r) => r.id)).toEqual(["z"]);
    expect(opts.suggested).toBeNull();
  });
});

describe("ModelComparisonService.getMatrix", () => {
  it("builds the matrix from the paired rows of the scored window, cached per scoring run", async () => {
    const { service, sql, redis } = build();
    const res = await service.getMatrix(28, "EU", T("2026-10-15T08:00:00Z"));
    expect(res.questions).toHaveLength(10);
    expect(res.window.days).toBe(28);
    expect(redis.set).toHaveBeenCalledWith(
      "model-comparison:v0:EU:28",
      expect.any(String),
      "EX",
      7200,
    );
    expect(sql.some((q) => q.includes("FROM forecast_archive_scores"))).toBe(
      true,
    );
  });
});
