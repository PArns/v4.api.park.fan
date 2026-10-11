import { NotFoundException } from "@nestjs/common";
import { ModelComparisonService } from "./model-comparison.service";

const T = (iso: string) => new Date(iso);
const PARK = "11111111-1111-4111-8111-111111111111";
const RIDE = "22222222-2222-4222-8222-222222222222";

/**
 * A non-UTC park: New York, 2026-10-10 (EDT, UTC−4). Published window
 * 10:00–12:00 local = 14:00–16:00 UTC; the 06:05 origin is 10:05 UTC.
 */
function build(opts: { rideInPark?: boolean; history?: unknown[] } = {}) {
  const sql: string[] = [];
  const curve = (surface: number, slotMinutes: number, waits: number[]) => ({
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
  const route = (q: string): unknown[] => {
    sql.push(q);
    if (q.includes("FROM parks WHERE id = $1")) {
      return [{ id: PARK, name: "Test Park", timezone: "America/New_York" }];
    }
    if (q.includes("FROM attractions WHERE id = $1")) {
      return opts.rideInPark === false ? [] : [{ id: RIDE, name: "Coaster" }];
    }
    if (q.includes("FROM forecast_archive_curves")) {
      return [
        curve(2, 60, [20, 40]), // plan_day, hourly
        curve(1, 15, [25, 25, 25, 30, 30, 35, 35, 35]), // park_hourly
      ];
    }
    if (q.includes("schedule_entries")) {
      return [
        {
          open: T("2026-10-10T14:00:00Z"),
          close: T("2026-10-10T16:00:00Z"),
        },
      ];
    }
    if (q.includes("FROM queue_data")) {
      return [
        { ts: T("2026-10-10T13:50:00Z"), status: "OPERATING", wait: 25 },
        { ts: T("2026-10-10T14:40:00Z"), status: "DOWN", wait: null },
        { ts: T("2026-10-10T15:10:00Z"), status: "OPERATING", wait: 35 },
      ];
    }
    if (q.includes("attraction_hourly_history")) return opts.history ?? [];
    return [];
  };
  const connection = {
    transaction: async (fn: (em: unknown) => unknown) =>
      fn({ query: async (q: string) => route(q) }),
  };
  const redis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
  };
  const service = new ModelComparisonService(
    { manager: { connection } } as never,
    redis as never,
  );
  return { service, sql, redis };
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

describe("ModelComparisonService.daySlots", () => {
  it("floors the opening to the quarter-hour and stops before the close", () => {
    const open = Date.parse("2026-10-10T14:05:00Z");
    const close = Date.parse("2026-10-10T14:40:00Z");
    expect(
      ModelComparisonService.daySlots([{ open, close }], []).map((s) =>
        new Date(s).toISOString().slice(11, 16),
      ),
    ).toEqual(["14:00", "14:15", "14:30"]);
    expect(ModelComparisonService.daySlots([], [])).toEqual([]);
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
