import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkHistoricalStatsService } from "./park-historical-stats.service";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";

/**
 * Covers the v2 historical-stats contract (headliner-only, queue_data_aggregates):
 * - avgCrowdLevel is occupancy-relative (period peak ÷ typical-day-peak), where
 *   typical-day-peak = the MEDIAN over operating days of the day_value
 *   (AVG-of-headliner daily peaks) — same source as the numerator, so a typical
 *   day ≈ 100% = moderate.
 * - rank, windowYears, displayable, generatedAt, schemaVersion, topN.
 */
describe("ParkHistoricalStatsService", () => {
  let service: ParkHistoricalStatsService;
  let redis: { get: jest.Mock; set: jest.Mock };
  let aggregateQuery: jest.Mock;

  const park = {
    id: "park-uuid",
    slug: "europa-park",
    timezone: "Europe/Berlin",
  } as Park;

  // Per-day rows. day_value_p90 set = 15×20, 15×40, 15×100 (45 days) → the
  // median (typical-day-peak) is 40. So 20→50% very_low, 40→100% moderate,
  // 100→250% extreme.
  const makeDays = (
    specs: Array<{
      month: number;
      dow: number;
      p90: number;
      p50: number;
      n: number;
    }>,
  ) =>
    specs.flatMap((s) =>
      Array.from({ length: s.n }, () => ({
        month: s.month,
        dow: s.dow,
        day_value_p90: s.p90,
        day_value_p50: s.p50,
      })),
    );

  const dayRows = makeDays([
    { month: 1, dow: 1, p90: 20, p50: 10, n: 15 }, // very_low
    { month: 4, dow: 0, p90: 40, p50: 20, n: 15 }, // moderate (the median)
    { month: 7, dow: 4, p90: 100, p50: 40, n: 15 }, // extreme
  ]);

  const headlinerRows = [{ id: "a1" }, { id: "a2" }];
  const topRows = [
    {
      slug: "blue-fire",
      name: "Blue Fire",
      avg_p50: 38,
      avg_p90: 68,
      sample_days: 120,
    },
    {
      slug: "wodan",
      name: "Wodan",
      avg_p50: 30,
      avg_p90: 55,
      sample_days: 110,
    },
  ];

  // All three raw queries hit the aggregate repo's manager; route by SQL.
  const route =
    (days: unknown[], headliners: unknown[], tops: unknown[]) =>
    (sql: string) => {
      if (sql.includes("headliner_attractions"))
        return Promise.resolve(headliners);
      if (sql.includes("per_attraction_day")) return Promise.resolve(days);
      if (sql.includes("ORDER BY avg_p90 DESC")) return Promise.resolve(tops);
      return Promise.resolve([]);
    };

  beforeEach(async () => {
    redis = { get: jest.fn().mockResolvedValue(null), set: jest.fn() };
    aggregateQuery = jest.fn(route(dayRows, headlinerRows, topRows));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkHistoricalStatsService,
        {
          provide: getRepositoryToken(QueueDataAggregate),
          useValue: { manager: { query: aggregateQuery } },
        },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(ParkHistoricalStatsService);
  });

  it("derives avgCrowdLevel relative to the typical-day-peak (median of daily peaks)", async () => {
    const result = await service.getParkHistoricalStats(park, 2);

    expect(result.byMonth.find((m) => m.month === 1)!.avgCrowdLevel).toBe(
      "very_low",
    ); // 20/40 = 50%
    expect(result.byMonth.find((m) => m.month === 4)!.avgCrowdLevel).toBe(
      "moderate",
    ); // 40/40 = 100%
    expect(result.byMonth.find((m) => m.month === 7)!.avgCrowdLevel).toBe(
      "extreme",
    ); // 100/40 = 250%
    expect(
      result.byDayOfWeek.find((d) => d.dayOfWeek === 0)!.avgCrowdLevel,
    ).toBe("moderate"); // 40/40 = 100%
  });

  it("reads 'unknown' from toCrowdLevel when there is no typical-day-peak baseline (not ratable)", () => {
    // Private helper: a missing/zero baseline (< 30 operating days) must read
    // "unknown" ("keine Prognose"), not a made-up "moderate".
    const toCrowdLevel = (
      service as unknown as {
        toCrowdLevel: (avgWaitP90: number, typicalDayPeak: number) => string;
      }
    ).toCrowdLevel.bind(service);
    expect(toCrowdLevel(40, 0)).toBe("unknown");
    expect(toCrowdLevel(40, 40)).toBe("moderate");
  });

  it("keeps avgCrowdScore (1.0–5.0, P50-based) for backwards compatibility", async () => {
    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.byMonth.find((m) => m.month === 1)!.avgCrowdScore).toBe(1.0); // 10/10
  });

  it("returns empty sections (not a crash) when there is no headliner data", async () => {
    aggregateQuery.mockImplementation(route([], [], []));

    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.byMonth).toEqual([]);
    expect(result.byDayOfWeek).toEqual([]);
    expect(result.meta.displayable).toBe(false);
  });

  it("assigns a 1-based rank to top attractions", async () => {
    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.topAttractions.map((a) => a.rank)).toEqual([1, 2]);
    expect(result.topAttractions[0].attractionSlug).toBe("blue-fire");
  });

  it("populates meta (totalSampleDays = operating-day count)", async () => {
    const result = await service.getParkHistoricalStats(park, 3);
    expect(result.meta.windowYears).toBe(3);
    expect(result.meta.schemaVersion).toBe(2);
    expect(result.meta.totalSampleDays).toBe(45);
    expect(result.meta.displayable).toBe(true); // 45 >= 30 (default)
    expect(() => new Date(result.meta.generatedAt).toISOString()).not.toThrow();
    expect(result.meta.parkSlug).toBe("europa-park");
  });

  it("marks displayable=false when below minSampleDays", async () => {
    const result = await service.getParkHistoricalStats(park, 2, 10, 100);
    expect(result.meta.displayable).toBe(false); // 45 < 100
  });

  it("restricts the day-values query to the park's headliner IDs", async () => {
    await service.getParkHistoricalStats(park, 2);
    const dayCall = aggregateQuery.mock.calls.find((c) =>
      String(c[0]).includes("per_attraction_day"),
    )!;
    expect(dayCall[1]).toContainEqual(["a1", "a2"]);
  });

  it("passes topN through to the top-attractions query", async () => {
    await service.getParkHistoricalStats(park, 2, 25);
    const topCall = aggregateQuery.mock.calls.find((c) =>
      String(c[0]).includes("ORDER BY avg_p90 DESC"),
    )!;
    const params = topCall[1] as unknown[];
    expect(params[params.length - 1]).toBe(25);
  });

  it("serves cached results without recomputing", async () => {
    const cached = {
      byMonth: [],
      byDayOfWeek: [],
      topAttractions: [],
      meta: {},
    };
    redis.get.mockResolvedValueOnce(JSON.stringify(cached));

    const result = await service.getParkHistoricalStats(park, 2);
    expect(result).toEqual(cached);
    expect(aggregateQuery).not.toHaveBeenCalled();
  });

  it("keys the cache by park, years, topN and minSampleDays (v2)", async () => {
    await service.getParkHistoricalStats(park, 2, 10, 30);
    expect(redis.get).toHaveBeenCalledWith(
      "park:historical-stats:v2:park-uuid:2:10:30",
    );
  });
});
