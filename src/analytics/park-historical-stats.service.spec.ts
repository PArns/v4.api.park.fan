import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkHistoricalStatsService } from "./park-historical-stats.service";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";

/**
 * Covers the v2 historical-stats contract:
 * - avgCrowdLevel is occupancy-relative (period peak ÷ typical-day-peak),
 *   using the same 6-tier thresholds as the live endpoint.
 * - rank, windowYears, displayable, generatedAt, schemaVersion, topN.
 */
describe("ParkHistoricalStatsService", () => {
  let service: ParkHistoricalStatsService;
  let redis: { get: jest.Mock; set: jest.Mock };
  let dailyQuery: jest.Mock;
  let aggregateQuery: jest.Mock;

  // Baseline (median daily peak) = 40 min, so occupancy% = p90 / 40 * 100.
  // 20→50% very_low · 30→75% low · 40→100% moderate · 56→140% high ·
  // 72→180% very_high · 100→250% extreme.
  const TYPICAL_DAY_PEAK = 40;

  const monthRows = [
    { month: 1, avg_wait_p50: 10, avg_wait_p90: 20, sample_days: 20 }, // very_low
    { month: 7, avg_wait_p50: 40, avg_wait_p90: 100, sample_days: 25 }, // extreme
  ];
  const dowRows = [
    { day_of_week: 0, avg_wait_p50: 25, avg_wait_p90: 56, sample_days: 30 }, // high
  ];
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

  const park = {
    id: "park-uuid",
    slug: "europa-park",
    timezone: "Europe/Berlin",
  } as Park;

  beforeEach(async () => {
    redis = { get: jest.fn().mockResolvedValue(null), set: jest.fn() };

    // Both queryByMonth/queryByDayOfWeek/queryTypicalDayPeak hit the daily repo;
    // route by inspecting the SQL.
    dailyQuery = jest.fn((sql: string) => {
      if (sql.includes("typical_day_peak")) {
        return Promise.resolve([{ typical_day_peak: TYPICAL_DAY_PEAK }]);
      }
      if (sql.includes("MONTH")) return Promise.resolve(monthRows);
      if (sql.includes("DOW")) return Promise.resolve(dowRows);
      return Promise.resolve([]);
    });
    aggregateQuery = jest.fn().mockResolvedValue(topRows);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkHistoricalStatsService,
        {
          provide: getRepositoryToken(ParkDailyStats),
          useValue: { manager: { query: dailyQuery } },
        },
        {
          provide: getRepositoryToken(QueueDataAggregate),
          useValue: { manager: { query: aggregateQuery } },
        },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(ParkHistoricalStatsService);
  });

  it("derives avgCrowdLevel occupancy-relative to the typical-day-peak baseline", async () => {
    const result = await service.getParkHistoricalStats(park, 2);

    const jan = result.byMonth.find((m) => m.month === 1)!;
    const jul = result.byMonth.find((m) => m.month === 7)!;
    expect(jan.avgCrowdLevel).toBe("very_low"); // 20/40 = 50%
    expect(jul.avgCrowdLevel).toBe("extreme"); //  100/40 = 250%
    expect(result.byDayOfWeek[0].avgCrowdLevel).toBe("high"); // 56/40 = 140%
  });

  it("keeps avgCrowdScore (1.0–5.0, P50-based) for backwards compatibility", async () => {
    const result = await service.getParkHistoricalStats(park, 2);
    const jan = result.byMonth.find((m) => m.month === 1)!;
    expect(jan.avgCrowdScore).toBe(1.0); // 10/10, clamped to >= 1.0
  });

  it("falls back to moderate when the park has no baseline", async () => {
    dailyQuery.mockImplementation((sql: string) => {
      if (sql.includes("typical_day_peak")) {
        return Promise.resolve([{ typical_day_peak: null }]);
      }
      if (sql.includes("MONTH")) return Promise.resolve(monthRows);
      if (sql.includes("DOW")) return Promise.resolve(dowRows);
      return Promise.resolve([]);
    });

    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.byMonth.every((m) => m.avgCrowdLevel === "moderate")).toBe(
      true,
    );
  });

  it("assigns a 1-based rank to top attractions", async () => {
    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.topAttractions.map((a) => a.rank)).toEqual([1, 2]);
    expect(result.topAttractions[0].attractionSlug).toBe("blue-fire");
  });

  it("populates the additive meta fields", async () => {
    const result = await service.getParkHistoricalStats(park, 3);
    expect(result.meta.windowYears).toBe(3);
    expect(result.meta.schemaVersion).toBe(2);
    expect(result.meta.totalSampleDays).toBe(45); // 20 + 25
    expect(result.meta.displayable).toBe(true); // 45 >= 30 (default)
    expect(() => new Date(result.meta.generatedAt).toISOString()).not.toThrow();
    expect(result.meta.parkSlug).toBe("europa-park");
  });

  it("marks displayable=false when below minSampleDays", async () => {
    const result = await service.getParkHistoricalStats(park, 2, 10, 100);
    expect(result.meta.displayable).toBe(false); // 45 < 100
  });

  it("passes topN through to the top-attractions query", async () => {
    await service.getParkHistoricalStats(park, 2, 25);
    const params = aggregateQuery.mock.calls[0][1] as unknown[];
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
    expect(dailyQuery).not.toHaveBeenCalled();
    expect(aggregateQuery).not.toHaveBeenCalled();
  });

  it("keys the cache by park, years, topN and minSampleDays (v2)", async () => {
    await service.getParkHistoricalStats(park, 2, 10, 30);
    expect(redis.get).toHaveBeenCalledWith(
      "park:historical-stats:v2:park-uuid:2:10:30",
    );
  });
});
