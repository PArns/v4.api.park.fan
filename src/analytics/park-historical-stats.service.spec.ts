import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkHistoricalStatsService } from "./park-historical-stats.service";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "../parks/entities/park.entity";

/**
 * Covers the v3 historical-stats contract (headliner-only, queue_data_aggregates):
 * - avgCrowdLevel is occupancy-relative (period peak ÷ typical-day-peak), where
 *   typical-day-peak = the MEDIAN over operating days of the day_value
 *   (AVG-of-headliner daily peaks) — same source as the numerator, so a typical
 *   day ≈ 100% = moderate.
 * - rank, windowYears, displayable, generatedAt, schemaVersion, topN.
 * - v3: `topAttractions` needs a sample floor before a ride may be ranked, and
 *   carries the ride's land and type so a caller does not need a second fetch.
 * - the hourly profile: which hours become columns, how a ride's series is
 *   aligned to them, and why the ranking is by peak hour rather than average.
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
    expect(result.meta.schemaVersion).toBe(3);
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

  it("passes topN and the attraction sample floor to the top-attractions query", async () => {
    await service.getParkHistoricalStats(park, 2, 25);
    const topCall = aggregateQuery.mock.calls.find((c) =>
      String(c[0]).includes("ORDER BY avg_p90 DESC"),
    )!;
    const params = topCall[1] as unknown[];
    expect(params[3]).toBe(25);
    // Ranking by average P90 makes a one-day average outrank a season's worth
    // of them — Walibi Hollands Sky Diver led its park's table off a single
    // measured day. $5 is the floor that keeps it out.
    expect(params[4]).toBe(20);
    expect(String(topCall[0])).toContain(
      "HAVING COUNT(DISTINCT DATE(qda.hour)) >= $5",
    );
  });

  it("reports the attraction sample floor it used in meta", async () => {
    const result = await service.getParkHistoricalStats(park, 2, 10, 30, 45);
    expect(result.meta.minAttractionDays).toBe(45);
  });

  it("carries land and type through, curated value winning", async () => {
    aggregateQuery.mockImplementation(
      route(dayRows, headlinerRows, [
        {
          slug: "blue-fire",
          name: "Blue Fire",
          land: "Iceland",
          attraction_type: "Roller Coaster",
          avg_p50: 38,
          avg_p90: 68,
          sample_days: 120,
        },
      ]),
    );
    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.topAttractions[0].land).toBe("Iceland");
    expect(result.topAttractions[0].attractionType).toBe("Roller Coaster");
    expect(
      String(aggregateQuery.mock.calls[1][0]) +
        String(aggregateQuery.mock.calls[2][0]),
    ).toContain("COALESCE(a.curated_land_name, a.land_name)");
  });

  it("nulls land and type rather than omitting them when the park publishes none", async () => {
    const result = await service.getParkHistoricalStats(park, 2);
    expect(result.topAttractions[0].land).toBeNull();
    expect(result.topAttractions[0].attractionType).toBeNull();
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

  it("keys the cache by park, years, topN, minSampleDays and the attraction floor (v3)", async () => {
    await service.getParkHistoricalStats(park, 2, 10, 30);
    expect(redis.get).toHaveBeenCalledWith(
      "park:historical-stats:v3:park-uuid:2:10:30:20",
    );
  });

  describe("getParkHourlyProfile", () => {
    /**
     * Three rides across 09:00–12:00. `late` only ever reported at 20:00, on
     * four days — the hour a park stayed open for on a handful of August
     * evenings, which must not become a column.
     */
    const hourRows = [
      ...[
        [9, 30, 40],
        [10, 55, 70],
        [11, 45, 60],
        [12, 25, 35],
      ].map(([h, p50, p90]) => ({
        slug: "voletarium",
        name: "Voletarium",
        land: "Iceland",
        hour_of_day: h,
        p50,
        p90,
        sample_days: 120,
      })),
      ...[
        [9, 20, 26],
        [10, 22, 30],
        [11, 24, 33],
        [12, 26, 36],
      ].map(([h, p50, p90]) => ({
        slug: "wodan",
        name: "Wodan",
        land: null,
        hour_of_day: h,
        p50,
        p90,
        sample_days: 118,
      })),
      {
        slug: "late-show",
        name: "Late Show",
        land: null,
        hour_of_day: 20,
        p50: 90,
        p90: 99,
        sample_days: 4,
      },
    ];

    const routeHourly = (rows: unknown[]) => (sql: string) =>
      Promise.resolve(String(sql).includes("hour_of_day") ? rows : []);

    it("keeps only hours measured on enough days, ascending", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(result.hours).toEqual([9, 10, 11, 12]);
    });

    it("aligns every ride's series with `hours` and gaps the missing cells", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      const vol = result.attractions.find(
        (a) => a.attractionSlug === "voletarium",
      )!;
      expect(vol.p50).toEqual([30, 55, 45, 25]);
      expect(vol.p90).toEqual([40, 70, 60, 35]);
      // Present in the rows, but with no cell inside the surviving hours.
      const late = result.attractions.find(
        (a) => a.attractionSlug === "late-show",
      );
      expect(late).toBeUndefined();
    });

    it("names the hour a ride actually peaks at", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(
        result.attractions.find((a) => a.attractionSlug === "voletarium")!
          .peakHour,
      ).toBe(10);
      // Wodan climbs all day — its peak is the last hour, not the busiest one
      // in the park.
      expect(
        result.attractions.find((a) => a.attractionSlug === "wodan")!.peakHour,
      ).toBe(12);
    });

    it("ranks by the busiest hour, not the all-day average", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      // Wodan's all-day average (23) beats nothing, but Voletarium's 10:00
      // spike (55) is what the table is about.
      expect(result.attractions.map((a) => a.attractionSlug)).toEqual([
        "voletarium",
        "wodan",
      ]);
    });

    it("refuses to be displayable when fewer than three hours survive", async () => {
      aggregateQuery.mockImplementation(
        routeHourly([
          {
            slug: "voletarium",
            name: "Voletarium",
            land: null,
            hour_of_day: 10,
            p50: 30,
            p90: 40,
            sample_days: 120,
          },
        ]),
      );
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(result.meta.displayable).toBe(false);
    });

    it("returns an empty, non-displayable table rather than throwing on no data", async () => {
      aggregateQuery.mockImplementation(routeHourly([]));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(result.hours).toEqual([]);
      expect(result.attractions).toEqual([]);
      expect(result.meta.displayable).toBe(false);
    });

    it("reads the hour bucket in the park's timezone, never UTC", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      await service.getParkHourlyProfile(park, 1, 8);
      const call = aggregateQuery.mock.calls.find((c) =>
        String(c[0]).includes("hour_of_day"),
      )!;
      expect(String(call[0])).toContain(
        "EXTRACT(HOUR FROM (qda.hour AT TIME ZONE $2))",
      );
      expect((call[1] as unknown[])[1]).toBe("Europe/Berlin");
    });

    it("over-fetches so a rope-drop spike is not cut before the peak re-rank", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      await service.getParkHourlyProfile(park, 1, 8);
      const call = aggregateQuery.mock.calls.find((c) =>
        String(c[0]).includes("hour_of_day"),
      )!;
      const params = call[1] as unknown[];
      expect(params[params.length - 1]).toBe(24); // 8 × 3
    });

    it("serves cached results without recomputing", async () => {
      const cached = { hours: [], attractions: [], meta: {} };
      redis.get.mockResolvedValueOnce(JSON.stringify(cached));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(result).toEqual(cached);
      expect(aggregateQuery).not.toHaveBeenCalled();
    });
  });
});
