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
     * Three rides across 09:00–12:00, plus a 20:00 row.
     *
     * `hour_days` is per (ride, hour) — the days THAT HOUR was measured — while
     * `sample_days` is the ride's measured days across the whole window and is
     * the same for all of its hours. The first version of this fixture set only
     * `sample_days`, low on the 20:00 row, and the projection filtered on it. It
     * passed, and production then drew 21:00–23:00 columns for Europa-Park at
     * 58–68 minutes, because the real query gives every hour of a ride the same
     * `sample_days` and the filter never fired. A fixture that encodes the
     * intended semantics rather than the query's actual output cannot catch that,
     * so these rows now carry both columns exactly as the SQL returns them.
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
        hour_days: 118,
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
        hour_days: 116,
        sample_days: 118,
      })),
      // The park stayed open to 20:00 on four evenings. Same `sample_days` as
      // every other row of a well-measured ride — only `hour_days` says the hour
      // is rare, which is the whole point.
      {
        slug: "voletarium",
        name: "Voletarium",
        land: "Iceland",
        hour_of_day: 20,
        p50: 90,
        p90: 99,
        hour_days: 4,
        sample_days: 120,
      },
    ];

    const routeHourly = (rows: unknown[]) => (sql: string) =>
      Promise.resolve(String(sql).includes("hour_of_day") ? rows : []);

    it("keeps only hours measured on enough days, ascending", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(result.hours).toEqual([9, 10, 11, 12]);
    });

    it("aligns every ride's series with `hours`", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      const vol = result.attractions.find(
        (a) => a.attractionSlug === "voletarium",
      )!;
      expect(vol.p50).toEqual([30, 55, 45, 25]);
      expect(vol.p90).toEqual([40, 70, 60, 35]);
      expect(vol.p50).toHaveLength(result.hours.length);
    });

    it("drops an hour the park was only open for on a handful of evenings", async () => {
      aggregateQuery.mockImplementation(routeHourly(hourRows));
      const result = await service.getParkHourlyProfile(park, 1, 8);
      // 20:00 carries the same window-wide sample_days (120) as every other
      // Voletarium row and would survive any filter that reads that column.
      expect(result.hours).not.toContain(20);
      expect(
        result.attractions.find((a) => a.attractionSlug === "voletarium")!.p50,
      ).not.toContain(90);
    });

    it("gaps a single thin cell rather than printing it beside well-measured ones", async () => {
      aggregateQuery.mockImplementation(
        routeHourly([
          ...hourRows,
          // Wodan reported 09:00 on six days only — the hour stays a column
          // because Voletarium measured it 118 times, but Wodan's cell does not.
          {
            slug: "latecomer",
            name: "Latecomer",
            land: null,
            hour_of_day: 9,
            p50: 99,
            p90: 120,
            hour_days: 6,
            sample_days: 90,
          },
          {
            slug: "latecomer",
            name: "Latecomer",
            land: null,
            hour_of_day: 10,
            p50: 30,
            p90: 40,
            hour_days: 88,
            sample_days: 90,
          },
        ]),
      );
      const result = await service.getParkHourlyProfile(park, 1, 8);
      const late = result.attractions.find(
        (a) => a.attractionSlug === "latecomer",
      )!;
      expect(result.hours).toContain(9);
      expect(late.p50[result.hours.indexOf(9)]).toBeNull();
      expect(late.p50[result.hours.indexOf(10)]).toBe(30);
    });

    /**
     * Two further well-measured rides, so a coverage test has a table big
     * enough to express "one ride out of many" — the production case was one
     * of eight. With the base fixture's two rides, one reporting an hour IS
     * half of them, which the rule lets through by design.
     */
    const extraRides = ["blue-fire", "silver-star"].flatMap((slug) =>
      [
        [9, 18, 24],
        [10, 26, 34],
        [11, 24, 31],
        [12, 20, 27],
      ].map(([h, p50, p90]) => ({
        slug,
        name: slug,
        land: null,
        hour_of_day: h,
        p50,
        p90,
        hour_days: 114,
        sample_days: 117,
      })),
    );

    it("drops an hour that only one ride in the table can fill", async () => {
      // Europa-Park's hotel guests enter at 08:15 through one queue. The hour
      // clears both day-count tests on that ride alone, and shipped a column
      // with seven of eight rows empty.
      aggregateQuery.mockImplementation(
        routeHourly([
          ...hourRows,
          ...extraRides,
          {
            slug: "voletarium",
            name: "Voletarium",
            land: "Iceland",
            hour_of_day: 7,
            p50: 0,
            p90: 2,
            hour_days: 115,
            sample_days: 120,
          },
        ]),
      );
      const result = await service.getParkHourlyProfile(park, 1, 8);
      // One of four rides reports 07:00.
      expect(result.hours).not.toContain(7);
      expect(
        result.attractions.every((a) => a.p50.length === result.hours.length),
      ).toBe(true);
    });

    it("keeps an hour once half the table reports it", async () => {
      aggregateQuery.mockImplementation(
        routeHourly([
          ...hourRows,
          ...extraRides,
          ...["voletarium", "wodan"].map((slug) => ({
            slug,
            name: slug,
            land: null,
            hour_of_day: 8,
            p50: 12,
            p90: 18,
            hour_days: 115,
            sample_days: 120,
          })),
        ]),
      );
      const result = await service.getParkHourlyProfile(park, 1, 8);
      // Two of four — exactly the share the rule admits.
      expect(result.hours).toContain(8);
    });

    it("recomputes peakHour against the trimmed axis", async () => {
      // Voletarium's highest reading sits in an hour only it reports, so that
      // hour is cut — peakHour must move to a column the response still has.
      aggregateQuery.mockImplementation(
        routeHourly([
          ...hourRows,
          ...extraRides,
          {
            slug: "voletarium",
            name: "Voletarium",
            land: "Iceland",
            hour_of_day: 7,
            p50: 200,
            p90: 220,
            hour_days: 115,
            sample_days: 120,
          },
        ]),
      );
      const result = await service.getParkHourlyProfile(park, 1, 8);
      const vol = result.attractions.find(
        (a) => a.attractionSlug === "voletarium",
      )!;
      expect(result.hours).not.toContain(7);
      expect(vol.peakHour).toBe(10);
      expect(result.hours).toContain(vol.peakHour!);
    });

    it("measures an hour against the best-observed hour, not a flat threshold", async () => {
      // A winter-only evening hour: 40 measured days clears the absolute floor
      // of 10 but is well under 40 % of the 118 days the midday hours carry.
      aggregateQuery.mockImplementation(
        routeHourly([
          ...hourRows,
          {
            slug: "voletarium",
            name: "Voletarium",
            land: "Iceland",
            hour_of_day: 19,
            p50: 40,
            p90: 50,
            hour_days: 40,
            sample_days: 120,
          },
        ]),
      );
      const result = await service.getParkHourlyProfile(park, 1, 8);
      expect(result.hours).not.toContain(19);
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
            hour_days: 118,
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
