import { Logger } from "@nestjs/common";
import type { Repository } from "typeorm";
import {
  TRIP_METRICS_MAX_BUFFERED,
  TripRequestMetricsService,
} from "./trip-request-metrics.service";
import type { TripRequestMetric } from "./trip-request-metric.entity";
import { metricRowOf, type TripRequestMetricRow } from "./trip-request-facts";

const row = (latencyMs = 10): TripRequestMetricRow =>
  metricRowOf({
    route: "plan",
    region: "orlando",
    at: new Date("2026-11-14T10:20:00Z"),
    latencyMs,
    trace: { cache: "miss" },
    status: 200,
  });

describe("TripRequestMetricsService", () => {
  const make = (repository: Partial<Record<"insert" | "query", jest.Mock>>) =>
    new TripRequestMetricsService(
      repository as unknown as Repository<TripRequestMetric>,
    );

  beforeEach(() => {
    jest.spyOn(Logger.prototype, "warn").mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  it("holds the rows until the write, then inserts them in chunks of 500", async () => {
    const insert = jest.fn(async (_rows: TripRequestMetricRow[]) => undefined);
    const metrics = make({ insert });
    for (let i = 0; i < 1201; i++) metrics.record(row(i));
    expect(insert).not.toHaveBeenCalled();
    expect(metrics.writerState()).toEqual({ buffered: 1201, dropped: 0 });

    expect(await metrics.flush()).toBe(1201);
    expect(insert.mock.calls.map(([rows]) => rows.length)).toEqual([
      500, 500, 201,
    ]);
    expect(metrics.writerState().buffered).toBe(0);
    expect(await metrics.flush()).toBe(0);
  });

  it("keeps what a failed insert did not take for the next write, and never throws", async () => {
    const insert = jest
      .fn()
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error("connection terminated"))
      .mockResolvedValue(undefined);
    const metrics = make({ insert });
    for (let i = 0; i < 700; i++) metrics.record(row(i));

    expect(await metrics.flush()).toBe(500);
    expect(metrics.writerState().buffered).toBe(200);
    metrics.record(row(9999));
    expect(await metrics.flush()).toBe(201);
    // The rows that waited go first, in their order.
    const written = insert.mock.calls[2][0] as TripRequestMetricRow[];
    expect(written[0].latencyMs).toBe(500);
    expect(written[written.length - 1].latencyMs).toBe(9999);
  });

  it("drops and counts past the cap rather than grow without end", async () => {
    const metrics = make({ insert: jest.fn() });
    for (let i = 0; i < TRIP_METRICS_MAX_BUFFERED + 3; i++)
      metrics.record(row());
    expect(metrics.writerState()).toEqual({
      buffered: TRIP_METRICS_MAX_BUFFERED,
      dropped: 3,
    });
  });

  it("runs one write at a time", async () => {
    let release: () => void = () => undefined;
    const insert = jest.fn(
      () => new Promise<void>((resolve) => (release = resolve)),
    );
    const metrics = make({ insert });
    metrics.record(row());
    const first = metrics.flush();
    const second = metrics.flush();
    release();
    expect(await first).toBe(1);
    expect(await second).toBe(1);
    expect(insert).toHaveBeenCalledTimes(1);
  });

  it("prunes in batches up to the cutoff 90 UTC days back", async () => {
    const query = jest
      .fn()
      .mockResolvedValueOnce([{ n: 10_000 }])
      .mockResolvedValueOnce([{ n: 37 }]);
    const metrics = make({ query });
    expect(await metrics.prune(new Date("2026-11-14T01:25:00Z"))).toBe(10_037);
    expect(query).toHaveBeenCalledTimes(2);
    expect(query.mock.calls[0][1]).toEqual(["2026-08-16T00:00:00.000Z"]);
  });

  it("writes what is left when the module stops", async () => {
    const insert = jest.fn(async () => undefined);
    const metrics = make({ insert });
    metrics.onModuleInit();
    metrics.record(row());
    await metrics.onModuleDestroy();
    expect(insert).toHaveBeenCalledTimes(1);
  });

  it("reads the window's buckets, narrowed by region and route, and shapes the answer", async () => {
    const query = jest.fn(async (sql: string, _params?: unknown[]) => {
      if (sql.includes("GROUP BY region, route")) {
        return [
          {
            region: "orlando",
            route: "plan",
            requests: "10",
            answered: "8",
            all_count: "8",
            all_p50: 12.4,
            all_p90: 400.2,
            all_p99: 900,
            all_max: 950,
            built_count: "3",
            built_p50: 380,
            built_p90: 700.6,
            built_p99: 940,
            built_max: 950,
            hit: "5",
            joined: "0",
            miss: "3",
            calendar_hit: "2",
            calendar_joined: "0",
            calendar_miss: "1",
            searched: "8",
            exact: "7",
            relaxed: "2",
            unplaced: "1",
            hopper: "4",
            owned: "2",
            avg_trip_days: "8.333",
            avg_parks: "4.5",
          },
        ];
      }
      if (sql.includes("unnest(relaxed)")) {
        return [{ route: "plan", code: "blocks_relaxed", requests: "2" }];
      }
      if (sql.includes("GROUPING SETS")) {
        return [
          {
            route: "plan",
            trip_days: 14,
            parks: 6,
            bases: null,
            sides: 0,
            requests: "1",
            p50: 900,
            p90: 900,
            max: 900,
          },
          {
            route: "plan",
            trip_days: 5,
            parks: 3,
            bases: null,
            sides: 0,
            requests: "2",
            p50: 300,
            p90: 380,
            max: 400,
          },
          {
            route: "plan",
            trip_days: 14,
            parks: null,
            bases: null,
            sides: 3,
            requests: "1",
            p50: 900,
            p90: 900,
            max: 900,
          },
          {
            route: "plan",
            trip_days: 5,
            parks: null,
            bases: null,
            sides: 3,
            requests: "2",
            p50: 300,
            p90: 380,
            max: 400,
          },
          {
            route: "plan",
            trip_days: null,
            parks: 3,
            bases: null,
            sides: 5,
            requests: "2",
            p50: 300,
            p90: 380,
            max: 400,
          },
          {
            route: "plan",
            trip_days: null,
            parks: null,
            bases: null,
            sides: 6,
            requests: "3",
            p50: 380,
            p90: 800,
            max: 900,
          },
        ];
      }
      if (sql.includes("status <> 200")) {
        return [
          {
            route: "plan",
            status: 400,
            error_code: "unknown_park",
            count: "2",
          },
        ];
      }
      return [];
    });
    const metrics = make({ query });
    const report = await metrics.ops({
      from: "2026-11-08",
      to: "2026-11-14",
      region: "orlando",
      route: "plan",
      limit: 1,
      series: false,
    });
    expect(query.mock.calls[0][1]).toEqual([
      "2026-11-08T00:00:00.000Z",
      "2026-11-15T00:00:00.000Z",
      "orlando",
      "plan",
    ]);
    expect(query.mock.calls[0][0]).toContain("region = $3");
    expect(query.mock.calls[0][0]).toContain("route = $4");
    expect(report).toMatchObject({
      days: 7,
      totals: { requests: 10, answered: 8, errors: 2 },
      routes: [
        {
          errorRate: 0.2,
          latencyMs: { count: 8, p50: 12, p90: 400, p99: 900, max: 950 },
          builtLatencyMs: { count: 3, p50: 380, p90: 701 },
          cache: { hit: 5, miss: 3, hitRate: 0.625 },
          calendarCache: { hit: 2, miss: 1 },
          exactShare: 0.875,
          relaxedShare: 0.25,
          unplacedShare: 0.125,
          hopperShare: 0.5,
          ownedTicketsShare: 0.25,
          avgTripDays: 8.3,
        },
      ],
      relaxed: [{ code: "blocks_relaxed", requests: 2, share: 0.25 }],
      errors: [{ status: 400, code: "unknown_park", count: 2 }],
      // The slowest whole shape first, cut to the limit.
      slowest: [{ tripDays: 14, parks: 6, requests: 1, p90: 900 }],
      byLength: [{ tripDays: 5 }, { tripDays: 14 }],
      byParks: [{ parks: 3, requests: 2 }],
      byBases: [{ bases: null, requests: 3 }],
      writer: { buffered: 0, dropped: 0 },
    });
    expect(report).not.toHaveProperty("series");
  });
});
