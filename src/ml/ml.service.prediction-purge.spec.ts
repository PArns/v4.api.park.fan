import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ConfigService } from "@nestjs/config";
import { MLService } from "./ml.service";
import { WaitTimePrediction } from "./entities/wait-time-prediction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * The windowed hourly purge.
 *
 * `wait_time_predictions` is partitioned on `createdAt`, so pruning on that
 * column lets Timescale skip whole chunks — the old `predictedTime` predicate
 * forced every chunk open. These tests pin the window arithmetic, because the
 * failure modes are silent: too wide a window and a run decompresses far more
 * than intended, too narrow and the backlog never clears.
 */
describe("MLService — windowed hourly prediction purge", () => {
  let service: MLService;
  let queries: Array<{ sql: string; params: unknown[] }>;
  let oldestCreatedAt: Date | null;

  const DAY = 24 * 60 * 60 * 1000;

  beforeEach(async () => {
    queries = [];
    oldestCreatedAt = null;

    const runQuery = (sql: string, params: unknown[] = []) => {
      queries.push({ sql, params });
      if (sql.includes("MIN(")) {
        return Promise.resolve([{ oldest: oldestCreatedAt }]);
      }
      if (sql.includes("DELETE FROM wait_time_predictions")) {
        return Promise.resolve([{ affected: "100" }]);
      }
      return Promise.resolve([]);
    };

    const manager = {
      query: jest.fn(runQuery),
      transaction: jest.fn((cb: (em: unknown) => Promise<unknown>) =>
        cb({ query: jest.fn(runQuery) }),
      ),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLService,
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: { manager },
        },
        { provide: getRepositoryToken(QueueData), useValue: {} },
        { provide: getRepositoryToken(Park), useValue: {} },
        { provide: getRepositoryToken(Attraction), useValue: {} },
        { provide: getRepositoryToken(ScheduleEntry), useValue: {} },
        { provide: ConfigService, useValue: { get: jest.fn() } },
        { provide: PredictionAccuracyService, useValue: {} },
        { provide: WeatherService, useValue: {} },
        { provide: AnalyticsService, useValue: {} },
        { provide: HolidaysService, useValue: {} },
        { provide: ParksService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: {} },
      ],
    }).compile();

    service = module.get<MLService>(MLService);
  });

  const deleteCalls = () =>
    queries.filter((q) => q.sql.includes("DELETE FROM wait_time_predictions"));

  it("is a no-op when there is nothing older than the cutoff", async () => {
    oldestCreatedAt = new Date("2026-07-20T00:00:00Z");

    const result = await service.purgeHourlyPredictionsBefore(
      new Date("2026-07-10T00:00:00Z"),
    );

    expect(result).toEqual({ deleted: 0, windows: 0, done: true });
    expect(deleteCalls()).toHaveLength(0);
  });

  it("is a no-op when the table holds no hourly rows at all", async () => {
    oldestCreatedAt = null;

    const result = await service.purgeHourlyPredictionsBefore(new Date());

    expect(result.done).toBe(true);
    expect(deleteCalls()).toHaveLength(0);
  });

  it("prunes on createdAt — never on predictedTime", async () => {
    oldestCreatedAt = new Date("2026-07-01T00:00:00Z");

    await service.purgeHourlyPredictionsBefore(
      new Date("2026-07-03T00:00:00Z"),
    );

    for (const call of deleteCalls()) {
      expect(call.sql).toContain('"createdAt" >=');
      expect(call.sql).toContain('"createdAt" <');
      expect(call.sql).not.toContain("predictedTime");
      expect(call.sql).toContain("'hourly'");
    }
  });

  it("walks one day-window per statement, oldest first and without gaps", async () => {
    oldestCreatedAt = new Date("2026-07-01T13:37:00Z"); // mid-day start

    const result = await service.purgeHourlyPredictionsBefore(
      new Date("2026-07-04T00:00:00Z"),
    );

    const calls = deleteCalls();
    expect(calls).toHaveLength(3);
    expect(result.windows).toBe(3);
    expect(result.done).toBe(true);

    // First window starts at the UTC day boundary, not at 13:37, so windows
    // line up run over run.
    expect((calls[0].params[0] as Date).toISOString()).toBe(
      "2026-07-01T00:00:00.000Z",
    );

    // Each window is exactly one day and starts where the previous ended.
    for (let i = 0; i < calls.length; i++) {
      const [start, end] = calls[i].params as [Date, Date];
      expect(end.getTime() - start.getTime()).toBe(DAY);
      if (i > 0) {
        const prevEnd = (deleteCalls()[i - 1].params as [Date, Date])[1];
        expect(start.getTime()).toBe(prevEnd.getTime());
      }
    }
  });

  it("never deletes past the cutoff, even mid-window", async () => {
    oldestCreatedAt = new Date("2026-07-01T00:00:00Z");
    const cutoff = new Date("2026-07-02T06:00:00Z"); // not on a day boundary

    await service.purgeHourlyPredictionsBefore(cutoff);

    const calls = deleteCalls();
    const lastEnd = (calls[calls.length - 1].params as [Date, Date])[1];
    expect(lastEnd.getTime()).toBe(cutoff.getTime());
    for (const call of calls) {
      const [, end] = call.params as [Date, Date];
      expect(end.getTime()).toBeLessThanOrEqual(cutoff.getTime());
    }
  });

  it("stops at maxWindows and reports the backlog as unfinished", async () => {
    oldestCreatedAt = new Date("2026-04-01T00:00:00Z"); // a long backlog

    const result = await service.purgeHourlyPredictionsBefore(
      new Date("2026-07-01T00:00:00Z"),
      { maxWindows: 5 },
    );

    expect(deleteCalls()).toHaveLength(5);
    expect(result.windows).toBe(5);
    expect(result.deleted).toBe(500); // 5 windows × 100 rows
    // The caller must be able to tell that work remains.
    expect(result.done).toBe(false);
  });

  it("lifts the Timescale decompression limit inside each window transaction", async () => {
    oldestCreatedAt = new Date("2026-07-01T00:00:00Z");

    await service.purgeHourlyPredictionsBefore(
      new Date("2026-07-02T00:00:00Z"),
    );

    // Compressed chunks abort a DELETE past 100k decompressed tuples;
    // the guard must be lifted per transaction, not globally.
    const guards = queries.filter((q) =>
      q.sql.includes("max_tuples_decompressed_per_dml_transaction"),
    );
    expect(guards).toHaveLength(1);
    expect(guards[0].sql).toContain("SET LOCAL");
  });

  it("sums deleted rows across windows", async () => {
    oldestCreatedAt = new Date("2026-07-01T00:00:00Z");

    const result = await service.purgeHourlyPredictionsBefore(
      new Date("2026-07-04T00:00:00Z"),
    );

    expect(result.deleted).toBe(300); // 3 windows × 100
  });
});
