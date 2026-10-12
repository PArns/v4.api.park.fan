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
import { ForecastAccuracyService } from "./services/forecast-accuracy.service";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * The 90-day chunk drop that replaced TimescaleDB retention job 1007.
 *
 * `drop_chunks` locks `attractions` ACCESS EXCLUSIVE (the hypertable has a
 * foreign key to it) before it looks for anything to drop, and every reader of
 * `attractions` queues behind a waiting request. What these tests pin is the
 * two properties that keep that from becoming an outage: the drop is not
 * attempted when no chunk is due, and when it is, it runs under a
 * transaction-local lock_timeout and treats the timeout as "try tomorrow".
 */
describe("MLService — prediction chunk retention", () => {
  let service: MLService;
  let outer: string[];
  let outerParams: unknown[][];
  let inner: Array<{ sql: string; params: unknown[] }>;
  let dueChunks: Array<{ chunk: string }>;
  /** show_chunks answer for the overdue probe (`older_than` 104 days). */
  let overdueChunks: Array<{ chunk: string }>;
  /** Consumed one per drop_chunks call; null = that attempt succeeds. */
  let dropErrors: Array<(Error & { code?: string }) | null>;

  const lockTimeout = () =>
    Object.assign(new Error("canceling statement due to lock timeout"), {
      code: "55P03",
    });
  const dropCalls = () => inner.filter((q) => q.sql.includes("drop_chunks"));

  beforeEach(async () => {
    outer = [];
    outerParams = [];
    inner = [];
    dueChunks = [];
    overdueChunks = [];
    dropErrors = [];

    const manager = {
      query: jest.fn((sql: string, params: unknown[] = []) => {
        outer.push(sql);
        outerParams.push(params);
        if (sql.includes("show_chunks")) {
          return Promise.resolve(
            params[0] === "90 days" ? dueChunks : overdueChunks,
          );
        }
        return Promise.resolve([]);
      }),
      transaction: jest.fn((cb: (em: unknown) => Promise<unknown>) =>
        cb({
          query: jest.fn((sql: string, params: unknown[] = []) => {
            inner.push({ sql, params });
            if (sql.includes("drop_chunks")) {
              const error = dropErrors.shift();
              if (error) return Promise.reject(error);
              return Promise.resolve(dueChunks);
            }
            return Promise.resolve([]);
          }),
        }),
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
        {
          provide: ForecastAccuracyService,
          useValue: { getProfile: jest.fn().mockResolvedValue(new Map()) },
        },
        { provide: WeatherService, useValue: {} },
        { provide: AnalyticsService, useValue: {} },
        { provide: HolidaysService, useValue: {} },
        { provide: ParksService, useValue: {} },
        { provide: REDIS_CLIENT, useValue: {} },
      ],
    }).compile();

    service = module.get<MLService>(MLService);
  });

  const CHUNK = { chunk: "_timescaledb_internal._hyper_16_1229_chunk" };
  // Retries without waiting; the spacing itself is a plain setTimeout.
  const fast = { retryDelayMs: 0 };

  it("does not call drop_chunks when no chunk is due — that call alone locks attractions", async () => {
    const result = await service.dropExpiredPredictionChunks(90);

    expect(result).toEqual({
      due: 0,
      dropped: 0,
      lockTimedOut: false,
      attempts: 0,
      overdue: 0,
    });
    expect(outer).toHaveLength(1);
    expect(outer[0]).toContain("show_chunks('wait_time_predictions'");
    expect(inner).toHaveLength(0);
  });

  it("sets a transaction-local lock_timeout before dropping", async () => {
    dueChunks = [CHUNK];

    const result = await service.dropExpiredPredictionChunks(90, {
      lockTimeoutMs: 1500,
    });

    expect(result).toEqual({
      due: 1,
      dropped: 1,
      lockTimedOut: false,
      attempts: 1,
      overdue: 0,
    });
    expect(inner).toHaveLength(2);
    // set_config(..., true) is SET LOCAL — it must not outlive the transaction
    // on a pooled connection.
    expect(inner[0].sql).toContain("set_config('lock_timeout', $1, true)");
    expect(inner[0].params).toEqual(["1500ms"]);
    expect(inner[1].sql).toContain("drop_chunks('wait_time_predictions'");
    expect(inner[1].params).toEqual(["90 days"]);
  });

  it("retries after a lock timeout and succeeds once the lock is free", async () => {
    dueChunks = [CHUNK];
    dropErrors = [lockTimeout(), lockTimeout(), null];

    const result = await service.dropExpiredPredictionChunks(90, fast);

    expect(result).toEqual({
      due: 1,
      dropped: 1,
      lockTimedOut: false,
      attempts: 3,
      overdue: 0,
    });
    expect(dropCalls()).toHaveLength(3);
    // Every attempt sets its own short lock_timeout — none of them waits long.
    const timeouts = inner.filter((q) => q.sql.includes("set_config"));
    expect(timeouts).toHaveLength(3);
    expect(timeouts.every((q) => q.params[0] === "2000ms")).toBe(true);
  });

  it("waits retryDelayMs between attempts", async () => {
    jest.useFakeTimers();
    try {
      dueChunks = [CHUNK];
      dropErrors = [lockTimeout(), null];

      const pending = service.dropExpiredPredictionChunks(90, {
        retryDelayMs: 30_000,
      });
      await jest.advanceTimersByTimeAsync(29_999);
      expect(dropCalls()).toHaveLength(1);
      await jest.advanceTimersByTimeAsync(1);
      const result = await pending;

      expect(dropCalls()).toHaveLength(2);
      expect(result.attempts).toBe(2);
    } finally {
      jest.useRealTimers();
    }
  });

  it("gives up after the last attempt without throwing, and counts overdue chunks", async () => {
    dueChunks = [CHUNK];
    overdueChunks = [CHUNK];
    dropErrors = [lockTimeout(), lockTimeout(), lockTimeout(), lockTimeout()];

    const result = await service.dropExpiredPredictionChunks(90, {
      ...fast,
      attempts: 4,
      overdueAfterDays: 14,
    });

    expect(result).toEqual({
      due: 1,
      dropped: 0,
      lockTimedOut: true,
      attempts: 4,
      overdue: 1,
    });
    expect(dropCalls()).toHaveLength(4);
    // The overdue probe looks 90 + 14 days back.
    expect(outerParams[outerParams.length - 1]).toEqual(["104 days"]);
  });

  it("reports zero overdue while the misses are recent", async () => {
    dueChunks = [CHUNK];
    overdueChunks = [];
    dropErrors = [lockTimeout(), lockTimeout()];

    const result = await service.dropExpiredPredictionChunks(90, {
      ...fast,
      attempts: 2,
    });

    expect(result.lockTimedOut).toBe(true);
    expect(result.overdue).toBe(0);
  });

  it("rethrows any other error without retrying", async () => {
    dueChunks = [CHUNK];
    dropErrors = [
      Object.assign(new Error("permission denied"), { code: "42501" }),
    ];

    await expect(service.dropExpiredPredictionChunks(90, fast)).rejects.toThrow(
      "permission denied",
    );
    expect(dropCalls()).toHaveLength(1);
  });
});
