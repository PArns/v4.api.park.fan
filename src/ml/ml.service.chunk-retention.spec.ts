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
  let inner: Array<{ sql: string; params: unknown[] }>;
  let dueChunks: Array<{ chunk: string }>;
  let dropError: (Error & { code?: string }) | null;

  beforeEach(async () => {
    outer = [];
    inner = [];
    dueChunks = [];
    dropError = null;

    const manager = {
      query: jest.fn((sql: string) => {
        outer.push(sql);
        if (sql.includes("show_chunks")) return Promise.resolve(dueChunks);
        return Promise.resolve([]);
      }),
      transaction: jest.fn((cb: (em: unknown) => Promise<unknown>) =>
        cb({
          query: jest.fn((sql: string, params: unknown[] = []) => {
            inner.push({ sql, params });
            if (sql.includes("drop_chunks")) {
              if (dropError) return Promise.reject(dropError);
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

  it("does not call drop_chunks when no chunk is due — that call alone locks attractions", async () => {
    const result = await service.dropExpiredPredictionChunks(90);

    expect(result).toEqual({ due: 0, dropped: 0, lockTimedOut: false });
    expect(outer).toHaveLength(1);
    expect(outer[0]).toContain("show_chunks('wait_time_predictions'");
    expect(inner).toHaveLength(0);
  });

  it("sets a transaction-local lock_timeout before dropping", async () => {
    dueChunks = [{ chunk: "_timescaledb_internal._hyper_16_1229_chunk" }];

    const result = await service.dropExpiredPredictionChunks(90, 1500);

    expect(result).toEqual({ due: 1, dropped: 1, lockTimedOut: false });
    expect(inner).toHaveLength(2);
    // set_config(..., true) is SET LOCAL — it must not outlive the transaction
    // on a pooled connection.
    expect(inner[0].sql).toContain("set_config('lock_timeout', $1, true)");
    expect(inner[0].params).toEqual(["1500ms"]);
    expect(inner[1].sql).toContain("drop_chunks('wait_time_predictions'");
    expect(inner[1].params).toEqual(["90 days"]);
  });

  it("reports a lock timeout instead of throwing, so the next run retries", async () => {
    dueChunks = [{ chunk: "_timescaledb_internal._hyper_16_1229_chunk" }];
    dropError = Object.assign(
      new Error("canceling statement due to lock timeout"),
      { code: "55P03" },
    );

    const result = await service.dropExpiredPredictionChunks(90);

    expect(result).toEqual({ due: 1, dropped: 0, lockTimedOut: true });
  });

  it("rethrows any other error", async () => {
    dueChunks = [{ chunk: "_timescaledb_internal._hyper_16_1229_chunk" }];
    dropError = Object.assign(new Error("permission denied"), {
      code: "42501",
    });

    await expect(service.dropExpiredPredictionChunks(90)).rejects.toThrow(
      "permission denied",
    );
  });
});
