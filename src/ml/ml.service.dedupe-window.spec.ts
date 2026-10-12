import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ConfigService } from "@nestjs/config";
import { MLService, DEDUPE_CREATED_AFTER_HOURS } from "./ml.service";
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
import { HYPERTABLES } from "../database/hypertables";

/**
 * The `createdAt` window of the pre-generation dedupe DELETE.
 *
 * `wait_time_predictions` is partitioned on `createdAt`, so this predicate is
 * what lets Timescale exclude chunks — without it the DELETE opened every one
 * and died with "tuple decompression limit exceeded" on parks whose stale rows
 * had been compressed. The width is load-bearing in both directions and both
 * failures are silent, which is why it is pinned here:
 *
 * - too NARROW and a superseded row that still holds a future slot survives
 *   next to the fresh one — which needs the window to fall below the horizon
 *   that produced the row (48h for hourly), or below the gap between two runs
 *   (the daily run skips parks that are closed and quiet);
 * - too WIDE and every run reaches across more chunks than it can find
 *   anything in — the 13 days it was until 2026-10-12 spanned three of the
 *   7-day chunks — and eventually into a compressed one, which is the
 *   failure above.
 */
describe("MLService — dedupe createdAt window", () => {
  let service: MLService;
  let params: Record<string, unknown>;
  let sql: string[];

  const HOUR = 60 * 60 * 1000;

  beforeEach(async () => {
    params = {};
    sql = [];

    const qb = {
      delete: () => qb,
      where: (clause: string, p?: Record<string, unknown>) => {
        sql.push(clause);
        Object.assign(params, p ?? {});
        return qb;
      },
      andWhere: (clause: string, p?: Record<string, unknown>) => {
        sql.push(clause);
        Object.assign(params, p ?? {});
        return qb;
      },
      execute: () => Promise.resolve({ affected: 7 }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLService,
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: { createQueryBuilder: () => qb },
        },
        { provide: getRepositoryToken(QueueData), useValue: {} },
        { provide: getRepositoryToken(Park), useValue: {} },
        {
          provide: getRepositoryToken(Attraction),
          useValue: {
            find: jest.fn().mockResolvedValue([{ id: "a-1" }, { id: "a-2" }]),
          },
        },
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

  it("still scopes the delete on the partition key", async () => {
    await service.deduplicatePredictions("park-1", "hourly");

    // The whole reason the predicate exists. Losing it brings back the
    // decompression failure, and no test of the width would notice.
    expect(sql.some((s) => s.includes('"createdAt" >='))).toBe(true);
    expect(params.createdAfter).toBeInstanceOf(Date);
  });

  it("still scopes the delete to this park and type and to future targets", async () => {
    await service.deduplicatePredictions("park-1", "hourly");

    // Without these the DELETE would reach other parks' rows, the other
    // prediction type, or already-past targets — a far worse failure than any
    // width mistake, and the window tests alone would not notice.
    const clauses = sql.join(" | ");
    expect(clauses).toContain("attractionId IN");
    expect(clauses).toContain("predictionType = :predictionType");
    expect(clauses).toContain("predictedTime >= :startTime");
    expect(clauses).toContain("predictedTime <= :endTime");
    expect(params.attractionIds).toEqual(["a-1", "a-2"]);
    expect(params.predictionType).toBe("hourly");
  });

  it("covers the full 48h generation horizon, with slack", async () => {
    const before = Date.now();
    await service.deduplicatePredictions("park-1", "hourly");
    const after = Date.now();

    const createdAfter = params.createdAfter as Date;
    const ageMs = after - createdAfter.getTime();

    // An hourly row created at T holds targets up to T+48h, so anything
    // created before now-48h cannot hold a future slot. Covering exactly 48h
    // would leave no slack for clock skew between this process and Postgres.
    expect(ageMs).toBeGreaterThan(48 * HOUR);
    expect(createdAfter.getTime()).toBeLessThanOrEqual(
      before - DEDUPE_CREATED_AFTER_HOURS.hourly * HOUR,
    );
    expect(createdAfter.getTime()).toBeGreaterThan(
      before - DEDUPE_CREATED_AFTER_HOURS.hourly * HOUR - 60_000,
    );
  });

  it("keeps both windows days below the configured compression policy", () => {
    const spec = HYPERTABLES.find((h) => h.table === "wait_time_predictions");
    expect(spec).toBeDefined();
    const compressAfterHours = spec!.compressAfterDays * 24;

    // A DELETE that reaches a compressed chunk is the original failure this
    // predicate exists to prevent. Asserted against the configured policy, not
    // against today's 14 days: lowering `compressAfterDays` (the audit proposes
    // 4) has to fail here rather than in production.
    for (const type of ["hourly", "daily"] as const) {
      expect(DEDUPE_CREATED_AFTER_HOURS[type]).toBeLessThanOrEqual(
        compressAfterHours - 24,
      );
    }
  });

  it("gives daily a wider window than hourly", async () => {
    await service.deduplicatePredictions("park-1", "hourly");
    const hourly = params.createdAfter as Date;

    params = {};
    await service.deduplicatePredictions("park-1", "daily");
    const daily = params.createdAfter as Date;

    // The daily run is skipped for parks that are not operating and had no ride
    // activity, so an off-season park can go 4-5 days between runs — past the
    // hourly window, which would then leave the previous cohort undeleted.
    expect(daily.getTime()).toBeLessThan(hourly.getTime());
    expect(DEDUPE_CREATED_AFTER_HOURS.daily).toBeGreaterThanOrEqual(5 * 24);
  });

  it("deletes nothing when the park has no attractions", async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLService,
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: {
            createQueryBuilder: () => {
              throw new Error("must not build a DELETE for an empty park");
            },
          },
        },
        { provide: getRepositoryToken(QueueData), useValue: {} },
        { provide: getRepositoryToken(Park), useValue: {} },
        {
          provide: getRepositoryToken(Attraction),
          useValue: { find: jest.fn().mockResolvedValue([]) },
        },
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

    await expect(
      module
        .get<MLService>(MLService)
        .deduplicatePredictions("park-1", "hourly"),
    ).resolves.toBe(0);
  });
});
