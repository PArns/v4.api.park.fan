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
 *   next to the fresh one (at 48h exactly, a run one minute late would start
 *   leaving its predecessor's last slot behind);
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

  it("covers the full 48h generation horizon, with slack", async () => {
    const before = Date.now();
    await service.deduplicatePredictions("park-1", "hourly");
    const after = Date.now();

    const createdAfter = params.createdAfter as Date;
    const ageMs = after - createdAfter.getTime();

    // An hourly row created at T holds targets up to T+48h, so anything
    // created before now-48h cannot hold a future slot. Covering exactly 48h
    // would leave no room for a late run or clock skew.
    expect(ageMs).toBeGreaterThan(48 * HOUR);
    expect(createdAfter.getTime()).toBeLessThanOrEqual(
      before - DEDUPE_CREATED_AFTER_HOURS * HOUR,
    );
    expect(createdAfter.getTime()).toBeGreaterThan(
      before - DEDUPE_CREATED_AFTER_HOURS * HOUR - 60_000,
    );
  });

  it("stays days below the 14-day compression policy", async () => {
    await service.deduplicatePredictions("park-1", "daily");

    const createdAfter = params.createdAfter as Date;
    const ageHours = (Date.now() - createdAfter.getTime()) / HOUR;

    // job 1006 compresses this hypertable at 14 days. A DELETE that reaches a
    // compressed chunk is the original failure; keep whole days of margin.
    // The audit's follow-up proposal is compress_after = 4 days, which this
    // window must still clear — hence the 96h bound rather than 14 × 24.
    expect(ageHours).toBeLessThanOrEqual(96 - 24);
  });

  it("uses the same window for both prediction types", async () => {
    await service.deduplicatePredictions("park-1", "hourly");
    const hourly = params.createdAfter as Date;

    params = {};
    await service.deduplicatePredictions("park-1", "daily");
    const daily = params.createdAfter as Date;

    // Daily rows are rewritten nightly, so the hourly-derived window covers
    // them with two missed nights to spare. One number, one derivation.
    expect(Math.abs(daily.getTime() - hourly.getTime())).toBeLessThan(60_000);
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
