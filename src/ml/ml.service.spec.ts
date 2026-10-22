import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ConfigService } from "@nestjs/config";
import { MLService } from "./ml.service";
import { WaitTimePrediction } from "./entities/wait-time-prediction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import {
  ScheduleEntry,
  ScheduleType,
} from "../parks/entities/schedule-entry.entity";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { ForecastAccuracyService } from "./services/forecast-accuracy.service";
import { WeatherService } from "../parks/weather.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { HolidaysService } from "../holidays/holidays.service";
import { ParksService } from "../parks/parks.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { getCurrentDateInTimezone } from "../common/utils/date.util";
import { currentSlotStartMs } from "../common/utils/best-visit-times.util";
import { PCN_MAX_FORECAST_AGE_H } from "./pcn-serving.constants";

describe("MLService", () => {
  let service: MLService;

  // Mock Redis
  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    ttl: jest.fn(),
    incr: jest.fn(),
    expire: jest.fn(),
    mget: jest.fn(),
    pipeline: jest.fn(),
  };

  // Mock Config Service
  const mockConfigService = {
    get: jest.fn((key: string) => {
      if (key === "ML_SERVICE_URL") return "http://localhost:8000";
      return undefined;
    }),
  };

  // Mock Repositories
  // Predictions are written through the query builder (insert().orIgnore()),
  // so the spec captures the rows there instead of on save().
  const insertedValues: Record<string, unknown>[][] = [];
  const mockPredictionRepository = {
    find: jest.fn(),
    save: jest.fn(),
    // Raw SQL reads (pcn_forecasts for the PCN champion-swap override).
    manager: { query: jest.fn().mockResolvedValue([]) },
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
      delete: jest.fn().mockReturnThis(),
      insert: jest.fn().mockReturnThis(),
      into: jest.fn().mockReturnThis(),
      orIgnore: jest.fn().mockReturnThis(),
      values: jest.fn(function (this: unknown, v: Record<string, unknown>[]) {
        insertedValues.push(v);
        return this;
      }),
      execute: jest.fn().mockResolvedValue({ affected: 0, identifiers: [] }),
    })),
  };

  /** All rows handed to insert().values() across chunks. */
  const allInserted = () => insertedValues.flat();

  const mockQueueDataRepository = {
    findOne: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      select: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      addOrderBy: jest.fn().mockReturnThis(),
      distinctOn: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      getRawMany: jest.fn().mockResolvedValue([]),
      getMany: jest.fn().mockResolvedValue([]),
    })),
  };

  const mockParkRepository = {
    findOne: jest.fn(),
  };

  const mockAttractionRepository = {
    findOne: jest.fn(),
    find: jest.fn(),
    createQueryBuilder: jest.fn(),
    query: jest.fn(),
  };

  const mockScheduleEntryRepository = {
    findOne: jest.fn(),
    find: jest.fn(),
  };

  // Mock Services
  const mockPredictionAccuracyService = {
    recordPredictions: jest.fn().mockResolvedValue(0),
  };

  // The measured error grid the TFT band and confidence are read from. Empty by
  // default so the tests that do not care about the band get the "no cell" path.
  const mockForecastAccuracyService = {
    getProfile: jest.fn().mockResolvedValue(new Map()),
  };

  const mockWeatherService = {
    getHourlyForecast: jest.fn(),
    getCurrentAndForecast: jest.fn(),
    getWeatherData: jest.fn(),
  };

  const mockAnalyticsService = {
    getCurrentOccupancy: jest.fn(),
    getP50BaselineFromCache: jest.fn(),
  };

  const mockHolidaysService = {
    isBridgeDay: jest.fn(),
    isEffectiveSchoolHoliday: jest.fn(),
    getHolidays: jest.fn(),
  };

  const mockParksService = {
    getOperatingDateRange: jest.fn(),
    isParkSeasonal: jest.fn(),
    getBatchParkStatus: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MLService,
        {
          provide: getRepositoryToken(WaitTimePrediction),
          useValue: mockPredictionRepository,
        },
        {
          provide: getRepositoryToken(QueueData),
          useValue: mockQueueDataRepository,
        },
        {
          provide: getRepositoryToken(Park),
          useValue: mockParkRepository,
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: mockAttractionRepository,
        },
        {
          provide: getRepositoryToken(ScheduleEntry),
          useValue: mockScheduleEntryRepository,
        },
        {
          provide: ConfigService,
          useValue: mockConfigService,
        },
        {
          provide: PredictionAccuracyService,
          useValue: mockPredictionAccuracyService,
        },
        {
          provide: ForecastAccuracyService,
          useValue: mockForecastAccuracyService,
        },
        {
          provide: WeatherService,
          useValue: mockWeatherService,
        },
        {
          provide: AnalyticsService,
          useValue: mockAnalyticsService,
        },
        {
          provide: HolidaysService,
          useValue: mockHolidaysService,
        },
        {
          provide: ParksService,
          useValue: mockParksService,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<MLService>(MLService);

    jest.clearAllMocks();
    mockForecastAccuracyService.getProfile.mockResolvedValue(new Map());
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("isHealthy", () => {
    it("should return true when ML service is healthy", async () => {
      const mlClient = (service as any).mlClient;
      mlClient.get = jest.fn().mockResolvedValue({
        status: 200,
        data: { status: "healthy" },
      });

      const result = await service.isHealthy();

      expect(result).toBe(true);
    });

    it("should return false when ML service is unreachable", async () => {
      const mlClient = (service as any).mlClient;
      mlClient.get = jest
        .fn()
        .mockRejectedValue(new Error("Connection refused"));

      const result = await service.isHealthy();

      expect(result).toBe(false);
    });
  });

  describe("getParkPredictions", () => {
    const parkId = "park-123";

    it("should return cached predictions when available", async () => {
      const cachedData = {
        predictions: [
          {
            attractionId: "attr-1",
            predictedTime: new Date().toISOString(),
            predictedWaitTime: 30,
            confidence: 0.9,
          },
        ],
      };

      // Park lookup is now done before the cache read for timezone-aware
      // cache keys; mock both calls.
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
        countryCode: "US",
        regionCode: null,
      });
      mockRedis.get.mockResolvedValue(JSON.stringify(cachedData));

      const result = await service.getParkPredictions(parkId, "hourly");

      expect(result).toEqual(cachedData);
      expect(mockRedis.get).toHaveBeenCalled();
    });

    it("collapses concurrent hourly cache misses into one ml-service build", async () => {
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
      });
      mockRedis.get.mockResolvedValue(null);
      const payload = { predictions: [], count: 0, modelVersion: "v" };
      const build = jest
        .spyOn(
          service as unknown as {
            buildRawParkPredictions: () => Promise<unknown>;
          },
          "buildRawParkPredictions",
        )
        .mockResolvedValue(payload);

      const results = await Promise.all([
        service.getRawParkPredictions(parkId, "hourly"),
        service.getRawParkPredictions(parkId, "hourly"),
      ]);

      expect(build).toHaveBeenCalledTimes(1);
      expect(results).toEqual([payload, payload]);
      build.mockRestore();
    });
  });

  describe("PCN champion-swap on the park-level path (PAR-817)", () => {
    const parkId = "park-pcn";
    const HOUR = 3_600_000;
    /** python `datetime.isoformat()` of a UTC-aware timestamp — what predict.py emits. */
    const pyIso = (ms: number) =>
      new Date(ms).toISOString().replace(".000Z", "+00:00");

    let farSlot: number; // lead 4 h → past the blend horizon, pure PCN
    let nearSlot: number; // lead 30 min → blended toward the current wait
    let catboostPayload: string;
    const prevFlag = process.env.SERVE_PCN_INTRADAY;

    beforeEach(() => {
      process.env.SERVE_PCN_INTRADAY = "true";
      const slot0 = currentSlotStartMs();
      farSlot = slot0 + 4 * HOUR;
      nearSlot = slot0 + 0.5 * HOUR;
      catboostPayload = JSON.stringify({
        predictions: [farSlot, nearSlot].map((ms) => ({
          attractionId: "attr-1",
          predictedTime: pyIso(ms),
          predictedWaitTime: 20,
          predictionType: "hourly",
          confidence: 80,
          crowdLevel: "low",
          baseline: 40,
          modelVersion: "v1",
        })),
        count: 2,
        modelVersion: "v1",
      });
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
        countryCode: "DE",
        regionCode: null,
      });
      mockRedis.get.mockResolvedValue(catboostPayload);
      // pcn_forecasts rows: q0.5 = 60 for both slots, q0.8 = 70.
      mockPredictionRepository.manager.query.mockResolvedValue(
        [farSlot, nearSlot].flatMap((ms) => [
          {
            aid: "attr-1",
            predicted_time: new Date(ms),
            quantile: 0.5,
            wait: "60",
          },
          {
            aid: "attr-1",
            predicted_time: new Date(ms),
            quantile: 0.8,
            wait: "70",
          },
        ]),
      );
      // Blend anchor: the ride's current wait.
      mockQueueDataRepository.createQueryBuilder.mockImplementation(() => ({
        select: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        distinctOn: jest.fn().mockReturnThis(),
        limit: jest.fn().mockReturnThis(),
        getRawMany: jest.fn().mockResolvedValue([]),
        getMany: jest
          .fn()
          .mockResolvedValue([{ attractionId: "attr-1", waitTime: 100 }]),
      }));
    });

    afterEach(() => {
      if (prevFlag === undefined) delete process.env.SERVE_PCN_INTRADAY;
      else process.env.SERVE_PCN_INTRADAY = prevFlag;
      mockPredictionRepository.manager.query.mockResolvedValue([]);
      mockRedis.get.mockReset();
    });

    it("applies the override to python's `+00:00` predictedTime strings", async () => {
      const result = await service.getParkPredictions(parkId, "hourly");
      const at = (ms: number) =>
        result.predictions.find((p) => Date.parse(p.predictedTime) === ms)!;

      // Lead 4 h: pure PCN q0.5, crowd level from q0.8 (70 / 40 = 175 %).
      expect(at(farSlot).predictedWaitTime).toBe(60);
      expect(at(farSlot).crowdLevel).toBe("very_high");
      expect(at(farSlot).modelVersion).toBe("v1+pcn");
      // Lead 30 min: the persistence blend serves the current wait (α = 1 up to 1 h),
      // so the anchor read works on this path too.
      expect(at(nearSlot).predictedWaitTime).toBe(100);
      expect(at(nearSlot).modelVersion).toBe("v1+pcn");
      // Only the value is served; the time string the client sees is unchanged.
      expect(at(farSlot).predictedTime).toBe(pyIso(farSlot));
    });

    it("getRawParkPredictions returns CatBoost untouched (the store path)", async () => {
      const raw = await service.getRawParkPredictions(parkId, "hourly");

      expect(raw).toEqual(JSON.parse(catboostPayload));
      expect(mockPredictionRepository.manager.query).not.toHaveBeenCalled();
    });

    it("never writes a PCN number into the Redis entry the store path reads", async () => {
      mockRedis.get.mockImplementation((key: string) =>
        Promise.resolve(
          key.startsWith("ml:active-attractions:")
            ? JSON.stringify(["attr-1"])
            : null,
        ),
      );
      mockAttractionRepository.find.mockResolvedValue([{ id: "attr-1" }]);
      mockWeatherService.getHourlyForecast.mockResolvedValue([]);
      jest
        .spyOn(service as any, "enrichForecastWithHolidays")
        .mockResolvedValue([]);
      jest.spyOn(service as any, "buildFeatureContext").mockResolvedValue({});
      mockAnalyticsService.getP50BaselineFromCache.mockResolvedValue(0);
      (mockAnalyticsService as any).getTypicalDayPeakFromCache = jest
        .fn()
        .mockResolvedValue(0);
      const mlClient = (service as any).mlClient;
      mlClient.post = jest
        .fn()
        .mockResolvedValue({ data: JSON.parse(catboostPayload) });

      const served = await service.getParkPredictions(parkId, "hourly");

      expect(served.predictions.every((p) => p.modelVersion === "v1+pcn")).toBe(
        true,
      );
      const written = mockRedis.set.mock.calls.find(
        ([key]) => typeof key === "string" && key.startsWith("ml:park:"),
      );
      expect(written).toBeDefined();
      expect(JSON.parse(written![1] as string)).toEqual(
        JSON.parse(catboostPayload),
      );
    });
  });

  // PAR-854: the accuracy row records what the reader was SERVED at generation
  // time, beside CatBoost's raw answer. These pin that the value is the served
  // path's own (override + blend, at the lead it had then) and that every
  // fallback lands on CatBoost with a `catboost` label.
  describe("servedIntradayValues (the served value the badge scores)", () => {
    const HOUR = 3_600_000;
    const prevFlag = process.env.SERVE_PCN_INTRADAY;
    let farSlot: number;
    let midSlot: number;
    let nearSlot: number;

    const row = (ms: number, wait = 20) =>
      ({
        attractionId: "attr-1",
        predictedTime: new Date(ms),
        predictedWaitTime: wait,
        predictionType: "hourly" as const,
        confidence: 80,
        crowdLevel: "low" as const,
        baseline: 40,
        modelVersion: "v1",
      }) as never;

    const pcnRows = (slots: number[]) =>
      slots.flatMap((ms) => [
        {
          aid: "attr-1",
          predicted_time: new Date(ms),
          quantile: 0.5,
          wait: "60",
        },
        {
          aid: "attr-1",
          predicted_time: new Date(ms),
          quantile: 0.8,
          wait: "70",
        },
      ]);

    beforeEach(() => {
      const slot0 = currentSlotStartMs();
      farSlot = slot0 + 4 * HOUR; // past the blend horizon → pure PCN
      midSlot = slot0 + 2 * HOUR; // inside the blend: α = 0.5
      nearSlot = slot0 + 0.5 * HOUR; // closer than the scoring lead
      mockQueueDataRepository.createQueryBuilder.mockImplementation(() => ({
        select: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        distinctOn: jest.fn().mockReturnThis(),
        limit: jest.fn().mockReturnThis(),
        getRawMany: jest.fn().mockResolvedValue([]),
        getMany: jest
          .fn()
          .mockResolvedValue([{ attractionId: "attr-1", waitTime: 100 }]),
      }));
    });

    afterEach(() => {
      if (prevFlag === undefined) delete process.env.SERVE_PCN_INTRADAY;
      else process.env.SERVE_PCN_INTRADAY = prevFlag;
      mockPredictionRepository.manager.query.mockResolvedValue([]);
    });

    it("records PCN with the blend at the lead it had at generation time", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(
        pcnRows([farSlot, midSlot]),
      );
      const slot0 = currentSlotStartMs();

      const served = await service.servedIntradayValues(
        [row(farSlot), row(midSlot)],
        slot0,
      );

      expect(served).toEqual([
        { wait: 60, source: "pcn" }, // 4 h out: pure PCN q0.5
        // 2 h out: α = 0.5 between the anchor (100) and PCN (60) → 80, exactly
        // what the park page showed at that moment.
        { wait: 80, source: "pcn" },
      ]);
    });

    it("records nothing for a slot closer than the scoring lead", async () => {
      // At 30 minutes the blend serves the anchor — the wait read now — and the
      // comparison job would score it against the reading nearest the slot,
      // largely itself. Null leaves the row's served columns untouched, so the
      // value kept is the last one shown at least two hours ahead.
      process.env.SERVE_PCN_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(
        pcnRows([nearSlot]),
      );

      const served = await service.servedIntradayValues(
        [row(nearSlot), row(farSlot)],
        currentSlotStartMs(),
      );

      expect(served[0]).toBeNull();
      expect(served[1]).not.toBeNull();
    });

    it("records nothing between one and two hours ahead either", async () => {
      // 60–120 minutes is still the blend's pure-anchor stretch at its near
      // end: scoring it would grade persistence (review of PR #452).
      process.env.SERVE_PCN_INTRADAY = "true";
      const slot = currentSlotStartMs() + 1.5 * HOUR;
      mockPredictionRepository.manager.query.mockResolvedValue(pcnRows([slot]));

      const served = await service.servedIntradayValues(
        [row(slot), row(farSlot)],
        currentSlotStartMs(),
      );

      expect(served[0]).toBeNull();
      expect(served[1]).not.toBeNull();
    });

    it("records CatBoost when the flag is off, without reading PCN at all", async () => {
      delete process.env.SERVE_PCN_INTRADAY;
      mockPredictionRepository.manager.query.mockResolvedValue(
        pcnRows([farSlot]),
      );

      const served = await service.servedIntradayValues([row(farSlot, 25)]);

      expect(served).toEqual([{ wait: 25, source: "catboost" }]);
      expect(mockPredictionRepository.manager.query).not.toHaveBeenCalled();
    });

    it("records CatBoost where PCN is stale or has no forecast for the slot", async () => {
      // The staleness guard lives in the SQL (created_at within
      // PCN_MAX_FORECAST_AGE_H): a frozen producer returns no rows, which is
      // what this simulates — and only the far slot has a fresh forecast.
      process.env.SERVE_PCN_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(
        pcnRows([farSlot]),
      );

      const served = await service.servedIntradayValues(
        [row(farSlot), row(nearSlot, 35)],
        currentSlotStartMs(),
      );

      expect(served).toEqual([
        { wait: 60, source: "pcn" },
        null, // too close to score
      ]);
      const [sql, params] = mockPredictionRepository.manager.query.mock
        .calls[0] as [string, unknown[]];
      expect(sql).toContain("f.created_at >= now() - ($3 || ' hours')");
      expect(params[2]).toBe(PCN_MAX_FORECAST_AGE_H);
    });

    it("records CatBoost when the PCN read fails", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockRejectedValue(
        new Error("relation pcn_forecasts does not exist"),
      );

      const served = await service.servedIntradayValues([row(farSlot, 25)]);

      expect(served).toEqual([{ wait: 25, source: "catboost" }]);
    });

    it("never mutates the rows it is given (they are what gets stored raw)", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(
        pcnRows([farSlot]),
      );
      const input = row(farSlot) as unknown as {
        predictedWaitTime: number;
        modelVersion: string;
      };

      await service.servedIntradayValues([input as never]);

      expect(input.predictedWaitTime).toBe(20);
      expect(input.modelVersion).toBe("v1");
    });
  });

  // PAR-911: Chronos-2 on the 45–120 min slots behind SERVE_CHRONOS_INTRADAY,
  // read from the serving cache the forward-archive capture writes.
  describe("Chronos-2 serving override (PAR-911)", () => {
    const parkId = "park-chronos";
    const MIN = 60_000;
    const prevPcn = process.env.SERVE_PCN_INTRADAY;
    const prevChronos = process.env.SERVE_CHRONOS_INTRADAY;
    // A slot-aligned "now", so a lead of exactly 120 min is reachable for the
    // recorder test below.
    const NOW = Math.floor(Date.UTC(2026, 9, 17, 12, 0) / 900_000) * 900_000;
    const LEADS = [30, 60, 120, 180];
    const pyIso = (ms: number) =>
      new Date(ms).toISOString().replace(".000Z", "+00:00");

    const chronosEntry = (originAt = NOW - 10 * MIN) =>
      JSON.stringify({
        originAt,
        revision: "29ec376",
        segments: [
          {
            slotStart: NOW,
            slotMinutes: 15,
            q50: Array.from({ length: 17 }, () => 42),
            q80: Array.from({ length: 17 }, () => 50),
            q95: Array.from({ length: 17 }, () => 60),
          },
        ],
      });

    const pcnRows = (wait: string) =>
      LEADS.flatMap((l) => [
        {
          aid: "attr-1",
          predicted_time: new Date(NOW + l * MIN),
          quantile: 0.5,
          wait,
        },
      ]);

    beforeEach(() => {
      jest.spyOn(Date, "now").mockReturnValue(NOW);
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
        countryCode: "DE",
        regionCode: null,
      });
      mockRedis.get.mockResolvedValue(
        JSON.stringify({
          predictions: LEADS.map((l) => ({
            attractionId: "attr-1",
            predictedTime: pyIso(NOW + l * MIN),
            predictedWaitTime: 20,
            predictionType: "hourly",
            confidence: 80,
            crowdLevel: "low",
            baseline: 40,
            modelVersion: "v1",
            uncertaintyMinutes: 5,
          })),
          count: LEADS.length,
          modelVersion: "v1",
        }),
      );
      mockRedis.mget.mockResolvedValue([chronosEntry()]);
      mockQueueDataRepository.createQueryBuilder.mockImplementation(() => ({
        select: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        andWhere: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        addOrderBy: jest.fn().mockReturnThis(),
        distinctOn: jest.fn().mockReturnThis(),
        limit: jest.fn().mockReturnThis(),
        getRawMany: jest.fn().mockResolvedValue([]),
        getMany: jest
          .fn()
          .mockResolvedValue([{ attractionId: "attr-1", waitTime: 100 }]),
      }));
    });

    afterEach(() => {
      jest.restoreAllMocks();
      if (prevPcn === undefined) delete process.env.SERVE_PCN_INTRADAY;
      else process.env.SERVE_PCN_INTRADAY = prevPcn;
      if (prevChronos === undefined) delete process.env.SERVE_CHRONOS_INTRADAY;
      else process.env.SERVE_CHRONOS_INTRADAY = prevChronos;
      mockPredictionRepository.manager.query.mockResolvedValue([]);
      mockRedis.get.mockReset();
      mockRedis.mget.mockReset();
    });

    const byLead = (preds: { predictedTime: string }[]) => (l: number) =>
      preds.find(
        (p) => Date.parse(p.predictedTime) === NOW + l * MIN,
      ) as unknown as {
        predictedWaitTime: number;
        modelVersion: string;
        crowdLevel: string;
        uncertaintyMinutes: number;
      };

    it("both flags off: CatBoost untouched and the cache never read", async () => {
      delete process.env.SERVE_PCN_INTRADAY;
      delete process.env.SERVE_CHRONOS_INTRADAY;

      const res = await service.getParkPredictions(parkId, "hourly");

      expect(res.predictions.every((p) => p.modelVersion === "v1")).toBe(true);
      expect(mockRedis.mget).not.toHaveBeenCalled();
    });

    it("Chronos only: 45–120 min from Chronos, the rest stays CatBoost", async () => {
      delete process.env.SERVE_PCN_INTRADAY;
      process.env.SERVE_CHRONOS_INTRADAY = "true";

      const res = await service.getParkPredictions(parkId, "hourly");
      const at = byLead(res.predictions);

      expect(at(30).modelVersion).toBe("v1");
      expect(at(30).predictedWaitTime).toBe(20);
      for (const l of [60, 120]) {
        expect(at(l).modelVersion).toBe("v1+chronos");
        expect(at(l).predictedWaitTime).toBe(40); // q50 42 → 40
        expect(at(l).uncertaintyMinutes).toBe(18); // q95 60 − q50 42
        expect(at(l).crowdLevel).toBe("high"); // q80 50 / 40 = 125 %
      }
      expect(at(180).modelVersion).toBe("v1");
      expect(mockPredictionRepository.manager.query).not.toHaveBeenCalled();
    });

    it("with PCN on: blend ≤ 30 min, Chronos 45–120, PCN beyond", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      process.env.SERVE_CHRONOS_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(pcnRows("60"));

      const res = await service.getParkPredictions(parkId, "hourly");
      const at = byLead(res.predictions);

      expect(at(30).modelVersion).toBe("v1+pcn");
      expect(at(30).predictedWaitTime).toBe(100); // pure anchor ≤ 1 h
      expect(at(60).modelVersion).toBe("v1+chronos");
      expect(at(120).modelVersion).toBe("v1+chronos");
      expect(at(180).modelVersion).toBe("v1+pcn");
    });

    it("falls back to PCN where the Chronos curve is stale", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      process.env.SERVE_CHRONOS_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(pcnRows("60"));
      mockRedis.mget.mockResolvedValue([chronosEntry(NOW - 91 * MIN)]);

      const res = await service.getParkPredictions(parkId, "hourly");

      expect(res.predictions.every((p) => p.modelVersion === "v1+pcn")).toBe(
        true,
      );
    });

    it("`chronos: false` (the archive's incumbent read) skips Chronos", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      process.env.SERVE_CHRONOS_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(pcnRows("60"));

      const res = await service.getParkPredictions(
        parkId,
        "hourly",
        undefined,
        undefined,
        { chronos: false },
      );

      expect(res.predictions.every((p) => p.modelVersion === "v1+pcn")).toBe(
        true,
      );
      expect(mockRedis.mget).not.toHaveBeenCalled();
    });

    it("servedIntradayValues records `chronos` where Chronos served the scored slot", async () => {
      process.env.SERVE_PCN_INTRADAY = "true";
      process.env.SERVE_CHRONOS_INTRADAY = "true";
      mockPredictionRepository.manager.query.mockResolvedValue(pcnRows("60"));
      const row = (l: number) =>
        ({
          attractionId: "attr-1",
          predictedTime: new Date(NOW + l * MIN),
          predictedWaitTime: 20,
          predictionType: "hourly" as const,
          confidence: 80,
          crowdLevel: "low" as const,
          baseline: 40,
          modelVersion: "v1",
        }) as never;

      const served = await service.servedIntradayValues(
        [row(60), row(120), row(180)],
        NOW,
      );

      expect(served).toEqual([
        null, // closer than the two-hour scored lead
        { wait: 40, source: "chronos" },
        { wait: 60, source: "pcn" },
      ]);
    });
  });

  describe("getTftDailyPredictions (the measured band)", () => {
    const parkId = "park-band";
    const today = getCurrentDateInTimezone("Europe/Berlin");
    const plus = (n: number) =>
      new Date(Date.parse(`${today}T00:00:00Z`) + n * 86_400_000)
        .toISOString()
        .slice(0, 10);

    const cell = (
      predictedBand: string,
      leadBucket: string,
      mae: number,
      uncertaintyP95: number | null,
    ) =>
      ({
        predictedBand,
        leadBucket,
        sampleSize: 10_000,
        mae,
        meanActual: 40,
        uncertaintyP95,
        computedAt: new Date(),
      }) as any;

    /**
     * Rows as the raw query returns them: one ride, one target day each.
     * `forecastDate` defaults to today — a forecast made this morning — and is
     * passed explicitly where the point is that it was not.
     */
    const arrange = (
      rows: Array<{ targetDate: string; peak: number; forecastDate?: string }>,
    ) => {
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "Europe/Berlin",
      });
      mockRedis.get.mockResolvedValue(null);
      mockRedis.set.mockResolvedValue("OK");
      mockAttractionRepository.query.mockResolvedValue(
        rows.map((r, i) => ({
          attractionId: `attr-${i}`,
          targetDate: r.targetDate,
          forecastDate: r.forecastDate ?? today,
          peak: String(r.peak),
        })),
      );
    };

    it("carries the cell's p95 as uncertaintyMinutes, picked by band AND lead", async () => {
      mockForecastAccuracyService.getProfile.mockResolvedValue(
        new Map([
          ["quiet|d1", cell("quiet", "d1", 8.6, 23.8)],
          ["quiet|d60", cell("quiet", "d60", 13.2, 38.1)],
          ["busy|d1", cell("busy", "d1", 20.8, 45.9)],
        ]),
      );
      arrange([
        { targetDate: plus(1), peak: 20 }, // quiet, d1
        { targetDate: plus(45), peak: 20 }, // quiet, widens to d60
        { targetDate: plus(1), peak: 80 }, // busy, d1
      ]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      expect(preds.map((p) => p.uncertaintyMinutes)).toEqual([24, 38, 46]);
    });

    it("filters the park as uuid, never by casting the indexed column (PAR-871)", async () => {
      mockForecastAccuracyService.getProfile.mockResolvedValue(new Map());
      arrange([]);

      await service.getTftDailyPredictions(parkId, 60);

      const [sql, params] = mockAttractionRepository.query.mock.calls[0];
      expect(sql).toContain('a."parkId" = $1::uuid');
      expect(sql).not.toContain('"parkId"::text');
      // The id the park lookup returned, so the bound value is the stored one.
      expect(params[0]).toBe(parkId);
    });

    it("buckets by the forecast's own age, not by how far off the day is", async () => {
      // The staleness guard admits a forecast up to three days old. A forecast
      // made 3 days ago for a day 5 days out was made at a distance of 8, and
      // the band measured at 8 is the wider one — taking the d7 cell because
      // "today + 5" reads as d7 would narrow it exactly when it should widen.
      mockForecastAccuracyService.getProfile.mockResolvedValue(
        new Map([
          ["quiet|d7", cell("quiet", "d7", 9.0, 26.4)],
          ["quiet|d14", cell("quiet", "d14", 9.9, 28.4)],
        ]),
      );
      arrange([{ targetDate: plus(5), peak: 20, forecastDate: plus(-3) }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      expect(preds[0].uncertaintyMinutes).toBe(28); // d14, not d7
    });

    it("leaves the band absent — not zero — when the grid has no cell", async () => {
      mockForecastAccuracyService.getProfile.mockResolvedValue(new Map());
      arrange([{ targetDate: plus(1), peak: 20 }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      expect(preds[0].uncertaintyMinutes).toBeNull();
    });

    it("still serves the forecast when the grid cannot be read", async () => {
      mockForecastAccuracyService.getProfile.mockRejectedValue(
        new Error("relation does not exist"),
      );
      arrange([{ targetDate: plus(1), peak: 20 }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      expect(preds).toHaveLength(1);
      expect(preds[0].predictedWaitTime).toBe(20);
      expect(preds[0].uncertaintyMinutes).toBeNull();
    });

    it("treats a row written before the column existed as no band at all", async () => {
      // The night between the deploy that adds `uncertainty_p95` and the next
      // nightly rebuild: the cell is there, the band is NULL. It must read as
      // "not known" — and it must NOT flow into the confidence term, where
      // `null / wait` is 0 and would award the highest possible confidence to
      // the one row that measured nothing.
      mockForecastAccuracyService.getProfile.mockResolvedValue(
        new Map([["quiet|d1", cell("quiet", "d1", 8.6, null)]]),
      );
      arrange([{ targetDate: plus(1), peak: 20 }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      expect(preds[0].uncertaintyMinutes).toBeNull();
      // 85 - 1*0.15 = 84.85, rounded to one decimal like every other answer.
      expect(preds[0].confidence).toBe(84.9);
    });

    it("forwards a measured zero rather than dropping it", async () => {
      mockForecastAccuracyService.getProfile.mockResolvedValue(
        new Map([["quiet|d1", cell("quiet", "d1", 8.6, 0)]]),
      );
      arrange([{ targetDate: plus(1), peak: 20 }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      expect(preds[0].uncertaintyMinutes).toBe(0);
    });

    it("replaces the invented 0.7 with CatBoost's own 0-100 formula", async () => {
      mockForecastAccuracyService.getProfile.mockResolvedValue(
        new Map([["quiet|d1", cell("quiet", "d1", 8.6, 23.8)]]),
      );
      arrange([{ targetDate: plus(1), peak: 20 }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      // time = 85 - 1*0.15 = 84.85; relative = min(23.8/20, 1) = 1 → model
      // floors at 30; 0.6*84.85 + 0.4*30 = 62.9. On CatBoost's scale, not 0.7.
      expect(preds[0].confidence).toBeCloseTo(62.9, 1);
    });

    it("falls back to the time term alone where there is no band", async () => {
      mockForecastAccuracyService.getProfile.mockResolvedValue(new Map());
      arrange([{ targetDate: plus(40), peak: 20 }]);

      const preds = await service.getTftDailyPredictions(parkId, 60);

      // 85 - 40*0.15 = 79.0, and nothing blended in.
      expect(preds[0].confidence).toBeCloseTo(79.0, 1);
    });
  });

  describe("getServingDailyPredictions (TFT near-term merge)", () => {
    const parkId = "park-123";
    const mkPred = (aid: string, date: string, wait: number, model: string) =>
      ({
        attractionId: aid,
        predictedTime: `${date}T12:00:00`,
        predictedWaitTime: wait,
        predictionType: "daily",
        confidence: 0.7,
        crowdLevel: "moderate",
        baseline: 0,
        modelVersion: model,
      }) as any;

    afterEach(() => jest.restoreAllMocks());

    it("overrides CatBoost with TFT for covered (attraction,date), keeps CatBoost for the rest", async () => {
      const near = "2026-05-25";
      const far = "2026-08-01";
      jest.spyOn(service, "getParkPredictions").mockResolvedValue({
        predictions: [
          mkPred("attr-1", near, 20, "catboost"),
          mkPred("attr-1", far, 25, "catboost"),
          mkPred("attr-2", near, 99, "catboost"),
        ],
        count: 3,
        modelVersion: "catboost-v1",
      } as any);
      jest
        .spyOn(service, "getTftDailyPredictions")
        .mockResolvedValue([mkPred("attr-1", near, 45, "tft")]);

      const res = await service.getServingDailyPredictions(parkId, 30);
      const byKey = new Map(
        res.predictions.map((p) => [
          `${p.attractionId}|${p.predictedTime.slice(0, 10)}`,
          p,
        ]),
      );
      // attr-1 near → TFT (45), not CatBoost (20)
      expect(byKey.get(`attr-1|${near}`)?.predictedWaitTime).toBe(45);
      expect(byKey.get(`attr-1|${near}`)?.modelVersion).toBe("tft");
      // attr-1 far + non-headliner attr-2 near → CatBoost untouched
      expect(byKey.get(`attr-1|${far}`)?.predictedWaitTime).toBe(25);
      expect(byKey.get(`attr-2|${near}`)?.predictedWaitTime).toBe(99);
      expect(res.count).toBe(3);
      expect(res.modelVersion).toContain("tft30");
    });

    it("falls back to pure CatBoost when TFT has no forecasts", async () => {
      jest.spyOn(service, "getParkPredictions").mockResolvedValue({
        predictions: [mkPred("attr-1", "2026-05-25", 20, "catboost")],
        count: 1,
        modelVersion: "catboost-v1",
      } as any);
      jest.spyOn(service, "getTftDailyPredictions").mockResolvedValue([]);

      const res = await service.getServingDailyPredictions(parkId, 30);
      expect(res.predictions).toHaveLength(1);
      expect(res.predictions[0].predictedWaitTime).toBe(20);
      expect(res.modelVersion).toBe("catboost-v1");
    });
  });

  describe("storePredictions", () => {
    const parkId = "park-1";
    const attractionId = "attr-1";
    const today = "2026-04-17";
    const tomorrow = "2026-04-18";
    const nextWeek = "2026-04-24";

    const predictions = [
      {
        attractionId,
        predictedTime: `${today}T12:00:00Z`,
        predictedWaitTime: 30,
        confidence: 0.9,
        crowdLevel: "moderate" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "hourly" as const,
      },
      {
        attractionId,
        predictedTime: `${tomorrow}T12:00:00Z`,
        predictedWaitTime: 10,
        confidence: 0.95,
        crowdLevel: "low" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "daily" as const,
      },
      {
        attractionId,
        predictedTime: `${nextWeek}T12:00:00Z`,
        predictedWaitTime: 50,
        confidence: 0.85,
        crowdLevel: "high" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "daily" as const,
      },
    ];

    beforeEach(() => {
      // storePredictions resolves attraction → park → timezone in ONE joined
      // query, so the raw rows carry the timezone alongside the parkId.
      mockAttractionRepository.createQueryBuilder.mockReturnValue({
        innerJoin: jest.fn().mockReturnThis(),
        select: jest.fn().mockReturnThis(),
        addSelect: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        getRawMany: jest
          .fn()
          .mockResolvedValue([{ id: attractionId, parkId, timezone: "UTC" }]),
      });
      mockAttractionRepository.find.mockResolvedValue([
        { id: attractionId, parkId },
      ]);
      mockParkRepository.findOne.mockResolvedValue({
        id: parkId,
        timezone: "UTC",
      });
      mockPredictionRepository.save.mockImplementation((entities) =>
        Promise.resolve(entities),
      );
      insertedValues.length = 0;
    });

    it("should store all predictions when park has no schedule history (e.g. Hellendoorn)", async () => {
      // The park's rides, as the diff-write resolves them before reading the
      // stored slots (deliberately not a join — see diffWriteHourlyPredictions).
      mockAttractionRepository.find.mockResolvedValue([{ id: attractionId }]);
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: null,
        maxDate: null,
      });
      mockParksService.isParkSeasonal.mockResolvedValue(false);
      mockScheduleEntryRepository.find.mockResolvedValue([]);

      await service.storePredictions(predictions);

      expect(allInserted()).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            predictedTime: new Date(`${today}T12:00:00Z`),
          }),
          expect.objectContaining({
            predictedTime: new Date(`${tomorrow}T12:00:00Z`),
          }),
          expect.objectContaining({
            predictedTime: new Date(`${nextWeek}T12:00:00Z`),
          }),
        ]),
      );
    });

    it("should filter out predictions for explicitly CLOSED days", async () => {
      // For a seasonal park the gap filter also skips dates without an
      // explicit OPERATING entry, so we have to mark the days we want
      // kept as OPERATING and the day we want dropped as CLOSED.
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
      });
      mockParksService.isParkSeasonal.mockResolvedValue(true);
      mockScheduleEntryRepository.find.mockResolvedValue([
        {
          parkId,
          date: new Date(`${today}T12:00:00Z`),
          scheduleType: ScheduleType.OPERATING,
        },
        {
          parkId,
          date: new Date(`${tomorrow}T12:00:00Z`),
          scheduleType: ScheduleType.CLOSED,
        },
        {
          parkId,
          date: new Date(`${nextWeek}T12:00:00Z`),
          scheduleType: ScheduleType.OPERATING,
        },
      ]);

      await service.storePredictions(predictions);

      const saved = allInserted();
      expect(saved).toHaveLength(2);
      expect(
        saved.find((p) =>
          (p.predictedTime as Date).toISOString().startsWith(tomorrow),
        ),
      ).toBeUndefined();
    });

    it("filters out a CLOSED day even when an event row comes after it (PAR-640)", async () => {
      // The schedule query has no order; an event row read after CLOSED used
      // to overwrite it, and the closed day's prediction was stored.
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
      });
      mockParksService.isParkSeasonal.mockResolvedValue(true);
      const row = (date: string, scheduleType: ScheduleType) => ({
        parkId,
        date: new Date(`${date}T12:00:00Z`),
        scheduleType,
      });
      mockScheduleEntryRepository.find.mockResolvedValue([
        row(today, ScheduleType.TICKETED_EVENT),
        row(today, ScheduleType.OPERATING),
        row(tomorrow, ScheduleType.CLOSED),
        row(tomorrow, ScheduleType.TICKETED_EVENT),
        row(nextWeek, ScheduleType.OPERATING),
      ]);

      await service.storePredictions(predictions);

      const savedDays = allInserted().map((p) =>
        (p.predictedTime as Date).toISOString().slice(0, 10),
      );
      expect(savedDays.sort()).toEqual([today, nextWeek]);
    });

    // The accuracy feedback filter reads `status`, which reached this side as
    // undefined for months because PredictionResponse did not declare it (PAR-117).
    // Everything was therefore recorded, and the filter's own rule was never
    // exercised. These pin the rule itself, in both directions.
    describe("accuracy feedback filter", () => {
      const hourly = (hour: string, status?: string) => ({
        attractionId,
        predictedTime: `${today}T${hour}:00:00Z`,
        predictedWaitTime: 30,
        confidence: 0.9,
        crowdLevel: "moderate" as const,
        baseline: 28,
        trend: "stable",
        modelVersion: "v1.0.0",
        predictionType: "hourly" as const,
        status,
      });

      beforeEach(() => {
        // No schedule history: the storage filter keeps every row, so whatever
        // does not reach recordPredictions was dropped by the feedback filter.
        mockParksService.getOperatingDateRange.mockResolvedValue({
          minDate: null,
          maxDate: null,
        });
        mockParksService.isParkSeasonal.mockResolvedValue(false);
        mockScheduleEntryRepository.find.mockResolvedValue([]);
        mockPredictionAccuracyService.recordPredictions.mockClear();
      });

      it("records OPERATING, UNKNOWN and not-yet-recorded rows alike", async () => {
        // UNKNOWN is a day nobody published a schedule for, in a park a ride wait
        // proved was running — not a scheduled closure. Undefined is every row
        // written before PAR-117 declared the field.
        await service.storePredictions([
          hourly("10", "OPERATING"),
          hourly("11", "UNKNOWN"),
          hourly("12"),
        ]);

        const recorded =
          mockPredictionAccuracyService.recordPredictions.mock.calls[0][0];
        expect(
          recorded.map((p: { status: string | null }) => p.status),
        ).toEqual(["OPERATING", "UNKNOWN", null]);
      });

      it("drops any other status, and every daily row", async () => {
        await service.storePredictions([
          hourly("10", "OPERATING"),
          hourly("11", "CLOSED"),
          {
            ...hourly("12", "OPERATING"),
            predictedTime: `${nextWeek}T12:00:00Z`,
            predictionType: "daily" as const,
          },
        ]);

        const recorded =
          mockPredictionAccuracyService.recordPredictions.mock.calls[0][0];
        expect(recorded).toHaveLength(1);
        expect(recorded[0].predictedTime).toEqual(
          new Date(`${today}T10:00:00Z`),
        );
      });

      it("hands the served values over aligned with the raw rows (PAR-854)", async () => {
        const prev = process.env.SERVE_PCN_INTRADAY;
        delete process.env.SERVE_PCN_INTRADAY;
        const inHours = (h: number) =>
          new Date(currentSlotStartMs() + h * 3_600_000).toISOString();
        try {
          await service.storePredictions([
            { ...hourly("10", "OPERATING"), predictedTime: inHours(2.5) },
            {
              ...hourly("11", "OPERATING"),
              predictedTime: inHours(3),
              predictedWaitTime: 45,
            },
            // Already under way: too close to score as served.
            hourly("12", "OPERATING"),
          ]);
        } finally {
          if (prev !== undefined) process.env.SERVE_PCN_INTRADAY = prev;
        }

        const [raw, served] =
          mockPredictionAccuracyService.recordPredictions.mock.calls[0];
        expect(raw).toHaveLength(3);
        // Flag off: CatBoost IS what was served, and it says so.
        expect(served).toEqual([
          { wait: 30, source: "catboost" },
          { wait: 45, source: "catboost" },
          null,
        ]);
      });

      // PAR-912: the quantiles have no column on the hypertable and travel to
      // the accuracy row beside it — aligned with the rows the feedback filter
      // kept, not with the input.
      it("hands q0.8/q0.95 over aligned with the recorded rows (PAR-912)", async () => {
        await service.storePredictions([
          {
            ...hourly("10", "OPERATING"),
            predictedQ80: 41.6,
            predictedQ95: 70,
          },
          { ...hourly("11", "CLOSED"), predictedQ80: 99, predictedQ95: 99 },
          hourly("12", "UNKNOWN"), // an ml-service without the fields
        ]);

        const [raw, , quantiles] =
          mockPredictionAccuracyService.recordPredictions.mock.calls[0];
        expect(raw).toHaveLength(2);
        expect(quantiles).toEqual([{ q80: 42, q95: 70 }, null]);
      });
    });

    it("should filter out predictions in seasonal gaps", async () => {
      // Seasonal park with operating history but no explicit schedule
      // entries for the predicted days → every prediction falls into a
      // gap and gets skipped, so save is never called.
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: "2026-01-01",
        maxDate: "2026-12-31",
      });
      mockParksService.isParkSeasonal.mockResolvedValue(true);
      mockScheduleEntryRepository.find.mockResolvedValue([]);

      await service.storePredictions(predictions);

      expect(allInserted()).toHaveLength(0);
    });
  });

  // PAR-874: the hourly rows of a park are diff-written — only the slots whose
  // value changed are rewritten — and freshness moved from each row's
  // createdAt to a per-ride Redis marker.
  describe("hourly diff-write and its readers (PAR-874)", () => {
    const parkId = "11111111-1111-4111-8111-111111111111";
    const attractionId = "22222222-2222-4222-8222-222222222222";
    const base = {
      attractionId,
      predictedWaitTime: 30,
      confidence: 68.2,
      uncertaintyMinutes: 12,
      crowdLevel: "moderate" as const,
      baseline: 32.5,
      modelVersion: "v1",
      status: "OPERATING",
      predictionType: "hourly" as const,
    };
    const inHours = (h: number) =>
      new Date(currentSlotStartMs() + h * 3_600_000);
    const pipelineSet = jest.fn();
    const pipelineExec = jest.fn().mockResolvedValue([]);
    let deletes: unknown[][];
    let storedRows: Record<string, unknown>[];
    let storedReads: string[];

    beforeEach(() => {
      insertedValues.length = 0;
      deletes = [];
      storedRows = [];
      storedReads = [];
      mockAttractionRepository.createQueryBuilder.mockReturnValue({
        innerJoin: jest.fn().mockReturnThis(),
        select: jest.fn().mockReturnThis(),
        addSelect: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        getRawMany: jest
          .fn()
          .mockResolvedValue([{ id: attractionId, parkId, timezone: "UTC" }]),
      });
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: null,
        maxDate: null,
      });
      mockParksService.isParkSeasonal.mockResolvedValue(false);
      mockScheduleEntryRepository.find.mockResolvedValue([]);
      mockPredictionRepository.manager.query.mockImplementation(
        (sql: string, params: unknown[]) => {
          if (sql.includes("DELETE FROM wait_time_predictions w")) {
            deletes.push(params);
            return Promise.resolve([
              { affected: String((params[0] as unknown[]).length) },
            ]);
          }
          if (sql.includes("FROM wait_time_predictions p")) {
            storedReads.push(sql);
            return Promise.resolve(storedRows);
          }
          return Promise.resolve([]);
        },
      );
      pipelineSet.mockClear();
      mockRedis.pipeline.mockReturnValue({
        set: pipelineSet,
        exec: pipelineExec,
      });
    });

    afterEach(() => {
      mockPredictionRepository.manager.query.mockReset();
      mockPredictionRepository.manager.query.mockResolvedValue([]);
    });

    it("writes nothing for an unchanged slot and only the changed one otherwise", async () => {
      storedRows = [
        {
          ...base,
          predictedTime: inHours(2),
          createdAt: new Date(Date.now() - 15 * 60_000),
        },
        {
          ...base,
          predictedTime: inHours(3),
          createdAt: new Date(Date.now() - 15 * 60_000),
        },
      ];

      await service.storePredictions(
        [
          { ...base, predictedTime: inHours(2).toISOString() },
          {
            ...base,
            predictedTime: inHours(3).toISOString(),
            predictedWaitTime: 45,
          },
        ],
        { replaceHourlyForPark: parkId },
      );

      // Only the changed slot is inserted…
      expect(allInserted()).toHaveLength(1);
      expect(allInserted()[0]).toMatchObject({
        predictedTime: inHours(3),
        predictedWaitTime: 45,
      });
      // …and only its superseded row deleted, bounded below this run.
      expect(deletes).toHaveLength(1);
      expect(deletes[0][0]).toEqual([attractionId]);
      expect(deletes[0][1]).toEqual([inHours(3).toISOString()]);
      // The ride is vouched for, so the unchanged row stays servable.
      expect(pipelineSet).toHaveBeenCalledWith(
        `ml:hourly-fresh:${attractionId}`,
        expect.any(String),
        "EX",
        1800,
      );
      // Accuracy tracking still sees the whole run, unchanged slots included.
      expect(
        mockPredictionAccuracyService.recordPredictions.mock.calls[0][0],
      ).toHaveLength(2);
    });

    it("reads the stored slots by attraction id, without joining attractions", async () => {
      // `attractions` is the table the nightly drop_chunks takes ACCESS
      // EXCLUSIVE on; a read running ~96 times a day per park must not queue
      // behind it. The park's rides are resolved separately instead.
      await service.storePredictions(
        [{ ...base, predictedTime: inHours(2).toISOString() }],
        { replaceHourlyForPark: parkId },
      );

      expect(storedReads).toHaveLength(1);
      expect(storedReads[0]).not.toMatch(/join\s+attractions/i);
      expect(storedReads[0]).toContain('"attractionId" = ANY($1::uuid[])');
      expect(mockAttractionRepository.find).toHaveBeenCalledWith({
        where: { parkId },
        select: { id: true },
      });
    });

    it("writes nothing at all when the run repeats the stored answer", async () => {
      storedRows = [
        {
          ...base,
          predictedTime: inHours(2),
          createdAt: new Date(Date.now() - 15 * 60_000),
        },
      ];
      await service.storePredictions(
        [{ ...base, predictedTime: inHours(2).toISOString() }],
        { replaceHourlyForPark: parkId },
      );
      expect(insertedValues).toHaveLength(0);
      expect(deletes).toHaveLength(0);
    });

    it("still removes every stored future slot when the schedule filter leaves nothing", async () => {
      // Seasonal park with history but no schedule rows: every slot is a gap.
      mockParksService.getOperatingDateRange.mockResolvedValue({
        minDate: "2026-01-01",
        maxDate: "2099-12-31",
      });
      mockParksService.isParkSeasonal.mockResolvedValue(true);
      storedRows = [
        {
          ...base,
          predictedTime: inHours(2),
          createdAt: new Date(Date.now() - 15 * 60_000),
        },
      ];
      await service.storePredictions(
        [{ ...base, predictedTime: inHours(2).toISOString() }],
        { replaceHourlyForPark: parkId },
      );
      expect(allInserted()).toHaveLength(0);
      expect(deletes).toHaveLength(1);
      expect(
        mockPredictionAccuracyService.recordPredictions,
      ).not.toHaveBeenCalled();
    });

    describe("readers", () => {
      const row = (h: number, createdMinAgo: number, wait = 30) =>
        Object.assign(new WaitTimePrediction(), {
          ...base,
          predictedWaitTime: wait,
          predictedTime: inHours(h),
          createdAt: new Date(Date.now() - createdMinAgo * 60_000),
        });
      const withRows = (rows: WaitTimePrediction[]) => {
        const qb = {
          where: jest.fn().mockReturnThis(),
          andWhere: jest.fn().mockReturnThis(),
          orderBy: jest.fn().mockReturnThis(),
          getMany: jest.fn().mockResolvedValue(rows),
        };
        mockPredictionRepository.createQueryBuilder.mockReturnValueOnce(
          qb as never,
        );
        return qb;
      };
      let prevFlag: string | undefined;
      beforeEach(() => {
        prevFlag = process.env.SERVE_PCN_INTRADAY;
        delete process.env.SERVE_PCN_INTRADAY;
        mockRedis.mget.mockReset();
      });
      afterEach(() => {
        if (prevFlag !== undefined) process.env.SERVE_PCN_INTRADAY = prevFlag;
      });

      it("serves an unchanged row hours old when the generator vouched for the ride", async () => {
        withRows([row(2, 6 * 60)]);
        mockRedis.mget.mockResolvedValue(["2026-10-12T20:45:00.000Z"]);
        const out = await service.getBatchStoredPredictions(
          [attractionId],
          "hourly",
          new Date(currentSlotStartMs()),
        );
        expect(out.get(attractionId)).toHaveLength(1);
      });

      it("hides the rows of a ride nobody vouched for — the old 30-minute rule", async () => {
        withRows([row(2, 6 * 60)]);
        mockRedis.mget.mockResolvedValue([null]);
        const out = await service.getBatchStoredPredictions(
          [attractionId],
          "hourly",
          new Date(currentSlotStartMs()),
        );
        expect(out.get(attractionId)).toBeUndefined();
      });

      it("hides them too when Redis cannot be read and nothing is recent", async () => {
        withRows([row(2, 6 * 60)]);
        mockRedis.mget.mockRejectedValue(new Error("redis down"));
        const out = await service.getBatchStoredPredictions(
          [attractionId],
          "hourly",
          new Date(currentSlotStartMs()),
        );
        expect(out.get(attractionId)).toBeUndefined();
      });

      it("treats a row written in the last 30 minutes as proof, without asking Redis", async () => {
        withRows([row(2, 6 * 60), row(3, 5)]);
        const out = await service.getBatchStoredPredictions(
          [attractionId],
          "hourly",
          new Date(currentSlotStartMs()),
        );
        expect(out.get(attractionId)).toHaveLength(2);
        expect(mockRedis.mget).not.toHaveBeenCalled();
      });

      it("serves one row per slot, the newest", async () => {
        withRows([row(0, 20, 10), row(0, 5, 20), row(1, 5, 30)]);
        const out = await service.getBatchStoredPredictions(
          [attractionId],
          "hourly",
          new Date(currentSlotStartMs()),
        );
        expect(out.get(attractionId)!.map((p) => p.predictedWaitTime)).toEqual([
          20, 30,
        ]);
      });

      it("bounds the scan by the generation horizon, not by the freshness window", async () => {
        const qb = withRows([]);
        await service.getStoredPredictions(attractionId, "hourly");
        const cutoffCall = qb.andWhere.mock.calls.find(([sql]) =>
          String(sql).includes("createdAt"),
        );
        const cutoff = (cutoffCall![1] as { createdAtCutoff: Date })
          .createdAtCutoff;
        expect(
          Math.abs(Date.now() - cutoff.getTime() - 50 * 3_600_000),
        ).toBeLessThan(60_000);
        // …and an hourly read without a start begins at the current slot.
        expect(
          qb.andWhere.mock.calls.some(([sql]) =>
            String(sql).includes("predictedTime >="),
          ),
        ).toBe(true);
      });

      it("leaves the daily window at 26 hours", async () => {
        const qb = withRows([]);
        await service.getStoredPredictions(attractionId, "daily");
        const cutoffCall = qb.andWhere.mock.calls.find(([sql]) =>
          String(sql).includes("createdAt"),
        );
        const cutoff = (cutoffCall![1] as { createdAtCutoff: Date })
          .createdAtCutoff;
        expect(
          Math.abs(Date.now() - cutoff.getTime() - 26 * 3_600_000),
        ).toBeLessThan(60_000);
      });
    });
  });
});
