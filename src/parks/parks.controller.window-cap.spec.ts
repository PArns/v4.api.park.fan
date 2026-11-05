import { Test, TestingModule } from "@nestjs/testing";
import { BadRequestException } from "@nestjs/common";
import { ParksController } from "./parks.controller";
import { ParksService } from "./parks.service";
import { WeatherService } from "./weather.service";
import { WeatherWarningsService } from "./weather-warnings.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AttractionIntegrationService } from "../attractions/services/attraction-integration.service";
import { ShowsService } from "../shows/shows.service";
import { RestaurantsService } from "../restaurants/restaurants.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { ParkHistoricalStatsService } from "../analytics/park-historical-stats.service";
import { MLService } from "../ml/ml.service";
import { PredictionAccuracyService } from "../ml/services/prediction-accuracy.service";
import { ParkIntegrationService } from "./services/park-integration.service";
import { ParkEnrichmentService } from "./services/park-enrichment.service";
import { CalendarService } from "./services/calendar.service";
import { PlanDayService } from "./services/plan-day.service";
import { BestDaysService } from "./services/best-days.service";
import { PopularityService } from "../popularity/popularity.service";
import { ParkRenameService } from "./services/park-rename.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Park } from "./entities/park.entity";

/**
 * The 90-day cap on /schedule and /weather: a window at the limit is served, a
 * wider one is a 400 before any service call (so no derivedHours Redis key and
 * no history query is created for it). Windows avoid the Oct 25 DST change.
 */
describe("ParksController › schedule and weather window cap", () => {
  let controller: ParksController;
  let parksService: {
    findByGeographicPath: jest.Mock;
    getSchedule: jest.Mock;
    hasOperatingSchedule: jest.Mock;
    getDerivedHistoricalHours: jest.Mock;
  };
  let weatherService: { getWeatherData: jest.Mock };

  const park = {
    id: "park-1",
    name: "Phantasialand",
    slug: "phantasialand",
    timezone: "Europe/Berlin",
  } as unknown as Park;

  beforeEach(async () => {
    parksService = {
      findByGeographicPath: jest.fn().mockResolvedValue(park),
      getSchedule: jest.fn().mockResolvedValue([]),
      hasOperatingSchedule: jest.fn().mockResolvedValue(false),
      getDerivedHistoricalHours: jest.fn().mockResolvedValue([]),
    };
    weatherService = { getWeatherData: jest.fn().mockResolvedValue([]) };

    const noop = {};
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        { provide: ParksService, useValue: parksService },
        { provide: WeatherService, useValue: weatherService },
        { provide: BestDaysService, useValue: noop },
        { provide: WeatherWarningsService, useValue: noop },
        { provide: AttractionsService, useValue: noop },
        { provide: AttractionIntegrationService, useValue: noop },
        { provide: ShowsService, useValue: noop },
        { provide: RestaurantsService, useValue: noop },
        { provide: QueueDataService, useValue: noop },
        { provide: AnalyticsService, useValue: noop },
        { provide: ParkHistoricalStatsService, useValue: noop },
        { provide: MLService, useValue: noop },
        { provide: PredictionAccuracyService, useValue: noop },
        { provide: ParkIntegrationService, useValue: noop },
        { provide: ParkEnrichmentService, useValue: noop },
        { provide: CalendarService, useValue: noop },
        { provide: PlanDayService, useValue: noop },
        { provide: PopularityService, useValue: noop },
        { provide: ParkRenameService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    controller = module.get<ParksController>(ParksController);
  });

  const call = (
    fn: "getScheduleByGeographicPath" | "getWeatherByGeographicPath",
    from?: string,
    to?: string,
  ) => controller[fn]("europe", "germany", "bruhl", "phantasialand", from, to);

  describe.each([
    ["schedule", "getScheduleByGeographicPath"],
    ["weather", "getWeatherByGeographicPath"],
  ] as const)("%s", (_name, fn) => {
    it("serves a window of 90 calendar days (both ends included)", async () => {
      await expect(call(fn, "2026-07-14", "2026-10-11")).resolves.toBeDefined();
    });

    it("rejects a window of 91 calendar days with 400", async () => {
      await expect(call(fn, "2026-07-14", "2026-10-12")).rejects.toBeInstanceOf(
        BadRequestException,
      );
      expect(parksService.getSchedule).not.toHaveBeenCalled();
      expect(weatherService.getWeatherData).not.toHaveBeenCalled();
    });

    it("rejects from=2000-01-01 with 400", async () => {
      await expect(call(fn, "2000-01-01", "2026-12-31")).rejects.toBeInstanceOf(
        BadRequestException,
      );
    });

    it("serves the default window", async () => {
      await expect(call(fn)).resolves.toBeDefined();
    });
  });

  describe("weather without `to` (PAR-1136)", () => {
    afterEach(() => jest.useRealTimers());

    it("defaults to the last 30 days up to the park's today", async () => {
      jest.useFakeTimers({ now: new Date("2026-11-05T12:00:00Z") });

      await call("getWeatherByGeographicPath");

      const [, from, to] = weatherService.getWeatherData.mock.calls[0];
      // Berlin is UTC+1 in November: 2026-10-06 00:00 to 2026-11-05 23:59:59.
      expect(from.toISOString()).toBe("2026-10-05T22:00:00.000Z");
      expect(to.toISOString()).toBe("2026-11-05T22:59:59.000Z");
    });
  });
});

describe("ParksController › yearly predictions during an ML outage (PAR-1136)", () => {
  function yearly(modelVersion: string) {
    const controller = Object.create(ParksController.prototype);
    Object.assign(controller, {
      parksService: {
        findByGeographicPath: jest.fn().mockResolvedValue({
          id: "p",
          name: "P",
          slug: "p",
          timezone: "UTC",
        }),
      },
      mlService: {
        getParkPredictionsYearly: jest
          .fn()
          .mockResolvedValue({ predictions: [], count: 0, modelVersion }),
      },
      parkIntegrationService: {
        aggregateDailyPredictions: jest.fn().mockResolvedValue([]),
        getYearlyHolidays: jest.fn().mockResolvedValue(null),
      },
    });
    return (controller as ParksController).getYearlyPredictionsByGeographicPath(
      "e",
      "c",
      "c",
      "p",
    );
  }

  it("flags an unreadable forecast as degraded", async () => {
    await expect(yearly("unavailable")).resolves.toMatchObject({
      predictions: [],
      isDegraded: true,
    });
  });

  it("leaves a real (even empty) forecast unflagged", async () => {
    expect(await yearly("serving-v1")).not.toHaveProperty("isDegraded");
  });
});
