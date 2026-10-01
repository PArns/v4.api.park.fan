import { Test } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
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
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";

/**
 * `page`, `limit`, `days` and `includeHourly` had no pipe. `?page=abc` reached
 * TypeORM's `skip(NaN)` and came back as HTTP 500, and any `includeHourly`
 * string became part of a Redis key (PAR-415). Runs the real HTTP pipeline:
 * the pipes sit on the routes, so calling the controller method directly
 * would pass without them.
 */
describe("ParksController › query parameters are validated", () => {
  let app: INestApplication;
  let findAllWithFilters: jest.Mock;
  let findAttraction: jest.Mock;
  let buildIntegratedResponse: jest.Mock;
  let buildCalendarResponse: jest.Mock;
  let findByGeographicPath: jest.Mock;
  let getTopParksWithScores: jest.Mock;
  let findByIds: jest.Mock;

  const base = "/v1/parks/europe/germany/bruhl/phantasialand";

  beforeAll(async () => {
    findByGeographicPath = jest.fn().mockResolvedValue({
      id: "park-1",
      slug: "phantasialand",
      timezone: "Europe/Berlin",
    });
    findAllWithFilters = jest.fn().mockResolvedValue({ data: [], total: 0 });
    findAttraction = jest.fn().mockResolvedValue({ id: "a-1" });
    buildIntegratedResponse = jest.fn().mockResolvedValue({});
    buildCalendarResponse = jest.fn().mockResolvedValue({ days: [] });
    getTopParksWithScores = jest.fn().mockResolvedValue([]);
    findByIds = jest.fn().mockResolvedValue([]);

    const noop = {};
    const moduleRef = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        {
          provide: ParksService,
          useValue: { findByGeographicPath, findByIds },
        },
        { provide: ParkIntegrationService, useValue: noop },
        {
          provide: AttractionsService,
          useValue: {
            findAllWithFilters,
            findByGeographicPath: findAttraction,
          },
        },
        { provide: CalendarService, useValue: { buildCalendarResponse } },
        { provide: PlanDayService, useValue: noop },
        { provide: BestDaysService, useValue: noop },
        { provide: WeatherService, useValue: noop },
        { provide: WeatherWarningsService, useValue: noop },
        {
          provide: AttractionIntegrationService,
          useValue: { buildIntegratedResponse },
        },
        { provide: ShowsService, useValue: noop },
        { provide: RestaurantsService, useValue: noop },
        { provide: QueueDataService, useValue: noop },
        { provide: AnalyticsService, useValue: noop },
        { provide: ParkHistoricalStatsService, useValue: noop },
        { provide: MLService, useValue: noop },
        { provide: PredictionAccuracyService, useValue: noop },
        { provide: ParkEnrichmentService, useValue: noop },
        { provide: PopularityService, useValue: { getTopParksWithScores } },
        { provide: ParkRenameService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    app = moduleRef.createNestApplication({ logger: false });
    app.setGlobalPrefix("v1");
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
        transformOptions: { enableImplicitConversion: true },
      }),
    );
    app.useGlobalFilters(new HttpExceptionFilter());
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(() => {
    findAllWithFilters.mockClear();
    buildIntegratedResponse.mockClear();
    buildCalendarResponse.mockClear();
    getTopParksWithScores.mockClear();
  });

  describe("attractions list", () => {
    it.each([
      ["page", "abc"],
      ["page", "1.5"],
      ["page", "1e21"],
      ["page", "Infinity"],
      ["limit", "abc"],
    ])("answers %s=%s with 400 and never queries", async (key, value) => {
      const res = await request(app.getHttpServer())
        .get(`${base}/attractions`)
        .query({ [key]: value });

      expect(res.status).toBe(400);
      expect(findAllWithFilters).not.toHaveBeenCalled();
    });

    it("defaults to page 1, limit 10", async () => {
      const res = await request(app.getHttpServer()).get(`${base}/attractions`);

      expect(res.status).toBe(200);
      expect(findAllWithFilters).toHaveBeenCalledWith(
        expect.objectContaining({ page: 1, limit: 10 }),
      );
    });

    it("clamps page and limit up to 1", async () => {
      const res = await request(app.getHttpServer())
        .get(`${base}/attractions`)
        .query({ page: "0", limit: "-3" });

      expect(res.status).toBe(200);
      expect(findAllWithFilters).toHaveBeenCalledWith(
        expect.objectContaining({ page: 1, limit: 1 }),
      );
    });

    it("clamps limit to 100", async () => {
      const res = await request(app.getHttpServer())
        .get(`${base}/attractions`)
        .query({ page: "2", limit: "100000" });

      expect(res.status).toBe(200);
      expect(findAllWithFilters).toHaveBeenCalledWith(
        expect.objectContaining({ page: 2, limit: 100 }),
      );
    });
  });

  describe("popular parks", () => {
    const url = "/v1/parks/popular";

    it.each([
      [undefined, 20],
      ["abc", 20],
      ["5", 5],
      ["0", 1],
      ["-3", 1],
      ["100", 100],
      ["100000", 100],
    ])("limit=%s reads %s ranked parks", async (limit, expected) => {
      const res = await request(app.getHttpServer())
        .get(url)
        .query(limit === undefined ? {} : { limit });

      expect(res.status).toBe(200);
      expect(getTopParksWithScores).toHaveBeenCalledWith(expected);
    });
  });

  describe("attraction detail", () => {
    const url = `${base}/attractions/taron`;

    it.each(["abc", "1.5"])("answers days=%s with 400", async (days) => {
      const res = await request(app.getHttpServer()).get(url).query({ days });

      expect(res.status).toBe(400);
      expect(buildIntegratedResponse).not.toHaveBeenCalled();
    });

    it.each([
      [undefined, 30],
      ["7", 7],
      ["0", 1],
      ["9999", 365],
    ])("days=%s reads %s days of history", async (days, expected) => {
      const res = await request(app.getHttpServer())
        .get(url)
        .query(days === undefined ? {} : { days });

      expect(res.status).toBe(200);
      expect(buildIntegratedResponse).toHaveBeenCalledWith(
        expect.anything(),
        expected,
      );
    });
  });

  describe("calendar", () => {
    const url = `${base}/calendar`;

    it("answers an unknown includeHourly with 400 and never builds", async () => {
      const res = await request(app.getHttpServer())
        .get(url)
        .query({ includeHourly: "foo" });

      expect(res.status).toBe(400);
      expect(res.body.message).toContain("today+tomorrow");
      expect(buildCalendarResponse).not.toHaveBeenCalled();
    });

    it.each(["today+tomorrow", "today", "none", "all"])(
      "passes includeHourly=%s through",
      async (includeHourly) => {
        const res = await request(app.getHttpServer())
          .get(url)
          .query({ includeHourly });

        expect(res.status).toBe(200);
        expect(buildCalendarResponse).toHaveBeenCalledWith(
          expect.anything(),
          expect.anything(),
          expect.anything(),
          includeHourly,
        );
      },
    );

    it("defaults to today+tomorrow", async () => {
      await request(app.getHttpServer()).get(url);

      expect(buildCalendarResponse).toHaveBeenCalledWith(
        expect.anything(),
        expect.anything(),
        expect.anything(),
        "today+tomorrow",
      );
    });
  });
});
