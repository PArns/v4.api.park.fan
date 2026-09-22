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
import { QueueType } from "../external-apis/themeparks/themeparks.types";

/**
 * `queueType` on the wait-times route is a Postgres enum filter. It had no
 * pipe, so `?queueType=NOT_A_TYPE` went through to the query and came back as
 * HTTP 500 with `invalid input value for enum queue_data_queuetype_enum`
 * (measured on production, 2026-09-22). A caller's typo is a 400, and the
 * response must not name a database type (PAR-401).
 *
 * This runs the real HTTP pipeline: the pipe only exists on the route, so
 * calling the controller method directly would pass without it.
 */
describe("ParksController › wait-times rejects an unknown queueType", () => {
  let app: INestApplication;
  let findByGeographicPath: jest.Mock;
  let getParkWaitTimesResponse: jest.Mock;

  const url = "/v1/parks/europe/germany/bruhl/phantasialand/wait-times";

  beforeAll(async () => {
    findByGeographicPath = jest.fn().mockResolvedValue({
      id: "park-1",
      slug: "phantasialand",
      timezone: "Europe/Berlin",
    });
    getParkWaitTimesResponse = jest.fn().mockResolvedValue({ attractions: [] });

    const noop = {};
    const moduleRef = await Test.createTestingModule({
      controllers: [ParksController],
      providers: [
        { provide: ParksService, useValue: { findByGeographicPath } },
        {
          provide: ParkIntegrationService,
          useValue: { getParkWaitTimesResponse },
        },
        { provide: AttractionsService, useValue: noop },
        { provide: CalendarService, useValue: noop },
        { provide: PlanDayService, useValue: noop },
        { provide: BestDaysService, useValue: noop },
        { provide: WeatherService, useValue: noop },
        { provide: WeatherWarningsService, useValue: noop },
        { provide: AttractionIntegrationService, useValue: noop },
        { provide: ShowsService, useValue: noop },
        { provide: RestaurantsService, useValue: noop },
        { provide: QueueDataService, useValue: noop },
        { provide: AnalyticsService, useValue: noop },
        { provide: ParkHistoricalStatsService, useValue: noop },
        { provide: MLService, useValue: noop },
        { provide: PredictionAccuracyService, useValue: noop },
        { provide: ParkEnrichmentService, useValue: noop },
        { provide: PopularityService, useValue: noop },
        { provide: ParkRenameService, useValue: noop },
        { provide: REDIS_CLIENT, useValue: noop },
      ],
    }).compile();

    app = moduleRef.createNestApplication({ logger: false });
    // Mirror main.ts: the global prefix, pipe and filter every request meets.
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
    findByGeographicPath.mockClear();
    getParkWaitTimesResponse.mockClear();
  });

  it("answers an unknown value with 400 and never queries", async () => {
    const res = await request(app.getHttpServer())
      .get(url)
      .query({ queueType: "NOT_A_TYPE" });

    expect(res.status).toBe(400);
    expect(JSON.stringify(res.body)).not.toMatch(/queuetype_enum/i);
    expect(res.body.message).toContain("STANDBY");
    expect(getParkWaitTimesResponse).not.toHaveBeenCalled();
    expect(findByGeographicPath).not.toHaveBeenCalled();
  });

  it("rejects a value in the wrong case", async () => {
    const res = await request(app.getHttpServer())
      .get(url)
      .query({ queueType: "standby" });

    expect(res.status).toBe(400);
  });

  it.each(Object.values(QueueType))("passes %s through", async (qt) => {
    const res = await request(app.getHttpServer())
      .get(url)
      .query({ queueType: qt });

    expect(res.status).toBe(200);
    expect(getParkWaitTimesResponse).toHaveBeenCalledWith(
      expect.objectContaining({ id: "park-1" }),
      qt,
    );
  });

  it("keeps the filter optional", async () => {
    const res = await request(app.getHttpServer()).get(url);

    expect(res.status).toBe(200);
    expect(getParkWaitTimesResponse).toHaveBeenCalledWith(
      expect.objectContaining({ id: "park-1" }),
      undefined,
    );
  });
});
