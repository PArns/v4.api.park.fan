import { Test } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { MLController } from "./ml.controller";
import { MLDashboardService } from "../services/ml-dashboard.service";
import { MLModelService } from "../services/ml-model.service";
import { PredictionAccuracyService } from "../services/prediction-accuracy.service";
import { HttpExceptionFilter } from "../../common/filters/http-exception.filter";

/**
 * `limit` on `models/history` and `models/metrics-history` and `days` on
 * `accuracy/system` had no upper bound (PAR-609): `limit` went straight into
 * TypeORM's `take`, and every `days` value is its own Redis key. Runs the real
 * HTTP pipeline, because the pipes sit on the routes.
 */
describe("MLController › query parameters are bounded", () => {
  let app: INestApplication;
  let getModelHistory: jest.Mock;
  let getMetricsHistory: jest.Mock;
  let getSystemAccuracyStats: jest.Mock;

  beforeAll(async () => {
    getModelHistory = jest.fn().mockResolvedValue([]);
    getMetricsHistory = jest.fn().mockResolvedValue({ history: [], total: 0 });
    getSystemAccuracyStats = jest.fn().mockResolvedValue({
      overall: { mae: 5, matchedPredictions: 100 },
      byPredictionType: {},
    });

    const moduleRef = await Test.createTestingModule({
      controllers: [MLController],
      providers: [
        { provide: MLDashboardService, useValue: {} },
        {
          provide: MLModelService,
          useValue: { getModelHistory, getMetricsHistory },
        },
        {
          provide: PredictionAccuracyService,
          useValue: {
            getSystemAccuracyStats,
            getTopBottomPerformers: jest.fn().mockResolvedValue({
              topPerformers: [],
              bottomPerformers: [],
            }),
            getServedIntradayAccuracy: jest.fn().mockResolvedValue(null),
            calculateAccuracyBadge: jest
              .fn()
              .mockReturnValue({ badge: "good", message: "" }),
          },
        },
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
    getModelHistory.mockClear();
    getMetricsHistory.mockClear();
    getSystemAccuracyStats.mockClear();
  });

  describe.each([
    ["/v1/ml/models/history", () => getModelHistory, 10],
    ["/v1/ml/models/metrics-history", () => getMetricsHistory, 30],
  ])("%s", (url, service, fallback) => {
    it.each([
      [undefined, fallback],
      ["5", 5],
      ["50", 50],
      ["0", 1],
      ["-3", 1],
      ["100", 100],
      ["100000", 100],
    ])("limit=%s reads %s models", async (limit, expected) => {
      const res = await request(app.getHttpServer())
        .get(url)
        .query(limit === undefined ? {} : { limit });

      expect(res.status).toBe(200);
      expect(service()).toHaveBeenCalledWith(expected);
    });

    it.each(["abc", "1.5"])(
      "answers limit=%s with 400 and never queries",
      async (limit) => {
        const res = await request(app.getHttpServer())
          .get(url)
          .query({ limit });

        expect(res.status).toBe(400);
        expect(service()).not.toHaveBeenCalled();
      },
    );
  });

  describe("accuracy/system", () => {
    const url = "/v1/ml/accuracy/system";

    it.each([
      [undefined, 7],
      ["30", 30],
      ["0", 1],
      ["-5", 1],
      ["365", 365],
      ["100000", 365],
    ])("days=%s reads %s days", async (days, expected) => {
      const res = await request(app.getHttpServer())
        .get(url)
        .query(days === undefined ? {} : { days });

      expect(res.status).toBe(200);
      expect(getSystemAccuracyStats).toHaveBeenCalledWith(expected);
      expect(res.body.period).toBe(`Last ${expected} days`);
    });

    it("answers days=abc with 400 and never queries", async () => {
      const res = await request(app.getHttpServer())
        .get(url)
        .query({ days: "abc" });

      expect(res.status).toBe(400);
      expect(getSystemAccuracyStats).not.toHaveBeenCalled();
    });
  });
});
