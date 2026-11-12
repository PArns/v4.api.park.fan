import { INestApplication, ValidationPipe } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import request from "supertest";
import { ForecastAccessGuard } from "../api-keys/forecast-access.guard";
import { PlanOptimizeController } from "./plan-optimize.controller";
import { PlanOptimizeService } from "./plan-optimize.service";

/**
 * The route over HTTP, through the pipe `main.ts` mounts: where it is, that its answer is never
 * cached, and that a malformed body is a 400 before the service runs. The key check is the
 * guard's own business and has its own specs.
 */
describe("PlanOptimizeController", () => {
  let app: INestApplication;
  const optimize = jest.fn();

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [PlanOptimizeController],
      providers: [{ provide: PlanOptimizeService, useValue: { optimize } }],
    })
      .overrideGuard(ForecastAccessGuard)
      .useValue({ canActivate: () => true })
      .compile();
    app = moduleRef.createNestApplication();
    app.useGlobalPipes(
      new ValidationPipe({
        whitelist: true,
        forbidNonWhitelisted: true,
        transform: true,
        transformOptions: { enableImplicitConversion: true },
      }),
    );
    await app.init();
  });

  afterAll(() => app.close());
  beforeEach(() => optimize.mockReset());

  const path = "/parks/europe/germany/bruehl/phantasialand/plan/optimize";

  it("answers the service's plan, never cached", async () => {
    optimize.mockResolvedValue({ date: "2026-11-14", status: "unchanged" });
    const response = await request(app.getHttpServer())
      .post(path)
      .send({
        date: "2026-11-14",
        mode: "day",
        entries: [{ id: "a", attractionSlug: "taron", startMinute: 600 }],
      })
      .expect(200);
    expect(response.headers["cache-control"]).toBe("no-store");
    expect(response.body.status).toBe("unchanged");
    expect(optimize).toHaveBeenCalledWith(
      {
        continent: "europe",
        country: "germany",
        city: "bruehl",
        parkSlug: "phantasialand",
      },
      expect.objectContaining({ mode: "day" }),
    );
  });

  it("refuses a malformed body before the service runs", async () => {
    await request(app.getHttpServer())
      .post(path)
      .send({ date: "2026-11-14", mode: "day", entries: [{ id: "a" }] })
      .expect(400);
    expect(optimize).not.toHaveBeenCalled();
  });
});
