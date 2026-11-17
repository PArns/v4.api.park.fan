import { Test } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import request from "supertest";
import { ParksService } from "../parks/parks.service";
import { ForecastAccessGuard } from "../api-keys/forecast-access.guard";
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";
import { ParkyTipController, tipCacheControl } from "./parky-tip.controller";
import { ParkyTipService } from "./parky-tip.service";

/** The pipes, the 204 and the header run through the real HTTP pipeline. */
describe("ParkyTipController", () => {
  let app: INestApplication;
  const tip = jest.fn();
  const url = "/v1/parks/europe/germany/bruhl/phantasialand/parky-tip";

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [ParkyTipController],
      providers: [
        { provide: ParkyTipService, useValue: { tip } },
        {
          provide: ParksService,
          useValue: {
            findByGeographicPath: jest
              .fn()
              .mockResolvedValue({ id: "park-1", timezone: "Europe/Berlin" }),
          },
        },
      ],
    })
      .overrideGuard(ForecastAccessGuard)
      .useValue({ canActivate: () => true })
      .compile();
    app = moduleRef.createNestApplication({ logger: false });
    app.setGlobalPrefix("v1");
    app.useGlobalFilters(new HttpExceptionFilter());
    await app.init();
  });

  afterAll(() => app.close());

  it("serves a tip with the edge cache header", async () => {
    const body = {
      date: "2026-12-05",
      verdict: "good",
      crowdLevel: "low",
      text: "Ruhiger Tag!",
      promptVersion: 1,
    };
    tip.mockResolvedValueOnce(body);
    const res = await request(app.getHttpServer())
      .get(url)
      .query({ date: "2026-12-05", locale: "de" })
      .expect(200);
    expect(res.body).toEqual(body);
    // The window depends on how much of Berlin's day is left: tipCacheControl below.
    expect(res.headers["cache-control"]).toMatch(
      /^public, max-age=(\d+), s-maxage=\1(, stale-while-revalidate=3600)?$/,
    );
  });

  it("keeps a tip six hours, never past the park's midnight", () => {
    expect(tipCacheControl(30_000)).toBe(
      "public, max-age=21600, s-maxage=21600, stale-while-revalidate=3600",
    );
    // At 23:00 a tip that says "tomorrow" lives one hour, and is never served stale.
    expect(tipCacheControl(3_600)).toBe("public, max-age=3600, s-maxage=3600");
  });

  it("answers 204 when there is nothing to say", async () => {
    tip.mockResolvedValueOnce(null);
    await request(app.getHttpServer())
      .get(url)
      .query({ date: "2026-12-05", locale: "de" })
      .expect(204);
  });

  it.each([
    [{ date: "2026-02-31", locale: "de" }],
    [{ date: "2026-12-05", locale: "pl" }],
    [{ locale: "de" }],
  ])("refuses %j", async (query) => {
    await request(app.getHttpServer()).get(url).query(query).expect(400);
  });
});
