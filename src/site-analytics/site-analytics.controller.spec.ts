import {
  Body,
  Controller,
  INestApplication,
  MiddlewareConsumer,
  Module,
  NestModule,
  Post,
  ValidationPipe,
} from "@nestjs/common";
import { Test } from "@nestjs/testing";
import request from "supertest";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";
import {
  AdminSiteAnalyticsController,
  SiteAnalyticsCollectController,
} from "./site-analytics.controller";
import { SiteAnalyticsCollectService } from "./site-analytics-collect.service";
import { SiteAnalyticsService } from "./site-analytics.service";
import { COLLECT_ROUTE } from "./site-analytics.module";
import { textBodyMiddleware } from "./text-body.middleware";

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15";
const hit = {
  t: "pageview",
  locale: "de",
  path: "/de",
  pageType: "home",
  viewport: "desktop",
};
const body = JSON.stringify({ v: 1, hits: [hit] });

/** Shows what another route receives for a text body. */
@Controller("echo")
class EchoController {
  @Post()
  echo(@Body() received: unknown) {
    return { type: typeof received };
  }
}

const collect = jest.fn().mockResolvedValue(1);
const reports = {
  overview: jest.fn().mockResolvedValue({ ok: "overview" }),
  breakdown: jest.fn().mockResolvedValue({ ok: "breakdown" }),
  events: jest.fn().mockResolvedValue({ ok: "events" }),
  event: jest.fn().mockResolvedValue({ ok: "event" }),
  search: jest.fn().mockResolvedValue({ ok: "search" }),
};

@Module({
  controllers: [
    SiteAnalyticsCollectController,
    AdminSiteAnalyticsController,
    EchoController,
  ],
  providers: [
    { provide: SiteAnalyticsCollectService, useValue: { collect } },
    { provide: SiteAnalyticsService, useValue: reports },
  ],
})
class TestModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(textBodyMiddleware()).forRoutes(COLLECT_ROUTE);
  }
}

/**
 * The real HTTP pipeline, because the text body, the validation and the
 * prefix all sit outside the handler. What is pinned: `sendBeacon`'s
 * `text/plain` and a JSON body both reach the collector, on this route only;
 * a malformed body is 400 and an oversized one 413; Do Not Track, Global
 * Privacy Control and, in production, a foreign origin are answered 204 and
 * not counted; the admin reports validate their windows.
 */
describe("SiteAnalyticsController", () => {
  let app: INestApplication;
  const previousEnv = process.env.NODE_ENV;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [TestModule],
    })
      .overrideGuard(AdminAuthGuard)
      .useValue({ canActivate: () => true })
      .compile();
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
    collect.mockClear();
    for (const fn of Object.values(reports)) fn.mockClear();
    process.env.NODE_ENV = previousEnv;
  });

  afterEach(() => {
    process.env.NODE_ENV = previousEnv;
  });

  const post = () =>
    request(app.getHttpServer())
      .post("/v1/analytics/collect")
      .set("User-Agent", UA)
      .set("CF-Connecting-IP", "203.0.113.7");

  describe("POST /v1/analytics/collect", () => {
    it("reads the text/plain body sendBeacon sends and answers 204, uncacheable", async () => {
      const response = await post()
        .set("Content-Type", "text/plain;charset=UTF-8")
        .send(body)
        .expect(204);
      expect(response.headers["cache-control"]).toContain("no-store");
      expect(collect).toHaveBeenCalledWith({
        hits: [hit],
        ip: "203.0.113.7",
        userAgent: UA,
      });
    });

    it("reads a JSON body too", async () => {
      await post()
        .set("Content-Type", "application/json")
        .send(body)
        .expect(204);
      expect(collect).toHaveBeenCalledTimes(1);
    });

    it("answers 204 whatever the hits hold: the collector drops them one by one", async () => {
      await post()
        .set("Content-Type", "text/plain")
        .send(JSON.stringify({ v: 1, hits: [{ t: "nonsense" }, 3] }))
        .expect(204);
      expect(collect).toHaveBeenCalledWith(
        expect.objectContaining({ hits: [{ t: "nonsense" }, 3] }),
      );
    });

    it("refuses a body that is not JSON or not the shape", async () => {
      for (const [type, payload] of [
        ["text/plain", "not json"],
        ["text/plain", JSON.stringify({ v: 2, hits: [hit] })],
        ["text/plain", JSON.stringify({ v: 1, hits: [] })],
        ["text/plain", JSON.stringify({ v: 1, hits: Array(21).fill(hit) })],
        ["application/json", "{not json"],
        ["application/json", JSON.stringify([hit])],
      ]) {
        await post().set("Content-Type", type).send(payload).expect(400);
      }
      await post().expect(400);
      expect(collect).not.toHaveBeenCalled();
    });

    it("refuses a body over 16 KiB", async () => {
      await post()
        .set("Content-Type", "text/plain")
        .send(JSON.stringify({ v: 1, hits: [hit], pad: "x".repeat(17000) }))
        .expect(413);
      expect(collect).not.toHaveBeenCalled();
    });

    it("counts nothing for Do Not Track or Global Privacy Control", async () => {
      for (const [header, value] of [
        ["DNT", "1"],
        ["Sec-GPC", "1"],
      ]) {
        await post()
          .set("Content-Type", "text/plain")
          .set(header, value)
          .send(body)
          .expect(204);
      }
      expect(collect).not.toHaveBeenCalled();
    });

    it("counts in production only what park.fan sent", async () => {
      process.env.NODE_ENV = "production";
      await post().set("Content-Type", "text/plain").send(body).expect(204);
      await post()
        .set("Content-Type", "text/plain")
        .set("Origin", "https://park-abc-arns.vercel.app")
        .send(body)
        .expect(204);
      await post()
        .set("Content-Type", "text/plain")
        .set("Referer", "https://park.fan.evil.example/")
        .send(body)
        .expect(204);
      expect(collect).not.toHaveBeenCalled();

      await post()
        .set("Content-Type", "text/plain")
        .set("Origin", "https://park.fan")
        .send(body)
        .expect(204);
      await post()
        .set("Content-Type", "text/plain")
        .set("Referer", "https://www.park.fan/de/parks")
        .send(body)
        .expect(204);
      expect(collect).toHaveBeenCalledTimes(2);
    });

    it("leaves every other route refusing a text body", async () => {
      const response = await request(app.getHttpServer())
        .post("/v1/echo")
        .set("Content-Type", "text/plain")
        .send(body)
        .expect(201);
      expect(response.body).toEqual({ type: "undefined" });
    });
  });

  describe("admin reports", () => {
    const get = (url: string) => request(app.getHttpServer()).get(url);

    it("serves each report with the default window", async () => {
      await get("/v1/admin/analytics/overview").expect(200, {
        ok: "overview",
      });
      expect(reports.overview).toHaveBeenCalledWith(
        expect.objectContaining({ days: 30 }),
      );
      await get("/v1/admin/analytics/breakdown?dim=referrer").expect(200);
      expect(reports.breakdown).toHaveBeenCalledWith(
        expect.objectContaining({ dim: "referrer", days: 30, limit: 50 }),
      );
      await get("/v1/admin/analytics/events?days=365&locale=nl").expect(200);
      expect(reports.events).toHaveBeenCalledWith(
        expect.objectContaining({ days: 365, locale: "nl" }),
      );
      await get("/v1/admin/analytics/events/search_opened?days=7").expect(200);
      expect(reports.event).toHaveBeenCalledWith(
        "search_opened",
        expect.objectContaining({ days: 7 }),
      );
      await get("/v1/admin/analytics/search?days=1").expect(200);
      expect(reports.search).toHaveBeenCalledWith(
        expect.objectContaining({ days: 1 }),
      );
    });

    it("refuses a window, a language, a dimension or a limit outside the lists", async () => {
      for (const url of [
        "/v1/admin/analytics/overview?days=0",
        "/v1/admin/analytics/overview?days=366",
        "/v1/admin/analytics/overview?locale=pt",
        "/v1/admin/analytics/breakdown",
        "/v1/admin/analytics/breakdown?dim=user",
        "/v1/admin/analytics/breakdown?dim=path&limit=201",
        "/v1/admin/analytics/search?days=abc",
      ]) {
        await get(url).expect(400);
      }
      expect(reports.overview).not.toHaveBeenCalled();
      expect(reports.breakdown).not.toHaveBeenCalled();
      expect(reports.search).not.toHaveBeenCalled();
    });
  });
});
