import { Test } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { PageViewsController } from "./page-views.controller";
import { PageViewsService } from "./page-views.service";
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";

/**
 * Runs the real HTTP pipeline, because the validation sits in the global pipe
 * and a direct method call would skip it. What is pinned: only a known kind,
 * a known language and a slug-shaped key reach the service, the write is a 204
 * either way and never CDN-cacheable, and the address and User-Agent the
 * service sees are the request's own.
 */
describe("PageViewsController", () => {
  let app: INestApplication;
  let record: jest.Mock;
  let top: jest.Mock;

  beforeAll(async () => {
    record = jest.fn().mockResolvedValue(true);
    top = jest.fn().mockResolvedValue([{ key: "a", views: 3 }]);

    const moduleRef = await Test.createTestingModule({
      controllers: [PageViewsController],
      providers: [
        {
          provide: PageViewsService,
          useValue: {
            record,
            top,
            windowStart: (days: number) => `start-${days}`,
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
    record.mockClear();
    top.mockClear();
  });

  it("counts a well-formed view and answers 204, uncacheable", async () => {
    const response = await request(app.getHttpServer())
      .post("/v1/page-views")
      .set("User-Agent", "Mozilla/5.0 (KHTML, like Gecko) Safari/605.1.15")
      .set("CF-Connecting-IP", "203.0.113.7")
      .send({ kind: "article", key: "tokyo-disney-resort-guide", locale: "de" })
      .expect(204);
    expect(response.headers["cache-control"]).toContain("no-store");
    expect(record).toHaveBeenCalledWith({
      kind: "article",
      key: "tokyo-disney-resort-guide",
      locale: "de",
      ip: "203.0.113.7",
      userAgent: "Mozilla/5.0 (KHTML, like Gecko) Safari/605.1.15",
    });
  });

  it("answers 204 for a view the service did not count", async () => {
    record.mockResolvedValueOnce(false);
    await request(app.getHttpServer())
      .post("/v1/page-views")
      .send({ kind: "news", key: "a-news-post", locale: "en" })
      .expect(204);
  });

  it("refuses an unknown kind or language, a malformed key and extra fields", async () => {
    for (const body of [
      { kind: "glossary", key: "coaster", locale: "de" },
      { kind: "news", key: "coaster", locale: "pt" },
      { kind: "news", key: "Has Spaces", locale: "de" },
      { kind: "news", key: "../etc/passwd", locale: "de" },
      { kind: "news", key: "x".repeat(121), locale: "de" },
      { kind: "news", key: "-leading-hyphen", locale: "de" },
      { kind: "news", key: "ok-key", locale: "de", url: "https://example.com" },
      { kind: "news", locale: "de" },
    ]) {
      await request(app.getHttpServer())
        .post("/v1/page-views")
        .send(body)
        .expect(400);
    }
    expect(record).not.toHaveBeenCalled();
  });

  it("refuses a park or a ride whose key is no UUID", async () => {
    for (const body of [
      { kind: "park", key: "europa-park", locale: "de" },
      { kind: "attraction", key: "junk", locale: "de" },
      {
        kind: "park",
        key: "3F1B2C4D-5E6F-4A7B-8C9D-0E1F2A3B4C5D",
        locale: "de",
      },
    ]) {
      await request(app.getHttpServer())
        .post("/v1/page-views")
        .send(body)
        .expect(400);
    }
    expect(record).not.toHaveBeenCalled();
  });

  it("counts nothing for a browser that sends Do Not Track or Global Privacy Control", async () => {
    for (const [header, value] of [
      ["DNT", "1"],
      ["Sec-GPC", "1"],
    ]) {
      await request(app.getHttpServer())
        .post("/v1/page-views")
        .set(header, value)
        .send({ kind: "news", key: "a-news-post", locale: "de" })
        .expect(204);
    }
    expect(record).not.toHaveBeenCalled();
  });

  it("accepts a park's UUID as the key", async () => {
    await request(app.getHttpServer())
      .post("/v1/page-views")
      .send({
        kind: "park",
        key: "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d",
        locale: "nl",
      })
      .expect(204);
    expect(record).toHaveBeenCalledTimes(1);
  });

  it("serves a ranking with its defaults", async () => {
    const response = await request(app.getHttpServer())
      .get("/v1/page-views/top?kind=news&locale=de")
      .expect(200);
    expect(response.body).toEqual({
      kind: "news",
      locale: "de",
      days: 30,
      from: "start-30",
      items: [{ key: "a", views: 3 }],
    });
    expect(top).toHaveBeenCalledWith("news", "de", 30, 10, expect.any(Date));
  });

  it("refuses a ranking without a language, outside the fixed windows or past 50 entries", async () => {
    for (const url of [
      "/v1/page-views/top?kind=news",
      "/v1/page-views/top?kind=news&locale=de&days=91",
      "/v1/page-views/top?kind=news&locale=de&days=14",
      "/v1/page-views/top?kind=news&locale=de&limit=0",
      "/v1/page-views/top?kind=news&locale=de&limit=51",
    ]) {
      await request(app.getHttpServer()).get(url).expect(400);
    }
    expect(top).not.toHaveBeenCalled();
  });

  it("serves the 7-day window the news list asks for", async () => {
    await request(app.getHttpServer())
      .get("/v1/page-views/top?kind=news&locale=fr&days=7&limit=40")
      .expect(200);
    expect(top).toHaveBeenCalledWith("news", "fr", 7, 40, expect.any(Date));
  });
});
