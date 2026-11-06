import { Test } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { PageViewsController } from "./page-views.controller";
import { PageViewsService } from "./page-views.service";
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";

/**
 * Runs the real HTTP pipeline, because the validation sits in the global pipe
 * and a direct method call would skip it. What is pinned: a ranking needs a
 * known kind and language and stays inside the fixed windows, and the old
 * beacon's write is gone.
 */
describe("PageViewsController", () => {
  let app: INestApplication;
  let top: jest.Mock;

  beforeAll(async () => {
    top = jest.fn().mockResolvedValue([{ key: "a", views: 3 }]);

    const moduleRef = await Test.createTestingModule({
      controllers: [PageViewsController],
      providers: [
        {
          provide: PageViewsService,
          useValue: {
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
    top.mockClear();
  });

  it("no longer takes the old beacon's write", async () => {
    await request(app.getHttpServer())
      .post("/v1/page-views")
      .send({ kind: "news", key: "a-news-post", locale: "de" })
      .expect(404);
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
