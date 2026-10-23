import { Controller, Get, INestApplication } from "@nestjs/common";
import { ValidationPipe } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import request from "supertest";
import { QueryInt } from "./query-int.pipe";

@Controller("probe")
class ProbeController {
  @Get()
  get(
    @QueryInt({ name: "limit", fallback: 6, min: 0 })
    limit: number,
  ) {
    return { limit };
  }
}

/**
 * Runs behind the same global `ValidationPipe` as `main.ts`, which converts a
 * `number`-typed parameter before `QueryIntPipe` sees it.
 */
describe("QueryIntPipe behind the global ValidationPipe", () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [ProbeController],
    }).compile();
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

  afterAll(async () => {
    await app.close();
  });

  it.each([
    ["/probe", 6],
    ["/probe?limit=", 6],
    ["/probe?limit=0", 0],
    ["/probe?limit=12", 12],
  ])("%s answers limit=%d", async (url, expected) => {
    const res = await request(app.getHttpServer()).get(url).expect(200);
    expect(res.body).toEqual({ limit: expected });
  });

  it("answers 400 for a non-integer", async () => {
    await request(app.getHttpServer()).get("/probe?limit=abc").expect(400);
  });
});
