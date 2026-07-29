// `marked` ships ESM only and AppService imports it at module load; the unit jest
// config (unlike test/jest-e2e.json) has no moduleNameMapper for it.
jest.mock("marked", () => ({ marked: { parse: (text: string) => text } }));

import { Test } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import request from "supertest";
import { AppController } from "./app.controller";
import { AppService } from "./app.service";

/**
 * `/robots.txt` must resolve at the HOST ROOT. It is served by the root controller,
 * so it only works while it stays in `setGlobalPrefix`'s exclude list — drop it there
 * and the route silently moves to `/v1/robots.txt`, which no crawler ever requests,
 * putting the 404 warning straight back in the log. That is the regression this guards.
 */
describe("AppController — robots.txt", () => {
  let app: INestApplication;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [AppController],
      providers: [AppService],
    }).compile();

    app = moduleRef.createNestApplication();
    // Mirror main.ts exactly.
    app.setGlobalPrefix("v1", { exclude: ["/", "/robots.txt"] });
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  it("serves plain-text robots directives at the root", async () => {
    const response = await request(app.getHttpServer())
      .get("/robots.txt")
      .expect(200);

    expect(response.headers["content-type"]).toContain("text/plain");
    expect(response.text).toContain("User-agent: *");
    expect(response.text).toContain("Disallow: /v1/");
    expect(response.text).toContain("Disallow: /api");
  });

  it("does not move robots.txt under the /v1 prefix", async () => {
    await request(app.getHttpServer()).get("/v1/robots.txt").expect(404);
  });
});
