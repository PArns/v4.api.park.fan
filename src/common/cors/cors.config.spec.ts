import { Controller, Get, INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import request from "supertest";
import {
  buildCorsOptions,
  corsOriginMatcher,
  isAllowedCorsOrigin,
} from "./cors.config";

@Controller("ping")
class PingController {
  @Get()
  ping(): { ok: boolean } {
    return { ok: true };
  }
}

async function appWithCors(isProduction: boolean): Promise<INestApplication> {
  const moduleRef = await Test.createTestingModule({
    controllers: [PingController],
  }).compile();
  const app = moduleRef.createNestApplication({ logger: false });
  app.enableCors(buildCorsOptions(isProduction));
  await app.init();
  return app;
}

describe("cors.config", () => {
  const ORIGINAL_ENV = process.env;
  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV };
    delete process.env.CORS_ORIGINS;
  });
  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  describe("corsOriginMatcher", () => {
    const preview = corsOriginMatcher("https://park-*-arns.vercel.app");

    it("keeps an entry without * as an exact origin", () => {
      expect(corsOriginMatcher("https://park.fan")).toBe("https://park.fan");
    });

    it("matches a Vercel preview through *", () => {
      expect(
        isAllowedCorsOrigin("https://park-7kbbuacsi-arns.vercel.app", [
          preview,
        ]),
      ).toBe(true);
      expect(
        isAllowedCorsOrigin(
          "https://park-fan-git-some-branch-arns.vercel.app",
          [preview],
        ),
      ).toBe(true);
    });

    it("never lets * cross a dot or a scheme", () => {
      for (const origin of [
        "https://park-x.evil.com-arns.vercel.app",
        "https://park-x-arns.vercel.app.evil.com",
        "http://park-x-arns.vercel.app",
        "https://park--arns.vercel.app",
      ]) {
        expect(isAllowedCorsOrigin(origin, [preview])).toBe(false);
      }
    });

    it("treats regex characters in an entry literally", () => {
      expect(
        isAllowedCorsOrigin("https://parkxfan", [
          corsOriginMatcher("https://park.fan"),
        ]),
      ).toBe(false);
      expect(
        isAllowedCorsOrigin("https://parkxfan-a", [
          corsOriginMatcher("https://park.fan-*"),
        ]),
      ).toBe(false);
    });
  });

  describe("production", () => {
    let app: INestApplication;
    afterEach(async () => {
      await app?.close();
    });

    it("allows park.fan by default", async () => {
      app = await appWithCors(true);
      const response = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "https://park.fan")
        .expect(200);
      expect(response.headers["access-control-allow-origin"]).toBe(
        "https://park.fan",
      );
    });

    // The bug this file was split out for: the old callback raised an error
    // for a missing Origin, which Express turned into a 500 for every
    // server-side caller, the frontend's own renders included.
    it("answers a request without Origin normally, without CORS headers", async () => {
      process.env.CORS_ORIGINS = "https://park.fan";
      app = await appWithCors(true);
      const response = await request(app.getHttpServer())
        .get("/ping")
        .expect(200);
      expect(response.body).toEqual({ ok: true });
      expect(response.headers["access-control-allow-origin"]).toBeUndefined();
    });

    it("answers a foreign origin normally, without CORS headers", async () => {
      app = await appWithCors(true);
      const response = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "https://example.com")
        .expect(200);
      expect(response.headers["access-control-allow-origin"]).toBeUndefined();
    });

    it("lets CORS_ORIGINS replace the default list", async () => {
      process.env.CORS_ORIGINS =
        "https://park.fan, https://park-*-arns.vercel.app";
      app = await appWithCors(true);
      const preview = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "https://park-7kbbuacsi-arns.vercel.app")
        .expect(200);
      expect(preview.headers["access-control-allow-origin"]).toBe(
        "https://park-7kbbuacsi-arns.vercel.app",
      );
      const www = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "https://www.park.fan")
        .expect(200);
      expect(www.headers["access-control-allow-origin"]).toBeUndefined();
    });

    it("exposes Retry-After to an allowed origin", async () => {
      app = await appWithCors(true);
      const response = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "https://park.fan")
        .expect(200);
      expect(response.headers["access-control-expose-headers"]).toBe(
        "Retry-After",
      );
    });

    it("answers a JSON write's preflight for park.fan, cacheable", async () => {
      app = await appWithCors(true);
      const response = await request(app.getHttpServer())
        .options("/ping")
        .set("Origin", "https://park.fan")
        .set("Access-Control-Request-Method", "PUT")
        .set("Access-Control-Request-Headers", "content-type")
        .expect(204);
      expect(response.headers["access-control-allow-origin"]).toBe(
        "https://park.fan",
      );
      expect(response.headers["access-control-allow-methods"]).toContain("PUT");
      expect(response.headers["access-control-max-age"]).toBe("7200");
    });

    it("gives a foreign origin's preflight no CORS headers", async () => {
      app = await appWithCors(true);
      const response = await request(app.getHttpServer())
        .options("/ping")
        .set("Origin", "https://example.com")
        .set("Access-Control-Request-Method", "PUT");
      expect(response.headers["access-control-allow-origin"]).toBeUndefined();
    });
  });

  describe("development", () => {
    let app: INestApplication;
    afterEach(async () => {
      await app?.close();
    });

    it("allows localhost and not park.fan by default", async () => {
      app = await appWithCors(false);
      const local = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "http://localhost:3000")
        .expect(200);
      expect(local.headers["access-control-allow-origin"]).toBe(
        "http://localhost:3000",
      );
      const prod = await request(app.getHttpServer())
        .get("/ping")
        .set("Origin", "https://park.fan")
        .expect(200);
      expect(prod.headers["access-control-allow-origin"]).toBeUndefined();
    });
  });
});
