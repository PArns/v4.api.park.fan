import { Controller, Get, INestApplication } from "@nestjs/common";
import { APP_GUARD } from "@nestjs/core";
import { Test } from "@nestjs/testing";
import { ThrottlerModule } from "@nestjs/throttler";
import request from "supertest";
import { ApiKeyService } from "../../api-keys/api-key.service";
import { HttpExceptionFilter } from "../filters/http-exception.filter";
import {
  getThrottlerOptions,
  isAnonymousDataRequest,
} from "../throttler/throttler.config";
import { CfThrottlerGuard } from "./cf-throttler.guard";

const VALID = `pfk_${"v".repeat(43)}`;
const MADE_UP = `pfk_${"m".repeat(43)}`;

@Controller("parks")
class ParksStub {
  @Get("x")
  read() {
    return { ok: true };
  }
}

@Controller("trips")
class TripsStub {
  @Get("x")
  read() {
    return { ok: true };
  }
}

describe("isAnonymousDataRequest", () => {
  it("covers the public data reads and nothing park.fan's browser calls", () => {
    expect(
      isAnonymousDataRequest(
        "GET",
        "/v1/parks/europe/germany/rust/europa-park",
      ),
    ).toBe(true);
    expect(isAnonymousDataRequest("GET", "/v1/discovery/geo")).toBe(true);
    expect(isAnonymousDataRequest("GET", "/v1/search?q=efteling")).toBe(true);
    expect(
      isAnonymousDataRequest("GET", "/v1/discovery/nearby?lat=1&lng=2"),
    ).toBe(false);
    expect(isAnonymousDataRequest("GET", "/v1/favorites?parkIds=a")).toBe(
      false,
    );
    expect(isAnonymousDataRequest("POST", "/v1/assistant/chat")).toBe(false);
    expect(isAnonymousDataRequest("POST", "/v1/analytics/collect")).toBe(false);
    expect(isAnonymousDataRequest("GET", "/v1/trips/abc")).toBe(false);
    expect(isAnonymousDataRequest("GET", "/v1/parkside")).toBe(false);
  });
});

describe("CfThrottlerGuard anonymous limits", () => {
  let app: INestApplication;
  const saved = { ...process.env };

  beforeAll(async () => {
    process.env.THROTTLE_LIMIT = "300";
    process.env.THROTTLE_ANON_LIMIT = "2";
    process.env.THROTTLE_ANON_HOURLY_LIMIT = "100";
    const moduleRef = await Test.createTestingModule({
      imports: [ThrottlerModule.forRoot(getThrottlerOptions())],
      controllers: [ParksStub, TripsStub],
      providers: [
        { provide: APP_GUARD, useClass: CfThrottlerGuard },
        {
          provide: ApiKeyService,
          useValue: {
            resolve: async (key: string) =>
              key === VALID
                ? {
                    id: "k",
                    usage: "non-commercial",
                    requestsPerHour: null,
                    allowedParkIds: null,
                  }
                : null,
          },
        },
      ],
    }).compile();
    app = moduleRef.createNestApplication();
    app.setGlobalPrefix("v1");
    app.useGlobalFilters(new HttpExceptionFilter());
    await app.init();
  });

  afterAll(async () => {
    await app.close();
    process.env = saved;
  });

  it("limits a data read without a key and says a key lifts it", async () => {
    const server = app.getHttpServer();
    await request(server)
      .get("/v1/parks/x")
      .set("X-Forwarded-For", "203.0.113.1")
      .expect(200);
    await request(server)
      .get("/v1/parks/x")
      .set("X-Forwarded-For", "203.0.113.1")
      .expect(200);
    const refused = await request(server)
      .get("/v1/parks/x")
      .set("X-Forwarded-For", "203.0.113.1")
      .expect(429);
    expect(refused.body).toMatchObject({ error: "rate-limited" });
    expect(refused.headers["retry-after"]).toBeDefined();
  });

  it("does not limit a route park.fan's browser calls", async () => {
    const server = app.getHttpServer();
    for (let i = 0; i < 5; i++) {
      await request(server)
        .get("/v1/trips/x")
        .set("X-Forwarded-For", "203.0.113.2")
        .expect(200);
    }
  });

  it("lifts the limit for an active key, not for a made-up one", async () => {
    const server = app.getHttpServer();
    for (let i = 0; i < 5; i++) {
      await request(server)
        .get("/v1/parks/x")
        .set("X-Forwarded-For", "203.0.113.3")
        .set("X-API-Key", VALID)
        .expect(200);
    }
    const statuses: number[] = [];
    for (let i = 0; i < 3; i++) {
      const r = await request(server)
        .get("/v1/parks/x")
        .set("X-Forwarded-For", "203.0.113.4")
        .set("X-API-Key", MADE_UP);
      statuses.push(r.status);
    }
    expect(statuses).toEqual([200, 200, 429]);
  });
});
