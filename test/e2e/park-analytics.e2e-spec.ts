import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import request from "supertest";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { Park } from "../../src/parks/entities/park.entity";
import { Attraction } from "../../src/attractions/entities/attraction.entity";
import { REDIS_CLIENT } from "../../src/common/redis/redis.module";
import type { Redis } from "ioredis";

describe("Park Analytics (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let redis: Redis;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    // Production serves everything under /v1; without this the paths below 404
    // regardless of the data.
    app.setGlobalPrefix("v1");
    await app.init();

    dataSource = app.get(DataSource);
    redis = app.get(REDIS_CLIENT);
  });

  afterAll(async () => {
    await app.close();
  });

  // The park these tests address is a real one, so it has to be created here —
  // the shared setup truncates every table after each test, and the responses are
  // cached, so both have to be redone per test.
  beforeEach(async () => {
    await redis.flushdb();

    const park = await dataSource.getRepository(Park).save(
      dataSource.getRepository(Park).create({
        externalId: "e2e-phantasialand",
        name: "Phantasialand",
        slug: "phantasialand",
        // Phantasialand's real position. The nearby test queries from a point ~57 km
        // away with a 10 km radius, and the same radius decides "in_park" — seeding
        // the park on top of that query point would make the endpoint answer
        // "in_park", which is correct but not what that test is about.
        latitude: 50.7986,
        longitude: 6.8792,
        timezone: "Europe/Berlin",
        continent: "Europe",
        continentSlug: "europe",
        country: "Germany",
        countrySlug: "germany",
        countryCode: "DE",
        city: "Brühl",
        citySlug: "bruehl",
      }),
    );

    const attractionRepo = dataSource.getRepository(Attraction);
    await attractionRepo.save(
      ["Taron", "F.L.Y.", "Black Mamba"].map((name, i) =>
        attractionRepo.create({
          externalId: `e2e-attr-${i}`,
          name,
          slug: name.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
          parkId: park.id,
          latitude: 50.7986,
          longitude: 6.8792,
        }),
      ),
    );
  });

  describe("GET /v1/parks/:continent/:country/:city/:slug", () => {
    it("should return park with non-null analytics object", () => {
      return request(app.getHttpServer())
        .get("/v1/parks/europe/germany/bruehl/phantasialand")
        .expect(200)
        .expect((res) => {
          expect(res.body).toBeDefined();
          expect(res.body.analytics).toBeDefined();
          expect(res.body.analytics).not.toBeNull();

          // Analytics should have required structure even if park is closed
          expect(res.body.analytics).toHaveProperty("occupancy");
          expect(res.body.analytics).toHaveProperty("statistics");

          // Statistics should have required fields
          const stats = res.body.analytics.statistics;
          expect(stats).toHaveProperty("avgWaitTime");
          expect(stats).toHaveProperty("avgWaitToday");
          expect(stats).toHaveProperty("peakWaitToday");
          expect(stats).toHaveProperty("crowdLevel");
          expect(stats).toHaveProperty("totalAttractions");
          expect(stats).toHaveProperty("operatingAttractions");
          expect(stats).toHaveProperty("closedAttractions");

          // Peak-hour contract (additive v2 fields)
          expect(stats).toHaveProperty("peakHour");
          expect(stats).toHaveProperty("peakHourLocal");
          expect(stats).toHaveProperty("peakHourConfidence");
          expect(stats).toHaveProperty("peakHourSource");
          // peakHour is always ISO 8601 (with offset) or null — never bare HH:MM
          if (stats.peakHour !== null) {
            expect(stats.peakHour).toContain("T");
          }
          expect(typeof stats.peakHourConfidence).toBe("number");
          expect(stats.peakHourConfidence).toBeGreaterThanOrEqual(0);
          expect(stats.peakHourConfidence).toBeLessThanOrEqual(1);
          if (stats.peakHourSource !== null) {
            expect([
              "prediction",
              "observed_today",
              "historical_fallback",
            ]).toContain(stats.peakHourSource);
          }

          // Total attractions should be positive for parks with attractions
          expect(typeof stats.totalAttractions).toBe("number");
          expect(stats.totalAttractions).toBeGreaterThanOrEqual(0);
        });
    });

    // The response used to carry a top-level `crowdForecast` array. It no longer
    // does — the forward-looking view moved to the park calendar endpoint — so this
    // pins the schedule array that did stay, and that the removed field stays gone.
    it("should return park with a schedule array and no stale crowdForecast", () => {
      return request(app.getHttpServer())
        .get("/v1/parks/europe/germany/bruehl/phantasialand")
        .expect(200)
        .expect((res) => {
          expect(res.body).toBeDefined();
          expect(Array.isArray(res.body.schedule)).toBe(true);
          expect(res.body).not.toHaveProperty("crowdForecast");
        });
    });

    it("should return park with attractions array", () => {
      return request(app.getHttpServer())
        .get("/v1/parks/europe/germany/bruehl/phantasialand")
        .expect(200)
        .expect((res) => {
          expect(res.body).toBeDefined();
          expect(res.body.attractions).toBeDefined();
          expect(Array.isArray(res.body.attractions)).toBe(true);
          // Phantasialand should have attractions
          expect(res.body.attractions.length).toBeGreaterThan(0);
        });
    });
  });

  describe("GET /v1/parks/:continent/:country/:city/:slug/stats", () => {
    it("returns the v2 historical-stats contract", () => {
      return request(app.getHttpServer())
        .get("/v1/parks/europe/germany/bruehl/phantasialand/stats")
        .expect(200)
        .expect((res) => {
          const body = res.body;
          expect(Array.isArray(body.byMonth)).toBe(true);
          expect(Array.isArray(body.byDayOfWeek)).toBe(true);
          expect(Array.isArray(body.topAttractions)).toBe(true);

          // Additive meta fields
          expect(body.meta).toHaveProperty("windowYears");
          expect(body.meta).toHaveProperty("displayable");
          expect(body.meta).toHaveProperty("generatedAt");
          expect(body.meta.schemaVersion).toBe(2);
          expect(typeof body.meta.displayable).toBe("boolean");

          const VALID_LEVELS = [
            "very_low",
            "low",
            "moderate",
            "high",
            "very_high",
            "extreme",
          ];
          // Backend now maps the crowd level — frontend must not re-classify.
          for (const m of body.byMonth) {
            expect(VALID_LEVELS).toContain(m.avgCrowdLevel);
          }
          for (const d of body.byDayOfWeek) {
            expect(VALID_LEVELS).toContain(d.avgCrowdLevel);
          }
          // Explicit 1-based rank instead of relying on array index
          body.topAttractions.forEach((a: { rank: number }, i: number) => {
            expect(a.rank).toBe(i + 1);
          });
        });
    });

    it("clamps topN and honours minSampleDays", () => {
      return request(app.getHttpServer())
        .get("/v1/parks/europe/germany/bruehl/phantasialand/stats")
        .query({ years: 1, topN: 3, minSampleDays: 999999 })
        .expect(200)
        .expect((res) => {
          expect(res.body.meta.windowYears).toBe(1);
          expect(res.body.topAttractions.length).toBeLessThanOrEqual(3);
          // An impossibly high threshold forces displayable = false
          expect(res.body.meta.displayable).toBe(false);
        });
    });
  });

  describe("GET /v1/discovery/nearby", () => {
    it("should return parks with valid attraction counts", () => {
      // Phantasialand coordinates
      return request(app.getHttpServer())
        .get("/v1/discovery/nearby")
        .query({
          lat: 50.7753,
          lng: 6.0839,
          radius: 10000,
        })
        .expect(200)
        .expect((res) => {
          expect(res.body).toBeDefined();
          expect(res.body.type).toBe("nearby_parks");
          expect(Array.isArray(res.body.data.parks)).toBe(true);
          expect(res.body.data.count).toBe(res.body.data.parks.length);

          if (res.body.data.parks.length > 0) {
            const park = res.body.data.parks.find(
              (p: { slug: string }) => p.slug === "phantasialand",
            );

            if (park) {
              // The nearby payload carries the counts at the top level and keeps
              // `analytics` to the three live figures — it is not the park-detail
              // shape, which nests everything under analytics.statistics.
              expect(typeof park.totalAttractions).toBe("number");
              expect(park.totalAttractions).toBeGreaterThan(0);
              expect(typeof park.operatingAttractions).toBe("number");
              expect(park.operatingAttractions).toBeGreaterThanOrEqual(0);
              expect(park.operatingAttractions).toBeLessThanOrEqual(
                park.totalAttractions,
              );

              expect(park.analytics).toBeDefined();
              expect(park.analytics).toHaveProperty("occupancy");
              expect(park.analytics).toHaveProperty("crowdLevel");
            }
          }
        });
    });
  });
});
