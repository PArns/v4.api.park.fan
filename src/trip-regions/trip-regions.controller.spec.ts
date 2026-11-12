import { Test } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import request from "supertest";
import { TripRegionsController } from "./trip-regions.controller";
import { TripRegionService } from "./trip-region.service";
import { TripPlanService } from "./trip-plan.service";
import { TripWhenService } from "./trip-when.service";
import { HolidaysService } from "../holidays/holidays.service";
import { WeatherService } from "../parks/weather.service";
import { ParksService } from "../parks/parks.service";
import { CalendarService } from "../parks/services/calendar.service";
import { ParkSeasonService } from "../parks/services/park-season.service";
import { ApiKeyService } from "../api-keys/api-key.service";
import { PredictionLeadSnapshotService } from "../ml/services/prediction-lead-snapshot.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import { ORLANDO } from "./regions";

const KEY = `pfk_${"t".repeat(43)}`;
const TZ = "America/New_York";

/**
 * The three routes through the real HTTP pipeline (pipes, the key guard, the
 * Cache-Control they leave with) and the real services, over stand-ins for the
 * database, the calendar and Redis. Dates are relative to the region's today,
 * because the routes refuse a window in the past.
 */
describe("TripRegionsController", () => {
  let app: INestApplication;
  let buildCalendarResponse: jest.Mock;
  const today = getCurrentDateInTimezone(TZ);
  const day = (n: number) => addIsoDays(today, n);

  const store = new Map<string, string>();
  const redis = {
    get: jest.fn(async (k: string) => store.get(k) ?? null),
    set: jest.fn(async (k: string, v: string) => {
      store.set(k, v);
      return "OK";
    }),
    mget: jest.fn(async (...keys: string[]) =>
      keys.map((k) => store.get(k) ?? null),
    ),
  };

  const destinations: Record<string, { id: string; name: string }> = {
    "walt-disney-world": { id: "d-wdw", name: "Walt Disney World® Resort" },
    "universal-orlando": { id: "d-uor", name: "Universal Orlando Resort" },
    "seaworld-orlando": {
      id: "d-sw",
      name: "SeaWorld Parks and Resorts Orlando",
    },
  };

  beforeAll(async () => {
    const findByGeographicPath = jest.fn(
      async (
        continent: string,
        country: string,
        city: string,
        slug: string,
      ) => {
        const config = ORLANDO.parks.find((p) => p.slug === slug);
        if (!config) return null;
        const destination = destinations[config.group] ?? {
          id: `d-${slug}`,
          name: slug,
        };
        return {
          id: `00000000-0000-4000-8000-${String(ORLANDO.parks.indexOf(config)).padStart(12, "0")}`,
          slug,
          name: slug,
          continentSlug: continent,
          countrySlug: country,
          citySlug: city,
          latitude: "28.4",
          longitude: "-81.5",
          timezone: TZ,
          destinationId: destination.id,
          destination,
          retiredAt: null,
        };
      },
    );

    // Every park open 09–21 with a forecast that dips mid-window; Magic
    // Kingdom closes at 18:00 on day 3 for a party.
    buildCalendarResponse = jest.fn(
      async (park: { slug: string }, fromDate: Date, toDate: Date) => {
        const days = [];
        for (
          let d = fromDate.toISOString().slice(0, 10);
          d <= toDate.toISOString().slice(0, 10);
          d = addIsoDays(d, 1)
        ) {
          const offset = Math.round(
            (Date.parse(`${d}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) /
              86_400_000,
          );
          days.push({
            date: d,
            status: "OPERATING",
            crowdLevel: "moderate",
            isHoliday: false,
            isSchoolVacation: false,
            isBridgeDay: false,
            isToday: d === today,
            hours: {
              openingTime: `${d}T14:00:00.000Z`,
              closingTime: `${addIsoDays(d, 1)}T02:00:00.000Z`,
              type: "OPERATING",
              isInferred: false,
            },
            headlinerForecast: {
              avgWait: 30 + ((offset * 7 + park.slug.length) % 40),
              rides: [],
            },
          });
        }
        return {
          meta: {
            slug: park.slug,
            timezone: TZ,
            hasOperatingSchedule: true,
            scheduleCoverage: { from: today, to: addIsoDays(today, 70) },
          },
          days,
        };
      },
    );

    const getSchedule = jest.fn(async (parkId: string) => {
      if (!parkId.endsWith("000000000000")) return [];
      const d = day(3);
      return [
        {
          date: d,
          scheduleType: "OPERATING",
          openingTime: new Date(`${d}T09:00:00-05:00`),
          closingTime: new Date(`${d}T18:00:00-05:00`),
          description: null,
        },
        {
          date: d,
          scheduleType: "TICKETED_EVENT",
          openingTime: new Date(`${d}T19:00:00-05:00`),
          closingTime: new Date(`${addIsoDays(d, 1)}T00:00:00-05:00`),
          description: null,
        },
      ];
    });

    const moduleRef = await Test.createTestingModule({
      controllers: [TripRegionsController],
      providers: [
        TripRegionService,
        TripPlanService,
        TripWhenService,
        {
          provide: HolidaysService,
          useValue: {
            // NRW has a school break from day 10 to day 20 (a Saturday-to-Sunday stretch or not).
            getHolidays: jest.fn(async (country: string) =>
              country === "DE"
                ? Array.from({ length: 11 }, (_, k) => ({
                    date: day(10 + k),
                    name: "Autumn Holidays",
                    country: "DE",
                    region: "DE-NW",
                    holidayType: "school",
                    isNationwide: false,
                  }))
                : [],
            ),
            getSchoolHolidayCoverage: jest.fn(async () => ({
              regional: new Map([["DE|NW", day(300)]]),
              national: new Map(),
              countryAny: new Map([["DE", day(300)]]),
            })),
          },
        },
        {
          provide: WeatherService,
          useValue: { getClimateNormals: jest.fn(async () => null) },
        },
        {
          provide: ParksService,
          useValue: { findByGeographicPath, getSchedule },
        },
        { provide: CalendarService, useValue: { buildCalendarResponse } },
        {
          provide: ParkSeasonService,
          useValue: { forParksInRange: jest.fn(async () => []) },
        },
        { provide: REDIS_CLIENT, useValue: redis },
        {
          provide: PredictionLeadSnapshotService,
          useValue: { getLeadTimeMae: jest.fn(async () => 14) },
        },
        {
          provide: ApiKeyService,
          useValue: {
            lookup: jest.fn(async (value: string) =>
              value === KEY
                ? {
                    status: "active",
                    key: {
                      id: "k1",
                      usage: "non-commercial",
                      requestsPerHour: null,
                      allowedParkIds: null,
                      expiresAt: null,
                    },
                  }
                : { status: "unknown" },
            ),
            countRequest: jest.fn(async () => null),
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
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(() => store.clear());

  describe("GET /v1/trip-regions/:region", () => {
    it("answers without a key, with the region's parks, groups and legs", async () => {
      const res = await request(app.getHttpServer())
        .get("/v1/trip-regions/orlando")
        .expect(200);
      expect(res.body.parks).toHaveLength(16);
      expect(res.body.groups).toEqual([
        {
          key: "walt-disney-world",
          name: "Walt Disney World Resort",
          kind: "resort",
        },
        {
          key: "universal-orlando",
          name: "Universal Orlando Resort",
          kind: "resort",
        },
        {
          key: "seaworld-orlando",
          name: "SeaWorld Parks and Resorts Orlando",
          kind: "resort",
        },
        { key: "day-trips", name: "Day trips", kind: "day_trip" },
      ]);
      const mk = res.body.parks.find(
        (p: { slug: string }) => p.slug === "magic-kingdom-park",
      );
      expect(mk).toMatchObject({
        path: "/parks/north-america/united-states/orlando/magic-kingdom-park",
        group: "walt-disney-world",
        type: "theme_park",
        headliner: true,
        latitude: 28.4,
      });
      const walk = res.body.travel.legs.find(
        (l: { from: string; to: string }) =>
          l.from === "universal-studios-florida" &&
          l.to === "universal-islands-of-adventure",
      );
      expect(walk).toMatchObject({ mode: "walk", minutes: 10 });
      const toMk = res.body.travel.legs.find(
        (l: { from: string; to: string }) =>
          l.from === "magic-kingdom-park" && l.to === "epcot",
      );
      expect(toMk).toMatchObject({ mode: "drive", via: "ttc" });
      expect(res.body.travel.attribution).toBe("© OpenStreetMap contributors");
      expect(res.headers["cache-control"]).toContain("max-age=3600");
      expect(res.headers["cdn-cache-control"]).toContain("max-age=86400");
    });

    it("answers an unknown region with 404", async () => {
      await request(app.getHttpServer())
        .get("/v1/trip-regions/atlantis")
        .expect(404);
    });
  });

  describe("GET /v1/trip-regions/:region/calendar", () => {
    it("needs a data key", async () => {
      const res = await request(app.getHttpServer())
        .get("/v1/trip-regions/orlando/calendar")
        .expect(401);
      expect(res.body.error ?? res.body.message?.error).toBeDefined();
      expect(JSON.stringify(res.body)).toContain("api-key-required");
      expect(res.headers["cache-control"]).toBe("private, no-store");
    });

    it("serves every park's days with a key, kept out of shared caches", async () => {
      const res = await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/calendar?from=${day(1)}&to=${day(5)}`)
        .set("X-API-Key", KEY)
        .expect(200);
      expect(res.headers["cache-control"]).toMatch(/^private, max-age=3600/);
      expect(res.body.meta).toMatchObject({
        region: "orlando",
        from: day(1),
        to: day(5),
      });
      expect(res.body.days).toHaveLength(5);
      // Read with fourteen days either side, so each day is priced against
      // the park's usual day, not against the window asked for.
      const calls = buildCalendarResponse.mock.calls;
      const [, readFrom, readTo] = calls[calls.length - 1];
      expect((readFrom as Date).toISOString().slice(0, 10)).toBe(day(1 - 14));
      expect((readTo as Date).getTime()).toBeGreaterThan(
        Date.parse(`${day(5 + 14)}T00:00:00Z`),
      );
      const party = res.body.days[2].parks["magic-kingdom-park"];
      expect(party).toMatchObject({
        status: "OPERATING",
        hoursPublished: true,
        closesAt: `${day(3)}T18:00:00-05:00`,
        evening: { kind: "ticketed_event" },
        basis: "forecast",
      });
      expect(party.dayHours).toBeUndefined();
      expect(party.headlinerWait % 5).toBe(0);
    });

    it.each([
      [
        "a window over 92 days",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 100)}`,
      ],
      [
        "a day past a year",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 360)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 370)}`,
      ],
      [
        "a day in the past",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), -3)}`,
      ],
      ["a day the calendar has not", "from=2027-02-30"],
      ["a malformed date", "from=tomorrow"],
    ])("refuses %s with 400", async (_what, qs) => {
      await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/calendar?${qs}`)
        .set("X-API-Key", KEY)
        .expect(400);
    });
  });

  describe("GET /v1/trip-regions/:region/best-days", () => {
    it("needs a data key", async () => {
      const res = await request(app.getHttpServer())
        .get("/v1/trip-regions/orlando/best-days")
        .expect(401);
      expect(JSON.stringify(res.body)).toContain("api-key-required");
    });

    it("names each park's quietest and busiest days and each resort's best dates", async () => {
      const res = await request(app.getHttpServer())
        .get(
          `/v1/trip-regions/orlando/best-days?from=${day(1)}&to=${day(10)}&limit=3`,
        )
        .set("X-API-Key", KEY)
        .expect(200);
      expect(res.headers["cache-control"]).toMatch(/^private/);
      expect(res.body.meta).toMatchObject({ from: day(1), to: day(10) });
      const mk = res.body.parks.find(
        (p: { slug: string }) => p.slug === "magic-kingdom-park",
      );
      expect(mk.quiet).toHaveLength(3);
      expect(mk.busy).toHaveLength(2);
      expect(res.body.groups.map((g: { key: string }) => g.key)).toEqual([
        "walt-disney-world",
        "universal-orlando",
        "seaworld-orlando",
      ]);
    });

    it("refuses a limit out of range with 400", async () => {
      await request(app.getHttpServer())
        .get("/v1/trip-regions/orlando/best-days?limit=0")
        .set("X-API-Key", KEY)
        .expect(400);
    });
  });

  describe("GET /v1/trip-regions/:region/plan", () => {
    it("needs a data key", async () => {
      const res = await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/plan?from=${day(1)}&to=${day(5)}`)
        .expect(401);
      expect(JSON.stringify(res.body)).toContain("api-key-required");
    });

    it("plans a trip and caches it by the normalized query", async () => {
      const qs = `from=${day(1)}&to=${day(8)}&parks=magic-kingdom-park:2,epcot,universal-epic-universe&hotel=disney&area=disney`;
      const res = await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/plan?${qs}`)
        .set("X-API-Key", KEY)
        .expect(200);
      expect(res.body.meta).toMatchObject({
        region: "orlando",
        objective: "headliner_wait",
      });
      expect(res.body.days).toHaveLength(8);
      expect(res.body.days[0].kind).toBe("arrival");
      expect(res.body.days[7].kind).toBe("departure");
      // EPCOT, listed without a count, gets the day the trip has to spare
      // after its one rest day; MK's explicit two stay two.
      expect(res.body.meta.split).toEqual([
        { slug: "epcot", visits: 2, auto: true },
      ]);
      expect(res.body.totals.visits).toBe(5);
      expect(res.body.totals.expectedError).toBe(14);
      expect(typeof res.body.totals.orderMatters).toBe("boolean");
      const mkDates = res.body.parks.find(
        (p: { slug: string }) => p.slug === "magic-kingdom-park",
      ).dates;
      expect(mkDates).toHaveLength(2);
      const parkDay = res.body.days.find(
        (d: { kind: string }) => d.kind === "park",
      );
      expect(parkDay.travel).toMatchObject({
        from: "hotel:disney",
        mode: "drive",
      });

      const calls = buildCalendarResponse.mock.calls.length;
      // Same plan, parameters in another order: served from Redis.
      await request(app.getHttpServer())
        .get(
          `/v1/trip-regions/orlando/plan?area=disney&hotel=disney&parks=epcot,universal-epic-universe,magic-kingdom-park:2&to=${day(8)}&from=${day(1)}`,
        )
        .set("X-API-Key", KEY)
        .expect(200);
      expect(buildCalendarResponse.mock.calls.length).toBe(calls);
    });

    it.each([
      [
        "a trip over 21 days",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 22)}`,
      ],
      [
        "a park outside the region",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 5)}&parks=europa-park`,
      ],
      [
        "four visits of one park",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 9)}&parks=epcot:4`,
      ],
      [
        "more visits than days without hop",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 2)}&parks=epcot,magic-kingdom-park,universal-epic-universe`,
      ],
      [
        "a lock outside the trip",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 5)}&parks=epcot&lock=epcot@${addIsoDays(getCurrentDateInTimezone(TZ), 9)}`,
      ],
      [
        "a lock on a park not in the plan",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 5)}&parks=epcot&lock=magic-kingdom-park@${addIsoDays(getCurrentDateInTimezone(TZ), 2)}`,
      ],
      [
        "an unknown hotel area",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 5)}&area=atlantis`,
      ],
      [
        "a malformed rest option",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 5)}&rest=sometimes`,
      ],
      [
        "an unknown parameter",
        `from=${addIsoDays(getCurrentDateInTimezone(TZ), 1)}&to=${addIsoDays(getCurrentDateInTimezone(TZ), 5)}&budget=low`,
      ],
      ["a missing trip", ""],
    ])("refuses %s with 400", async (_what, qs) => {
      await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/plan?${qs}`)
        .set("X-API-Key", KEY)
        .expect(400);
    });
  });
  describe("GET /v1/trip-regions/:region/when", () => {
    const parks = "parks=epcot,magic-kingdom-park";

    it("needs a data key", async () => {
      const res = await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/when?${parks}`)
        .expect(401);
      expect(JSON.stringify(res.body)).toContain("api-key-required");
    });

    it("ranks the windows of a range and caches the answer by its query", async () => {
      const qs = `from=${day(1)}&to=${day(40)}&days=5&${parks}&limit=3&gap=5`;
      const res = await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/when?${qs}`)
        .set("X-API-Key", KEY)
        .expect(200);
      expect(res.headers["cache-control"]).toMatch(/^private, max-age=3600/);
      expect(res.body.meta).toMatchObject({
        region: "orlando",
        from: day(1),
        to: day(40),
        days: 5,
        search: { exact: true, windows: 36 },
      });
      expect(res.body.windows).toHaveLength(3);
      expect(res.body.windows[0]).toMatchObject({ rank: 1 });
      expect(res.body.windows[0].plan.days).toHaveLength(5);
      expect(res.body.weeks.length).toBeGreaterThan(4);
      expect(res.body.months[0]).toHaveProperty("hurricaneSeason");

      const calls = buildCalendarResponse.mock.calls.length;
      await request(app.getHttpServer())
        .get(
          `/v1/trip-regions/orlando/when?gap=5&limit=3&parks=magic-kingdom-park,epcot&days=5&to=${day(40)}&from=${day(1)}`,
        )
        .set("X-API-Key", KEY)
        .expect(200);
      expect(buildCalendarResponse.mock.calls.length).toBe(calls);
    });

    it("keeps the windows inside a school break with within", async () => {
      const res = await request(app.getHttpServer())
        .get(
          `/v1/trip-regions/orlando/when?from=${day(1)}&to=${day(40)}&days=4&${parks}&within=DE-NW`,
        )
        .set("X-API-Key", KEY)
        .expect(200);
      expect(res.body.meta.within).toMatchObject({
        code: "DE-NW",
        name: "Nordrhein-Westfalen",
      });
      const [period] = res.body.meta.within.periods;
      expect(res.body.windows.length).toBeGreaterThan(0);
      for (const w of res.body.windows) {
        expect(w.from >= period.from && w.to <= period.to).toBe(true);
      }
    });

    it.each([
      ["a range past a year", `from=${day(1)}&to=${day(370)}`],
      ["a range in the past", `from=${day(-3)}`],
      ["a trip of two days", "days=2"],
      ["a trip of 22 days", "days=22"],
      ["a range shorter than the trip", `from=${day(1)}&to=${day(3)}&days=7`],
      ["an unknown within code", "within=DE-XX"],
      ["a malformed within code", "within=nrw"],
      ["more than ten windows", "limit=11"],
      ["an unknown parameter", "budget=low"],
      ["a lock, which a window cannot take", `lock=epcot@${day(2)}`],
    ])("refuses %s with 400", async (_what, qs) => {
      await request(app.getHttpServer())
        .get(`/v1/trip-regions/orlando/when?${parks}&${qs}`)
        .set("X-API-Key", KEY)
        .expect(400);
    });
  });

  describe("GET /v1/trip-regions/:region/school-breaks", () => {
    it("answers without a key, by stored region code, a day at the edge", async () => {
      const res = await request(app.getHttpServer())
        .get(
          `/v1/trip-regions/orlando/school-breaks?from=${day(1)}&to=${day(60)}`,
        )
        .expect(200);
      expect(res.headers["cdn-cache-control"]).toContain("max-age=86400");
      const nrw = res.body.regions.find(
        (r: { code: string }) => r.code === "DE-NW",
      );
      expect(nrw).toMatchObject({
        country: "DE",
        name: "Nordrhein-Westfalen",
        through: day(300),
      });
      expect(nrw.periods).toHaveLength(1);
      expect(nrw.periods[0].from <= day(10)).toBe(true);
      expect(nrw.periods[0].to >= day(20)).toBe(true);
      // Countries whose stored data does not reach far enough are left out.
      expect(
        res.body.regions.every((r: { country: string }) => r.country === "DE"),
      ).toBe(true);
    });

    it("refuses a malformed country list with 400", async () => {
      await request(app.getHttpServer())
        .get("/v1/trip-regions/orlando/school-breaks?countries=Germany")
        .expect(400);
    });
  });
});
