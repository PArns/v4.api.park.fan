import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { FavoritesService } from "./favorites.service";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { ParksService } from "../parks/parks.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AttractionIntegrationService } from "../attractions/services/attraction-integration.service";
import { ShowsService } from "../shows/shows.service";
import { RestaurantsService } from "../restaurants/restaurants.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { MLService } from "../ml/ml.service";
import { PopularityService } from "../popularity/popularity.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Coverage for FavoritesService — the user-supplied-IDs endpoint that
 * fans out to parks/attractions/shows/restaurants. The most important
 * guarantees are:
 *   1. UUID validation — non-UUID inputs are dropped silently so a bad
 *      `?parks=foo` parameter can't reach any repository as an
 *      attempted lookup of literal "foo".
 *   2. Redis cache hit/miss path — a hit returns immediately without
 *      touching repositories; a miss falls back to fetch+enrich and
 *      then writes the result back.
 *   3. Empty input → empty response (no wasted Redis or DB calls).
 */
describe("FavoritesService", () => {
  let service: FavoritesService;

  const parkRepo = { find: jest.fn().mockResolvedValue([]) };
  const attractionRepo = { find: jest.fn().mockResolvedValue([]) };
  const showRepo = { find: jest.fn().mockResolvedValue([]) };
  const restaurantRepo = { find: jest.fn().mockResolvedValue([]) };

  const parksService = {
    getBatchParkStatus: jest.fn().mockResolvedValue(new Map()),
    getBatchSchedules: jest
      .fn()
      .mockResolvedValue({ today: new Map(), next: new Map() }),
    getBatchHasOperatingSchedule: jest.fn().mockResolvedValue(new Map()),
  };
  const attractionsService = {};
  const attractionIntegrationService = {
    buildIntegratedResponse: jest.fn(),
  };
  const showsService = {};
  const restaurantsService = {};
  const queueDataService = {
    findCurrentStatusByAttractionIds: jest.fn().mockResolvedValue(new Map()),
  };
  const analyticsService = {
    getBatchParkOccupancy: jest.fn().mockResolvedValue(new Map()),
    getBatchParkStatistics: jest.fn().mockResolvedValue(new Map()),
    getBatchEffectiveStartTime: jest.fn().mockResolvedValue(new Map()),
    calculateParkOccupancy: jest
      .fn()
      .mockResolvedValue({ current: 0, breakdown: { currentAvgWait: 0 } }),
    getParkStatistics: jest.fn().mockResolvedValue(null),
    determineCrowdLevel: jest.fn().mockReturnValue("moderate"),
    getAttractionSparklinesBatch: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionP50s: jest.fn().mockResolvedValue(new Map()),
    getLoadRating: jest.fn().mockReturnValue({ rating: "moderate" }),
    getRopeDropForPark: jest.fn().mockResolvedValue(new Map()),
  };
  const mlService = {
    getParkPredictions: jest.fn().mockResolvedValue({ predictions: [] }),
  };
  const popularityService = {
    recordParkHit: jest.fn().mockResolvedValue(undefined),
    recordParkHits: jest.fn().mockResolvedValue(undefined),
  };

  const redisStore = new Map<string, string>();
  const redis = {
    get: jest.fn((k: string) => Promise.resolve(redisStore.get(k) ?? null)),
    set: jest.fn((k: string, v: string) => {
      redisStore.set(k, v);
      return Promise.resolve("OK");
    }),
    setex: jest.fn((k: string, _ttl: number, v: string) => {
      redisStore.set(k, v);
      return Promise.resolve("OK");
    }),
    mget: jest.fn((...keys: string[]) =>
      Promise.resolve(keys.map((k) => redisStore.get(k) ?? null)),
    ),
    ttl: jest.fn().mockResolvedValue(-1),
    del: jest.fn(),
  };

  beforeEach(async () => {
    redisStore.clear();
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        FavoritesService,
        { provide: getRepositoryToken(Park), useValue: parkRepo },
        { provide: getRepositoryToken(Attraction), useValue: attractionRepo },
        { provide: getRepositoryToken(Show), useValue: showRepo },
        { provide: getRepositoryToken(Restaurant), useValue: restaurantRepo },
        { provide: ParksService, useValue: parksService },
        { provide: AttractionsService, useValue: attractionsService },
        {
          provide: AttractionIntegrationService,
          useValue: attractionIntegrationService,
        },
        { provide: ShowsService, useValue: showsService },
        { provide: RestaurantsService, useValue: restaurantsService },
        { provide: QueueDataService, useValue: queueDataService },
        { provide: AnalyticsService, useValue: analyticsService },
        { provide: PopularityService, useValue: popularityService },
        { provide: MLService, useValue: mlService },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    service = module.get(FavoritesService);
  });

  const validParkUuid = "11111111-2222-3333-4444-555555555555";

  describe("UUID input filtering", () => {
    it("silently drops non-UUID ids before reaching any repository", async () => {
      const result = await service.getFavorites(
        [validParkUuid, "not-a-uuid", "drop-me"],
        [],
        [],
        [],
      );

      // Only the valid UUID survives — the repo call filter is built
      // from the cleaned list, never includes the garbage strings.
      expect(parkRepo.find).toHaveBeenCalledTimes(1);
      const findArg = parkRepo.find.mock.calls[0][0] as {
        where: { id: unknown };
      };
      // TypeORM `In([...])` operator wraps the array — check the value.
      expect(JSON.stringify(findArg.where.id)).toContain(validParkUuid);
      expect(JSON.stringify(findArg.where.id)).not.toContain("not-a-uuid");
      void result;
    });

    it("never hits any repository when every input id is invalid", async () => {
      const result = await service.getFavorites(
        ["not-a-uuid"],
        ["also-bad"],
        ["nope"],
        ["junk"],
      );

      // After filtering all 4 lists are empty, so the service short-
      // circuits to an empty response and never queries the DB.
      expect(parkRepo.find).not.toHaveBeenCalled();
      expect(attractionRepo.find).not.toHaveBeenCalled();
      expect(showRepo.find).not.toHaveBeenCalled();
      expect(restaurantRepo.find).not.toHaveBeenCalled();
      // Response shape is still well-formed (4 empty arrays).
      expect(result).toEqual(
        expect.objectContaining({
          parks: [],
          attractions: [],
          shows: [],
          restaurants: [],
        }),
      );
    });

    it("returns an empty response for completely empty input without touching repositories", async () => {
      const result = await service.getFavorites([], [], [], []);

      expect(result.parks).toEqual([]);
      expect(result.attractions).toEqual([]);
      expect(result.shows).toEqual([]);
      expect(result.restaurants).toEqual([]);
      // No repository touched — Redis lookup is fine (cache check is
      // cheap), but no SQL fans out for an empty input.
      expect(parkRepo.find).not.toHaveBeenCalled();
      expect(attractionRepo.find).not.toHaveBeenCalled();
      expect(showRepo.find).not.toHaveBeenCalled();
      expect(restaurantRepo.find).not.toHaveBeenCalled();
    });
  });

  describe("Rope-drop enrichment", () => {
    const validAttractionUuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    it("attaches the ropeDrop recommendation on the attraction slow path", async () => {
      // Attraction integrated cache misses (empty Redis) → slow path build.
      attractionRepo.find.mockResolvedValueOnce([
        {
          id: validAttractionUuid,
          name: "Seven Dwarfs Mine Train",
          slug: "seven-dwarfs-mine-train",
          parkId: validParkUuid,
          latitude: null,
          longitude: null,
          park: {
            id: validParkUuid,
            name: "Magic Kingdom",
            slug: "magic-kingdom",
            timezone: "America/New_York",
          },
        },
      ]);

      // Today's opening time so the UTC instants resolve.
      const opening = new Date("2026-06-11T13:00:00.000Z"); // 09:00 local EDT
      parksService.getBatchSchedules.mockResolvedValueOnce({
        today: new Map([[validParkUuid, [{ openingTime: opening }]]]),
        next: new Map(),
      });

      const stored = {
        worth: true,
        strength: "high" as const,
        confidence: "high" as const,
        busyPeak: 95,
        openWait: 20,
        savings: 75,
        rideByMinutesAfterOpen: 30,
        bestSlotMinutesAfterOpen: 600,
        bestSlotWait: 25,
        endOfDayWorth: true,
        endOfDaySavings: 70,
        byDaytype: {
          weekend: { openWait: 20, busyPeak: 95, savings: 75 },
          weekday: { openWait: 15, busyPeak: 80, savings: 65 },
        },
      };
      analyticsService.getRopeDropForPark.mockResolvedValueOnce(
        new Map([[validAttractionUuid, stored]]),
      );

      const result = await service.getFavorites(
        [],
        [validAttractionUuid],
        [],
        [],
      );

      expect(analyticsService.getRopeDropForPark).toHaveBeenCalledWith(
        validParkUuid,
      );
      const attraction = result.attractions[0];
      expect(attraction.ropeDrop).toBeDefined();
      expect(attraction.ropeDrop?.worth).toBe(true);
      expect(attraction.ropeDrop?.savings).toBe(75);
      // UTC instants resolved against today's opening (opening + offsets).
      expect(attraction.ropeDrop?.rideByUtc).toBe("2026-06-11T13:30:00.000Z");
      expect(attraction.ropeDrop?.bestSlotUtc).toBe("2026-06-11T23:00:00.000Z");
    });
  });

  describe("Redis cache contract", () => {
    it("returns the cached response on a hit and skips all DB lookups", async () => {
      const cached = {
        parks: [{ id: validParkUuid, name: "Phantasialand" }],
        attractions: [],
        shows: [],
        restaurants: [],
      };
      // Pre-populate Redis with whatever cache key the service builds.
      // We can't know it upfront, so seed via redis.get returning data
      // on any call.
      redis.get.mockImplementationOnce(async () => JSON.stringify(cached));

      const result = await service.getFavorites([validParkUuid], [], [], []);

      expect(result).toEqual(cached);
      // No repository touched — pure cache hit.
      expect(parkRepo.find).not.toHaveBeenCalled();
    });

    it("falls back to fetch+enrich on cache miss and writes the response back", async () => {
      // redis.get returns null by default (empty store). DB returns
      // a single park; enrichment skeleton is hit-or-miss but the
      // basic shape is what we check.
      parkRepo.find.mockResolvedValueOnce([
        {
          id: validParkUuid,
          name: "Phantasialand",
          slug: "phantasialand",
          country: "Germany",
          city: "Brühl",
          timezone: "Europe/Berlin",
          attractions: [],
          shows: [],
          restaurants: [],
        },
      ]);

      await service.getFavorites([validParkUuid], [], [], []);

      // Cache write happened — proves the miss path completed.
      expect(redis.set).toHaveBeenCalled();
      const [, payload] = redis.set.mock.calls[0];
      expect(typeof payload).toBe("string");
      // Payload is JSON, parses cleanly.
      expect(() => JSON.parse(payload as string)).not.toThrow();
    });
  });
});
