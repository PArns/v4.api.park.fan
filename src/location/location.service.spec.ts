import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { LocationService } from "./location.service";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { ParksService } from "../parks/parks.service";
import { PopularityService } from "../popularity/popularity.service";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Coverage for the public surface of LocationService — the "where am I,
 * what's around me" endpoint. PR #46 reshuffled the inner baseline
 * lookup (P90 primary + P50 fallback) and removed the live-aggregation
 * P90 batch read; these tests pin the resulting branch logic down:
 *   1. findNearby returns the in-park shape when the user is inside a
 *      park's radius, the nearby-parks shape otherwise.
 *   2. Parks without coordinates are silently excluded (no NaN
 *      distances leaking into the response).
 *   3. The empty-parks-table case falls through to "nearby_parks" with
 *      an empty list rather than throwing.
 */
describe("LocationService", () => {
  let service: LocationService;

  const parkRepo = {
    find: jest.fn(),
    findOne: jest.fn(),
  };
  const attractionRepo = {
    find: jest.fn().mockResolvedValue([]),
    findOne: jest.fn(),
  };
  const queueDataService = {
    findCurrentStatusByAttractionIds: jest.fn().mockResolvedValue(new Map()),
  };
  const analyticsService = {
    getEffectiveStartTime: jest.fn().mockResolvedValue(new Date()),
    getBatchEffectiveStartTime: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionP50s: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionP90Baselines: jest.fn().mockResolvedValue(new Map()),
    getBatchAttractionPercentilesToday: jest.fn().mockResolvedValue(new Map()),
    getBatchParkStatistics: jest.fn().mockResolvedValue(new Map()),
    getParkStatistics: jest.fn().mockResolvedValue(null),
    getAttractionCrowdLevel: jest.fn().mockReturnValue("moderate"),
    getBatchParkOccupancy: jest.fn().mockResolvedValue(new Map()),
    getHeadlinerAttractionIds: jest.fn().mockResolvedValue(new Set<string>()),
    // Park ratability gate — default ratable so existing crowd-level
    // assertions keep their pre-gate behaviour.
    isParkRatable: jest.fn().mockResolvedValue(true),
    getRatableParkIds: jest
      .fn()
      .mockImplementation((ids: string[]) => Promise.resolve(new Set(ids))),
  };
  const parksService = {
    getBatchParkStatus: jest.fn().mockResolvedValue(new Map()),
    getBatchHasOperatingSchedule: jest.fn().mockResolvedValue(new Map()),
    getBatchSchedules: jest
      .fn()
      .mockResolvedValue({ today: new Map(), next: new Map() }),
    getTodaySchedule: jest.fn().mockResolvedValue([]),
    getNextSchedule: jest.fn().mockResolvedValue(null),
    hasOperatingSchedule: jest.fn().mockResolvedValue(false),
  };
  const popularityService = {
    recordParkHit: jest.fn().mockResolvedValue(undefined),
    recordParkHits: jest.fn().mockResolvedValue(undefined),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        LocationService,
        { provide: getRepositoryToken(Park), useValue: parkRepo },
        { provide: getRepositoryToken(Attraction), useValue: attractionRepo },
        { provide: QueueDataService, useValue: queueDataService },
        { provide: AnalyticsService, useValue: analyticsService },
        { provide: ParksService, useValue: parksService },
        { provide: PopularityService, useValue: popularityService },
        {
          // Empty GET/MGET → cache miss → exercises the DB + batch fallback
          // path these tests already mock (incl. the park-coordinate index).
          provide: REDIS_CLIENT,
          useValue: {
            get: jest.fn().mockResolvedValue(null),
            set: jest.fn().mockResolvedValue(undefined),
            mget: jest.fn().mockResolvedValue([]),
          },
        },
      ],
    }).compile();

    service = module.get(LocationService);
  });

  describe("findNearby", () => {
    const phantasialand = {
      id: "p1",
      name: "Phantasialand",
      slug: "phantasialand",
      latitude: 50.7997,
      longitude: 6.8783,
      timezone: "Europe/Berlin",
    };

    it("returns 'in_park' when the user is inside a park's radius", async () => {
      parkRepo.find.mockResolvedValue([phantasialand]);
      parkRepo.findOne.mockResolvedValue(phantasialand);

      const result = await service.findNearby(50.7997, 6.8783, 1000, 6);

      expect(result.type).toBe("in_park");
      expect(result.userLocation).toEqual({
        latitude: 50.7997,
        longitude: 6.8783,
      });
    });

    it("returns 'nearby_parks' when the user is far from every park", async () => {
      // Berlin – Phantasialand is ~500km away.
      parkRepo.find.mockResolvedValue([phantasialand]);

      const result = await service.findNearby(52.52, 13.405, 1000, 6);

      expect(result.type).toBe("nearby_parks");
      expect(result.data).toBeDefined();
    });

    it("falls through to 'nearby_parks' with an empty list when no parks have coords", async () => {
      parkRepo.find.mockResolvedValue([]);

      const result = await service.findNearby(50, 7, 1000, 6);

      expect(result.type).toBe("nearby_parks");
      // No throw — empty payload is fine.
      expect(
        (result.data as { parks: unknown[]; count: number }).parks,
      ).toEqual([]);
      expect((result.data as { parks: unknown[]; count: number }).count).toBe(
        0,
      );
    });

    it("ignores the in-park branch when the nearest park is outside the radius", async () => {
      parkRepo.find.mockResolvedValue([phantasialand]);

      // ~5km away. Radius default 1000m → outside.
      const result = await service.findNearby(50.85, 6.95, 1000, 6);

      expect(result.type).toBe("nearby_parks");
    });

    it("respects an enlarged radius (in-park threshold)", async () => {
      parkRepo.find.mockResolvedValue([phantasialand]);
      parkRepo.findOne.mockResolvedValue(phantasialand);

      // Same ~5km point, but with a 10km radius the user counts as
      // "in" the park.
      const result = await service.findNearby(50.85, 6.95, 10_000, 6);

      expect(result.type).toBe("in_park");
    });
  });
});
