import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { RestaurantsService } from "./restaurants.service";
import { Restaurant } from "./entities/restaurant.entity";
import { RestaurantLiveData } from "./entities/restaurant-live-data.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import {
  EntityLiveResponse,
  EntityType,
  LiveStatus,
} from "../external-apis/themeparks/themeparks.types";
import { generateSlug } from "../common/utils/slug.util";

describe("RestaurantsService", () => {
  let service: RestaurantsService;

  const mockRestaurantRepository = {
    findOne: jest.fn(),
    find: jest.fn(),
    create: jest.fn((x) => x),
    save: jest.fn().mockResolvedValue(undefined),
  };

  const mockRestaurantLiveDataRepository = {
    findOne: jest.fn(),
    create: jest.fn((x) => x),
    save: jest.fn().mockResolvedValue(undefined),
  };

  const mockThemeParksClient = {
    getEntityChildren: jest.fn(),
    getEntity: jest.fn(),
  };

  const mockThemeParksMapper = {
    mapRestaurant: jest.fn(),
  };

  const mockParksService = {
    ensureParksLoaded: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        RestaurantsService,
        {
          provide: getRepositoryToken(Restaurant),
          useValue: mockRestaurantRepository,
        },
        {
          provide: getRepositoryToken(RestaurantLiveData),
          useValue: mockRestaurantLiveDataRepository,
        },
        { provide: ThemeParksClient, useValue: mockThemeParksClient },
        { provide: ThemeParksMapper, useValue: mockThemeParksMapper },
        { provide: ParksService, useValue: mockParksService },
      ],
    }).compile();

    service = module.get<RestaurantsService>(RestaurantsService);

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("shouldSaveDiningAvailability (via saveDiningAvailability)", () => {
    const baseLiveData: EntityLiveResponse = {
      id: "ext-rest-1",
      name: "Test Restaurant",
      entityType: EntityType.RESTAURANT,
      status: LiveStatus.CLOSED,
      lastUpdated: new Date().toISOString(),
    };

    it("saves when no previous live data exists", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue(null);

      const result = await service.saveDiningAvailability(
        "rest-1",
        baseLiveData,
      );

      expect(result).toBe(1);
      expect(mockRestaurantLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the status changed", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        waitTime: null,
        partySize: null,
        operatingHours: null,
        timestamp: new Date(),
        restaurant: { park: { timezone: "UTC" } },
      });

      const result = await service.saveDiningAvailability("rest-1", {
        ...baseLiveData,
        status: LiveStatus.OPERATING,
      });

      expect(result).toBe(1);
      expect(mockRestaurantLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the wait time changed", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.OPERATING,
        waitTime: 10,
        partySize: null,
        operatingHours: null,
        timestamp: new Date(),
        restaurant: { park: { timezone: "UTC" } },
      });

      const result = await service.saveDiningAvailability("rest-1", {
        ...baseLiveData,
        status: LiveStatus.OPERATING,
        diningAvailability: { partySize: 0, waitTime: 25 },
      });

      expect(result).toBe(1);
      expect(mockRestaurantLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the party size changed", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.OPERATING,
        waitTime: null,
        partySize: 2,
        operatingHours: null,
        timestamp: new Date(),
        restaurant: { park: { timezone: "UTC" } },
      });

      const result = await service.saveDiningAvailability("rest-1", {
        ...baseLiveData,
        status: LiveStatus.OPERATING,
        diningAvailability: { partySize: 4, waitTime: 0 },
      });

      expect(result).toBe(1);
      expect(mockRestaurantLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the operating hours changed", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        waitTime: null,
        partySize: null,
        operatingHours: [
          { type: "OPERATING", startTime: "09:00", endTime: "18:00" },
        ],
        timestamp: new Date(),
        restaurant: { park: { timezone: "UTC" } },
      });

      const result = await service.saveDiningAvailability("rest-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
        operatingHours: [
          { type: "OPERATING", startTime: "09:00", endTime: "20:00" },
        ],
      });

      expect(result).toBe(1);
      expect(mockRestaurantLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves on day rollover even when nothing else changed", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        waitTime: null,
        partySize: null,
        operatingHours: null,
        // 25h guarantees a different UTC calendar date than "now" regardless
        // of what time the test happens to run at.
        timestamp: new Date(Date.now() - 25 * 60 * 60 * 1000),
        restaurant: { park: { timezone: "UTC" } },
      });

      const result = await service.saveDiningAvailability("rest-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
      });

      expect(result).toBe(1);
      expect(mockRestaurantLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("skips saving when nothing changed", async () => {
      mockRestaurantLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        waitTime: null,
        partySize: null,
        operatingHours: null,
        timestamp: new Date(),
        restaurant: { park: { timezone: "UTC" } },
      });

      const result = await service.saveDiningAvailability("rest-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
      });

      expect(result).toBe(0);
      expect(mockRestaurantLiveDataRepository.save).not.toHaveBeenCalled();
    });
  });

  describe("syncRestaurants", () => {
    it("upserts restaurants, keeps slugs unique, and only syncs ThemeParks.wiki parks", async () => {
      mockParksService.ensureParksLoaded.mockResolvedValue([
        { id: "park-1", externalId: "tp_1" },
        // Queue-Times-only park: never reaches the ThemeParks.wiki client.
        { id: "park-2", externalId: "qt-2" },
      ]);

      mockThemeParksClient.getEntityChildren.mockResolvedValue({
        children: [
          {
            id: "rest-ext-1",
            name: "Existing Diner",
            entityType: "RESTAURANT",
          },
          {
            id: "rest-ext-new-a",
            name: "Duplicate Grill",
            entityType: "RESTAURANT",
          },
          {
            id: "rest-ext-new-b",
            name: "Duplicate Grill",
            entityType: "RESTAURANT",
          },
          // Not a restaurant — must be filtered out before any mapping happens.
          { id: "show-ext", name: "Some Show", entityType: "SHOW" },
        ],
      });

      // Promise.all order matches the array literal in the service: first the
      // externalId lookup, then the park's used-slugs lookup.
      mockRestaurantRepository.find
        .mockResolvedValueOnce([
          {
            id: "existing-rest-id",
            externalId: "rest-ext-1",
            slug: "existing-diner",
          },
        ])
        .mockResolvedValueOnce([{ slug: "existing-diner" }]);

      mockThemeParksMapper.mapRestaurant.mockImplementation(
        (apiData: { id: string; name: string }, parkId: string) => ({
          externalId: apiData.id,
          name: apiData.name,
          slug: generateSlug(apiData.name),
          parkId,
        }),
      );

      const syncedCount = await service.syncRestaurants();

      // Wiki-only filter: the Queue-Times park is never asked for children.
      expect(mockThemeParksClient.getEntityChildren).toHaveBeenCalledTimes(1);
      expect(mockThemeParksClient.getEntityChildren).toHaveBeenCalledWith(
        "tp_1",
      );

      // Non-RESTAURANT children never reach the mapper.
      expect(mockThemeParksMapper.mapRestaurant).toHaveBeenCalledTimes(3);

      // Insert path: two restaurants sharing a name get distinct slugs, and
      // the existing one is carried through by reference (update path).
      expect(mockRestaurantRepository.save).toHaveBeenCalledWith([
        expect.objectContaining({
          id: "existing-rest-id",
          name: "Existing Diner",
        }),
        expect.objectContaining({
          externalId: "rest-ext-new-a",
          slug: "duplicate-grill",
        }),
        expect.objectContaining({
          externalId: "rest-ext-new-b",
          slug: "duplicate-grill-2",
        }),
      ]);

      expect(syncedCount).toBe(3);
    });
  });
});
