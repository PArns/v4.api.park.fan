import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { RestaurantsService } from "./restaurants.service";
import { Restaurant } from "./entities/restaurant.entity";
import { RestaurantLiveData } from "./entities/restaurant-live-data.entity";
import {
  EntityLiveResponse,
  EntityType,
  LiveStatus,
} from "../external-apis/themeparks/themeparks.types";

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
});
