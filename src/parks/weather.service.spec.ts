import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { WeatherService } from "./weather.service";
import { WeatherData } from "./entities/weather-data.entity";
import { REDIS_CLIENT } from "../common/redis/redis.module";

describe("WeatherService", () => {
  let service: WeatherService;

  const mockRedis = {
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
  };

  const mockWeatherDataRepository = {
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      update: jest.fn().mockReturnThis(),
      set: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 0 }),
      getMany: jest.fn().mockResolvedValue([]),
    })),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WeatherService,
        {
          provide: getRepositoryToken(WeatherData),
          useValue: mockWeatherDataRepository,
        },
        {
          provide: REDIS_CLIENT,
          useValue: mockRedis,
        },
      ],
    }).compile();

    service = module.get<WeatherService>(WeatherService);

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("saveWeatherData", () => {
    it("should save new weather data", async () => {
      const parkId = "park-123";
      const weatherData = [
        {
          date: "2025-12-15",
          temperatureMax: 28,
          temperatureMin: 18,
          precipitationSum: 0,
          rainSum: 0,
          snowfallSum: 0,
          weatherCode: 0,
          windSpeedMax: 15,
        },
      ];

      mockWeatherDataRepository.findOne.mockResolvedValue(null);
      mockWeatherDataRepository.save.mockResolvedValue({});

      const result = await service.saveWeatherData(
        parkId,
        weatherData,
        "current",
      );

      expect(result).toBe(1);
      expect(mockWeatherDataRepository.save).toHaveBeenCalled();
    });
  });

  describe("getWeatherData", () => {
    it("should return weather data for date range", async () => {
      const parkId = "park-123";
      const start = new Date("2025-12-01");
      const end = new Date("2025-12-07");

      mockWeatherDataRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      const result = await service.getWeatherData(parkId, start, end);

      expect(result).toEqual([]);
    });
  });

  describe("getCurrentAndForecast", () => {
    it("should return cached data when available", async () => {
      const parkId = "park-123";
      const cachedData = {
        current: {
          parkId,
          date: new Date().toISOString(),
          temperatureMax: 25,
        },
        forecast: [],
      };

      mockRedis.get.mockResolvedValue(JSON.stringify(cachedData));

      const result = await service.getCurrentAndForecast(parkId);

      expect(result).toBeDefined();
      expect(mockRedis.get).toHaveBeenCalledWith(`weather:forecast:${parkId}`);
    });

    it("should fetch and cache data when not cached", async () => {
      mockRedis.get.mockResolvedValue(null);
      mockWeatherDataRepository
        .createQueryBuilder()
        .getMany.mockResolvedValue([]);

      const result = await service.getCurrentAndForecast("park-123");

      expect(result).toHaveProperty("current");
      expect(result).toHaveProperty("forecast");
      expect(mockRedis.set).toHaveBeenCalled();
    });
  });

  describe("hasHistoricalData", () => {
    it("should return true when historical data exists", async () => {
      mockWeatherDataRepository.count.mockResolvedValue(10);

      const result = await service.hasHistoricalData("park-123");

      expect(result).toBe(true);
    });

    it("should return false when no historical data exists", async () => {
      mockWeatherDataRepository.count.mockResolvedValue(0);

      const result = await service.hasHistoricalData("park-999");

      expect(result).toBe(false);
    });
  });
});
