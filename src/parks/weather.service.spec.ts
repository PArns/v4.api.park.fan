import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { WeatherService } from "./weather.service";
import { WeatherData } from "./entities/weather-data.entity";
import { Park } from "./entities/park.entity";
import { OpenMeteoClient } from "../external-apis/weather/open-meteo.client";
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
    upsert: jest.fn().mockResolvedValue({ identifiers: [] }),
    update: jest.fn(),
    count: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      update: jest.fn().mockReturnThis(),
      set: jest.fn().mockReturnThis(),
      setParameters: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 0 }),
      getMany: jest.fn().mockResolvedValue([]),
    })),
  };

  const mockParkRepository = {
    findOne: jest.fn().mockResolvedValue({
      id: "park-123",
      timezone: "America/Los_Angeles",
    }),
  };

  const mockOpenMeteoClient = {
    getHistoricalWeather: jest.fn(),
    getWeatherForecast: jest.fn(),
    getMinutelyNowcast: jest.fn(),
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
          provide: getRepositoryToken(Park),
          useValue: mockParkRepository,
        },
        {
          provide: OpenMeteoClient,
          useValue: mockOpenMeteoClient,
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
      expect(mockWeatherDataRepository.upsert).toHaveBeenCalled();
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

  describe("getNowcast", () => {
    const PARK_ID = "park-nowcast";
    // Fix "now" to a wall-clock time that aligns with a 15-min slot boundary
    // for deterministic slot math.
    const NOW = new Date("2026-05-21T14:00:00.000Z");

    // Build an Open-Meteo-shaped response with 15-min steps starting at NOW.
    // `precip` is the mm value for each consecutive 15-min slot.
    // `codes` is an optional matching array of WMO codes (defaults to 0).
    // `gusts` is an optional matching array of wind gusts in km/h (defaults to 10).
    const buildNowcast = (
      precip: number[],
      codes?: number[],
      gusts?: number[],
    ) => {
      const steps = precip.map((p, i) => {
        const t = new Date(NOW.getTime() + i * 15 * 60 * 1000);
        // Open-Meteo returns naive local ISO without timezone suffix.
        // With park timezone "UTC", that's identical to the UTC wall clock.
        const localIso = t.toISOString().replace(/:\d\d\.\d{3}Z$/, ":00");
        return {
          time: localIso,
          precipitation: p,
          precipitationProbability: p > 0 ? 80 : 5,
          weatherCode: codes?.[i] ?? (p >= 0.1 ? 61 : 0),
          windSpeed: 10,
          windGusts: gusts?.[i] ?? 10,
        };
      });
      return {
        steps,
        current: {
          time: steps[0].time,
          precipitation: precip[0],
          weatherCode: codes?.[0] ?? 0,
          isDay: true,
          windSpeed: 10,
          windGusts: gusts?.[0] ?? 10,
        },
      };
    };

    beforeEach(() => {
      jest.useFakeTimers();
      jest.setSystemTime(NOW);
      mockRedis.get.mockReset();
      mockRedis.set.mockReset();
      mockOpenMeteoClient.getMinutelyNowcast.mockReset();
      mockParkRepository.findOne.mockReset();
      mockParkRepository.findOne.mockResolvedValue({
        id: PARK_ID,
        latitude: 50.0,
        longitude: 7.0,
        timezone: "UTC",
      });
    });

    afterEach(() => {
      jest.useRealTimers();
    });

    it("returns null when park has no coordinates", async () => {
      mockParkRepository.findOne.mockResolvedValueOnce({
        id: PARK_ID,
        latitude: null,
        longitude: null,
        timezone: "UTC",
      });

      const result = await service.getNowcast(PARK_ID);

      expect(result).toBeNull();
      expect(mockOpenMeteoClient.getMinutelyNowcast).not.toHaveBeenCalled();
    });

    it("returns null when upstream call fails", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockRejectedValueOnce(
        new Error("Open-Meteo API down"),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result).toBeNull();
    });

    it("returns cached response without calling upstream", async () => {
      const cached = {
        observedAt: NOW.toISOString(),
        nextUpdateAt: new Date(NOW.getTime() + 15 * 60 * 1000).toISOString(),
        currentlyRaining: false,
        currentPrecipitationMm: 0,
        currentWeatherCode: 0,
        rainStartsAt: null,
        rainStartsInMinutes: null,
        rainStartsIntensityMm: null,
        rainStartsIntensity: null,
        rainEndsAt: null,
        rainEndsInMinutes: null,
        thunderstormAt: null,
        thunderstormInMinutes: null,
        steps: [],
      };
      mockRedis.get.mockResolvedValueOnce(JSON.stringify(cached));

      const result = await service.getNowcast(PARK_ID);

      expect(result).toEqual(cached);
      expect(mockOpenMeteoClient.getMinutelyNowcast).not.toHaveBeenCalled();
      expect(mockParkRepository.findOne).not.toHaveBeenCalled();
    });

    it("detects current rain and predicts when it ends", async () => {
      // Slot 0 (now): 1.2 mm — raining
      // Slot 1 (+15 min): 0.5 mm — still raining
      // Slot 2 (+30 min): 0.0 mm — dry → rainEndsAt
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([1.2, 0.5, 0.0, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result).not.toBeNull();
      expect(result!.currentlyRaining).toBe(true);
      expect(result!.currentPrecipitationMm).toBe(1.2);
      // Rain ends 30 min from NOW
      expect(result!.rainEndsAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.rainEndsInMinutes).toBe(30);
      // Already raining → no rainStartsAt
      expect(result!.rainStartsAt).toBeNull();
      expect(result!.rainStartsInMinutes).toBeNull();
    });

    it("predicts when rain starts and how strong, plus when it ends", async () => {
      // Slot 0 (now): 0 — dry
      // Slot 1 (+15 min): 0 — dry
      // Slot 2 (+30 min): 0.8 mm — moderate rain starts
      // Slot 3 (+45 min): 0.3 mm — light rain
      // Slot 4 (+60 min): 0.0 — dry → rainEndsAt
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.8, 0.3, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result).not.toBeNull();
      expect(result!.currentlyRaining).toBe(false);
      expect(result!.rainStartsAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.rainStartsInMinutes).toBe(30);
      expect(result!.rainStartsIntensityMm).toBe(0.8);
      expect(result!.rainStartsIntensity).toBe("moderate");
      expect(result!.rainEndsAt).toBe(
        new Date(NOW.getTime() + 60 * 60 * 1000).toISOString(),
      );
      expect(result!.rainEndsInMinutes).toBe(60);
    });

    it("classifies rain intensity as light when below moderate threshold", async () => {
      // Slot 0 dry, Slot 1 = 0.3 mm → light (< 0.625)
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.3, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.rainStartsIntensity).toBe("light");
      expect(result!.rainStartsIntensityMm).toBe(0.3);
    });

    it("classifies rain intensity as heavy at or above 1.9 mm per 15-min slot", async () => {
      // Slot 0 dry, Slot 1 = 2.5 mm → heavy
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 2.5, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.rainStartsIntensity).toBe("heavy");
      expect(result!.rainStartsIntensityMm).toBe(2.5);
    });

    it("leaves rain fields null when forecast is fully dry", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentlyRaining).toBe(false);
      expect(result!.rainStartsAt).toBeNull();
      expect(result!.rainStartsInMinutes).toBeNull();
      expect(result!.rainStartsIntensityMm).toBeNull();
      expect(result!.rainStartsIntensity).toBeNull();
      expect(result!.rainEndsAt).toBeNull();
    });

    it("leaves rainEndsAt null when rain continues beyond the forecast window", async () => {
      // Currently raining and every future slot is also wet.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.5, 0.5, 0.5, 0.5]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentlyRaining).toBe(true);
      expect(result!.rainEndsAt).toBeNull();
      expect(result!.rainEndsInMinutes).toBeNull();
    });

    it("detects upcoming thunderstorm via WMO codes 95/96/99", async () => {
      // No rain, but slot 3 has thunderstorm code 95.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0], [0, 0, 0, 95]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.thunderstormAt).toBe(
        new Date(NOW.getTime() + 45 * 60 * 1000).toISOString(),
      );
      expect(result!.thunderstormInMinutes).toBe(45);
      // Code 95 is thunderstorm WITHOUT hail.
      expect(result!.hailAt).toBeNull();
      expect(result!.hailInMinutes).toBeNull();
    });

    it("flags hail separately from thunderstorm when WMO code is 96 or 99", async () => {
      // Slot 2 = code 96 (thunderstorm with slight hail).
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0], [0, 0, 96, 0]),
      );

      const result = await service.getNowcast(PARK_ID);

      // Both thunderstorm and hail should fire on this slot.
      expect(result!.thunderstormAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.thunderstormInMinutes).toBe(30);
      expect(result!.hailAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.hailInMinutes).toBe(30);
    });

    it("flags a storm when wind gusts reach the 75 km/h threshold", async () => {
      // Slot 0/1 calm, slot 2 = 80 km/h gust → storm alert at +30 min.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0], undefined, [10, 20, 80, 60]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.stormAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.stormInMinutes).toBe(30);
      // Wind data on the current slot + the peak across the window.
      expect(result!.currentWindGustsKmh).toBe(10);
      expect(result!.peakWindGustsKmh).toBe(80);
    });

    it("does not flag a storm when gusts stay below 75 km/h", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0], undefined, [40, 50, 70]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.stormAt).toBeNull();
      expect(result!.stormInMinutes).toBeNull();
      expect(result!.peakWindGustsKmh).toBe(70);
    });

    it("reports nextUpdateAt 15 minutes after observedAt", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.observedAt).toBe(NOW.toISOString());
      expect(result!.nextUpdateAt).toBe(
        new Date(NOW.getTime() + 15 * 60 * 1000).toISOString(),
      );
    });

    it("persists the derived result to Redis with a 15-minute TTL", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.5, 0.0]),
      );

      await service.getNowcast(PARK_ID);

      expect(mockRedis.set).toHaveBeenCalledWith(
        `weather:nowcast:park:${PARK_ID}`,
        expect.any(String),
        "EX",
        15 * 60,
      );
      const stored = JSON.parse(mockRedis.set.mock.calls[0][1]);
      expect(stored.rainStartsInMinutes).toBe(15);
      expect(stored.rainStartsIntensity).toBe("light");
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
