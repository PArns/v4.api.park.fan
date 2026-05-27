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
    // `opts` lets individual tests override the `current` snapshot and the
    // `daily` summary (temperature / humidity / day-night / min-max).
    const buildNowcast = (
      precip: number[],
      codes?: number[],
      gusts?: number[],
      opts: {
        temperature?: number | null;
        apparentTemperature?: number | null;
        humidity?: number | null;
        isDay?: boolean | null;
        temperatureMax?: number | null;
        temperatureMin?: number | null;
      } = {},
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
      // `pick` honours an explicit `null` override (distinct from "not provided").
      const pick = <T>(key: keyof typeof opts, fallback: T): T | null =>
        key in opts ? ((opts[key] as T | null) ?? null) : fallback;

      return {
        steps,
        current: {
          time: steps[0].time,
          temperature: pick("temperature", 15.4),
          apparentTemperature: pick("apparentTemperature", 13.1),
          humidity: pick("humidity", 78),
          precipitation: precip[0],
          weatherCode: codes?.[0] ?? 0,
          isDay: pick("isDay", true),
          windSpeed: 10,
          windGusts: gusts?.[0] ?? 10,
        },
        daily: {
          temperatureMax: pick("temperatureMax", 14),
          temperatureMin: pick("temperatureMin", 8),
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

    it("serves a cached ParkNowcast verbatim without re-fetching", async () => {
      const cached: Partial<Awaited<ReturnType<typeof service.getNowcast>>> = {
        observedAt: NOW.toISOString(),
        nextUpdateAt: new Date(NOW.getTime() + 15 * 60 * 1000).toISOString(),
        currentlyRaining: true,
        currentPrecipitationMm: 0.7,
        rainEndsAt: new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      };
      mockRedis.get.mockResolvedValueOnce(JSON.stringify(cached));

      const result = await service.getNowcast(PARK_ID);

      // Whole cached object is returned as-is.
      expect(result).toEqual(cached);
      // Upstream + park lookup are skipped on cache hits.
      expect(mockOpenMeteoClient.getMinutelyNowcast).not.toHaveBeenCalled();
      expect(mockParkRepository.findOne).not.toHaveBeenCalled();
    });

    it("detects current rain and predicts when it ends as a timestamp", async () => {
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
      expect(result!.rainEndsAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      // Already raining → no rainStartsAt.
      expect(result!.rainStartsAt).toBeNull();
      expect(result!.rainStartsIntensityMm).toBeNull();
      expect(result!.rainStartsIntensity).toBeNull();
    });

    it("predicts rain start (with intensity) and end as absolute timestamps", async () => {
      // Slot 0/1 dry; slot 2 (+30 min) = 0.8 mm (moderate); slot 4 (+60 min) dry.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.8, 0.3, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentlyRaining).toBe(false);
      expect(result!.rainStartsAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.rainStartsIntensityMm).toBe(0.8);
      expect(result!.rainStartsIntensity).toBe("moderate");
      expect(result!.rainEndsAt).toBe(
        new Date(NOW.getTime() + 60 * 60 * 1000).toISOString(),
      );
    });

    it("classifies rain intensity as light when below the moderate threshold", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.3, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.rainStartsIntensity).toBe("light");
      expect(result!.rainStartsIntensityMm).toBe(0.3);
    });

    it("classifies rain intensity as heavy at or above 1.9 mm per 15-min slot", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 2.5, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.rainStartsIntensity).toBe("heavy");
      expect(result!.rainStartsIntensityMm).toBe(2.5);
    });

    it("leaves all rain fields null when the forecast is fully dry", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentlyRaining).toBe(false);
      expect(result!.rainStartsAt).toBeNull();
      expect(result!.rainStartsIntensityMm).toBeNull();
      expect(result!.rainStartsIntensity).toBeNull();
      expect(result!.rainEndsAt).toBeNull();
    });

    it("leaves rainEndsAt null when rain continues beyond the forecast window", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.5, 0.5, 0.5, 0.5]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentlyRaining).toBe(true);
      expect(result!.rainEndsAt).toBeNull();
    });

    it("detects an upcoming thunderstorm via WMO codes 95/96/99 with a clear-time", async () => {
      // Slot 3 = thunderstorm, slot 4 = thunderstorm continues, slot 5 = clear.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0, 0, 0, 95, 95, 0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.thunderstormStartsAt).toBe(
        new Date(NOW.getTime() + 45 * 60 * 1000).toISOString(),
      );
      expect(result!.thunderstormEndsAt).toBe(
        new Date(NOW.getTime() + 75 * 60 * 1000).toISOString(),
      );
      // Code 95 is thunderstorm WITHOUT hail → hail fields stay null.
      expect(result!.hailStartsAt).toBeNull();
      expect(result!.hailEndsAt).toBeNull();
    });

    it("flags hail separately when WMO code 96 is present, with a clear-time", async () => {
      // Slot 2 = hail (code 96), slot 3 = clear.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0], [0, 0, 96, 0]),
      );

      const result = await service.getNowcast(PARK_ID);

      const start = new Date(NOW.getTime() + 30 * 60 * 1000).toISOString();
      const end = new Date(NOW.getTime() + 45 * 60 * 1000).toISOString();
      expect(result!.thunderstormStartsAt).toBe(start);
      expect(result!.thunderstormEndsAt).toBe(end);
      expect(result!.hailStartsAt).toBe(start);
      expect(result!.hailEndsAt).toBe(end);
    });

    it("leaves thunderstormEndsAt null when the thunderstorm continues past the window", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0], [0, 0, 95]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.thunderstormStartsAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.thunderstormEndsAt).toBeNull();
    });

    it("flags a storm when wind gusts reach 75 km/h, with a clear-time when they die down", async () => {
      // Slot 2 = 80 km/h gust → storm; slot 3 = 60 km/h → calm again.
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0, 0.0], undefined, [10, 20, 80, 60]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.stormStartsAt).toBe(
        new Date(NOW.getTime() + 30 * 60 * 1000).toISOString(),
      );
      expect(result!.stormEndsAt).toBe(
        new Date(NOW.getTime() + 45 * 60 * 1000).toISOString(),
      );
      expect(result!.currentWindGustsKmh).toBe(10);
      expect(result!.peakWindGustsKmh).toBe(80);
    });

    it("does not flag a storm when gusts stay below 75 km/h", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0], undefined, [40, 50, 70]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.stormStartsAt).toBeNull();
      expect(result!.stormEndsAt).toBeNull();
      expect(result!.peakWindGustsKmh).toBe(70);
    });

    it("sets observedAt to the fetch time and nextUpdateAt 15 min later", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.observedAt).toBe(NOW.toISOString());
      expect(result!.nextUpdateAt).toBe(
        new Date(NOW.getTime() + 15 * 60 * 1000).toISOString(),
      );
    });

    it(
      "returns a fully stable response across the cache window — every " +
        "field is either an absolute timestamp or a snapshot at observedAt, " +
        "so two reads minutes apart are identical",
      async () => {
        // Forecast: dry at fetch, rain at +30 min for 15 min, then dry.
        const raw = buildNowcast([0.0, 0.0, 0.8, 0.0]);
        mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(raw);

        // First call (cache miss) → derives + populates the cache.
        const first = await service.getNowcast(PARK_ID);
        const cachedJson = mockRedis.set.mock.calls[0][1] as string;

        // 10 min later: same cache entry is replayed.
        jest.setSystemTime(new Date(NOW.getTime() + 10 * 60 * 1000));
        mockRedis.get.mockResolvedValueOnce(cachedJson);

        const second = await service.getNowcast(PARK_ID);

        // The whole object is byte-for-byte identical. No field silently
        // drifts as wall-clock time moves forward within the cache TTL.
        expect(second).toEqual(first);
      },
    );

    it("caches the fully derived ParkNowcast with a 30-minute TTL", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.5, 0.0]),
      );

      const result = await service.getNowcast(PARK_ID);

      // Upstream cache cadence is 30 min (~half Open-Meteo's ~15 min refresh);
      // decoupled from the client's 15 min poll via on-read nextUpdateAt.
      expect(mockRedis.set).toHaveBeenCalledWith(
        `weather:nowcast:park:${PARK_ID}`,
        expect.any(String),
        "EX",
        30 * 60,
      );
      const stored = JSON.parse(mockRedis.set.mock.calls[0][1]);
      // The stored payload IS the response — no derivation happens on a hit.
      expect(stored).toEqual(result);
    });

    it("exposes the current temperature / feels-like / humidity snapshot", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0, 0.0, 0.0], undefined, undefined, {
          temperature: 15.4,
          apparentTemperature: 13.1,
          humidity: 78,
        }),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentTemperatureC).toBe(15.4);
      expect(result!.currentApparentTemperatureC).toBe(13.1);
      expect(result!.currentHumidity).toBe(78);
    });

    it("exposes today's daily min / max temperature for the park", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0], undefined, undefined, {
          temperatureMax: 14,
          temperatureMin: 8,
        }),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.temperatureMaxC).toBe(14);
      expect(result!.temperatureMinC).toBe(8);
    });

    it("exposes isDay so clients can pick day/night icon variants", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0], undefined, undefined, { isDay: false }),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.isDay).toBe(false);
    });

    it("propagates null temperature / humidity when upstream omits them", async () => {
      mockOpenMeteoClient.getMinutelyNowcast.mockResolvedValueOnce(
        buildNowcast([0.0], undefined, undefined, {
          temperature: null,
          apparentTemperature: null,
          humidity: null,
          isDay: null,
          temperatureMax: null,
          temperatureMin: null,
        }),
      );

      const result = await service.getNowcast(PARK_ID);

      expect(result!.currentTemperatureC).toBeNull();
      expect(result!.currentApparentTemperatureC).toBeNull();
      expect(result!.currentHumidity).toBeNull();
      expect(result!.isDay).toBeNull();
      expect(result!.temperatureMaxC).toBeNull();
      expect(result!.temperatureMinC).toBeNull();
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
