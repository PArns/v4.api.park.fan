import { readFileSync, readdirSync } from "fs";
import { join } from "path";
import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { WeatherService } from "./weather.service";
import { WeatherData } from "./entities/weather-data.entity";
import { Park } from "./entities/park.entity";
import { OpenMeteoClient } from "../external-apis/weather/open-meteo.client";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * A climate normal is not a forecast, and the wait-time model cannot tell the
 * difference. Everything it reads weather from is pinned here at the boundary:
 * the service methods that feed it, the table it trains on and the code that
 * builds its request. A change that lets a normal into any of them fails here.
 */
describe("climate normals never reach the wait-time model", () => {
  const normals = { "07-11": { temperatureMax: 25 } };
  const weatherRepo = {
    findOne: jest.fn(),
    save: jest.fn(),
    upsert: jest.fn(),
    createQueryBuilder: jest.fn(() => ({
      where: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      orderBy: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([]),
    })),
  };
  const parkRepo = {
    findOne: jest.fn().mockResolvedValue({
      id: "p1",
      timezone: "Europe/Berlin",
      latitude: 50.8,
      longitude: 6.87,
    }),
  };
  const client = {
    peekClimateNormals: jest.fn(),
    fetchClimateNormals: jest.fn(),
    getHourlyForecast: jest.fn().mockResolvedValue({ hours: [] }),
  };
  const redis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
    del: jest.fn(),
    incr: jest.fn().mockResolvedValue(1),
    expire: jest.fn().mockResolvedValue(1),
  };
  let service: WeatherService;

  beforeEach(async () => {
    jest.clearAllMocks();
    parkRepo.findOne.mockResolvedValue({
      id: "p1",
      timezone: "Europe/Berlin",
      latitude: 50.8,
      longitude: 6.87,
    });
    client.peekClimateNormals.mockResolvedValue(normals);
    client.fetchClimateNormals.mockResolvedValue(normals);
    redis.incr.mockResolvedValue(1);
    client.getHourlyForecast.mockResolvedValue({ hours: [] });
    redis.get.mockResolvedValue(null);
    const module = await Test.createTestingModule({
      providers: [
        WeatherService,
        { provide: getRepositoryToken(WeatherData), useValue: weatherRepo },
        { provide: getRepositoryToken(Park), useValue: parkRepo },
        { provide: OpenMeteoClient, useValue: client },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();
    service = module.get(WeatherService);
  });

  it("reading normals writes nothing to weather_data", async () => {
    await expect(service.getClimateNormals("p1")).resolves.toEqual(normals);
    await service.warmClimateNormals("p1");
    expect(weatherRepo.save).not.toHaveBeenCalled();
    expect(weatherRepo.upsert).not.toHaveBeenCalled();
  });

  it("the forecast readers never ask for normals", async () => {
    await service.getHourlyForecast("p1");
    await service.getCurrentAndForecast("p1");
    expect(client.peekClimateNormals).not.toHaveBeenCalled();
    expect(client.fetchClimateNormals).not.toHaveBeenCalled();
  });

  it("a cache miss returns null at once and warms in the background", async () => {
    client.peekClimateNormals.mockResolvedValue(null);
    await expect(service.getClimateNormals("p1")).resolves.toBeNull();
    await new Promise((r) => setImmediate(r));
    expect(client.fetchClimateNormals).toHaveBeenCalledWith(50.8, 6.87);
  });

  it("stops warming once the daily budget is spent", async () => {
    redis.incr.mockResolvedValue(21);
    await service.warmClimateNormals("p1");
    expect(client.fetchClimateNormals).not.toHaveBeenCalled();
  });

  it("does not retry a failed park for an hour, and never throws", async () => {
    client.fetchClimateNormals.mockRejectedValue(new Error("429"));
    await expect(service.warmClimateNormals("p1")).resolves.toBeUndefined();
    expect(redis.set).toHaveBeenCalledWith(
      "weather:climate-normals:failed:p1",
      "1",
      "EX",
      3600,
    );
    redis.get.mockResolvedValue("1");
    client.fetchClimateNormals.mockClear();
    await service.warmClimateNormals("p1");
    expect(client.fetchClimateNormals).not.toHaveBeenCalled();
  });

  it("serves null, not an empty normal, for a park without coordinates", async () => {
    parkRepo.findOne.mockResolvedValue({ id: "p1", latitude: null });
    await expect(service.getClimateNormals("p1")).resolves.toBeNull();
    expect(client.peekClimateNormals).not.toHaveBeenCalled();
  });

  it("treats a coordinate of 0 as a coordinate", async () => {
    parkRepo.findOne.mockResolvedValue({ id: "p1", latitude: 0, longitude: 0 });
    await expect(service.getClimateNormals("p1")).resolves.toEqual(normals);
  });

  describe("source boundary", () => {
    const root = join(__dirname, "..", "..");
    const read = (...parts: string[]) =>
      readFileSync(join(root, ...parts), "utf8");

    it("the ML service (Python) knows nothing about normals", () => {
      const dir = join(root, "ml-service");
      const files = readdirSync(dir).filter((f) => f.endsWith(".py"));
      expect(files.length).toBeGreaterThan(0);
      for (const f of files) {
        expect({
          f,
          hit: /climate[_-]?normal/i.test(read("ml-service", f)),
        }).toEqual({ f, hit: false });
      }
    });

    it("MLService builds its weather input from the forecast readers only", () => {
      const src = read("src", "ml", "ml.service.ts");
      expect(src).toContain("weatherService.getHourlyForecast");
      expect(src).not.toMatch(/getClimateNormals|climate[_-]?normal/i);
    });

    it("weather_data has no normal as a data type", () => {
      const src = read("src", "parks", "entities", "weather-data.entity.ts");
      expect(src).not.toMatch(/climate|normal/i);
    });

    it("the training query reads only weather_data", () => {
      const src = read("ml-service", "db.py");
      expect(src).toContain("FROM weather_data");
      expect(src).not.toMatch(/climate/i);
    });
  });
});
