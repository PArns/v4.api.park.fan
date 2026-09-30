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
    getClimateNormals: jest.fn().mockResolvedValue(normals),
    getHourlyForecast: jest.fn().mockResolvedValue({ hours: [] }),
  };
  const redis = {
    get: jest.fn().mockResolvedValue(null),
    set: jest.fn().mockResolvedValue("OK"),
    del: jest.fn(),
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
    client.getClimateNormals.mockResolvedValue(normals);
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
    expect(weatherRepo.save).not.toHaveBeenCalled();
    expect(weatherRepo.upsert).not.toHaveBeenCalled();
  });

  it("caches normals apart from the forecast keys the model's readers use", async () => {
    await service.getClimateNormals("p1");
    const keys = redis.set.mock.calls.map((c) => c[0] as string);
    expect(keys).toEqual(["weather:climate-normals:p1"]);
  });

  it("the forecast readers never ask for normals", async () => {
    await service.getHourlyForecast("p1");
    await service.getCurrentAndForecast("p1");
    expect(client.getClimateNormals).not.toHaveBeenCalled();
  });

  it("serves null, not an empty normal, for a park without coordinates", async () => {
    parkRepo.findOne.mockResolvedValue({ id: "p1", latitude: null });
    await expect(service.getClimateNormals("p1")).resolves.toBeNull();
    expect(client.getClimateNormals).not.toHaveBeenCalled();
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
