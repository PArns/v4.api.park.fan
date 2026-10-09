import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { WeatherWarningsService } from "./weather-warnings.service";
import { WeatherWarning } from "./entities/weather-warning.entity";
import { ParksService } from "./parks.service";
import { MeteoGateWarningsClient } from "../external-apis/weather/meteogate-warnings.client";
import { BrightSkyWarningsClient } from "../external-apis/weather/brightsky-warnings.client";
import { SourceWeatherWarning } from "../external-apis/weather/weather-warning.types";

const HOUR = 3_600_000;
const future = (hours: number) => new Date(Date.now() + hours * HOUR);
const past = (hours: number) => new Date(Date.now() - hours * HOUR);

/** A NL park inside the default test bbox (lon 4–5, lat 52–53). */
const nlPark = {
  id: "park-nl",
  countryCode: "NL",
  latitude: "52.5",
  longitude: "4.5",
};

function warning(
  overrides: Partial<SourceWeatherWarning> = {},
): SourceWeatherWarning {
  return {
    source: "meteogate",
    alertId: "alert-1",
    countryCode: "NL",
    event: "Onweer",
    eventEn: "Thunderstorm",
    severity: "Moderate",
    onset: past(1).toISOString(),
    expires: future(3).toISOString(),
    areas: [{ description: "Noord-Holland", bbox: [4, 52, 5, 53] }],
    ...overrides,
  };
}

describe("WeatherWarningsService", () => {
  let service: WeatherWarningsService;

  const insert = jest.fn();
  const del = jest.fn();
  const mockRepo = {
    find: jest.fn(),
    manager: {
      transaction: jest.fn(async (cb: (em: unknown) => Promise<void>) =>
        cb({ delete: del, insert }),
      ),
    },
  };
  const mockParks = { findAll: jest.fn() };
  const mockSource = {
    supportsCountry: jest.fn(),
    getActiveWarnings: jest.fn(),
    fetchAreaGeometry: jest.fn(),
  };
  const mockBrightSky = { getActiveWarningsForPoint: jest.fn() };

  beforeEach(async () => {
    jest.clearAllMocks();
    mockSource.supportsCountry.mockReturnValue(true);
    mockSource.getActiveWarnings.mockResolvedValue([]);
    mockParks.findAll.mockResolvedValue([nlPark]);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WeatherWarningsService,
        { provide: getRepositoryToken(WeatherWarning), useValue: mockRepo },
        { provide: ParksService, useValue: mockParks },
        { provide: MeteoGateWarningsClient, useValue: mockSource },
        { provide: BrightSkyWarningsClient, useValue: mockBrightSky },
      ],
    }).compile();

    service = module.get(WeatherWarningsService);
    jest.spyOn(service["logger"], "log").mockImplementation();
    jest.spyOn(service["logger"], "warn").mockImplementation();
  });

  describe("getActiveWarnings", () => {
    it("queries unexpired rows for the park, most severe first", async () => {
      const rows = [{ id: "w1" }];
      mockRepo.find.mockResolvedValue(rows);

      const result = await service.getActiveWarnings("park-nl");

      expect(result).toBe(rows);
      const arg = mockRepo.find.mock.calls[0][0];
      expect(arg.where.parkId).toBe("park-nl");
      expect(arg.where.expires).toBeDefined();
      expect(arg.order).toEqual({ severity: "DESC", onset: "ASC" });
    });
  });

  describe("syncWarnings", () => {
    it("writes no rows and still replaces the set when nothing is active", async () => {
      const result = await service.syncWarnings();

      expect(result).toEqual({ countries: 1, activeWarnings: 0, parkRows: 0 });
      expect(del).toHaveBeenCalledTimes(1);
      expect(insert).not.toHaveBeenCalled();
    });

    it("stores one row for a warning whose bbox contains the park", async () => {
      mockSource.getActiveWarnings.mockResolvedValue([warning()]);

      const result = await service.syncWarnings();

      expect(result).toEqual({ countries: 1, activeWarnings: 1, parkRows: 1 });
      expect(del).toHaveBeenCalledWith(WeatherWarning, {
        parkId: expect.anything(),
      });
      const rows = insert.mock.calls[0][1];
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        parkId: "park-nl",
        alertId: "alert-1",
        event: "Onweer",
        eventEn: "Thunderstorm",
        severity: "Moderate",
        area: "Noord-Holland",
        headline: null,
      });
      expect(rows[0].onset).toBeInstanceOf(Date);
      expect(rows[0].expires).toBeInstanceOf(Date);
    });

    it("keeps warnings of different severity as separate rows", async () => {
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({ alertId: "a", severity: "Moderate" }),
        warning({ alertId: "b", severity: "Severe" }),
      ]);

      const result = await service.syncWarnings();

      expect(result.parkRows).toBe(2);
      const severities = insert.mock.calls[0][1].map(
        (r: { severity: string }) => r.severity,
      );
      expect(severities.sort()).toEqual(["Moderate", "Severe"]);
    });

    it("drops expired warnings before matching and counting", async () => {
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({ alertId: "old", expires: past(1).toISOString() }),
        warning({ alertId: "no-expiry", expires: undefined }),
        warning({ alertId: "live" }),
      ]);

      const result = await service.syncWarnings();

      expect(result.activeWarnings).toBe(1);
      expect(
        insert.mock.calls[0][1].map((r: { alertId: string }) => r.alertId),
      ).toEqual(["live"]);
    });

    it("skips a warning whose bbox misses the park", async () => {
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({ areas: [{ bbox: [10, 40, 11, 41] }] }),
      ]);

      const result = await service.syncWarnings();

      expect(result.activeWarnings).toBe(1);
      expect(result.parkRows).toBe(0);
      expect(insert).not.toHaveBeenCalled();
    });

    it("refines the bbox match with the exact polygon", async () => {
      const outside = {
        type: "Polygon",
        coordinates: [
          [
            [4.8, 52.8],
            [4.9, 52.8],
            [4.9, 52.9],
            [4.8, 52.9],
            [4.8, 52.8],
          ],
        ],
      };
      mockSource.fetchAreaGeometry.mockResolvedValueOnce(outside);
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({
          areas: [{ bbox: [4, 52, 5, 53], geometryUrl: "https://x/geo" }],
        }),
      ]);

      const result = await service.syncWarnings();

      expect(mockSource.fetchAreaGeometry).toHaveBeenCalledWith(
        "https://x/geo",
      );
      expect(result.parkRows).toBe(0);
    });

    it("keeps the match when the polygon contains the park", async () => {
      const inside = {
        type: "Polygon",
        coordinates: [
          [
            [4.4, 52.4],
            [4.6, 52.4],
            [4.6, 52.6],
            [4.4, 52.6],
            [4.4, 52.4],
          ],
        ],
      };
      mockSource.fetchAreaGeometry.mockResolvedValueOnce(inside);
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({
          areas: [{ bbox: [4, 52, 5, 53], geometryUrl: "https://x/geo" }],
        }),
      ]);

      const result = await service.syncWarnings();

      expect(result.parkRows).toBe(1);
    });

    it("keeps the bbox match when the polygon cannot be fetched", async () => {
      mockSource.fetchAreaGeometry.mockResolvedValueOnce(null);
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({
          areas: [{ bbox: [4, 52, 5, 53], geometryUrl: "https://x/geo" }],
        }),
      ]);

      const result = await service.syncWarnings();

      expect(result.parkRows).toBe(1);
    });

    it("stores one row per park and alert when several areas match", async () => {
      mockSource.getActiveWarnings.mockResolvedValue([
        warning({
          areas: [
            { description: "First", bbox: [4, 52, 5, 53] },
            { description: "Second", bbox: [4, 52, 5, 53] },
          ],
        }),
      ]);

      await service.syncWarnings();

      const rows = insert.mock.calls[0][1];
      expect(rows).toHaveLength(1);
      expect(rows[0].area).toBe("First");
    });

    it("collapses hourly segments into one row spanning the full window", async () => {
      const first = warning({
        alertId: "seg-1",
        onset: past(2).toISOString(),
        expires: future(1).toISOString(),
      });
      const last = warning({
        alertId: "seg-3",
        onset: past(0.5).toISOString(),
        expires: future(5).toISOString(),
      });
      mockSource.getActiveWarnings.mockResolvedValue([last, first]);

      const result = await service.syncWarnings();

      expect(result.activeWarnings).toBe(2);
      expect(result.parkRows).toBe(1);
      const row = insert.mock.calls[0][1][0];
      expect(row.alertId).toBe("seg-3");
      expect(row.expires.getTime()).toBe(new Date(last.expires!).getTime());
      expect(row.onset.getTime()).toBe(new Date(first.onset!).getTime());
    });

    it("skips parks without a country, coordinates or source coverage", async () => {
      mockParks.findAll.mockResolvedValue([
        { ...nlPark, id: "no-cc", countryCode: null },
        { ...nlPark, id: "nan-lon", longitude: "abc" },
        { ...nlPark, id: "uncovered", countryCode: "US" },
      ]);
      mockSource.supportsCountry.mockImplementation(
        (cc: string) => cc !== "US",
      );

      const result = await service.syncWarnings();

      expect(result.countries).toBe(0);
      expect(mockSource.getActiveWarnings).not.toHaveBeenCalled();
      expect(mockRepo.manager.transaction).not.toHaveBeenCalled();
    });

    it("queries Bright Sky per park for Germany and skips MeteoGate", async () => {
      mockParks.findAll.mockResolvedValue([
        { id: "de-1", countryCode: "DE", latitude: "48.2", longitude: "7.7" },
        { id: "de-2", countryCode: "DE", latitude: "51.0", longitude: "6.0" },
      ]);
      mockBrightSky.getActiveWarningsForPoint.mockImplementation(
        async (lat: number) =>
          lat > 50
            ? []
            : [
                warning({
                  source: "brightsky",
                  countryCode: "DE",
                  alertId: "dwd-1",
                  event: "Hitzewarnung",
                  areas: [{ description: "Ortenaukreis" }],
                }),
              ],
      );

      const result = await service.syncWarnings();

      expect(mockSource.getActiveWarnings).not.toHaveBeenCalled();
      expect(mockBrightSky.getActiveWarningsForPoint).toHaveBeenCalledWith(
        48.2,
        7.7,
      );
      expect(result).toEqual({ countries: 1, activeWarnings: 1, parkRows: 1 });
      expect(insert.mock.calls[0][1][0]).toMatchObject({
        parkId: "de-1",
        event: "Hitzewarnung",
        area: "Ortenaukreis",
      });
      expect(del).toHaveBeenCalledWith(WeatherWarning, {
        parkId: expect.anything(),
      });
    });

    it("treats a Bright Sky failure for one park as no warnings", async () => {
      mockParks.findAll.mockResolvedValue([
        { id: "de-1", countryCode: "DE", latitude: "48.2", longitude: "7.7" },
      ]);
      mockBrightSky.getActiveWarningsForPoint.mockRejectedValue(
        new Error("503"),
      );

      const result = await service.syncWarnings();

      expect(result.parkRows).toBe(0);
      expect(del).toHaveBeenCalledTimes(1);
    });

    it("logs and leaves stored rows alone when the source fails", async () => {
      mockSource.getActiveWarnings.mockRejectedValue(new Error("timeout"));

      const result = await service.syncWarnings();

      expect(result).toEqual({ countries: 1, activeWarnings: 0, parkRows: 0 });
      expect(mockRepo.manager.transaction).not.toHaveBeenCalled();
      expect(service["logger"].warn).toHaveBeenCalledWith(
        expect.stringContaining("NL: timeout"),
      );
    });

    it("keeps syncing other countries after one fails", async () => {
      mockParks.findAll.mockResolvedValue([
        nlPark,
        { id: "be-1", countryCode: "BE", latitude: "50.8", longitude: "4.4" },
      ]);
      mockSource.getActiveWarnings.mockImplementation(async (cc: string) => {
        if (cc === "NL") throw new Error("down");
        return [
          warning({
            countryCode: "BE",
            alertId: "be-alert",
            areas: [{ bbox: [4, 50, 5, 51] }],
          }),
        ];
      });

      const result = await service.syncWarnings();

      expect(result).toEqual({ countries: 2, activeWarnings: 1, parkRows: 1 });
      expect(insert.mock.calls[0][1][0].parkId).toBe("be-1");
    });
  });
});
