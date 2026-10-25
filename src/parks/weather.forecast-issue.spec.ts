import { buildForecastIssueRows, WeatherService } from "./weather.service";
import { WeatherForecastIssue } from "./entities/weather-forecast-issue.entity";
import { WeatherProcessor } from "../queues/processors/weather.processor";
import { DailyWeather } from "../external-apis/weather/open-meteo.client";

const dayOf = (date: string): DailyWeather => ({
  date,
  temperatureMax: 20,
  temperatureMin: 10,
  precipitationSum: 1.5,
  rainSum: 1.5,
  snowfallSum: 0,
  weatherCode: 61,
  windSpeedMax: 12,
});

describe("weather_forecast_issues (PAR-1061)", () => {
  it("counts lead days from the park-local date of the issue", () => {
    // 00:00 UTC is still the previous evening in Los Angeles: the forecast's
    // first day (the park's today) is lead 0 there, not -1.
    const issuedAt = new Date("2026-10-24T00:00:00Z");
    const rows = buildForecastIssueRows(
      { id: "p1", timezone: "America/Los_Angeles" },
      [dayOf("2026-10-23"), dayOf("2026-10-24"), dayOf("2026-11-07")],
      issuedAt,
    );
    expect(rows.map((r) => r.leadDays)).toEqual([0, 1, 15]);
    expect(rows[0]).toMatchObject({
      parkId: "p1",
      targetDate: "2026-10-23",
      issuedAt,
      precipSum: 1.5,
      snowSum: 0,
      weatherCode: 61,
    });

    const berlin = buildForecastIssueRows(
      { id: "p1", timezone: "Europe/Berlin" },
      [dayOf("2026-10-24")],
      issuedAt,
    );
    expect(berlin[0].leadDays).toBe(0);
  });

  it("inserts idempotently: ON CONFLICT DO NOTHING per (park, target, issued_at)", async () => {
    const qb = {
      insert: jest.fn().mockReturnThis(),
      into: jest.fn().mockReturnThis(),
      values: jest.fn().mockReturnThis(),
      orIgnore: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({}),
    };
    const service = new WeatherService(
      { manager: { createQueryBuilder: () => qb } } as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
    );
    const n = await service.saveForecastIssue(
      { id: "p1", timezone: "UTC" },
      [dayOf("2026-10-24"), dayOf("2026-10-25")],
      new Date("2026-10-24T12:00:00Z"),
    );
    expect(n).toBe(2);
    expect(qb.into).toHaveBeenCalledWith(WeatherForecastIssue);
    expect(qb.values.mock.calls[0][0]).toHaveLength(2);
    expect(qb.orIgnore).toHaveBeenCalled();
  });

  describe("the full weather sync", () => {
    const parks = [
      { id: "a", name: "A", timezone: "UTC", latitude: 1, longitude: 1 },
      // Same coordinate as A: shares the response and therefore its issued_at.
      { id: "b", name: "B", timezone: "UTC", latitude: 1, longitude: 1 },
    ];
    const response = {
      days: [dayOf("2026-10-24"), dayOf("2026-10-25")],
      current: null,
      fetchedAt: "2026-10-24T12:00:05.000Z",
    };
    const build = () => {
      const weatherService = {
        saveWeatherData: jest.fn().mockResolvedValue(1),
        saveForecastIssue: jest.fn().mockResolvedValue(2),
      };
      const client = {
        getDailyWeather: jest.fn().mockResolvedValue(response),
      };
      const redis = {
        get: jest.fn().mockResolvedValue(null),
        set: jest.fn(),
        del: jest.fn(),
        ttl: jest.fn(),
      };
      const processor = new WeatherProcessor(
        { findAll: jest.fn().mockResolvedValue(parks) } as never,
        weatherService as never,
        client as never,
        redis as never,
      );
      return { processor, weatherService, client };
    };

    it("archives one issue per park, from the response it already fetched", async () => {
      const { processor, weatherService, client } = build();
      await processor.handleSyncWeather({ data: {} } as never);

      expect(weatherService.saveForecastIssue).toHaveBeenCalledTimes(2);
      for (const [park, days, issuedAt] of weatherService.saveForecastIssue.mock
        .calls) {
        expect(parks.map((p) => p.id)).toContain(park.id);
        // All 16 forecast days including the park's today (lead 0).
        expect(days).toBe(response.days);
        expect(issuedAt).toEqual(new Date(response.fetchedAt));
      }
      // 1 unique location + 1 per park — the archive adds no call.
      expect(client.getDailyWeather).toHaveBeenCalledTimes(3);
    });

    it("archives nothing on the current-only run", async () => {
      const { processor, weatherService } = build();
      await processor.handleSyncWeather({
        data: { currentOnly: true },
      } as never);
      expect(weatherService.saveForecastIssue).not.toHaveBeenCalled();
    });
  });
});
