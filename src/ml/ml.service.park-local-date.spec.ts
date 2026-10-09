import { MLService } from "./ml.service";
import {
  parkLocalHourToUtcIso,
  synthesizeHourlyFromDaily,
} from "../parks/weather.service";
import { OpenMeteoClient } from "../external-apis/weather/open-meteo.client";

/**
 * PAR-818: CatBoost predictions carry UTC instants, schedules and TFT rows are
 * keyed on the park-local day, and Open-Meteo answers park-local wall-clock
 * hours without an offset. These pin the two conversions at the boundary.
 */
describe("MLService.localDateOf", () => {
  it("reads an instant on the park's clock, not the UTC date", () => {
    // 20:00 in Los Angeles (PDT) is 03:00 UTC of the next day.
    expect(
      MLService.localDateOf("2026-07-05T03:00:00+00:00", "America/Los_Angeles"),
    ).toBe("2026-07-04");
    // 08:00 in Tokyo is 23:00 UTC of the previous day.
    expect(
      MLService.localDateOf("2026-07-03T23:00:00.000Z", "Asia/Tokyo"),
    ).toBe("2026-07-04");
  });

  it("takes an offset-less TFT day at its word", () => {
    expect(
      MLService.localDateOf("2026-07-04T12:00:00", "America/Los_Angeles"),
    ).toBe("2026-07-04");
  });
});

describe("parkLocalHourToUtcIso", () => {
  it("turns an Open-Meteo local hour into the UTC instant it names", () => {
    expect(parkLocalHourToUtcIso("2026-07-04T14:00", "Europe/Berlin")).toBe(
      "2026-07-04T12:00:00.000Z",
    );
    expect(parkLocalHourToUtcIso("2026-07-04T14:00", "Asia/Tokyo")).toBe(
      "2026-07-04T05:00:00.000Z",
    );
    expect(
      parkLocalHourToUtcIso("2026-01-15T16:00", "America/Los_Angeles"),
    ).toBe("2026-01-16T00:00:00.000Z");
  });

  it("leaves a string that already is an instant on the same instant", () => {
    expect(parkLocalHourToUtcIso("2026-07-04T12:00:00Z", "Asia/Tokyo")).toBe(
      "2026-07-04T12:00:00.000Z",
    );
    expect(
      parkLocalHourToUtcIso("2026-07-04T14:00:00+02:00", "Asia/Tokyo"),
    ).toBe("2026-07-04T12:00:00.000Z");
  });
});

describe("hourly weather across a DST change (PAR-818)", () => {
  it("asks Open-Meteo for GMT and returns explicit UTC instants", async () => {
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
      mget: jest.fn().mockResolvedValue([null, null]),
      del: jest.fn(),
    };
    // Berlin's spring-forward night: in GMT every hour exists exactly once.
    const time = ["2026-03-29T00:00", "2026-03-29T01:00", "2026-03-29T02:00"];
    const get = jest.fn().mockResolvedValue({
      data: { hourly: { time, temperature_2m: [1, 2, 3] } },
    });
    const client = new OpenMeteoClient({} as never, redis as never);
    (client as unknown as { client: { get: jest.Mock } }).client = { get };

    const res = await client.getHourlyForecast(52.5, 13.4);

    expect(get.mock.calls[0][1].params.timezone).toBe("GMT");
    expect(
      res.hours.map((h) => parkLocalHourToUtcIso(h.time, "Europe/Berlin")),
    ).toEqual([
      "2026-03-29T00:00:00.000Z",
      "2026-03-29T01:00:00.000Z",
      "2026-03-29T02:00:00.000Z",
    ]);
  });

  const day = (date: string) => ({
    date: date as unknown as Date,
    temperatureMin: 0,
    temperatureMax: 10,
    precipitationSum: 23,
    rainSum: 0,
    snowfallSum: 0,
    weatherCode: 3,
    windSpeedMax: 10,
  });

  it("synthesises 23 distinct hours on spring-forward and 25 on fall-back", () => {
    const spring = synthesizeHourlyFromDaily(
      [day("2026-03-29")],
      "Europe/Berlin",
    );
    expect(spring).toHaveLength(23);
    expect(new Set(spring.map((h) => h.time)).size).toBe(23);
    expect(spring[0].time).toBe("2026-03-28T23:00:00.000Z"); // local 00:00 CET
    expect(spring[22].time).toBe("2026-03-29T21:00:00.000Z"); // local 23:00 CEST
    // The day's precipitation is spread over the hours the day actually has.
    const total = spring.reduce((s, h) => s + (h.precipitation ?? 0), 0);
    expect(total).toBeCloseTo(23);

    const autumn = synthesizeHourlyFromDaily(
      [day("2026-10-25")],
      "Europe/Berlin",
    );
    expect(autumn).toHaveLength(25);
    expect(new Set(autumn.map((h) => h.time)).size).toBe(25);
  });
});
