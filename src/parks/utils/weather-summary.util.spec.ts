import {
  toClimateNormalSummary,
  toWeatherSummary,
  weatherView,
} from "./weather-summary.util";

/**
 * A day nobody has a forecast for must not read as a cold, clear day.
 *
 * `weather_data` carries a row per park per day, and the row at the edge of the
 * forecast window is written before the model reaches that far: every column
 * null, a placeholder. The calendar used to map it with `Number(x) ?? 0`, which
 * cannot work — `Number(null)` is 0, so the `?? 0` never runs and a missing
 * temperature became a real one. On 2026-08-28 that put "0°–0°" on the calendar
 * tile of 22 parks, all on the same day: the sixteenth.
 */
describe("toWeatherSummary", () => {
  const aRow = (overrides: Record<string, unknown> = {}) => ({
    temperatureMin: 12.5,
    temperatureMax: 21,
    precipitationSum: 1.2,
    snowfallSum: 0,
    windSpeedMax: 14,
    weatherCode: 3,
    ...overrides,
  });

  it("maps a full row", () => {
    expect(toWeatherSummary(aRow())).toMatchObject({
      tempMin: 12.5,
      tempMax: 21,
      icon: 3,
      precipitationMm: 1.2,
      windMax: 14,
    });
  });

  it("serves nothing for a row with no temperatures at all", () => {
    // The placeholder row at the edge of the forecast window.
    expect(
      toWeatherSummary({
        temperatureMin: null,
        temperatureMax: null,
        precipitationSum: null,
        snowfallSum: null,
        windSpeedMax: null,
        weatherCode: null,
      }),
    ).toBeUndefined();
  });

  it("serves nothing for a missing row", () => {
    expect(toWeatherSummary(null)).toBeUndefined();
    expect(toWeatherSummary(undefined)).toBeUndefined();
  });

  it("never turns a null temperature into 0 °C", () => {
    // The half-filled case: a code arrived, the temperatures did not. Zero is a
    // temperature people plan around — it must not stand in for "unknown".
    const summary = toWeatherSummary(
      aRow({ temperatureMin: null, temperatureMax: null, weatherCode: 3 }),
    );
    expect(summary).toBeUndefined();
  });

  it("keeps a genuine 0 °C", () => {
    // A winter day really can be 0°, and that is not missing data.
    const summary = toWeatherSummary(
      aRow({ temperatureMin: 0, temperatureMax: 0 }),
    );
    expect(summary?.tempMin).toBe(0);
    expect(summary?.tempMax).toBe(0);
  });

  it("fills one end from the other when only one is known", () => {
    // Better a single figure than a range with an invented end.
    expect(toWeatherSummary(aRow({ temperatureMin: null }))).toMatchObject({
      tempMin: 21,
      tempMax: 21,
    });
  });

  it("reads the decimal columns as numbers, not strings", () => {
    // Postgres `numeric` comes back from TypeORM as a string.
    const summary = toWeatherSummary(
      aRow({ temperatureMin: "12.50", temperatureMax: "21.00" }),
    );
    expect(summary?.tempMin).toBe(12.5);
    expect(summary?.tempMax).toBe(21);
  });

  it("omits the optional fields it has no value for", () => {
    const summary = toWeatherSummary(
      aRow({ snowfallSum: null, windSpeedMax: null }),
    );
    expect(summary?.snowMm).toBeUndefined();
    expect(summary?.windMax).toBeUndefined();
  });

  it("says 'unknown' only about the condition, and only when there is none", () => {
    const summary = toWeatherSummary(aRow({ weatherCode: null }));
    expect(summary?.condition).toBe("unknown");
    expect(summary?.icon).toBe(0);
  });
});

describe("weather code 0", () => {
  it("is clear sky, not unknown", () => {
    const summary = toWeatherSummary({
      temperatureMin: 10,
      temperatureMax: 20,
      weatherCode: 0,
    });
    expect(summary?.condition).not.toBe("unknown");
    expect(summary?.icon).toBe(0);
  });
});

describe("weather basis", () => {
  const normal = {
    temperatureMin: 14,
    temperatureMax: 26.5,
    precipitationSum: 2.1,
    rainSum: 2.1,
    snowfallSum: 0,
    windSpeedMax: 18,
    weatherCode: 3,
  };

  it("marks a synced day as a forecast", () => {
    expect(toWeatherSummary(normal)?.basis).toBe("forecast");
  });

  it("marks a normal as one and names its period", () => {
    expect(toClimateNormalSummary(normal)).toMatchObject({
      basis: "climate_normal",
      normalPeriod: "2015-2024",
      tempMin: 14,
      tempMax: 26.5,
    });
  });

  it("carries the spread of a normal that has one, and none for an older one", () => {
    expect(
      toClimateNormalSummary({
        ...normal,
        temperatureMaxP25: 24.9,
        temperatureMaxP75: 28.1,
        temperatureMinP25: 12.8,
        temperatureMinP75: 15.3,
        wetDayShare: 0.41,
      }),
    ).toMatchObject({
      tempMaxP25: 24.9,
      tempMaxP75: 28.1,
      tempMinP25: 12.8,
      tempMinP75: 15.3,
      wetDayPercent: 41,
    });
    expect(toClimateNormalSummary(normal)).not.toHaveProperty("tempMaxP25");
  });

  it("serves no normal for a day without one or without a temperature", () => {
    expect(toClimateNormalSummary(undefined)).toBeUndefined();
    expect(
      toClimateNormalSummary({
        ...normal,
        temperatureMin: null,
        temperatureMax: null,
      }),
    ).toBeUndefined();
  });
});

describe("the extra daily fields", () => {
  const row = {
    temperatureMin: "7.00",
    temperatureMax: "9.80",
    precipitationSum: "47.50",
    windSpeedMax: "32.00",
    windGustsMax: "61.20",
    precipitationProbabilityMax: 96,
    precipitationHours: "14.0",
    uvIndexMax: "0.85",
    sunshineHours: "1.5",
    weatherCode: 81,
  };

  it("carries the chance of rain, its hours, the gusts, UV and sunshine as numbers", () => {
    expect(toWeatherSummary(row)).toMatchObject({
      precipitationMm: 47.5,
      precipitationProbability: 96,
      precipitationHours: 14,
      windMax: 32,
      windGustsMax: 61.2,
      uvIndexMax: 0.85,
      sunshineHours: 1.5,
    });
  });

  it("leaves them out of a row stored before they were fetched", () => {
    const summary = toWeatherSummary({
      temperatureMin: 7,
      temperatureMax: 9.8,
      precipitationSum: 2,
    });
    expect(summary).not.toHaveProperty("precipitationProbability", 0);
    expect(summary?.precipitationProbability).toBeUndefined();
    expect(summary?.windGustsMax).toBeUndefined();
  });
});

describe("weatherView", () => {
  it("names every figure with its unit and drops the legacy rainChance", () => {
    const view = weatherView(
      toWeatherSummary({
        temperatureMin: 7,
        temperatureMax: 9.8,
        precipitationSum: 12.6,
        windSpeedMax: 32,
        windGustsMax: 61.2,
        precipitationProbabilityMax: 80,
        precipitationHours: 9,
        weatherCode: 53,
      }),
    );
    expect(view).toMatchObject({
      basis: "forecast",
      weatherCode: 53,
      precipitationMm: 12.6,
      precipitationChancePercent: 80,
      precipitationHours: 9,
      windMaxKmh: 32,
      windGustsMaxKmh: 61.2,
    });
    expect(view).not.toHaveProperty("rainChance");
    expect(JSON.stringify(view)).not.toContain("uvIndexMax");
  });

  it("passes no weather through as none", () => {
    expect(weatherView(undefined)).toBeUndefined();
    expect(weatherView(null)).toBeNull();
  });

  it("says a normal is the date's historical average, with the range half of such days fell in", () => {
    const view = weatherView(
      toClimateNormalSummary({
        temperatureMin: 9.2,
        temperatureMax: 16.7,
        temperatureMaxP25: 14.6,
        temperatureMaxP75: 18.9,
        temperatureMinP25: 7.1,
        temperatureMinP75: 11.4,
        wetDayShare: 0.27,
        precipitationSum: 3.3,
        rainSum: 3.3,
        snowfallSum: 0,
        windSpeedMax: 14,
        weatherCode: 3,
      }),
    );
    expect(view).toMatchObject({
      basis: "climate_normal",
      note: "historical average for this calendar date (2015-2024), not a forecast for the day",
      typicalHighC: "15-19",
      typicalLowC: "7-11",
      rainDayChancePercent: 27,
    });
  });

  it("says no historical note on a forecast", () => {
    const view = weatherView(
      toWeatherSummary({ temperatureMin: 7, temperatureMax: 9.8 }),
    );
    expect(view).not.toHaveProperty("note");
    expect(view).not.toHaveProperty("typicalHighC");
  });
});
