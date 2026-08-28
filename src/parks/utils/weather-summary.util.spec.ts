import { toWeatherSummary } from "./weather-summary.util";

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
