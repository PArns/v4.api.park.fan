import { toDailyWeather } from "./open-meteo.client";

/**
 * The sixteenth day of a 16-day forecast.
 *
 * Open-Meteo lists every date the request asked for in `daily.time`, but its
 * value arrays run out at the model's horizon. Mapped by index that produced a
 * `weather_data` row with every column null — and the calendar downstream read
 * those nulls as 0, putting "0°–0°" on 22 parks' tiles on 2026-08-28.
 */
describe("toDailyWeather", () => {
  it("keeps the days the model actually forecast", () => {
    const days = toDailyWeather({
      time: ["2026-09-10", "2026-09-11"],
      temperature_2m_max: [22, 20.4],
      temperature_2m_min: [15.7, 14],
      weathercode: [51, 53],
    });

    expect(days).toHaveLength(2);
    expect(days[0]).toMatchObject({ date: "2026-09-10", temperatureMax: 22 });
  });

  it("drops the trailing day the arrays have no numbers for", () => {
    const days = toDailyWeather({
      time: ["2026-09-11", "2026-09-12"],
      temperature_2m_max: [20.4, null],
      temperature_2m_min: [14, null],
      weathercode: [53, null],
    });

    expect(days.map((day) => day.date)).toEqual(["2026-09-11"]);
  });

  it("drops a day whose arrays simply end short", () => {
    const days = toDailyWeather({
      time: ["2026-09-11", "2026-09-12"],
      temperature_2m_max: [20.4],
      temperature_2m_min: [14],
    });

    expect(days.map((day) => day.date)).toEqual(["2026-09-11"]);
  });

  it("keeps a day that has a temperature but nothing else", () => {
    // Half a reading is still a reading; only "no temperature at all" is not.
    const days = toDailyWeather({
      time: ["2026-09-12"],
      temperature_2m_max: [18],
      temperature_2m_min: [null],
    });

    expect(days).toHaveLength(1);
    expect(days[0]!.temperatureMin).toBeNull();
  });

  it("keeps a genuine 0 °C day", () => {
    const days = toDailyWeather({
      time: ["2026-12-24"],
      temperature_2m_max: [0],
      temperature_2m_min: [0],
      weathercode: [71],
    });

    expect(days).toHaveLength(1);
  });
});
