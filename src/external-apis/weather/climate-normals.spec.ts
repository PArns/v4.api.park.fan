import {
  ArchiveDaily,
  CLIMATE_NORMAL_PERIOD,
  buildClimateNormals,
  hasClimateSpread,
  monthDayKey,
  quantile,
} from "./climate-normals";

/** One `value` per day for every year in `years`, for the given MM-DD list. */
function series(
  years: number[],
  monthDays: string[],
  value: (year: number, md: string) => number | null,
): { time: string[]; values: (number | null)[] } {
  const time: string[] = [];
  const values: (number | null)[] = [];
  for (const y of years) {
    for (const md of monthDays) {
      time.push(`${y}-${md}`);
      values.push(value(y, md));
    }
  }
  return { time, values };
}

describe("buildClimateNormals", () => {
  const years = Array.from({ length: 10 }, (_, i) => 2015 + i);

  it("names the period it averages over", () => {
    expect(CLIMATE_NORMAL_PERIOD).toBe("2015-2024");
  });

  it("averages a calendar day across the years", () => {
    const { time, values } = series(years, ["07-11"], (y) => 20 + (y - 2015));
    const normals = buildClimateNormals({
      time,
      temperature_2m_max: values,
      temperature_2m_min: values.map((v) => (v === null ? null : v - 10)),
    });
    // mean of 20..29 = 24.5
    expect(normals["07-11"]).toMatchObject({
      temperatureMax: 24.5,
      temperatureMin: 14.5,
    });
  });

  it("smooths over ±3 days so one freak day is not the normal", () => {
    const days = [
      "07-08",
      "07-09",
      "07-10",
      "07-11",
      "07-12",
      "07-13",
      "07-14",
    ];
    const { time, values } = series([2020], days, (_y, md) =>
      md === "07-11" ? 40 : 20,
    );
    const normals = buildClimateNormals({
      time,
      temperature_2m_max: values,
    });
    // (6 × 20 + 40) / 7 = 22.857…
    expect(normals["07-11"].temperatureMax).toBe(22.9);
  });

  it("ignores years outside the named period", () => {
    const { time, values } = series([2010, 2020, 2030], ["07-11"], (y) =>
      y === 2020 ? 20 : 99,
    );
    const normals = buildClimateNormals({ time, temperature_2m_max: values });
    expect(normals["07-11"].temperatureMax).toBe(20);
  });

  it("carries 02-29 and wraps the window across the new year", () => {
    const { time, values } = series(
      [2016, 2020],
      ["01-01", "12-31", "02-29"],
      () => 5,
    );
    const normals = buildClimateNormals({ time, temperature_2m_max: values });
    expect(normals["02-29"].temperatureMax).toBe(5);
    expect(normals["01-01"].temperatureMax).toBe(5);
    expect(normals["12-31"].temperatureMax).toBe(5);
  });

  it("leaves a day out instead of storing nulls when nothing was measured", () => {
    const { time, values } = series(years, ["07-11"], () => null);
    const normals = buildClimateNormals({ time, temperature_2m_max: values });
    expect(normals["07-11"]).toBeUndefined();
    expect(Object.keys(normals)).toHaveLength(0);
  });

  it("keeps a real 0 °C", () => {
    const { time, values } = series(years, ["01-15"], () => 0);
    const normals = buildClimateNormals({ time, temperature_2m_max: values });
    expect(normals["01-15"].temperatureMax).toBe(0);
  });

  it("takes the most frequent weather code, the lower on a tie", () => {
    const { time, values } = series([2015, 2016, 2017, 2018], ["07-11"], (y) =>
      y <= 2016 ? 61 : 3,
    );
    const temps = values.map(() => 20);
    const normals = buildClimateNormals({
      time,
      temperature_2m_max: temps,
      weathercode: values,
    } as ArchiveDaily);
    expect(normals["07-11"].weatherCode).toBe(3);
  });

  it("gives the range half of the days fell in: the 25th to 75th percentile", () => {
    // Highs 20..29 over ten years: the quartiles interpolate between ranks.
    const { time, values } = series(years, ["07-11"], (y) => 20 + (y - 2015));
    const normals = buildClimateNormals({
      time,
      temperature_2m_max: values,
      temperature_2m_min: values.map((v) => (v === null ? null : v - 10)),
    });
    expect(normals["07-11"]).toMatchObject({
      temperatureMax: 24.5,
      temperatureMaxP25: 22.3,
      temperatureMaxP75: 26.8,
      temperatureMinP25: 12.3,
      temperatureMinP75: 16.8,
    });
  });

  it("counts a rain day from 1 mm, so drizzle is not one", () => {
    // Four years dry, three with 0.4 mm of drizzle, three with real rain.
    const { time, values } = series(years, ["07-11"], (y) =>
      y < 2019 ? 0 : y < 2022 ? 0.4 : 6,
    );
    const normals = buildClimateNormals({
      time,
      temperature_2m_max: values.map(() => 25),
      precipitation_sum: values,
    });
    expect(normals["07-11"].wetDayShare).toBe(0.3);
    expect(normals["07-11"].precipitationSum).toBe(1.9);
  });

  it("says whether cached normals carry the spread", () => {
    const { time, values } = series(years, ["07-11"], () => 20);
    const fresh = buildClimateNormals({ time, temperature_2m_max: values });
    expect(hasClimateSpread(fresh)).toBe(true);
    expect(
      hasClimateSpread({
        "07-11": {
          temperatureMax: 20,
          temperatureMin: 10,
          precipitationSum: 1,
          rainSum: 1,
          snowfallSum: 0,
          windSpeedMax: 10,
          weatherCode: 3,
        },
      }),
    ).toBe(false);
  });

  it("interpolates a quantile and keeps an empty set empty", () => {
    expect(quantile([1, 2, 3, 4, 5], 0.25)).toBe(2);
    expect(quantile([null, 4, undefined], 0.75)).toBe(4);
    expect(quantile([null], 0.5)).toBeNull();
  });

  it("maps a date string to its MM-DD key", () => {
    expect(monthDayKey("2027-07-11")).toBe("07-11");
  });
});
