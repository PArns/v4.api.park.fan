import {
  ArchiveDaily,
  CLIMATE_NORMAL_PERIOD,
  buildClimateNormals,
  monthDayKey,
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

  it("maps a date string to its MM-DD key", () => {
    expect(monthDayKey("2027-07-11")).toBe("07-11");
  });
});
