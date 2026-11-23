import type { ClimateNormalDay } from "../external-apis/weather/climate-normals";
import { JAPAN, ORLANDO } from "./regions";
import {
  climateOfDates,
  climateOfNormals,
  climateParkOfDay,
} from "./trip-climate";

const day = (
  high: number,
  over: Partial<ClimateNormalDay> = {},
): ClimateNormalDay => ({
  temperatureMax: high,
  temperatureMin: high - 8,
  temperatureMaxP25: high - 1.5,
  temperatureMaxP75: high + 1.5,
  temperatureMinP25: high - 9.5,
  temperatureMinP75: high - 6.5,
  wetDayShare: 0.3,
  precipitationSum: 2.4,
  rainSum: 2.4,
  snowfallSum: 0,
  windSpeedMax: 12,
  weatherCode: 3,
  ...over,
});

describe("a span's typical weather", () => {
  it("gives a day its means, the range half of such days fell in, and the chance of rain", () => {
    expect(climateOfNormals([day(17)], 1)).toEqual({
      tempMax: 17,
      tempMin: 9,
      tempMaxP25: 15.5,
      tempMaxP75: 18.5,
      tempMinP25: 7.5,
      tempMinP75: 10.5,
      precipitationMm: 2.4,
      wetDayPercent: 30,
      basis: "climate_normal",
      period: "2015-2024",
    });
  });

  it("means a window's days and totals its rain", () => {
    const normals = {
      "10-01": day(20),
      "10-02": day(19),
      "10-03": day(18, { wetDayShare: 0.6, precipitationSum: 6 }),
    };
    expect(
      climateOfDates(normals, ["2027-10-01", "2027-10-02", "2027-10-03"]),
    ).toMatchObject({
      tempMax: 19,
      tempMaxP25: 17.5,
      tempMaxP75: 20.5,
      precipitationMm: 11,
      wetDayPercent: 40,
    });
  });

  it("leaves the range out for normals cached before the spread, never a guess", () => {
    const old = day(17, {
      temperatureMaxP25: undefined,
      temperatureMaxP75: undefined,
      temperatureMinP25: undefined,
      temperatureMinP75: undefined,
      wetDayShare: undefined,
    });
    const climate = climateOfNormals([old], 1);
    expect(climate).toMatchObject({ tempMax: 17, tempMin: 9 });
    expect(climate).not.toHaveProperty("tempMaxP25");
    expect(climate).not.toHaveProperty("wetDayPercent");
  });

  it("says nothing for a span it has no normal for on some day", () => {
    expect(
      climateOfDates({ "10-01": day(20) }, ["2027-10-01", "2027-10-02"]),
    ).toBeUndefined();
    expect(climateOfDates(null, ["2027-10-01"])).toBeUndefined();
    expect(
      climateOfNormals([day(20, { temperatureMin: null })], 1),
    ).toBeUndefined();
  });
});

describe("where a plan day's weather is read", () => {
  it("reads a park day at its park, and the first park of a hop day", () => {
    expect(
      climateParkOfDay(ORLANDO, {
        parks: [{ slug: "epcot" }, { slug: "magic-kingdom-park" }],
      }),
    ).toBe("epcot");
  });

  it("reads a day without a park at its base's nearest park, else the region's climate park", () => {
    expect(climateParkOfDay(JAPAN, { parks: [], base: "osaka" })).toBe(
      "universal-studios-japan",
    );
    expect(climateParkOfDay(JAPAN, { parks: [] })).toBe(
      JAPAN.weather.climatePark,
    );
    expect(climateParkOfDay(ORLANDO, { parks: [] })).toBe("magic-kingdom-park");
  });
});
