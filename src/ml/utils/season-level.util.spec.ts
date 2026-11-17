import type { HolidayRow } from "../../holidays/holidays.service";
import { eachIsoDate } from "../../common/utils/date.util";
import {
  applySeasonLevel,
  seasonDaysOff,
  seasonOn,
  type RunningSeason,
  type SeasonReference,
} from "./season-level.util";

describe("season level (PAR-1343)", () => {
  const wintertraum: RunningSeason = {
    id: "w",
    startDate: "2026-11-14",
    endDate: "2027-01-24",
    // Closed on 24 and 25 December: a listed season counts its own days only.
    dates: ["2026-11-28", "2026-12-01", "2026-12-23", "2026-12-26"],
  };
  const range: RunningSeason = {
    id: "h",
    startDate: "2026-10-03",
    endDate: "2026-11-01",
    dates: null,
  };
  const phantasialand = {
    countryCode: "DE",
    // The geocoder's spelling; the holiday rows say "DE-NW".
    regionCode: "NRW",
    timezone: "Europe/Berlin",
  };
  const holiday = (
    date: string,
    overrides: Partial<HolidayRow> = {},
  ): HolidayRow => ({
    date,
    name: "Christmas Holidays",
    localName: "Weihnachtsferien",
    country: "DE",
    region: "DE-NW",
    holidayType: "school",
    isNationwide: false,
    ...overrides,
  });
  const christmasBreak = eachIsoDate("2026-12-23", "2027-01-06").map((d) =>
    holiday(d),
  );
  // Saturday 28 November, and Wednesday 23 December, the first day of the
  // break; Tuesday 1 December is a working day.
  const daysOff = seasonDaysOff(
    wintertraum.dates!,
    christmasBreak,
    phantasialand,
  );

  const row = (attractionId: string, day: string, wait: number) => ({
    attractionId,
    predictedTime: `${day}T12:00:00`,
    predictedWaitTime: wait,
    modelVersion: "tft",
    crowdLevel: "very_low" as string,
  });
  const dayOf = (p: { predictedTime: string }) => p.predictedTime.slice(0, 10);
  const ref = (
    dayOff: boolean,
    level: number,
    days = 3,
    attractionId = "taron",
  ): SeasonReference => ({
    seasonId: "w",
    attractionId,
    dayOff,
    days,
    level,
  });

  it("reads a listed season by its days and a plain one by its range", () => {
    expect(seasonOn([wintertraum], "2026-12-23")?.id).toBe("w");
    expect(seasonOn([wintertraum], "2026-12-24")).toBeUndefined();
    expect(seasonOn([range], "2026-10-13")?.id).toBe("h");
    expect(seasonOn([range], "2026-11-02")).toBeUndefined();
  });

  describe("seasonDaysOff", () => {
    it("counts the weekend and a school-break weekday as days off", () => {
      expect(daysOff).toEqual(["2026-11-28", "2026-12-23", "2026-12-26"]);
    });

    it("counts a public holiday and the bridge day after it", () => {
      const ascension = holiday("2026-05-14", {
        name: "Ascension Day",
        localName: "Christi Himmelfahrt",
        region: null,
        holidayType: "public",
        isNationwide: true,
      });
      expect(
        seasonDaysOff(
          ["2026-05-13", "2026-05-14", "2026-05-15"],
          [ascension],
          phantasialand,
        ),
      ).toEqual(["2026-05-14", "2026-05-15"]);
    });

    it("leaves a neighbouring region's break a working day", () => {
      const lowerSaxony = holiday("2026-12-01", { region: "DE-NI" });
      expect(
        seasonDaysOff(["2026-12-01"], [lowerSaxony], phantasialand),
      ).toEqual([]);
    });

    it("reads the country's own weekend", () => {
      const riyadh = {
        countryCode: "SA",
        regionCode: null,
        timezone: "Asia/Riyadh",
      };
      // Friday and Saturday off, Sunday a working day.
      expect(
        seasonDaysOff(["2026-11-27", "2026-11-28", "2026-11-29"], [], riyadh),
      ).toEqual(["2026-11-27", "2026-11-28"]);
    });
  });

  it("serves what the ride measured in the season, by day type", () => {
    const out = applySeasonLevel(
      [
        row("taron", "2026-11-28", 15),
        row("taron", "2026-12-01", 10),
        row("taron", "2026-12-23", 10),
      ],
      [wintertraum],
      [ref(true, 62), ref(false, 33)],
      daysOff,
      dayOf,
    );
    // Half-up to 5, like every served wait. The Wednesday of the break is
    // served the days-off level, not the working days'.
    expect(out.map((p) => p.predictedWaitTime)).toEqual([60, 35, 60]);
    expect(out[0].modelVersion).toBe("tft+season");
    expect(out[0].crowdLevel).toBe("unknown");
  });

  it("never serves one day type what the other measured", () => {
    // The opening weekend and a Monday: three days in all, none of them
    // enough for its own day type.
    const rows = [
      row("taron", "2026-11-28", 15),
      row("taron", "2026-12-01", 10),
    ];
    expect(
      applySeasonLevel(
        rows,
        [wintertraum],
        [ref(true, 55, 2), ref(false, 25, 1)],
        daysOff,
        dayOf,
      ),
    ).toEqual(rows);

    // A full working-day reference leaves the Saturday to the model.
    const out = applySeasonLevel(
      rows,
      [wintertraum],
      [ref(true, 55, 2), ref(false, 25)],
      daysOff,
      dayOf,
    );
    expect(out.map((p) => p.predictedWaitTime)).toEqual([15, 25]);
  });

  it("leaves the model's number where nothing was measured yet", () => {
    const rows = [
      row("taron", "2026-11-28", 15), // two days only
      row("fly", "2026-11-28", 20), // no reference for this ride
      row("taron", "2026-12-24", 15), // not a day of the season
    ];
    const out = applySeasonLevel(
      rows,
      [wintertraum],
      [ref(true, 62, 2), ref(false, 41)],
      daysOff,
      dayOf,
    );
    expect(out).toEqual(rows);
  });

  it("returns the list untouched without a running season", () => {
    const rows = [row("taron", "2026-11-28", 15)];
    expect(applySeasonLevel(rows, [], [ref(false, 41)], [], dayOf)).toBe(rows);
  });
});
