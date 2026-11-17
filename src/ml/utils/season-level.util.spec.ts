import type { HolidayRow } from "../../holidays/holidays.service";
import { eachIsoDate } from "../../common/utils/date.util";
import {
  applySeasonLevel,
  seasonDayTypes,
  seasonOn,
  seasonReferences,
  type MeasuredSeasonDay,
  type RunningSeason,
  type SeasonDayType,
  type SeasonReference,
} from "./season-level.util";

describe("season level (PAR-1343)", () => {
  const wintertraum: RunningSeason = {
    id: "w",
    startDate: "2026-11-14",
    endDate: "2027-01-24",
    // Closed on 24 and 25 December: a listed season counts its own days only.
    dates: [
      "2026-11-27",
      "2026-11-28",
      "2026-12-01",
      "2026-12-22",
      "2026-12-23",
      "2026-12-26",
    ],
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
    influencingRegions: [
      { countryCode: "DE", regionCode: "DE-NI" },
      { countryCode: "NL", regionCode: "NL-LI" },
      { countryCode: "BE", regionCode: null },
    ],
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
  // North Rhine-Westphalia's school breaks are stored into 2027's summer.
  const nrwThrough = "2027-07-20";
  // Friday 27 November, an eve; Saturday 28 November, a day off; Tuesday
  // 1 December, a working day; Tuesday 22 December, the eve of the break;
  // Wednesday 23 December, its first day; Saturday 26 December.
  const dayTypes = seasonDayTypes(
    wintertraum.dates!,
    christmasBreak,
    phantasialand,
    nrwThrough,
  );

  const row = (
    attractionId: string,
    day: string,
    wait: number,
    modelVersion = "tft",
  ) => ({
    attractionId,
    predictedTime: `${day}T12:00:00`,
    predictedWaitTime: wait,
    modelVersion,
    crowdLevel: "very_low" as string,
  });
  const dayOf = (p: { predictedTime: string }) => p.predictedTime.slice(0, 10);
  const ref = (
    dayType: SeasonDayType,
    level: number,
    days = 3,
    attractionId = "taron",
    ratio: number | null = null,
    ratioDays = ratio === null ? 0 : days,
  ): SeasonReference => ({
    seasonId: "w",
    attractionId,
    dayType,
    days,
    level,
    ratioDays,
    ratio,
  });

  it("reads a listed season by its days and a plain one by its range", () => {
    expect(seasonOn([wintertraum], "2026-12-23")?.id).toBe("w");
    expect(seasonOn([wintertraum], "2026-12-24")).toBeUndefined();
    expect(seasonOn([range], "2026-10-13")?.id).toBe("h");
    expect(seasonOn([range], "2026-11-02")).toBeUndefined();
  });

  describe("seasonDayTypes", () => {
    it("types the weekend, a break, and the working day before each", () => {
      expect(dayTypes).toEqual({
        daysOff: ["2026-11-28", "2026-12-23", "2026-12-26"],
        eves: ["2026-11-27", "2026-12-22"],
        unknownDays: [],
      });
    });

    it("counts a public holiday and the bridge day after it, with its eve", () => {
      const ascension = holiday("2026-05-14", {
        name: "Ascension Day",
        localName: "Christi Himmelfahrt",
        region: null,
        holidayType: "public",
        isNationwide: true,
      });
      expect(
        seasonDayTypes(
          ["2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15"],
          [ascension],
          phantasialand,
          nrwThrough,
        ),
      ).toEqual({
        daysOff: ["2026-05-14", "2026-05-15"],
        eves: ["2026-05-13"],
        unknownDays: [],
      });
    });

    it("makes Thursday the eve where the weekend is Friday and Saturday", () => {
      const riyadh = {
        countryCode: "SA",
        regionCode: null,
        timezone: "Asia/Riyadh",
      };
      expect(
        seasonDayTypes(
          eachIsoDate("2026-11-25", "2026-11-29"),
          [],
          riyadh,
          "2027-06-30",
        ),
      ).toEqual({
        daysOff: ["2026-11-27", "2026-11-28"],
        eves: ["2026-11-26"],
        unknownDays: [],
      });
    });

    it("counts a break or holiday in an influencing region as a day off", () => {
      const days = [
        "2026-04-27", // Koningsdag, stored for NL without a region
        "2026-11-04", // a Walloon school-break day; BE counts as a whole
        "2026-11-30",
        "2026-12-01", // Lower Saxony's break
        "2026-12-03", // a North Holland break: not an influencing region
        "2026-12-08", // a Bavarian break: not one either
      ];
      const rows = [
        holiday("2026-04-27", {
          name: "King's Day",
          country: "NL",
          region: null,
          holidayType: "public",
          isNationwide: true,
        }),
        holiday("2026-11-04", { country: "BE", region: "BE-WAL" }),
        holiday("2026-12-01", { region: "DE-NI" }),
        holiday("2026-12-03", { country: "NL", region: "NL-NH" }),
        holiday("2026-12-08", { region: "DE-BY" }),
      ];

      expect(seasonDayTypes(days, rows, phantasialand, nrwThrough)).toEqual({
        daysOff: ["2026-04-27", "2026-11-04", "2026-12-01"],
        // Monday 30 November is the eve of Lower Saxony's break day.
        eves: ["2026-11-30"],
        unknownDays: [],
      });
      // Without influencing regions every one of them is a working day.
      expect(
        seasonDayTypes(
          days,
          rows,
          { ...phantasialand, influencingRegions: null },
          nrwThrough,
        ),
      ).toEqual({ daysOff: [], eves: [], unknownDays: [] });
    });

    it("leaves an observance in an influencing region a working day", () => {
      const observance = holiday("2026-12-01", {
        country: "BE",
        region: null,
        holidayType: "observance",
        isNationwide: true,
      });
      expect(
        seasonDayTypes(["2026-12-01"], [observance], phantasialand, nrwThrough)
          .daysOff,
      ).toEqual([]);
    });

    it("reads no school break where the country stores none", () => {
      const riyadh = {
        countryCode: "SA",
        regionCode: null,
        timezone: "Asia/Riyadh",
      };
      // Friday and Saturday off; Thursday and Sunday could be break days.
      expect(
        seasonDayTypes(
          ["2026-11-26", "2026-11-27", "2026-11-28", "2026-11-29"],
          [],
          riyadh,
          null,
        ),
      ).toEqual({
        daysOff: ["2026-11-27", "2026-11-28"],
        eves: [],
        unknownDays: ["2026-11-26", "2026-11-29"],
      });
    });

    it("leaves a weekday past the school data unknown, and the day before it", () => {
      expect(
        seasonDayTypes(
          [
            "2027-07-19",
            "2027-07-20",
            "2027-07-21",
            "2027-08-06",
            "2027-08-07",
          ],
          [],
          phantasialand,
          nrwThrough,
        ),
      ).toEqual({
        daysOff: ["2027-08-07"],
        eves: [],
        // Tuesday 20 July is the last stored day, but Wednesday is not known,
        // so neither is whether Tuesday is an eve. Friday 6 August is past
        // the data itself.
        unknownDays: ["2027-07-20", "2027-07-21", "2027-08-06"],
      });
    });
  });

  describe("seasonReferences", () => {
    // `seasonReferences` reads the types it is handed: every day these do
    // not list counts as a working day, the calendar's weekends included.
    const working = eachIsoDate("2026-11-06", "2026-11-19");
    const types = {
      daysOff: ["2026-11-21", "2026-11-22"],
      eves: ["2026-11-20"],
      unknownDays: [] as string[],
    };
    const measured = (
      day: string,
      peak: number,
      forecast: number | null = null,
      attractionId = "taron",
    ): MeasuredSeasonDay => ({
      seasonId: "w",
      attractionId,
      day,
      peak,
      forecast,
    });

    it("takes the median of the latest six days of the type", () => {
      // Eight working days: four at 10, then four at 40. All eight would
      // give 25; the latest six give 40.
      const days = working.slice(0, 8);
      expect(days).toHaveLength(8);
      const [reference] = seasonReferences(
        days.map((d, i) => measured(d, i < 4 ? 10 : 40)),
        types,
      );
      expect(reference).toMatchObject({
        dayType: "working",
        days: 6,
        level: 40,
        ratioDays: 0,
        ratio: null,
      });
    });

    it("keeps each day type, ride and season apart and drops unknown days", () => {
      const refs = seasonReferences(
        [
          measured("2026-11-20", 50),
          measured("2026-11-21", 60),
          measured("2026-11-22", 70),
          measured("2026-11-23", 30),
          measured("2026-11-23", 12, null, "fly"),
          { ...measured("2026-11-23", 99), seasonId: "h" },
          measured("2026-11-24", 99),
        ],
        { ...types, unknownDays: ["2026-11-24"] },
      );
      const find = (dayType: SeasonDayType, attractionId = "taron", s = "w") =>
        refs.find(
          (r) =>
            r.dayType === dayType &&
            r.attractionId === attractionId &&
            r.seasonId === s,
        );
      expect(refs).toHaveLength(5);
      expect(find("eve")).toMatchObject({ days: 1, level: 50 });
      expect(find("off")).toMatchObject({ days: 2, level: 65 });
      expect(find("working")).toMatchObject({ days: 1, level: 30 });
      expect(find("working", "fly")).toMatchObject({ days: 1, level: 12 });
      expect(find("working", "taron", "h")).toMatchObject({ level: 99 });
    });

    it("divides by TFT's forecast, passing over a day without a usable one", () => {
      // Newest first: no forecast, a forecast under 5, then seven usable
      // days. The latest six of those are 2, 2, 2, 3, 3, 3; the seventh (9)
      // is past the window.
      const days = working.slice(0, 9).reverse();
      const forecasts: Array<[number, number | null]> = [
        [60, null],
        [60, 4],
        [40, 20],
        [40, 20],
        [40, 20],
        [30, 10],
        [30, 10],
        [30, 10],
        [90, 10],
      ];
      const [reference] = seasonReferences(
        days.map((d, i) => measured(d, forecasts[i][0], forecasts[i][1])),
        types,
      );
      expect(reference.ratioDays).toBe(6);
      expect(reference.ratio).toBeCloseTo(2.5);
      // The level reads the latest six measured days, forecast or not.
      expect(reference).toMatchObject({ days: 6, level: 40 });
    });
  });

  describe("applySeasonLevel", () => {
    it("serves the level of each day's own type in level mode", () => {
      const out = applySeasonLevel(
        [
          row("taron", "2026-11-27", 10),
          row("taron", "2026-11-28", 15),
          row("taron", "2026-12-01", 10),
          row("taron", "2026-12-22", 10),
          row("taron", "2026-12-23", 10),
        ],
        [wintertraum],
        [ref("off", 62, 3, "taron", 3), ref("eve", 48), ref("working", 33)],
        dayTypes,
        dayOf,
        "level",
      );
      // Half-up to 5, like every served wait. The Wednesday of the break is
      // served the days-off level, the Tuesday before it the eves'.
      expect(out.map((p) => p.predictedWaitTime)).toEqual([50, 60, 35, 50, 60]);
      expect(out[0].modelVersion).toBe("tft+season");
      expect(out[0].crowdLevel).toBe("unknown");
    });

    it("scales a TFT row by the ratio and serves CatBoost the level in ratio mode", () => {
      const out = applySeasonLevel(
        [
          row("taron", "2026-11-28", 15),
          row("taron", "2026-12-26", 25),
          row("taron", "2026-12-23", 20, "catboost-v1"),
          row("fly", "2026-11-28", 15),
        ],
        [wintertraum],
        [
          ref("off", 62, 4, "taron", 2.4),
          // Measured, but only two days with a forecast.
          ref("off", 41, 4, "fly", 3, 2),
        ],
        dayTypes,
        dayOf,
        "ratio",
      );
      expect(out.map((p) => p.predictedWaitTime)).toEqual([
        35, // 15 × 2.4 = 36
        60, // 25 × 2.4 = 60: TFT's own day-to-day shape survives
        60, // CatBoost's number was never measured against: the level
        40, // the ratio is short of days: the level
      ]);
      expect(out.map((p) => p.modelVersion)).toEqual([
        "tft+season",
        "tft+season",
        "catboost-v1+season",
        "tft+season",
      ]);
    });

    it("never serves one day type what another measured", () => {
      // The opening weekend and a Tuesday: three days in all, none of them
      // enough for its own day type.
      const rows = [
        row("taron", "2026-11-28", 15),
        row("taron", "2026-12-01", 10),
      ];
      expect(
        applySeasonLevel(
          rows,
          [wintertraum],
          [ref("off", 55, 2), ref("working", 25, 1)],
          dayTypes,
          dayOf,
          "ratio",
        ),
      ).toEqual(rows);

      // A full working-day reference leaves the Saturday and the eve to the
      // model.
      const out = applySeasonLevel(
        [...rows, row("taron", "2026-11-27", 10)],
        [wintertraum],
        [ref("off", 55, 2), ref("working", 25)],
        dayTypes,
        dayOf,
        "level",
      );
      expect(out.map((p) => p.predictedWaitTime)).toEqual([15, 25, 10]);
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
        [ref("off", 62, 2), ref("working", 41)],
        dayTypes,
        dayOf,
        "ratio",
      );
      expect(out).toEqual(rows);
    });

    it("returns the list untouched without a running season", () => {
      const rows = [row("taron", "2026-11-28", 15)];
      expect(
        applySeasonLevel(
          rows,
          [],
          [ref("working", 41)],
          { daysOff: [], eves: [], unknownDays: [] },
          dayOf,
          "ratio",
        ),
      ).toBe(rows);
    });

    it("serves an unknown day the model's number whatever was measured", () => {
      const rows = [row("taron", "2026-12-01", 10)];
      expect(
        applySeasonLevel(
          rows,
          [wintertraum],
          [ref("off", 62), ref("working", 33, 3, "taron", 2)],
          { daysOff: [], eves: [], unknownDays: ["2026-12-01"] },
          dayOf,
          "ratio",
        ),
      ).toEqual(rows);
    });
  });
});
