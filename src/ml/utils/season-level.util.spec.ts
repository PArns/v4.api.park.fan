import type { HolidayRow } from "../../holidays/holidays.service";
import { eachIsoDate } from "../../common/utils/date.util";
import { buildSchoolCoverage } from "../../common/utils/holiday.utils";
import {
  applySeasonLevel,
  isSeasonRatioRow,
  seasonDayTypes,
  seasonOn,
  seasonReferences,
  seasonSchoolCoverage,
  servedLevelSource,
  SEASON_RATIO_MAX,
  SEASON_RATIO_MIN,
  type MeasuredSeasonDay,
  type RunningSeason,
  type SeasonDayType,
  type SeasonDayTypes,
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
      "2026-12-02",
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
  // North Rhine-Westphalia's school breaks, and the neighbours', are stored
  // into 2027's summer.
  const nrwThrough = "2027-07-20";
  const covered = { park: nrwThrough, neighbours: nrwThrough };
  // Friday 27 November, an eve; Saturday 28 November, a day off; Tuesday
  // 1 December, a working day; Wednesday 2 December, a Lower Saxon break day;
  // Tuesday 22 December, the eve of the break; Wednesday 23 December, its
  // first day; Saturday 26 December.
  const dayTypes = seasonDayTypes(
    wintertraum.dates!,
    [...christmasBreak, holiday("2026-12-02", { region: "DE-NI" })],
    phantasialand,
    covered,
  );
  const noTypes: SeasonDayTypes = {
    daysOff: [],
    neighbourDays: [],
    eves: [],
    unknownDays: [],
  };

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
    confidence: 80 as number | null,
    uncertaintyMinutes: 12 as number | null,
  });
  const dayOf = (p: { predictedTime: string }) => p.predictedTime.slice(0, 10);
  /** A reference with three days, no ratio, and its level as its highest P90. */
  const ref = (
    dayType: SeasonDayType,
    level: number,
    overrides: Partial<SeasonReference> = {},
  ): SeasonReference => ({
    seasonId: "w",
    attractionId: "taron",
    dayType,
    days: 3,
    level,
    maxPeak: level,
    ratioDays: overrides.ratio != null ? (overrides.days ?? 3) : 0,
    ratio: null,
    ...overrides,
  });

  it("reads a listed season by its days and a plain one by its range", () => {
    expect(seasonOn([wintertraum], "2026-12-23")?.id).toBe("w");
    expect(seasonOn([wintertraum], "2026-12-24")).toBeUndefined();
    expect(seasonOn([range], "2026-10-13")?.id).toBe("h");
    expect(seasonOn([range], "2026-11-02")).toBeUndefined();
  });

  describe("servedLevelSource", () => {
    it("files a ratio row and a level row under season, and tells them apart", () => {
      expect(servedLevelSource("tft+season-ratio")).toBe("season");
      expect(servedLevelSource("tft+season")).toBe("season");
      expect(servedLevelSource("catboost-v1+season")).toBe("season");
      expect(servedLevelSource("tft")).toBe("tft");
      expect(servedLevelSource("catboost-v1")).toBe("catboost");
      expect(servedLevelSource(undefined)).toBe("catboost");
      expect(isSeasonRatioRow("tft+season-ratio")).toBe(true);
      expect(isSeasonRatioRow("tft+season")).toBe(false);
      expect(isSeasonRatioRow("tft")).toBe(false);
      expect(isSeasonRatioRow(null)).toBe(false);
    });
  });

  describe("seasonDayTypes", () => {
    it("types the weekend, a break, a neighbour's break and the eve of each own day off", () => {
      // Tuesday 1 December is no eve: the day after is the neighbours'.
      expect(dayTypes).toEqual({
        daysOff: ["2026-11-28", "2026-12-23", "2026-12-26"],
        neighbourDays: ["2026-12-02"],
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
          covered,
        ),
      ).toEqual({
        ...noTypes,
        daysOff: ["2026-05-14", "2026-05-15"],
        eves: ["2026-05-13"],
      });
    });

    it("makes Thursday the eve where the weekend is Friday and Saturday", () => {
      const riyadh = {
        countryCode: "SA",
        regionCode: null,
        timezone: "Asia/Riyadh",
      };
      expect(
        seasonDayTypes(eachIsoDate("2026-11-25", "2026-11-29"), [], riyadh, {
          park: "2027-06-30",
          neighbours: null,
        }),
      ).toEqual({
        ...noTypes,
        daysOff: ["2026-11-27", "2026-11-28"],
        eves: ["2026-11-26"],
      });
    });

    it("makes a break or holiday in an influencing region a neighbour day, not a day off", () => {
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

      // Monday 30 November is a working day: an eve is the day before the
      // park's own day off, and a neighbour's break is not one.
      expect(seasonDayTypes(days, rows, phantasialand, covered)).toEqual({
        ...noTypes,
        neighbourDays: ["2026-04-27", "2026-11-04", "2026-12-01"],
      });
      // Without influencing regions every one of them is a working day.
      expect(
        seasonDayTypes(
          days,
          rows,
          { ...phantasialand, influencingRegions: null },
          covered,
        ),
      ).toEqual(noTypes);
    });

    it("puts an own day off before a neighbour day, and a neighbour day before an eve", () => {
      const belgian = (date: string) =>
        holiday(date, { country: "BE", region: "BE-VLG" });
      expect(
        seasonDayTypes(
          ["2026-11-26", "2026-11-27", "2026-11-28"],
          ["2026-11-26", "2026-11-27", "2026-11-28"].map(belgian),
          phantasialand,
          covered,
        ),
      ).toEqual({
        ...noTypes,
        // Saturday is the park's own weekend; Friday, the eve of it, and
        // Thursday are the Belgians' break.
        daysOff: ["2026-11-28"],
        neighbourDays: ["2026-11-26", "2026-11-27"],
      });
    });

    it("leaves an observance in an influencing region a working day", () => {
      const observance = holiday("2026-12-01", {
        country: "BE",
        region: null,
        holidayType: "observance",
        isNationwide: true,
      });
      expect(
        seasonDayTypes(["2026-12-01"], [observance], phantasialand, covered),
      ).toEqual(noTypes);
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
          { park: null, neighbours: null },
        ),
      ).toEqual({
        ...noTypes,
        daysOff: ["2026-11-27", "2026-11-28"],
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
          { park: nrwThrough, neighbours: null },
        ),
      ).toEqual({
        ...noTypes,
        daysOff: ["2027-08-07"],
        // Tuesday 20 July is the last stored day, but Wednesday is not known,
        // so neither is whether Tuesday is an eve. Friday 6 August is past
        // the data itself.
        unknownDays: ["2027-07-20", "2027-07-21", "2027-08-06"],
      });
    });

    it("leaves a working day past a neighbour's school data unknown", () => {
      const days = [
        "2026-12-03",
        "2026-12-04", // the neighbours' last stored day
        "2026-12-07",
        "2026-12-08", // a Belgian public holiday, stored
        "2026-12-11",
        "2026-12-12",
      ];
      const rows = [
        holiday("2026-12-08", {
          country: "BE",
          region: null,
          holidayType: "public",
          isNationwide: true,
        }),
      ];
      expect(
        seasonDayTypes(days, rows, phantasialand, {
          park: nrwThrough,
          neighbours: "2026-12-04",
        }),
      ).toEqual({
        // A neighbour's break past its data could fall on Monday or on
        // Friday, which would make them neighbour days, so neither is an eve
        // or a working day. A weekend and a stored holiday stay known.
        daysOff: ["2026-12-12"],
        neighbourDays: ["2026-12-08"],
        eves: ["2026-12-04"],
        unknownDays: ["2026-12-07", "2026-12-11"],
      });
      // With no neighbour storing any break, nothing is unknown for them.
      expect(
        seasonDayTypes(days, rows, phantasialand, {
          park: nrwThrough,
          neighbours: null,
        }).unknownDays,
      ).toEqual([]);
    });
  });

  describe("seasonSchoolCoverage", () => {
    const today = "2026-11-17";
    const coverage = buildSchoolCoverage([
      { country: "DE", region: "DE-NW", through: "2027-07-20" },
      { country: "DE", region: "DE-NI", through: "2027-07-01" },
      { country: "NL", region: "NL-LI", through: "2027-06-15" },
      { country: "BE", region: null, through: "2027-06-30" },
      // Stored as measured on 2026-10-24 (school-holiday-coverage.md).
      { country: "MX", region: null, through: "2026-07-15" },
      { country: "IE", region: null, through: "2026-04-12" },
      { country: "US", region: "US-TX", through: "2027-06-04" },
      { country: "US", region: "US-OK", through: "2027-05-21" },
    ]);

    it("reads the park's own edge and the earliest of its neighbours'", () => {
      expect(seasonSchoolCoverage(coverage, phantasialand, today)).toEqual({
        park: "2027-07-20",
        neighbours: "2027-06-15",
      });
    });

    it("leaves out a neighbour that stores no school break at all", () => {
      expect(
        seasonSchoolCoverage(
          coverage,
          {
            ...phantasialand,
            influencingRegions: [
              { countryCode: "SE", regionCode: null },
              { countryCode: "BE", regionCode: null },
            ],
          },
          today,
        ),
      ).toEqual({ park: "2027-07-20", neighbours: "2027-06-30" });
      expect(
        seasonSchoolCoverage(
          coverage,
          {
            ...phantasialand,
            influencingRegions: [{ countryCode: "SE", regionCode: null }],
          },
          today,
        ),
      ).toEqual({ park: "2027-07-20", neighbours: null });
    });

    it("leaves out a neighbour whose school data ended before the park's today, and keeps one that reaches it", () => {
      const withIreland = {
        ...phantasialand,
        influencingRegions: [
          { countryCode: "IE", regionCode: null },
          { countryCode: "NL", regionCode: "NL-LI" },
        ],
      };
      expect(seasonSchoolCoverage(coverage, withIreland, today)).toEqual({
        park: "2027-07-20",
        neighbours: "2027-06-15",
      });
      // On Ireland's last stored day its edge still bounds the others.
      expect(seasonSchoolCoverage(coverage, withIreland, "2026-04-12")).toEqual(
        { park: "2027-07-20", neighbours: "2026-04-12" },
      );
    });

    it("types a US park's working days although Mexico's school data ended in July", () => {
      // A park in Texas, two of whose influencing regions are Oklahoma and Mexico.
      const texas = {
        countryCode: "US",
        regionCode: "US-TX",
        timezone: "America/Chicago",
        influencingRegions: [
          { countryCode: "US", regionCode: "US-OK" },
          { countryCode: "MX", regionCode: null },
        ],
      };
      const edges = seasonSchoolCoverage(coverage, texas, "2026-11-20");
      expect(edges).toEqual({ park: "2027-06-04", neighbours: "2027-05-21" });

      // Holiday in the Park from Saturday 21 November.
      const types = seasonDayTypes(
        eachIsoDate("2026-11-21", "2026-12-04"),
        [],
        texas,
        edges,
      );
      expect(types).toEqual({
        daysOff: ["2026-11-21", "2026-11-22", "2026-11-28", "2026-11-29"],
        neighbourDays: [],
        eves: ["2026-11-27", "2026-12-04"],
        unknownDays: [],
      });
    });
  });

  describe("seasonReferences", () => {
    // `seasonReferences` reads the types it is handed: every day these do
    // not list counts as a working day, the calendar's weekends included.
    const working = eachIsoDate("2026-11-06", "2026-11-19");
    const types: SeasonDayTypes = {
      daysOff: ["2026-11-21", "2026-11-22"],
      neighbourDays: ["2026-11-25"],
      eves: ["2026-11-20"],
      unknownDays: [],
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
        maxPeak: 40,
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
          measured("2026-11-25", 44),
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
      expect(refs).toHaveLength(6);
      expect(find("eve")).toMatchObject({ days: 1, level: 50 });
      expect(find("off")).toMatchObject({ days: 2, level: 65, maxPeak: 70 });
      expect(find("neighbour")).toMatchObject({ days: 1, level: 44 });
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
      expect(reference).toMatchObject({ days: 6, level: 40, maxPeak: 60 });
    });

    it("counts a ratio day older than the level's six in the highest P90", () => {
      const days = working.slice(0, 7).reverse();
      const [reference] = seasonReferences(
        days.map((d, i) => (i < 6 ? measured(d, 20) : measured(d, 90, 30))),
        types,
      );
      expect(reference).toMatchObject({
        days: 6,
        level: 20,
        ratioDays: 1,
        ratio: 3,
        maxPeak: 90,
      });
    });
  });

  describe("applySeasonLevel", () => {
    it("serves the level of each day's own type in level mode, without a band", () => {
      const out = applySeasonLevel(
        [
          row("taron", "2026-11-27", 10),
          row("taron", "2026-11-28", 15),
          row("taron", "2026-12-01", 10),
          row("taron", "2026-12-02", 10),
          row("taron", "2026-12-22", 10),
          row("taron", "2026-12-23", 10),
        ],
        [wintertraum],
        [
          ref("off", 62, { ratio: 3 }),
          ref("neighbour", 41),
          ref("eve", 48),
          ref("working", 33),
        ],
        dayTypes,
        dayOf,
        "level",
      );
      // Half-up to 5, like every served wait. The Wednesday of the break is
      // served the days-off level, the Tuesday before it the eves'.
      expect(out.map((p) => p.predictedWaitTime)).toEqual([
        50, 60, 35, 40, 50, 60,
      ]);
      expect(out[0]).toMatchObject({
        modelVersion: "tft+season",
        crowdLevel: "unknown",
        uncertaintyMinutes: null,
        confidence: null,
      });
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
          ref("off", 62, { days: 4, ratio: 2.4, maxPeak: 70 }),
          // Measured, but only two days with a forecast.
          ref("off", 41, {
            days: 4,
            attractionId: "fly",
            ratio: 3,
            ratioDays: 2,
          }),
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
        "tft+season-ratio",
        "tft+season-ratio",
        "catboost-v1+season",
        "tft+season",
      ]);
      // A scaled forecast keeps none of TFT's band or confidence.
      for (const p of out) {
        expect([p.uncertaintyMinutes, p.confidence]).toEqual([null, null]);
      }
    });

    it("holds the ratio to its bounds and the product to the highest P90 measured", () => {
      const served = (wait: number, overrides: Partial<SeasonReference>) =>
        applySeasonLevel(
          [row("taron", "2026-11-28", wait)],
          [wintertraum],
          [ref("off", 50, overrides)],
          dayTypes,
          dayOf,
          "ratio",
        )[0].predictedWaitTime;
      expect([SEASON_RATIO_MIN, SEASON_RATIO_MAX]).toEqual([0.5, 3]);

      // An opening week TFT had forecast near nothing: a ratio of 5.
      expect(served(32, { ratio: 5, maxPeak: 200 })).toBe(95); // 32 × 3
      expect(served(32, { ratio: 5, maxPeak: 55 })).toBe(55); // not 160
      expect(served(40, { ratio: 0.2, maxPeak: 200 })).toBe(20); // 40 × 0.5
      expect(served(20, { ratio: 2, maxPeak: 200 })).toBe(40);
    });

    it("raises a type without a reference of its own to a quieter type's level", () => {
      const out = applySeasonLevel(
        [
          row("taron", "2026-11-27", 10), // an eve: the working days' 33
          row("taron", "2026-12-22", 40), // an eve the model puts above it
          row("taron", "2026-12-02", 20), // a neighbour day: the working days'
          row("taron", "2026-11-28", 15), // a day off: the highest available
          row("taron", "2026-12-01", 10), // a working day: its own level
        ],
        [wintertraum],
        [
          ref("working", 33),
          ref("eve", 48, { days: 2 }), // two days: no reference yet
          ref("off", 70, { days: 2 }),
        ],
        dayTypes,
        dayOf,
        "level",
      );
      expect(out.map((p) => [p.predictedWaitTime, p.modelVersion])).toEqual([
        [35, "tft+season"],
        [40, "tft"],
        [35, "tft+season"],
        [35, "tft+season"],
        [35, "tft+season"],
      ]);
      // The model's own row keeps its band.
      expect(out[1]).toMatchObject({ uncertaintyMinutes: 12, confidence: 80 });
    });

    it("floors a day off by the highest of the eves, the neighbour days and the working days", () => {
      const refs = [
        ref("working", 33),
        ref("neighbour", 41),
        ref("eve", 48, { ratio: 2 }),
      ];
      for (const mode of ["level", "ratio"] as const) {
        const [saturday] = applySeasonLevel(
          [row("taron", "2026-11-28", 15)],
          [wintertraum],
          refs,
          dayTypes,
          dayOf,
          mode,
        );
        // A floor is a measured level in either mode, never a ratio.
        expect([saturday.predictedWaitTime, saturday.modelVersion]).toEqual([
          50,
          "tft+season",
        ]);
      }
    });

    it("never floors a type with a reference of its own", () => {
      const [saturday] = applySeasonLevel(
        [row("taron", "2026-11-28", 15)],
        [wintertraum],
        [ref("off", 20), ref("working", 33)],
        dayTypes,
        dayOf,
        "level",
      );
      expect(saturday.predictedWaitTime).toBe(20);
    });

    it("never serves one day type what another measured, below three days", () => {
      // The opening weekend and a Tuesday: three days in all, none of them
      // enough for its own day type, so there is no floor either.
      const rows = [
        row("taron", "2026-11-28", 15),
        row("taron", "2026-12-01", 10),
      ];
      expect(
        applySeasonLevel(
          rows,
          [wintertraum],
          [ref("off", 55, { days: 2 }), ref("working", 25, { days: 1 })],
          dayTypes,
          dayOf,
          "ratio",
        ),
      ).toEqual(rows);
    });

    it("leaves the model's number where nothing was measured yet", () => {
      const rows = [
        row("taron", "2026-11-28", 15), // two days only
        row("fly", "2026-11-28", 20), // no reference for this ride
        row("taron", "2026-12-24", 15), // not a day of the season
        row("taron", "2026-12-01", 10), // a working day: nothing floors it
      ];
      const out = applySeasonLevel(
        rows,
        [wintertraum],
        [ref("off", 62, { days: 2 }), ref("working", 41, { days: 2 })],
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
          noTypes,
          dayOf,
          "ratio",
        ),
      ).toBe(rows);
    });

    it("serves an unknown day the model's number, floor or not", () => {
      const rows = [row("taron", "2026-12-01", 10)];
      expect(
        applySeasonLevel(
          rows,
          [wintertraum],
          [ref("off", 62), ref("working", 33, { ratio: 2 })],
          { ...noTypes, unknownDays: ["2026-12-01"] },
          dayOf,
          "ratio",
        ),
      ).toEqual(rows);
    });
  });
});
