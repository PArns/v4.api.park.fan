import type { HolidayRow } from "../holidays/holidays.service";
import { addIsoDays, eachIsoDate } from "../common/utils/date.util";
import {
  lastYearDayFacts,
  lastYearFactDays,
  lastYearReferenceDay,
  lastYearReferenceDayReading,
  lastYearReferenceMonths,
  lastYearSeasonsFrom,
  loadLastYearDayFacts,
  type LastYearReferenceOptions,
  type LastYearSeason,
} from "./last-year-reference";

const park = {
  countryCode: "DE",
  regionCode: "NRW",
  timezone: "Europe/Berlin",
};

const holiday = (date: string, name: string): HolidayRow => ({
  date,
  name,
  localName: name,
  country: "DE",
  region: null,
  holidayType: "public",
  isNationwide: true,
});

/** NRW's Easter break, a school row on every day of it. */
const schoolBreak = (from: string, to: string): HolidayRow[] =>
  eachIsoDate(from, to).map((date) => ({
    date,
    name: "Easter Holidays",
    localName: "Osterferien",
    country: "DE",
    region: "DE-NW",
    holidayType: "school",
    isNationwide: false,
  }));

const EASTER = [
  holiday("2026-04-03", "Good Friday"),
  holiday("2026-04-06", "Easter Monday"),
  holiday("2027-03-26", "Good Friday"),
  holiday("2027-03-29", "Easter Monday"),
];
const EASTER_BREAKS = [
  ...schoolBreak("2026-03-30", "2026-04-11"),
  ...schoolBreak("2027-03-22", "2027-04-03"),
];
const ASCENSION = [
  holiday("2026-05-14", "Ascension Day"),
  holiday("2027-05-06", "Ascension Day"),
];

/** The reference day of `date` with these rows, school data stored through mid-2027. */
function refer(
  date: string,
  rows: HolidayRow[],
  options: LastYearReferenceOptions = {},
  schoolThrough: string | null = "2027-07-20",
): string {
  const facts = lastYearDayFacts(
    lastYearFactDays(date, date),
    rows,
    park,
    schoolThrough,
  );
  return lastYearReferenceDay(date, facts, options);
}

describe("lastYearReferenceDay", () => {
  it("reads Easter Monday 2027 as Easter Monday 2026, not the Monday 52 weeks back", () => {
    expect(refer("2027-03-29", EASTER)).toBe("2026-04-06");
    expect(refer("2027-03-26", EASTER)).toBe("2026-04-03");
  });

  it("moves a plain Monday whose day 52 weeks back was Easter Monday by a week", () => {
    // 52 weeks before 5 April 2027 is Easter Monday 2026.
    expect(refer("2027-04-05", EASTER)).toBe("2026-03-30");
  });

  it("reads a school-break day as a break day of the same weekday last year", () => {
    // Tuesday 23 March 2027 is in the break; 24 March 2026 was not, 31 March was.
    expect(refer("2027-03-23", EASTER_BREAKS)).toBe("2026-03-31");
  });

  it("moves a plain day off last year's break, unless the break is not known yet", () => {
    // Wednesday 7 April 2027 is after the break; 8 April 2026 was in it.
    expect(refer("2027-04-07", EASTER_BREAKS)).toBe("2026-04-15");
    // Past the stored school data the day may be a break day: no move.
    expect(refer("2027-04-07", EASTER_BREAKS, {}, "2026-12-31")).toBe(
      "2026-04-08",
    );
  });

  it("reads a bridge day as last year's bridge day, the holiday as last year's", () => {
    expect(refer("2027-05-07", ASCENSION)).toBe("2026-05-15");
    expect(refer("2027-05-06", ASCENSION)).toBe("2026-05-14");
  });

  it("reads 52 weeks back without holidays, and for a day without facts", () => {
    for (const date of ["2027-03-29", "2027-07-14", "2028-02-29"]) {
      expect(refer(date, [])).toBe(addIsoDays(date, -364));
    }
    expect(lastYearReferenceDay("2027-03-29", new Map())).toBe("2026-03-30");
  });

  it("matches a holiday by any of its names when two share a day", () => {
    const rows = [
      holiday("2026-06-04", "Corpus Christi"),
      holiday("2027-05-27", "Corpus Christi"),
      holiday("2027-05-27", "Founding Day"),
    ];
    expect(refer("2027-05-27", rows)).toBe("2026-06-04");
  });

  it("never picks a day that has not been measured yet", () => {
    const options = { before: "2026-04-01" };
    expect(refer("2027-03-29", EASTER, options)).toBe("2026-03-30");
    expect(refer("2027-04-12", EASTER, options)).toBe("2026-03-30");
  });

  describe("inside a park season", () => {
    const nights = (
      status: string,
      dates: string[] | null,
    ): LastYearSeason => ({
      kind: "halloween",
      name: "Fright Nights",
      startDate: dates ? dates[0] : "2027-09-01",
      endDate: dates ? dates[dates.length - 1] : "2027-10-31",
      dates,
      status,
    });
    const lastYear = nights("confirmed", [
      "2026-09-25",
      "2026-10-02",
      "2026-10-16",
      "2026-10-23",
    ]);

    it("prefers a night of last year's edition for a night of this year's", () => {
      const seasons = [
        lastYear,
        nights("confirmed", ["2027-10-01", "2027-10-08", "2027-10-15"]),
      ];
      // Friday 9 October 2026 was no event night; 2 October was.
      expect(refer("2027-10-08", [], { seasons })).toBe("2026-10-02");
    });

    it("compares a season without its nights by last year's range", () => {
      const seasons = [lastYear, nights("announced", null)];
      expect(refer("2027-10-08", [], { seasons })).toBe("2026-10-09");
      // Before last year's first night, the range starts later.
      expect(refer("2027-09-17", [], { seasons })).toBe("2026-09-25");
    });

    it("reads nothing from a season that is only expected", () => {
      const seasons = [
        lastYear,
        nights("expected", ["2027-10-01", "2027-10-08", "2027-10-15"]),
      ];
      expect(refer("2027-10-08", [], { seasons })).toBe("2026-10-09");
    });

    it("keeps the holiday state first", () => {
      const rows = schoolBreak("2027-10-04", "2027-10-16").concat(
        schoolBreak("2026-10-05", "2026-10-10"),
      );
      const seasons = [
        lastYear,
        nights("confirmed", ["2027-10-01", "2027-10-08", "2027-10-15"]),
      ];
      // 2 and 16 October 2026 were nights but no break days; 9 October was a break day.
      expect(refer("2027-10-08", rows, { seasons })).toBe("2026-10-09");
    });
  });
});

describe("the days the park was closed or nothing was measured", () => {
  /** NRW's Christmas breaks, a school row on every day. */
  const christmasBreak = (from: string, to: string): HolidayRow[] =>
    schoolBreak(from, to).map((row) => ({
      ...row,
      name: "Christmas Holidays",
      localName: "Weihnachtsferien",
    }));
  const CHRISTMAS = [
    ...christmasBreak("2025-12-22", "2026-01-06"),
    ...christmasBreak("2026-12-23", "2027-01-06"),
    holiday("2025-12-25", "Christmas Day"),
    holiday("2025-12-26", "St. Stephen's Day"),
    holiday("2026-01-01", "New Year's Day"),
    holiday("2026-12-25", "Christmas Day"),
    holiday("2026-12-26", "St. Stephen's Day"),
    holiday("2027-01-01", "New Year's Day"),
  ];
  /** Phantasialand's winter 2025/26: shut until Christmas, and on New Year's Day. */
  const phantasialandOpen = (day: string) =>
    day >= "2025-12-26" && day !== "2026-01-01";

  it("passes over a break day the park was shut on for the next break day", () => {
    // Wednesday 23 December 2026, a break day: 52 weeks back is Wednesday 24 December 2025.
    expect(refer("2026-12-23", CHRISTMAS)).toBe("2025-12-24");
    expect(
      refer("2026-12-23", CHRISTMAS, { measuredOpen: phantasialandOpen }),
    ).toBe("2025-12-31");
  });

  it("goes on to the next kind of day when none of its own was open", () => {
    const measuredOpen = (day: string) =>
      phantasialandOpen(day) && day !== "2025-12-31";
    // No open break Wednesday within three weeks: the plain Wednesday nearest.
    expect(refer("2026-12-23", CHRISTMAS, { measuredOpen })).toBe("2026-01-07");
  });

  it("passes over a holiday's namesake the park was shut on", () => {
    // Christmas Day 2025 was shut: the holiday of the same weekday nearest
    // 52 weeks back, St. Stephen's Day.
    expect(refer("2026-12-25", CHRISTMAS)).toBe("2025-12-25");
    expect(
      refer("2026-12-25", CHRISTMAS, { measuredOpen: phantasialandOpen }),
    ).toBe("2025-12-26");
  });

  it("reads 52 weeks back when the park was open on none of the days", () => {
    expect(refer("2026-12-23", CHRISTMAS, { measuredOpen: () => false })).toBe(
      "2025-12-24",
    );
  });

  it("reads only the days it picks when it learns by reading", async () => {
    const facts = lastYearDayFacts(
      lastYearFactDays("2026-12-23", "2026-12-23"),
      CHRISTMAS,
      park,
      "2027-07-20",
    );
    const read = jest.fn(async (day: string) => phantasialandOpen(day));
    await expect(
      lastYearReferenceDayReading("2026-12-23", facts, {}, read),
    ).resolves.toBe("2025-12-31");
    expect(read.mock.calls.map(([day]) => day)).toEqual([
      "2025-12-24",
      "2025-12-31",
    ]);

    // Nothing open: each day picked is read once, then 52 weeks back.
    const shut = jest.fn(async (_day: string) => false);
    await expect(
      lastYearReferenceDayReading("2026-12-23", facts, {}, shut),
    ).resolves.toBe("2025-12-24");
    const days = shut.mock.calls.map(([day]) => day);
    expect(new Set(days).size).toBe(days.length);
  });
});

describe("lastYearReferenceMonths", () => {
  it("lists every past month a reference day can sit in, from today on", () => {
    // 52 weeks before 1 and 31 March 2027, 36 days either side.
    expect(
      lastYearReferenceMonths("2027-03-01", "2027-03-31", "2026-11-17"),
    ).toEqual(["2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]);
    // Nothing a year back has been measured yet past today.
    expect(
      lastYearReferenceMonths("2027-11-10", "2027-11-20", "2026-11-17"),
    ).toEqual(["2026-10", "2026-11"]);
    expect(
      lastYearReferenceMonths("2026-11-20", "2026-11-10", "2026-11-17"),
    ).toEqual([]);
  });
});

describe("lastYearFactDays and lastYearSeasonsFrom", () => {
  it("cover the days and the year back as far as a reference day reaches", () => {
    const days = lastYearFactDays("2027-03-01", "2027-03-31");
    expect(days[0]).toBe(addIsoDays("2027-03-01", -364 - 36));
    expect(days[days.length - 1]).toBe("2027-03-31");
    expect(days).toContain(addIsoDays("2027-03-31", -364 + 36));
    expect(days).not.toContain(addIsoDays("2027-03-31", -364 + 37));
    expect(lastYearSeasonsFrom("2027-03-01")).toBe(
      addIsoDays("2027-03-01", -364 - 21),
    );
  });
});

describe("loadLastYearDayFacts", () => {
  it("reads the park's country three days past the days, and nothing without a country", async () => {
    const holidays = {
      getHolidays: jest.fn(async () => EASTER),
      getSchoolHolidayCoverage: jest.fn(async () => {
        throw new Error("down");
      }),
    };
    const facts = await loadLastYearDayFacts(
      holidays,
      { ...park, regionCode: "NRW" },
      "2027-03-29",
      "2027-03-29",
    );
    expect(holidays.getHolidays).toHaveBeenCalledWith(
      "DE",
      addIsoDays("2027-03-29", -364 - 36 - 3),
      "2027-04-01",
    );
    expect(facts?.get("2027-03-29")).toEqual({
      isHoliday: true,
      isBridgeDay: false,
      isSchoolVacation: false,
      holidayNames: ["Easter Monday"],
      // Without the coverage no day outside a stored break is known.
      schoolVacationUnknown: true,
    });
    await expect(
      loadLastYearDayFacts(
        holidays,
        { countryCode: null, regionCode: null, timezone: "UTC" },
        "2027-03-29",
        "2027-03-29",
      ),
    ).resolves.toBeNull();
  });
});
