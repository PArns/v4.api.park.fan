import type { HolidayRow } from "../../holidays/holidays.service";
import {
  buildLocalHolidayMap,
  dayHolidayFlags,
  isLocalHoliday,
} from "./holiday.utils";

/**
 * The local-holiday rule the calendar and the yearly forecast share. The
 * weekdays below are real: 2026-05-14 is a Thursday, 2026-10-16 a Friday.
 */
describe("holiday.utils › local holidays", () => {
  const park = {
    countryCode: "DE",
    // The geocoder's spelling; the holiday rows say "DE-NW".
    regionCode: "NRW",
    timezone: "Europe/Berlin",
  };

  const row = (overrides: Partial<HolidayRow>): HolidayRow => ({
    date: "2026-10-12",
    name: "Autumn Holidays",
    localName: "Herbstferien",
    country: "DE",
    region: "DE-NW",
    holidayType: "school",
    isNationwide: false,
    ...overrides,
  });

  describe("isLocalHoliday", () => {
    it("takes the park's own region through its alias", () => {
      expect(isLocalHoliday(row({}), park)).toBe(true);
    });

    it("takes a nationwide row and a row without a region", () => {
      expect(
        isLocalHoliday(row({ region: "DE-BY", isNationwide: true }), park),
      ).toBe(true);
      expect(isLocalHoliday(row({ region: null }), park)).toBe(true);
    });

    it("leaves out another region and another country", () => {
      expect(isLocalHoliday(row({ region: "DE-BY" }), park)).toBe(false);
      expect(
        isLocalHoliday(
          row({ country: "NL", region: null, isNationwide: true }),
          park,
        ),
      ).toBe(false);
    });
  });

  describe("buildLocalHolidayMap", () => {
    it("keys a row by the day it names, whatever the park's zone", () => {
      const map = buildLocalHolidayMap(
        [row({ date: "2026-12-25", region: null, isNationwide: true })],
        { ...park, timezone: "Pacific/Auckland" },
      );

      expect([...map.keys()]).toEqual(["2026-12-25"]);
    });

    it("keeps both types of a day and names it after the public holiday", () => {
      const map = buildLocalHolidayMap(
        [
          row({ date: "2026-10-03" }),
          row({
            date: "2026-10-03",
            name: "German Unity Day",
            localName: "Tag der Deutschen Einheit",
            region: null,
            holidayType: "public",
            isNationwide: true,
          }),
        ],
        park,
      );

      expect(map.get("2026-10-03")).toEqual({
        name: "Tag der Deutschen Einheit",
        type: "public",
        allTypes: ["school", "public"],
      });
    });
  });

  describe("dayHolidayFlags", () => {
    const autumnBreak = [
      "2026-10-12",
      "2026-10-13",
      "2026-10-14",
      "2026-10-15",
      "2026-10-16",
    ].map((date) => row({ date }));

    it("names a break day and carries the break over its weekend without a name", () => {
      const flags = dayHolidayFlags(
        ["2026-10-12", "2026-10-17", "2026-10-19"],
        autumnBreak,
        park,
      );

      expect(flags.get("2026-10-12")).toEqual({
        isHoliday: false,
        isSchoolVacation: true,
        isBridgeDay: false,
        schoolVacationName: "Autumn Holidays",
      });
      expect(flags.get("2026-10-17")).toEqual({
        isHoliday: false,
        isSchoolVacation: true,
        isBridgeDay: false,
      });
      expect(flags.get("2026-10-19")).toEqual({
        isHoliday: false,
        isSchoolVacation: false,
        isBridgeDay: false,
      });
    });

    it("gives the public holiday its English name and marks the Friday after it as a bridge day", () => {
      const ascension = row({
        date: "2026-05-14",
        name: "Ascension Day",
        localName: "Christi Himmelfahrt",
        region: null,
        holidayType: "public",
        isNationwide: true,
      });

      const flags = dayHolidayFlags(
        ["2026-05-14", "2026-05-15"],
        [ascension],
        park,
      );

      expect(flags.get("2026-05-14")).toEqual({
        isHoliday: true,
        isSchoolVacation: false,
        isBridgeDay: false,
        holidayName: "Ascension Day",
      });
      expect(flags.get("2026-05-15")).toEqual({
        isHoliday: false,
        isSchoolVacation: false,
        isBridgeDay: true,
      });
    });

    it("keeps a holiday on its own day in a park east of UTC+12", () => {
      // Rainbow's End, Pacific/Auckland (UTC+13 in December): noon UTC on
      // the 25th is already the 26th there, which is where the calendar used
      // to put Christmas.
      const auckland = {
        countryCode: "NZ",
        regionCode: "NZ-AUK",
        timezone: "Pacific/Auckland",
      };
      const nzRow = (date: string, name: string): HolidayRow =>
        row({
          date,
          name,
          localName: name,
          country: "NZ",
          region: null,
          holidayType: "public",
          isNationwide: true,
        });

      const flags = dayHolidayFlags(
        ["2026-12-25", "2026-12-26", "2026-12-27", "2026-12-28", "2026-12-29"],
        [
          nzRow("2026-12-25", "Christmas Day"),
          nzRow("2026-12-28", "Boxing Day"),
        ],
        auckland,
      );

      const holidays = [...flags.entries()]
        .filter(([, day]) => day.isHoliday || day.isBridgeDay)
        .map(([date, day]) => [date, day.holidayName, day.isBridgeDay]);
      expect(holidays).toEqual([
        ["2026-12-25", "Christmas Day", false],
        ["2026-12-28", "Boxing Day", false],
      ]);
    });

    it("ignores a neighbouring region's break", () => {
      const flags = dayHolidayFlags(
        ["2026-10-12"],
        [row({ region: "DE-BY" })],
        park,
      );

      expect(flags.get("2026-10-12")).toEqual({
        isHoliday: false,
        isSchoolVacation: false,
        isBridgeDay: false,
      });
    });
  });
});
