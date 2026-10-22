import { BadRequestException } from "@nestjs/common";
import { parseDateRange } from "./date-parsing.util";
import { formatInParkTimezone } from "./date.util";

describe("parseDateRange", () => {
  const berlin = { timezone: "Europe/Berlin" };

  it("parses a valid range into midnight and end-of-day in the park timezone", () => {
    const { fromDate, toDate } = parseDateRange(
      "2026-11-01",
      "2026-11-30",
      berlin,
    );

    // Berlin is UTC+1 on those dates.
    expect(fromDate.toISOString()).toBe("2026-10-31T23:00:00.000Z");
    expect(toDate.toISOString()).toBe("2026-11-30T22:59:59.000Z");
  });

  it('rejects a "to" day the month does not have', () => {
    // `new Date("2026-11-31")` silently rolls over to 1 December, so the
    // format check accepted it — but `fromZonedTime`, which produces the value
    // actually used, returns Invalid Date for that same string. The invalid
    // Date then travelled all the way into formatInParkTimezone and surfaced
    // as `RangeError: Invalid time value` — a 500 for what is a bad request.
    expect(() => parseDateRange("2026-11-01", "2026-11-31", berlin)).toThrow(
      BadRequestException,
    );
  });

  it('rejects a "from" day the month does not have', () => {
    expect(() => parseDateRange("2026-02-30", "2026-03-05", berlin)).toThrow(
      BadRequestException,
    );
  });

  it("still rejects an outright unparseable date", () => {
    expect(() => parseDateRange("not-a-date", undefined, berlin)).toThrow(
      BadRequestException,
    );
  });

  it("defaults the range when no dates are given", () => {
    const { fromDate, toDate } = parseDateRange(undefined, undefined, berlin);

    expect(isNaN(fromDate.getTime())).toBe(false);
    expect(isNaN(toDate.getTime())).toBe(false);
    expect(toDate.getTime()).toBeGreaterThan(fromDate.getTime());
  });

  describe("default dates", () => {
    beforeEach(() => {
      jest.useFakeTimers().setSystemTime(new Date("2026-10-22T12:00:00Z"));
    });
    afterEach(() => jest.useRealTimers());

    const day = (d: Date, timezone: string) =>
      formatInParkTimezone(d, timezone);

    it.each([
      ["America/New_York", "2026-11-21", "2026-10-22", "2026-10-15"],
      ["Europe/Berlin", "2026-11-21", "2026-10-22", "2026-10-15"],
      ["Asia/Tokyo", "2026-11-21", "2026-10-22", "2026-10-15"],
    ])(
      "%s: to = from + N days on the calendar",
      (timezone, ahead30, ahead0, fromWeekAgo) => {
        const thirty = parseDateRange(undefined, undefined, { timezone });
        expect(day(thirty.toDate, timezone)).toBe(ahead30);

        const zero = parseDateRange(undefined, undefined, {
          timezone,
          defaultToDaysAhead: 0,
        });
        expect(day(zero.toDate, timezone)).toBe(ahead0);

        const weekAgo = parseDateRange(undefined, undefined, {
          timezone,
          defaultFromDaysAgo: 7,
        });
        expect(day(weekAgo.fromDate, timezone)).toBe(fromWeekAgo);
      },
    );

    it("still answers 400 for an impossible from without a to", () => {
      expect(() => parseDateRange("2026-02-30", undefined, berlin)).toThrow(
        BadRequestException,
      );
    });
  });
});
