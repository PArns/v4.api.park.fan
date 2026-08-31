import { BadRequestException } from "@nestjs/common";
import { parseDateRange } from "./date-parsing.util";

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
});
