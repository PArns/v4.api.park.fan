import { BadRequestException } from "@nestjs/common";
import { EVENT_DIMS, PAGE_DIMS } from "./site-analytics.constants";
import { HourlyReach } from "./site-analytics.retention";
import {
  SqlParams,
  averageSeconds,
  bounceRate,
  daysBetween,
  defaultInterval,
  hoursOf,
  parseFilters,
  periodOf,
  periodStartOf,
  periodText,
  rate,
  refuseFiltersBeyond,
  seriesPeriods,
  siteAnalyticsWindow,
  visitFilters,
  whereOf,
} from "./site-analytics.query";

const NOW = new Date("2026-10-28T07:12:40Z");

/**
 * The pieces every report is built from. What is pinned: a window is N UTC
 * days with today and the N before, the window before ending at the same
 * hour, and none past 180 days; a from–to window is any stretch up to today
 * with the same length before it, whole; the series step follows the length
 * unless asked, hours for two days at most; weeks start on Monday and a
 * period the window does not hold whole is partial; a filter is a known
 * dimension and a value that only ever reaches SQL as a parameter; a filter
 * on a dimension the rows lack is refused; rates and averages divide by zero
 * to zero or null.
 */
describe("site-analytics.query", () => {
  it("makes a window of N UTC days with today, and the N before it up to the same hour", () => {
    expect(siteAnalyticsWindow({ days: 30 }, NOW)).toEqual({
      from: "2026-09-29",
      to: "2026-10-28",
      start: "2026-09-29T00:00:00.000Z",
      end: "2026-10-29T00:00:00.000Z",
      days: 30,
      locale: null,
      filters: [],
      interval: "day",
      // 07:12 today: the window before ends with its last day's 07 hour.
      previous: {
        from: "2026-08-30",
        to: "2026-09-28",
        start: "2026-08-30T00:00:00.000Z",
        end: "2026-09-28T08:00:00.000Z",
      },
      hourly: false,
      // The chunk of 30 July is the oldest the nightly job keeps today.
      horizon: "2026-07-30",
      reach: expect.any(HourlyReach),
    });
    expect(siteAnalyticsWindow({ days: 1, locale: "nl" }, NOW)).toMatchObject({
      from: "2026-10-28",
      previous: {
        from: "2026-10-27",
        to: "2026-10-27",
        start: "2026-10-27T00:00:00.000Z",
        end: "2026-10-27T08:00:00.000Z",
      },
      locale: "nl",
      hourly: true,
    });
    // In the last hour of the day the window before is whole.
    expect(
      siteAnalyticsWindow({ days: 7 }, new Date("2026-10-28T23:30:00Z"))
        .previous?.end,
    ).toBe("2026-10-22T00:00:00.000Z");
  });

  it("has no window before past 180 days, which would lie past the daily rows' retention", () => {
    expect(siteAnalyticsWindow({ days: 180 }, NOW).previous).not.toBeNull();
    expect(siteAnalyticsWindow({ days: 181 }, NOW).previous).toBeNull();
  });

  it("gives every window its own reach, which says nothing until a statement has read hourly rows past the horizon", () => {
    const first = siteAnalyticsWindow({ days: 365 }, NOW);
    const second = siteAnalyticsWindow({ days: 365 }, NOW);
    first.reach.note(
      [{ start: "2025-10-29T00:00:00.000Z", end: "2026-10-29T00:00:00.000Z" }],
      new Set(["2026-01-01"]),
    );
    expect(first.reach.hourlyFrom).toBe("2026-07-30");
    expect(second.reach.hourlyFrom).toBeNull();
  });

  it("makes a from–to window ending before today, with a whole window before it", () => {
    expect(
      siteAnalyticsWindow({ from: "2026-10-01", to: "2026-10-07" }, NOW),
    ).toMatchObject({
      from: "2026-10-01",
      to: "2026-10-07",
      start: "2026-10-01T00:00:00.000Z",
      end: "2026-10-08T00:00:00.000Z",
      days: 7,
      interval: "day",
      previous: {
        from: "2026-09-24",
        to: "2026-09-30",
        start: "2026-09-24T00:00:00.000Z",
        end: "2026-10-01T00:00:00.000Z",
      },
    });
    // A from–to window that ends today compares like days=N.
    expect(
      siteAnalyticsWindow({ from: "2026-10-22", to: "2026-10-28" }, NOW)
        .previous,
    ).toEqual(siteAnalyticsWindow({ days: 7 }, NOW).previous);
  });

  it.each([
    [{ from: "2026-10-01" }, /go together/],
    [{ from: "2026-10-01", to: "2026-10-07", days: 7 }, /not both/],
    [{ from: "2026-02-30", to: "2026-03-02" }, /YYYY-MM-DD/],
    [{ from: "2026-10-07", to: "2026-10-01" }, /after to/],
    [{ from: "2026-10-28", to: "2026-10-29" }, /after today/],
    [{ from: "2025-10-26", to: "2026-10-27" }, /more than 366/],
    [{ days: 0 }, /days is 1 to 365/],
    [{ days: 3, interval: "hour" }, /at most 2 days/],
    [{ days: 3, interval: "year" }, /interval is/],
  ])("refuses the window %j", (query, message) => {
    const attempt = () => siteAnalyticsWindow(query as never, NOW);
    expect(attempt).toThrow(BadRequestException);
    expect(attempt).toThrow(message);
  });

  it("steps by the hour for a day, the day to 92, the week to 183, the month beyond", () => {
    expect([1, 2, 92, 93, 183, 184].map(defaultInterval)).toEqual([
      "hour",
      "day",
      "day",
      "week",
      "week",
      "month",
    ]);
    expect(
      siteAnalyticsWindow({ days: 2, interval: "hour" }, NOW),
    ).toMatchObject({ interval: "hour", hourly: true });
    expect(siteAnalyticsWindow({ days: 365 }, NOW).interval).toBe("month");
  });

  it("starts a week on Monday and a month on the first, UTC", () => {
    expect(periodStartOf("2026-10-28", "week")).toBe("2026-10-26");
    expect(periodStartOf("2026-10-26", "week")).toBe("2026-10-26");
    expect(periodStartOf("2026-10-04T13:00:00Z", "week")).toBe("2026-09-28");
    expect(periodStartOf("2026-10-28", "month")).toBe("2026-10-01");
    expect(periodStartOf("2026-10-28", "day")).toBe("2026-10-28");
    expect(periodStartOf("2026-10-28T07:00:00Z", "hour")).toBe(
      "2026-10-28T07:00:00Z",
    );
  });

  it("lists every period of a window and marks those it does not hold whole", () => {
    expect(
      seriesPeriods(
        { from: "2026-10-01", to: "2026-10-28", interval: "week" },
        NOW,
      ),
    ).toEqual([
      { day: "2026-09-28", partial: true },
      { day: "2026-10-05", partial: false },
      { day: "2026-10-12", partial: false },
      { day: "2026-10-19", partial: false },
      { day: "2026-10-26", partial: true },
    ]);
    expect(
      seriesPeriods(
        { from: "2026-09-01", to: "2026-09-30", interval: "month" },
        NOW,
      ),
    ).toEqual([{ day: "2026-09-01", partial: false }]);
    expect(
      seriesPeriods(
        { from: "2026-10-27", to: "2026-10-28", interval: "day" },
        NOW,
      ),
    ).toEqual([
      { day: "2026-10-27", partial: false },
      { day: "2026-10-28", partial: true },
    ]);
    const hours = seriesPeriods(
      { from: "2026-10-27", to: "2026-10-28", interval: "hour" },
      NOW,
    );
    expect(hours).toHaveLength(24 + 8);
    expect(hours[0]).toEqual({ day: "2026-10-27T00:00:00Z", partial: false });
    expect(hours[hours.length - 1]).toEqual({
      day: "2026-10-28T07:00:00Z",
      partial: true,
    });
  });

  it("tells visit filters apart and refuses a filter on what the rows do not carry", () => {
    const window = siteAnalyticsWindow(
      { days: 7, filter: ["channel:search", "country:nl"] },
      NOW,
    );
    expect(visitFilters(window)).toEqual([{ dim: "channel", value: "search" }]);
    expect(() =>
      refuseFiltersBeyond(window, "The events report", EVENT_DIMS),
    ).toThrow(/channel; the visit dimensions can only filter visits/);
    expect(() =>
      refuseFiltersBeyond(
        siteAnalyticsWindow({ days: 7, filter: ["browser_language:de"] }, NOW),
        "The events report",
        EVENT_DIMS,
      ),
    ).toThrow(/do not carry browser_language$/);
    expect(() =>
      refuseFiltersBeyond(
        siteAnalyticsWindow({ days: 7, filter: ["browser_language:de"] }, NOW),
        "The not-found report",
        PAGE_DIMS,
      ),
    ).not.toThrow();
  });

  it("parses filters and leaves out what is not one", () => {
    expect(
      parseFilters([
        "channel:search",
        "path:/de/parks:odd",
        "user:42",
        "country:",
        "nonsense",
        "path:/de\u0000x",
      ]),
    ).toEqual([
      { dim: "channel", value: "search" },
      { dim: "path", value: "/de/parks:odd" },
    ]);
    expect(parseFilters(undefined)).toEqual([]);
  });

  it("refuses a window of year 0000 to today at once, without listing its days", () => {
    const started = Date.now();
    expect(() =>
      siteAnalyticsWindow({ from: "0000-01-01", to: "2026-10-28" }, NOW),
    ).toThrow(/covers more than/);
    expect(Date.now() - started).toBeLessThan(100);
  });

  it("puts every value into a parameter and only known columns into the SQL", () => {
    const window = siteAnalyticsWindow(
      {
        days: 7,
        locale: "de",
        filter: ["channel:search' OR 1=1 --", "entry:/de"],
      },
      NOW,
    );
    const params = new SqlParams();
    expect(whereOf(params, window, "current", "e")).toBe(
      "e.bucket >= $1::timestamptz AND e.bucket < $2::timestamptz AND e.locale = $3 AND e.channel = $4 AND e.entry = $5",
    );
    expect(params.values).toEqual([
      "2026-10-22T00:00:00.000Z",
      "2026-10-29T00:00:00.000Z",
      "de",
      "search' OR 1=1 --",
      "/de",
    ]);
    expect(() =>
      whereOf(
        new SqlParams(),
        {
          ...window,
          locale: null,
          filters: [{ dim: "id; DROP" as never, value: "x" }],
        },
        "current",
      ),
    ).toThrow();
  });

  it("reads both windows in one range, leaving out the hours between them", () => {
    const window = siteAnalyticsWindow({ days: 7 }, NOW);
    const params = new SqlParams();
    expect(whereOf(params, window, "both")).toBe(
      "bucket >= $1::timestamptz AND bucket < $2::timestamptz AND NOT (bucket >= $3::timestamptz AND bucket < $4::timestamptz)",
    );
    expect(params.values).toEqual([
      "2026-10-15T00:00:00.000Z",
      "2026-10-29T00:00:00.000Z",
      "2026-10-21T08:00:00.000Z",
      "2026-10-22T00:00:00.000Z",
    ]);
    // Without a window before, both is the window alone.
    const long = new SqlParams();
    expect(whereOf(long, siteAnalyticsWindow({ days: 365 }, NOW), "both")).toBe(
      "bucket >= $1::timestamptz AND bucket < $2::timestamptz",
    );
  });

  it("buckets by the UTC day or hour and prints the period after the grouping", () => {
    expect(periodOf(false)).toBe("time_bucket('1 day'::interval, bucket)");
    expect(periodOf(true, "e")).toBe(
      "time_bucket('1 hour'::interval, e.bucket)",
    );
    const bucket = new Date("2026-10-28T06:00:00Z");
    expect(periodText(bucket, false)).toBe("2026-10-28");
    expect(periodText(bucket, true)).toBe("2026-10-28T06:00:00Z");
    expect(periodText("2026-10-27 22:00:00+00", false)).toBe("2026-10-27");
    expect(periodText(null, true)).toBeNull();
  });

  it("rounds a rate to four places, divides by zero to zero, and bounces what never saw a second page", () => {
    expect(rate(1, 3)).toBe(0.3333);
    expect(rate(5, 0)).toBe(0);
    expect(bounceRate(4, 1)).toBe(0.75);
    expect(bounceRate(0, 0)).toBe(0);
    expect(bounceRate(2, 5)).toBe(0);
    expect(averageSeconds(100, 3)).toBe(33);
    expect(averageSeconds(5, 0)).toBeNull();
  });

  it("lists every day of a range and the hours of today so far", () => {
    expect(daysBetween("2026-10-30", "2026-11-02")).toEqual([
      "2026-10-30",
      "2026-10-31",
      "2026-11-01",
      "2026-11-02",
    ]);
    expect(hoursOf("2026-10-28", NOW)).toEqual(
      Array.from({ length: 8 }, (_, h) => `2026-10-28T0${h}:00:00Z`),
    );
    expect(hoursOf("2026-10-27", NOW)).toHaveLength(24);
  });
});
