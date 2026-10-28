import {
  SqlParams,
  bounceRate,
  daysBetween,
  hoursOf,
  parseFilters,
  periodOf,
  periodText,
  rate,
  refuseVisitFilters,
  siteAnalyticsWindow,
  visitFilters,
  whereOf,
} from "./site-analytics.query";

const NOW = new Date("2026-10-28T07:12:40Z");

/**
 * The pieces every report is built from. What is pinned: a window is N UTC
 * days with today and the N before, the window before ending at the same
 * hour, and none past 180 days; a filter is a known dimension and a value
 * that only ever reaches SQL as a parameter; a visit filter is refused where
 * it cannot apply; rates divide by zero to zero.
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
      // 07:12 today: the window before ends with its last day's 07 hour.
      previous: {
        from: "2026-08-30",
        to: "2026-09-28",
        start: "2026-08-30T00:00:00.000Z",
        end: "2026-09-28T08:00:00.000Z",
      },
      hourly: false,
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

  it("has no window before past 180 days, which would lie past the retention", () => {
    expect(siteAnalyticsWindow({ days: 180 }, NOW).previous).not.toBeNull();
    expect(siteAnalyticsWindow({ days: 181 }, NOW).previous).toBeNull();
  });

  it("tells visit filters apart and refuses them where only page views or events count", () => {
    const window = siteAnalyticsWindow(
      { days: 7, filter: ["channel:search", "country:nl"] },
      NOW,
    );
    expect(visitFilters(window)).toEqual([{ dim: "channel", value: "search" }]);
    expect(() => refuseVisitFilters(window, "The events report")).toThrow(
      /channel can only filter visits/,
    );
    expect(() =>
      refuseVisitFilters(
        siteAnalyticsWindow({ days: 7, filter: ["country:nl"] }, NOW),
        "The events report",
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
      ]),
    ).toEqual([
      { dim: "channel", value: "search" },
      { dim: "path", value: "/de/parks:odd" },
    ]);
    expect(parseFilters(undefined)).toEqual([]);
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
