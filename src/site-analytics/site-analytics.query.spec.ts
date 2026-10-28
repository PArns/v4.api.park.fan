import {
  SqlParams,
  bounceRate,
  daysBetween,
  hoursOf,
  parseFilters,
  periodOf,
  rate,
  siteAnalyticsWindow,
  whereOf,
} from "./site-analytics.query";

const NOW = new Date("2026-10-28T07:12:40Z");

/**
 * The pieces every report is built from. What is pinned: a window is N UTC
 * days with today and the N before; a filter is a known dimension and a value
 * that only ever reaches SQL as a parameter; rates divide by zero to zero.
 */
describe("site-analytics.query", () => {
  it("makes a window of N UTC days with today, the N days before it, and hours for one day", () => {
    expect(siteAnalyticsWindow({ days: 30 }, NOW)).toEqual({
      from: "2026-09-29",
      to: "2026-10-28",
      days: 30,
      locale: null,
      filters: [],
      previousFrom: "2026-08-30",
      previousTo: "2026-09-28",
      hourly: false,
    });
    expect(siteAnalyticsWindow({ days: 1, locale: "nl" }, NOW)).toMatchObject({
      from: "2026-10-28",
      previousFrom: "2026-10-27",
      previousTo: "2026-10-27",
      locale: "nl",
      hourly: true,
    });
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
    const params = new SqlParams();
    const where = whereOf(
      params,
      {
        locale: "de",
        filters: [
          { dim: "channel", value: "search' OR 1=1 --" },
          { dim: "entry", value: "/de" },
        ],
      },
      "2026-10-01",
      "2026-10-07",
      "e",
    );
    expect(where).toBe(
      "e.bucket >= $1::timestamptz AND e.bucket < $2::timestamptz AND e.locale = $3 AND e.channel = $4 AND e.entry = $5",
    );
    expect(params.values).toEqual([
      "2026-10-01T00:00:00Z",
      "2026-10-08T00:00:00Z",
      "de",
      "search' OR 1=1 --",
      "/de",
    ]);
    expect(() =>
      whereOf(
        new SqlParams(),
        { locale: null, filters: [{ dim: "id; DROP" as never, value: "x" }] },
        "2026-10-01",
        "2026-10-01",
      ),
    ).toThrow();
  });

  it("buckets by the UTC day or hour, whatever the session's zone", () => {
    expect(periodOf(false)).toContain("date_trunc('day', bucket, 'UTC')");
    expect(periodOf(true, "e")).toContain(
      "date_trunc('hour', e.bucket, 'UTC')",
    );
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
