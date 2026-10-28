import {
  DAILY_RETENTION_DAYS,
  HOURLY_RETENTION_DAYS,
  HYPERTABLE_RETENTION_DAYS,
  ROLLUP_MARKER_RETENTION_DAYS,
  SITE_CHUNK_DAYS,
} from "./site-analytics.constants";
import {
  HYPERTABLES,
  SITE_ANALYTICS_HYPERTABLES,
} from "../database/hypertables";
import { MAX_ADMIN_WINDOW_DAYS } from "../page-views/page-view.constants";
import {
  HourlyReach,
  chunkStartDay,
  hourlyHorizon,
  retentionCutoff,
  unrolledDays,
} from "./site-analytics.retention";

/**
 * The retention of the statistic's tables. What is pinned: the hourly rows
 * outlive the longest window anything reads from them, a day's hourly chunk is
 * dropped only as a whole and only after the cutoff, the horizon a report
 * measures against is the start of the oldest chunk the nightly job keeps, a
 * chunk is droppable only when every day with rows in it has every marker,
 * and a report is cut only by a day whose hourly chunk can be gone.
 */
describe("site-analytics.retention", () => {
  describe("the constants", () => {
    it("keeps the hourly rows for 90 days and the daily level and the visitor counts for 400", () => {
      expect(HOURLY_RETENTION_DAYS).toBe(90);
      expect(DAILY_RETENTION_DAYS).toBe(400);
    });

    it("gives every hypertable of the statistic a retention, the hourly ones the shorter", () => {
      expect(Object.keys(HYPERTABLE_RETENTION_DAYS).sort()).toEqual(
        [...SITE_ANALYTICS_HYPERTABLES].sort(),
      );
      expect(HYPERTABLE_RETENTION_DAYS.site_pageviews).toBe(90);
      expect(HYPERTABLE_RETENTION_DAYS.site_events).toBe(90);
      expect(HYPERTABLE_RETENTION_DAYS.site_pageviews_daily).toBe(400);
      expect(HYPERTABLE_RETENTION_DAYS.site_events_daily).toBe(400);
    });

    it("keeps the markers longer than the daily rows they vouch for, which live a chunk past 400 days", () => {
      expect(ROLLUP_MARKER_RETENTION_DAYS).toBeGreaterThanOrEqual(
        DAILY_RETENTION_DAYS + SITE_CHUNK_DAYS,
      );
    });

    it("cuts the week-chunked tables by the same number of days as the retention counts with", () => {
      for (const table of [
        "site_pageviews",
        "site_events",
        "site_pageviews_daily",
        "site_events_daily",
      ]) {
        expect(HYPERTABLES.find((spec) => spec.table === table)).toMatchObject({
          chunkInterval: `${SITE_CHUNK_DAYS} days`,
        });
      }
    });

    it("holds the longest window the most-read rankings read from the hourly rows", () => {
      expect(MAX_ADMIN_WINDOW_DAYS).toBeLessThanOrEqual(HOURLY_RETENTION_DAYS);
    });
  });

  describe("chunkStartDay", () => {
    it("is the Thursday on or before the day, the way TimescaleDB aligns a week chunk to the epoch", () => {
      expect(chunkStartDay("1970-01-01")).toBe("1970-01-01");
      expect(chunkStartDay("2026-07-30")).toBe("2026-07-30");
      expect(chunkStartDay("2026-07-31")).toBe("2026-07-30");
      expect(chunkStartDay("2026-08-05")).toBe("2026-07-30");
      expect(chunkStartDay("2026-08-06")).toBe("2026-08-06");
      expect(chunkStartDay("2026-07-29")).toBe("2026-07-23");
    });
  });

  describe("retentionCutoff", () => {
    it("is the UTC midnight that many days before today, whatever the hour", () => {
      expect(
        retentionCutoff(
          new Date("2026-10-28T01:47:00Z"),
          HOURLY_RETENTION_DAYS,
        ),
      ).toBe("2026-07-30T00:00:00.000Z");
      expect(
        retentionCutoff(
          new Date("2026-10-28T23:59:59Z"),
          HOURLY_RETENTION_DAYS,
        ),
      ).toBe("2026-07-30T00:00:00.000Z");
      expect(
        retentionCutoff(new Date("2026-10-28T12:00:00Z"), DAILY_RETENTION_DAYS),
      ).toBe("2025-09-23T00:00:00.000Z");
    });
  });

  describe("hourlyHorizon", () => {
    it("is the start of the chunk that holds the cutoff day, which the job keeps", () => {
      // The cutoff day is itself a chunk's first day.
      expect(hourlyHorizon(new Date("2026-10-28T07:00:00Z"))).toBe(
        "2026-07-30",
      );
      // 31 July to 5 August lie inside the chunk that began on the 30th.
      expect(hourlyHorizon(new Date("2026-10-29T07:00:00Z"))).toBe(
        "2026-07-30",
      );
      expect(hourlyHorizon(new Date("2026-11-03T07:00:00Z"))).toBe(
        "2026-07-30",
      );
      // The cutoff reaches the next chunk's first day: the old chunk is due.
      expect(hourlyHorizon(new Date("2026-11-04T07:00:00Z"))).toBe(
        "2026-08-06",
      );
    });

    it("lets a row live from 90 days to a chunk more", () => {
      const lifetimes: number[] = [];
      // Every day a row can be written on, against the horizon on the day the
      // job would have taken its chunk.
      for (let back = 0; back < 14; back++) {
        const written = new Date(
          Date.parse("2026-07-30T00:00:00Z") + back * 86_400_000,
        );
        const writtenDay = written.toISOString().slice(0, 10);
        let age = 0;
        for (;;) {
          const today = new Date(written.getTime() + age * 86_400_000);
          if (hourlyHorizon(today) > writtenDay) break;
          age += 1;
        }
        lifetimes.push(age);
      }
      expect(Math.min(...lifetimes)).toBeGreaterThanOrEqual(
        HOURLY_RETENTION_DAYS,
      );
      expect(Math.max(...lifetimes)).toBeLessThanOrEqual(
        HOURLY_RETENTION_DAYS + SITE_CHUNK_DAYS,
      );
    });
  });

  describe("unrolledDays", () => {
    const wanted = [
      { source: "pageviews", cuboid: 1 },
      { source: "pageviews", cuboid: 2 },
      { source: "events", cuboid: 1 },
    ];
    const markers = (day: string) => [
      { source: "pageviews", cuboid: 1, day },
      { source: "pageviews", cuboid: 2, day },
      { source: "events", cuboid: 1, day },
    ];

    it("is empty when every day has the marker of every wanted cuboid", () => {
      expect(
        unrolledDays(
          ["2026-07-23", "2026-07-24"],
          [...markers("2026-07-23"), ...markers("2026-07-24")],
          wanted,
        ),
      ).toEqual([]);
    });

    it("names a day that has no marker at all and a day that misses one cuboid of one source", () => {
      expect(
        unrolledDays(
          ["2026-07-23", "2026-07-24", "2026-07-25"],
          [
            ...markers("2026-07-23"),
            { source: "pageviews", cuboid: 1, day: "2026-07-24" },
            { source: "pageviews", cuboid: 2, day: "2026-07-24" },
          ],
          wanted,
        ),
      ).toEqual(["2026-07-24", "2026-07-25"]);
    });

    it("does not take a marker of the other source, or of a cuboid nobody wants, for the one it misses", () => {
      expect(
        unrolledDays(
          ["2026-07-23"],
          [
            { source: "pageviews", cuboid: 1, day: "2026-07-23" },
            { source: "pageviews", cuboid: 2, day: "2026-07-23" },
            { source: "events", cuboid: 2, day: "2026-07-23" },
            { source: "pageviews", cuboid: 99, day: "2026-07-23" },
          ],
          wanted,
        ),
      ).toEqual(["2026-07-23"]);
    });

    it("has nothing to say about no days", () => {
      expect(unrolledDays([], [], wanted)).toEqual([]);
    });
  });

  describe("HourlyReach", () => {
    const range = (start: string, end: string) => ({
      start: `${start}T00:00:00.000Z`,
      end: `${end}T00:00:00.000Z`,
    });

    it("reaches before the horizon when any range starts before it", () => {
      const reach = new HourlyReach("2026-07-30");
      expect(reachesBefore(reach, "2026-07-30", "2026-10-29")).toBe(false);
      expect(reachesBefore(reach, "2026-07-29", "2026-10-29")).toBe(true);
      expect(
        reach.reachesBefore([
          range("2026-05-01", "2026-05-08"),
          range("2026-09-01", "2026-10-29"),
        ]),
      ).toBe(true);
    });

    it("is cut by a rolled day before the horizon, whose chunk is dropped", () => {
      const reach = new HourlyReach("2026-07-30");
      reach.note([range("2026-07-01", "2026-10-29")], new Set(["2026-07-10"]));
      expect(reach.hourlyFrom).toBe("2026-07-30");
    });

    it("is not cut when no day before the horizon is rolled: a younger statistic, or a chunk waiting for its roll-up", () => {
      const reach = new HourlyReach("2026-07-30");
      reach.note([range("2026-07-01", "2026-10-29")], new Set());
      reach.note(
        [range("2026-07-01", "2026-10-29")],
        new Set(["2026-07-30", "2026-08-15"]),
      );
      expect(reach.hourlyFrom).toBeNull();
    });

    it("looks only at the days of the ranges, and not past the horizon", () => {
      const reach = new HourlyReach("2026-07-30");
      reach.note([range("2026-08-01", "2026-09-01")], new Set(["2026-07-10"]));
      reach.note([range("2026-06-01", "2026-06-10")], new Set(["2026-07-10"]));
      expect(reach.hourlyFrom).toBeNull();
    });

    it("counts a day the range starts inside of, which an hour of the window before may start in", () => {
      const reach = new HourlyReach("2026-07-30");
      reach.note(
        [
          {
            start: "2026-07-20T09:00:00.000Z",
            end: "2026-07-20T10:00:00.000Z",
          },
        ],
        new Set(["2026-07-20"]),
      );
      expect(reach.hourlyFrom).toBe("2026-07-30");
    });

    it("stays cut once a statement has cut it", () => {
      const reach = new HourlyReach("2026-07-30");
      reach.note([range("2026-07-01", "2026-07-30")], new Set(["2026-07-02"]));
      reach.note([range("2026-09-01", "2026-09-02")], new Set());
      expect(reach.hourlyFrom).toBe("2026-07-30");
    });
  });
});

/** Whether a range from `start` to `end` reaches before the reach's horizon. */
function reachesBefore(reach: HourlyReach, start: string, end: string) {
  return reach.reachesBefore([
    { start: `${start}T00:00:00.000Z`, end: `${end}T00:00:00.000Z` },
  ]);
}
