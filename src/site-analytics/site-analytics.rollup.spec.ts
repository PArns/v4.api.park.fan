import {
  EVENT_CUBOIDS,
  EVENT_COLUMNS,
  PAGEVIEW_COLUMNS,
  PAGEVIEW_CUBOIDS,
  chooseCuboid,
  cuboidColumns,
  rollupSql,
  rolledSource,
  rowsOf,
  splitRanges,
  takeRolledDaysWhole,
} from "./site-analytics.rollup";
import { SqlParams, siteAnalyticsWindow } from "./site-analytics.query";

const NOW = new Date("2026-10-28T07:12:40Z");
const DAY = "T00:00:00.000Z";

/** The narrow dimensions whose pairs are cuboids, in the order of their numbers. */
const NARROW = [
  "page_type",
  "device",
  "browser",
  "os",
  "country",
  "browser_language",
  "channel",
  "referrer",
];

/**
 * The daily level's building blocks. What is pinned: the cuboid numbers never
 * move (a stored row and a marker carry them), and every dimension a cuboid
 * names is a column of its table; a statement reads the smallest cuboid with
 * all its columns, or the hourly rows; whole rolled days come from the daily
 * rows and the rest (today, a day not yet rolled, the hour a window before
 * ends at) from the hourly ones; the SQL that writes a day groups by exactly
 * the cuboid's dimensions and fills the others with empty values, and writes
 * the marker only when it is the first to.
 */
describe("site-analytics.rollup", () => {
  describe("the cuboids", () => {
    it("number the page-view groupings 1 to 40 and keep the pairs in the order of the narrow dimensions", () => {
      expect(PAGEVIEW_CUBOIDS.map((c) => c.id)).toEqual(
        Array.from({ length: 40 }, (_, i) => i + 1),
      );
      expect(PAGEVIEW_CUBOIDS.slice(0, 12)).toEqual([
        { id: 1, dims: [] },
        { id: 2, dims: ["path", "page_type"] },
        { id: 3, dims: ["page_type"] },
        { id: 4, dims: ["device"] },
        { id: 5, dims: ["browser"] },
        { id: 6, dims: ["os"] },
        { id: 7, dims: ["country"] },
        { id: 8, dims: ["browser_language"] },
        { id: 9, dims: ["entry"] },
        { id: 10, dims: ["referrer"] },
        { id: 11, dims: ["channel"] },
        { id: 12, dims: ["page_type", "entry", "referrer", "channel"] },
      ]);
      const pairs = NARROW.flatMap((a, i) =>
        NARROW.slice(i + 1).map((b) => [a, b]),
      );
      expect(PAGEVIEW_CUBOIDS.slice(12).map((c) => c.dims)).toEqual(pairs);
    });

    it("number the event groupings 1 to 4", () => {
      expect(EVENT_CUBOIDS).toEqual([
        { id: 1, dims: ["name"] },
        { id: 2, dims: ["name", "props_key"] },
        { id: 3, dims: ["name", "page_type", "device", "props_key"] },
        { id: 4, dims: ["name", "entity"] },
      ]);
    });

    it("name only columns their table has, each once, and no two the same set", () => {
      for (const [cuboids, columns] of [
        [PAGEVIEW_CUBOIDS, PAGEVIEW_COLUMNS],
        [EVENT_CUBOIDS, EVENT_COLUMNS],
      ] as const) {
        const seen = new Set<string>();
        for (const cuboid of cuboids) {
          expect(new Set(cuboid.dims).size).toBe(cuboid.dims.length);
          for (const dim of cuboid.dims) {
            expect(columns as readonly string[]).toContain(dim);
          }
          const key = [...cuboid.dims].sort().join(",");
          expect(seen.has(key)).toBe(false);
          seen.add(key);
        }
      }
    });

    it("hold the properties with their key and always the bucket, the language, the network and the measures", () => {
      expect(cuboidColumns("events", EVENT_CUBOIDS[1])).toEqual([
        "bucket",
        "locale",
        "network",
        "name",
        "props_key",
        "props",
        "count",
      ]);
      expect(cuboidColumns("pageviews", PAGEVIEW_CUBOIDS[0])).toEqual([
        "bucket",
        "locale",
        "network",
        "views",
        "seconds",
        "visits",
        "engaged",
        "visit_seconds",
        "exits",
        "visit_span_seconds",
      ]);
    });
  });

  describe("chooseCuboid", () => {
    it("takes the smallest cuboid that has every column, whatever order they come in", () => {
      expect(chooseCuboid("pageviews", [])?.id).toBe(1);
      expect(chooseCuboid("pageviews", ["country"])?.id).toBe(7);
      expect(chooseCuboid("pageviews", ["os", "country"])?.id).toBe(31);
      expect(chooseCuboid("pageviews", ["country", "os"])?.id).toBe(31);
      expect(chooseCuboid("pageviews", ["path"])?.id).toBe(2);
      expect(
        chooseCuboid("pageviews", ["entry", "channel", "page_type"])?.id,
      ).toBe(12);
      expect(chooseCuboid("events", ["name", "props_key"])?.id).toBe(2);
      expect(chooseCuboid("events", ["name", "props_key", "device"])?.id).toBe(
        3,
      );
    });

    it("never needs the language, which every cuboid keeps", () => {
      expect(chooseCuboid("pageviews", ["locale"])?.id).toBe(1);
      expect(chooseCuboid("pageviews", ["locale", "os"])?.id).toBe(6);
    });

    it("answers null when no cuboid has them all, so the hourly rows are read", () => {
      expect(chooseCuboid("pageviews", ["path", "country"])).toBeNull();
      expect(chooseCuboid("pageviews", ["entry", "os"])).toBeNull();
      expect(chooseCuboid("events", ["name", "country"])).toBeNull();
    });
  });

  describe("splitRanges", () => {
    const rolled = (...days: string[]) => new Set(days);

    it("reads whole rolled days daily and joins neighbours into one range", () => {
      expect(
        splitRanges(
          [{ start: `2026-10-20${DAY}`, end: `2026-10-23${DAY}` }],
          rolled("2026-10-20", "2026-10-21", "2026-10-22"),
        ),
      ).toEqual({
        daily: [{ start: `2026-10-20${DAY}`, end: `2026-10-23${DAY}` }],
        hourly: [],
      });
    });

    it("reads today, and a day the job has not reached, hourly", () => {
      expect(
        splitRanges(
          [{ start: `2026-10-25${DAY}`, end: `2026-10-29${DAY}` }],
          rolled("2026-10-25", "2026-10-26"),
        ),
      ).toEqual({
        daily: [{ start: `2026-10-25${DAY}`, end: `2026-10-27${DAY}` }],
        hourly: [{ start: `2026-10-27${DAY}`, end: `2026-10-29${DAY}` }],
      });
    });

    it("splits around a gap, and takes a day a range cuts hourly even when it is rolled", () => {
      expect(
        splitRanges(
          [{ start: `2026-10-20${DAY}`, end: "2026-10-23T08:00:00.000Z" }],
          rolled("2026-10-20", "2026-10-22", "2026-10-23"),
        ),
      ).toEqual({
        daily: [
          { start: `2026-10-20${DAY}`, end: `2026-10-21${DAY}` },
          { start: `2026-10-22${DAY}`, end: `2026-10-23${DAY}` },
        ],
        hourly: [
          { start: `2026-10-21${DAY}`, end: `2026-10-22${DAY}` },
          { start: `2026-10-23${DAY}`, end: "2026-10-23T08:00:00.000Z" },
        ],
      });
    });

    it("takes a range that starts inside a day hourly up to the next midnight", () => {
      expect(
        splitRanges(
          [{ start: "2026-10-20T08:00:00.000Z", end: `2026-10-22${DAY}` }],
          rolled("2026-10-20", "2026-10-21"),
        ),
      ).toEqual({
        daily: [{ start: `2026-10-21${DAY}`, end: `2026-10-22${DAY}` }],
        hourly: [
          { start: "2026-10-20T08:00:00.000Z", end: `2026-10-21${DAY}` },
        ],
      });
    });

    it("splits each range of a pair on its own", () => {
      const result = splitRanges(
        [
          { start: `2026-10-01${DAY}`, end: "2026-10-03T08:00:00.000Z" },
          { start: `2026-10-05${DAY}`, end: `2026-10-07${DAY}` },
        ],
        rolled("2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06"),
      );
      expect(result.daily).toEqual([
        { start: `2026-10-01${DAY}`, end: `2026-10-03${DAY}` },
        { start: `2026-10-05${DAY}`, end: `2026-10-07${DAY}` },
      ]);
      expect(result.hourly).toEqual([
        { start: `2026-10-03${DAY}`, end: "2026-10-03T08:00:00.000Z" },
      ]);
    });
  });

  describe("takeRolledDaysWhole", () => {
    const cut = [
      { start: "2026-06-01T00:00:00.000Z", end: "2026-06-30T08:00:00.000Z" },
    ];
    const rolled = new Set(["2026-06-30"]);

    it("gives a range that ends inside a rolled day before the horizon the rest of that day", () => {
      expect(takeRolledDaysWhole(cut, "2026-07-30", rolled)).toEqual([
        { start: "2026-06-01T00:00:00.000Z", end: "2026-07-01T00:00:00.000Z" },
      ]);
    });

    it("leaves a range alone that ends at midnight, inside a day not rolled up, or from the horizon on", () => {
      const whole = [
        { start: "2026-06-01T00:00:00.000Z", end: "2026-07-01T00:00:00.000Z" },
      ];
      expect(takeRolledDaysWhole(whole, "2026-07-30", rolled)).toEqual(whole);
      expect(takeRolledDaysWhole(cut, "2026-07-30", new Set())).toEqual(cut);
      expect(takeRolledDaysWhole(cut, "2026-06-30", rolled)).toEqual(cut);
      expect(takeRolledDaysWhole(cut, "2026-06-29", rolled)).toEqual(cut);
    });

    it("does not change the ranges it was given", () => {
      const copy = structuredClone(cut);
      takeRolledDaysWhole(cut, "2026-07-30", rolled);
      expect(cut).toEqual(copy);
    });
  });

  describe("rolledSource", () => {
    const window = siteAnalyticsWindow(
      { days: 7, filter: ["country:de"], locale: "de" },
      NOW,
    );
    const days = new Set(["2026-10-22", "2026-10-23", "2026-10-24"]);

    it("reads the rolled days from the cuboid and the rest from the hourly rows, both under the filters", () => {
      const params = new SqlParams();
      const source = rolledSource(
        "pageviews",
        PAGEVIEW_CUBOIDS[15],
        params,
        window,
        [{ start: window.start, end: window.end }],
        days,
        "site_pageviews",
      ) as string;
      expect(source).toContain(
        "FROM site_pageviews_daily WHERE cuboid = $3::smallint",
      );
      expect(source).toContain("UNION ALL SELECT");
      expect(source).toMatch(/FROM site_pageviews WHERE \(bucket >= /);
      expect(source.endsWith(") AS site_pageviews")).toBe(true);
      // Only the cuboid's columns, so a statement that reads another fails.
      expect(source).toContain(
        "SELECT bucket, locale, network, page_type, country, views, seconds",
      );
      expect(source).not.toContain("browser_language");
      expect(
        source.match(
          /locale = \$\d+ AND network IS DISTINCT FROM 'datacenter' AND country = \$\d+/g,
        ),
      ).toHaveLength(2);
      // The window's language and filter once, however many tables read them.
      expect(params.values.slice(0, 3)).toEqual(["de", "de", 16]);
      expect(params.values).toHaveLength(7);
    });

    it("leaves out the hourly branch when every day is rolled", () => {
      const params = new SqlParams();
      const source = rolledSource(
        "events",
        EVENT_CUBOIDS[0],
        params,
        { locale: null, filters: [] },
        [{ start: `2026-10-22${DAY}`, end: `2026-10-24${DAY}` }],
        days,
        "e",
      ) as string;
      expect(source).not.toContain("UNION ALL");
      expect(source).toContain("FROM site_events_daily WHERE cuboid = $1");
      expect(source.endsWith(") AS e")).toBe(true);
    });

    it("sums each branch over the columns the statement reads when it asks for no day", () => {
      const params = new SqlParams();
      const source = rolledSource(
        "pageviews",
        PAGEVIEW_CUBOIDS[1],
        params,
        { locale: null, filters: [] },
        [{ start: window.start, end: window.end }],
        days,
        "site_pageviews",
        ["path"],
      ) as string;
      const [daily, hourly] = source.split(" UNION ALL ");
      expect(daily).toContain(
        "(SELECT path, SUM(views) AS views, SUM(seconds) AS seconds",
      );
      expect(daily).toContain("FROM site_pageviews_daily WHERE cuboid = $1");
      expect(daily).toMatch(/GROUP BY path$/);
      expect(hourly).toContain("FROM site_pageviews WHERE");
      expect(hourly).toMatch(/GROUP BY path\) AS site_pageviews$/);
      expect(source).not.toContain("bucket,");
    });

    it("keeps an event's properties with their key when it sums over them", () => {
      const source = rolledSource(
        "events",
        EVENT_CUBOIDS[1],
        new SqlParams(),
        { locale: null, filters: [] },
        [{ start: `2026-10-22${DAY}`, end: `2026-10-24${DAY}` }],
        days,
        "e",
        ["name", "props_key"],
      ) as string;
      expect(source).toContain(
        "SELECT name, props_key, props, SUM(count) AS count FROM site_events_daily",
      );
      expect(source).toContain("GROUP BY name, props_key, props) AS e");
    });

    it("is null when no whole day of the ranges is rolled", () => {
      expect(
        rolledSource(
          "pageviews",
          PAGEVIEW_CUBOIDS[0],
          new SqlParams(),
          window,
          [{ start: window.start, end: window.end }],
          new Set(),
          "site_pageviews",
        ),
      ).toBeNull();
    });
  });

  describe("rowsOf", () => {
    const rolledDays = jest.fn(
      async () => new Set(["2026-10-25", "2026-10-26"]),
    );
    beforeEach(() => rolledDays.mockClear());

    it("reads the hourly table as before for an hourly series, without asking what is rolled", async () => {
      const window = siteAnalyticsWindow({ days: 2, interval: "hour" }, NOW);
      const params = new SqlParams();
      const rows = await rowsOf(
        rolledDays,
        "pageviews",
        params,
        window,
        "both",
        ["country"],
      );
      expect(rows.from).toBe("site_pageviews");
      expect(rows.where).toContain("bucket >= $1::timestamptz");
      expect(rolledDays).not.toHaveBeenCalled();
    });

    it("reads the hourly table when no cuboid has the columns, aliased as the statement asks", async () => {
      const window = siteAnalyticsWindow({ days: 7 }, NOW);
      const rows = await rowsOf(
        rolledDays,
        "events",
        new SqlParams(),
        window,
        "current",
        ["name", "country"],
        { alias: "e" },
      );
      expect(rows.from).toBe("site_events e");
      expect(rows.where).toContain("e.bucket >= $1::timestamptz");
      expect(rolledDays).not.toHaveBeenCalled();
    });

    it("adds the window's filters to the columns it needs", async () => {
      const window = siteAnalyticsWindow({ days: 7, filter: ["os:ios"] }, NOW);
      const rows = await rowsOf(
        rolledDays,
        "pageviews",
        new SqlParams(),
        window,
        "current",
        ["country"],
      );
      expect(rolledDays).toHaveBeenCalledWith("pageviews", 31);
      expect(rows.where).toBe("TRUE");
      expect(rows.from).toContain("site_pageviews_daily");
    });

    it("reads the hourly table when nothing in the window is rolled", async () => {
      const window = siteAnalyticsWindow({ days: 1 }, NOW);
      const rows = await rowsOf(
        rolledDays,
        "pageviews",
        new SqlParams(),
        window,
        "current",
        [],
      );
      expect(rows.from).toBe("site_pageviews");
    });

    describe("what the window's reach learns", () => {
      /** Every day from 400 days back to yesterday is rolled up. */
      const everyDay = async () =>
        new Set(
          Array.from({ length: 400 }, (_, i) =>
            new Date(Date.parse("2026-10-27T00:00:00Z") - i * 86_400_000)
              .toISOString()
              .slice(0, 10),
          ),
        );
      const plan = async (
        query: Parameters<typeof siteAnalyticsWindow>[0],
        needed: string[],
        rolled: () => Promise<ReadonlySet<string>> = everyDay,
        scope: "current" | "both" = "both",
        source: "pageviews" | "events" = "pageviews",
      ) => {
        const window = siteAnalyticsWindow(query, NOW);
        const asked = jest.fn(rolled);
        await rowsOf(asked, source, new SqlParams(), window, scope, needed);
        return { window, asked };
      };

      it("is cut when a statement no cuboid serves reads a window that starts before the horizon", async () => {
        const { window, asked } = await plan(
          { days: 120, filter: ["path:/de/news"] },
          ["country"],
        );
        expect(window.reach.hourlyFrom).toBe("2026-07-30");
        // The days of cuboid 1, the day alone, say which days were rolled.
        expect(asked).toHaveBeenCalledWith("pageviews", 1);
      });

      it("asks an event statement for the first cuboid of the events", async () => {
        const { window, asked } = await plan(
          { days: 120, filter: ["country:de"] },
          ["name"],
          everyDay,
          "both",
          "events",
        );
        expect(window.reach.hourlyFrom).toBe("2026-07-30");
        expect(asked).toHaveBeenCalledWith("events", 1);
      });

      it("is cut by an hourly series over a day before the horizon", async () => {
        const window = siteAnalyticsWindow(
          { from: "2026-06-01", to: "2026-06-02", interval: "hour" },
          NOW,
        );
        await rowsOf(
          everyDay,
          "pageviews",
          new SqlParams(),
          window,
          "both",
          [],
        );
        expect(window.reach.hourlyFrom).toBe("2026-07-30");
      });

      it("is cut by the window before alone, which then starts earlier than the window", async () => {
        // 60 days: the window starts 29 August, the one before on 30 June.
        const { window } = await plan({ days: 60, filter: ["path:/de/news"] }, [
          "country",
        ]);
        expect(window.from).toBe("2026-08-30");
        expect(window.reach.hourlyFrom).toBe("2026-07-30");
        // The window alone is held whole.
        const current = await plan(
          { days: 60, filter: ["path:/de/news"] },
          ["country"],
          everyDay,
          "current",
        );
        expect(current.window.reach.hourlyFrom).toBeNull();
      });

      it("is not cut by a window the hourly rows hold whole, and reads no markers for it", async () => {
        const { window, asked } = await plan(
          { days: 30, filter: ["path:/de/news"] },
          ["country"],
        );
        expect(window.reach.hourlyFrom).toBeNull();
        expect(asked).not.toHaveBeenCalled();
      });

      it("is not cut by a window older than the statistic: no day before the horizon is rolled", async () => {
        const { window } = await plan(
          { days: 120, filter: ["path:/de/news"] },
          ["country"],
          async () => new Set(["2026-09-01", "2026-09-02"]),
        );
        expect(window.reach.hourlyFrom).toBeNull();
      });

      it("is not cut by a statement a cuboid serves: the days before the horizon come from the daily rows", async () => {
        const { window } = await plan({ days: 365 }, ["country"]);
        expect(window.reach.hourlyFrom).toBeNull();
      });

      it("is not cut by the window before's last day, which a cuboid's daily row holds whole past the horizon", async () => {
        // 120 days: the window before ends on 1 July, at the 08 hour, with
        // the hourly rows of that day gone.
        const window = siteAnalyticsWindow({ days: 120 }, NOW);
        expect(window.previous?.end).toBe("2026-06-30T08:00:00.000Z");
        const params = new SqlParams();
        const rows = await rowsOf(
          everyDay,
          "pageviews",
          params,
          window,
          "both",
          ["country"],
        );
        expect(window.reach.hourlyFrom).toBeNull();
        // Daily rows to the end of the day, hourly rows for today only.
        expect(params.values).toContain("2026-07-01T00:00:00.000Z");
        expect(params.values).not.toContain("2026-06-30T08:00:00.000Z");
        expect(rows.from).toContain("site_pageviews_daily");
      });

      it("is cut by a day rolled up for the first cuboid and not for the cuboid the statement reads, whose hourly rows are gone", async () => {
        // A cuboid added after the hourly rows of these days were dropped.
        const { window } = await plan(
          { days: 120 },
          ["country"],
          async () => new Set(),
        );
        expect(window.reach.hourlyFrom).toBeNull();
        const window2 = siteAnalyticsWindow({ days: 120 }, NOW);
        const days = await everyDay();
        await rowsOf(
          async (_source, cuboid) => (cuboid === 1 ? days : new Set<string>()),
          "pageviews",
          new SqlParams(),
          window2,
          "both",
          ["country"],
        );
        expect(window2.reach.hourlyFrom).toBe("2026-07-30");
      });

      it("is not cut by the hours of a cuboid's window that no marker covers", async () => {
        // Rolled days are daily; today and the days not rolled stay hourly,
        // none of them before the horizon.
        const { window } = await plan(
          { days: 365 },
          ["country"],
          async () => new Set(["2026-10-20"]),
        );
        expect(window.reach.hourlyFrom).toBeNull();
      });
    });
  });

  describe("rollupSql", () => {
    it("groups a page-view cuboid by its dimensions and fills the others with empty values", () => {
      const sql = rollupSql("pageviews", PAGEVIEW_CUBOIDS[15]);
      expect(sql).toContain("INSERT INTO site_pageviews_daily");
      expect(sql).toContain("GROUP BY locale, network, page_type, country");
      expect(sql).toContain("(bucket, cuboid, locale, network, path,");
      expect(sql).toMatch(
        /locale, network, '', page_type, '', '', '', country, '', '', '', '', SUM\(views\)::bigint/,
      );
      expect(sql).toContain("FROM site_pageviews, open");
      expect(sql).toContain("SUM(visit_span_seconds)::bigint");
    });

    it("writes the marker with the rows' count only where the cuboid has none for the day", () => {
      const sql = rollupSql("pageviews", PAGEVIEW_CUBOIDS[0]);
      expect(sql).toContain("WHERE NOT EXISTS");
      expect(sql).toContain("source = 'pageviews' AND cuboid = $3::smallint");
      expect(sql).toContain("INSERT INTO site_rollup_days");
      expect(sql).toContain("ON CONFLICT (source, cuboid, day) DO NOTHING");
      expect(sql).toContain("GROUP BY locale, network\n");
    });

    it("keeps an event's properties with their key, and the entity as NULL outside its cuboid", () => {
      const withProps = rollupSql("events", EVENT_CUBOIDS[1]);
      expect(withProps).toContain(
        "GROUP BY locale, network, name, props_key, props",
      );
      expect(withProps).toMatch(/NULL::uuid, SUM\(count\)::bigint/);
      const withEntity = rollupSql("events", EVENT_CUBOIDS[3]);
      expect(withEntity).toContain("GROUP BY locale, network, name, entity");
      expect(withEntity).toContain("'', '{}'::jsonb, entity");
    });
  });
});
