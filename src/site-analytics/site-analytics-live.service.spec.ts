import { ServiceUnavailableException } from "@nestjs/common";
import { SiteAnalyticsRedisMock } from "../../test/mocks/site-analytics-redis.mock";
import { SiteAnalyticsLiveService } from "./site-analytics-live.service";
import {
  SITE_ANALYTICS_KEYS as KEYS,
  minuteKey,
} from "./site-analytics.constants";

const NOW = new Date("2026-10-28T12:34:56.789Z");
const minute = (ago: number) =>
  minuteKey(new Date(NOW.getTime() - ago * 60_000));

/**
 * The live view, from the collector's minute keys alone. What is pinned: the
 * shape the admin page reads; 31 minutes oldest first, the current one last;
 * pages, countries and devices over five minutes, channels, referrers and
 * events over thirty, summed over the minutes and cut to their tops; one
 * language reads only its own keys; Redis away is a 503, not zeros.
 */
describe("SiteAnalyticsLiveService", () => {
  let redis: SiteAnalyticsRedisMock;
  let service: SiteAnalyticsLiveService;
  const hash = (key: string, fields: Record<string, number>) =>
    redis.hashes.set(
      key,
      new Map(Object.entries(fields).map(([k, v]) => [k, String(v)])),
    );

  beforeEach(() => {
    redis = new SiteAnalyticsRedisMock();
    service = new SiteAnalyticsLiveService(redis as never);
  });

  it("answers the live shape from the minute keys", async () => {
    redis.sets.set(KEYS.activeMinute(minute(0), "all"), new Set(["a", "b"]));
    redis.sets.set(KEYS.activeMinute(minute(4), "all"), new Set(["b", "c"]));
    // Five minutes ago is outside "active now", inside the per-minute series.
    redis.sets.set(KEYS.activeMinute(minute(5), "all"), new Set(["d"]));
    redis.counters.set(KEYS.pageviewsMinute(minute(0), "all"), 3);
    redis.counters.set(KEYS.pageviewsMinute(minute(30), "all"), 1);
    redis.counters.set(KEYS.eventsMinute(minute(0), "all"), 2);
    hash(KEYS.live("pages", minute(0), "all"), {
      [JSON.stringify(["/de", "home"])]: 2,
      [JSON.stringify(["/de/news", "news_index"])]: 1,
    });
    hash(KEYS.live("pages", minute(4), "all"), {
      [JSON.stringify(["/de/news", "news_index"])]: 2,
    });
    // Older than five minutes: no longer on the page list.
    hash(KEYS.live("pages", minute(5), "all"), {
      [JSON.stringify(["/de/old", "news"])]: 9,
    });
    hash(KEYS.live("countries", minute(1), "all"), { de: 3, zz: 1 });
    hash(KEYS.live("devices", minute(1), "all"), { mobile: 2, desktop: 2 });
    hash(KEYS.live("channels", minute(20), "all"), { search: 2 });
    hash(KEYS.live("channels", minute(0), "all"), { direct: 1, search: 1 });
    hash(KEYS.live("referrers", minute(29), "all"), { google: 2 });
    hash(KEYS.live("referrers", minute(31), "all"), { bing: 5 });
    hash(KEYS.live("events", minute(10), "all"), { search_opened: 4 });

    const live = await service.live(null, NOW);

    expect(live.at).toBe("2026-10-28T12:34:56Z");
    expect(live.activeVisitors).toBe(3);
    expect(live.perMinute).toHaveLength(31);
    expect(live.perMinute[0]).toEqual({
      minute: "2026-10-28T12:04:00Z",
      pageviews: 1,
      events: 0,
      visitors: 0,
    });
    expect(live.perMinute[25]).toMatchObject({ visitors: 1 });
    expect(live.perMinute[30]).toEqual({
      minute: "2026-10-28T12:34:00Z",
      pageviews: 3,
      events: 2,
      visitors: 2,
    });
    expect(live.pages).toEqual([
      { path: "/de/news", pageType: "news_index", views: 3 },
      { path: "/de", pageType: "home", views: 2 },
    ]);
    expect(live.countries).toEqual([
      { country: "DE", views: 3 },
      { country: "ZZ", views: 1 },
    ]);
    expect(live.devices).toEqual([
      { device: "desktop", views: 2 },
      { device: "mobile", views: 2 },
    ]);
    expect(live.channels).toEqual([
      { channel: "search", visits: 3 },
      { channel: "direct", visits: 1 },
    ]);
    expect(live.referrers).toEqual([{ referrer: "google", visits: 2 }]);
    expect(live.events).toEqual([{ name: "search_opened", count: 4 }]);
  });

  it("cuts the pages to ten, and reads one language's keys only", async () => {
    hash(
      KEYS.live("pages", minute(0), "de"),
      Object.fromEntries(
        Array.from({ length: 12 }, (_, i) => [
          JSON.stringify([`/de/p${String(i).padStart(2, "0")}`, "park"]),
          20 - i,
        ]),
      ),
    );
    hash(KEYS.live("pages", minute(0), "all"), {
      [JSON.stringify(["/nl", "home"])]: 50,
    });
    const live = await service.live("de", NOW);
    expect(live.pages).toHaveLength(10);
    expect(live.pages[0]).toEqual({
      path: "/de/p00",
      pageType: "park",
      views: 20,
    });
    expect(live.pages.map((p) => p.path)).not.toContain("/nl");
  });

  it("leaves the data centres' readers out unless asked, and adds them up when asked", async () => {
    redis.sets.set(KEYS.activeMinute(minute(0), "all"), new Set(["a"]));
    redis.sets.set(KEYS.activeMinute(minute(0), "dca"), new Set(["x", "y"]));
    redis.counters.set(KEYS.pageviewsMinute(minute(0), "all"), 2);
    redis.counters.set(KEYS.pageviewsMinute(minute(0), "dca"), 5);
    hash(KEYS.live("countries", minute(0), "all"), { de: 2 });
    hash(KEYS.live("countries", minute(0), "dca"), { us: 5 });

    const without = await service.live(null, NOW);
    expect(without.activeVisitors).toBe(1);
    expect(without.perMinute[30]).toMatchObject({ pageviews: 2 });
    expect(without.countries).toEqual([{ country: "DE", views: 2 }]);

    const only = await service.live(null, NOW, "only");
    expect(only.activeVisitors).toBe(2);
    expect(only.countries).toEqual([{ country: "US", views: 5 }]);

    const all = await service.live(null, NOW, "include");
    expect(all.activeVisitors).toBe(3);
    expect(all.perMinute[30]).toMatchObject({ pageviews: 7, visitors: 3 });
    expect(all.countries).toEqual([
      { country: "US", views: 5 },
      { country: "DE", views: 2 },
    ]);
  });

  it("answers 503 when Redis does not", async () => {
    redis.failNext = ["down"];
    await expect(service.live(null, NOW)).rejects.toBeInstanceOf(
      ServiceUnavailableException,
    );
  });
});
