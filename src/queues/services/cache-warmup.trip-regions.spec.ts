import { CacheWarmupService } from "./cache-warmup.service";
import { CacheKeys } from "../../common/cache/cache-keys";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { shiftMonth } from "../../trip-regions/last-year-snapshot";

/**
 * The calendar warmup's trip-region branch: a region park's months reach +12
 * instead of +3, the last forecast day is recorded from what the calendar
 * returned, and the past months the `last_year` days read are written once
 * and then left alone.
 */
describe("CacheWarmupService › trip-region parks", () => {
  const TZ = "America/New_York";
  const today = getCurrentDateInTimezone(TZ);
  const forecastEnd = addIsoDays(today, 150);

  function setup(existing: string[] = []) {
    const store = new Map<string, string>(existing.map((k) => [k, "{}"]));
    const redis = {
      del: jest.fn(async () => 1),
      expire: jest.fn(async () => 1),
      exists: jest.fn(async (k: string) => (store.has(k) ? 1 : 0)),
      set: jest.fn(async (k: string, v: string) => {
        store.set(k, v);
        return "OK";
      }),
    };
    const built: string[] = [];
    const calendarService = {
      buildCalendarResponse: jest.fn(async (_park: unknown, from: Date) => {
        const first = from.toISOString().slice(0, 10);
        built.push(first.slice(0, 7));
        const days = [];
        for (
          let d = first;
          d.slice(0, 7) === first.slice(0, 7);
          d = addIsoDays(d, 1)
        ) {
          days.push({
            date: d,
            status: "OPERATING",
            crowdLevel: "moderate",
            headlinerForecast:
              d < today
                ? { avgWait: 40, actual: true, rides: [] }
                : d <= forecastEnd
                  ? { avgWait: 40, rides: [] }
                  : undefined,
          });
        }
        return { meta: {}, days };
      }),
    };
    const bestDaysService = { precomputeForPark: jest.fn(async () => "slug") };
    const service = new CacheWarmupService(
      {} as never,
      {} as never,
      redis as never,
      {} as never,
      {} as never,
      {} as never,
      calendarService as never,
      bestDaysService as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
      {} as never,
    );
    return { service, redis, store, built };
  }

  const park = (city: string, slug: string) => ({
    id: "00000000-0000-4000-8000-000000000001",
    slug,
    timezone: TZ,
    continentSlug: "north-america",
    countrySlug: "united-states",
    citySlug: city,
  });

  const warm = (service: CacheWarmupService, p: unknown) =>
    (
      service as unknown as {
        warmupCalendarForPark: (p: unknown, force: boolean) => Promise<unknown>;
      }
    ).warmupCalendarForPark(p, true);

  const current = today.slice(0, 7);
  const months = (from: number, to: number) =>
    Array.from({ length: to - from + 1 }, (_, i) =>
      shiftMonth(current, from + i),
    );

  it("warms an ordinary park −1…+3 and nothing for trip regions", async () => {
    const { service, built, store } = setup();
    await warm(service, park("bruhl", "phantasialand"));
    expect(built).toEqual(months(-1, 3));
    expect([...store.keys()].some((k) => k.startsWith("trip-regions:"))).toBe(
      false,
    );
  });

  it("warms a region park to +12, records its forecast end and writes its past months", async () => {
    const { service, built, store } = setup();
    const p = park("orlando", "magic-kingdom-park");
    await warm(service, p);
    expect(built.slice(0, 14)).toEqual(months(-1, 12));
    expect(store.get(CacheKeys.tripForecastUntil(p.id))).toBe(forecastEnd);
    for (const ym of months(-6, -2)) {
      const snapshot = JSON.parse(
        store.get(CacheKeys.tripLastYearMonth(p.id, ym)) ?? "null",
      );
      expect(snapshot[`${ym}-01`]).toEqual({ c: "moderate", w: 40 });
    }
    expect(built.slice(14)).toEqual(months(-6, -2));
  });

  it("leaves a past month that is already kept alone", async () => {
    const p = park("winter-haven", "legoland-florida");
    const kept = months(-6, -2).map((ym) =>
      CacheKeys.tripLastYearMonth(p.id, ym),
    );
    const { service, built } = setup(kept);
    await warm(service, p);
    expect(built).toEqual(months(-1, 12));
  });
});
