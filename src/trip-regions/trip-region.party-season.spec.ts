import { ORLANDO } from "./regions";
import {
  assembleRegionCalendar,
  type ParkCalendarSource,
  type RegionCalendarDayLike,
  type RegionParkRecord,
  type RegionSeasonSource,
} from "./trip-region.assembly";
import fixture from "./__fixtures__/disney-2026-10-27-12-22.json";

/**
 * Magic Kingdom, Animal Kingdom and Hollywood Studios as production answered
 * on 2026-11-12 (`/calendar`, `/schedule`), with the researcher's seasons the
 * dev shim reads, for 2026-11-10 … 12-08 and fourteen days either side. In
 * party season nearly every other Magic Kingdom night closes to day guests at
 * 18:00; the feed still loses one of the two TICKETED_EVENT rows on many of
 * them, so several party nights show only through the season's `dates`.
 */
describe("party season on production's answers", () => {
  type FixturePark = {
    calendar: ParkCalendarSource["calendar"];
    schedule: ParkCalendarSource["rows"];
    seasons: Array<Omit<RegionSeasonSource, "parkId">>;
  };
  const parks = fixture.parks as unknown as Record<string, FixturePark>;
  const record = (slug: string): RegionParkRecord => ({
    config: ORLANDO.parks.find((p) => p.slug === slug)!,
    id: `id-${slug}`,
    slug,
    name: slug,
    path: `/parks/x/${slug}`,
    latitude: 0,
    longitude: 0,
    timezone: "America/New_York",
  });
  const build = (from: string, to: string) =>
    assembleRegionCalendar({
      region: ORLANDO,
      from,
      to,
      today: "2026-11-12",
      computedAt: "t",
      sources: Object.entries(parks).map(([slug, data]) => ({
        park: record(slug),
        calendar: {
          meta: data.calendar.meta,
          days: data.calendar.days as RegionCalendarDayLike[],
        },
        rows: data.schedule,
        lastYear: new Map(),
      })),
      seasons: Object.entries(parks).flatMap(([slug, data]) =>
        data.seasons.map((s) => ({
          ...s,
          parkId: `id-${slug}`,
          priceFrom: null,
          priceCurrency: null,
          attractionIds: null,
          url: null,
          sourceUrl: null,
          confirmedAt: null,
        })),
      ),
      forecastUntil: [],
    });

  it.each([
    ["one day", "2026-11-24", "2026-11-24"],
    ["eleven days", "2026-11-20", "2026-11-30"],
  ])(
    "calls Magic Kingdom's 24 November party night early and picks Animal Kingdom (%s)",
    (_w, from, to) => {
      const day = build(from, to).days.find((d) => d.date === "2026-11-24")!;
      expect(day.parks["magic-kingdom-park"]).toMatchObject({
        closesAt: "2026-11-24T18:00:00-05:00",
        closesEarly: true,
        evening: { kind: "ticketed_event" },
      });
      expect(day.best?.["walt-disney-world"]).toBe(
        "disneys-animal-kingdom-theme-park",
      );
      expect(day.bestWhy?.["walt-disney-world"]).toBe("closes_early_elsewhere");
    },
  );

  it("marks every 18:00 party night in the window and no full night", () => {
    const days = build("2026-11-10", "2026-12-08").days;
    for (const day of days) {
      const mk = day.parks["magic-kingdom-park"];
      if (mk.status !== "OPERATING" || !mk.closesAt) continue;
      const party = mk.evening?.kind === "ticketed_event";
      const closesAt18 = mk.closesAt.slice(11, 16) === "18:00";
      if (party && closesAt18) {
        expect({ date: day.date, closesEarly: mk.closesEarly }).toEqual({
          date: day.date,
          closesEarly: true,
        });
      }
      if (!party && !closesAt18) {
        expect({ date: day.date, closesEarly: mk.closesEarly }).toEqual({
          date: day.date,
          closesEarly: undefined,
        });
      }
    }
  });
});
