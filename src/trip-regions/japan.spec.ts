import { addIsoDays } from "../common/utils/date.util";
import { JAPAN } from "./regions";
import {
  normalizePlanQuery,
  optimizePlan,
  planResponse,
} from "./trip-plan.assembly";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";

/**
 * Japan's config planned end to end on a synthetic calendar: the design's
 * example trip (land at Haneda on Sat 10 Apr 2027, fly home from Kansai on
 * Mon 19 Apr, the five parks) with the nights the visitor fixes and without.
 */
function calendarOf(from: string, days: number): InternalRegionCalendar {
  const out: InternalRegionCalendar["days"] = [];
  for (let i = 0; i < days; i++) {
    const date = addIsoDays(from, i);
    const parks: Record<string, TripParkDayInternal> = {};
    JAPAN.parks.forEach((p, k) => {
      const wait = 25 + ((k * 11 + i * 9) % 30);
      parks[p.slug] = {
        status: "OPERATING",
        hoursPublished: true,
        crowdLevel: "moderate",
        headlinerWait: Math.round(wait / 5) * 5,
        waitExact: wait,
        basis: "forecast",
        dayHours: 12,
        usualHours: 12,
        opensAt: `${date}T09:00:00+09:00`,
        closesAt: `${date}T21:00:00+09:00`,
      };
    });
    out.push({ date, isHoliday: false, isSchoolVacation: false, parks });
  }
  return {
    meta: {
      region: "japan",
      timezone: "Asia/Tokyo",
      from,
      to: addIsoDays(from, days - 1),
      computedAt: "t",
      publishedHoursUntil: {},
    },
    parks: JAPAN.parks.map((p) => p.slug),
    days: out,
    seasons: [],
  };
}

function plan(extra: Record<string, string>) {
  const q = normalizePlanQuery(
    JAPAN,
    JAPAN.parks.map((p) => p.slug),
    {
      from: "2027-04-10",
      to: "2027-04-19",
      parks:
        "tokyo-disneyland,tokyo-disneysea,fuji-q-highland,legoland-japan,universal-studios-japan",
      split: "fixed",
      startBase: "tokyo",
      endBase: "osaka",
      ...extra,
    } as never,
    "2027-01-04",
  );
  const calendar = calendarOf(q.from, 10);
  const trip = optimizePlan(JAPAN, q, calendar);
  return planResponse(JAPAN, q, calendar, trip, {
    computedAt: "t",
    today: "2027-01-04",
    leadMae: new Map(),
  });
}

describe("Japan", () => {
  it("plans the design's trip with the nights the visitor fixed", () => {
    const answer = plan({
      bases:
        "tokyo:4@maihama,fujikawaguchiko:1,nagoya:1,osaka:3@universal_city",
    });
    const stays = (answer.meta as { stays: Array<Record<string, unknown>> })
      .stays;
    expect(stays.map((s) => [s.base, s.nights])).toEqual([
      ["tokyo", 4],
      ["fujikawaguchiko", 1],
      ["nagoya", 1],
      ["osaka", 3],
    ]);
    expect(answer.unplaced).toEqual([]);
    expect(answer.totals).toMatchObject({ moves: 3, visits: 5 });
  });

  it("chooses the stays itself, keeping Fuji-Q a day trip unless a night there pays", () => {
    const answer = plan({});
    const stays = (answer.meta as { stays: Array<Record<string, unknown>> })
      .stays;
    expect(stays[0].base).toBe("tokyo");
    expect(stays[stays.length - 1].base).toBe("osaka");
    expect(answer.unplaced).toEqual([]);
    expect(answer.meta.exact).toBe(true);
  });
});
