import { addIsoDays } from "../common/utils/date.util";
import { regionPlan, type CellTweak } from "./__fixtures__/region-days";
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

  describe("a long trip on last year's flat days", () => {
    // Production's days for 14–30 January 2027 (2026-11-14), one figure a
    // day: last year's measured waits, rounded to 5, so many days tie.
    // "-": closed.
    const WAITS: Record<string, string> = {
      "tokyo-disneyland": "80 80 80 75 75 60 60 65 60 90 65 65 60 65 55 65 85",
      "tokyo-disneysea":
        "155 90 140 115 110 100 100 115 105 125 105 110 105 110 110 70 115",
      "universal-studios-japan":
        "130 95 95 85 100 100 100 100 95 95 90 100 100 100 100 95 95",
      "fuji-q-highland": "- 50 60 55 50 55 - - 50 65 55 50 55 - - 50 65",
      "legoland-japan": "- 15 25 30 20 - - - 15 20 20 25 25 20 20 20 15",
    };
    /** The days that close early: 18:30 and 19:00 against 21:00. */
    const HOURS: Record<string, number> = {
      "tokyo-disneysea|2027-01-22": 9.5,
      "tokyo-disneyland|2027-01-28": 10,
      "tokyo-disneysea|2027-01-28": 10,
    };
    const days: CellTweak = (slug, date, cell) => {
      const i = Math.round(
        (Date.parse(`${date}T00:00:00Z`) - Date.parse("2027-01-14T00:00:00Z")) /
          86_400_000,
      );
      const figure = WAITS[slug].split(" ")[i];
      if (figure === "-") {
        cell.status = "CLOSED";
        cell.crowdLevel = "closed";
        cell.basis = "none";
        delete cell.headlinerWait;
        delete cell.waitExact;
        return;
      }
      const wait = Number(figure);
      cell.headlinerWait = wait;
      cell.waitExact = wait;
      cell.basis = "last_year";
      cell.dayHours = HOURS[`${slug}|${date}`] ?? 12;
    };
    const answer = regionPlan(
      JAPAN,
      {
        from: "2027-01-14",
        to: "2027-01-30",
        parks: "tokyo-disneyland:2,tokyo-disneysea:2,universal-studios-japan:2",
      },
      "2026-11-14",
      days,
    );

    it("spreads the park days over each stay instead of a week of rest", () => {
      // Without the spread: DisneySea on the 19th, Disneyland on the 20th
      // and 22nd, then six rest days to the 29th (production, on its
      // unrounded waits: rest from the 21st to the 25th and on the 27th).
      expect(answer.meta.stays?.map((st) => [st.base, st.nights])).toEqual([
        ["osaka", 4],
        ["tokyo", 12],
      ]);
      expect(
        answer.days.map(
          (d) =>
            `${d.date.slice(5)} ${d.kind}${d.parks.length > 0 ? ` ${d.parks[0].slug}` : ""}`,
        ),
      ).toEqual([
        "01-14 arrival",
        "01-15 park universal-studios-japan",
        "01-16 rest",
        "01-17 park universal-studios-japan",
        "01-18 travel",
        "01-19 park tokyo-disneysea",
        "01-20 rest",
        "01-21 rest",
        "01-22 park tokyo-disneyland",
        "01-23 rest",
        "01-24 rest",
        "01-25 rest",
        "01-26 park tokyo-disneyland",
        "01-27 rest",
        "01-28 rest",
        "01-29 park tokyo-disneysea",
        "01-30 departure",
      ]);
      // The same waits as the cheapest plan: TDS 70 and 100, TDL 60 twice.
      expect(answer.totals).toMatchObject({ headlinerWait: 78, parkDays: 6 });
    });

    it("calls its rest days spare and names the parks that fit them", () => {
      const rests = answer.days.filter((d) => d.kind === "rest");
      expect(rests).toHaveLength(8);
      expect(rests.every((d) => d.spare === true)).toBe(true);
      expect(answer.totals).toMatchObject({ restDays: 8, spareDays: 8 });
      // LEGOLAND Japan from Osaka on the one spare day there; Fuji-Q as a
      // day trip from Tokyo, closed on 20, 21, 27 and 28 January.
      expect(answer.spareDayParks).toEqual([
        {
          park: "fuji-q-highland",
          days: [
            { date: "2027-01-25", headlinerWait: 50, crowdLevel: "moderate" },
            { date: "2027-01-24", headlinerWait: 55, crowdLevel: "moderate" },
            { date: "2027-01-23", headlinerWait: 65, crowdLevel: "moderate" },
          ],
        },
        {
          park: "legoland-japan",
          days: [
            { date: "2027-01-16", headlinerWait: 25, crowdLevel: "moderate" },
          ],
        },
      ]);
    });
  });
});
