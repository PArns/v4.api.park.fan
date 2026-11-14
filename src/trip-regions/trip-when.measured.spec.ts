import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import {
  lastYearMonthFromDays,
  monthOf,
  type LastYearMonth,
} from "./last-year-snapshot";
import type { TripDayStatus } from "./trip-calendar.util";
import { normalizePlanQuery } from "./trip-plan.assembly";
import {
  assembleRegionCalendar,
  type InternalRegionCalendar,
  type ParkCalendarSource,
  type RegionCalendarDayLike,
} from "./trip-region.assembly";
import { findTripDates, planWindow } from "./trip-when";
import fixture from "./__fixtures__/orlando-measured-2026-11-14.json";

/**
 * Past TFT's 60 days, `/when` ranks Orlando on what was measured, not on the
 * long-range forecast (PAR-1319). On production's answers of 2026-11-14 the
 * long-range level read 42 % below the same dates of 2026 and almost flat,
 * and the windows ranked on it agreed with the measured ones at a Spearman of
 * 0.13. Here the windows of the served calendar (the forecast to 14 May
 * 2027, then last year) are ranked against those of the same calendar with
 * every forecast taken out, which reads last year on every day: past day 60
 * the two must rank alike.
 */

const TODAY = fixture.today;
const FROM = "2027-01-14"; // day 61
const TO = "2027-11-13";
const DAYS = 10;
const HEADLINERS = Object.keys(fixture.parks);

type Row = [string, string, string, number | null, number | null, number];
const STATUS: Record<string, TripDayStatus> = {
  O: "OPERATING",
  C: "CLOSED",
  U: "UNKNOWN",
};

function dayOf([date, status, crowdLevel, avg, exact, measured]: Row) {
  const day: RegionCalendarDayLike = {
    date,
    status: STATUS[status],
    crowdLevel,
  };
  if (avg != null) {
    day.headlinerForecast = {
      avgWait: avg,
      ...(exact != null && { exactWait: exact }),
      ...(measured === 1 && { actual: true }),
    };
  }
  return day;
}

/** The region calendar of [from, to] on the fixture, with or without the served forecast. */
function calendarOf(
  from: string,
  to: string,
  forecast: "served" | "none",
): InternalRegionCalendar {
  const sources: ParkCalendarSource[] = HEADLINERS.map((slug) => {
    const park = (
      fixture.parks as unknown as Record<
        string,
        { publishedHoursUntil: string; days: Row[] }
      >
    )[slug];
    const days = park.days.map(dayOf);
    const past = days.filter((d) => d.date < TODAY);
    const lastYear = new Map<string, LastYearMonth>();
    for (const ym of new Set(past.map((d) => monthOf(d.date)))) {
      lastYear.set(
        ym,
        lastYearMonthFromDays(
          past.filter((d) => monthOf(d.date) === ym),
          TODAY,
        ),
      );
    }
    const future = days
      .filter((d) => d.date >= TODAY)
      .map((d) =>
        forecast === "served" || d.headlinerForecast?.actual
          ? d
          : { ...d, headlinerForecast: undefined, crowdLevel: "unknown" },
      );
    const config = ORLANDO.parks.find((p) => p.slug === slug)!;
    return {
      park: {
        config,
        id: `id-${slug}`,
        slug,
        name: slug,
        path: `/parks/x/${slug}`,
        latitude: 0,
        longitude: 0,
        timezone: "America/New_York",
      },
      calendar: {
        meta: { scheduleCoverage: { to: park.publishedHoursUntil } },
        days: future,
      },
      rows: [],
      lastYear,
    };
  });
  return assembleRegionCalendar({
    region: ORLANDO,
    from,
    to,
    today: TODAY,
    computedAt: "t",
    sources,
    seasons: [],
    forecastUntil: [],
  });
}

const plan = normalizePlanQuery(
  ORLANDO,
  ORLANDO.parks.map((p) => p.slug),
  { from: FROM, to: addIsoDays(FROM, DAYS - 1), parks: HEADLINERS.join(",") },
  TODAY,
);

/** Every third window's plan figure (`totals.exactWait`), by start date. */
function windowFigures(calendar: InternalRegionCalendar): Map<string, number> {
  const out = new Map<string, number>();
  for (let i = 0; i + DAYS <= calendar.days.length; i += 3) {
    const from = calendar.days[i].date;
    const to = calendar.days[i + DAYS - 1].date;
    const trip = planWindow(
      ORLANDO,
      { ...plan, from, to },
      {
        ...calendar,
        meta: { ...calendar.meta, from, to },
        days: calendar.days.slice(i, i + DAYS),
      },
    );
    out.set(from, trip.plan.totals.exactWait ?? Infinity);
  }
  return out;
}

function spearman(a: Map<string, number>, b: Map<string, number>): number {
  const keys = [...a.keys()].filter((k) => b.has(k));
  const ranks = (m: Map<string, number>) => {
    const sorted = [...keys].sort((x, y) => m.get(x)! - m.get(y)!);
    return new Map(sorted.map((k, i) => [k, i]));
  };
  const ra = ranks(a);
  const rb = ranks(b);
  const n = keys.length;
  const d2 = keys.reduce((s, k) => s + (ra.get(k)! - rb.get(k)!) ** 2, 0);
  return 1 - (6 * d2) / (n * (n * n - 1));
}

describe("/when past day 60 ranks Orlando on the measured days (production, 2026-11-14)", () => {
  const served = calendarOf(FROM, TO, "served");
  const measured = calendarOf(FROM, TO, "none");

  it("reads every headliner day there on last year, the long-range forecast beside it", () => {
    const feb = served.days.find((d) => d.date === "2027-02-17")!;
    const mk = feb.parks["magic-kingdom-park"];
    expect(mk.basis).toBe("last_year");
    expect(mk.forecastWait).toBeDefined();
    expect(mk.headlinerWait).toBe(mk.lastYearWait);
    // The long-range level that used to rank the day: far below what was measured.
    expect(mk.forecastWait!).toBeLessThan(mk.lastYearWait! * 0.8);
  });

  it("ranks the windows as the measured days do (Spearman ≥ 0.99)", () => {
    const rho = spearman(windowFigures(served), windowFigures(measured));
    expect(rho).toBeGreaterThanOrEqual(0.99);
  });

  it("names the same top windows, on last year's basis", () => {
    const query = (calendar: InternalRegionCalendar) =>
      findTripDates({
        region: ORLANDO,
        calendar,
        query: { from: FROM, to: TO, days: DAYS, limit: 5, gap: DAYS, plan },
      }).windows.map((w) => ({ from: w.from, wait: w.headlinerWait }));
    const top = query(served);
    expect(top).toEqual(query(measured));
    expect(top).toHaveLength(5);
  });

  it("would not if the long-range forecast still ranked the days", () => {
    // The served calendar with each day's figure put back to the forecast's:
    // what the ranking read before PAR-1319.
    const old: InternalRegionCalendar = {
      ...served,
      days: served.days.map((d) => ({
        ...d,
        parks: Object.fromEntries(
          Object.entries(d.parks).map(([slug, c]) => [
            slug,
            c.forecastExact != null
              ? {
                  ...c,
                  waitExact: c.forecastExact,
                  headlinerWait: c.forecastWait,
                }
              : c,
          ]),
        ),
      })),
    };
    expect(spearman(windowFigures(old), windowFigures(measured))).toBeLessThan(
      0.5,
    );
  });
});
