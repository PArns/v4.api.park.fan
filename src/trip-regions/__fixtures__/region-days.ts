import { addIsoDays } from "../../common/utils/date.util";
import type { RegionConfig } from "../regions";
import type { TripParkDayInternal } from "../trip-calendar.util";
import {
  normalizePlanQuery,
  optimizePlan,
  planResponse,
} from "../trip-plan.assembly";
import type {
  InternalRegionCalendar,
  RegionParkRecord,
} from "../trip-region.assembly";

/**
 * A real region's days without a database, for the specs of the regions
 * themselves (`southern-california.spec.ts`, `paris.spec.ts`): every park open
 * 09:00–21:00 with a wait that differs by park and day, tweaked per cell.
 */

export type CellTweak = (
  slug: string,
  date: string,
  cell: TripParkDayInternal,
) => void;

/** The UTC offset of a timezone at noon on a date, as ISO writes it (`-08:00`). */
export function offsetOn(date: string, timezone: string): string {
  const name = new Intl.DateTimeFormat("en-US", {
    timeZone: timezone,
    timeZoneName: "longOffset",
  })
    .formatToParts(new Date(`${date}T12:00:00Z`))
    .find((p) => p.type === "timeZoneName")?.value;
  const m = /GMT([+-]\d{2}:\d{2})?/.exec(name ?? "");
  return m?.[1] ?? "+00:00";
}

/** The days of a region from `from`, `days` long. */
export function regionCalendar(
  region: RegionConfig,
  from: string,
  days: number,
  tweak: CellTweak = () => undefined,
): InternalRegionCalendar {
  const out: InternalRegionCalendar["days"] = [];
  for (let i = 0; i < days; i++) {
    const date = addIsoDays(from, i);
    const offset = offsetOn(date, region.timezone);
    const parks: Record<string, TripParkDayInternal> = {};
    region.parks.forEach((p, k) => {
      const wait = 30 + ((k * 7 + i * 11) % 25);
      const cell: TripParkDayInternal = {
        status: "OPERATING",
        hoursPublished: true,
        crowdLevel: "moderate",
        headlinerWait: Math.round(wait / 5) * 5,
        waitExact: wait,
        basis: "forecast",
        dayHours: 12,
        usualHours: 12,
        opensAt: `${date}T09:00:00${offset}`,
        closesAt: `${date}T21:00:00${offset}`,
      };
      tweak(p.slug, date, cell);
      parks[p.slug] = cell;
    });
    out.push({ date, isHoliday: false, isSchoolVacation: false, parks });
  }
  return {
    meta: {
      region: region.slug,
      timezone: region.timezone,
      from,
      to: addIsoDays(from, days - 1),
      computedAt: "t",
      publishedHoursUntil: {},
    },
    parks: region.parks.map((p) => p.slug),
    days: out,
    seasons: [],
  };
}

/** The calendar days from `from` to `to`, both included. */
export function daysBetween(from: string, to: string): number {
  return (
    Math.round(
      (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
        86_400_000,
    ) + 1
  );
}

/** The parts of a plan answer these specs read. */
export interface PlanAnswer {
  meta: Record<string, unknown> & {
    stays?: Array<Record<string, unknown>>;
    ticketSuggestion?: Array<{
      resort: string;
      product: string;
      days?: number;
      extraDays?: number;
    }>;
  };
  days: Array<
    Record<string, unknown> & {
      date: string;
      kind: string;
      parks: Array<{ slug: string }>;
      perks?: string[];
      warnings: string[];
    }
  >;
  unplaced: Array<{ slug: string; reason: string }>;
  totals: Record<string, unknown>;
  spareDayParks?: Array<{ park: string; days: Array<Record<string, unknown>> }>;
}

/** The days of a longer calendar from `from` to `to`, as a calendar of their own. */
export function sliceCalendar(
  calendar: InternalRegionCalendar,
  from: string,
  to: string,
): InternalRegionCalendar {
  return {
    ...calendar,
    meta: { ...calendar.meta, from, to },
    days: calendar.days.filter((d) => d.date >= from && d.date <= to),
  };
}

/**
 * A plan for a query on the region's days, as the route answers it: on the
 * fixture's own days, or on `calendar`'s for the trip's dates.
 */
export function regionPlan(
  region: RegionConfig,
  query: Record<string, string>,
  today: string,
  tweak?: CellTweak,
  from?: InternalRegionCalendar,
): PlanAnswer {
  const q = normalizePlanQuery(
    region,
    region.parks.map((p) => p.slug),
    query as never,
    today,
  );
  const calendar = from
    ? sliceCalendar(from, q.from, q.to)
    : regionCalendar(region, q.from, daysBetween(q.from, q.to), tweak);
  const trip = optimizePlan(region, q, calendar);
  return planResponse(region, q, calendar, trip, {
    computedAt: "t",
    today,
    leadMae: new Map(),
  }) as unknown as PlanAnswer;
}

/** The region's parks as resolved rows, for the region route. */
export function regionRecords(region: RegionConfig): RegionParkRecord[] {
  return region.parks.map((config, i) => ({
    config,
    id: `id-${i}`,
    slug: config.slug,
    name: config.slug,
    path: `/parks/${config.continent}/${config.country}/${config.city}/${config.slug}`,
    latitude: 0,
    longitude: 0,
    timezone: region.timezone,
  }));
}
