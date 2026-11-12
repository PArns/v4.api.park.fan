/**
 * `GET /v1/trip-regions/:region/best-days`: per park its quietest and busiest
 * days of a window, per resort the quietest dates to go at all. Read off the
 * region calendar, so it says nothing the calendar does not. Pure.
 *
 * Ranked on the plan's own day cost (`dayCost`): the unrounded headliner wait
 * (`headlinerWait` is rounded to five minutes, and a ranking decided by that
 * rounding is a tie the rounding invented; park.fan,
 * docs/rules/the-quietest-weekday-is-the-parks-quiet-end.md), dearer where the
 * day guests must leave early. A night the day guests lose to a party ranks
 * behind every full day whatever its waits.
 */
import { addIsoDays } from "../common/utils/date.util";
import type { RegionConfig } from "./regions";
import type { TripBasis, TripParkDayInternal } from "./trip-calendar.util";
import {
  planCellOf,
  rankWait,
  windowMedianHours,
  type InternalCalendarDay,
  type InternalRegionCalendar,
} from "./trip-region.assembly";
import { dayCost, losesEvening } from "./trip-optimizer";

/** A flat window: the rounded waits spread less than this. */
export const FLAT_SPREAD_MINUTES = 10;
/** Busiest days named per park. */
const BUSY_DAYS = 2;
/** Quietest dates named per resort. */
const GROUP_QUIET_DATES = 3;

export type BestDayReason =
  | "weekday"
  | "holiday_ends"
  | "school_break"
  | "party_night_short_hours"
  | "closes_early"
  | "extended_evening"
  | "early_entry"
  | "early_park_admission"
  | "long_range"
  | "quietest_in_window"
  | "estimate_last_year";

export interface BestDay {
  date: string;
  headlinerWait: number;
  crowdLevel: string;
  basis: TripBasis;
  reasons: BestDayReason[];
}

export interface TripBestDaysPark {
  slug: string;
  /** The basis most of the park's ranked days have. */
  basis: TripBasis;
  quiet: BestDay[];
  busy: BestDay[];
  /** Set when the window's rounded waits spread less than ten minutes. */
  flat?: { min: number; max: number };
}

export interface TripBestDays {
  meta: {
    region: string;
    timezone: string;
    from: string;
    to: string;
    computedAt: string;
    forecastUntil?: string;
  };
  parks: TripBestDaysPark[];
  groups: Array<{
    key: string;
    quiet: Array<{ date: string; slug: string; headlinerWait: number }>;
  }>;
}

const BASIS_ORDER: TripBasis[] = [
  "forecast",
  "long_range",
  "last_year",
  "none",
];

const weekdayOf = (date: string): number =>
  new Date(`${date}T12:00:00Z`).getUTCDay();

interface Candidate {
  date: string;
  day: TripParkDayInternal;
  wait: number;
  /** `dayCost`: what the ranking reads. */
  cost: number;
  /** The day guests lose the evening to a ticketed event. */
  shortened: boolean;
}

/** The region's best days over a calendar window. */
export function regionBestDays(
  region: RegionConfig,
  calendar: InternalRegionCalendar,
  limit: number,
): TripBestDays {
  const dayByDate = new Map(calendar.days.map((d) => [d.date, d]));
  const medianHours = new Map(
    calendar.parks.map((slug) => [
      slug,
      windowMedianHours(calendar.days, slug),
    ]),
  );
  const parks = calendar.parks.map((slug) =>
    parkBestDays(slug, calendar.days, dayByDate, limit),
  );

  const groups = region.groups
    .filter((g) => g.kind === "resort")
    .map((g) => ({
      key: g.key,
      quiet: calendar.days
        .flatMap((d) => {
          const slug = d.best?.[g.key];
          const park = slug ? d.parks[slug] : undefined;
          if (!slug || park?.headlinerWait == null) return [];
          const wait = dayCost(planCellOf(park), {
            type: "theme_park",
            medianHours: medianHours.get(slug),
          });
          return [
            { date: d.date, slug, headlinerWait: park.headlinerWait, wait },
          ];
        })
        .sort((a, b) => a.wait - b.wait || a.date.localeCompare(b.date))
        .slice(0, GROUP_QUIET_DATES)
        .map(({ wait: _wait, ...entry }) => entry),
    }));

  return {
    meta: {
      region: calendar.meta.region,
      timezone: calendar.meta.timezone,
      from: calendar.meta.from,
      to: calendar.meta.to,
      computedAt: calendar.meta.computedAt,
      ...(calendar.meta.forecastUntil && {
        forecastUntil: calendar.meta.forecastUntil,
      }),
    },
    parks,
    groups,
  };
}

function parkBestDays(
  slug: string,
  days: InternalCalendarDay[],
  dayByDate: Map<string, InternalCalendarDay>,
  limit: number,
): TripBestDaysPark {
  const all = days.map((d) => ({ date: d.date, day: d.parks[slug] }));
  const medianHours = windowMedianHours(days, slug);
  const candidates: Candidate[] = all.flatMap(({ date, day }) => {
    const wait = day ? rankWait(day) : undefined;
    if (!day || day.status === "CLOSED" || wait === undefined) return [];
    if (day.headlinerWait == null) return [];
    const cell = planCellOf(day);
    const cost = dayCost(
      { ...cell, status: "OPERATING" },
      { type: "theme_park", medianHours },
    );
    const shortened = losesEvening(cell, medianHours);
    return [{ date, day, wait, cost, shortened }];
  });

  const quietOrder = [...candidates].sort(
    (a, b) =>
      Number(a.shortened) - Number(b.shortened) ||
      a.cost - b.cost ||
      (b.day.dayHours ?? 0) - (a.day.dayHours ?? 0) ||
      a.date.localeCompare(b.date),
  );
  const quietest = quietOrder.find((c) => !c.shortened) ?? quietOrder[0];
  const busyOrder = [...candidates].sort(
    (a, b) => b.cost - a.cost || a.date.localeCompare(b.date),
  );

  const describe = (c: Candidate): BestDay => ({
    date: c.date,
    headlinerWait: c.day.headlinerWait as number,
    crowdLevel: c.day.crowdLevel,
    basis: c.day.basis,
    reasons: reasonsFor(c, dayByDate, c === quietest),
  });

  const counts = new Map<TripBasis, number>();
  for (const c of candidates) {
    counts.set(c.day.basis, (counts.get(c.day.basis) ?? 0) + 1);
  }
  const basis =
    [...BASIS_ORDER].sort(
      (a, b) => (counts.get(b) ?? 0) - (counts.get(a) ?? 0),
    )[0] ?? "none";

  const rounded = candidates.map((c) => c.day.headlinerWait as number);
  const min = Math.min(...rounded);
  const max = Math.max(...rounded);
  return {
    slug,
    basis: candidates.length > 0 ? basis : "none",
    quiet: quietOrder.slice(0, limit).map(describe),
    busy: busyOrder.slice(0, BUSY_DAYS).map(describe),
    ...(candidates.length > 1 &&
      max - min < FLAT_SPREAD_MINUTES && { flat: { min, max } }),
  };
}

function reasonsFor(
  c: Candidate,
  dayByDate: Map<string, InternalCalendarDay>,
  quietest: boolean,
): BestDayReason[] {
  const reasons: BestDayReason[] = [];
  const info = dayByDate.get(c.date);
  const weekday = weekdayOf(c.date);
  if (weekday >= 1 && weekday <= 5 && !info?.isHoliday) reasons.push("weekday");
  const previous = dayByDate.get(addIsoDays(c.date, -1));
  if (
    previous &&
    (previous.isHoliday || previous.isSchoolVacation) &&
    !info?.isHoliday &&
    !info?.isSchoolVacation
  ) {
    reasons.push("holiday_ends");
  }
  if (info?.isSchoolVacation) reasons.push("school_break");
  if (c.shortened) reasons.push("party_night_short_hours");
  else if (c.day.closesEarly) reasons.push("closes_early");
  if (c.day.evening?.kind === "extended_evening") {
    reasons.push("extended_evening");
  }
  if (c.day.early) reasons.push(c.day.early.kind);
  if (c.day.basis === "long_range") reasons.push("long_range");
  if (c.day.basis === "last_year") reasons.push("estimate_last_year");
  if (quietest) reasons.push("quietest_in_window");
  return reasons;
}
