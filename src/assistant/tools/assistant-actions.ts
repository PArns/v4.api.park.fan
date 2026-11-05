import { DataSource, In } from "typeorm";
import { formatInTimeZone } from "date-fns-tz";
import { z } from "zod";
import { McpToolError, type McpToolContext } from "../../mcp/mcp-tool";
import { isAmbiguous, resolvePark } from "../../mcp/tools/park-ref";
import { getFrontendUrl } from "../../config/oauth.config";
import { Attraction } from "../../attractions/entities/attraction.entity";
import type { Park } from "../../parks/entities/park.entity";
import { ASSISTANT_LOCALES, type AssistantLocale } from "../system-prompt";

/**
 * Parky's actions on the page: what the browser does for it, never the API. A tool returns an
 * {@link ActionOutcome}; the chat sends the action to the widget as an `action` event, which
 * checks it again and carries it out, and the model only reads `result`.
 */

/** The park as the trip planner files it. */
export interface PlannerPark {
  slug: string;
  name: string;
  geo: { continent: string; country: string; city: string };
  timezone: string;
}

/** What the widget may be asked to do. */
/** A card the widget draws in the chat, from live data, with the page it links to. */
export type AssistantCard =
  | {
      kind: "park";
      name: string;
      place: string | null;
      status: string | null;
      crowdLevel: string | null;
      opens: string | null;
      closes: string | null;
      waits: Array<{ name: string; waitMinutes: number }>;
      url: string | null;
    }
  | {
      kind: "ride";
      name: string;
      park: string | null;
      status: string | null;
      crowdLevel: string | null;
      waitMinutes: number | null;
      url: string | null;
      /** Each badge only when true or known; absent never means "no". */
      ropeDrop?: { strength: "high" | "moderate" | null; savings: number };
      singleRider?: true;
      virtualLine?: true;
      paidExpress?: true;
      /** In a curated works period today. */
      worksPeriod?: true;
      outOfSeason?: true;
    }
  | {
      kind: "rides";
      /** The park the list belongs to, its page the card's link. */
      park: string;
      order: "longest" | "shortest";
      /** The waiting rides, then up to 3 that are DOWN (waitMinutes null). */
      rides: Array<{
        name: string;
        status: string | null;
        waitMinutes: number | null;
        url: string | null;
      }>;
      url: string | null;
    }
  | {
      kind: "shows";
      park: string;
      url: string | null;
      /** YYYY-MM-DD, park-local. */
      date: string;
      /** At most 10, by next time; times with the park's offset, never Z. */
      shows: Array<{
        name: string;
        status: string | null;
        times: string[];
        url: string | null;
      }>;
    }
  | {
      kind: "restaurants";
      park: string;
      url: string | null;
      /** At most 12. */
      restaurants: Array<{
        name: string;
        status: string | null;
        land: string | null;
        cuisine: string | null;
        requiresReservation: boolean | null;
      }>;
    }
  | {
      kind: "parks";
      parks: Array<{
        name: string;
        place: string | null;
        url: string | null;
        status: string | null;
        crowdLevel: string | null;
        avgWaitMinutes: number | null;
      }>;
    }
  | {
      kind: "calendar";
      park: string;
      /** The park's page; the widget appends its localized calendar subpage. */
      url: string | null;
      /** Consecutive park-local days, 7 to 42. */
      days: CalendarCardDay[];
    }
  | {
      kind: "tiles";
      park: string;
      url: string | null;
      /** The park-local day the tiles describe. */
      date: string;
      /** True: right now, today. False: the forecast for `date`. */
      live: boolean;
      crowdLevel: string | null;
      /** Live only. */
      avgWaitMinutes: number | null;
      /** Live: the peak so far today. Forecast: the expected headliner peak. */
      peakWaitMinutes: number | null;
      /** Live only. */
      openRides: number | null;
      /** Live only. */
      totalRides: number | null;
      opens: string | null;
      closes: string | null;
      weather: {
        condition: string | null;
        /** The WMO code. */
        weatherCode: number | null;
        tempMin: number | null;
        tempMax: number | null;
        climateNormal: boolean;
      } | null;
    };

export interface CalendarCardDay {
  /** YYYY-MM-DD, park-local. */
  date: string;
  status: "OPERATING" | "CLOSED" | "UNKNOWN";
  /** The calendar's level, `unknown` included; null when closed. */
  crowdLevel: string | null;
  /** ISO 8601 with the park's offset. */
  opens: string | null;
  closes: string | null;
  /** A public or school holiday the calendar counts for this park. */
  holiday: boolean;
}

/** The fields of `park_forecast`'s answer the calendar and tiles cards read. */
export interface ForecastSummary {
  park: { name: string; pageUrl: string | null; timezone: string };
  scheduleCoverage: { from: string | null; to: string | null };
  days: Array<{
    date: string;
    status: string;
    opens: string | null;
    closes: string | null;
    crowdLevel: string;
    expectedPeakWaitMinutes?: number;
    holiday?: boolean;
    schoolVacation?: boolean;
    weather?: {
      basis?: string;
      condition?: string | null;
      weatherCode?: number | null;
      tempMin?: number | null;
      tempMax?: number | null;
    } | null;
  }>;
}

/**
 * One forecast day as the cards show it. Past the published schedule the status is the calendar's
 * guess, so it reads UNKNOWN and carries no hours (docs/rules/absent-facts.md).
 */
function cardDay(
  day: ForecastSummary["days"][number],
  coverage: ForecastSummary["scheduleCoverage"],
  timezone: string,
): CalendarCardDay {
  const published = coverage.to !== null && day.date <= coverage.to;
  const status = !published
    ? "UNKNOWN"
    : day.status === "OPERATING" || day.status === "CLOSED"
      ? day.status
      : "UNKNOWN";
  return {
    date: day.date,
    status,
    crowdLevel:
      status === "CLOSED" || day.crowdLevel === "closed"
        ? null
        : day.crowdLevel,
    opens: published ? parkInstant(day.opens, timezone) : null,
    closes: published ? parkInstant(day.closes, timezone) : null,
    holiday: !!(day.holiday || day.schoolVacation),
  };
}

/**
 * An instant with the park's own UTC offset (`+00:00`, never `Z`): the widget reads the park's
 * wall clock off the offset.
 */
export function parkInstant(
  value: string | null | undefined,
  timezone: string,
): string | null {
  if (!value) return null;
  const at = new Date(value);
  return Number.isNaN(at.getTime())
    ? null
    : formatInTimeZone(at, timezone, "yyyy-MM-dd'T'HH:mm:ssxxx");
}

/** The calendar card: `count` days of the forecast from `from` on. */
export function calendarCard(
  forecast: ForecastSummary,
  from: string,
  count: number,
): Extract<AssistantCard, { kind: "calendar" }> {
  return {
    kind: "calendar",
    park: forecast.park.name,
    url: forecast.park.pageUrl ? siteOnlyPath(forecast.park.pageUrl) : null,
    days: forecast.days
      .filter((day) => day.date >= from)
      .slice(0, count)
      .map((day) =>
        cardDay(day, forecast.scheduleCoverage, forecast.park.timezone),
      ),
  };
}

/** The fields of `get_park`'s summary the live tiles read. */
export interface LiveParkSummary {
  crowd?: {
    level?: string | null;
    avgWaitMinutes?: number | null;
    peakWaitTodayMinutes?: number | null;
  } | null;
  scheduleToday?: Array<{ opens?: string | null; closes?: string | null }>;
  rides?: { total?: number; byStatus?: Record<string, number> };
}

/**
 * The tiles card for `date`: the live state when `live` is given (today), else the forecast.
 * Levels are passed on as the calendar or the live reading serves them, never re-derived.
 */
export function tilesCard(
  forecast: ForecastSummary,
  date: string,
  live: LiveParkSummary | null,
): Extract<AssistantCard, { kind: "tiles" }> {
  const found = forecast.days.find((day) => day.date === date);
  const timezone = forecast.park.timezone;
  const day = found
    ? cardDay(found, forecast.scheduleCoverage, timezone)
    : null;
  const weather = found?.weather;
  const minutes = (value: number | null | undefined) =>
    value != null ? Math.round(value) : null;
  const today = live?.scheduleToday?.[0];
  return {
    kind: "tiles",
    park: forecast.park.name,
    url: forecast.park.pageUrl ? siteOnlyPath(forecast.park.pageUrl) : null,
    date,
    live: live !== null,
    crowdLevel: live ? (live.crowd?.level ?? null) : (day?.crowdLevel ?? null),
    avgWaitMinutes: live ? minutes(live.crowd?.avgWaitMinutes) : null,
    peakWaitMinutes: live
      ? minutes(live.crowd?.peakWaitTodayMinutes)
      : day?.crowdLevel != null
        ? minutes(found?.expectedPeakWaitMinutes)
        : null,
    openRides:
      live?.rides?.total != null ? (live.rides.byStatus?.OPERATING ?? 0) : null,
    totalRides: live?.rides?.total ?? null,
    opens: live ? parkInstant(today?.opens, timezone) : (day?.opens ?? null),
    closes: live ? parkInstant(today?.closes, timezone) : (day?.closes ?? null),
    weather: weather
      ? {
          condition: weather.condition ?? null,
          weatherCode: weather.weatherCode ?? null,
          tempMin: weather.tempMin ?? null,
          tempMax: weather.tempMax ?? null,
          climateNormal: weather.basis === "climate_normal",
        }
      : null,
  };
}

export type ClientAction =
  | { type: "navigate"; path: string }
  | { type: "card"; card: AssistantCard }
  | { type: "request_location" }
  | { type: "planner_open_day"; park: PlannerPark; date: string }
  | {
      type: "planner_add_rides";
      park: PlannerPark;
      date: string;
      rides: Array<{ slug: string; name: string }>;
    }
  | {
      type: "planner_add_block";
      park: PlannerPark;
      date: string;
      block: {
        label: string;
        icon: PlannerBlockIcon;
        startMinute?: number;
        durationMinutes: number;
      };
    }
  | {
      type: "planner_optimize";
      park: PlannerPark;
      date: string;
      mode: "order" | "headliners";
    }
  /** Entry ids are always ones the browser sent in the chat's planner snapshot. */
  | {
      type: "planner_remove";
      park: PlannerPark;
      date: string;
      entryIds: string[];
    }
  | {
      type: "planner_move";
      park: PlannerPark;
      date: string;
      entryId: string;
      toIndex: number;
    }
  | {
      type: "planner_update_block";
      park: PlannerPark;
      date: string;
      entryId: string;
      label?: string;
      /** HH:MM, the park's clock. */
      start?: string;
      durationMinutes?: number;
    }
  | {
      type: "planner_add_show";
      park: PlannerPark;
      date: string;
      showSlug: string;
      /** HH:MM, the park's clock, one of the show's times that day. */
      start: string;
    }
  | {
      type: "planner_set_prefs";
      park: PlannerPark;
      date: string;
      prefs: {
        riderHeightCm?: number;
        avoidWet?: boolean;
        earlyEntry?: boolean;
      };
    }
  | {
      type: "planner_mark_done";
      park: PlannerPark;
      date: string;
      entryIds: string[];
      done: boolean;
    }
  | { type: "planner_clear_day"; park: PlannerPark; date: string }
  | { type: "planner_reserve_days"; park: PlannerPark; dates: string[] }
  | { type: "planner_undo"; park: PlannerPark; date: string }
  | { type: "planner_share" }
  /** The same page in another of the site's languages, carried out once the answer is complete. */
  | { type: "switch_locale"; locale: AssistantLocale }
  /** Parky stored the visitor's feedback; the widget shows it under the answer. */
  | { type: "feedback_sent" };

/** The trip planner's block icons (park.fan `PLANNER_BLOCK_ICONS`). */
export const PLANNER_BLOCK_ICONS = [
  "break",
  "food",
  "show",
  "shop",
  "photo",
  "meet",
  "star",
] as const;
export type PlannerBlockIcon = (typeof PLANNER_BLOCK_ICONS)[number];

/** "HH:MM" in the park's clock as minutes since midnight. */
export const clockTime = z
  .string()
  .regex(/^([01]\d|2[0-3]):[0-5]\d$/)
  .transform((value) => {
    const [hours, minutes] = value.split(":").map(Number);
    return hours * 60 + minutes;
  });

/** A tool result that also asks the widget to act. */
export class ActionOutcome {
  constructor(
    readonly action: ClientAction,
    readonly result: unknown,
  ) {}
}

/** A park.fan path of slugs, from an absolute park.fan URL or a path. */
const PAGE_PATH = /^\/[a-z0-9\-/]*$/;

/** The path of a park.fan page, or null for anything else (another host, a query, a scheme). */
export function siteOnlyPath(url: string): string | null {
  const site = new URL(getFrontendUrl());
  let parsed: URL;
  try {
    parsed = new URL(url, site);
  } catch {
    return null;
  }
  const host = parsed.hostname.replace(/^www\./, "");
  if (host !== site.hostname.replace(/^www\./, "")) return null;
  if (parsed.search || parsed.username || parsed.password) return null;
  const path = parsed.pathname.toLowerCase();
  return PAGE_PATH.test(path) && path.length <= 300 ? path : null;
}

/**
 * Every park and ride page url in `value`, at any depth, in the chat's language: the handlers
 * build `https://park.fan/parks/…`, which would link a Dutch answer to the English page. Only
 * `/parks/` is rewritten; its localized subpages (the calendar's `calendrier-temps-attente`)
 * are not built here. Cards carry site-only paths, which the widget localizes itself.
 */
export function localizePageUrls(value: unknown, locale: string): unknown {
  if (!(ASSISTANT_LOCALES as readonly string[]).includes(locale)) return value;
  const parks = `${getFrontendUrl()}/parks/`;
  // LocationService hands out API paths (`/v1/parks/<geo>/<park>[/<ride>]`), which mirror the
  // page paths one to one; as they stand the model would link the JSON.
  const apiParks = "/v1/parks/";
  const visit = (item: unknown): unknown => {
    if (typeof item === "string") {
      if (item.startsWith(parks)) {
        return `${getFrontendUrl()}/${locale}/parks/${item.slice(parks.length)}`;
      }
      return item.startsWith(apiParks)
        ? `${getFrontendUrl()}/${locale}/parks/${item.slice(apiParks.length)}`
        : item;
    }
    if (Array.isArray(item)) return item.map(visit);
    if (item && typeof item === "object") {
      return Object.fromEntries(
        Object.entries(item).map(([key, entry]) => [key, visit(entry)]),
      );
    }
    return item;
  };
  return visit(value);
}

/** A day as the planner keys it, YYYY-MM-DD, no further back than yesterday and within a year. */
export const planDate = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .refine((value) => {
    const day = Date.parse(`${value}T00:00:00Z`);
    const now = Date.now();
    return day > now - 2 * 86_400_000 && day < now + 366 * 86_400_000;
  }, "must be a day within the next year")
  .describe("The day in the park's local calendar, YYYY-MM-DD");

export function plannerPark(park: Park): PlannerPark {
  if (!park.continentSlug || !park.countrySlug || !park.citySlug) {
    throw new McpToolError(`${park.name} has no page to plan on`);
  }
  return {
    slug: park.slug,
    name: park.name,
    geo: {
      continent: park.continentSlug,
      country: park.countrySlug,
      city: park.citySlug,
    },
    timezone: park.timezone,
  };
}

/** The park for a planner action, or the candidates when the reference is ambiguous. */
export async function planPark(ctx: McpToolContext, reference: string) {
  const found = await resolvePark(ctx, reference);
  return isAmbiguous(found) ? found : plannerPark(found);
}

/** The rides of one park by id, in the order asked, refusing ids of other parks. */
export async function parkRides(
  ctx: McpToolContext,
  parkSlug: string,
  ids: string[],
): Promise<Array<{ slug: string; name: string }>> {
  const rides = await ctx
    .get(DataSource)
    .getRepository(Attraction)
    .find({
      where: { id: In(ids.map((id) => id.toLowerCase())) },
      relations: { park: true },
    });
  const byId = new Map(rides.map((ride) => [ride.id, ride]));
  return ids.map((id) => {
    const ride = byId.get(id.toLowerCase());
    if (!ride || ride.park?.slug !== parkSlug) {
      throw new McpToolError(`Ride ${id} is not in this park; use find_rides`);
    }
    return { slug: ride.slug, name: ride.name };
  });
}
