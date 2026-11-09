import type { Type } from "@nestjs/common";
import type Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { McpToolError, type McpToolContext } from "../../mcp/mcp-tool";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { findParksTool } from "../../mcp/tools/find-parks.tool";
import { getParkTool } from "../../mcp/tools/get-park.tool";
import type {
  FunFactLocale,
  FunFacts,
} from "../../parks/utils/curated-park-facts.util";
import { findAttractionsTool } from "../../mcp/tools/find-attractions.tool";
import { getAttractionTool } from "../../mcp/tools/get-attraction.tool";
import {
  compactDay,
  parkForecastTool,
} from "../../mcp/tools/park-forecast.tool";
import { CalendarService } from "../../parks/services/calendar.service";
import { parseDateRange } from "../../common/utils/date-parsing.util";
import { isAmbiguous, resolvePark } from "../../mcp/tools/park-ref";
import { attractionPageUrl, parkPageUrl } from "../../mcp/tools/links";
import { BestDaysService } from "../../parks/services/best-days.service";
import { ParksService } from "../../parks/parks.service";
import { ShowsService } from "../../shows/shows.service";
import type { Show } from "../../shows/entities/show.entity";
import type { ShowLiveData } from "../../shows/entities/show-live-data.entity";
import { isCurrentlyInSeason } from "../../attractions/utils/curated-attraction-facts.util";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { WeatherService } from "../../parks/weather.service";
import { PlanDayService } from "../../parks/services/plan-day.service";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { DataSource, In, IsNull, type EntityTarget } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { Show as ShowEntity } from "../../shows/entities/show.entity";
import { Restaurant } from "../../restaurants/entities/restaurant.entity";
import { FavoritesService } from "../../favorites/favorites.service";
import type { FavoritesResponseDto } from "../../favorites/dto/favorites-response.dto";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { worksPeriodOn } from "../../attractions/utils/curated-out-of-service.util";
import { getNoLiveWaitTimesReason } from "../../parks/data/live-wait-time-sources";
import {
  RIDE_ALERT_THRESHOLD_MAX,
  RIDE_ALERT_THRESHOLD_MIN,
} from "../../ride-alerts/dto/ride-alert.dto";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import {
  compareRides,
  comparedRides,
  isRideOfType,
  RIDE_RANKINGS,
  RIDE_TYPE_FILTERS,
} from "../../attractions/utils/ride-facts.util";
import { getCountryISOCode } from "../../common/constants/country-codes.constant";
import { AssistantPostsClient } from "./assistant-posts.client";
import { getFrontendUrl } from "../../config/oauth.config";
import {
  bookableHours,
  localizeTimes,
  parkClockInstant,
  parkHourInstant,
} from "../assistant-time";
import type { PlanDayDto, PlanDayRideDto } from "../../parks/dto/plan-day.dto";
import { formatInTimeZone } from "date-fns-tz";
import { unfoldedCloseHour } from "../../common/utils/day-shape.util";
import { LocationService } from "../../location/location.service";
import { GeoipService } from "../../geoip/geoip.service";
import { ASSISTANT_LOCALES, languageName } from "../system-prompt";
import { junkReason } from "../assistant-junk";
import { AssistantRecordsService } from "../assistant-records.service";
import { AssistantRateLimitService } from "../assistant-rate-limit.service";
import { FEEDBACK_KINDS } from "../entities/site-feedback.entity";
import { AssistantConversation } from "../entities/assistant-conversation.entity";

/** How long a session may not chat after Parky ended one of its chats. */
export const CHAT_END_COOLDOWN_MINUTES = 15;

/** What `end_chat` may name as the reason; nothing milder qualifies. */
export const CHAT_END_REASONS = [
  "insults",
  "harassment",
  "slurs",
  "sexual_harassment",
  "hate_bait",
] as const;
import {
  ActionOutcome,
  calendarCard,
  clockTime,
  parkInstant,
  tilesCard,
  type ForecastSummary,
  type LiveParkSummary,
  localizePageUrls,
  parkRides,
  planDate,
  PLANNER_BLOCK_ICONS,
  planPark,
  plannerPark,
  siteOnlyPath,
  type AssistantCard,
  type ClientAction,
} from "./assistant-actions";
import {
  plannerClock,
  plannerEntryId,
  plannerPrefs,
  type PlannerDay,
  type PlannerSnapshot,
  type AlertsSnapshot,
  FAVORITE_ENTITIES,
  type FavoriteEntity,
  type FavoritesSnapshot,
} from "../assistant.schemas";

/** The fields of `get_park`'s summary a card reads. */
type ParkSummary =
  | { ambiguous: true }
  | {
      name: string;
      city?: string | null;
      country?: string | null;
      status?: string | null;
      pageUrl?: string | null;
      crowd?: { level?: string | null; avgWaitMinutes?: number | null } | null;
      scheduleToday?: Array<{ opens?: string | null; closes?: string | null }>;
      nextSchedule?: unknown;
      rides?: {
        longestWaits?: Array<{ id: string; name: string; waitMinutes: number }>;
        down?: Array<{ id: string; name: string }>;
      };
    };

/**
 * The ride card's optional badges: each key only when it is true or known, so an absent one
 * means "nobody said", never "no" (docs/rules/absent-facts.md).
 */
export function rideBadges(ride: RideSummary) {
  const facts = ride.facts ?? {};
  const fastPass = facts.fastPass as { price?: number | null } | null;
  const worksPeriod = ride.worksPeriod as { runningToday?: boolean } | null;
  return {
    ...(ride.ropeDrop?.worth && {
      ropeDrop: {
        strength: ride.ropeDrop.strength,
        savings: ride.ropeDrop.savingsMinutes,
      },
    }),
    ...(facts.hasSingleRider === true && { singleRider: true as const }),
    ...(facts.hasVirtualLine === true && { virtualLine: true as const }),
    // A price of 0 is a free product (Europa-Park's Virtual Line), not a paid lane.
    ...(fastPass && fastPass.price !== 0 && { paidExpress: true as const }),
    ...(worksPeriod?.runningToday === true && { worksPeriod: true as const }),
    ...(facts.isCurrentlyInSeason === false && { outOfSeason: true as const }),
  };
}

/** Most rides that are down a rides card lists after the waits. */
const MAX_DOWN_ROWS = 3;

/** The fields of `get_attraction`'s answer a card reads. */
interface RideSummary {
  name: string;
  park?: { name: string } | null;
  status?: string | null;
  effectiveStatus?: string | null;
  crowdLevel?: string | null;
  pageUrl?: string | null;
  queues?: Array<{ type: string; waitMinutes: number | null }>;
  worksPeriod?: unknown;
  facts?: {
    hasSingleRider?: boolean | null;
    hasVirtualLine?: boolean | null;
    fastPass?: unknown;
    isCurrentlyInSeason?: boolean | null;
  };
  ropeDrop?: {
    worth: boolean;
    strength: "high" | "moderate" | null;
    savingsMinutes: number;
  } | null;
}

/**
 * Everything Parky may call: read-only, public park data and park.fan's own
 * posts, actions the browser carries out (a card, a park.fan page, the trip
 * planner, the site's language), and one write, the visitor's feedback in their
 * own words. No web search, no fetching of arbitrary URLs, no admin data.
 *
 * The park tools reuse the admin MCP server's handlers, which already return
 * compact, model-oriented summaries; the descriptions here are Parky's own,
 * since the MCP ones point at admin tools Parky does not have.
 */

/**
 * The MCP handlers take a context with a principal, which none of the reused
 * ones reads. This one names the caller and holds the least role.
 */
const PARKY: AdminPrincipal = {
  userId: null,
  email: "parky@park.fan",
  displayName: "Parky",
  role: "viewer",
  sessionToken: null,
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
};

/** What a tool run gets: the app's providers, the chat's language and where the visitor is. */
export interface AssistantToolContext {
  get<T>(token: Type<T>): T;
  locale: string;
  /** The caller's address, for a GeoIP guess; never passed to the model. */
  ip?: string | null;
  /** The position the visitor shared in this question, rounded to about a kilometre. */
  location?: { lat: number; lng: number } | null;
  /** The browser's IANA zone; every instant in a result is shown in it. */
  timeZone?: string;
  /** The visitor's trip planner as the browser sent it; the only source of entry ids. */
  planner?: PlannerSnapshot | null;
  /** The browser's push state and alerts as it sent them. */
  alerts?: AlertsSnapshot | null;
  /** The ids of the favourites as the browser sent them. */
  favorites?: FavoritesSnapshot | null;
  /**
   * The chat this answer belongs to, all of it from the request and the server's own records:
   * what `submit_feedback` stores comes from here, never from the model's input.
   */
  chat?: {
    conversationId: string;
    /** The page the question was asked on, a checked path of slugs. */
    pagePath: string;
    session: string;
    sessionHash: string;
    /** The question being answered and the one before it, as the visitor typed them. */
    message: string;
    previousMessage: string | null;
  };
  /** This answer so far; without it, the tools that act once per answer refuse. */
  answer?: AnswerState;
}

/** One answer's progress: its round of tool calls, and the once-per-answer tools that ran. */
export interface AnswerState {
  round: number;
  done: Set<string>;
}

/**
 * Claims the one run per answer of a tool that acts, so neither the model nor text it read can
 * repeat it in a loop, and two calls in one round cannot both pass. Throws, as a refusal the model
 * reads, when it was claimed already or there is no answer to count in. `release` hands the run
 * back after a refusal that sent nothing.
 */
function claimOnce(
  ctx: AssistantToolContext,
  name: string,
): { round: number; release: () => void } {
  const answer = ctx.answer;
  if (!answer) throw new McpToolError(`${name} is not available here`);
  if (answer.done.has(name)) {
    throw new McpToolError(`${name} already ran in this answer`);
  }
  answer.done.add(name);
  return { round: answer.round, release: () => answer.done.delete(name) };
}

/** Shortest feedback worth storing, after trimming: "ok" and "yes, do it" say nothing. */
const MIN_FEEDBACK_CHARS = 12;
/** As long as the feedback form lets a visitor write. */
const MAX_FEEDBACK_CHARS = 2000;

/**
 * The visitor's own words for `submit_feedback`: the question being answered, the one before it,
 * or both, exactly as typed. Null when that says too little to keep.
 */
export function feedbackQuote(
  chat: NonNullable<AssistantToolContext["chat"]>,
  quote: "this_message" | "previous_message" | "both",
): string | null {
  const parts =
    quote === "this_message"
      ? [chat.message]
      : quote === "previous_message"
        ? [chat.previousMessage]
        : [chat.previousMessage, chat.message];
  const text = parts
    .filter((part): part is string => !!part?.trim())
    .map((part) => part.trim())
    .join("\n\n")
    .slice(0, MAX_FEEDBACK_CHARS);
  if (text.length < MIN_FEEDBACK_CHARS || junkReason(text)) return null;
  return text;
}

interface AssistantTool<Shape extends z.ZodRawShape> {
  name: string;
  /** Shown in the widget while the tool runs, e.g. "wait-times"; none for a page change. */
  status?: AssistantToolStatus;
  description: string;
  input: z.ZodObject<Shape>;
  run(
    input: z.infer<z.ZodObject<Shape>>,
    ctx: AssistantToolContext,
  ): Promise<unknown>;
}

/** The kinds of lookup the widget names while Parky waits for a tool. */
export type AssistantToolStatus =
  | "parks"
  | "wait-times"
  | "rides"
  | "forecast"
  | "posts"
  | "planner"
  | "location"
  | "weather"
  | "glossary";

function tool<Shape extends z.ZodRawShape>(
  definition: AssistantTool<Shape>,
): AssistantTool<z.ZodRawShape> {
  return definition as unknown as AssistantTool<z.ZodRawShape>;
}

function mcp(ctx: AssistantToolContext): McpToolContext {
  return { admin: PARKY, get: ctx.get.bind(ctx) };
}

/** A day of the visitor's plan, the park checked; the planner snapshot is the only source. */
async function plannedDay(
  ctx: AssistantToolContext,
  reference: string,
  date: string,
) {
  const park = await planPark(mcp(ctx), reference);
  if ("ambiguous" in park) return park;
  const day = ctx.planner?.days.find(
    (planned) => planned.park.slug === park.slug && planned.date === date,
  );
  if (!day) {
    throw new McpToolError(`${park.name} on ${date} is not in the user's plan`);
  }
  return { park, day };
}

/** The entries by id, in the order asked; an id the plan does not hold is refused, never guessed. */
function plannedEntries(day: PlannerDay, ids: readonly string[]) {
  return ids.map((id) => {
    const entry = day.entries.find((planned) => planned.id === id);
    if (!entry) {
      throw new McpToolError(
        `Entry ${id} is not in this day's plan; take ids from the plan in This chat`,
      );
    }
    return entry;
  });
}

/**
 * One performance of a park's show: the show by slug, `start` (HH:MM, the park's clock) one of
 * its verified times that day. A refusal names the real times, so the model can correct itself.
 */
async function showPerformance(
  ctx: AssistantToolContext,
  reference: string,
  date: string,
  showSlug: string,
  start: string,
) {
  const found = await resolvePark(mcp(ctx), reference);
  if (isAmbiguous(found)) return found;
  const park = plannerPark(found);
  const shows = ctx.get(ShowsService);
  const [all, starts] = await Promise.all([
    shows.findByParkId(found.id),
    shows.getShowtimeInstantsOnDate(found.id, found.timezone, date),
  ]);
  const show = all.find((candidate) => candidate.slug === showSlug);
  if (!show) {
    throw new McpToolError(
      `${park.name} has no show ${showSlug}; use get_shows`,
    );
  }
  const instants = starts.get(show.id) ?? [];
  const times = instants.map((instant) =>
    formatInTimeZone(new Date(instant), found.timezone, "HH:mm"),
  );
  const at = times.indexOf(start);
  if (at < 0) {
    throw new McpToolError(
      times.length
        ? `${show.name} starts at ${times.join(", ")} that day, park time`
        : `${show.name} has no published time that day`,
    );
  }
  return { park, show, timezone: found.timezone, instant: instants[at] };
}

/** A post's or glossary term's page: `/<locale>/blog|news|glossary/<slug>`. */
const CONTENT_PATH =
  /^\/(?:en|de|nl|fr|es|it)\/(?:blog|news|glossary)\/[a-z0-9]+(?:-[a-z0-9]+)*$/;

const parkRef = z
  .string()
  .min(2)
  .max(120)
  .describe('Park id from search_parks, or its slug like "phantasialand"');

const isoDay = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

/** How far `park_forecast` looks ahead, today included. */
const FORECAST_DAYS = 60;

/** Days from `from` to `to`, both YYYY-MM-DD: calendar days, no clock involved. */
function daysBetween(from: string, to: string): number {
  return Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );
}

/** The calendar or tiles card of one park, from `park_forecast` and, live, `get_park`. */
async function dayCard(
  ctx: AssistantToolContext,
  reference: string,
  kind: "calendar" | "tiles",
  start: string | undefined,
  days: number,
): Promise<unknown> {
  const found = await resolvePark(mcp(ctx), reference);
  if (isAmbiguous(found)) return found;
  const today = getCurrentDateInTimezone(found.timezone);
  const first = start ?? today;
  const count = kind === "calendar" ? days : 1;
  const ahead = daysBetween(today, first);
  if (ahead < 0 || ahead + count > FORECAST_DAYS) {
    throw new McpToolError(
      `Pick days from ${today} up to ${FORECAST_DAYS} days ahead`,
    );
  }
  const live = kind === "tiles" && first === today;
  const [forecast, summary] = await Promise.all([
    parkForecastTool.handler(
      { park: found.id, days: ahead + count },
      mcp(ctx),
    ) as Promise<ForecastSummary>,
    live
      ? (getParkTool.handler(
          { park: found.id, longestWaits: 0 },
          mcp(ctx),
        ) as Promise<LiveParkSummary>)
      : null,
  ]);
  if (kind === "calendar") {
    const card = calendarCard(forecast, first, count);
    const open = card.days.filter((day) => day.status === "OPERATING").length;
    return new ActionOutcome(
      { type: "card", card },
      {
        shown: `${card.days.length} days of ${card.park} from ${first}, ${open} open`,
      },
    );
  }
  const card = tilesCard(forecast, first, summary);
  const { url: _url, ...shown } = card;
  return new ActionOutcome({ type: "card", card }, { shown });
}

/**
 * The days a year before `date` (YYYY-MM-DD) worth comparing: the same date (29 February becomes
 * the 28th), and the day of the same weekday nearest to it, at most three days off, because a
 * Saturday says more about a Saturday than the date does. Calendar days only, no clock.
 */
export function lastYearDays(date: string): {
  sameDate: string;
  sameWeekday: string;
} {
  const [year, month, day] = date.split("-");
  const sameDate = `${Number(year) - 1}-${month}-${month === "02" && day === "29" ? "28" : day}`;
  const weekday = (iso: string) => new Date(`${iso}T00:00:00Z`).getUTCDay();
  const ahead = (weekday(date) - weekday(sameDate) + 7) % 7;
  return {
    sameDate,
    sameWeekday: addIsoDays(sameDate, ahead > 3 ? ahead - 7 : ahead),
  };
}

/** One past day for `get_park_history` as the calendar serves it, or null without one. */
function pastDay(
  day: ReturnType<typeof compactDay> | undefined,
): Record<string, unknown> | null {
  if (!day) return null;
  return {
    date: day.date,
    weekday: formatInTimeZone(new Date(`${day.date}T12:00:00Z`), "UTC", "EEEE"),
    status: day.status,
    opens: day.opens,
    closes: day.closes,
    // A past day's level counts only where it was measured: the calendar falls back to a stored
    // prediction, which a year back is a forecast, not what happened (docs/rules/absent-facts.md).
    crowdLevel: day.forecastIsActual ? day.crowdLevel : "unknown",
    // The recorded peak of the headliners, only where it was measured.
    headlinerPeakWaitMinutes: day.forecastIsActual
      ? (day.expectedPeakWaitMinutes ?? null)
      : null,
    holiday: day.holiday,
    schoolVacation: day.schoolVacation,
    events: day.events,
  };
}

/** Most shows one answer lists. */
const MAX_SHOWS = 30;

/** The entity each favourite kind is looked up in. */
const FAVORITE_TABLES: Record<
  FavoriteEntity,
  EntityTarget<{ id: string; name: string }>
> = {
  park: Park,
  attraction: Attraction,
  show: ShowEntity,
  restaurant: Restaurant,
};

/**
 * `/v1/favorites`' answer cut to what "how are my favourites doing" needs; show times stay
 * instants, so they reach the model in the visitor's clock.
 */
export function favoritesSummary(found: FavoritesResponseDto) {
  return {
    parks: found.parks.map((park) => ({
      id: park.id,
      name: park.name,
      status: park.status,
      crowdLevel: park.analytics?.crowdLevel ?? null,
      avgWaitMinutes: park.analytics?.avgWaitTime ?? null,
    })),
    rides: found.attractions.map((ride) => ({
      id: ride.id,
      name: ride.name,
      park: ride.park?.name ?? null,
      status: ride.effectiveStatus ?? ride.status ?? null,
      waitMinutes:
        ride.queues?.find((queue) => queue.queueType === "STANDBY")?.waitTime ??
        null,
      crowdLevel: ride.crowdLevel ?? null,
    })),
    shows: found.shows.map((show) => ({
      id: show.id,
      name: show.name,
      park: show.park?.name ?? null,
      status: show.status,
      times: (show.showtimes ?? []).map((time) => time.startTime),
    })),
    restaurants: found.restaurants.map((restaurant) => ({
      id: restaurant.id,
      name: restaurant.name,
      park: restaurant.park?.name ?? null,
      status: restaurant.status,
    })),
  };
}

/** The fields of `get_attraction`'s answer that say why a ride is not running. */
interface RideClosure {
  status?: string | null;
  effectiveStatus?: string | null;
  outage?: {
    startedAt: string;
    signal: string;
    estimate?: {
      recoveryWithin30?: number;
      recoveryWithin60?: number;
      recoveryWindow?: { from: string; to?: string } | null;
    };
  } | null;
  worksPeriod?: {
    to: string | null;
    toUncertain: boolean;
    runningToday: boolean;
  } | null;
  facts?: {
    seasonMonths?: number[] | null;
    isCurrentlyInSeason?: boolean | null;
  };
  downtime?: { kind: string; medianMinutes?: number } | null;
}

/**
 * Why a ride is not running and what is known about its return, so "when does it reopen" is
 * answered from data: a breakdown (since when, how such outages ended before), a works period
 * (its last closed day, when the park named one), out of season (the next season month), or
 * `unknown`. Null while the ride runs.
 */
export function reopeningOf(ride: RideClosure, month: number) {
  const status = ride.effectiveStatus ?? ride.status ?? null;
  if (status === "OPERATING") return null;
  if (ride.worksPeriod?.runningToday) {
    return {
      reason: "works" as const,
      closedUntil: ride.worksPeriod.to,
      ...(ride.worksPeriod.toUncertain && { estimatedByParkFan: true }),
    };
  }
  if (ride.facts?.isCurrentlyInSeason === false) {
    const months = ride.facts.seasonMonths ?? [];
    const next =
      months.find((candidate) => candidate > month) ?? months[0] ?? null;
    return {
      reason: "season" as const,
      seasonMonths: months,
      nextSeasonMonth: next,
    };
  }
  if (status === null) return null;
  if (status === "DOWN" && ride.outage) {
    const estimate = ride.outage.estimate;
    return {
      reason: "breakdown" as const,
      downSince: ride.outage.startedAt,
      reportedByPark: ride.outage.signal === "down",
      ...(estimate && {
        backWithin30MinutesPercent: estimate.recoveryWithin30,
        backWithin60MinutesPercent: estimate.recoveryWithin60,
        ...(estimate.recoveryWindow && { likelyBack: estimate.recoveryWindow }),
      }),
      ...(ride.downtime?.kind === "figures" && {
        typicalOutageMinutes: ride.downtime.medianMinutes,
      }),
    };
  }
  return { reason: "unknown" as const, status };
}

/**
 * The park-local day to look for the park's next opening from: the day after a works period's
 * last closed day, or the first of the next season month (next year when it has come round).
 * Null when nothing says when the closure ends.
 */
export function reopenSearchFrom(
  reopening: ReturnType<typeof reopeningOf>,
  today: string,
): string | null {
  if (reopening?.reason === "works" && reopening.closedUntil) {
    return addIsoDays(reopening.closedUntil, 1);
  }
  if (reopening?.reason === "season" && reopening.nextSeasonMonth) {
    const [year, month] = today.split("-").map(Number);
    const nextYear = reopening.nextSeasonMonth <= month ? year + 1 : year;
    return `${nextYear}-${String(reopening.nextSeasonMonth).padStart(2, "0")}-01`;
  }
  return null;
}

/**
 * When one ride of a planned day starts, if later than the park's opening (Phantasialand 09:00,
 * Taron 10:00), as an instant for the visitor's clock, and the weekdays it usually runs on.
 */
export function rideOpeningOf(
  ride: PlanDayRideDto,
  plan: Pick<PlanDayDto, "context" | "timezone">,
) {
  return {
    ...(ride.opensAt && {
      opensAt: parkClockInstant(
        plan.context.date,
        ride.opensAt,
        plan.context.openHour,
        plan.timezone,
      ),
      opensAtConfidence: ride.opensAtConfidence,
    }),
    ...(ride.runsOnWeekdays && { runsOnWeekdays: ride.runsOnWeekdays }),
  };
}

/** The forecast hour with the shortest wait among `hours`, or null. */
function shortestHour<T extends { wait: number }>(hours: readonly T[]) {
  return hours.reduce<T | null>(
    (low, hour) => (low === null || hour.wait < low.wait ? hour : low),
    null,
  );
}

/**
 * A park's shows on one day for `get_shows`: those with a showtime, earliest first, each start an
 * instant so it reaches the model in the visitor's clock. Status and duration come from a live
 * reading of the show only; without one they are null, never a guessed "closed".
 */
export function showsOnDay(
  shows: readonly Show[],
  live: ReadonlyMap<string, ShowLiveData>,
  starts: ReadonlyMap<string, string[]>,
  timezone: string,
) {
  const listed = shows
    .filter((show) => starts.get(show.id)?.length)
    .map((show) => {
      const reading = live.get(show.id);
      const timed = reading?.showtimes?.find((t) => t.startTime && t.endTime);
      const minutes = timed
        ? Math.round(
            (Date.parse(timed.endTime!) - Date.parse(timed.startTime)) / 60_000,
          )
        : NaN;
      return {
        id: show.id,
        name: show.name,
        slug: show.slug,
        ...(show.landName && { land: show.landName }),
        status: reading?.status ?? null,
        showtimes: starts.get(show.id) as string[],
        ...(minutes > 0 && minutes < 600 && { durationMinutes: minutes }),
        ...(show.isSeasonal && {
          seasonMonths: show.seasonMonths,
          inSeasonNow: isCurrentlyInSeason(
            {
              isSeasonal: true,
              seasonMonths: show.seasonMonths,
              seasonOutSince: null,
            },
            timezone,
          ),
        }),
      };
    })
    .sort((a, b) => a.showtimes[0].localeCompare(b.showtimes[0]));
  return {
    shows: listed.slice(0, MAX_SHOWS),
    withShowtimes: listed.length,
    withoutShowtimes: shows.length - listed.length,
  };
}

/** One park's shows on a day (default the park's today), for `get_shows` and its card. */
async function showsOf(
  ctx: AssistantToolContext,
  reference: string,
  date: string | undefined,
) {
  const found = await resolvePark(mcp(ctx), reference);
  if (isAmbiguous(found)) return found;
  const shows = ctx.get(ShowsService);
  const today = getCurrentDateInTimezone(found.timezone);
  const day = date ?? today;
  const [all, live, starts] = await Promise.all([
    shows.findByParkId(found.id),
    day === today
      ? shows.findCurrentStatusByPark(found.id)
      : Promise.resolve(new Map<string, ShowLiveData>()),
    shows.getShowtimeInstantsOnDate(found.id, found.timezone, day),
  ]);
  return {
    park: found,
    date: day,
    ...showsOnDay(all, live, starts, found.timezone),
  };
}

/**
 * A park's restaurants as the park page shows them (deduplicated by name, with live status),
 * optionally only those whose name or cuisine holds `query`. Absent facts stay null.
 */
async function restaurantsOf(
  ctx: AssistantToolContext,
  reference: string,
  query: string | undefined,
) {
  const found = await resolvePark(mcp(ctx), reference);
  if (isAmbiguous(found)) return found;
  const park = await ctx.get(ParksService).loadParkRelations(found);
  const dto = await ctx
    .get(ParkIntegrationService)
    .buildIntegratedResponse(park, false, false);
  const lands = new Map(
    (park.restaurants ?? []).map((r) => [r.id, r.landName || null]),
  );
  const all = dto.restaurants ?? [];
  const needle = query ? foldText(query) : null;
  const matched = needle
    ? all.filter((r) =>
        foldText(`${r.name} ${r.cuisineType ?? ""}`).includes(needle),
      )
    : all;
  return {
    park: park.name,
    parkStatus: dto.status ?? null,
    pageUrl: parkPageUrl(park),
    total: all.length,
    matched: matched.length,
    restaurants: matched.map((r) => ({
      id: r.id,
      name: r.name,
      // Without a reading of its own the payload guesses from the park's status; that is
      // no statement about the stand (docs/rules/absent-facts.md).
      status: r.lastUpdated ? (r.status ?? null) : null,
      land: lands.get(r.id) ?? null,
      cuisine: r.cuisineType || null,
      requiresReservation: r.requiresReservation ?? null,
    })),
  };
}

/** Most shows and restaurants a card lists. */
const MAX_SHOW_CARD = 10;
const MAX_RESTAURANT_CARD = 12;

/**
 * The shows card: at most ten, by each show's next start (shows that are over for the day last),
 * every time with the park's offset. Shows have no page of their own.
 */
export function showsCard(
  park: { name: string; timezone: string; pageUrl: string | null },
  date: string,
  shows: Array<{ name: string; status: string | null; showtimes: string[] }>,
  now: number,
): Extract<AssistantCard, { kind: "shows" }> {
  const next = (times: string[]) =>
    times.map(Date.parse).find((at) => at >= now) ?? Number.POSITIVE_INFINITY;
  return {
    kind: "shows",
    park: park.name,
    url: park.pageUrl ? siteOnlyPath(park.pageUrl) : null,
    date,
    shows: [...shows]
      .sort((a, b) => next(a.showtimes) - next(b.showtimes))
      .slice(0, MAX_SHOW_CARD)
      .map((show) => ({
        name: show.name,
        status: show.status,
        times: show.showtimes
          .map((at) => parkInstant(at, park.timezone))
          .filter((at): at is string => at !== null),
        url: null,
      })),
  };
}

/** Most restaurants one answer lists; Phantasialand has 46. */
const MAX_RESTAURANTS = 25;

/** Lower case without accents, so "eis" finds "Eiscafé" and "creme" finds "Crème". */
export function foldText(text: string): string {
  return text.normalize("NFD").replace(/\p{M}/gu, "").toLowerCase();
}

const CROWD_ORDER = [
  "very_low",
  "low",
  "moderate",
  "high",
  "very_high",
  "extreme",
];

export const ASSISTANT_TOOLS = [
  tool({
    name: "search_parks",
    status: "parks",
    description:
      "Find theme parks by name, city or country (typo-tolerant). Returns id, name, slug, city, " +
      "country, whether it is open now, the live crowd level and today's opening hours. Use the " +
      "id for the other tools.",
    input: z.object({
      query: z
        .string()
        .min(2)
        .max(100)
        .describe('e.g. "europa park" or "orlando"'),
    }),
    run: ({ query }, ctx) =>
      findParksTool.handler({ query, limit: 5 }, mcp(ctx)),
  }),
  tool({
    name: "get_park_live",
    status: "wait-times",
    description:
      "The park right now: open or closed, today's hours and the next opening, crowd level, " +
      "average and peak waits today, weather now, only the 8 longest current standby waits, " +
      "rides that are down, rides in maintenance (window: until when), maintenance starting in " +
      "the next 30 days (maintenanceAhead), rides that have not run today, the park's pageUrl and the " +
      "editors' funFacts about it. Not the shortest waits, one ride type or a recommendation: " +
      "show_card kind rides order shortest (with type for coasters, dark rides, water or " +
      "family rides) has every waiting ride.",
    input: z.object({ park: parkRef }),
    run: async ({ park }, ctx) => {
      const live = (await getParkTool.handler(
        { park, longestWaits: 8 },
        mcp(ctx),
      )) as Record<string, unknown>;
      // Every time reaches the model in the visitor's clock, so the park's own clock and zone
      // only cost tokens.
      const {
        localTime: _localTime,
        timezone: _timezone,
        funFacts,
        ...rest
      } = live;
      if (rest.retiredAt === null) delete rest.retiredAt;
      // English stands in for an untranslated locale: the model retells it in the chat's language.
      const facts = funFacts as FunFacts | null;
      const told = facts?.[ctx.locale as FunFactLocale] ?? facts?.en;
      if (told) rest.funFacts = told;
      return rest;
    },
  }),
  tool({
    name: "find_rides",
    status: "rides",
    description:
      "Find rides by name only (typo-tolerant), optionally within one park: id (for get_ride), " +
      'name, park, status and standby wait, at most 5. Not by type or figure: for "the fastest ' +
      'coaster" or "coasters in Germany" use compare_rides.',
    input: z.object({
      query: z.string().min(2).max(100).describe('Ride name, e.g. "taron"'),
      parkId: z
        .string()
        .uuid()
        .optional()
        .describe("Only rides of this park (id from search_parks)"),
    }),
    run: ({ query, parkId }, ctx) =>
      findAttractionsTool.handler({ query, parkId, limit: 5 }, mcp(ctx)),
  }),
  tool({
    name: "find_restaurants",
    status: "parks",
    description:
      "A park's restaurants, snack bars and food stands, optionally only those whose name or " +
      'cuisine contains query (e.g. "eis", "ice", "pizza"): name, status now where the park ' +
      "reports it, cuisine and reservation where known, land, and the park's pageUrl. For " +
      '"where can I eat", "every ice-cream stand"; plan_add_block puts a stop in the plan. No ' +
      "menus, prices or opening hours.",
    input: z.object({
      park: parkRef,
      query: z.string().trim().min(2).max(60).optional(),
    }),
    run: async ({ park: reference, query }, ctx) => {
      const found = await restaurantsOf(ctx, reference, query);
      if ("ambiguous" in found) return found;
      return {
        ...found,
        restaurants: found.restaurants.slice(0, MAX_RESTAURANTS).map((r) => ({
          name: r.name,
          status: r.status,
          ...(r.cuisine && { cuisine: r.cuisine }),
          ...(r.requiresReservation !== null && {
            requiresReservation: r.requiresReservation,
          }),
          ...(r.land && { land: r.land }),
        })),
      };
    },
  }),
  tool({
    name: "get_park_history",
    status: "forecast",
    description:
      "How a park's day went one year earlier, as park.fan's calendar recorded it: the same " +
      "date last year and the nearest day of the same weekday (weekday matters more), each " +
      "with its hours, crowd level (unknown when nothing was measured), the headliners' " +
      "recorded peak wait, holidays and events; null for a day park.fan has nothing on. For " +
      '"how busy was it on this date last year". For "last Halloween" give this year\'s ' +
      "Halloween as date. Only one year back, not other years or the days ahead.",
    input: z.object({
      park: parkRef,
      date: isoDay
        .optional()
        .describe(
          "The day to look a year back from, YYYY-MM-DD; default the park's today",
        ),
    }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const today = getCurrentDateInTimezone(found.timezone);
      const day = date ?? today;
      const { sameDate, sameWeekday } = lastYearDays(day);
      if (sameDate >= today) {
        throw new McpToolError("That day a year back has not happened yet");
      }
      const past = [sameDate, sameWeekday].filter((d) => d < today).sort();
      const [from, to] = [past[0], past[past.length - 1]];
      // The calendar's own history path: a handful of past days, each cached for a day.
      const { fromDate, toDate } = parseDateRange(from, to, {
        timezone: found.timezone,
      });
      const calendar = await ctx
        .get(CalendarService)
        .buildCalendarResponse(found, fromDate, toDate, "none");
      const days = new Map(
        calendar.days.map((d) => [d.date, compactDay(d, found.timezone)]),
      );
      return {
        park: found.name,
        askedAbout: day,
        sameDateLastYear: pastDay(days.get(sameDate)),
        ...(sameWeekday !== sameDate &&
          sameWeekday < today && {
            sameWeekdayLastYear: pastDay(days.get(sameWeekday)),
          }),
      };
    },
  }),
  tool({
    name: "get_shows",
    status: "rides",
    description:
      "One park's shows with their showtimes on a day (today by default), earliest first: " +
      "name, land, status now (today only, where the park reports it), every start time, the " +
      "duration where known, and season. Shows without a time that day are counted, not listed. " +
      'For "when is the next show", "what\'s on today". Parks seldom publish times beyond ' +
      "today, so another day is often empty; no parades the park does not list as a show.",
    input: z.object({
      park: parkRef,
      date: isoDay
        .optional()
        .describe("YYYY-MM-DD in the park's calendar; default today"),
    }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await showsOf(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, ...rest } = found;
      return { park: park.name, ...rest };
    },
  }),
  tool({
    name: "get_ride",
    status: "rides",
    description:
      "One ride now: status, every queue with its wait, crowd level and trend, a running outage, " +
      "facts (minimum height in cm, gets you wet, single rider, indoor/outdoor, season, land, " +
      "the paid fast pass, headliner), its profile where park.fan has one (type, track figures " +
      "in order, launches, inversions, manufacturer, model, opening year, top speed, height, " +
      "length, duration), typical waits, reliability, ropeDrop (whether arriving at opening " +
      "pays off: wait at opening vs the busy peak, ride by when, or betterLateInTheDay), today's " +
      "best visit times (bestVisitTimesToday: the slots our intraday forecast rates optimal or " +
      "good, renewed every 15 minutes), its pageUrl, today: opensAt (when the ride usually " +
      "starts, later than the park's gates; absent means not known, never 'with the park') and " +
      "runsOnWeekdays (ISO, Monday 1), worksPeriods (every planned maintenance or works window: " +
      "from and to are park-local days, both inclusive, either may be absent; toUncertain means " +
      "the end is our estimate; past windows stay on the list), and, while it is not running, " +
      "reopening: why (breakdown, works, season or unknown) and what is known about its return.",
    input: z.object({
      rideId: z
        .string()
        .uuid()
        .describe("Ride id from find_rides or get_park_live"),
    }),
    run: async ({ rideId }, ctx) => {
      const ride = (await getAttractionTool.handler(
        { attractionId: rideId, historyDays: 0 },
        mcp(ctx),
      )) as Record<string, unknown>;
      delete ride.predictionAccuracy;
      // Today's readings one by one and the bookkeeping around them answer nothing a visitor
      // asks; the averages and the peak stay. New objects, because the response may be cached.
      const statistics = ride.statistics as Record<string, unknown> | null;
      if (statistics) {
        const {
          history: _history,
          dataPoints: _dataPoints,
          timestamp: _timestamp,
          ...kept
        } = statistics;
        ride.statistics = kept;
      }
      ride.facts = Object.fromEntries(
        Object.entries(ride.facts as Record<string, unknown>).filter(
          ([, value]) => value !== null,
        ),
      );
      const parkId = (ride.park as { id?: string } | null)?.id;
      const parks = ctx.get(ParksService);
      const park = parkId ? await parks.findById(parkId) : null;
      if (park) {
        // The park's calendar month and day: the season and the opening calendar are park-local.
        const today = getCurrentDateInTimezone(park.timezone);
        // A failed plan costs the ride's day, not the ride.
        const plan = await Promise.resolve()
          .then(() => ctx.get(PlanDayService).buildPlanDay(park, today))
          .catch(() => null);
        const planned = plan?.rides.find(
          (r) => r.attractionId === rideId.toLowerCase(),
        );
        if (plan && planned) ride.today = rideOpeningOf(planned, plan);
        const reopening = reopeningOf(
          ride as RideClosure,
          Number(today.slice(5, 7)),
        );
        if (reopening) {
          const from = reopenSearchFrom(reopening, today);
          const opening = from
            ? await parks.getFirstOperatingDayFrom(park.id, from)
            : null;
          ride.reopening = {
            ...reopening,
            // The DATE column arrives as "YYYY-MM-DD" from the driver; a Date is its UTC day.
            ...(from && {
              reopensOn: opening
                ? typeof opening.date === "string"
                  ? (opening.date as string).slice(0, 10)
                  : opening.date.toISOString().slice(0, 10)
                : null,
            }),
          };
        }
      }
      return ride;
    },
  }),
  tool({
    name: "compare_rides",
    status: "rides",
    description:
      "The rides of one park, or of every park in a country, ranked by a fact: top speed, height, " +
      "length, duration, inversions, launches, opening year or minimum rider height (cm), highest " +
      "first unless order asc. type keeps only coasters, dark rides (simulators included), water " +
      "rides or family rides. With riderHeightCm only " +
      'the rides someone that tall may ride. For "the fastest ride in X", "the fastest coaster in ' +
      'Germany", "the newest coaster", "what can my 1 m child ride". Give park or country. Counts ' +
      "what it left out for lack of data (say so when many are). No waits: those are show_card " +
      "kind rides.",
    input: z.object({
      park: parkRef.optional(),
      country: z
        .string()
        .trim()
        .min(2)
        .max(60)
        .optional()
        .describe('Instead of park: a country, "Germany" or its code "DE"'),
      by: z.enum(RIDE_RANKINGS),
      order: z.enum(["desc", "asc"]).default("desc"),
      type: z.enum(RIDE_TYPE_FILTERS).optional(),
      riderHeightCm: z.number().int().min(50).max(250).optional(),
      limit: z.number().int().min(1).max(15).default(5),
    }),
    run: async (
      { park: reference, country, by, order, type, riderHeightCm, limit },
      ctx,
    ) => {
      if (!reference === !country) {
        throw new McpToolError("Give either park or country");
      }
      const attractions = ctx.get(DataSource).getRepository(Attraction);
      let scope: { park: string } | { country: string; parks: number };
      let found: Attraction[];
      if (reference) {
        const park = await resolvePark(mcp(ctx), reference);
        if (isAmbiguous(park)) return park;
        scope = { park: park.name };
        found = await attractions.find({
          where: { parkId: park.id, retiredAt: IsNull() },
        });
      } else {
        // One query for the whole country, its parks joined for their names.
        const code = getCountryISOCode(country as string);
        found = await attractions
          .createQueryBuilder("attraction")
          .innerJoinAndSelect("attraction.park", "park")
          .where("attraction.retiredAt IS NULL")
          .andWhere("park.retiredAt IS NULL")
          .andWhere(
            "(park.countryCode = :code OR lower(park.country) = lower(:country))",
            { code: code ?? "", country },
          )
          .getMany();
        if (found.length === 0) {
          throw new McpToolError(`No parks in "${country}"; try search_parks`);
        }
        scope = {
          country: found[0].park.country,
          parks: new Set(found.map((ride) => ride.parkId)).size,
        };
      }
      const profiles = await ctx
        .get(RideProfileService)
        .findByAttractions(found.map((attraction) => attraction.id));
      return {
        ...scope,
        by,
        order,
        ...(type && { type }),
        ...compareRides(comparedRides(found, profiles), {
          by,
          order,
          type,
          riderHeightCm,
          limit,
        }),
      };
    },
  }),
  tool({
    name: "get_ride_forecast",
    status: "forecast",
    description:
      "The predicted standby wait of one ride per hour of a day (park local hours), the hour with " +
      'the shortest predicted wait, and how far ahead the forecast is (tier). For "when is the ' +
      'queue for X shortest". Today by default. A day a maintenance window shuts the ride has ' +
      "no hours and says closedForMaintenance with the window instead.",
    input: z.object({
      rideId: z
        .string()
        .uuid()
        .describe("Ride id from find_rides or get_park_live"),
      date: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}$/)
        .optional()
        .describe("YYYY-MM-DD in the park's calendar; default today"),
    }),
    run: async ({ rideId, date }, ctx) => {
      const ride = await ctx
        .get(DataSource)
        .getRepository(Attraction)
        .findOne({
          where: { id: rideId.toLowerCase() },
          relations: { park: { destination: true } },
        });
      if (!ride?.park)
        throw new McpToolError(`No ride ${rideId}; try find_rides`);
      const day = date ?? getCurrentDateInTimezone(ride.park.timezone);
      // The plan leaves a ride out on a day a works window covers; this says why its hours are
      // empty, so "no forecast" is never what the visitor hears.
      const maintenance = worksPeriodOn(ride, day);
      const plan = await ctx.get(PlanDayService).buildPlanDay(ride.park, day);
      const tz = ride.park.timezone;
      const isToday = day === getCurrentDateInTimezone(tz);
      // Never an hour after closing, and today never one that is already over.
      const hours = bookableHours(
        plan.rides.find((r) => r.attractionId === ride.id)?.hours ?? [],
        plan.context,
        isToday ? Number(formatInTimeZone(new Date(), tz, "H")) : null,
      );
      const { openHour, closeHour } = plan.context;
      const best = shortestHour(hours);
      return {
        ride: ride.name,
        park: ride.park.name,
        date: day,
        tier: plan.tier,
        parkStatus: plan.context.status,
        opens: openHour !== null ? parkHourInstant(day, openHour, tz) : null,
        closes:
          openHour !== null && closeHour !== null
            ? parkHourInstant(day, unfoldedCloseHour(openHour, closeHour), tz)
            : null,
        // Park-local hours become instants, so they reach the model in the visitor's clock.
        hours: hours.map((hour) => ({
          at: parkHourInstant(day, hour.hour, ride.park.timezone),
          waitMinutes: hour.wait,
        })),
        shortestAt: best
          ? {
              at: parkHourInstant(day, best.hour, ride.park.timezone),
              waitMinutes: best.wait,
            }
          : null,
        ...(maintenance && { closedForMaintenance: maintenance }),
      };
    },
  }),
  tool({
    name: "get_park_day",
    status: "forecast",
    description:
      "One day at one park as park.fan's trip planner sees it: whether and when it is open, the " +
      "expected crowd level, the day's weather (absent beyond about 14 days), holiday, school " +
      "break, bridge day and weekend, neighbouring holidays, hotel early entry; every ride with " +
      "opensAt (when it starts, later than the park's gates; absent means not known), the " +
      "weekdays it runs on, the predicted peak and the hour with the shortest wait; and the " +
      "shows with their times (projected: what the show did on a recent matching day, not a " +
      'promise). For "what is the day like", "which rides open later", "a plan for Saturday". ' +
      "Rides a maintenance or works window shuts that day are not in rides but in " +
      "ridesInMaintenance, each with its window. Today by default.",
    input: z.object({ park: parkRef, date: planDate.optional() }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const park = await ctx.get(ParksService).findById(found.id);
      if (!park)
        throw new McpToolError(`No park ${reference}; try search_parks`);
      const tz = park.timezone;
      const day = date ?? getCurrentDateInTimezone(tz);
      const planDays = ctx.get(PlanDayService);
      // A failed lookup costs the list, never the day.
      const [plan, inMaintenance] = await Promise.all([
        planDays.buildPlanDay(park, day),
        Promise.resolve()
          .then(() => planDays.ridesInMaintenance(park, day))
          .catch(() => []),
      ]);
      const { openHour, closeHour, ...context } = plan.context;
      const currentHour =
        day === getCurrentDateInTimezone(tz)
          ? Number(formatInTimeZone(new Date(), tz, "H"))
          : null;
      return {
        park: park.name,
        tier: plan.tier,
        ...context,
        opens: openHour !== null ? parkHourInstant(day, openHour, tz) : null,
        closes:
          openHour !== null && closeHour !== null
            ? parkHourInstant(day, unfoldedCloseHour(openHour, closeHour), tz)
            : null,
        rides: plan.rides.map((ride) => {
          const shortest = shortestHour(
            bookableHours(ride.hours, plan.context, currentHour),
          );
          return {
            id: ride.attractionId,
            name: ride.attractionName,
            ...(ride.land && { land: ride.land }),
            ...(ride.isHeadliner && { headliner: true }),
            ...(ride.downYesterday && { downYesterday: true }),
            ...rideOpeningOf(ride, plan),
            predictedPeakWaitMinutes: ride.dayPeak,
            shortestAt: shortest && {
              at: parkHourInstant(day, shortest.hour, tz),
              waitMinutes: shortest.wait,
            },
          };
        }),
        ...(plan.ridesUnavailable && {
          ridesUnavailable: plan.ridesUnavailable,
        }),
        ...(inMaintenance.length > 0 && {
          ridesInMaintenance: inMaintenance.map((ride) => ({
            id: ride.id,
            name: ride.name,
            window: ride.window,
          })),
        }),
        shows: plan.shows.map((show) => ({
          name: show.showName,
          source: show.source,
          times: show.times.map((time) =>
            parkClockInstant(day, time, openHour, tz),
          ),
        })),
      };
    },
  }),
  tool({
    name: "get_park_forecast",
    status: "forecast",
    description:
      "The days ahead for one park: per day the opening hours or closed, the expected crowd level, " +
      "the expected peak wait of the headliners, weather (basis climate_normal is the long-run " +
      "average, not a forecast), holidays, school breaks, bridge days and the park's events " +
      "(Halloween, Christmas season, special openings, festivals); plus today's hourly " +
      "forecast. days: 1 to 31 from today, default 7. Each day's weather belongs to that date " +
      "only; never move it to another day. Days after " +
      "scheduleCoverage.to have no published hours yet: treat their status as unknown. Past " +
      "days: get_park_history.",
    input: z.object({
      park: parkRef,
      days: z
        .number()
        .int()
        .min(1)
        .max(31)
        .default(7)
        .describe("Days ahead, today included"),
    }),
    run: async ({ park, days }, ctx) => {
      const forecast = (await parkForecastTool.handler(
        { park, days },
        mcp(ctx),
      )) as Record<string, unknown>;
      delete forecast.servingModels;
      // `todayHourly` counts park-local hours of the park's today; make them instants.
      const parkInfo = forecast.park as { timezone?: string } | undefined;
      const forecastDays = forecast.days as Array<{ date: string }> | undefined;
      const hourly = forecast.todayHourly as Array<
        { hour: number } & Record<string, unknown>
      > | null;
      const today = forecastDays?.[0]?.date;
      if (hourly && parkInfo?.timezone && today) {
        forecast.todayHourly = hourly.map(({ hour, ...rest }) => ({
          at: parkHourInstant(today, hour, parkInfo.timezone as string),
          ...rest,
        }));
      }
      return forecast;
    },
  }),
  tool({
    name: "get_park_weather",
    status: "weather",
    description:
      "Weather at one park right now and for the next hours: temperature, whether it rains, when " +
      "rain or a thunderstorm starts and ends (ISO times), wind gusts, and an hourly forecast " +
      "for the next 12 hours (weatherCode is the WMO code). For days ahead use get_park_forecast.",
    input: z.object({ park: parkRef }),
    run: async ({ park: reference }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const weather = ctx.get(WeatherService);
      const [nowcast, hourly] = await Promise.all([
        weather.getNowcast(found.id).catch(() => null),
        weather.getHourlyForecast(found.id).catch(() => []),
      ]);
      const now = Date.now();
      return {
        park: { name: found.name, timezone: found.timezone },
        now: nowcast && {
          temperatureC: nowcast.currentTemperatureC,
          feelsLikeC: nowcast.currentApparentTemperatureC,
          raining: nowcast.currentlyRaining,
          rainStartsAt: nowcast.rainStartsAt,
          rainEndsAt: nowcast.rainEndsAt,
          thunderstormStartsAt: nowcast.thunderstormStartsAt,
          thunderstormEndsAt: nowcast.thunderstormEndsAt,
          peakWindGustsKmh: nowcast.peakWindGustsKmh,
          todayMinC: nowcast.temperatureMinC,
          todayMaxC: nowcast.temperatureMaxC,
        },
        nextHours: hourly
          .filter((hour) => Date.parse(hour.time) >= now - 3_600_000)
          .slice(0, 12)
          .map((hour) => ({
            time: hour.time,
            temperatureC: hour.temperature,
            precipitationMm: hour.precipitation,
            weatherCode: hour.weatherCode,
            windKmh: hour.windSpeed,
          })),
      };
    },
  }),
  tool({
    name: "get_best_days",
    status: "forecast",
    description:
      "The 10 quietest rated open days of one park in the next 90 days (or a range you give), " +
      "quietest first, and the typical crowd per weekday from history (dayOfWeek 0 = Sunday, " +
      "lower avgCrowdScore = quieter). Only the quietest ten, not a ranking of every day: a day " +
      'missing from the list is not a day to avoid. Use for "when should I go".',
    input: z.object({
      park: parkRef,
      from: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}$/)
        .optional()
        .describe("First day, YYYY-MM-DD; default today"),
      to: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}$/)
        .optional()
        .describe("Last day, YYYY-MM-DD; default 90 days ahead"),
    }),
    run: async ({ park: reference, from, to }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const result = await ctx
        .get(BestDaysService)
        .getBestDays(found, from, to);
      const open = result.days.filter((day) => day.crowdLevel !== "closed");
      // A day without a rating is no quiet day: ranked, `indexOf` -1 would put it first.
      const rated = open.filter((day) => CROWD_ORDER.includes(day.crowdLevel));
      return {
        park: { id: found.id, name: found.name, pageUrl: parkPageUrl(found) },
        window: { from: result.meta.windowFrom, to: result.meta.windowTo },
        openDays: open.length,
        unratedOpenDays: open.length - rated.length,
        tenQuietestOpenDays: rated
          .map((day) => ({
            date: day.date,
            crowdLevel: day.crowdLevel,
            holiday: day.isHoliday || undefined,
            schoolVacation: day.isSchoolVacation || undefined,
          }))
          .sort(
            (a, b) =>
              CROWD_ORDER.indexOf(a.crowdLevel) -
                CROWD_ORDER.indexOf(b.crowdLevel) ||
              a.date.localeCompare(b.date),
          )
          .slice(0, 10),
        byDayOfWeek: result.byDayOfWeek ?? null,
      };
    },
  }),
  tool({
    name: "search_posts",
    status: "posts",
    description:
      "Search park.fan's own news and blog posts in the chat's language. Returns title, " +
      "description, date, slug and url. Use get_post for a post's text.",
    input: z.object({
      query: z
        .string()
        .min(2)
        .max(100)
        .describe("Words to look for, e.g. a park or topic"),
      kind: z
        .enum(["news", "article", "any"])
        .default("any")
        .describe("news for news items, article for guides and blog posts"),
    }),
    run: ({ query, kind }, ctx) =>
      ctx.get(AssistantPostsClient).search(ctx.locale, query, kind),
  }),
  tool({
    name: "get_post",
    status: "posts",
    description:
      "The text of one park.fan post, by its slug from search_posts. Long posts are shortened: " +
      "with a question, text is the intro and the sections that answer it best. sections lists " +
      "all the post's headings, omitted the ones left out; for an omitted one, call again with a " +
      "question naming it.",
    input: z.object({
      slug: z
        .string()
        .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)
        .max(160)
        .describe("Slug from search_posts"),
      question: z
        .string()
        .min(2)
        .max(200)
        .optional()
        .describe("The user's question or its key words"),
    }),
    run: ({ slug, question }, ctx) =>
      ctx.get(AssistantPostsClient).post(ctx.locale, slug, question),
  }),
  tool({
    name: "look_up_glossary",
    status: "glossary",
    description:
      "park.fan's glossary of theme-park words: ride types, coaster elements, queue systems, " +
      "industry terms (e.g. airtime, launch, single rider, B&M, dark ride). Returns the term, a " +
      "short definition and its page. Use it for what a word means, and for a nerdy fact.",
    input: z.object({
      term: z.string().trim().min(3).max(60).describe("The word to look up"),
    }),
    run: async ({ term }, ctx) => {
      const found = (await ctx
        .get(AssistantPostsClient)
        .glossary(ctx.locale, term)) as {
        results?: Array<{
          name: string;
          slug: string;
          shortDefinition: string;
          category?: string;
        }>;
      };
      return {
        terms: (found.results ?? [])
          .filter((t) => /^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(t.slug))
          .map((t) => ({
            name: t.name,
            definition: t.shortDefinition,
            category: t.category ?? null,
            url: `${getFrontendUrl()}/${ctx.locale}/glossary/${t.slug}`,
          })),
      };
    },
  }),
  tool({
    name: "find_parks_nearby",
    status: "location",
    description:
      "Parks near the user, or, when they are inside a park, the rides around them with distance " +
      "and live wait. Uses the position the user shared in this chat; without one, a rough guess " +
      "from their network (precise: false) - then offer request_location. " +
      "Inside a park, the park's page shows a compass pointing at the rides: open it with open_page.",
    input: z.object({
      limit: z.number().int().min(1).max(8).default(5),
    }),
    run: async ({ limit }, ctx) => {
      let at = ctx.location ?? null;
      const precise = at !== null;
      if (!at && ctx.ip) {
        const guess = ctx.get(GeoipService).lookupCoordinates(ctx.ip);
        if (guess) at = { lat: guess.latitude, lng: guess.longitude };
      }
      if (!at) return { precise: false, unknown: true };
      const nearby = await ctx
        .get(LocationService)
        .findNearby(at.lat, at.lng, 1000, limit);
      // The position is the visitor's own; the model needs only what is around it.
      const { userLocation: _position, ...rest } = nearby;
      return { precise, ...rest };
    },
  }),
  tool({
    name: "request_location",
    description:
      "Ask the user, in the chat window, to share their position for a precise answer: parks " +
      "near them, or the rides around them inside a park. They decide; if they share, their next " +
      "message carries it. Only when location would clearly help.",
    input: z.object({}),
    run: () =>
      Promise.resolve(
        new ActionOutcome(
          { type: "request_location" },
          { asked: "The user sees a button to share their position." },
        ),
      ),
  }),
  tool({
    name: "get_directions",
    status: "location",
    description:
      "A Google Maps link with directions to a park's entrance coordinates, for when the user asks " +
      "how to get there. Give it as a Markdown link.",
    input: z.object({ park: parkRef }),
    run: async ({ park: reference }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      if (found.latitude == null || found.longitude == null) {
        return { park: found.name, directions: null };
      }
      const destination = `${Number(found.latitude)},${Number(found.longitude)}`;
      return {
        park: found.name,
        city: found.city,
        mapsUrl: `https://www.google.com/maps/dir/?api=1&destination=${destination}`,
      };
    },
  }),
  tool({
    name: "show_card",
    description:
      "Show a live card in the chat, drawn with park.fan's own badges and wait chips: a park " +
      "(status, crowd level, today's hours, longest waits), a ride (status, standby wait, crowd " +
      "level), a park's rides with their current waits (kind rides, longest or shortest " +
      "first, then rides that are down; type for coasters, dark rides, water or family rides " +
      "only; the result lists each row with its type ids), several parks to click " +
      "(kind parks), a park's crowd calendar over 7 to 42 days with levels, hours and holidays " +
      "(kind calendar), one day's tiles: live today, else the forecast for date (kind tiles), " +
      "a park's shows with their times on a day (kind shows), or its restaurants and food " +
      "stands, optionally matching query (kind restaurants). Whenever the answer lists rides " +
      "with waits, parks, shows or places to eat, show the matching kind instead of writing " +
      "the list as text; then name your pick in a sentence without repeating the card's numbers. " +
      "Calendar and tiles reach 60 days ahead, never back; rankings by speed or height are " +
      "compare_rides.",
    input: z.object({
      kind: z.enum([
        "park",
        "ride",
        "rides",
        "parks",
        "calendar",
        "tiles",
        "shows",
        "restaurants",
      ]),
      query: z
        .string()
        .trim()
        .min(2)
        .max(60)
        .optional()
        .describe(
          'For kind restaurants: only names or cuisines with this, e.g. "eis"',
        ),
      from: isoDay
        .optional()
        .describe("For kind calendar: first day; default the park's today"),
      days: z
        .number()
        .int()
        .min(7)
        .max(42)
        .default(14)
        .describe("For kind calendar: how many days"),
      date: isoDay
        .optional()
        .describe("For kind tiles: the day; default today, live"),
      parks: z
        .array(parkRef)
        .min(1)
        .max(8)
        .optional()
        .describe("For kind parks: park ids or slugs, in the order to show"),
      park: parkRef
        .optional()
        .describe("For kind park, rides, calendar or tiles: park id or slug"),
      order: z
        .enum(["longest", "shortest"])
        .default("longest")
        .describe("For kind rides: longest or shortest waits first"),
      type: z
        .enum(RIDE_TYPE_FILTERS)
        .optional()
        .describe("For kind rides: only rides of this type"),
      limit: z.number().int().min(1).max(10).default(6),
      rideId: z.string().uuid().optional().describe("For kind ride: ride id"),
    }),
    run: async (
      {
        kind,
        park,
        parks,
        rideId,
        order,
        type,
        limit,
        from,
        days,
        date,
        query,
      },
      ctx,
    ) => {
      if (kind === "shows") {
        if (!park) throw new McpToolError("kind shows needs park");
        const found = await showsOf(ctx, park, date);
        if ("ambiguous" in found) return found;
        const card = showsCard(
          {
            name: found.park.name,
            timezone: found.park.timezone,
            pageUrl: parkPageUrl(found.park),
          },
          found.date,
          found.shows,
          Date.now(),
        );
        return new ActionOutcome(
          { type: "card", card },
          {
            shown: `${card.shows.length} shows of ${card.park} on ${card.date}`,
            withoutShowtimes: found.withoutShowtimes,
          },
        );
      }
      if (kind === "restaurants") {
        if (!park) throw new McpToolError("kind restaurants needs park");
        const found = await restaurantsOf(ctx, park, query);
        if ("ambiguous" in found) return found;
        const card: AssistantCard = {
          kind: "restaurants",
          park: found.park,
          url: found.pageUrl ? siteOnlyPath(found.pageUrl) : null,
          // The id is for set_favorite, not part of the card.
          restaurants: found.restaurants
            .slice(0, MAX_RESTAURANT_CARD)
            .map(({ id: _id, ...shown }) => shown),
        };
        return new ActionOutcome(
          { type: "card", card },
          {
            shown: `${card.restaurants.length} of ${found.matched} places to eat at ${found.park}`,
          },
        );
      }
      if (kind === "calendar" || kind === "tiles") {
        if (!park) throw new McpToolError(`kind ${kind} needs park`);
        return dayCard(
          ctx,
          park,
          kind,
          kind === "calendar" ? from : date,
          days,
        );
      }
      if (kind === "parks") {
        if (!parks?.length) throw new McpToolError("kind parks needs parks");
        // Each row with its live state, as the park cards on the site show it.
        const rows = (
          await Promise.all(
            parks.map(async (reference) => {
              const found = await resolvePark(mcp(ctx), reference).catch(
                () => null,
              );
              if (!found || isAmbiguous(found)) return null;
              const live = (await getParkTool
                .handler({ park: found.id, longestWaits: 0 }, mcp(ctx))
                .catch(() => null)) as ParkSummary | null;
              const summary = live && !("ambiguous" in live) ? live : null;
              const pageUrl = parkPageUrl(found);
              return {
                name: found.name,
                place:
                  [found.city, found.country].filter(Boolean).join(", ") ||
                  null,
                url: pageUrl ? siteOnlyPath(pageUrl) : null,
                status: summary?.status ?? null,
                crowdLevel: summary?.crowd?.level ?? null,
                avgWaitMinutes:
                  summary?.crowd?.avgWaitMinutes != null
                    ? Math.round(summary.crowd.avgWaitMinutes)
                    : null,
              };
            }),
          )
        ).filter((row) => row !== null);
        const card: AssistantCard = { kind: "parks", parks: rows };
        return new ActionOutcome(
          { type: "card", card },
          // The rows without their links, so the model can say which park it would pick.
          { shown: rows.map(({ url: _url, ...row }) => row) },
        );
      }
      if (kind === "rides") {
        if (!park) throw new McpToolError("kind rides needs park");
        // Every operating ride with a posted wait, longest first, so both ends can be cut.
        // With a type every waiting ride, so a coaster further down the list is not cut.
        const live = (await getParkTool.handler(
          { park, longestWaits: type ? 500 : 30 },
          mcp(ctx),
        )) as ParkSummary;
        if ("ambiguous" in live) return live;
        let all = live.rides?.longestWaits ?? [];
        let down = live.rides?.down ?? [];
        // The types also go to the model with each row, so it can say which ride it would pick.
        const profiles = await ctx
          .get(RideProfileService)
          .findByAttractions([...all, ...down].map((ride) => ride.id));
        const typesOf = (ride: { id: string }) =>
          profiles.get(ride.id)?.types ?? [];
        let typeUnknown = 0;
        if (type) {
          typeUnknown = [...all, ...down].filter(
            (ride) => typesOf(ride).length === 0,
          ).length;
          const ofType = (ride: { id: string }) =>
            isRideOfType(typesOf(ride), type);
          all = all.filter(ofType);
          down = down.filter(ofType);
        }
        const waiting = (order === "shortest" ? [...all].reverse() : all).slice(
          0,
          limit,
        );
        // Rides down right now follow the waits; a full "shortest" list has no room for them.
        const room =
          order === "shortest"
            ? Math.min(MAX_DOWN_ROWS, limit - waiting.length)
            : MAX_DOWN_ROWS;
        const picked: Array<{
          id: string;
          name: string;
          waitMinutes: number | null;
          status: string;
        }> = [
          ...waiting.map((ride) => ({ ...ride, status: "OPERATING" })),
          ...down.slice(0, Math.max(0, room)).map((ride) => ({
            id: ride.id,
            name: ride.name,
            waitMinutes: null,
            status: "DOWN",
          })),
        ];
        // Each row links its own ride page, built from the ride's park and slug.
        const found = await ctx
          .get(DataSource)
          .getRepository(Attraction)
          .find({
            where: { id: In(picked.map((ride) => ride.id)) },
            relations: { park: true },
          });
        const pages = new Map(
          found.map((ride) => {
            const pageUrl = ride.park
              ? attractionPageUrl(ride.park, ride)
              : null;
            return [ride.id, pageUrl ? siteOnlyPath(pageUrl) : null];
          }),
        );
        const rides = picked.map((ride) => ({
          name: ride.name,
          status: ride.status,
          waitMinutes: ride.waitMinutes,
          url: pages.get(ride.id) ?? null,
        }));
        const card: AssistantCard = {
          kind: "rides",
          park: live.name,
          order,
          rides,
          url: live.pageUrl ? siteOnlyPath(live.pageUrl) : null,
        };
        return new ActionOutcome(
          { type: "card", card },
          {
            shown: `${rides.length} ${type ? `${type.replace("_", " ")}s` : "rides"} of ${live.name}`,
            rows: picked.map((ride) => {
              const types = typesOf(ride);
              return {
                name: ride.name,
                status: ride.status,
                waitMinutes: ride.waitMinutes,
                ...(types.length > 0 && { types }),
              };
            }),
            ...(type && { typeUnknown }),
          },
        );
      }
      if (kind === "park") {
        if (!park) throw new McpToolError("kind park needs park");
        const live = (await getParkTool.handler(
          { park, longestWaits: 3 },
          mcp(ctx),
        )) as ParkSummary;
        if ("ambiguous" in live) return live;
        const today = live.scheduleToday?.[0];
        const card: AssistantCard = {
          kind: "park",
          name: live.name,
          place: [live.city, live.country].filter(Boolean).join(", ") || null,
          status: live.status ?? null,
          crowdLevel: live.crowd?.level ?? null,
          opens: today?.opens ?? null,
          closes: today?.closes ?? null,
          waits: (live.rides?.longestWaits ?? []).map((ride) => ({
            name: ride.name,
            waitMinutes: ride.waitMinutes,
          })),
          url: live.pageUrl ? siteOnlyPath(live.pageUrl) : null,
        };
        // What the card says, so the sentence after it needs no second lookup.
        const { url: _url, ...shown } = card;
        return new ActionOutcome(
          { type: "card", card },
          {
            shown,
            ...(live.status !== "OPERATING" && {
              nextOpening: live.nextSchedule ?? null,
            }),
          },
        );
      }
      if (!rideId) throw new McpToolError("kind ride needs rideId");
      const ride = (await getAttractionTool.handler(
        { attractionId: rideId, historyDays: 0 },
        mcp(ctx),
      )) as RideSummary;
      const standby = ride.queues?.find((queue) => queue.type === "STANDBY");
      const card: AssistantCard = {
        kind: "ride",
        name: ride.name,
        park: ride.park?.name ?? null,
        status: ride.effectiveStatus ?? ride.status ?? null,
        crowdLevel: ride.crowdLevel ?? null,
        waitMinutes: standby?.waitMinutes ?? null,
        url: ride.pageUrl ? siteOnlyPath(ride.pageUrl) : null,
        ...rideBadges(ride),
      };
      const { url: _url, ...shown } = card;
      return new ActionOutcome({ type: "card", card }, { shown });
    },
  }),
  tool({
    name: "open_page",
    description:
      "Take the user to a park.fan page in their browser. Give exactly one target: a park (id or slug), a ride (rideId), or a post or " +
      "glossary term (its url from search_posts or look_up_glossary). The page address is " +
      "built from the park.fan data, never typed by you.",
    input: z.object({
      park: parkRef.optional(),
      rideId: z
        .string()
        .uuid()
        .optional()
        .describe("Ride id from find_rides or get_park_live"),
      url: z
        .string()
        .max(400)
        .optional()
        .describe(
          "A post's or glossary term's url, exactly as a tool returned it",
        ),
    }),
    run: async ({ park, rideId, url }, ctx) => {
      let path: string | null = null;
      if (rideId) {
        const ride = await ctx
          .get(DataSource)
          .getRepository(Attraction)
          .findOne({
            where: { id: rideId.toLowerCase() },
            relations: { park: true },
          });
        if (!ride?.park) throw new McpToolError("No such ride; try find_rides");
        const pageUrl = attractionPageUrl(ride.park, ride);
        path = pageUrl ? siteOnlyPath(pageUrl) : null;
      } else if (park) {
        const found = await resolvePark(mcp(ctx), park);
        if (isAmbiguous(found)) return found;
        const pageUrl = parkPageUrl(found);
        path = pageUrl ? siteOnlyPath(pageUrl) : null;
      } else if (url) {
        const candidate = siteOnlyPath(url);
        // Posts and glossary terms only; parks and rides go by id, so no address is guessed.
        path = candidate && CONTENT_PATH.test(candidate) ? candidate : null;
      }
      if (!path) {
        throw new McpToolError(
          "Give a park, a rideId, or a post or glossary url a tool returned",
        );
      }
      return new ActionOutcome({ type: "navigate", path }, { opened: true });
    },
  }),
  tool({
    name: "plan_day",
    status: "planner",
    description:
      "Open a day at one park in the user's trip planner (it opens beside the page). The plan " +
      "lives in their browser.",
    input: z.object({ park: parkRef, date: planDate }),
    run: async ({ park: reference, date }, ctx) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      return new ActionOutcome(
        { type: "planner_open_day", park, date },
        { planned: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_add_rides",
    status: "planner",
    description:
      "Add rides to the user's trip planner for one park and day, in the order given; the " +
      "planner places them after what is already planned. Ride ids " +
      "come from find_rides or get_park_live and must belong to the park. A ride a maintenance " +
      "window shuts that day is not added: it comes back in notAdded with the window, to tell " +
      "the user.",
    input: z.object({
      park: parkRef,
      date: planDate,
      rideIds: z
        .array(z.string().uuid())
        .min(1)
        .max(10)
        .describe("Ride ids, at most ten"),
    }),
    run: async ({ park: reference, date, rideIds }, ctx) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      const found = await parkRides(mcp(ctx), park.slug, rideIds, date);
      // The planner refuses a ride in maintenance on that day by every other way in, so this
      // way in does too, and says which and until when instead of reporting it added.
      const rides = found
        .filter((ride) => !ride.maintenance)
        .map(({ slug, name }) => ({ slug, name }));
      const notAdded = found.flatMap((ride) =>
        ride.maintenance
          ? [
              {
                name: ride.name,
                reason: "maintenance",
                window: ride.maintenance,
              },
            ]
          : [],
      );
      const result = {
        added: rides.map((ride) => ride.name),
        ...(notAdded.length > 0 && { notAdded }),
        park: park.name,
        date,
      };
      return rides.length > 0
        ? new ActionOutcome(
            { type: "planner_add_rides", park, date, rides },
            result,
          )
        : result;
    },
  }),
  tool({
    name: "plan_add_block",
    status: "planner",
    description:
      "Add a block of the user's own to their trip planner: a break, a meal, a show they named, " +
      "shopping, a photo stop or a meeting point. A show's start time " +
      "comes from get_shows, a food stop from find_restaurants.",
    input: z.object({
      park: parkRef,
      date: planDate,
      label: z
        .string()
        .trim()
        .min(1)
        .max(40)
        .describe("A short label, e.g. Mittagessen"),
      icon: z.enum(PLANNER_BLOCK_ICONS),
      start: clockTime
        .optional()
        .describe(
          "Start in the park's local time, HH:MM; omit to place it after the plan",
        ),
      durationMinutes: z.number().int().min(10).max(240).default(60),
    }),
    run: async (
      { park: reference, date, label, icon, start, durationMinutes },
      ctx,
    ) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      return new ActionOutcome(
        {
          type: "planner_add_block",
          park,
          date,
          block: { label, icon, startMinute: start, durationMinutes },
        },
        { added: label, park: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_optimize",
    status: "planner",
    description:
      "Let the trip planner sort one planned day for the shortest waits (mode order), or first " +
      "add the park's headliners that are missing and then sort (mode headliners). The planner " +
      "shows the result and an undo.",
    input: z.object({
      park: parkRef,
      date: planDate,
      mode: z.enum(["order", "headliners"]).default("order"),
    }),
    run: async ({ park: reference, date, mode }, ctx) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      return new ActionOutcome(
        { type: "planner_optimize", park, date, mode },
        { optimizing: park.name, date, mode },
      );
    },
  }),
  tool({
    name: "plan_remove",
    status: "planner",
    description:
      "Remove entries from one planned day. Entry ids from the plan in This chat.",
    input: z.object({
      park: parkRef,
      date: planDate,
      entryIds: z.array(plannerEntryId).min(1).max(40),
    }),
    run: async ({ park: reference, date, entryIds }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, day } = found;
      const entries = plannedEntries(day, entryIds);
      return new ActionOutcome(
        { type: "planner_remove", park, date, entryIds },
        { removed: entries.map((entry) => entry.name), park: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_move",
    status: "planner",
    description:
      "Move one entry of a planned day to another place in its order, toIndex counting from 0.",
    input: z.object({
      park: parkRef,
      date: planDate,
      entryId: plannerEntryId,
      toIndex: z.number().int().min(0).max(39),
    }),
    run: async ({ park: reference, date, entryId, toIndex }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, day } = found;
      const [entry] = plannedEntries(day, [entryId]);
      if (toIndex >= day.entries.length) {
        throw new McpToolError(
          `The day has ${day.entries.length} entries; toIndex is at most ${day.entries.length - 1}`,
        );
      }
      return new ActionOutcome(
        { type: "planner_move", park, date, entryId, toIndex },
        { moved: entry.name, toIndex, park: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_update_block",
    status: "planner",
    description:
      "Change a block of the user's own (a break, a meal, a stop) in a planned day: its label, " +
      "start or length. Blocks only, not rides or shows.",
    input: z.object({
      park: parkRef,
      date: planDate,
      entryId: plannerEntryId,
      label: z.string().trim().min(1).max(40).optional(),
      start: plannerClock
        .optional()
        .describe("New start in the park's local time, HH:MM"),
      durationMinutes: z.number().int().min(10).max(240).optional(),
    }),
    run: async (
      { park: reference, date, entryId, label, start, durationMinutes },
      ctx,
    ) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, day } = found;
      const [entry] = plannedEntries(day, [entryId]);
      if (entry.kind !== "block") {
        throw new McpToolError(
          `${entry.name} is a ${entry.kind}, not a block; only blocks change here`,
        );
      }
      if (label === undefined && start === undefined && !durationMinutes) {
        throw new McpToolError("Give a label, a start or a length to change");
      }
      return new ActionOutcome(
        {
          type: "planner_update_block",
          park,
          date,
          entryId,
          ...(label !== undefined && { label }),
          ...(start !== undefined && { start }),
          ...(durationMinutes !== undefined && { durationMinutes }),
        },
        { updated: entry.name, park: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_add_show",
    status: "planner",
    description:
      "Add one of a park's shows to the user's trip planner at one of its times that day. The " +
      "show's slug and times come from get_shows; start is the park's local time, HH:MM.",
    input: z.object({
      park: parkRef,
      date: planDate,
      showSlug: z
        .string()
        .max(160)
        .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)
        .describe("Slug from get_shows"),
      start: plannerClock.describe("Start in the park's local time, HH:MM"),
    }),
    run: async ({ park: reference, date, showSlug, start }, ctx) => {
      const found = await showPerformance(
        ctx,
        reference,
        date,
        showSlug,
        start,
      );
      if ("ambiguous" in found) return found;
      const { park, show } = found;
      return new ActionOutcome(
        { type: "planner_add_show", park, date, showSlug, start },
        { added: show.name, start, park: park.name, date },
      );
    },
  }),
  tool({
    name: "set_ride_alert",
    status: "planner",
    description:
      "Have park.fan send the user a push message about one ride: kind reopen once it runs " +
      "again, kind wait_below once the standby wait drops under thresholdMinutes. Ride id from " +
      "find_rides or get_park_live. One alert per ride, a new one replaces it.",
    input: z.object({
      park: parkRef,
      rideId: z.string().uuid(),
      kind: z.enum(["wait_below", "reopen"]),
      thresholdMinutes: z
        .number()
        .int()
        .min(RIDE_ALERT_THRESHOLD_MIN)
        .max(RIDE_ALERT_THRESHOLD_MAX)
        .optional()
        .describe("For wait_below: notify under this many minutes"),
    }),
    run: async ({ park: reference, rideId, kind, thresholdMinutes }, ctx) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      if (ctx.alerts?.push === "unavailable") {
        throw new McpToolError("This browser cannot receive push messages");
      }
      if (kind === "wait_below" && thresholdMinutes === undefined) {
        throw new McpToolError("A wait_below alert needs thresholdMinutes");
      }
      const ride = await ctx
        .get(DataSource)
        .getRepository(Attraction)
        .findOne({
          where: { id: rideId.toLowerCase() },
          relations: { park: true },
        });
      if (!ride || ride.park?.slug !== park.slug) {
        throw new McpToolError(
          `Ride ${rideId} is not in this park; use find_rides`,
        );
      }
      // The push endpoint's own refusals, checked first so Parky never offers what it rejects.
      const unreadable = getNoLiveWaitTimesReason(
        ride.park.citySlug,
        ride.park.slug,
      );
      if (unreadable) {
        throw new McpToolError(
          `${park.name}'s wait times cannot be read (${unreadable}); no alert would fire`,
        );
      }
      if (kind === "reopen" && ride.openWithPark) {
        throw new McpToolError(`${ride.name} is open whenever the park is`);
      }
      return new ActionOutcome(
        {
          type: "alert_ride_set",
          park,
          rideId: ride.id,
          rideSlug: ride.slug,
          rideName: ride.name,
          kind,
          ...(kind === "wait_below" && { thresholdMinutes }),
        },
        {
          alert: kind,
          ride: ride.name,
          ...(kind === "wait_below" && { thresholdMinutes }),
          push: ctx.alerts?.push ?? null,
        },
      );
    },
  }),
  tool({
    name: "set_show_reminder",
    status: "planner",
    description:
      "Have park.fan remind the user by push before one of today's performances of a show. Slug " +
      "and times from get_shows; start is the park's local time, HH:MM.",
    input: z.object({
      park: parkRef,
      showSlug: z
        .string()
        .max(160)
        .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)
        .describe("Slug from get_shows"),
      start: plannerClock.describe("Start in the park's local time, HH:MM"),
    }),
    run: async ({ park: reference, showSlug, start }, ctx) => {
      if (ctx.alerts?.push === "unavailable") {
        throw new McpToolError("This browser cannot receive push messages");
      }
      const resolved = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(resolved)) return resolved;
      const today = getCurrentDateInTimezone(resolved.timezone);
      const found = await showPerformance(
        ctx,
        resolved.id,
        today,
        showSlug,
        start,
      );
      if ("ambiguous" in found) return found;
      const { park, show, timezone, instant } = found;
      return new ActionOutcome(
        {
          type: "alert_show_set",
          park,
          showId: show.id,
          showSlug,
          showName: show.name,
          start: parkInstant(instant, timezone) as string,
        },
        { reminder: show.name, start, push: ctx.alerts?.push ?? null },
      );
    },
  }),
  tool({
    name: "remove_alert",
    status: "planner",
    description:
      "Remove one of the user's push alerts: a ride's (rideSlug) or a show reminder (showSlug), " +
      "as listed in This chat.",
    input: z.object({
      park: parkRef,
      rideSlug: z.string().max(160).optional(),
      showSlug: z.string().max(160).optional(),
    }),
    run: async ({ park: reference, rideSlug, showSlug }, ctx) => {
      if (!rideSlug === !showSlug) {
        throw new McpToolError("Give either rideSlug or showSlug");
      }
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      const held = rideSlug
        ? ctx.alerts?.rides.find(
            (alert) => alert.park === park.slug && alert.rideSlug === rideSlug,
          )
        : ctx.alerts?.shows.find(
            (alert) => alert.park === park.slug && alert.showSlug === showSlug,
          );
      if (!held) {
        throw new McpToolError("The user has no such alert; see This chat");
      }
      return new ActionOutcome(
        {
          type: "alert_remove",
          park,
          ...(rideSlug ? { rideSlug } : { showSlug }),
        },
        { removed: held.name },
      );
    },
  }),
  tool({
    name: "get_favorites",
    status: "rides",
    description:
      "The user's favourite parks, rides, shows and restaurants right now: name, park, status, " +
      "a ride's standby wait and crowd level, a park's crowd level and average wait, a show's " +
      'times today. For "how are my favourites doing" or "which of my rides is shortest".',
    input: z.object({}),
    run: async (_input, ctx) => {
      const ids = ctx.favorites;
      if (!ids) throw new McpToolError("The user has no favourites yet");
      const found = await ctx
        .get(FavoritesService)
        .getFavorites(ids.parks, ids.attractions, ids.shows, ids.restaurants);
      return favoritesSummary(found);
    },
  }),
  tool({
    name: "set_favorite",
    status: "rides",
    description:
      "Add a park, ride, show or restaurant to the user's favourites (on true) or remove it (on " +
      "false). The id comes from search_parks, find_rides, get_shows or find_restaurants.",
    input: z.object({
      entity: z.enum(FAVORITE_ENTITIES),
      id: z.string().uuid(),
      on: z.boolean().default(true),
    }),
    run: async ({ entity, id, on }, ctx) => {
      const repository = ctx
        .get(DataSource)
        .getRepository<{ id: string; name: string }>(FAVORITE_TABLES[entity]);
      const row = await repository.findOne({
        where: { id: id.toLowerCase() },
        select: { id: true, name: true },
      });
      if (!row) {
        throw new McpToolError(`No ${entity} with id ${id}; look it up first`);
      }
      return new ActionOutcome(
        { type: "favorite_set", entity, id: row.id, name: row.name, on },
        { [on ? "added" : "removed"]: row.name },
      );
    },
  }),
  tool({
    name: "plan_set_party",
    status: "planner",
    description:
      "Tell the trip planner about the party for one planned day: the smallest rider's height " +
      "in cm, whether to stay dry, whether they have early entry. The planner then flags rides " +
      "that do not fit.",
    input: z.object({
      park: parkRef,
      date: planDate,
      prefs: plannerPrefs,
    }),
    run: async ({ park: reference, date, prefs }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park } = found;
      if (Object.keys(prefs).length === 0) {
        throw new McpToolError("Give a height, avoidWet or earlyEntry");
      }
      return new ActionOutcome(
        { type: "planner_set_prefs", park, date, prefs },
        { set: prefs, park: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_mark_done",
    status: "planner",
    description:
      "Tick entries of a planned day off as done (or back, done false).",
    input: z.object({
      park: parkRef,
      date: planDate,
      entryIds: z.array(plannerEntryId).min(1).max(40),
      done: z.boolean().default(true),
    }),
    run: async ({ park: reference, date, entryIds, done }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, day } = found;
      const entries = plannedEntries(day, entryIds);
      return new ActionOutcome(
        { type: "planner_mark_done", park, date, entryIds, done },
        { marked: entries.map((entry) => entry.name), done, park: park.name },
      );
    },
  }),
  tool({
    name: "plan_clear_day",
    status: "planner",
    description:
      "Empty one planned day completely, only after the user said yes to your question in this " +
      "chat; the planner keeps an undo.",
    input: z.object({
      park: parkRef,
      date: planDate,
      confirmed: z
        .literal(true)
        .describe("true only once the user said yes to clearing this day"),
    }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, day } = found;
      return new ActionOutcome(
        { type: "planner_clear_day", park, date },
        { cleared: park.name, date, entries: day.entries.length },
      );
    },
  }),
  tool({
    name: "plan_trip",
    status: "planner",
    description:
      "Reserve several days at one park in the trip planner at once (a trip), empty days the " +
      "user fills later.",
    input: z.object({
      park: parkRef,
      dates: z.array(planDate).min(1).max(6),
    }),
    run: async ({ park: reference, dates }, ctx) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      const unique = [...new Set(dates)].sort();
      return new ActionOutcome(
        { type: "planner_reserve_days", park, dates: unique },
        { reserved: unique, park: park.name },
      );
    },
  }),
  tool({
    name: "plan_undo",
    status: "planner",
    description:
      "Undo your last change to one planned day. The planner refuses it when the user changed " +
      "the day since.",
    input: z.object({ park: parkRef, date: planDate }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park } = found;
      return new ActionOutcome(
        { type: "planner_undo", park, date },
        { undoing: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_share",
    status: "planner",
    description:
      "Open the planner's share dialog for the user's plan (a link to send).",
    input: z.object({}),
    run: async (_input, ctx) => {
      if (!ctx.planner?.days.length) {
        throw new McpToolError("The user has nothing planned to share yet");
      }
      return new ActionOutcome({ type: "planner_share" }, { sharing: true });
    },
  }),
  tool({
    name: "switch_language",
    description:
      "Switch park.fan to another of its languages: the same page opens in it and the chat " +
      "stays open.",
    input: z.object({
      locale: z.enum(ASSISTANT_LOCALES).describe("en, de, nl, fr, es or it"),
    }),
    run: ({ locale }, ctx) => {
      if (locale === ctx.locale) {
        throw new McpToolError(
          `The page is in ${languageName(locale)} already`,
        );
      }
      const { round } = claimOnce(ctx, "switch_language");
      // Only before any lookup: then nothing but the visitor's own words can have asked for it,
      // never a post, a glossary entry or a name a tool brought in.
      if (round > 0) {
        throw new McpToolError(
          "switch_language only comes first in an answer; answer without switching",
        );
      }
      return Promise.resolve(
        new ActionOutcome(
          { type: "switch_locale", locale },
          { switched: true, answerIn: languageName(locale) },
        ),
      );
    },
  }),
  tool({
    name: "submit_feedback",
    description:
      "Pass the user's feedback about park.fan on to the team, in their own words as they wrote " +
      "them: a bug, a wrong wait or figure, a missing park or ride, a feature wish, praise or " +
      "criticism of the site. quote names the user's message that holds it: this_message, " +
      "previous_message when they only just said yes to sending it, or both.",
    input: z.object({
      kind: z.enum(FEEDBACK_KINDS),
      quote: z
        .enum(["this_message", "previous_message", "both"])
        .default("this_message"),
    }),
    run: async ({ kind, quote }, ctx) => {
      const chat = ctx.chat;
      if (!chat) throw new McpToolError("Feedback cannot be sent from here");
      const { release } = claimOnce(ctx, "submit_feedback");
      const refuse = (reason: string) => {
        release();
        return new McpToolError(reason);
      };
      const message = feedbackQuote(chat, quote);
      if (!message) {
        throw refuse(
          "That message holds no feedback to pass on; nothing was sent",
        );
      }
      const records = ctx.get(AssistantRecordsService);
      const refusal = await records.chatFeedbackRefusal(
        chat.conversationId,
        message,
      );
      if (refusal === "repeat") {
        throw refuse("The team has this already; nothing new sent");
      }
      if (refusal === "cap") {
        throw refuse(
          "This chat has passed on all it may; nothing sent. The feedback tab takes more",
        );
      }
      const verdict = await ctx
        .get(AssistantRateLimitService)
        .feedback(chat.session, ctx.ip ?? null);
      if (!verdict.allowed) {
        throw new McpToolError(
          "Too much feedback from here for now; nothing sent",
        );
      }
      await records.addFeedback({
        message,
        email: null,
        locale: ctx.locale,
        pagePath: chat.pagePath,
        conversationId: chat.conversationId,
        sessionHash: chat.sessionHash,
        source: "chat",
        kind,
      });
      return new ActionOutcome({ type: "feedback_sent" }, { sent: true });
    },
  }),
  tool({
    name: "end_chat",
    description:
      "Close this chat for sustained abuse only, after one friendly warning earlier in this " +
      `chat (# Teasing and tests). The user cannot write again for ${CHAT_END_COOLDOWN_MINUTES} minutes.`,
    input: z.object({
      reason: z.enum(CHAT_END_REASONS),
      note: z
        .string()
        .trim()
        .max(200)
        .optional()
        .describe("A few words on what happened, for the team"),
    }),
    run: async ({ reason, note }, ctx) => {
      const chat = ctx.chat;
      if (!chat) throw new McpToolError("end_chat is not available here");
      // The warning comes first, so there has to be an earlier question in this chat.
      if (chat.previousMessage === null) {
        throw new McpToolError(
          "Warn once in character first; end_chat only after that",
        );
      }
      claimOnce(ctx, "end_chat");
      await ctx
        .get(DataSource)
        .getRepository(AssistantConversation)
        .update(chat.conversationId, {
          endedAt: new Date(),
          endedReason: note ? `${reason}: ${note}` : reason,
        });
      await ctx
        .get(AssistantRateLimitService)
        .endChats(chat.session, CHAT_END_COOLDOWN_MINUTES * 60);
      return new ActionOutcome(
        { type: "chat_end", cooldownMinutes: CHAT_END_COOLDOWN_MINUTES },
        { ended: true, sayGoodbye: "one short, calm sentence in character" },
      );
    },
  }),
];

const BY_NAME = new Map(ASSISTANT_TOOLS.map((t) => [t.name, t]));

/** The tool list as the Messages API takes it, in a fixed order for caching. */
/**
 * A JSON schema without what only costs tokens: the `$schema` dialect, and the long pattern zod
 * writes beside `format: "uuid"`. The model writes ids it was given, and zod still checks every
 * input before a tool runs.
 */
export function leanSchema(schema: unknown): unknown {
  if (Array.isArray(schema)) return schema.map(leanSchema);
  if (!schema || typeof schema !== "object") return schema;
  const node = schema as Record<string, unknown>;
  return Object.fromEntries(
    Object.entries(node)
      .filter(
        ([key]) =>
          key !== "$schema" && !(key === "pattern" && node.format === "uuid"),
      )
      .map(([key, value]) => [key, leanSchema(value)]),
  );
}

export const ASSISTANT_TOOL_PARAMS: Anthropic.Tool[] = ASSISTANT_TOOLS.map(
  (t) => ({
    name: t.name,
    description: t.description,
    input_schema: leanSchema(
      z.toJSONSchema(t.input, { io: "input" }),
    ) as Anthropic.Tool.InputSchema,
  }),
);

/** The status the widget shows for a tool, or null for an unknown name. */
export function toolStatus(name: string): AssistantToolStatus | null {
  return BY_NAME.get(name)?.status ?? null;
}

/** Results cut here: a tool that overflows costs tokens and says nothing more. */
const MAX_RESULT_CHARS = 12_000;

/**
 * Said with every card and page change: the visitor already reads the text the model wrote
 * before the call, and the chat streams every round's text, so a repeat is shown twice.
 */
const ALREADY_SHOWN =
  "The user already sees the card or page and any text you wrote before this call; do not repeat it, add at most one new sentence.";

/**
 * A tool call's input as the chat log stores it. The party's details (a rider's height, staying
 * dry, early entry) can come from the visitor's planner snapshot, which is passed to the model
 * and never stored, so the log keeps only which of them were set.
 */
export function storedToolInput(input: unknown): unknown {
  if (!input || typeof input !== "object" || Array.isArray(input)) return input;
  const { riderHeightCm, prefs, ...rest } = input as Record<string, unknown>;
  return {
    ...rest,
    ...(riderHeightCm !== undefined && { riderHeightCm: "set" }),
    ...(prefs !== null &&
      typeof prefs === "object" && { prefs: Object.keys(prefs) }),
  };
}

/**
 * Runs one tool call and returns what goes back to the model. A wrong name or
 * input, or a failing tool, becomes an error result the model can read; the
 * message of an unexpected error stays in the log.
 */
export async function runAssistantTool(
  name: string,
  rawInput: unknown,
  ctx: AssistantToolContext,
  onError: (error: unknown) => void,
): Promise<{ content: string; isError: boolean; action?: ClientAction }> {
  const definition = BY_NAME.get(name);
  if (!definition) return { content: `Unknown tool ${name}`, isError: true };
  const input = definition.input.safeParse(rawInput);
  if (!input.success) {
    return {
      content: `Invalid input: ${z.prettifyError(input.error)}`,
      isError: true,
    };
  }
  try {
    const outcome = await definition.run(input.data, ctx);
    const action =
      outcome instanceof ActionOutcome ? outcome.action : undefined;
    const plain = outcome instanceof ActionOutcome ? outcome.result : outcome;
    const shown =
      action?.type === "card" || action?.type === "navigate"
        ? { ...(plain as object), alreadyShown: ALREADY_SHOWN }
        : plain;
    const result = JSON.stringify(
      localizeTimes(localizePageUrls(shown, ctx.locale), ctx.timeZone ?? "UTC"),
    );
    return {
      content:
        result.length > MAX_RESULT_CHARS
          ? `${result.slice(0, MAX_RESULT_CHARS)}… (cut)`
          : result,
      isError: false,
      ...(action && { action }),
    };
  } catch (error) {
    if (error instanceof McpToolError)
      return { content: error.message, isError: true };
    onError(error);
    return {
      content: "The lookup failed; say you could not check it.",
      isError: true,
    };
  }
}
