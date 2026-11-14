import { HttpException, type Type } from "@nestjs/common";
import type Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { McpToolError, type McpToolContext } from "../../mcp/mcp-tool";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { findParksTool } from "../../mcp/tools/find-parks.tool";
import { getParkTool } from "../../mcp/tools/get-park.tool";
import {
  resolveAbbreviations,
  resolveCuratedPark,
  type FunFactLocale,
  type FunFacts,
} from "../../parks/utils/curated-park-facts.util";
import { findAttractionsTool } from "../../mcp/tools/find-attractions.tool";
import { getAttractionTool } from "../../mcp/tools/get-attraction.tool";
import {
  compactDay,
  parkForecastTool,
} from "../../mcp/tools/park-forecast.tool";
import { CalendarService } from "../../parks/services/calendar.service";
import { parseDateRange } from "../../common/utils/date-parsing.util";
import {
  type AmbiguousPark,
  isAmbiguous,
  nameKey,
  resolvePark,
} from "../../mcp/tools/park-ref";
import { attractionPageUrl, parkPageUrl } from "../../mcp/tools/links";
import {
  BEST_DAYS_WINDOW,
  BestDaysService,
} from "../../parks/services/best-days.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { ParksService } from "../../parks/parks.service";
import type { CalendarDay } from "../../parks/dto/integrated-calendar.dto";
import { ShowsService } from "../../shows/shows.service";
import type { Show } from "../../shows/entities/show.entity";
import type { ShowLiveData } from "../../shows/entities/show-live-data.entity";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../../attractions/utils/curated-attraction-facts.util";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { WeatherService } from "../../parks/weather.service";
import { PlanDayService } from "../../parks/services/plan-day.service";
import type { PlanDayTier } from "../../parks/dto/plan-day.dto";
import {
  addIsoDays,
  getCurrentDateInTimezone,
  lastYearDays,
} from "../../common/utils/date.util";
import { DataSource, In, IsNull, type EntityTarget } from "typeorm";
import type { IndoorOutdoor } from "../../common/types/indoor-outdoor.type";
import { Park } from "../../parks/entities/park.entity";
import { frontendParkPath } from "../../common/utils/frontend-url.util";
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
  isSheltered,
  LEFT_OUT_NAMES_LIMIT,
  partyTooShort,
  riderFits,
  RIDE_RANKINGS,
  RIDE_TYPE_FILTERS,
  SHELTER_FILTERS,
  shelterCoverage,
} from "../../attractions/utils/ride-facts.util";
import { getCountryISOCode } from "../../common/constants/country-codes.constant";
import {
  AssistantPostsClient,
  type FoundPost,
  type FoundTerm,
} from "./assistant-posts.client";
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
import {
  MAX_CALENDAR_DAYS,
  TripRegionService,
} from "../../trip-regions/trip-region.service";
import { TripPlanService } from "../../trip-regions/trip-plan.service";
import { TripAccuracyService } from "../../trip-regions/trip-accuracy.service";
import {
  TICKETS_PATTERN,
  type TripPlanQueryDto,
} from "../../trip-regions/dto/trip-region-query.dto";
import {
  findRegion,
  hotelPoint,
  ORLANDO,
  regionOfPark,
  MOVE_BUFFER_OPTIONS,
  stayRulesOf,
  TRIP_REGIONS,
  type RegionConfig,
} from "../../trip-regions/regions";
import {
  MAX_PARK_DAYS,
  MAX_TRIP_DAYS,
  MAX_VISITS_PER_PARK,
} from "../../trip-regions/trip-optimizer";
import { buildLegs, findLeg } from "../../trip-regions/travel";
import {
  closeMinute,
  closesEarly,
  isoInZone,
  readDaySlots,
  scheduleRowDate,
} from "../../trip-regions/trip-calendar.util";
import { usualDay } from "../../trip-regions/trip-region.assembly";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import { TripWhenService } from "../../trip-regions/trip-when.service";
import { normalizeWhenQuery } from "../../trip-regions/trip-when.assembly";
import {
  WHEN_MAX_DAYS,
  WHEN_MAX_LIMIT,
  WHEN_MAX_RANGE_DAYS,
  WHEN_MIN_DAYS,
} from "../../trip-regions/trip-when";
import type { TripWhenQueryDto } from "../../trip-regions/dto/trip-when.dto";
import {
  heatAndStormWatch,
  hubPath,
  hubPathOf,
  luggagePhrase,
  regionParkFor,
  seasonsSummary,
  slotsSummary,
  travelSummary,
  tripDatesSummary,
  vacationItinerary,
  weekdayOf,
  type AmbiguousRegionPark,
  type RegionParkName,
  type SeasonRow,
  type TripPlanLike,
} from "./assistant-trip";
import {
  changeTripParams,
  clockOfMinute,
  entryMinute,
  interviewCore,
  interviewGuide,
  interviewHolds,
  nextInterviewStep,
  openRefinements,
  optimizerEntry,
  pickTrip,
  planQueryOf,
  tripPlanChange,
  tripSearchOf,
  type TripChange,
  type VisitorTrip,
  waitAt,
  writeTripQuery,
} from "./assistant-plans";
import { PlanOptimizeService } from "../../planner/plan-optimize.service";
import type { PlanOptimizeRequestDto } from "../../planner/dto/plan-optimize.dto";
import { cachedPlanDay } from "../../parks/services/plan-day-cache";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import type { Redis } from "ioredis";

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
  cardDay,
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
  type PlannerPark,
  siteOnlyPath,
  type AssistantCard,
  type ClientAction,
  type PostCardRow,
} from "./assistant-actions";
import {
  DAY_INTERVIEW_CORE,
  DAY_INTERVIEW_REFINE,
  INTERVIEW_REFINE,
  type InterviewSnapshot,
  type InterviewStep,
  TRIP_INTERVIEW_CORE,
  TRIP_INTERVIEW_REFINE,
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
  /** The browser's IANA zone: "now" for the visitor, and the zone of a time no park claims. */
  timeZone?: string;
  /** Told the zone of every park a call resolves; its times are then shown in that park's clock. */
  notePark?: (timezone: string) => void;
  /** The visitor's trip planner as the browser sent it; the only source of entry ids. */
  planner?: PlannerSnapshot | null;
  /** The visitor's trips, one per region, the on-screen one first (`visitorTrips`). */
  trips?: readonly VisitorTrip[];
  /**
   * The trip regions live on park.fan, as the widget's registry marks them; absent (an older
   * widget): every region. Parky plans, changes and links these only (`regionLive`).
   */
  liveRegions?: ReadonlySet<string>;
  /** Parky's interview, while one runs, as the browser holds it. */
  interview?: InterviewSnapshot | null;
  /** The browser's push state and alerts as it sent them. */
  alerts?: AlertsSnapshot | null;
  /** The ids of the favourites as the browser sent them. */
  favorites?: FavoritesSnapshot | null;
  /** The ids of the rides the visitor marked as ridden, as the browser sent them. */
  ridden?: ReadonlySet<string>;
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
  return { admin: PARKY, get: ctx.get.bind(ctx), onPark: ctx.notePark };
}

/** The parts of a park's live crowd reading that describe this moment, not the day so far. */
const LIVE_CROWD_NOW = [
  "level",
  "avgWaitMinutes",
  "occupancyPercent",
  "comparedToTypicalPercent",
] as const;

/**
 * `get_park`'s summary, its crowd reading of this moment only while the park runs. Closed, the
 * reading sits at its floor, and Parky called a closed park "very low for the moment". The level
 * becomes null with the reason beside it; today's average and peak so far stay, since they
 * describe hours the park was open.
 */
async function parkNow(
  ctx: AssistantToolContext,
  input: { park: string; longestWaits: number },
): Promise<Record<string, unknown>> {
  const live = (await getParkTool.handler(input, mcp(ctx))) as Record<
    string,
    unknown
  >;
  if (live.status === "OPERATING" || !live.crowd) return live;
  const crowd = { ...(live.crowd as Record<string, unknown>) };
  for (const key of LIVE_CROWD_NOW) delete crowd[key];
  return {
    ...live,
    crowd: {
      ...crowd,
      level: null,
      levelWhy: `no live crowd while the park is ${String(live.status ?? "not reporting").toLowerCase()}; it exists only while the park runs`,
    },
  };
}

/** A day of the visitor's plan, the park checked; the planner snapshot is the only source. */
async function plannedDay(
  ctx: AssistantToolContext,
  reference: string,
  date: string,
) {
  const park = await planPark(mcp(ctx), reference);
  if ("ambiguous" in park) return park;
  const day = snapshotDay(ctx, park.slug, date);
  if (!day) {
    throw new McpToolError(
      `${park.name} on ${date} is not in the user's plan; open it with plan_day first. A party's height for a new day goes to get_park_day and plan_add_rides as riderHeightCm`,
    );
  }
  return { park, day };
}

/** The rides' rows by id, one query, for the filters that read curated facts. */
async function attractionsById(
  ctx: AssistantToolContext,
  rides: ReadonlyArray<{ id: string }>,
): Promise<Map<string, Attraction>> {
  if (rides.length === 0) return new Map();
  const found = await ctx
    .get(DataSource)
    .getRepository(Attraction)
    .find({ where: { id: In(rides.map((ride) => ride.id)) } });
  return new Map(found.map((ride) => [ride.id, ride]));
}

/** Each ride's hand-decided `indoorOutdoor`, absent where nobody decided it. */
function shelterMap(
  rows: ReadonlyMap<string, Attraction>,
): Map<string, IndoorOutdoor> {
  return new Map(
    [...rows.values()].flatMap((ride) =>
      ride.indoorOutdoor ? [[ride.id, ride.indoorOutdoor] as const] : [],
    ),
  );
}

/** `{ ridden: true }` for a ride the visitor marked as ridden, nothing otherwise. */
function riddenMark(ctx: AssistantToolContext, rideId: string) {
  return ctx.ridden?.has(rideId) ? { ridden: true as const } : {};
}

/** Enough for every waiting ride of the biggest park, so a list can be cut at either end. */
const ALL_WAITING_RIDES = 500;

/** How many of the longest waits a park's live summary carries. */
const LIVE_LONGEST_WAITS = 8;

/** How many of them the park card draws. */
const PARK_CARD_WAITS = 3;

/**
 * A park right now as `get_park_live` hands it to the model, which the park card returns too, so
 * one call answers both: the live summary, with the editors' fun facts in the chat's language.
 */
async function parkLiveSummary(
  ctx: AssistantToolContext,
  park: string,
): Promise<Record<string, unknown>> {
  const live = await parkNow(ctx, { park, longestWaits: LIVE_LONGEST_WAITS });
  // The park's clock right now and its zone stay: every time reaches the model in that zone.
  const { funFacts, ...rest } = live;
  if (rest.retiredAt === null) delete rest.retiredAt;
  // The day's rating beside the live one: before opening the live level is null, and "how full
  // will it get today?" is the calendar's to answer, labelled as such.
  const entity =
    typeof rest.id === "string"
      ? await ctx
          .get(ParksService)
          .findById(rest.id)
          .catch(() => null)
      : null;
  if (entity) {
    const today = getCurrentDateInTimezone(entity.timezone);
    const day = await calendarDays(ctx, entity, [today])
      .then(({ days }) => days.get(today))
      .catch(() => undefined);
    if (day) {
      rest.todayExpected = {
        crowdLevel: day.crowdLevel,
        expectedPeakWaitMinutes: day.headlinerForecast?.avgWait,
        note: "the calendar's rating of the whole day, not the live level",
      };
    }
  }
  // English stands in for an untranslated locale: the model retells it in the chat's language.
  const facts = funFacts as FunFacts | null;
  const told = facts?.[ctx.locale as FunFactLocale] ?? facts?.en;
  if (told) rest.funFacts = told;
  return rest;
}

/** A ride by its id, or by its name with its park, so no search round goes before the tool. */
const rideRef = {
  rideId: z
    .string()
    .uuid()
    .optional()
    .describe("Ride id from find_rides or a ride list"),
  ride: z
    .string()
    .trim()
    .min(2)
    .max(100)
    .optional()
    .describe('Instead of rideId: the ride\'s name, e.g. "Taron", with park'),
};

/** Rides of the same name to pick from, handed back instead of a guess. */
interface AmbiguousRide {
  ambiguous: true;
  hint: string;
  rides: Array<{ id: string; name: string; park: string | null }>;
}

/**
 * The ride's id from `rideId`, or from `ride` (a name) within `park`: the only match, or the one
 * whose name is the reference itself; several equal candidates come back to pick from.
 */
async function rideIdOf(
  ctx: AssistantToolContext,
  input: { rideId?: string; ride?: string; park?: string },
): Promise<string | AmbiguousRide | AmbiguousPark> {
  if (input.rideId) return input.rideId;
  if (!input.ride) throw new McpToolError("Give rideId, or ride with its park");
  const park = input.park ? await resolvePark(mcp(ctx), input.park) : null;
  if (park && isAmbiguous(park)) return park;
  const found = (await findAttractionsTool.handler(
    { query: input.ride, parkId: park?.id, limit: 5 },
    mcp(ctx),
  )) as {
    attractions?: Array<{ id: string; name: string; park?: { name?: string } }>;
  };
  const hits = found.attractions ?? [];
  if (hits.length === 0) {
    throw new McpToolError(
      `No ride "${input.ride}"${park ? ` at ${park.name}` : ""}; try find_rides`,
    );
  }
  const key = nameKey(input.ride);
  const exact = hits.filter((hit) => nameKey(hit.name) === key);
  const pick = hits.length === 1 ? hits : exact;
  if (pick.length === 1) return pick[0].id;
  return {
    ambiguous: true,
    hint: "Several rides match; call again with one rideId",
    rides: (exact.length > 1 ? exact : hits).map((hit) => ({
      id: hit.id,
      name: hit.name,
      park: hit.park?.name ?? null,
    })),
  };
}

/**
 * One ride as get_ride returns it: the ride's answer without the readings of today one by one,
 * its unknown facts named, and, from its park's day, when it opens today and when it reopens.
 * The ride card hands the same to the model, so no get_ride follows a card.
 */
async function rideDetails(
  ctx: AssistantToolContext,
  rideId: string,
): Promise<Record<string, unknown>> {
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
  const facts = Object.entries(ride.facts as Record<string, unknown>);
  ride.facts = Object.fromEntries(facts.filter(([, value]) => value !== null));
  // Named, so a missing height reads "not recorded", never "no limit".
  const unknownFacts = facts
    .filter(([, value]) => value === null)
    .map(([key]) => key);
  if (unknownFacts.length > 0) ride.unknownFacts = unknownFacts;
  const parkId = (ride.park as { id?: string } | null)?.id;
  const parks = ctx.get(ParksService);
  const park = parkId ? await parks.findById(parkId) : null;
  if (park) ctx.notePark?.(park.timezone);
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
      ride.reopening = {
        ...reopening,
        ...(from && { reopensOn: await firstOpenDay(ctx, park.id, from) }),
      };
    }
  }
  return ride;
}

/** The visitor's planned day at this park and date, as the planner snapshot holds it. */
function snapshotDay(
  ctx: AssistantToolContext,
  parkSlug: string,
  date: string,
): PlannerDay | undefined {
  return ctx.planner?.days.find(
    (planned) => planned.park.slug === parkSlug && planned.date === date,
  );
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

/**
 * A park's own sub-pages `open_page` can open, by their English segment, which the widget respells
 * in the reader's language (`localizedPath` in the frontend).
 */
const PARK_SECTIONS = ["calendar", "averages", "kids"] as const;
type ParkSection = (typeof PARK_SECTIONS)[number];
const PARK_SECTION_SEGMENTS: Record<ParkSection, string> = {
  calendar: "wait-time-calendar",
  averages: "average-wait-times",
  kids: "with-kids",
};

/** The years the average-wait page reads, the frontend's `PARK_STATS_PAGE_QUERY.years`. */
const AVERAGES_PAGE_YEARS = 2;

/**
 * When the with-kids page exists: the frontend's `KIDS_PAGE_GATE` (`lib/parks/kids-page.ts`),
 * held against the park's rides here so Parky never opens a 404.
 */
const KIDS_PAGE_GATE = { minRidesWithHeight: 20, minShareWithHeight: 0.5 };

/**
 * Why a park has no such sub-page, or null when it has one. Every park has the calendar; the
 * average-wait page needs enough measured days, the with-kids page enough known rider heights.
 */
async function missingSection(
  ctx: AssistantToolContext,
  park: Park,
  section: ParkSection,
): Promise<string | null> {
  if (section === "averages") {
    const stats = await ctx
      .get(ParkHistoricalStatsService)
      .getParkHistoricalStats(park, AVERAGES_PAGE_YEARS)
      .catch(() => null);
    if (!stats) {
      return `the average waits of ${park.name} cannot be read just now`;
    }
    return stats.meta.displayable
      ? null
      : `${park.name} has no average-wait page yet: only ${stats.meta.totalSampleDays} measured days`;
  }
  if (section === "kids") {
    const rides = await ctx
      .get(DataSource)
      .getRepository(Attraction)
      .find({ where: { parkId: park.id, retiredAt: IsNull() } });
    const withHeight = rides.filter(
      (ride) => (resolveCuratedFacts(ride).minimumHeight ?? 0) > 0,
    ).length;
    const enough =
      withHeight >= KIDS_PAGE_GATE.minRidesWithHeight &&
      rides.length > 0 &&
      withHeight / rides.length >= KIDS_PAGE_GATE.minShareWithHeight;
    return enough
      ? null
      : `${park.name} has no with-kids page: park.fan knows the minimum height of only ${withHeight} of its ${rides.length} rides`;
  }
  return null;
}

/**
 * The site's pages without one park, ride or post that `open_page` can open. `day_planner` is the
 * day planner (`/trip-planner` on the site); `trip_hub` a live region's hub with its trip planner.
 */
const SITE_PAGES = [
  "alerts",
  "best_time",
  "favorites",
  "day_planner",
  "how_it_works",
  "fancast",
  "trip_hub",
] as const;
const SITE_PAGE_PATHS: Record<
  Exclude<(typeof SITE_PAGES)[number], "trip_hub">,
  string
> = {
  alerts: "/alerts",
  best_time: "/best-time-to-visit",
  favorites: "/favorites",
  day_planner: "/trip-planner",
  how_it_works: "/how-park-fan-works",
  fancast: "/fancast",
};

/** The region a hub path (`/trip-regions/<slug>…`) belongs to. */
const HUB_REGION = /^\/trip-regions\/([a-z0-9-]+)/;

/**
 * A post's or glossary term's page: `/<locale>/blog|news/<slug>`, or the glossary under its
 * segment in any of the six languages (`/de/glossar/<slug>`).
 */
const CONTENT_PATH =
  /^\/(?:en|de|nl|fr|es|it)\/(?:blog|news|glossary|glossar|glossaire|glossario|woordenboek|glosario)\/[a-z0-9]+(?:-[a-z0-9]+)*$/;

/** A slug as park.fan's posts and glossary terms have them. */
const CONTENT_SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

/** At most this many posts or glossary terms on one card: a chat bubble, not the blog. */
const MAX_POST_CARD = 3;
const MAX_GLOSSARY_CARD = 3;

/** A cover on park.fan itself, optionally versioned (`/media/…/cover.webp?v=3`). */
const SITE_IMAGE = /^\/[A-Za-z0-9._\-/]{1,250}(?:\?[A-Za-z0-9=&._-]{0,60})?$/;
/** A CSS `object-position` in percent, as the post covers carry it. */
const OBJECT_POSITION = /^\d{1,3}(?:\.\d+)?% \d{1,3}(?:\.\d+)?%$/;

/** Posts matching `query` in the chat's language, best first, or the posts with `slugs`. */
async function findPosts(
  ctx: AssistantToolContext,
  query: string,
  kind: "news" | "article" | "any",
  slugs?: readonly string[],
): Promise<FoundPost[]> {
  const found = (await ctx
    .get(AssistantPostsClient)
    .search(ctx.locale, query, kind, slugs)) as { posts?: FoundPost[] };
  return (found.posts ?? []).filter((post) => CONTENT_SLUG.test(post.slug));
}

/** What the model reads of a found post; the cover and its crop say nothing to it. */
function postRow(post: FoundPost) {
  return {
    title: post.title,
    description: post.description,
    date: post.date,
    // The search dates an article by its last change; the post's own date is its first.
    ...(post.updated && { updated: true }),
    kind: post.kind,
    slug: post.slug,
    url: post.url,
  };
}

/** One post's text for a question, or why it could not be read this time. */
async function readPost(
  ctx: AssistantToolContext,
  slug: string,
  question: string,
): Promise<unknown> {
  try {
    return await ctx.get(AssistantPostsClient).post(ctx.locale, slug, question);
  } catch {
    return { error: "could not read the post right now" };
  }
}

/** A found post as a card row, or null when its page is not on park.fan. */
function postCardRow(post: FoundPost): PostCardRow | null {
  const url = siteOnlyPath(post.url);
  if (!url) return null;
  const image =
    typeof post.image === "string" && SITE_IMAGE.test(post.image)
      ? post.image
      : null;
  return {
    title: post.title,
    url,
    date: post.date.slice(0, 10),
    updated: post.updated === true,
    news: post.kind === "news",
    readingTimeMinutes:
      typeof post.readingTimeMinutes === "number"
        ? post.readingTimeMinutes
        : null,
    excerpt: post.description ?? null,
    category: post.category ?? null,
    image,
    imagePosition:
      image &&
      typeof post.imagePosition === "string" &&
      OBJECT_POSITION.test(post.imagePosition)
        ? post.imagePosition
        : null,
  };
}

/** Rows of a rides card when the model names no limit. */
const DEFAULT_RIDE_ROWS = 6;

/**
 * Posts as a card: the best matches for a query, or the posts the model already has by slug.
 * With a question, the result also carries the first post's text, so showing a guide and
 * answering from it takes one call.
 */
async function postsCard(
  ctx: AssistantToolContext,
  {
    query,
    postKind,
    slugs,
    question,
    limit,
  }: {
    query?: string;
    postKind: "news" | "article" | "any";
    slugs?: string[];
    question?: string;
    limit?: number;
  },
) {
  if (!query && !slugs?.length) {
    throw new McpToolError("kind posts needs query or slugs");
  }
  const found = await findPosts(
    ctx,
    slugs?.length ? "" : (query ?? ""),
    slugs?.length ? "any" : postKind,
    slugs,
  );
  const picked = found.slice(
    0,
    Math.min(limit ?? MAX_POST_CARD, MAX_POST_CARD),
  );
  const rows = picked.flatMap((post) => postCardRow(post) ?? []);
  if (rows.length === 0) {
    return {
      shown: 0,
      why: slugs?.length
        ? "no park.fan post has these slugs in this language"
        : `no park.fan ${postKind === "any" ? "post" : postKind} matches "${query}" in this language`,
    };
  }
  const card: AssistantCard = { kind: "posts", posts: rows };
  return new ActionOutcome(
    { type: "card", card },
    {
      shown: `${rows.length} posts`,
      posts: picked.map(postRow),
      ...(question && {
        read: await readPost(ctx, picked[0].slug, question),
      }),
    },
  );
}

/** Glossary terms as a card: the best match, or up to three when the model asks. */
async function glossaryCard(
  ctx: AssistantToolContext,
  query: string,
  limit?: number,
) {
  const terms = (await findTerms(ctx, query)).slice(
    0,
    Math.min(limit ?? 1, MAX_GLOSSARY_CARD),
  );
  const rows = terms.flatMap((term) => {
    const url = siteOnlyPath(term.url);
    return url ? [{ ...term, url }] : [];
  });
  if (rows.length === 0) {
    return { shown: 0, why: `the glossary has no term for "${query}"` };
  }
  const card: AssistantCard = { kind: "glossary", terms: rows };
  return new ActionOutcome(
    { type: "card", card },
    { shown: `${rows.length} glossary terms`, terms },
  );
}

/** Glossary terms for a word, best first, each with its page in the chat's language. */
async function findTerms(ctx: AssistantToolContext, term: string) {
  const found = (await ctx
    .get(AssistantPostsClient)
    .glossary(ctx.locale, term)) as { results?: FoundTerm[] };
  return (found.results ?? [])
    .filter((t) => CONTENT_SLUG.test(t.slug))
    .map((t) => ({
      name: t.name,
      definition: t.shortDefinition,
      category: t.category ?? null,
      // The frontend names the canonical page (`/de/glossar/…`); `/glossary/` redirects there.
      url:
        t.url && siteOnlyPath(t.url)
          ? t.url
          : `${getFrontendUrl()}/${ctx.locale}/glossary/${t.slug}`,
    }));
}

const parkRef = z
  .string()
  .min(2)
  .max(120)
  .describe(
    'Park id, its slug like "phantasialand", or its name like "Europa-Park" (several matches come back to pick from)',
  );

const isoDay = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);

/** A rider's height in cm, as the planner and the height filters take it. */
const riderHeight = z.number().int().min(50).max(250);

/** How far back `get_park_history` reads a single day: a year, as its year-back view. */
const HISTORY_DAYS = 366;

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
      ? (parkNow(ctx, {
          park: found.id,
          longestWaits: 0,
        }) as Promise<LiveParkSummary>)
      : null,
  ]);
  if (kind === "calendar") {
    const card = calendarCard(forecast, first, count);
    const open = card.days.filter((day) => day.status === "OPERATING").length;
    return new ActionOutcome(
      { type: "card", card },
      {
        shown: `${card.days.length} days of ${card.park} from ${first}, ${open} open`,
        // The days as the card shows them, so the sentence after it can name a pick; UNKNOWN
        // is a day whose hours are not published yet, never a closure.
        days: card.days.map(({ date, status, crowdLevel, holiday }) => ({
          date,
          status,
          crowdLevel,
          ...(holiday && { holiday }),
        })),
      },
    );
  }
  const card = tilesCard(forecast, first, summary);
  const { url: _url, ...shown } = card;
  const tileDay = forecast.days.find((day) => day.date === first);
  const status = tileDay
    ? cardDay(tileDay, forecast.scheduleCoverage, forecast.park.timezone).status
    : "UNKNOWN";
  return new ActionOutcome(
    { type: "card", card },
    {
      shown,
      // The tiles carry no status: a closed day or one without published hours says so here.
      status,
      ...(status === "UNKNOWN" && {
        why: "the park has not published that day's hours; the tiles show park.fan's estimate",
      }),
    },
  );
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
      // A closed park's live level sits at its floor and is no reading at all.
      crowdLevel:
        park.status === "OPERATING"
          ? (park.analytics?.crowdLevel ?? null)
          : null,
      avgWaitMinutes:
        park.status === "OPERATING"
          ? (park.analytics?.avgWaitTime ?? null)
          : null,
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
  const timed = new Set(listed.map((show) => show.id));
  return {
    shows: listed.slice(0, MAX_SHOWS),
    withShowtimes: listed.length,
    withoutShowtimes: shows.length - listed.length,
    // Named, so "is the parade on?" gets "no time published", not "there is none".
    withoutShowtimesNames: shows
      .filter((show) => !timed.has(show.id))
      .slice(0, MAX_SHOWS)
      .map((show) => show.name),
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

/** Most parks one `compare_parks` reads; a country beyond it is asked for a city. */
const MAX_COMPARED_PARKS = 30;

/** Calendars `compare_parks` builds at once: a cold month is a full build, so never thirty. */
const CALENDAR_BATCH = 6;

/** `run` over `items`, `size` at a time, results in order. */
async function inBatches<T, R>(
  items: readonly T[],
  size: number,
  run: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (let at = 0; at < items.length; at += size) {
    results.push(...(await Promise.all(items.slice(at, at + size).map(run))));
  }
  return results;
}

/**
 * The park's first day with published opening hours on or after `from` (YYYY-MM-DD), a year
 * ahead at most, or null when none is published.
 */
async function firstOpenDay(
  ctx: AssistantToolContext,
  parkId: string,
  from: string,
): Promise<string | null> {
  const opening = await ctx
    .get(ParksService)
    .getFirstOperatingDayFrom(parkId, from);
  if (!opening) return null;
  // The DATE column arrives as "YYYY-MM-DD" from the driver; a Date is its UTC day.
  return typeof opening.date === "string"
    ? (opening.date as string).slice(0, 10)
    : opening.date.toISOString().slice(0, 10);
}

/**
 * The last day the park has published opening hours for (park-local YYYY-MM-DD), or null when
 * it publishes none: past it a day's status is the calendar's estimate.
 */
async function publishedUntil(
  ctx: AssistantToolContext,
  park: Park,
): Promise<string | null> {
  // A failed lookup reads as "not published": the day is then an estimate, never a fact.
  const range = await Promise.resolve()
    .then(() =>
      ctx.get(ParksService).getOperatingDateRange(park.id, park.timezone),
    )
    .catch(() => null);
  return range?.maxDate ?? null;
}

/** How far ahead `get_days` reads a day: the yearly forecast's reach. */
const DAYS_AHEAD = 365;

/** How many quiet hours `get_hours` names. */
const BEST_HOURS = 3;

/**
 * What a day plan's numbers are, in the words the model passes on: the tier decides whether
 * an hour is a forecast, a spread day forecast or last year's pattern (plan-day/tiers.md).
 */
const TIER_MEANING: Record<PlanDayTier, string> = {
  observed: "what the queues did that day",
  measured: "park.fan's hourly forecast",
  composed: "the day's forecast spread over each ride's usual day",
  climatology: "how comparable days went a year earlier: not a forecast",
  long_range: "too far ahead for an hourly curve",
};

/** How many of a stretch's quietest open days `get_days` ranks. */
const QUIETEST_DAYS = 5;

/** The days `get_days` reads when it is given none: the week ahead. */
const DEFAULT_DAYS = 7;

/** Most days one `get_days` reads: ten named days, or a month as a range. */
const MAX_COMPARED_DATES = 31;

/**
 * One park's days as the calendar every park page shows them, read month by month so a
 * comparison of May and December builds two months, not the eight between them.
 */
async function calendarDays(
  ctx: AssistantToolContext,
  park: Park,
  dates: readonly string[],
) {
  const byMonth = new Map<string, string[]>();
  for (const date of dates) {
    const month = date.slice(0, 7);
    byMonth.set(month, [...(byMonth.get(month) ?? []), date]);
  }
  const calendars = await Promise.all(
    [...byMonth.values()].map((days) => {
      const sorted = [...days].sort();
      const { fromDate, toDate } = parseDateRange(
        sorted[0],
        sorted[sorted.length - 1],
        { timezone: park.timezone },
      );
      return ctx
        .get(CalendarService)
        .buildCalendarResponse(park, fromDate, toDate, "none");
    }),
  );
  const days = new Map(
    calendars.flatMap((calendar) => calendar.days.map((d) => [d.date, d])),
  );
  return {
    days,
    coverage: calendars[0]?.meta.scheduleCoverage ?? { from: null, to: null },
    hasOperatingSchedule: calendars[0]?.meta.hasOperatingSchedule ?? false,
  };
}

/** One park's day for `compare_parks`, from the calendar every park page shows, or why not. */
async function comparedParkDay(
  ctx: AssistantToolContext,
  park: Park,
  date: string | undefined,
) {
  const today = getCurrentDateInTimezone(park.timezone);
  const day = date ?? today;
  const ahead = daysBetween(today, day);
  if (ahead < 0) {
    return { park, day, why: "that day is over in the park's time zone" };
  }
  if (ahead > DAYS_AHEAD) {
    return {
      park,
      day,
      why: `park.fan looks ahead ${DAYS_AHEAD} days`,
    };
  }
  // A week either side: the park's usual close, which a day guest's close is read against.
  const window = Array.from({ length: 15 }, (_, i) =>
    addIsoDays(day, i - EARLY_CLOSE_WINDOW_DAYS),
  );
  const [{ days, coverage, hasOperatingSchedule }, rows] = await Promise.all([
    calendarDays(ctx, park, window),
    // The fortnight's evening rows: a party night is no usual day. Without
    // them the closes alone decide.
    Promise.resolve()
      .then(() => {
        const { fromDate, toDate } = parseDateRange(
          window[0],
          window[window.length - 1],
          { timezone: park.timezone },
        );
        return ctx.get(ParksService).getSchedule(park.id, fromDate, toDate);
      })
      .catch(() => []),
  ]);
  const found = days.get(day);
  if (!found) return { park, day, why: "the calendar has no such day" };
  return {
    park,
    day,
    found: compactDay(found, park.timezone),
    published:
      hasOperatingSchedule && coverage.to !== null && day <= coverage.to,
    // Null for a park that publishes no hours at all, whatever the coverage says.
    publishedUntil: hasOperatingSchedule ? coverage.to : null,
    earlyClose: dayGuestEarlyClose(park, day, days, rows),
  };
}

/** Days either side of a compared day that give a park's usual close. */
const EARLY_CLOSE_WINDOW_DAYS = 7;

/**
 * Whether the day guests must leave early on a day, read the way the trip
 * calendar reads it (`trip-calendar.util.ts`): the OPERATING close two hours or
 * more before the park's usual close over the surrounding fortnight, or a
 * ticketed evening (a party night) after a close earlier than usual. Null
 * when the day is a full one or its close is unknown.
 */
function dayGuestEarlyClose(
  park: Park,
  day: string,
  days: Map<string, CalendarDay>,
  rows: Parameters<typeof readDaySlots>[0],
): { closes: string; usualClose: string } | null {
  const rowsOn = (date: string) =>
    rows.filter((r) => scheduleRowDate(r, park.timezone) === date);
  const near = [...days.values()]
    .filter((d) => d.status === "OPERATING" && d.hours && !d.hours.isInferred)
    .map((d) => ({
      date: d.date,
      c: {
        status: "OPERATING" as const,
        hoursPublished: true,
        crowdLevel: d.crowdLevel,
        basis: "none" as const,
        closesAt: isoInZone(d.hours!.closingTime, park.timezone),
        evening: readDaySlots(rowsOn(d.date), park.timezone, "early_entry")
          .evening,
      },
    }));
  const today = near.find((d) => d.date === day);
  const usual = usualDay(near).close;
  if (!today || usual === undefined) return null;
  const closesAt = today.c.closesAt;
  const party = today.c.evening?.kind === "ticketed_event";
  const early =
    closesEarly(day, closesAt, usual) ||
    (party && closeMinute(day, closesAt) < usual);
  if (!early) return null;
  const clock = (minute: number) =>
    `${String(Math.floor(minute / 60) % 24).padStart(2, "0")}:${String(Math.round(minute % 60)).padStart(2, "0")}`;
  return {
    closes: clock(closeMinute(day, closesAt)),
    usualClose: clock(usual),
  };
}

/** Parks whose expected waits are this close count as alike, and a full day goes first. */
const SIMILAR_WAIT_MINUTES = 15;

/**
 * A park the day guests must leave early moves behind every full-day park
 * with a similar expected wait (within {@link SIMILAR_WAIT_MINUTES}), so a party
 * night's quieter queues do not win on minutes the visitor cannot use.
 */
function behindFullDays<
  T extends {
    found: { expectedPeakWaitMinutes?: number };
    earlyClose?: unknown;
  },
>(sorted: T[]): T[] {
  const out = [...sorted];
  for (let moved = true; moved;) {
    moved = false;
    for (let i = 0; i + 1 < out.length; i++) {
      const [a, b] = [out[i], out[i + 1]];
      const gap = Math.abs(
        (a.found.expectedPeakWaitMinutes ?? 0) -
          (b.found.expectedPeakWaitMinutes ?? 0),
      );
      if (a.earlyClose && !b.earlyClose && gap <= SIMILAR_WAIT_MINUTES) {
        [out[i], out[i + 1]] = [b, a];
        moved = true;
      }
    }
  }
  return out;
}

/** The trip regions the trip tools take (`region`), Orlando first. */
const REGION_SLUGS = Object.keys(TRIP_REGIONS) as [string, ...string[]];

/** The trip regions' names in one phrase: "Orlando", "Orlando, Japan and Paris". */
function tripRegionNames(): string {
  const names = Object.values(TRIP_REGIONS).map((r) => r.name);
  return names.length <= 1
    ? (names[0] ?? "")
    : `${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

/** The `region` every trip tool takes; Orlando without it, as before regions were many. */
const regionInput = z
  .enum(REGION_SLUGS)
  .optional()
  .describe(`The trip region: ${REGION_SLUGS.join(", ")}; default orlando`);

/** A hotel kind, validated by the region (Orlando's in the description). */
const hotelInput = z
  .string()
  .regex(/^[a-z0-9_]+$/)
  .optional()
  .describe(
    `Whose perks count; default the area's. Orlando: ${Object.keys(ORLANDO.perks).join(", ")}`,
  );

/** A hotel area of the region, validated by it (Orlando's in the description). */
const areaInput = z
  .string()
  .regex(/^[a-z0-9_-]+$/)
  .optional()
  .describe(
    `Where they stay, a hotel area of the region. Orlando: ${ORLANDO.hotelAreas.map((a) => a.key).join(", ")}`,
  );

/**
 * How Parky writes the tickets a visitor holds: the compact form of `tickets=`
 * (docs/frontend/trip-regions.md#tickets), with the ids a visitor names most.
 */
const TICKETS_DESCRIPTION =
  "Tickets they already have, comma-separated: product[.days][.h|.h?][@first day]. .h: with " +
  "Park Hopper or park-to-park; .h?: they don't know (planned without, the result says what " +
  "it would add). Orlando's products: wdw-date-based.1-10, wdw-date-based-plus.2-10 (water parks), " +
  "wdw-uk-14-day-magic, wdw-uk-7-day-magic, wdw-4-park-magic, wdw-annual-pass, " +
  "uor-date-based.1-5, uor-date-based-volcano-bay.3-5, uor-1-day-plus-epic, uor-epic-1-day, " +
  "uor-uk-all-parks, uor-uk-three-park-adventure, uor-annual-pass, seaworld-1-day, " +
  "seaworld-two-park, seaworld-three-park-3-day, seaworld-uk-3-visit, discovery-cove, " +
  "legoland-1-day, legoland-2-day; the rest in the Orlando hub's ticket list. Another " +
  "region's ids: an unknown id is answered with the region's list. Without a first day the plan picks it";

/** The days `get_park_events` reads when given no end: a month. */
const DEFAULT_EVENT_DAYS = 31;

/** Whether a trip region is live on park.fan, so Parky may plan, change and link it. */
export function regionLive(
  ctx: Pick<AssistantToolContext, "liveRegions">,
  slug: string,
): boolean {
  return !ctx.liveRegions || ctx.liveRegions.has(slug);
}

/** The refusal for a region not live yet: its hub is not on the site, so nothing links there. */
function comingRegion(slug: string): McpToolError {
  const name = TRIP_REGIONS[slug]?.name ?? slug;
  return new McpToolError(
    `${name}'s trip planner is not on park.fan yet; it opens later. Do not plan, offer or link a ${name} trip: say it is coming, and help with what park.fan has`,
  );
}

/**
 * A trip region and its parks as the database has them, named as the site names them; a region
 * not live yet is refused (`regionLive`).
 */
async function tripRegion(ctx: AssistantToolContext, slug = ORLANDO.slug) {
  if (!regionLive(ctx, slug)) throw comingRegion(slug);
  const regions = ctx.get(TripRegionService);
  // With its ticket catalog: the product names and links Parky gives are the admin's.
  const region = await regions.regionWithTickets(slug);
  const resolved = await regions.resolveParks(region);
  const parks: RegionParkName[] = resolved.map(({ config, park }) => ({
    slug: config.slug,
    id: park.id,
    name: resolveCuratedPark(park).name,
    abbreviations: resolveAbbreviations(park),
  }));
  return {
    regions,
    region,
    parks,
    names: new Map(parks.map((p) => [p.slug, p.name])),
  };
}

/**
 * The region park a reference names, else the candidates. A park elsewhere is refused with
 * `elsewhere`, and a park of another trip region names that region, so the model says what is
 * missing, or calls again with the right region, instead of guessing.
 */
async function regionPark(
  ctx: AssistantToolContext,
  reference: string,
  parks: readonly RegionParkName[],
  elsewhere: string,
): Promise<RegionParkName | AmbiguousRegionPark | AmbiguousPark> {
  const matched = regionParkFor(reference, parks);
  if (matched) return matched;
  const found = await resolvePark(mcp(ctx), reference);
  if (isAmbiguous(found)) return found;
  const inRegion = parks.find((p) => p.id === found.id);
  if (!inRegion) {
    const other = regionOfPark(found);
    if (other && !regionLive(ctx, other.slug)) throw comingRegion(other.slug);
    throw new McpToolError(
      other
        ? `${found.name} is in the trip region ${other.slug}: call again with region ${other.slug}`
        : `${found.name}: ${elsewhere}`,
    );
  }
  return inRegion;
}

/** A trip region park's heat and storm warnings on a compared day; nothing elsewhere. */
function regionWatch(
  park: Park,
  day: { date?: string; weather?: { tempMax?: number; basis?: string } | null },
): { watch?: string[] } {
  const region = regionOfPark(park);
  const config = region?.parks.find((p) => p.slug === park.slug);
  if (!region || !config || !day.date) return {};
  const watch = heatAndStormWatch(
    region,
    day.date,
    day.weather,
    config.type === "theme_park",
  );
  return watch.length > 0 ? { watch } : {};
}

/**
 * A trip's plan as the model retells it (`vacationItinerary`): normalized and planned by the plan
 * route's own service, an hour in Redis by the normalized query, so the planner's own request and
 * Parky's read share one plan. A 400 is a refusal the model reads.
 */
async function itineraryOf(
  ctx: AssistantToolContext,
  trip: Awaited<ReturnType<typeof tripRegion>>,
  query: TripPlanQueryDto,
) {
  const { regions, region, parks, names } = trip;
  const plans = ctx.get(TripPlanService);
  const normalized = await tripCall(() => plans.normalize(region.slug, query));
  const plan = await tripCall(() => plans.plan(region.slug, query));
  // The plan read this calendar a moment ago: an hour in Redis, so this is a cache hit.
  const calendar = await regions.internalCalendar(
    region,
    normalized.from,
    normalized.to,
  );
  return vacationItinerary({
    plan,
    calendar,
    query: normalized,
    region,
    names,
    ids: new Map(parks.map((p) => [p.slug, p.id])),
  });
}

/**
 * The plan of the visitor's trip in a region, for "This chat"'s digest: null when the trip has no
 * dates or the plan route refuses it.
 */
export async function visitorTripItinerary(
  ctx: AssistantToolContext,
  trip: VisitorTrip,
): Promise<ReturnType<typeof vacationItinerary> | null> {
  const query = planQueryOf(trip.params);
  if (!query) return null;
  return itineraryOf(ctx, await tripRegion(ctx, trip.region.slug), query);
}

/** The visitor's trip a trip tool means, or a refusal that says what is missing. */
function visitorTrip(ctx: AssistantToolContext, region?: string): VisitorTrip {
  const trips = ctx.trips ?? [];
  const trip = pickTrip(trips, region);
  if (trip) return trip;
  throw new McpToolError(
    trips.length > 1
      ? `The user has trips in ${trips.map((t) => t.region.slug).join(" and ")}: call again with region`
      : "The user has no trip in the trip planner; plan_vacation plans one, change_trip with dates starts one in their planner",
  );
}

/**
 * The plan route's refusals the trip planner shows as a problem with a way out rather than an
 * error: more visits than the days hold. A change that leads there is still made.
 */
const PLANNER_PROBLEM = /^More park visits|park visits$/;

/** A region with bases: the stays a visitor fixes, as the planner's `bases=`. */
const basesInput = z
  .string()
  .regex(
    /^[a-z0-9-]+(:\d{1,2})?(@[a-z0-9_-]+)?(,[a-z0-9-]+(:\d{1,2})?(@[a-z0-9_-]+)?){0,9}$/,
  )
  .optional()
  .describe(
    "A region with bases (Japan, Paris): the stays they fixed, base[:nights][@hotel area], comma-separated; :0 means no night there (its parks a day trip); the plan picks the rest. In change_trip the whole list, empty clears it",
  );

/** A base of a region with bases, by its key (`tokyo`, `disneyland-paris`). */
const baseInput = z
  .string()
  .regex(/^[a-z0-9-]+$/)
  .optional();

/** An airport or station a region's bases are reached from, by its code. */
const gatewayInput = z
  .string()
  .regex(/^[A-Za-z]{3,4}$/)
  .optional();

/**
 * The base a gateway starts or ends a trip in: the one base that lists it, or none (the plan
 * picks) where several do, as the planner maps "you land in" (trip-regions.md#bases).
 */
export function gatewayBase(region: RegionConfig, code: string): string {
  const wanted = code.toUpperCase();
  const served = (region.bases ?? []).filter((b) =>
    (b.gateways ?? []).some((g) => g.code === wanted),
  );
  if (served.length === 0) {
    const codes = (region.bases ?? []).flatMap((b) =>
      (b.gateways ?? []).map((g) => `${g.code} (${g.name})`),
    );
    throw new McpToolError(
      `${wanted} is not a way into ${region.name}: ${[...new Set(codes)].join(", ")}`,
    );
  }
  return served.length === 1 ? served[0].key : "";
}

/** The nights the plan's stay suggestion fixes, read off the plan route's own answer (cached). */
async function staySuggestionOf(
  ctx: AssistantToolContext,
  region: string,
  query: TripPlanQueryDto,
): Promise<Record<string, number> | null> {
  const plan = (await tripCall(() =>
    ctx.get(TripPlanService).plan(region, query),
  )) as unknown as TripPlanLike;
  const suggestion = plan.meta.staySuggestion as
    { nights?: Record<string, number> } | undefined;
  return suggestion?.nights && Object.keys(suggestion.nights).length > 0
    ? suggestion.nights
    : null;
}

/**
 * A region with bases: the minutes a hotel change takes on top of the train, the visitor's choice
 * (PAR-1304); the region's own without it.
 */
const moveBufferInput = z
  .union([z.literal(30), z.literal(60), z.literal(90), z.literal(120)])
  .optional()
  .describe(
    "A region with bases: minutes for each hotel change on top of the train; 30 with bags sent ahead by courier, 60 carried along, 90 or 120 for a slower pace. Default the region's (Japan 30)",
  );

/** A trip route's 400 or 404 as a refusal the model reads. */
async function tripCall<T>(run: () => Promise<T> | T): Promise<T> {
  try {
    return await run();
  } catch (error) {
    if (error instanceof HttpException) throw new McpToolError(error.message);
    throw error;
  }
}

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
      "The park right now: open or closed, today's hours and the next opening, the live crowd " +
      "level (null with levelWhy while closed), todayExpected (the calendar's rating of the whole " +
      'day, for "how full will it get today"), ' +
      "average and peak waits today, weather now, only the 8 longest current standby waits, " +
      "rides that are down, rides in maintenance (window: until when), maintenance starting in " +
      "the next 30 days (maintenanceAhead), rides that have not run today, the park's pageUrl and the " +
      "editors' funFacts about it. Not the shortest waits, one ride type or a recommendation: " +
      "show_card kind rides order shortest (with type for coasters, dark rides, water or " +
      "family rides) has every waiting ride.",
    input: z.object({ park: parkRef }),
    run: ({ park }, ctx) => parkLiveSummary(ctx, park),
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
      park: parkRef.optional().describe("Only rides of this park"),
    }),
    run: async ({ query, park }, ctx) => {
      const found = park ? await resolvePark(mcp(ctx), park) : null;
      if (found && isAmbiguous(found)) return found;
      return findAttractionsTool.handler(
        { query, parkId: found?.id, limit: 5 },
        mcp(ctx),
      );
    },
  }),
  tool({
    name: "find_restaurants",
    status: "parks",
    description:
      "A park's restaurants, snack bars and food stands, optionally only those whose name or " +
      'cuisine contains query (e.g. "eis", "ice", "pizza"): name, status now where the park ' +
      "reports it, cuisine and reservation where known, land, and the park's pageUrl. For " +
      '"where can I eat", "every ice-cream stand"; plan_add_block puts a stop in the plan, ' +
      "set_favorite takes its id. No menus, prices or opening hours. show_card kind restaurants " +
      "returns the same rows: call one, never both.",
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
          // The id is what set_favorite takes.
          id: r.id,
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
      'Halloween as date. With on instead, that one past day itself ("last Saturday", ' +
      '"on 3 August"), up to a year back. Never the days ahead.',
    input: z.object({
      park: parkRef,
      date: isoDay
        .optional()
        .describe(
          "The day to look a year back from, YYYY-MM-DD; default the park's today",
        ),
      on: isoDay
        .optional()
        .describe("Instead of date: a past day to read itself, YYYY-MM-DD"),
    }),
    run: async ({ park: reference, date, on }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const today = getCurrentDateInTimezone(found.timezone);
      if (on) {
        if (date) throw new McpToolError("Give either date or on");
        const back = daysBetween(on, today);
        if (back <= 0 || back > HISTORY_DAYS) {
          throw new McpToolError(
            `Pick a past day from ${addIsoDays(today, -HISTORY_DAYS)} to yesterday`,
          );
        }
        const { fromDate, toDate } = parseDateRange(on, on, {
          timezone: found.timezone,
        });
        const calendar = await ctx
          .get(CalendarService)
          .buildCalendarResponse(found, fromDate, toDate, "none");
        const day = calendar.days.find((d) => d.date === on);
        return {
          park: found.name,
          day: pastDay(day && compactDay(day, found.timezone)),
        };
      }
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
      "duration where known, and season. Shows without a time that day are named apart. " +
      'For "when is the next show", "what\'s on today". Parks seldom publish times beyond ' +
      "today, so another day is often empty (get_park_day projects them); no parades the park " +
      "does not list as a show. show_card kind shows returns the same rows: call one, never both.",
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
      ...rideRef,
      park: parkRef.optional().describe("With ride: the ride's park"),
    }),
    run: async (input, ctx) => {
      const resolved = await rideIdOf(ctx, input);
      if (typeof resolved !== "string") return resolved;
      return rideDetails(ctx, resolved);
    },
  }),
  tool({
    name: "compare_rides",
    status: "rides",
    description:
      "The rides of one park, or of every park in a country, ranked by a fact: top speed, height, " +
      "length, duration, inversions, launches, opening year or minimum rider height (cm), highest " +
      "first unless order asc. type keeps only coasters, dark rides (simulators included), water " +
      "rides or family rides; shelter keeps only the rides that keep you dry in the rain " +
      "(covered: indoors or a roofed queue; indoor: the ride too). With riderHeightCm only " +
      "the rides someone that tall may ride. Without by, every ride of the filters by name, the " +
      "ones not ridden yet first (notRidden counts them), the others marked ridden where the user " +
      'ticked them off ("which coasters haven\'t we done"; raise limit to see them all). ' +
      'For "the fastest ride in X", "the fastest coaster in ' +
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
      by: z
        .enum(RIDE_RANKINGS)
        .optional()
        .describe(
          "The fact to rank by; leave it out for every ride of the filters, by name",
        ),
      order: z.enum(["desc", "asc"]).default("desc"),
      type: z.enum(RIDE_TYPE_FILTERS).optional(),
      shelter: z.enum(SHELTER_FILTERS).optional(),
      riderHeightCm: riderHeight.optional(),
      limit: z.number().int().min(1).max(15).default(5),
    }),
    run: async (
      {
        park: reference,
        country,
        by,
        order,
        type,
        shelter,
        riderHeightCm,
        limit,
      },
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
      const ridden = ctx.ridden;
      const { firstCount, ...compared } = compareRides(
        comparedRides(found, profiles),
        {
          by,
          order,
          type,
          shelter,
          riderHeightCm,
          limit,
          // "Which haven't we done": a list cut at the limit starts with those.
          ...(ridden?.size && { first: (ride) => !ridden.has(ride.id) }),
        },
      );
      return {
        ...scope,
        by,
        order,
        ...(type && { type }),
        ...(shelter && { shelter }),
        ...compared,
        ...(firstCount !== undefined && { notRidden: firstCount }),
        rides: compared.rides.map((ride) => ({
          ...ride,
          ...riddenMark(ctx, ride.id),
        })),
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
      ...rideRef,
      park: parkRef.optional().describe("With ride: the ride's park"),
      date: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}$/)
        .optional()
        .describe("YYYY-MM-DD in the park's calendar; default today"),
    }),
    run: async ({ date, ...reference }, ctx) => {
      const resolved = await rideIdOf(ctx, reference);
      if (typeof resolved !== "string") return resolved;
      const rideId = resolved;
      const ride = await ctx
        .get(DataSource)
        .getRepository(Attraction)
        .findOne({
          where: { id: rideId.toLowerCase() },
          relations: { park: { destination: true } },
        });
      if (!ride?.park)
        throw new McpToolError(`No ride ${rideId}; try find_rides`);
      ctx.notePark?.(ride.park.timezone);
      const day = date ?? getCurrentDateInTimezone(ride.park.timezone);
      // The plan leaves a ride out on a day a works window covers; this says why its hours are
      // empty, so "no forecast" is never what the visitor hears.
      const maintenance = worksPeriodOn(ride, day);
      const plan = await ctx.get(PlanDayService).buildPlanDay(ride.park, day);
      const tz = ride.park.timezone;
      const isToday = day === getCurrentDateInTimezone(tz);
      // Never an hour after closing, and today never one that is already over.
      const planned = plan.rides.find((r) => r.attractionId === ride.id);
      const hours = bookableHours(
        planned?.hours ?? [],
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
        ...(plan.ridesUnavailable && {
          ridesUnavailable: plan.ridesUnavailable,
        }),
        ...(!planned &&
          !maintenance &&
          !plan.ridesUnavailable && {
            notInDayPlan:
              "park.fan's plan for that day has no hours for this ride (out of season, not on that weekday, or no forecast); get_ride says why it is not running",
          }),
      };
    },
  }),
  tool({
    name: "get_hours",
    status: "forecast",
    description:
      "One day at one park hour by hour, today by default and up to a year ahead: for each hour " +
      "the park is open, the headliners' expected average standby wait and how many of them carry " +
      "a value; hours before opening and after closing are not listed, and today's hours already " +
      "over are marked past. bestHours are the three quietest hours still to come, busiestHour " +
      "the worst. A closed day has no hours and names the next day the park opens; hoursPublished " +
      "false means the park has not published that day's hours, so it is park.fan's estimate. " +
      "tier and tierWhy say what the numbers are (the hourly model up to 48 hours ahead, the " +
      "day's forecast spread over each ride's usual day, or how comparable days went a year " +
      'earlier). For "when is it quietest in the day", "is the afternoon better", "when should ' +
      "we arrive\". One ride: get_ride's bestVisitTimesToday today, get_ride_forecast another day.",
    input: z.object({ park: parkRef, date: planDate.optional() }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const tz = found.timezone;
      const today = getCurrentDateInTimezone(tz);
      const day = date ?? today;
      const [plan, until] = await Promise.all([
        ctx.get(PlanDayService).buildPlanDay(found, day),
        publishedUntil(ctx, found),
      ]);
      const hoursPublished = until !== null && day <= until;
      const { openHour, closeHour, status } = plan.context;
      const base = {
        park: found.name,
        date: day,
        status,
        hoursPublished,
        ...(!hoursPublished && {
          statusWhy: until
            ? `the park has published its hours up to ${until}; status and hours are park.fan's estimate`
            : "the park publishes no opening hours; status and hours are park.fan's estimate",
        }),
        tier: plan.tier,
        tierWhy: TIER_MEANING[plan.tier],
      };
      if (status === "CLOSED" || openHour === null) {
        return {
          ...base,
          hours: [],
          ...(status === "CLOSED" && {
            nextOpenDay: await firstOpenDay(ctx, found.id, addIsoDays(day, 1)),
          }),
          ...(plan.ridesUnavailable && {
            ridesUnavailable: plan.ridesUnavailable,
          }),
        };
      }
      // The headliners carry the day a visitor feels; a park without known ones counts all.
      const headliners = plan.rides.filter((ride) => ride.isHeadliner);
      const counted = headliners.length > 0 ? headliners : plan.rides;
      const waits = new Map<number, number[]>();
      for (const ride of counted) {
        for (const { hour, wait } of bookableHours(
          ride.hours,
          plan.context,
          null,
        )) {
          waits.set(hour, [...(waits.get(hour) ?? []), wait]);
        }
      }
      const currentHour =
        day === today ? Number(formatInTimeZone(new Date(), tz, "H")) : null;
      const hours = [...waits.entries()]
        .sort(([a], [b]) => a - b)
        .map(([hour, list]) => ({
          at: parkHourInstant(day, hour, tz),
          waitMinutes: Math.round(
            list.reduce((sum, w) => sum + w, 0) / list.length,
          ),
          rides: list.length,
          ...(currentHour !== null &&
            hour < currentHour && { past: true as const }),
        }));
      const ahead = hours.filter((hour) => !("past" in hour));
      return {
        ...base,
        opens: parkHourInstant(day, openHour, tz),
        closes:
          closeHour !== null
            ? parkHourInstant(day, unfoldedCloseHour(openHour, closeHour), tz)
            : null,
        counted: headliners.length > 0 ? "headliners" : "all rides",
        bestHours: [...ahead]
          .sort((a, b) => a.waitMinutes - b.waitMinutes)
          .slice(0, BEST_HOURS)
          .map((hour) => hour.at),
        busiestHour:
          [...hours].sort((a, b) => b.waitMinutes - a.waitMinutes)[0]?.at ??
          null,
        ...(hours.length === 0 && {
          hoursWhy:
            "no ride has an hourly figure for that day; get_days has the day's level",
        }),
        hours,
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
      "ridesInMaintenance, each with its window. Each ride carries its minimumHeightCm where " +
      "known; with the party's height in the plan, tooShortForParty marks the rides its " +
      "shortest rider may not take and plannedTooShort names those already planned (riderHeightCm " +
      "for a party the plan does not hold yet). hoursPublished false: the park has not published " +
      "that day's hours, so status and hours are park.fan's estimate (statusWhy). Today by default.",
    input: z.object({
      park: parkRef,
      date: planDate.optional(),
      riderHeightCm: riderHeight
        .optional()
        .describe(
          "The party's shortest rider, when the plan does not hold it yet",
        ),
    }),
    run: async ({ park: reference, date, riderHeightCm }, ctx) => {
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const park = await ctx.get(ParksService).findById(found.id);
      if (!park)
        throw new McpToolError(`No park ${reference}; try search_parks`);
      const tz = park.timezone;
      const day = date ?? getCurrentDateInTimezone(tz);
      const planDays = ctx.get(PlanDayService);
      // A failed lookup costs the list, never the day.
      const [plan, inMaintenance, until] = await Promise.all([
        planDays.buildPlanDay(park, day),
        Promise.resolve()
          .then(() => planDays.ridesInMaintenance(park, day))
          .catch(() => []),
        publishedUntil(ctx, park),
      ]);
      const hoursPublished = until !== null && day <= until;
      const { openHour, closeHour, ...context } = plan.context;
      const currentHour =
        day === getCurrentDateInTimezone(tz)
          ? Number(formatInTimeZone(new Date(), tz, "H"))
          : null;
      const visitorDay = snapshotDay(ctx, park.slug, day);
      const party = visitorDay?.prefs?.riderHeightCm ?? riderHeightCm;
      const tooShort = new Set(
        plan.rides.filter((ride) => partyTooShort(ride.minimumHeight, party)),
      );
      const planned = new Set(
        visitorDay?.entries.flatMap((entry) =>
          entry.kind === "ride" && entry.rideSlug ? [entry.rideSlug] : [],
        ),
      );
      const plannedTooShort = [...tooShort].filter((ride) =>
        planned.has(ride.attractionSlug),
      );
      return {
        park: park.name,
        tier: plan.tier,
        ...context,
        hoursPublished,
        ...(!hoursPublished && {
          statusWhy: until
            ? `the park has published its hours up to ${until}; status and hours are park.fan's estimate`
            : "the park publishes no opening hours; status and hours are park.fan's estimate",
        }),
        opens: openHour !== null ? parkHourInstant(day, openHour, tz) : null,
        closes:
          openHour !== null && closeHour !== null
            ? parkHourInstant(day, unfoldedCloseHour(openHour, closeHour), tz)
            : null,
        ...(plan.ridesUnavailable && {
          ridesUnavailable: plan.ridesUnavailable,
        }),
        ...(party !== undefined && { partyRiderHeightCm: party }),
        ...(plannedTooShort.length > 0 && {
          plannedTooShort: plannedTooShort.map((ride) => ({
            id: ride.attractionId,
            name: ride.attractionName,
            minimumHeightCm: ride.minimumHeight,
          })),
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
        // Last, since it is the long list: a cut at the size limit takes rides, not the day.
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
            ...(ride.minimumHeight !== undefined && {
              minimumHeightCm: ride.minimumHeight,
            }),
            ...(tooShort.has(ride) && { tooShortForParty: true }),
            ...riddenMark(ctx, ride.attractionId),
            ...rideOpeningOf(ride, plan),
            predictedPeakWaitMinutes: ride.dayPeak,
            shortestAt: shortest && {
              at: parkHourInstant(day, shortest.hour, tz),
              waitMinutes: shortest.wait,
            },
          };
        }),
      };
    },
  }),
  tool({
    name: "get_park_weather",
    status: "weather",
    description:
      "Weather at one park right now and for the next hours: temperature, whether it rains, when " +
      "rain or a thunderstorm starts and ends (ISO times), wind gusts, and an hourly forecast " +
      "for the next 12 hours (weatherCode is the WMO code). For days ahead use get_days.",
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
      "The 10 quietest rated open days of one park in the next 90 days (or a range inside them), " +
      "quietest first, and the typical crowd per weekday from history (dayOfWeek 0 = Sunday, " +
      "lower avgCrowdScore = quieter). Only the quietest ten, not a ranking of every day: a day " +
      "missing from the list is not a day to avoid. Days the park is closed and days whose hours " +
      'are not published yet are counted apart, never ranked. Use for "when should I go" in the ' +
      "next three months; later days and particular days are get_days.",
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
      // The snapshot reaches 90 days: a later range came back empty and read as "closed".
      const last = addIsoDays(
        getCurrentDateInTimezone(found.timezone),
        BEST_DAYS_WINDOW,
      );
      if ((from && from > last) || (to && to > last)) {
        throw new McpToolError(
          `get_best_days reads up to ${last}; for later days use get_days (up to a year ahead)`,
        );
      }
      const [result, until] = await Promise.all([
        ctx.get(BestDaysService).getBestDays(found, from, to),
        publishedUntil(ctx, found),
      ]);
      // Past the published hours a status is the calendar's estimate: neither open nor closed.
      const notPublished = result.days.filter(
        (day) => day.status === "UNKNOWN" || !until || day.date > until,
      );
      const known = result.days.filter((day) => !notPublished.includes(day));
      const open = known.filter((day) => day.status === "OPERATING");
      // A day without a rating is no quiet day: ranked, `indexOf` -1 would put it first.
      const rated = open.filter((day) => CROWD_ORDER.includes(day.crowdLevel));
      const closedDays = known.length - open.length;
      return {
        park: { id: found.id, name: found.name, pageUrl: parkPageUrl(found) },
        window: { from: result.meta.windowFrom, to: result.meta.windowTo },
        hoursPublishedUntil: until,
        openDays: open.length,
        closedDays,
        ...(notPublished.length > 0 && {
          hoursNotPublishedDays: notPublished.length,
          hoursNotPublishedWhy: until
            ? `the park has published its hours up to ${until}`
            : "the park publishes no opening hours",
        }),
        // A window the park is shut for gets its next opening, not just an empty list.
        ...(open.length === 0 &&
          known.length > 0 && {
            nextOpenDay: await firstOpenDay(
              ctx,
              found.id,
              addIsoDays(result.meta.windowTo, 1),
            ),
          }),
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
    name: "compare_parks",
    status: "forecast",
    description:
      "Several parks on one day, quietest first: whether each is open, its hours, the crowd " +
      "level park.fan's calendar expects (one day rating for every park, so they compare " +
      "fairly) and the headliners' expected peak wait. Give a country (with city to narrow it) " +
      'or 2 to 10 parks. For "which park is quietest on Saturday", "where in Germany on 28 ' +
      'June", "which of these three". Closed parks (with the day each opens next), open ones ' +
      "without a rating and parks whose hours for that day are not published yet are listed " +
      "apart, each with the reason, never as quiet. A park whose day guests must leave early " +
      "(closesEarlyForDayGuests, with its usualClose) ranks behind a full day within 15 minutes. " +
      "Today by default, up to a year ahead; one " +
      "park's best days are get_best_days, its particular days get_days.",
    input: z.object({
      country: z
        .string()
        .trim()
        .min(2)
        .max(60)
        .optional()
        .describe('A country, "Germany" or its code "DE"'),
      city: z
        .string()
        .trim()
        .min(2)
        .max(60)
        .optional()
        .describe("With country: only the parks of this city"),
      parks: z
        .array(parkRef)
        .min(2)
        .max(10)
        .optional()
        .describe("Instead of country: park ids or slugs"),
      date: isoDay
        .optional()
        .describe("YYYY-MM-DD in the parks' calendar; default today"),
    }),
    run: async ({ country, city, parks, date }, ctx) => {
      if (!country === !parks) {
        throw new McpToolError("Give either country or parks");
      }
      let found: Park[];
      if (parks) {
        const resolved = await Promise.all(
          parks.map((reference) => resolvePark(mcp(ctx), reference)),
        );
        const ambiguous = resolved.find(isAmbiguous);
        if (ambiguous) return ambiguous;
        found = resolved as Park[];
      } else {
        const query = ctx
          .get(DataSource)
          .getRepository(Park)
          .createQueryBuilder("park")
          .where("park.retiredAt IS NULL")
          .andWhere(
            "(park.countryCode = :code OR lower(park.country) = lower(:country))",
            { code: getCountryISOCode(country as string) ?? "", country },
          );
        if (city) {
          query.andWhere("lower(park.city) = lower(:city)", { city });
        }
        found = await query.getMany();
        if (found.length === 0) {
          throw new McpToolError(
            `No parks in "${[city, country].filter(Boolean).join(", ")}"; try search_parks`,
          );
        }
        if (found.length > MAX_COMPARED_PARKS) {
          throw new McpToolError(
            `${found.length} parks in ${country}: name a city, or up to 10 parks`,
          );
        }
      }
      const read = await inBatches(found, CALENDAR_BATCH, (park) =>
        comparedParkDay(ctx, park, date).catch(() => ({
          park,
          day: date ?? null,
          why: "its calendar could not be read just now",
        })),
      );
      type Read = (typeof read)[number];
      const known = read.filter(
        (d): d is Read & { found: ReturnType<typeof compactDay> } =>
          "found" in d && d.found !== undefined,
      );
      const row = ({ park, found: d, ...rest }: (typeof known)[number]) => ({
        id: park.id,
        name: park.name,
        city: park.city,
        opens: d.opens,
        closes: d.closes,
        ...("earlyClose" in rest &&
          rest.earlyClose && {
            closesEarlyForDayGuests: true,
            usualClose: rest.earlyClose.usualClose,
          }),
        hoursInferred: d.hoursInferred,
        crowdLevel: d.crowdLevel,
        expectedPeakWaitMinutes: d.expectedPeakWaitMinutes,
        holiday: d.holiday,
        schoolVacation: d.schoolVacation,
        ...regionWatch(park, d),
      });
      // Past a park's published schedule its status is the calendar's estimate, never a fact.
      const inSchedule = known.filter((d) => "published" in d && d.published);
      const estimated = known.filter((d) => !inSchedule.includes(d));
      // Within it, a day the schedule names no status for is neither open nor closed.
      const unstated = inSchedule.filter((d) => d.found.status === "UNKNOWN");
      const published = inSchedule.filter((d) => !unstated.includes(d));
      const open = published.filter((d) => d.found.status === "OPERATING");
      const closed = published.filter((d) => d.found.status === "CLOSED");
      // A day without a rating is no quiet day: ranked, `indexOf` -1 would put it first.
      const rated = open.filter((d) =>
        CROWD_ORDER.includes(d.found.crowdLevel),
      );
      const rank = (d: (typeof known)[number]) =>
        CROWD_ORDER.indexOf(d.found.crowdLevel);
      const reopening = await inBatches(closed, CALENDAR_BATCH, (d) =>
        firstOpenDay(ctx, d.park.id, addIsoDays(d.day as string, 1)).catch(
          () => null,
        ),
      );
      return {
        date: date ?? read[0]?.day ?? null,
        ranked: behindFullDays(
          [...rated].sort(
            (a, b) =>
              rank(a) - rank(b) ||
              (a.found.expectedPeakWaitMinutes ?? 0) -
                (b.found.expectedPeakWaitMinutes ?? 0),
          ),
        ).map(row),
        unrated: open
          .filter((d) => !rated.includes(d))
          .map((d) => ({ ...row(d), why: "no crowd rating for that day" })),
        closed: closed.map((d, i) => ({
          name: d.park.name,
          nextOpenDay: reopening[i],
        })),
        hoursNotPublished: estimated.map((d) => ({
          ...row(d),
          estimatedStatus: d.found.status,
          why:
            "publishedUntil" in d && d.publishedUntil
              ? `hours published up to ${d.publishedUntil}; this is park.fan's estimate`
              : "the park publishes no opening hours; this is park.fan's estimate",
        })),
        notKnown: [
          ...read
            .filter((d) => !("found" in d) || d.found === undefined)
            .map((d) => ({
              name: d.park.name,
              why: "why" in d ? d.why : null,
            })),
          ...unstated.map((d) => ({
            name: d.park.name,
            why: "the park's published schedule names no opening status for that day",
          })),
        ],
      };
    },
  }),
  tool({
    name: "get_days",
    status: "forecast",
    description:
      "The days at one park, up to a year ahead: per day its weekday, whether it is open, " +
      "closed or its hours are not published yet, the hours, the crowd level park.fan's calendar " +
      "expects, the headliners' expected peak wait, holidays, school breaks, bridge days, events, " +
      "the rides a maintenance window shuts, and the weather where a forecast reaches. A closed " +
      "day names the next day the park opens. Give 1 to 10 dates, or from and to for a stretch of " +
      'up to 31 days; nothing gives the next 7 days. For "Saturday or Sunday?", "10 or 17 ' +
      'May?", "is it open on 28 December?", "the next two weeks". The hours of one day are ' +
      "get_hours; past days get_park_history.",
    input: z.object({
      park: parkRef,
      dates: z
        .array(isoDay)
        .min(1)
        .max(10)
        .optional()
        .describe("YYYY-MM-DD in the park's calendar"),
      from: isoDay.optional().describe("Instead of dates: first day"),
      to: isoDay.optional().describe("With from: last day"),
    }),
    run: async ({ park: reference, dates, from, to }, ctx) => {
      if (dates && (from || to)) {
        throw new McpToolError("Give either dates, or from and to");
      }
      const found = await resolvePark(mcp(ctx), reference);
      if (isAmbiguous(found)) return found;
      const today = getCurrentDateInTimezone(found.timezone);
      const first = from ?? today;
      const lastAsked = to ?? addIsoDays(first, DEFAULT_DAYS - 1);
      const asked = dates
        ? [...new Set(dates)].sort()
        : Array.from(
            { length: Math.max(0, daysBetween(first, lastAsked) + 1) },
            (_, i) => addIsoDays(first, i),
          );
      if (asked.length === 0 || asked.length > MAX_COMPARED_DATES) {
        throw new McpToolError(`Give 1 to ${MAX_COMPARED_DATES} days`);
      }
      if (asked[0] < today) {
        throw new McpToolError(
          `${asked[0]} is over; past days are get_park_history with on`,
        );
      }
      const last = addIsoDays(today, DAYS_AHEAD);
      if (asked[asked.length - 1] > last) {
        throw new McpToolError(`park.fan looks ahead up to ${last}`);
      }
      const [{ days, coverage, hasOperatingSchedule }, maintenance] =
        await Promise.all([
          calendarDays(ctx, found, asked),
          // A failed lookup costs the maintenance names, never the days.
          Promise.resolve()
            .then(() => ctx.get(PlanDayService).maintenanceWindows(found))
            .catch(() => []),
        ]);
      const rows = await Promise.all(
        asked.map(async (date) => {
          const day = days.get(date);
          const weekday = formatInTimeZone(
            new Date(`${date}T12:00:00Z`),
            "UTC",
            "EEEE",
          );
          if (!day)
            return { date, weekday, status: "UNKNOWN", why: "no calendar day" };
          const compact = compactDay(day, found.timezone, maintenance);
          // Past the published schedule a status is the calendar's guess, not the park's word.
          const published =
            hasOperatingSchedule && coverage.to !== null && date <= coverage.to;
          const base = {
            weekday,
            ...compact,
            hoursPublished: published,
          };
          if (!published) {
            return {
              ...base,
              status: "UNKNOWN",
              estimatedStatus: compact.status,
            };
          }
          if (compact.status === "UNKNOWN") {
            return {
              ...base,
              why: "the park's published schedule names no opening status for this day",
            };
          }
          if (compact.status === "CLOSED") {
            return {
              ...base,
              crowdLevel: undefined,
              expectedPeakWaitMinutes: undefined,
              nextOpenDay: await firstOpenDay(
                ctx,
                found.id,
                addIsoDays(date, 1),
              ),
            };
          }
          return base;
        }),
      );
      const rank = (row: (typeof rows)[number]) =>
        "crowdLevel" in row && row.crowdLevel
          ? CROWD_ORDER.indexOf(row.crowdLevel)
          : -1;
      const open = rows.filter(
        (row) => row.status === "OPERATING" && rank(row) >= 0,
      );
      const ranked = [...open].sort(
        (a, b) =>
          rank(a) - rank(b) ||
          ((a as { expectedPeakWaitMinutes?: number })
            .expectedPeakWaitMinutes ?? 0) -
            ((b as { expectedPeakWaitMinutes?: number })
              .expectedPeakWaitMinutes ?? 0),
      );
      const quietest = ranked[0];
      const unpublished = rows.some((row) => row.status === "UNKNOWN");
      return {
        park: { id: found.id, name: found.name, pageUrl: parkPageUrl(found) },
        hoursPublishedUntil: hasOperatingSchedule ? coverage.to : null,
        // Once for every UNKNOWN day, not on each row: a month of rows stays under the size limit.
        ...(unpublished && {
          unknownDaysWhy: hasOperatingSchedule
            ? `the park has published its hours up to ${coverage.to}; estimatedStatus is park.fan's estimate`
            : "the park publishes no opening hours; estimatedStatus is park.fan's estimate",
        }),
        quietestOpenDay: quietest?.date ?? null,
        // A stretch ranked, so "the quietest days in May" needs no reading of every row.
        quietestOpenDays: ranked.slice(0, QUIETEST_DAYS).map((row) => row.date),
        days: rows,
      };
    },
  }),
  tool({
    name: "plan_vacation",
    status: "forecast",
    description:
      "A trip in a trip region (Orlando by default) day by day: which park when, so the big " +
      "rides queue least, with rest days, hotel perks and party nights. Per day the parks with " +
      "headlinerWait, crowd level, basis, hours, perks, events, travel and why, and evening where " +
      "the park closes early (a second park with a hopper ticket, else a free venue); totals; " +
      "parksLeftOut with leftOutFixes where the chosen parks outnumber the park days; " +
      "hubUrl (the region's hub with this plan) and reserve (plan_trip's days). Up to 21 days and " +
      "16 visits; parks default to the region's headliners (Orlando: the 7 big Disney and " +
      "Universal parks). With tickets they already have, each ticket's valid dates and days bind " +
      "the plan (tickets); without, ticketsToBuy says which ticket the plan needs per resort and " +
      "whether a hopper is worth adding.",
    input: z.object({
      region: regionInput,
      from: isoDay.describe("Arrival day"),
      to: isoDay.describe("Departure day"),
      parks: z
        .array(
          z.object({
            park: z.string().min(2).max(120),
            visits: z.number().int().min(1).max(MAX_VISITS_PER_PARK).default(1),
          }),
        )
        .min(1)
        .max(MAX_PARK_DAYS)
        .optional(),
      hotel: hotelInput,
      area: areaInput,
      blocks: z.boolean().optional().describe("Each resort's days together"),
      fireworks: z
        .enum(["want", "any"])
        .optional()
        .describe(
          "want: keep the evening show, no party nights unless nothing else fits; any (default): price by the hours",
        ),
      rest: z
        .string()
        .regex(/^(auto|none|max:[1-9])$/)
        .optional()
        .describe("auto, none, or max:n park days in a row"),
      ticket: z
        .enum(["base", "hopper"])
        .optional()
        .describe(
          "Without tickets: hopper plans every resort with Disney Park Hopper or Universal park-to-park (Universal Studios and Islands of Adventure, not Epic); base (default): one park a day",
        ),
      hopper: z
        .array(z.string().regex(/^[a-z0-9-]+$/))
        .optional()
        .describe(
          "Without tickets: the resorts planned with the hopper (Orlando: walt-disney-world, universal-orlando)",
        ),
      tickets: z
        .string()
        .regex(TICKETS_PATTERN)
        .optional()
        .describe(TICKETS_DESCRIPTION),
      hop: z
        .boolean()
        .optional()
        .describe(
          "Two parks of a resort a day, for more visits than days; needs a hopper",
        ),
      bases: basesInput,
      startBase: z
        .string()
        .regex(/^[a-z0-9-]+$/)
        .optional()
        .describe(
          "A region with bases: where they land (the base of the airport)",
        ),
      endBase: z
        .string()
        .regex(/^[a-z0-9-]+$/)
        .optional()
        .describe("A region with bases: where they fly home from"),
      moveBuffer: moveBufferInput,
      arrival: z.enum(["free", "park"]).optional(),
      departure: z.enum(["free", "park"]).optional(),
      locks: z
        .array(
          z.object({
            park: z.string().min(2).max(120).describe('A park, or "rest"'),
            date: isoDay,
          }),
        )
        .max(MAX_TRIP_DAYS)
        .optional()
        .describe("Days the user fixed"),
    }),
    run: async (input, ctx) => {
      if (daysBetween(input.from, input.to) + 1 > MAX_TRIP_DAYS) {
        throw new McpToolError(
          `A plan covers at most ${MAX_TRIP_DAYS} days, arrival and departure included; plan the trip in parts`,
        );
      }
      const { regions, region, parks, names } = await tripRegion(
        ctx,
        input.region,
      );
      ctx.notePark?.(region.timezone);
      const orlandoOnly = `plan_vacation plans the ${region.name} parks only`;
      let parkList: string | undefined;
      if (input.parks) {
        const visits = new Map<string, number>();
        for (const { park: reference, visits: count } of input.parks) {
          const found = await regionPark(ctx, reference, parks, orlandoOnly);
          if ("ambiguous" in found) return found;
          visits.set(
            found.slug,
            Math.min(
              MAX_VISITS_PER_PARK,
              (visits.get(found.slug) ?? 0) + count,
            ),
          );
        }
        parkList = [...visits]
          .map(([slug, count]) => `${slug}:${count}`)
          .join(",");
      }
      const locks: string[] = [];
      for (const lock of input.locks ?? []) {
        if (lock.park.trim().toLowerCase() === "rest") {
          locks.push(`rest@${lock.date}`);
          continue;
        }
        const found = await regionPark(ctx, lock.park, parks, orlandoOnly);
        if ("ambiguous" in found) return found;
        locks.push(`${found.slug}@${lock.date}`);
      }
      const flag = (value: boolean | undefined) =>
        value === undefined ? undefined : value ? "1" : "0";
      const query: TripPlanQueryDto = {
        from: input.from,
        to: input.to,
        parks: parkList,
        hotel: input.hotel,
        area: input.area,
        arrival: input.arrival,
        departure: input.departure,
        bases: input.bases,
        startBase: input.startBase,
        endBase: input.endBase,
        moveBuffer: input.moveBuffer,
        blocks: flag(input.blocks),
        fireworks: input.fireworks,
        rest: input.rest,
        ticket: input.ticket,
        hopper: input.hopper?.join(","),
        tickets: input.tickets,
        hop: flag(input.hop),
        lock: locks.length > 0 ? locks.join(",") : undefined,
      };
      return itineraryOf(ctx, { regions, region, parks, names }, query);
    },
  }),
  // The visitor's own trip in the hub's planner: read it, change it, undo Parky's change
  // (docs/frontend/assistant.md#the-visitors-plans).
  tool({
    name: "get_trip_plan",
    status: "forecast",
    description:
      "The user's own trip in the trip planner (This chat names it): the plan day by day as " +
      "plan_vacation gives one (parks, headlinerWait, hours, party nights, why, watch, evening, " +
      "tickets, ticketsToBuy, season, stays, parksLeftOut with leftOutFixes, visitsDropped, " +
      "pinsNotKept, hubUrl, reserve), plus the party " +
      "they told you. region only when they have trips in several.",
    input: z.object({ region: regionInput }),
    run: async ({ region }, ctx) => {
      const trip = visitorTrip(ctx, region);
      const search = tripSearchOf(trip.params);
      const query = planQueryOf(trip.params);
      if (!query) {
        throw new McpToolError(
          search
            ? `The trip has no dates yet: they are searching ${search.days} days between ${search.from} and ${search.to}${search.within ? ` in the school breaks of ${search.within}` : ""}; find_trip_dates answers that`
            : "The trip has no dates yet: ask for them, then change_trip with dates",
        );
      }
      ctx.notePark?.(trip.region.timezone);
      const plan = await itineraryOf(
        ctx,
        await tripRegion(ctx, trip.region.slug),
        query,
      );
      return {
        ...(trip.onScreen
          ? { shown: "on screen in the hub's planner now" }
          : { shown: "remembered by this browser; the hub opens it" }),
        ...(trip.party && { party: trip.party }),
        ...plan,
      };
    },
  }),
  tool({
    name: "change_trip",
    status: "forecast",
    description:
      "Change the user's trip in the trip planner; the planner shows the change and an undo. " +
      "Any of: dates; parks (visits 1-3 sets a park, 0 removes it; the others stay); pin (a " +
      'park or "rest" on a date: "Magic Kingdom on Thursday", "a rest day on the 6th"); unpin ' +
      "(dates); swap (two dates trade what is on them); rest (auto, none, max:n park days in a " +
      "row); tickets (the compact form, empty clears); hotel, area; hopper (resorts); fireworks; " +
      "evenings; arrival, departure (park: a park that day); search (look for the best dates " +
      "instead, as the planner's date finder: the dates go). A region with bases (Japan, Paris), " +
      "the route: bases (the stays they fix: city, nights, hotel area), startBase or endBase " +
      "(the city they land in or fly home from), landAt or leaveFrom (an airport or station " +
      "code; it picks the city where only one is served from it), moveBuffer, " +
      "applyStaySuggestion (the plan's staySuggestion). Without a trip, dates, parks or search " +
      "start one. The result says which days moved, the big rides' wait before and after, the " +
      "stays before and after, and parksLeftOut with leftOutFixes.",
    input: z.object({
      region: regionInput,
      dates: z.object({ from: isoDay, to: isoDay }).optional(),
      parks: z
        .array(
          z.object({
            park: z.string().min(2).max(120),
            visits: z.number().int().min(0).max(MAX_VISITS_PER_PARK),
          }),
        )
        .min(1)
        .max(MAX_PARK_DAYS)
        .optional(),
      pin: z
        .array(
          z.object({
            park: z.string().min(2).max(120).describe('A park, or "rest"'),
            date: isoDay,
          }),
        )
        .min(1)
        .max(MAX_TRIP_DAYS)
        .optional(),
      unpin: z.array(isoDay).min(1).max(MAX_TRIP_DAYS).optional(),
      swap: z.object({ a: isoDay, b: isoDay }).optional(),
      rest: z
        .string()
        .regex(/^(auto|none|max:[1-9])$/)
        .optional(),
      tickets: z
        .string()
        .regex(TICKETS_PATTERN)
        .or(z.literal(""))
        .optional()
        .describe(TICKETS_DESCRIPTION),
      hotel: hotelInput,
      area: areaInput,
      hopper: z
        .array(z.string().regex(/^[a-z0-9-]+$/))
        .max(6)
        .optional()
        .describe("The resorts planned with a hopper; empty: none"),
      fireworks: z.enum(["want", "any"]).optional(),
      evenings: z
        .boolean()
        .optional()
        .describe(
          "An evening at a second park or a venue after an early close",
        ),
      arrival: z.enum(["free", "park"]).optional(),
      departure: z.enum(["free", "park"]).optional(),
      moveBuffer: moveBufferInput,
      search: z
        .object({
          from: isoDay.describe("The range to search, its first day"),
          to: isoDay.describe("Its last day"),
          days: z.number().int().min(WHEN_MIN_DAYS).max(WHEN_MAX_DAYS),
          within: z
            .string()
            .regex(/^[A-Z]{2}-[A-Za-z]{1,10}$/)
            .optional()
            .describe(
              "Only in the school breaks of this region, as find_trip_dates",
            ),
        })
        .optional()
        .describe("Look for the best dates instead of planning given ones"),
      bases: basesInput.or(z.literal("")),
      startBase: baseInput
        .or(z.literal(""))
        .describe(
          "A region with bases: where they land; empty, the plan picks",
        ),
      endBase: baseInput
        .or(z.literal(""))
        .describe(
          "A region with bases: where they fly home from; empty, the plan picks",
        ),
      landAt: gatewayInput.describe(
        "The airport or station they arrive at (HND, CDG, XED)",
      ),
      leaveFrom: gatewayInput.describe(
        "The airport or station they leave from",
      ),
      applyStaySuggestion: z
        .literal(true)
        .optional()
        .describe(
          "Take the plan's staySuggestion: its nights become the fixed stays",
        ),
    }),
    run: async (input, ctx) => {
      const trips = ctx.trips ?? [];
      const current = pickTrip(trips, input.region);
      if (!current && !input.dates && !input.parks && !input.search) {
        visitorTrip(ctx, input.region);
      }
      if (input.dates && input.search) {
        throw new McpToolError("Give dates or a search, not both");
      }
      const slug =
        current?.region.slug ?? input.region ?? pickTrip(trips)?.region.slug;
      const trip = await tripRegion(ctx, slug);
      const { region, parks } = trip;
      ctx.notePark?.(region.timezone);
      const elsewhere = `change_trip changes the ${region.name} trip only`;
      const change: TripChange = {
        ...(input.dates && { dates: input.dates }),
        ...(input.unpin && { unpin: input.unpin }),
        ...(input.swap && { swap: input.swap }),
        ...(input.rest !== undefined && { rest: input.rest }),
        ...(input.tickets !== undefined && { tickets: input.tickets }),
        ...(input.hotel !== undefined && { hotel: input.hotel }),
        ...(input.area !== undefined && { area: input.area }),
        ...(input.hopper !== undefined && { hopper: input.hopper }),
        ...(input.fireworks !== undefined && { fireworks: input.fireworks }),
        ...(input.evenings !== undefined && { evenings: input.evenings }),
        ...(input.arrival !== undefined && { arrival: input.arrival }),
        ...(input.departure !== undefined && { departure: input.departure }),
        ...(input.moveBuffer !== undefined && { moveBuffer: input.moveBuffer }),
        ...(input.search && { search: input.search }),
        ...(input.bases !== undefined && { bases: input.bases }),
        ...(input.startBase !== undefined && { startBase: input.startBase }),
        ...(input.endBase !== undefined && { endBase: input.endBase }),
      };
      const routeAsked =
        input.bases !== undefined ||
        input.startBase !== undefined ||
        input.endBase !== undefined ||
        input.landAt !== undefined ||
        input.leaveFrom !== undefined ||
        input.applyStaySuggestion !== undefined ||
        input.moveBuffer !== undefined;
      if (routeAsked && !region.bases?.length) {
        throw new McpToolError(
          `${region.name} has one base for the whole trip: the route, the stays and the hotel-change buffer are for a region with bases; hotel and area set where they stay`,
        );
      }
      if (input.landAt) change.startBase = gatewayBase(region, input.landAt);
      if (input.leaveFrom)
        change.endBase = gatewayBase(region, input.leaveFrom);
      if (input.parks) {
        change.parks = [];
        for (const { park: reference, visits } of input.parks) {
          const found = await regionPark(ctx, reference, parks, elsewhere);
          if ("ambiguous" in found) return found;
          change.parks.push({ slug: found.slug, visits });
        }
      }
      if (input.pin) {
        change.pin = [];
        for (const { park: reference, date } of input.pin) {
          if (reference.trim().toLowerCase() === "rest") {
            change.pin.push({ target: "rest", date });
            continue;
          }
          const found = await regionPark(ctx, reference, parks, elsewhere);
          if ("ambiguous" in found) return found;
          change.pin.push({ target: found.slug, date });
        }
      }
      const before = current ? planQueryOf(current.params) : null;
      // A swap reads what is on each day, and the comparison needs the plan before: one plan,
      // and the planner on screen asked for the same a moment ago.
      const was = before
        ? await itineraryOf(ctx, trip, before).catch(() => null)
        : null;
      if (input.applyStaySuggestion) {
        const nights = before
          ? await staySuggestionOf(ctx, region.slug, before)
          : null;
        if (!nights) {
          throw new McpToolError(
            "The plan has no stay suggestion: it shows one only where they fixed nights and another split saves time",
          );
        }
        change.stayNights = nights;
      }
      let edited: ReturnType<typeof changeTripParams>;
      try {
        edited = changeTripParams(
          current?.params ?? new URLSearchParams(),
          change,
          {
            // A trip started here takes the parks named; a trip's own list starts from the defaults.
            headliners: current
              ? region.parks.filter((p) => p.headliner).map((p) => p.slug)
              : [],
            moveBufferDefault: stayRulesOf(region).moveOverheadMinutes,
            days: was?.days.map((day) => ({
              date: day.date,
              kind: day.kind,
              parks: (day.parks ?? []).map((p) => ({
                slug:
                  parks.find((park) => park.name === p.park)?.slug ??
                  String(p.park),
              })),
            })),
          },
        );
      } catch (error) {
        throw new McpToolError((error as Error).message);
      }
      const after = planQueryOf(edited.params);
      // The plan route checks the new trip; a refusal sends nothing to the planner. Too many
      // visits for the days is the planner's own problem state, which it shows with its way out,
      // so the change still lands: an interview sets the dates before the parks.
      let plan: Awaited<ReturnType<typeof itineraryOf>> | null = null;
      let problem: string | null = null;
      if (after) {
        try {
          plan = await itineraryOf(ctx, trip, after);
        } catch (error) {
          if (
            !(error instanceof McpToolError) ||
            !PLANNER_PROBLEM.test(error.message)
          ) {
            throw error;
          }
          problem = error.message;
        }
      }
      return new ActionOutcome(
        {
          type: "trip_set",
          region: region.slug,
          query: writeTripQuery(edited.params),
          change: edited.kinds,
        },
        {
          changed: edited.kinds,
          shownIn: current?.onScreen
            ? "the planner on screen, which re-plans"
            : "the hub, which opens with it",
          ...(plan
            ? tripPlanChange(was, plan)
            : problem
              ? {
                  notPlannedYet: `${problem}; the planner says the same with its way out, so the parks or the days come next`,
                }
              : { plan: "no dates yet, so nothing is planned" }),
        },
      );
    },
  }),
  tool({
    name: "trip_undo",
    status: "planner",
    description:
      "Undo your last change to the user's trip. The planner refuses it when the user changed " +
      "the trip since.",
    input: z.object({ region: regionInput }),
    run: (input, ctx) => {
      const trip = visitorTrip(ctx, input.region);
      return Promise.resolve(
        new ActionOutcome(
          { type: "trip_undo", region: trip.region.slug },
          { undoing: trip.region.name },
        ),
      );
    },
  }),
  // A region's own facts, its days at a glance, and how sure its forecast is.
  tool({
    name: "get_trip_region",
    status: "forecast",
    description:
      "A live trip region's facts, for questions the plan tools do not answer: its resorts and " +
      "parks (headliners marked), whose hotels give which perks, its hotel areas, in a region " +
      "with bases the cities with their airports and stations and the hotel-change buffers, " +
      "its ticket ids by resort, its storm season and its hub. Use its ids in plan_vacation " +
      "and change_trip (tickets, hotel, area, bases, landAt).",
    input: z.object({ region: regionInput }),
    run: async ({ region: slug }, ctx) => {
      const { region, names } = await tripRegion(ctx, slug);
      const parkName = (p: string) => names.get(p) ?? p;
      const groupName = (key: string) =>
        region.groups.find((g) => g.key === key)?.name ?? key;
      const baseOfArea = new Map(
        (region.bases ?? []).flatMap((b) =>
          b.hotelAreas.map((area) => [area, b.key] as const),
        ),
      );
      const rules = stayRulesOf(region);
      const storm = region.weather.storm;
      return {
        name: region.name,
        hubUrl: `${getFrontendUrl()}${hubPathOf(region)}`,
        resorts: region.groups.map((g) => ({
          key: g.key,
          name: g.name,
          ...(g.kind === "day_trip" && { dayTrip: true }),
          parks: region.parks
            .filter((p) => p.group === g.key)
            .map(
              (p) => `${parkName(p.slug)}${p.headliner ? " (headliner)" : ""}`,
            ),
        })),
        hotels: Object.entries(region.hotelNames).map(([kind, name]) => ({
          kind,
          whose: name,
          perks: [
            ...new Set(
              (region.perks[kind] ?? []).map(
                (rule) =>
                  `${region.perkNames[rule.perk] ?? rule.perk} at ${rule.parks.map(parkName).join(", ")}`,
              ),
            ),
          ],
        })),
        hotelAreas: region.hotelAreas.map((a) => ({
          key: a.key,
          name: a.name,
          hotel: a.hotel,
          ...(baseOfArea.has(a.key) && { base: baseOfArea.get(a.key) }),
        })),
        ...(region.bases?.length && {
          bases: region.bases.map((b) => ({
            key: b.key,
            name: b.name,
            ...(b.gateways?.length && {
              gateways: b.gateways.map((g) => `${g.code} ${g.name}`),
            }),
            reaches: b.parks.map(
              (p) => `${parkName(p.slug)} ${p.minutes} min by ${p.mode}`,
            ),
            ...(b.stayNote && { stay: b.stayNote }),
          })),
          moveBuffer: {
            options: [...MOVE_BUFFER_OPTIONS],
            default: rules.moveOverheadMinutes,
          },
          ...(luggagePhrase(region) && { luggage: luggagePhrase(region) }),
          routeMeans:
            "bases: city[:nights][@hotel area], :0 a day trip from elsewhere; a gateway listed under one city starts or ends the trip there (landAt, leaveFrom); moveBuffer: minutes for each hotel change on top of the train",
        }),
        tickets: (region.tickets ?? [])
          .filter((t) => !t.archived)
          .map(
            (t) =>
              `${t.id}: ${t.name} (${groupName(t.resort)}${t.days === "unlimited" ? ", unlimited days" : t.days.min === t.days.max ? `, ${t.days.min} days` : `, ${t.days.min}-${t.days.max} days`}${t.hopping !== "none" ? ", hopping" : ""})`,
          ),
        ...(storm && {
          stormSeason: `${storm.kind} season, its peak ${storm.peak.from} to ${storm.peak.to} (month-day): the planner ranks windows there lower`,
        }),
      };
    },
  }),
  tool({
    name: "get_trip_days",
    status: "forecast",
    description:
      "A live trip region's days at a glance, as its hub's calendar shows them: for each day " +
      "of a span (default the next 31 days, at most 92), each resort's park to pick that day " +
      "(the big rides' wait at their busiest, crowd level, why), and per resort its quietest " +
      'and busiest days of the span. For "which days in March are best for Disney" or ' +
      '"the quietest week"; resort narrows to one resort.',
    input: z.object({
      region: regionInput,
      from: isoDay.optional(),
      to: isoDay.optional(),
      resort: z
        .string()
        .regex(/^[a-z0-9-]+$/)
        .optional()
        .describe("A resort key (get_trip_region), e.g. walt-disney-world"),
    }),
    run: async ({ region: slug, from, to, resort }, ctx) => {
      const { regions, region, names } = await tripRegion(ctx, slug);
      ctx.notePark?.(region.timezone);
      const resorts = region.groups.filter(
        (g) => g.kind === "resort" && (!resort || g.key === resort),
      );
      if (resorts.length === 0) {
        throw new McpToolError(
          `No resort ${resort} in ${region.name}: ${region.groups
            .filter((g) => g.kind === "resort")
            .map((g) => g.key)
            .join(", ")}`,
        );
      }
      const calendar = await tripCall(() =>
        regions.calendar(region.slug, from, to),
      );
      const picks = resorts.map((g) => ({
        key: g.key,
        name: g.name,
        days: calendar.days.flatMap((day) => {
          const best = (day as { best?: Record<string, string> }).best?.[g.key];
          const cell = best ? day.parks[best] : undefined;
          if (!best || !cell || cell.headlinerWait == null) return [];
          const why = (day as { bestWhy?: Record<string, string> }).bestWhy?.[
            g.key
          ];
          return [
            {
              date: day.date,
              park: names.get(best) ?? best,
              wait: cell.headlinerWait,
              crowd: cell.crowdLevel,
              basis: cell.basis,
              why,
            },
          ];
        }),
      }));
      const line = (p: (typeof picks)[number]["days"][number]) =>
        `${p.date} (${weekdayOf(p.date)}): ${p.park} ${p.wait} min, ${p.crowd}${p.why === "closes_early_elsewhere" ? ", the others close early" : ""}${p.basis === "long_range" ? ", long-range" : ""}`;
      return {
        region: region.name,
        range: { from: calendar.meta.from, to: calendar.meta.to },
        means:
          "per resort the park to pick each day: the big rides' wait at their busiest in minutes (lower is quieter) and the crowd level; days with no forecast yet are left out",
        resorts: picks.map((p) => {
          const ranked = [...p.days].sort((a, b) => a.wait - b.wait);
          return {
            resort: p.name,
            ...(ranked.length > 0
              ? {
                  quietest: ranked.slice(0, 3).map(line),
                  busiest: ranked.slice(-3).reverse().map(line),
                  days: p.days.map(line),
                }
              : {
                  days: "no forecast in this span yet: get_park_events or find_trip_dates read further out",
                }),
          };
        }),
        holidays: calendar.days
          .filter((d) => d.holidays?.length || d.isSchoolVacation)
          .map(
            (d) =>
              `${d.date}: ${[...(d.holidays ?? []), ...(d.isSchoolVacation ? ["school break"] : [])].join(", ")}`,
          ),
      };
    },
  }),
  tool({
    name: "get_forecast_accuracy",
    status: "forecast",
    description:
      "How accurate a live trip region's forecast was: the big rides' peak wait as forecast a " +
      "week ahead against what each day did, the mean miss in minutes over the last 30 " +
      'measured days. For "how sure are you", "how good is your forecast"; a plan\'s ' +
      "totals.expectedError is the same measurement at its own lead times.",
    input: z.object({ region: regionInput }),
    run: async ({ region: slug }, ctx) => {
      const { region } = await tripRegion(ctx, slug);
      const { forecastAccuracy: a } = await tripCall(() =>
        ctx.get(TripAccuracyService).accuracy(region.slug),
      );
      if (!a) {
        return {
          region: region.name,
          accuracy: null,
          why: "too few measured days yet for a fair figure; the Fancast page has the forecasts' track record",
        };
      }
      return {
        region: region.name,
        accuracy: `a week ahead, the big rides' peak wait was off by ${a.maeMinutes} minutes on average, over ${a.parks} parks and the ${a.days} days ${a.from} to ${a.to}`,
        leadDays: a.leadDays,
        maeMinutes: a.maeMinutes,
        means:
          "the typical miss, not the largest; it grows with how far ahead the day is, and a day further out than about two months is estimated from last year, not forecast",
        fancast: "open_page page fancast has the full track record",
      };
    },
  }),
  // find_trip_dates: when to go, for a trip whose parks and length are known
  // (docs/frontend/trip-regions-when.md).
  tool({
    name: "find_trip_dates",
    status: "forecast",
    description:
      "When to go to a trip region (Orlando by default): ranks every trip window of the given " +
      "length in a range (a year, or from/to; at most a year ahead) by the plan it would get, and " +
      "returns the best windows with headlinerWait against the range's average, why, what to " +
      "watch, events, lastYear where the figures are last year's, and hubUrl; plus the months. " +
      "within: a school-holiday region code (DE-NW, NL-GR for the Dutch north, GB-ENG, BE-NL, " +
      "BE-FR, FR-ZA/ZB/ZC, AT-WI, ES-MD, IT-LO, …) keeps the windows inside its breaks. Parks " +
      "default to the region's headliners.",
    input: z.object({
      region: regionInput,
      year: z
        .number()
        .int()
        .min(2026)
        .max(2030)
        .optional()
        .describe("A calendar year instead of from/to"),
      from: isoDay.optional(),
      to: isoDay.optional(),
      days: z
        .number()
        .int()
        .min(WHEN_MIN_DAYS)
        .max(WHEN_MAX_DAYS)
        .optional()
        .describe("Trip days, arrival and departure included; default 10"),
      parks: z
        .array(
          z.object({
            park: z.string().min(2).max(120),
            visits: z.number().int().min(1).max(MAX_VISITS_PER_PARK).optional(),
          }),
        )
        .min(1)
        .max(MAX_PARK_DAYS)
        .optional(),
      within: z
        .string()
        .regex(/^[A-Za-z]{2}(-[\p{L}\p{N}]{1,12}){1,2}$/u)
        .optional()
        .describe(
          "The visitor's school-holiday region, when they travel in school breaks",
        ),
      hotel: hotelInput,
      area: areaInput,
      blocks: z.boolean().optional(),
      fireworks: z.enum(["want", "any"]).optional(),
      split: z.enum(["auto", "fixed"]).optional(),
      rest: z
        .string()
        .regex(/^(auto|none|max:[1-9])$/)
        .optional(),
      hop: z.boolean().optional(),
      bases: basesInput,
      startBase: z
        .string()
        .regex(/^[a-z0-9-]+$/)
        .optional()
        .describe(
          "A region with bases: where they land (the base of the airport)",
        ),
      endBase: z
        .string()
        .regex(/^[a-z0-9-]+$/)
        .optional()
        .describe("A region with bases: where they fly home from"),
      moveBuffer: moveBufferInput,
      arrival: z.enum(["free", "park"]).optional(),
      departure: z.enum(["free", "park"]).optional(),
      limit: z.number().int().min(1).max(WHEN_MAX_LIMIT).optional(),
      tickets: z
        .string()
        .regex(TICKETS_PATTERN)
        .optional()
        .describe(
          "Tickets they already have, as for plan_vacation but without @start dates",
        ),
      suggest: z
        .boolean()
        .optional()
        .describe("Name the tickets each window's plan needs"),
    }),
    run: async (input, ctx) => {
      const { regions, region, parks, names } = await tripRegion(
        ctx,
        input.region,
      );
      ctx.notePark?.(region.timezone);
      const today = regions.today(region);
      let from = input.from;
      let to = input.to;
      if (input.year && !from && !to) {
        const last = addIsoDays(today, WHEN_MAX_RANGE_DAYS - 1);
        from = `${input.year}-01-01` > today ? `${input.year}-01-01` : today;
        to = `${input.year}-12-31` < last ? `${input.year}-12-31` : last;
        if (from > to) {
          throw new McpToolError(
            `${input.year} is outside today … a year ahead (${today} … ${last})`,
          );
        }
      }
      let parkList: string | undefined;
      if (input.parks) {
        const listed: string[] = [];
        for (const { park: reference, visits } of input.parks) {
          const found = await regionPark(
            ctx,
            reference,
            parks,
            `find_trip_dates covers the ${region.name} parks only`,
          );
          if ("ambiguous" in found) return found;
          listed.push(visits ? `${found.slug}:${visits}` : found.slug);
        }
        parkList = [...new Set(listed)].join(",");
      }
      const flag = (value: boolean | undefined) =>
        value === undefined ? undefined : value ? "1" : "0";
      const query: TripWhenQueryDto = {
        from,
        to,
        days: input.days,
        parks: parkList,
        within: input.within,
        hotel: input.hotel,
        area: input.area,
        blocks: flag(input.blocks),
        fireworks: input.fireworks,
        split: input.split,
        rest: input.rest,
        hop: flag(input.hop),
        arrival: input.arrival,
        departure: input.departure,
        bases: input.bases,
        startBase: input.startBase,
        endBase: input.endBase,
        moveBuffer: input.moveBuffer,
        limit: input.limit,
        tickets: input.tickets,
        suggest: flag(input.suggest),
      };
      const result = await tripCall(() =>
        ctx.get(TripWhenService).when(region.slug, query),
      );
      const normalized = await tripCall(() =>
        normalizeWhenQuery(
          region,
          parks.map((p) => p.slug),
          query,
          today,
        ),
      );
      return tripDatesSummary({
        result,
        plan: normalized.plan,
        region,
        names,
      });
    },
  }),
  tool({
    name: "get_park_events",
    status: "forecast",
    description:
      "The named seasons and events (food festivals, Christmas or Halloween party nights, " +
      "Horror Nights, a closure) of one park, or of all parks of a trip region without park " +
      "(Orlando by default), each confirmed, announced or expected (last year's pattern). For a " +
      "trip region's parks also the days with an early slot (Orlando: Early Entry, Early Park " +
      "Admission), Extended Evening Hours and party nights, and when day guests leave. The next " +
      "31 days by default, up to 92.",
    input: z.object({
      region: regionInput,
      park: z.string().min(2).max(120).optional(),
      from: isoDay.optional(),
      to: isoDay.optional(),
    }),
    run: async ({ region: regionSlug, park: reference, from, to }, ctx) => {
      let { regions, region, parks, names } = await tripRegion(ctx, regionSlug);
      let slug: string | null = null;
      let elsewhere: Park | null = null;
      if (reference) {
        const matched = regionParkFor(reference, parks);
        if (matched && "ambiguous" in matched) return matched;
        if (matched) {
          slug = matched.slug;
        } else {
          const found = await resolvePark(mcp(ctx), reference);
          if (isAmbiguous(found)) return found;
          const inRegion = parks.find((p) => p.id === found.id);
          const other = inRegion ? null : regionOfPark(found);
          if (inRegion) slug = inRegion.slug;
          else if (other) {
            // A park of another trip region reads that region's calendar.
            ({ regions, region, parks, names } = await tripRegion(
              ctx,
              other.slug,
            ));
            slug = parks.find((p) => p.id === found.id)?.slug ?? null;
            if (!slug) elsewhere = found;
          } else elsewhere = found;
        }
      }
      if (elsewhere) {
        const today = getCurrentDateInTimezone(elsewhere.timezone);
        const first = from ?? today;
        const last = to ?? addIsoDays(first, DEFAULT_EVENT_DAYS - 1);
        if (first < today || last < first) {
          throw new McpToolError(`Pick days from ${today} on, in order`);
        }
        if (
          daysBetween(first, last) >= MAX_CALENDAR_DAYS ||
          last > addIsoDays(today, DAYS_AHEAD)
        ) {
          throw new McpToolError(
            `Pick at most ${MAX_CALENDAR_DAYS} days, up to a year ahead`,
          );
        }
        const seasons = await ctx
          .get(ParkSeasonService)
          .forParkInRange(elsewhere.id, first, last);
        const name = elsewhere.name;
        return {
          scope: name,
          from: first,
          to: last,
          ...seasonsSummary(
            seasons.map((s) => ({ ...s, parkName: name })),
            first,
            last,
          ),
          slots: `early and evening hours per day are known for the ${tripRegionNames()} parks only; get_days has this park's days`,
        };
      }
      ctx.notePark?.(region.timezone);
      const first = from ?? regions.today(region);
      const last = to ?? addIsoDays(first, DEFAULT_EVENT_DAYS - 1);
      await tripCall(() =>
        regions.validateWindow(region, first, last, MAX_CALENDAR_DAYS),
      );
      const calendar = await regions.internalCalendar(region, first, last);
      const seasons = (calendar.seasons as unknown as SeasonRow[])
        .filter((s) => !slug || s.parkSlug === slug)
        .map((s) => ({
          ...s,
          parkName: names.get(s.parkSlug ?? "") ?? s.parkSlug ?? "",
        }));
      return {
        scope: slug ? names.get(slug) : region.name,
        from: first,
        to: last,
        ...seasonsSummary(seasons, first, last),
        slots: slotsSummary(
          calendar.days,
          slug ? [slug] : calendar.parks,
          names,
          region.timezone,
          region,
        ),
        slotsWhy:
          "slots come with the published opening hours; a day past them lists none yet",
      };
    },
  }),
  tool({
    name: "get_travel_time",
    status: "parks",
    description:
      "Between two parks of a trip region (Orlando by default), or from a hotel area to a park: " +
      "km and minutes of a drive without traffic, or the train where the region travels by " +
      "rail, where a drive parks (Magic Kingdom: the TTC), and the resort's own transport " +
      "(monorail, Skyliner, boat, bus, Hogwarts Express, walking). Trip regions only.",
    input: z.object({
      region: regionInput,
      from: z.string().min(2).max(120).optional(),
      area: z
        .string()
        .regex(/^[a-z0-9_-]+$/)
        .optional()
        .describe(
          `Instead of from, a hotel area of the region. Orlando: ${ORLANDO.hotelAreas.map((a) => a.key).join(", ")}`,
        ),
      to: z.string().min(2).max(120),
    }),
    run: async ({ region: regionSlug, from, area, to }, ctx) => {
      if (!from === !area) {
        throw new McpToolError("Give either from (a park) or area");
      }
      const { region, parks } = await tripRegion(ctx, regionSlug);
      const orlandoOnly = `park.fan has travel times between the ${region.name} parks and from ${region.name} hotel areas only, none for this park`;
      const target = await regionPark(ctx, to, parks, orlandoOnly);
      if ("ambiguous" in target) return target;
      let start: { point: string; name: string };
      if (area) {
        const hotelArea = region.hotelAreas.find((a) => a.key === area);
        if (!hotelArea) {
          throw new McpToolError(
            `${area} is not a hotel area of ${region.name}: ${region.hotelAreas.map((a) => a.key).join(", ")}`,
          );
        }
        start = { point: hotelPoint(area), name: hotelArea.name };
      } else {
        const found = await regionPark(ctx, from as string, parks, orlandoOnly);
        if ("ambiguous" in found) return found;
        start = { point: found.slug, name: found.name };
      }
      if (start.point === target.slug) {
        throw new McpToolError("That is the same park");
      }
      const leg = findLeg(buildLegs(region), start.point, target.slug);
      if (!leg)
        throw new McpToolError("park.fan has no travel time for this pair");
      return travelSummary(leg, start.name, target.name, region);
    },
  }),
  tool({
    name: "search_posts",
    status: "posts",
    description:
      "Search park.fan's own news and blog posts in the chat's language. Returns title, " +
      "description, date, slug and url, best first. With question, the result also holds the " +
      "best post's text for it (read), so no get_post is needed; to show posts as a card, " +
      "show_card kind posts does the same search.",
    input: z.object({
      query: z
        .string()
        .min(2)
        .max(100)
        .describe(
          "Two or three key words, e.g. a park and a topic: every word must match, so never the whole question",
        ),
      kind: z
        .enum(["news", "article", "any"])
        .default("any")
        .describe("news for news items, article for guides and blog posts"),
      question: z
        .string()
        .min(2)
        .max(200)
        .optional()
        .describe("The user's question, to read the best post for it"),
    }),
    run: async ({ query, kind, question }, ctx) => {
      const posts = await findPosts(ctx, query, kind);
      const best = posts[0];
      return {
        posts: posts.map(postRow),
        ...(posts.length === 0 && {
          why: `no park.fan ${kind === "any" ? "post" : kind} matches "${query}" in this language`,
        }),
        ...(question &&
          best && { read: await readPost(ctx, best.slug, question) }),
      };
    },
  }),
  tool({
    name: "get_post",
    status: "posts",
    description:
      "The text of one park.fan post, by its slug (from search_posts, a posts card or the page). Long posts are shortened: " +
      "with a question, text is the intro and the sections that answer it best. sections lists " +
      "all the post's headings, omitted the ones left out; for an omitted one, call again with a " +
      "question naming it.",
    input: z.object({
      slug: z
        .string()
        .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)
        .max(160)
        .describe("The post's slug"),
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
      "short definition and its page. Use it for what a word means, and for a nerdy fact; " +
      "show_card kind glossary does the same lookup and shows the term as a card.",
    input: z.object({
      term: z.string().trim().min(3).max(60).describe("The word to look up"),
    }),
    run: async ({ term }, ctx) => ({ terms: await findTerms(ctx, term) }),
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
      "(status, crowd level, today's hours, longest waits; the result also holds everything " +
      "get_park_live returns, so never call both for one park), a ride (status, standby wait, crowd " +
      "level; by rideId or by its name with park; the result also holds everything get_ride " +
      "returns, so never call both for one ride), a park's rides with their current waits (kind rides, longest or shortest " +
      "first, then rides that are down; type for coasters, dark rides, water or family rides " +
      "only; shelter for rides that keep you dry in the rain; the result lists each row with " +
      "its type ids), several parks to click " +
      "(kind parks), a park's crowd calendar over 7 to 42 days with levels, hours and holidays " +
      "(kind calendar), one day's tiles: live today, else the forecast for date (kind tiles), " +
      "a park's shows with their times on a day (kind shows), its restaurants and food " +
      "stands, optionally matching query (kind restaurants), park.fan's own guides and news " +
      "with cover and teaser (kind posts: query, or the slugs of posts you already have; with " +
      "question the result also holds the first post's text, as search_posts does), or " +
      "glossary terms with their definition (kind glossary, query the word). Whenever the " +
      "answer lists rides with waits, parks, shows or places to eat, or rests on a post or a " +
      "glossary term, show the matching kind instead of writing the list or the link as text; " +
      "then name your pick in a sentence without repeating the card's numbers. " +
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
        "posts",
        "glossary",
      ]),
      query: z
        .string()
        .trim()
        .min(2)
        .max(100)
        .optional()
        .describe(
          'For kind restaurants: only names or cuisines with this, e.g. "eis"; for kind posts: two or three key words, every one must match, e.g. "Quick Pass Phantasialand", never the whole question; for kind glossary: the word',
        ),
      postKind: z
        .enum(["news", "article", "any"])
        .default("any")
        .describe(
          "For kind posts with query: news, article (guides and blog) or any",
        ),
      slugs: z
        .array(z.string().regex(CONTENT_SLUG).max(160))
        .min(1)
        .max(MAX_POST_CARD)
        .optional()
        .describe(
          "For kind posts: exactly these posts, by slug, instead of a search",
        ),
      question: z
        .string()
        .min(2)
        .max(200)
        .optional()
        .describe(
          "For kind posts: the user's question; the result then holds the first post's text for it",
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
        .describe(
          "For kind park, ride (with a ride's name), rides, calendar, tiles, shows or restaurants: the park's name, slug or id",
        ),
      order: z
        .enum(["longest", "shortest"])
        .default("longest")
        .describe("For kind rides: longest or shortest waits first"),
      type: z
        .enum(RIDE_TYPE_FILTERS)
        .optional()
        .describe("For kind rides: only rides of this type"),
      shelter: z
        .enum(SHELTER_FILTERS)
        .optional()
        .describe(
          "For kind rides: covered (indoors or a roofed queue) or indoor (the ride too)",
        ),
      riderHeightCm: riderHeight
        .optional()
        .describe(
          "For kind rides: only rides someone this tall may ride; each row carries its limits",
        ),
      limit: z
        .number()
        .int()
        .min(1)
        .max(10)
        .optional()
        .describe(
          "Rows: kind rides 6 by default; kind posts at most 3 (default 3), kind glossary at most 3 (default 1)",
        ),
      rideId: z.string().uuid().optional().describe("For kind ride: ride id"),
      ride: z
        .string()
        .trim()
        .min(2)
        .max(100)
        .optional()
        .describe(
          'For kind ride, instead of rideId: the ride\'s name, e.g. "Taron", with park',
        ),
    }),
    run: async (
      {
        kind,
        park,
        parks,
        rideId,
        ride,
        order,
        type,
        shelter,
        riderHeightCm,
        limit,
        from,
        days,
        date,
        query,
        postKind,
        slugs,
        question,
      },
      ctx,
    ) => {
      if (kind === "posts") {
        return postsCard(ctx, { query, postKind, slugs, question, limit });
      }
      if (kind === "glossary") {
        if (!query)
          throw new McpToolError("kind glossary needs query, the word");
        return glossaryCard(ctx, query, limit);
      }
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
            shows: card.shows.map(({ name, status, times }) => ({
              name,
              status,
              times,
            })),
            withoutShowtimes: found.withoutShowtimes,
            withoutShowtimesNames: found.withoutShowtimesNames,
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
            // With their ids, which set_favorite takes.
            restaurants: found.restaurants
              .slice(0, MAX_RESTAURANT_CARD)
              .map(({ id, name, cuisine, land }) => ({
                id,
                name,
                ...(cuisine && { cuisine }),
                ...(land && { land }),
              })),
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
        const notShown: Array<{ park: string; why: string }> = [];
        const rows = (
          await Promise.all(
            parks.map(async (reference) => {
              const found = await resolvePark(mcp(ctx), reference).catch(
                () => null,
              );
              if (!found || isAmbiguous(found)) {
                notShown.push({
                  park: reference,
                  why: found
                    ? "several parks share this slug; give the id"
                    : "no park with this id or slug; try search_parks",
                });
                return null;
              }
              const live = (await parkNow(ctx, {
                park: found.id,
                longestWaits: 0,
              }).catch(() => null)) as ParkSummary | null;
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
          {
            shown: rows.map(({ url: _url, ...row }) => row),
            ...(notShown.length > 0 && { notShown }),
          },
        );
      }
      if (kind === "rides") {
        if (!park) throw new McpToolError("kind rides needs park");
        // Every operating ride with a posted wait, longest first, so both ends can be cut: with
        // only the 30 longest, "shortest" at a big park was the shortest of the long ones.
        const live = (await parkNow(ctx, {
          park,
          longestWaits: ALL_WAITING_RIDES,
        })) as ParkSummary;
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
        let typeUnknownRides: Array<{ id: string; name: string }> = [];
        if (type) {
          const untyped = [...all, ...down].filter(
            (ride) => typesOf(ride).length === 0,
          );
          typeUnknown = untyped.length;
          typeUnknownRides = untyped
            .slice(0, LEFT_OUT_NAMES_LIMIT)
            .map((ride) => ({ id: ride.id, name: ride.name }));
          const ofType = (ride: { id: string }) =>
            isRideOfType(typesOf(ride), type);
          all = all.filter(ofType);
          down = down.filter(ofType);
        }
        // Read only for a rain or height question: one query for every candidate, before any
        // is cut.
        const rows =
          shelter || riderHeightCm !== undefined
            ? await attractionsById(ctx, [...all, ...down])
            : null;
        const shelterOf = shelter && rows ? shelterMap(rows) : null;
        const candidates = [...all, ...down];
        const coverage =
          shelter && shelterOf
            ? {
                ...shelterCoverage(candidates.map((r) => shelterOf.get(r.id))),
                shelterUnknownRides: candidates
                  .filter((r) => !shelterOf.has(r.id))
                  .slice(0, LEFT_OUT_NAMES_LIMIT)
                  .map((r) => ({ id: r.id, name: r.name })),
              }
            : null;
        if (shelter && shelterOf) {
          const dry = (ride: { id: string }) =>
            isSheltered(shelterOf.get(ride.id), shelter);
          all = all.filter(dry);
          down = down.filter(dry);
        }
        const heightOf = (ride: { id: string }) => {
          const row = rows?.get(ride.id);
          if (!row) return {};
          const facts = resolveCuratedFacts(row);
          return {
            ...(facts.minimumHeight !== null && {
              minimumHeightCm: facts.minimumHeight,
            }),
            ...(facts.maximumHeight !== null && {
              maximumHeightCm: facts.maximumHeight,
            }),
          };
        };
        let heights: Record<string, unknown> | null = null;
        if (riderHeightCm !== undefined) {
          const unknown = [...all, ...down].filter(
            (ride) => heightOf(ride).minimumHeightCm === undefined,
          );
          const fits = (ride: { id: string }) =>
            riderFits(heightOf(ride), riderHeightCm);
          const tooShort = [...all, ...down].filter(
            (ride) => !unknown.includes(ride) && !fits(ride),
          );
          heights = {
            heightUnknown: unknown.length,
            heightUnknownRides: unknown
              .slice(0, LEFT_OUT_NAMES_LIMIT)
              .map((ride) => ({ id: ride.id, name: ride.name })),
            notAllowed: tooShort.length,
            notAllowedRides: tooShort
              .slice(0, LEFT_OUT_NAMES_LIMIT)
              .map((ride) => ({
                id: ride.id,
                name: ride.name,
                ...heightOf(ride),
              })),
          };
          all = all.filter(fits);
          down = down.filter(fits);
        }
        const rowCount = limit ?? DEFAULT_RIDE_ROWS;
        const waiting = (order === "shortest" ? [...all].reverse() : all).slice(
          0,
          rowCount,
        );
        // Rides down right now follow the waits; a full "shortest" list has no room for them.
        const room =
          order === "shortest"
            ? Math.min(MAX_DOWN_ROWS, rowCount - waiting.length)
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
            shown: `${rides.length} ${shelter ? `${shelter} ` : ""}${type ? `${type.replace("_", " ")}s` : "rides"} of ${live.name}`,
            rows: picked.map((ride) => {
              const types = typesOf(ride);
              const dryness = shelterOf?.get(ride.id);
              return {
                id: ride.id,
                name: ride.name,
                status: ride.status,
                waitMinutes: ride.waitMinutes,
                ...heightOf(ride),
                ...(types.length > 0 && { types }),
                ...(dryness && { indoorOutdoor: dryness }),
                ...riddenMark(ctx, ride.id),
              };
            }),
            ...(type && { typeUnknown, typeUnknownRides }),
            ...coverage,
            ...heights,
            // An empty card of a closed park says why, so it never reads as "no such rides".
            ...(live.status !== "OPERATING" && {
              parkStatus: live.status ?? null,
              why: "the park is not operating now, so no ride has a wait",
              nextOpening: live.nextSchedule ?? null,
            }),
          },
        );
      }
      if (kind === "park") {
        if (!park) throw new McpToolError("kind park needs park");
        const summary = await parkLiveSummary(ctx, park);
        const live = summary as ParkSummary;
        if ("ambiguous" in live) return live;
        const today = live.scheduleToday?.[0];
        const card: AssistantCard = {
          kind: "park",
          name: live.name,
          place: [live.city, live.country].filter(Boolean).join(", ") || null,
          status: live.status ?? null,
          crowdLevel: live.crowd?.level ?? null,
          // With the park's own offset: the widget reads the park's clock off it.
          opens: parkInstant(today?.opens, String(summary.timezone ?? "UTC")),
          closes: parkInstant(today?.closes, String(summary.timezone ?? "UTC")),
          waits: (live.rides?.longestWaits ?? [])
            .slice(0, PARK_CARD_WAITS)
            .map((ride) => ({
              name: ride.name,
              waitMinutes: ride.waitMinutes,
            })),
          url: live.pageUrl ? siteOnlyPath(live.pageUrl) : null,
        };
        // What the card says and everything get_park_live would, so no second lookup follows.
        const { url: _url, ...shown } = card;
        return new ActionOutcome(
          { type: "card", card },
          {
            shown,
            live: summary,
            ...(live.status !== "OPERATING" && {
              nextOpening: live.nextSchedule ?? null,
            }),
          },
        );
      }
      if (!rideId && !ride) {
        throw new McpToolError("kind ride needs rideId, or ride with its park");
      }
      const resolved = await rideIdOf(ctx, { rideId, ride, park });
      if (typeof resolved !== "string") return resolved;
      const details = await rideDetails(ctx, resolved);
      const found = details as unknown as RideSummary;
      const standby = found.queues?.find((queue) => queue.type === "STANDBY");
      const card: AssistantCard = {
        kind: "ride",
        name: found.name,
        park: found.park?.name ?? null,
        status: found.effectiveStatus ?? found.status ?? null,
        crowdLevel: found.crowdLevel ?? null,
        waitMinutes: standby?.waitMinutes ?? null,
        url: found.pageUrl ? siteOnlyPath(found.pageUrl) : null,
        ...rideBadges(found),
      };
      const { url: _url, ...shown } = card;
      // What the card says and everything get_ride would, so no second lookup follows.
      return new ActionOutcome(
        { type: "card", card },
        { shown, ride: details },
      );
    },
  }),
  tool({
    name: "open_page",
    description:
      "Take the user to a park.fan page in their browser. Give exactly one target: a park (id or " +
      "slug; with section its wait-time calendar, its average waits or its page for a visit " +
      "with kids where it has one, else the error says why), a ride (rideId), a post or glossary " +
      "term (its url from search_posts or look_up_glossary), a region's hub with a plan (url: " +
      "plan_vacation's hubUrl), 2 to 4 parks side by side (compare), or one of the site's pages " +
      "(page: alerts, the user's push alerts; best_time, the best time to visit across parks; " +
      "favorites; day_planner, the day planner for one park day; how_it_works, how park.fan gets " +
      "its waits and forecasts; fancast, how accurate the forecasts were; trip_hub with region, a " +
      "live region's hub with its crowd calendar and trip planner). The page address is built " +
      "from the park.fan data, never typed by you.",
    input: z.object({
      park: parkRef.optional(),
      section: z
        .enum(PARK_SECTIONS)
        .optional()
        .describe(
          "With park: calendar (wait-time calendar), averages (average waits), kids (with kids)",
        ),
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
          "A post's or glossary term's url, or a hubUrl, exactly as a tool returned it",
        ),
      compare: z
        .array(parkRef)
        .min(2)
        .max(4)
        .optional()
        .describe("Park ids or slugs for the park comparison"),
      page: z.enum(SITE_PAGES).optional(),
      region: regionInput.describe(
        "With page trip_hub: the region; default orlando",
      ),
    }),
    run: async ({ park, section, rideId, url, compare, page, region }, ctx) => {
      const targets = [park, rideId, url, compare, page].filter(
        (target) => target !== undefined,
      );
      if (targets.length !== 1 || (section && !park)) {
        throw new McpToolError(
          "Give exactly one target: park (with or without section), rideId, url, compare or page",
        );
      }
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
        const missing = section
          ? await missingSection(ctx, found, section)
          : null;
        if (missing) throw new McpToolError(missing);
        const pageUrl = parkPageUrl(found);
        const base = pageUrl ? siteOnlyPath(pageUrl) : null;
        path =
          base && section ? `${base}/${PARK_SECTION_SEGMENTS[section]}` : base;
      } else if (url) {
        const candidate = siteOnlyPath(url);
        // Posts, glossary terms and the hub with a plan only; parks and rides go by id, so no
        // address is guessed.
        path =
          candidate && CONTENT_PATH.test(candidate) ? candidate : hubPath(url);
        const hubRegion = path && HUB_REGION.exec(path)?.[1];
        if (hubRegion && !regionLive(ctx, hubRegion))
          throw comingRegion(hubRegion);
      } else if (compare) {
        const found = await Promise.all(
          compare.map((reference) => resolvePark(mcp(ctx), reference)),
        );
        const ambiguous = found.find(isAmbiguous);
        if (ambiguous) return ambiguous;
        // The long form, continent/country/city/park: a bare slug is shared by Paris and Anaheim.
        const refs = (found as Park[]).map((one) =>
          frontendParkPath(one)?.replace(/^\/parks\//, ""),
        );
        path = refs.every(Boolean)
          ? `/compare?parks=${[...new Set(refs)].join(",")}`
          : null;
      } else if (page === "trip_hub") {
        const slug = region ?? ORLANDO.slug;
        if (!regionLive(ctx, slug)) throw comingRegion(slug);
        path = hubPathOf({ slug });
      } else if (page) {
        path = SITE_PAGE_PATHS[page];
      }
      if (!path) {
        throw new McpToolError(
          "Give a park, a rideId, a post or glossary url a tool returned, compare or page",
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
      "the user. Nor is a ride the party's shortest rider (the day's riderHeightCm) is under " +
      "the minimum for: notAdded with reason too_short and the minimum. despiteHeight adds such " +
      "a ride anyway, only when the user asked for it after hearing its minimum.",
    input: z.object({
      park: parkRef,
      date: planDate,
      rideIds: z
        .array(z.string().uuid())
        .min(1)
        .max(10)
        .describe("Ride ids, at most ten"),
      riderHeightCm: riderHeight
        .optional()
        .describe(
          "The party's shortest rider, when the plan does not hold it yet",
        ),
      despiteHeight: z
        .boolean()
        .optional()
        .describe(
          "Add rides the party's shortest rider is too short for (an adult rides alone)",
        ),
    }),
    run: async (
      { park: reference, date, rideIds, riderHeightCm, despiteHeight },
      ctx,
    ) => {
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      const found = await parkRides(mcp(ctx), park.slug, rideIds, date);
      const party =
        snapshotDay(ctx, park.slug, date)?.prefs?.riderHeightCm ??
        riderHeightCm;
      const tooShort = (ride: (typeof found)[number]) =>
        !despiteHeight && partyTooShort(ride.minimumHeightCm, party);
      // The planner refuses a ride in maintenance on that day by every other way in, so this
      // way in does too, and says which and until when instead of reporting it added. A ride
      // the child may not take is never picked for the party unasked.
      const rides = found
        .filter((ride) => !ride.maintenance && !tooShort(ride))
        .map(({ slug, name }) => ({ slug, name }));
      const notAdded = found.flatMap((ride): Record<string, unknown>[] =>
        ride.maintenance
          ? [
              {
                id: ride.id,
                name: ride.name,
                reason: "maintenance",
                window: ride.maintenance,
              },
            ]
          : tooShort(ride)
            ? [
                {
                  id: ride.id,
                  name: ride.name,
                  reason: "too_short",
                  minimumHeightCm: ride.minimumHeightCm,
                  riderHeightCm: party,
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
      "shopping, a photo stop or a meeting point. A show's start time comes from get_shows " +
      "for today and get_park_day's shows for another day (projected, so say so); a food stop " +
      "from find_restaurants. A show with a listed time goes in with plan_add_show instead.",
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
      "Move one entry of a planned day: to another place in its order (toIndex, from 0) or to a " +
      "time (start, HH:MM on the park's clock; a show only to one of its times that day).",
    input: z.object({
      park: parkRef,
      date: planDate,
      entryId: plannerEntryId,
      toIndex: z.number().int().min(0).max(39).optional(),
      start: plannerClock.optional(),
    }),
    run: async ({ park: reference, date, entryId, toIndex, start }, ctx) => {
      if ((toIndex === undefined) === (start === undefined)) {
        throw new McpToolError("Give toIndex or start, one of them");
      }
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park, day } = found;
      const [entry] = plannedEntries(day, [entryId]);
      if (toIndex !== undefined && toIndex >= day.entries.length) {
        throw new McpToolError(
          `The day has ${day.entries.length} entries; toIndex is at most ${day.entries.length - 1}`,
        );
      }
      if (start !== undefined && entry.kind === "show") {
        if (!entry.showSlug) {
          throw new McpToolError(
            `${entry.name} is a show; remove it and add it again with plan_add_show at the new time`,
          );
        }
        const show = await showPerformance(
          ctx,
          reference,
          date,
          entry.showSlug,
          start,
        );
        if ("ambiguous" in show) return show;
      }
      return new ActionOutcome(
        {
          type: "planner_move",
          park,
          date,
          entryId,
          ...(toIndex !== undefined ? { toIndex } : { start }),
        },
        {
          moved: entry.name,
          ...(toIndex !== undefined ? { toIndex } : { start }),
          park: park.name,
          date,
        },
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
      "find_rides or a ride list, or the ride's name. One alert per ride, a new one replaces it.",
    input: z.object({
      park: parkRef,
      ...rideRef,
      kind: z.enum(["wait_below", "reopen"]),
      thresholdMinutes: z
        .number()
        .int()
        .min(RIDE_ALERT_THRESHOLD_MIN)
        .max(RIDE_ALERT_THRESHOLD_MAX)
        .optional()
        .describe("For wait_below: notify under this many minutes"),
    }),
    run: async (
      { park: reference, rideId: id, ride: name, kind, thresholdMinutes },
      ctx,
    ) => {
      const resolved = await rideIdOf(ctx, {
        rideId: id,
        ride: name,
        park: reference,
      });
      if (typeof resolved !== "string") return resolved;
      const rideId = resolved;
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
      "Reserve days in the trip planner at once, empty days the user fills later: several days " +
      "at one park, or a trip across parks (days, as plan_vacation's or get_trip_plan's reserve). " +
      "riderHeightCm: the party's smallest rider, written to every day.",
    input: z.object({
      park: parkRef.optional(),
      dates: z.array(planDate).min(1).max(6).optional(),
      days: z
        .array(z.object({ park: z.string().min(2).max(120), date: planDate }))
        .min(1)
        .max(MAX_PARK_DAYS)
        .optional()
        .describe("Instead of park and dates"),
      riderHeightCm: riderHeight.optional(),
    }),
    run: async ({ park: reference, dates, days, riderHeightCm }, ctx) => {
      const prefs =
        riderHeightCm !== undefined ? { prefs: { riderHeightCm } } : {};
      if (days) {
        if (reference || dates) {
          throw new McpToolError("Give either park and dates, or days");
        }
        const byReference = new Map<string, PlannerPark>();
        for (const { park: ref } of days) {
          if (byReference.has(ref)) continue;
          const park = await planPark(mcp(ctx), ref);
          if ("ambiguous" in park) return park;
          byReference.set(ref, park);
        }
        const seen = new Set<string>();
        const trip = days
          .map(({ park: ref, date }) => ({
            park: byReference.get(ref) as PlannerPark,
            date,
          }))
          .filter(({ park, date }) => {
            const key = `${park.slug}@${date}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          })
          .sort(
            (a, b) =>
              a.date.localeCompare(b.date) ||
              a.park.slug.localeCompare(b.park.slug),
          );
        return new ActionOutcome(
          { type: "planner_reserve_trip", days: trip, ...prefs },
          {
            reserved: trip.map(({ park, date }) => ({ park: park.name, date })),
          },
        );
      }
      if (!reference || !dates) {
        throw new McpToolError("Give either park and dates, or days");
      }
      const park = await planPark(mcp(ctx), reference);
      if ("ambiguous" in park) return park;
      const unique = [...new Set(dates)].sort();
      return new ActionOutcome(
        { type: "planner_reserve_days", park, dates: unique, ...prefs },
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
    name: "review_plan_day",
    status: "planner",
    description:
      "One day of the user's plan as they hold it, priced: each entry with its start and, for " +
      "a ride, the forecast wait when its queue is joined then; the day's total queueing, idle " +
      "time, when it ends against closing, rides joined after closing, clashes; rides a " +
      "maintenance window shuts that day, rides the party's smallest rider is too short for, a " +
      'show at a time it does not play; sortingSaves, what plan_optimize would save. For "how ' +
      'long do I wait in total", "does my day work", and the hint worth giving about a day.',
    input: z.object({ park: parkRef, date: planDate }),
    run: async ({ park: reference, date }, ctx) => {
      const found = await plannedDay(ctx, reference, date);
      if ("ambiguous" in found) return found;
      const { park: planned, day } = found;
      const entries = day.entries.flatMap((entry) => {
        const sent = optimizerEntry(entry);
        return sent ? [sent] : [];
      });
      if (entries.length === 0) {
        throw new McpToolError(
          `Nothing is planned on ${date} at ${planned.name} yet`,
        );
      }
      const park = await ctx
        .get(ParksService)
        .findByGeographicPath(
          planned.geo.continent,
          planned.geo.country,
          planned.geo.city,
          planned.slug,
        );
      if (!park) throw new McpToolError(`No park ${reference}`);
      ctx.notePark?.(park.timezone);
      const planDays = ctx.get(PlanDayService);
      const redis = ctx.get(REDIS_CLIENT as unknown as Type<Redis>);
      const [plan, works, priced] = await Promise.all([
        cachedPlanDay(redis, planDays, park, date),
        planDays.ridesInWorks(park, date).catch(() => []),
        ctx
          .get(PlanOptimizeService)
          .optimize({ ...planned.geo, parkSlug: planned.slug }, {
            date,
            mode: "day",
            entries,
            ...(day.prefs && { prefs: day.prefs }),
            preview: true,
          } as PlanOptimizeRequestDto)
          .catch(() => null),
      ]);
      const party = day.prefs?.riderHeightCm;
      const rides = new Map(plan.rides.map((r) => [r.attractionSlug, r]));
      const inWorks = new Map(works.map((w) => [w.attractionSlug, w]));
      const shows = new Map(plan.shows.map((show) => [show.showSlug, show]));
      const clockMinute = (time: string) => {
        const [h, m] = time.split(":").map(Number);
        return (h * 60 + m) % 1440;
      };
      const before = priced?.before ?? null;
      const after = priced?.status === "optimized" ? priced.after : null;
      const closeHour = plan.context.closeHour;
      return {
        park: planned.name,
        date,
        ...(party !== undefined && { partyRiderHeightCm: party }),
        entries: day.entries.map((entry) => {
          const minute = entryMinute(entry);
          const ride = entry.rideSlug ? rides.get(entry.rideSlug) : undefined;
          const works = entry.rideSlug
            ? inWorks.get(entry.rideSlug)
            : undefined;
          const show = entry.showSlug ? shows.get(entry.showSlug) : undefined;
          const wait =
            ride && minute !== null && !entry.done
              ? waitAt(ride, minute)
              : null;
          return {
            id: entry.id,
            kind: entry.kind,
            name: entry.name,
            ...(minute !== null && { at: clockOfMinute(minute) }),
            ...(entry.done && { done: true }),
            ...(wait !== null && { waitMinutes: wait }),
            ...(works && { inMaintenance: works.worksPeriod }),
            ...(ride &&
              partyTooShort(ride.minimumHeight, party) && {
                tooShortForParty: { minimumHeightCm: ride.minimumHeight },
              }),
            ...(show &&
              minute !== null &&
              !show.times.some((t) => clockMinute(t) === minute % 1440) && {
                notAShowTime: { showTimes: show.times },
              }),
          };
        }),
        ...(before
          ? {
              totalWaitMinutes: Math.round(before.totalWaitMinutes),
              idleMinutes: Math.round(before.idleMinutes),
              endsAt: clockOfMinute(before.endMinute),
              ...(closeHour !== null && {
                closes: clockOfMinute(
                  unfoldedCloseHour(plan.context.openHour ?? 0, closeHour) * 60,
                ),
              }),
              ...(before.overflow > 0 && {
                ridesJoinedAfterClosing: before.overflow,
              }),
              ...(before.clashes > 0 && { clashes: before.clashes }),
              sortingSaves: after
                ? `plan_optimize would save about ${Math.max(0, Math.round(before.totalWaitMinutes - after.totalWaitMinutes))} min of queueing`
                : "the order is as good as it gets",
            }
          : {
              totals: priced?.refusal
                ? `not priced: ${priced.refusal}`
                : "the day could not be priced right now",
            }),
        means:
          "waitMinutes: the forecast standby wait when that ride's queue is joined at its start; totalWaitMinutes: every queue of the day as planned",
      };
    },
  }),
  tool({
    name: "suggest_replies",
    description:
      "Two to four short replies the user can tap under your answer, each sent as their " +
      "message: written as they would say it, in their language, under 40 characters, the " +
      "likeliest first. Call it last, alone, after your answer's text (# Replies).",
    input: z.object({
      replies: z.array(z.string().trim().min(1).max(40)).min(2).max(4),
    }),
    run: ({ replies }, ctx) => {
      claimOnce(ctx, "suggest_replies");
      const seen = new Set<string>();
      const kept = replies
        .map((reply) => reply.replace(/\s+/g, " ").replace(/[*_`#>[\]]/g, ""))
        .filter((reply) => {
          const key = reply.toLowerCase();
          if (!reply || seen.has(key) || /https?:|www\./i.test(reply))
            return false;
          if (junkReason(reply)) return false;
          seen.add(key);
          return true;
        });
      if (kept.length === 0) {
        throw new McpToolError("None of these replies can be shown");
      }
      return Promise.resolve(
        new ActionOutcome(
          { type: "replies", replies: kept },
          { shown: kept.length },
        ),
      );
    },
  }),
  tool({
    name: "interview",
    status: "planner",
    description:
      "Parky's interview, which plans a trip or a park day from simple to detailed (# Interview): " +
      "start one (kind, and region, park or date where known); answered or skip for the step the " +
      "user just answered or passed (any step, also one they answered early; party for a trip's " +
      "party, mustDoRides for its rides); ask to take up a refinement they picked once the plan " +
      "stands; end when they have enough (outcome done) or want to stop (left). The result names " +
      "the next step: what to ask, which tool takes the answer, ideas for the replies.",
    input: z.object({
      op: z.enum(["start", "answered", "skip", "ask", "end"]),
      kind: z.enum(["trip", "day"]).optional().describe("start only"),
      region: regionInput,
      park: parkRef.optional().describe("A park day's park, start only"),
      date: planDate.optional().describe("A park day's date, start only"),
      step: z
        .enum([
          ...new Set([
            ...TRIP_INTERVIEW_CORE,
            ...TRIP_INTERVIEW_REFINE,
            ...DAY_INTERVIEW_CORE,
            ...DAY_INTERVIEW_REFINE,
          ]),
        ] as [string, ...string[]])
        .optional()
        .describe(
          "answered, skip: the step, default the one being asked; ask: the refinement",
        ),
      also: z
        .array(
          z.enum([
            ...new Set([
              ...TRIP_INTERVIEW_CORE,
              ...TRIP_INTERVIEW_REFINE,
              ...DAY_INTERVIEW_CORE,
              ...DAY_INTERVIEW_REFINE,
            ]),
          ] as [string, ...string[]]),
        )
        .max(8)
        .optional()
        .describe("answered: other steps this message answered too"),
      outcome: z.enum(["done", "left"]).optional().describe("end only"),
      party: z
        .object({
          size: z.number().int().min(1).max(30).optional(),
          heightsCm: z
            .array(z.number().int().min(50).max(250))
            .max(10)
            .optional()
            .describe("The children's heights; adults need none"),
        })
        .optional(),
      mustDoRides: z
        .array(
          z.object({
            park: z.string().min(2).max(120),
            rideIds: z.array(z.uuid()).min(1).max(12),
          }),
        )
        .max(8)
        .optional()
        .describe("A trip's must-do rides by park, for the filing"),
    }),
    run: async (input, ctx) => {
      claimOnce(ctx, "interview");
      if (input.region && !regionLive(ctx, input.region)) {
        throw comingRegion(input.region);
      }
      const running = ctx.interview ?? null;
      if (input.op === "end") {
        if (!running) throw new McpToolError("No interview is running");
        const outcome = input.outcome ?? "left";
        return new ActionOutcome(
          { type: "interview_state", interview: null, outcome },
          {
            ended: outcome,
            say:
              outcome === "done"
                ? "one warm line on the plan they have; no question"
                : "one short line: everything answered so far stays in the planner, and they can pick it up any time",
          },
        );
      }
      let state: InterviewSnapshot;
      if (input.op === "start") {
        if (!input.kind) throw new McpToolError("start needs kind");
        let park: string | undefined;
        if (input.park) {
          const found = await planPark(mcp(ctx), input.park);
          if ("ambiguous" in found) return found;
          park = found.slug;
        }
        const region =
          input.region ??
          (input.kind === "trip"
            ? pickTrip(ctx.trips ?? [])?.region.slug
            : undefined);
        state = {
          kind: input.kind,
          ...(region && { region }),
          ...(park && { park }),
          ...(input.date && { date: input.date }),
          step: interviewCore(input.kind)[0],
          answered: [],
          skipped: [],
          mustDoRides: [],
          source: running?.source ?? "chat",
        };
      } else {
        if (!running) {
          throw new McpToolError("No interview is running; start one first");
        }
        state = {
          ...running,
          // The region step's answer: the interview names its region from here on.
          ...(running.kind === "trip" &&
            input.region && { region: input.region }),
          answered: [...running.answered],
          skipped: [...running.skipped],
          mustDoRides: [...running.mustDoRides],
        };
        if (input.op === "ask") {
          const open = openRefinements(state);
          if (!input.step || !open.includes(input.step as InterviewStep)) {
            throw new McpToolError(
              `ask takes a refinement still open: ${open.join(", ") || "none left"}`,
            );
          }
        } else {
          const step = (input.step ?? running.step) as InterviewStep;
          if (step === INTERVIEW_REFINE) {
            throw new McpToolError(
              "Name the step that was answered, or use ask for a refinement they picked",
            );
          }
          const list = input.op === "answered" ? "answered" : "skipped";
          const also =
            input.op === "answered" ? (input.also ?? []) : ([] as string[]);
          state[list] = [
            ...new Set([...state[list], step, ...(also as InterviewStep[])]),
          ];
        }
        for (const { park: reference, rideIds } of input.mustDoRides ?? []) {
          const found = await planPark(mcp(ctx), reference);
          if ("ambiguous" in found) return found;
          const kept = state.mustDoRides.filter((m) => m.park !== found.slug);
          const had =
            state.mustDoRides.find((m) => m.park === found.slug)?.rides ?? [];
          state.mustDoRides = [
            ...kept,
            {
              park: found.slug,
              rides: [...new Set([...had, ...rideIds])].slice(0, 12),
            },
          ].slice(0, 8);
        }
      }
      const next =
        input.op === "ask"
          ? (input.step as InterviewStep)
          : nextInterviewStep(state);
      const party =
        input.party && state.kind === "trip"
          ? { size: input.party.size, heightsCm: input.party.heightsCm ?? [] }
          : undefined;
      const region = state.region ?? pickTrip(ctx.trips ?? [])?.region.slug;
      const extras = {
        ...(party && { party }),
        ...(party && region && { region }),
      };
      if (!next) {
        return new ActionOutcome(
          {
            type: "interview_state",
            interview: null,
            outcome: "done",
            ...extras,
          },
          {
            done: true,
            say: "the plan is in the planner and every refinement is in: two sentences on what stands out, then suggest_replies for changes (swap two days, why this order, add a rest day)",
          },
        );
      }
      state.step = next;
      const trip = pickTrip(ctx.trips ?? [], state.region) ?? null;
      return new ActionOutcome(
        { type: "interview_state", interview: state, ...extras },
        {
          next: interviewGuide(
            state,
            next,
            next === INTERVIEW_REFINE
              ? null
              : interviewHolds(state, next, trip, ctx.planner),
            trip?.region ?? (state.region ? findRegion(state.region) : null),
          ),
          ...(state.mustDoRides.length > 0 && {
            mustDoRides: state.mustDoRides,
          }),
          then: "one short question or the plan in your voice, and end with suggest_replies",
        },
      );
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
    // The parks this call is about, so its times come in their clock, not the visitor's.
    const zones = new Set<string>();
    const callCtx: AssistantToolContext = {
      ...ctx,
      notePark: (timezone) => zones.add(timezone),
    };
    const outcome = await definition.run(input.data, callCtx);
    const action =
      outcome instanceof ActionOutcome ? outcome.action : undefined;
    const plain = outcome instanceof ActionOutcome ? outcome.result : outcome;
    const zone = zones.size === 1 ? [...zones][0] : null;
    const shown =
      action?.type === "card" || action?.type === "navigate"
        ? { ...(plain as object), alreadyShown: ALREADY_SHOWN }
        : plain;
    const labelled =
      zone && shown && typeof shown === "object" && !Array.isArray(shown)
        ? {
            // The park's own now beside its times: "the next show" is the next after this, not
            // after the visitor's clock, which the chat context gives.
            timesIn: `park time, ${zone}; now there ${formatInTimeZone(Date.now(), zone, "yyyy-MM-dd HH:mm")}`,
            ...shown,
          }
        : shown;
    const result = JSON.stringify(
      localizeTimes(
        localizePageUrls(labelled, ctx.locale),
        zone,
        [...zones][0] ?? ctx.timeZone ?? "UTC",
      ),
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
