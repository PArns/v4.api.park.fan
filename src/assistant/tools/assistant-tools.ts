import type { Type } from "@nestjs/common";
import type Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { McpToolError, type McpToolContext } from "../../mcp/mcp-tool";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { findParksTool } from "../../mcp/tools/find-parks.tool";
import { getParkTool } from "../../mcp/tools/get-park.tool";
import { findAttractionsTool } from "../../mcp/tools/find-attractions.tool";
import { getAttractionTool } from "../../mcp/tools/get-attraction.tool";
import { parkForecastTool } from "../../mcp/tools/park-forecast.tool";
import { isAmbiguous, resolvePark } from "../../mcp/tools/park-ref";
import { attractionPageUrl, parkPageUrl } from "../../mcp/tools/links";
import { BestDaysService } from "../../parks/services/best-days.service";
import { WeatherService } from "../../parks/weather.service";
import { PlanDayService } from "../../parks/services/plan-day.service";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";
import { DataSource, In } from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AssistantPostsClient } from "./assistant-posts.client";
import { getFrontendUrl } from "../../config/oauth.config";
import {
  bookableHours,
  localizeTimes,
  parkHourInstant,
} from "../assistant-time";
import { formatInTimeZone } from "date-fns-tz";
import { unfoldedCloseHour } from "../../common/utils/day-shape.util";
import { LocationService } from "../../location/location.service";
import { GeoipService } from "../../geoip/geoip.service";
import {
  ActionOutcome,
  clockTime,
  parkRides,
  planDate,
  PLANNER_BLOCK_ICONS,
  planPark,
  siteOnlyPath,
  type AssistantCard,
  type ClientAction,
} from "./assistant-actions";

/** The fields of `get_park`'s summary a card reads. */
type ParkSummary =
  | { ambiguous: true }
  | {
      name: string;
      city?: string | null;
      country?: string | null;
      status?: string | null;
      pageUrl?: string | null;
      crowd?: { level?: string | null } | null;
      scheduleToday?: Array<{ opens?: string | null; closes?: string | null }>;
      rides?: {
        longestWaits?: Array<{ id: string; name: string; waitMinutes: number }>;
      };
    };

/** The fields of `get_attraction`'s answer a card reads. */
interface RideSummary {
  name: string;
  park?: { name: string } | null;
  status?: string | null;
  effectiveStatus?: string | null;
  crowdLevel?: string | null;
  pageUrl?: string | null;
  queues?: Array<{ type: string; waitMinutes: number | null }>;
}

/**
 * Everything Parky may call: read-only, public park data and park.fan's own
 * posts, plus three actions the browser carries out (open a park.fan page, open
 * a day in the trip planner, add rides to it). No web search, no fetching of
 * arbitrary URLs, no admin data.
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

/** A post's or glossary term's page: `/<locale>/blog|news|glossary/<slug>`. */
const CONTENT_PATH =
  /^\/(?:en|de|nl|fr|es|it)\/(?:blog|news|glossary)\/[a-z0-9]+(?:-[a-z0-9]+)*$/;

const parkRef = z
  .string()
  .min(2)
  .max(120)
  .describe('Park id from search_parks, or its slug like "phantasialand"');

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
      "The park right now: open or closed, today's hours (park local time), crowd level, average " +
      "and peak waits today, weather now, the longest current standby waits, rides that are down " +
      "and rides that have not run today, and the park's pageUrl.",
    input: z.object({ park: parkRef }),
    run: ({ park }, ctx) =>
      getParkTool.handler({ park, longestWaits: 8 }, mcp(ctx)),
  }),
  tool({
    name: "find_rides",
    status: "rides",
    description:
      "Find rides by name, optionally within one park. Returns ids for get_ride.",
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
    name: "get_ride",
    status: "rides",
    description:
      "One ride now: status, every queue with its wait, crowd level and trend, a running outage, " +
      "facts (minimum height, gets you wet, single rider, indoor/outdoor, season), typical waits " +
      "on weekdays and weekends, reliability, and its pageUrl.",
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
      return ride;
    },
  }),
  tool({
    name: "get_ride_forecast",
    status: "forecast",
    description:
      "The predicted standby wait of one ride per hour of a day (park local hours), the hour with " +
      'the shortest predicted wait, and how far ahead the forecast is (tier). For "when is the ' +
      'queue for X shortest". Today by default.',
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
      const best = hours.reduce<(typeof hours)[number] | null>(
        (low, hour) => (low === null || hour.wait < low.wait ? hour : low),
        null,
      );
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
      };
    },
  }),
  tool({
    name: "get_park_forecast",
    status: "forecast",
    description:
      "The days ahead for one park: per day the opening hours or closed, the expected crowd level, " +
      "the expected peak wait of the headliners, weather (basis climate_normal is the long-run " +
      "average, not a forecast) and holidays; plus today's hourly forecast. Days after " +
      "scheduleCoverage.to have no published hours yet: treat their status as unknown.",
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
      "The quietest upcoming open days of one park in the next 90 days (or a range you give), " +
      "best first, and the typical crowd per weekday from history (dayOfWeek 0 = Sunday, lower " +
      'avgCrowdScore = quieter). Use for "when should I go".',
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
      return {
        park: { id: found.id, name: found.name, pageUrl: parkPageUrl(found) },
        window: { from: result.meta.windowFrom, to: result.meta.windowTo },
        openDays: open.length,
        quietestDays: open
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
      "The text of one park.fan post (shortened), by its slug from search_posts.",
    input: z.object({
      slug: z
        .string()
        .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/)
        .max(160)
        .describe("Slug from search_posts"),
    }),
    run: ({ slug }, ctx) =>
      ctx.get(AssistantPostsClient).post(ctx.locale, slug),
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
      "first), or several parks to click (kind parks). Whenever the answer lists rides with " +
      "waits, show kind rides, and whenever it lists parks, show kind parks, instead of writing " +
      "the list as text; keep the text to a sentence and do not repeat the card's numbers.",
    input: z.object({
      kind: z.enum(["park", "ride", "rides", "parks"]),
      parks: z
        .array(parkRef)
        .min(1)
        .max(8)
        .optional()
        .describe("For kind parks: park ids or slugs, in the order to show"),
      park: parkRef
        .optional()
        .describe("For kind park or rides: park id or slug"),
      order: z
        .enum(["longest", "shortest"])
        .default("longest")
        .describe("For kind rides: longest or shortest waits first"),
      limit: z.number().int().min(1).max(10).default(6),
      rideId: z.string().uuid().optional().describe("For kind ride: ride id"),
    }),
    run: async ({ kind, park, parks, rideId, order, limit }, ctx) => {
      if (kind === "parks") {
        if (!parks?.length) throw new McpToolError("kind parks needs parks");
        const rows: Array<{
          name: string;
          place: string | null;
          url: string | null;
        }> = [];
        for (const reference of parks) {
          const found = await resolvePark(mcp(ctx), reference).catch(
            () => null,
          );
          if (!found || isAmbiguous(found)) continue;
          const pageUrl = parkPageUrl(found);
          rows.push({
            name: found.name,
            place:
              [found.city, found.country].filter(Boolean).join(", ") || null,
            url: pageUrl ? siteOnlyPath(pageUrl) : null,
          });
        }
        const card: AssistantCard = { kind: "parks", parks: rows };
        return new ActionOutcome(
          { type: "card", card },
          { shown: rows.map((row) => row.name) },
        );
      }
      if (kind === "rides") {
        if (!park) throw new McpToolError("kind rides needs park");
        // Every operating ride with a posted wait, longest first, so both ends can be cut.
        const live = (await getParkTool.handler(
          { park, longestWaits: 30 },
          mcp(ctx),
        )) as ParkSummary;
        if ("ambiguous" in live) return live;
        const all = live.rides?.longestWaits ?? [];
        const picked = (order === "shortest" ? [...all].reverse() : all).slice(
          0,
          limit,
        );
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
          { shown: `${rides.length} rides of ${live.name}` },
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
        return new ActionOutcome({ type: "card", card }, { shown: card.name });
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
      };
      return new ActionOutcome({ type: "card", card }, { shown: card.name });
    },
  }),
  tool({
    name: "open_page",
    description:
      "Take the user to a park.fan page in their browser, only when they ask to go to, see or " +
      "open it. Give exactly one target: a park (id or slug), a ride (rideId), or a post or " +
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
      "Open a day at one park in the user's trip planner (it opens beside the page). Only when " +
      "the user asks to plan a visit. The plan lives in their browser.",
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
      "planner places them after what is already planned. Only when the user asks. Ride ids " +
      "come from find_rides or get_park_live and must belong to the park.",
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
      const rides = await parkRides(mcp(ctx), park.slug, rideIds);
      return new ActionOutcome(
        { type: "planner_add_rides", park, date, rides },
        { added: rides.map((ride) => ride.name), park: park.name, date },
      );
    },
  }),
  tool({
    name: "plan_add_block",
    status: "planner",
    description:
      "Add a block of the user's own to their trip planner: a break, a meal, a show they named, " +
      "shopping, a photo stop or a meeting point. Only when the user asks.",
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
      "shows the result and an undo. Only when the user asks.",
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
];

const BY_NAME = new Map(ASSISTANT_TOOLS.map((t) => [t.name, t]));

/** The tool list as the Messages API takes it, in a fixed order for caching. */
export const ASSISTANT_TOOL_PARAMS: Anthropic.Tool[] = ASSISTANT_TOOLS.map(
  (t) => ({
    name: t.name,
    description: t.description,
    input_schema: z.toJSONSchema(t.input, {
      io: "input",
    }) as Anthropic.Tool.InputSchema,
  }),
);

/** The status the widget shows for a tool, or null for an unknown name. */
export function toolStatus(name: string): AssistantToolStatus | null {
  return BY_NAME.get(name)?.status ?? null;
}

/** Results cut here: a tool that overflows costs tokens and says nothing more. */
const MAX_RESULT_CHARS = 12_000;

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
    const result = JSON.stringify(
      localizeTimes(
        outcome instanceof ActionOutcome ? outcome.result : outcome,
        ctx.timeZone ?? "UTC",
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
