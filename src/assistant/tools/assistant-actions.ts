import { DataSource, In } from "typeorm";
import { z } from "zod";
import { McpToolError, type McpToolContext } from "../../mcp/mcp-tool";
import { isAmbiguous, resolvePark } from "../../mcp/tools/park-ref";
import { getFrontendUrl } from "../../config/oauth.config";
import { Attraction } from "../../attractions/entities/attraction.entity";
import type { Park } from "../../parks/entities/park.entity";

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
    }
  | {
      kind: "rides";
      /** The park the list belongs to, its page the card's link. */
      park: string;
      order: "longest" | "shortest";
      rides: Array<{ name: string; waitMinutes: number; url: string | null }>;
      url: string | null;
    }
  | {
      kind: "parks";
      parks: Array<{ name: string; place: string | null; url: string | null }>;
    };

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
    };

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
