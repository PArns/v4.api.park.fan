import { z } from "zod";
import { uuidInput } from "../mcp-tool";
import {
  PARK_SEASON_KINDS,
  PARK_SEASON_STATUSES,
  type ParkSeason,
} from "../../parks/entities/park-season.entity";
import {
  SEASON_DATE_RE,
  SEASON_TIME_RE,
  type SeasonInput,
} from "../../parks/services/park-season.service";
import { SEASON_LIMITS } from "../../admin/content/dto/curation.dto";
import { TRIP_REGIONS } from "../../trip-regions/regions";
import type { AdminSeasonView } from "../../admin/content/admin-season.service";

/**
 * The season fields the season tools share, bounded as the admin's
 * `SeasonWriteDto` bounds them; `ParkSeasonService` checks the rest (a real
 * date, the end after the start, dates inside the range, a price with its
 * currency, http(s) links, attractions of the park).
 */

/** Every trip region's slug, for a `region` input. */
export const TRIP_REGION_SLUGS = Object.keys(TRIP_REGIONS) as [
  string,
  ...string[],
];

const KIND =
  "halloween, christmas (a Christmas event or party), winter (a winter season, Christmas or " +
  "not: Wintertraum, Winter Efteling), summer_nights (late summer openings), special_event (anything else " +
  "with a name), opening (the park's own operating season), closure (the park shut), " +
  "maintenance (named rides or an area down: attractionIds)";
const STATUS =
  "confirmed (the park published it), announced, expected (it ran last year, nothing published " +
  "yet), cancelled";

const date = () => z.string().regex(SEASON_DATE_RE, "must be YYYY-MM-DD");
const time = () =>
  z.string().regex(SEASON_TIME_RE, "must be HH:MM, 00:00–23:59");

/** The fields of a season, each optional; `null` clears one. update_trip_season takes these. */
export const seasonPatchFields = {
  kind: z.enum(PARK_SEASON_KINDS).optional().describe(KIND),
  name: z
    .string()
    .max(SEASON_LIMITS.name)
    .nullable()
    .optional()
    .describe(
      "The event's own name as the park prints it, untranslated (\"Halloween Horror Nights\"); null: the site's label for the kind",
    ),
  startDate: date().optional().describe("First day, park-local YYYY-MM-DD"),
  endDate: date()
    .optional()
    .describe("Last day, inclusive; the startDate for a one-day event"),
  dates: z
    .array(date())
    .max(SEASON_LIMITS.dates)
    .nullable()
    .optional()
    .describe(
      "The single days inside the range it runs, when not every day (weekends plus some dates); null or []: every day",
    ),
  status: z.enum(PARK_SEASON_STATUSES).optional().describe(STATUS),
  separateTicket: z
    .boolean()
    .optional()
    .describe("Needs more than a normal day ticket"),
  priceFrom: z
    .number()
    .min(0)
    .max(100_000)
    .nullable()
    .optional()
    .describe("Cheapest advertised price, with priceCurrency"),
  priceCurrency: z
    .string()
    .max(3)
    .nullable()
    .optional()
    .describe("ISO 4217, e.g. EUR, USD, JPY"),
  opensAt: time()
    .nullable()
    .optional()
    .describe("Park-local opening time on its days, HH:MM"),
  closesAt: time()
    .nullable()
    .optional()
    .describe(
      "Park-local closing time, HH:MM; midnight is 00:00 (read as the next day)",
    ),
  attractionIds: z
    .array(uuidInput())
    .max(100)
    .nullable()
    .optional()
    .describe(
      "Rides of this park the season is about (a maintenance window, a re-themed ride); find_attractions has the ids",
    ),
  url: z
    .string()
    .max(SEASON_LIMITS.url)
    .nullable()
    .optional()
    .describe("The park's own page for the event, http(s)"),
  sourceUrl: z
    .string()
    .max(SEASON_LIMITS.url)
    .nullable()
    .optional()
    .describe("Where the dates were read, http(s)"),
  confirmedAt: z
    .string()
    .max(40)
    .nullable()
    .optional()
    .describe(
      "When the dates were last checked against the source, ISO 8601 (2026-08-20T09:00:00Z)",
    ),
  note: z
    .string()
    .max(SEASON_LIMITS.note)
    .nullable()
    .optional()
    .describe("Anything a later reader needs that the fields cannot hold"),
};

/** create_trip_season's fields: the kind and the range are required. */
export const seasonCreateFields = {
  ...seasonPatchFields,
  kind: z.enum(PARK_SEASON_KINDS).describe(KIND),
  startDate: date().describe("First day, park-local YYYY-MM-DD"),
  endDate: date().describe(
    "Last day, inclusive; the startDate for a one-day event. Not announced yet: leave the season out",
  ),
};

type PatchInput = {
  [K in keyof typeof seasonPatchFields]?: z.infer<
    (typeof seasonPatchFields)[K]
  >;
};

/** The service's input from a tool's: given fields only, ids lowercased. */
export function seasonInputOf(input: PatchInput): Partial<SeasonInput> {
  const out: Partial<SeasonInput> = {};
  for (const key of Object.keys(seasonPatchFields) as Array<keyof PatchInput>) {
    if (input[key] !== undefined) {
      (out as Record<string, unknown>)[key] = input[key];
    }
  }
  if (input.attractionIds) {
    out.attractionIds = input.attractionIds.map((id) => id.toLowerCase());
  }
  return out;
}

/** A season as the season tools answer it; the park by its curated name where the list gave it. */
export function seasonView(
  season: Omit<ParkSeason, "park">,
  park?: AdminSeasonView["park"],
) {
  return {
    id: season.id,
    park: park ?? { id: season.parkId },
    kind: season.kind,
    name: season.name,
    startDate: season.startDate,
    endDate: season.endDate,
    dates: season.dates,
    status: season.status,
    separateTicket: season.separateTicket,
    priceFrom: season.priceFrom === null ? null : Number(season.priceFrom),
    priceCurrency: season.priceCurrency,
    opensAt: season.opensAt,
    closesAt: season.closesAt,
    attractionIds: season.attractionIds,
    url: season.url,
    sourceUrl: season.sourceUrl,
    confirmedAt: season.confirmedAt,
    note: season.note,
    updatedAt: season.updatedAt,
  };
}

/**
 * A season on one day of a tool's calendar: what it is, and how settled where
 * that is not `confirmed`, since an `expected` date is no promise.
 */
export function seasonDayView(
  season: Pick<ParkSeason, "kind" | "name" | "status" | "separateTicket">,
) {
  return {
    kind: season.kind,
    name: season.name,
    status: season.status === "confirmed" ? undefined : season.status,
    separateTicket: season.separateTicket || undefined,
  };
}
