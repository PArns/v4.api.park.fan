import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { asToolRefusal, dryRunInput } from "../mcp-write";
import {
  AdminSeasonService,
  MAX_SEASON_IMPORT_ROWS,
  SEASON_IMPORTS_PER_HOUR,
} from "../../admin/content/admin-season.service";
import { SEASON_LIMITS } from "../../admin/content/dto/curation.dto";
import { PARK_SEASON_KINDS } from "../../parks/entities/park-season.entity";

/**
 * A row of `scripts/trip-regions/seasons/*.json`. Kinds, statuses, dates and
 * times are plain bounded strings: a wrong one is reported on its row, as the
 * script did, rather than refusing the file.
 */
const row = z.object({
  region: z
    .string()
    .max(60)
    .nullable()
    .optional()
    .describe(
      "The trip region whose park the row means; needed where two regions list the slug (disneyland-park: southern-california or paris)",
    ),
  parkSlug: z.string().min(1).max(100).describe("The park's slug"),
  kind: z
    .string()
    .max(40)
    .describe(`One of ${PARK_SEASON_KINDS.join(", ")}`),
  name: z
    .string()
    .max(SEASON_LIMITS.name)
    .nullable()
    .optional()
    .describe("The event's own name, untranslated"),
  startDate: z
    .string()
    .max(10)
    .nullable()
    .describe("YYYY-MM-DD; a row without one is reported and skipped"),
  endDate: z
    .string()
    .max(10)
    .nullable()
    .optional()
    .describe("YYYY-MM-DD; a row without one is reported and skipped"),
  dates: z
    .array(z.string().max(10))
    .max(SEASON_LIMITS.dates)
    .nullable()
    .optional()
    .describe("The single days it runs, when not every day of the range"),
  status: z
    .string()
    .max(20)
    .nullable()
    .optional()
    .describe("confirmed, announced (the default), expected or cancelled"),
  separateTicket: z
    .boolean()
    .nullable()
    .optional()
    .describe("Needs more than a day ticket"),
  priceFrom: z
    .union([z.number().min(0).max(100_000), z.string().max(12)])
    .nullable()
    .optional()
    .describe(
      "Cheapest price, with priceCurrency; a number or its decimal string",
    ),
  priceCurrency: z.string().max(3).nullable().optional().describe("ISO 4217"),
  opensAt: z.string().max(5).nullable().optional().describe("HH:MM"),
  closesAt: z
    .string()
    .max(5)
    .nullable()
    .optional()
    .describe("HH:MM; midnight is 00:00"),
  url: z
    .string()
    .max(SEASON_LIMITS.url)
    .nullable()
    .optional()
    .describe("The park's page for the event"),
  sourceUrl: z
    .string()
    .max(SEASON_LIMITS.url)
    .nullable()
    .optional()
    .describe("Where the dates were read"),
  notes: z
    .string()
    .max(SEASON_LIMITS.note)
    .nullable()
    .optional()
    .describe("Stored as the season's note"),
});

export const importTripSeasonsTool = defineMcpTool({
  name: "import_trip_seasons",
  title: "Import park seasons",
  description:
    "Imports researched park seasons and events, the rows of scripts/trip-regions/seasons/*.json " +
    "passed as they are (read the file, send its array as rows). Each row is checked as the " +
    "admin's form checks a season, its park found (by the trip region's park list, else the one " +
    "park with that slug), and compared with what is on file: a season of the same park, kind and " +
    "name whose dates overlap is skipped as already there. A dry run (the default) writes nothing " +
    "and answers per row create, exists or invalid with the reason; with dryRun false each new row " +
    "is created with its own audit row under this account (outcome created, with its id, or " +
    "failed) and the parks are published once. A row without an end date is reported and waits. " +
    `At most ${MAX_SEASON_IMPORT_ROWS} rows a call and ${SEASON_IMPORTS_PER_HOUR} imports an hour; ` +
    "one writing import runs at a time. Show the person the dry run before writing.",
  minRole: "editor",
  writes: "add",
  input: {
    rows: z
      .array(row)
      .min(1)
      .max(MAX_SEASON_IMPORT_ROWS)
      .describe("The seasons, in the file's shape"),
    region: z
      .string()
      .max(60)
      .optional()
      .describe("A trip region for every row that names none, e.g. japan"),
    dryRun: dryRunInput("what it would create, skip and refuse, per row"),
  },
  async handler({ rows, region, dryRun }, ctx) {
    return asToolRefusal(() =>
      ctx.get(AdminSeasonService).import(
        rows.map((r) => ({ ...r, region: r.region ?? region ?? null })),
        { dryRun },
        ctx.admin,
      ),
    );
  },
});
