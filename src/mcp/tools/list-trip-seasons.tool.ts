import { z } from "zod";
import { defineMcpTool, uuidInput } from "../mcp-tool";
import { asToolRefusal } from "../mcp-write";
import {
  PARK_SEASON_KINDS,
  PARK_SEASON_STATUSES,
} from "../../parks/entities/park-season.entity";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import { seasonView, TRIP_REGION_SLUGS } from "./trip-season.inputs";

export const listTripSeasonsTool = defineMcpTool({
  name: "list_trip_seasons",
  title: "List park seasons",
  description:
    "The park seasons and events on file, the rows the admin's Seasons page lists: Halloween and " +
    "Christmas events, summer nights, special events, a park's operating season, closures and " +
    "maintenance windows, newest start first. Each with its id, park, kind, name, range, the " +
    "single dates when not every day, status, whether it needs its own ticket, price, hours, " +
    "links, source and note. Filter by year (seasons overlapping it), kind, status, park id or " +
    "trip region. The ids are what update_trip_season and delete_trip_season take.",
  input: {
    year: z
      .number()
      .int()
      .min(2000)
      .max(2100)
      .optional()
      .describe("Seasons overlapping this year"),
    kind: z.enum(PARK_SEASON_KINDS).optional().describe("Only this kind"),
    status: z
      .enum(PARK_SEASON_STATUSES)
      .optional()
      .describe("Only this status"),
    park: uuidInput()
      .optional()
      .describe("Only this park's; find_parks has the ids"),
    region: z
      .enum(TRIP_REGION_SLUGS)
      .optional()
      .describe("Only the parks of this trip region"),
    current: z
      .boolean()
      .default(false)
      .describe(
        "Only seasons running today (UTC day across parks, the park's own with park)",
      ),
    limit: z
      .number()
      .int()
      .min(1)
      .max(200)
      .default(50)
      .describe("Seasons to return"),
    offset: z
      .number()
      .int()
      .min(0)
      .max(10_000)
      .default(0)
      .describe("Seasons to skip"),
  },
  async handler(input, ctx) {
    const { total, seasons } = await asToolRefusal(() =>
      ctx.get(AdminSeasonService).list({
        year: input.year,
        kind: input.kind,
        status: input.status,
        parkId: input.park?.toLowerCase(),
        region: input.region,
        current: input.current,
        limit: input.limit,
        offset: input.offset,
      }),
    );
    return {
      total,
      offset: input.offset,
      seasons: seasons.map((season) => seasonView(season, season.park)),
    };
  },
});
