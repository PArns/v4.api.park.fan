import { z } from "zod";
import { defineMcpTool, McpToolError } from "../mcp-tool";
import { addIsoDays } from "../../common/utils/date.util";
import { utcDay } from "../../common/utils/reader-hash.util";
import { DAY_PATTERN } from "../../site-analytics/site-analytics.query";
import { TRIP_REGIONS } from "../../trip-regions/regions";
import {
  TRIP_METRIC_ROUTES,
  UNKNOWN_REGION,
} from "../../trip-regions/metrics/trip-request-facts";
import {
  TRIP_METRICS_RETENTION_DAYS,
  TripRequestMetricsService,
} from "../../trip-regions/metrics/trip-request-metrics.service";

const REGIONS = Object.keys(TRIP_REGIONS).concat(UNKNOWN_REGION) as [
  string,
  ...string[],
];

/** The window of UTC days the report reads, inside the retention. */
export function opsWindow(
  input: { days?: number; from?: string; to?: string },
  now = new Date(),
): { from: string; to: string } {
  const today = utcDay(now);
  const oldest = addIsoDays(today, -(TRIP_METRICS_RETENTION_DAYS - 1));
  if (input.from || input.to) {
    if (!input.from || !input.to || input.days !== undefined) {
      throw new McpToolError("Give from and to together, or days alone");
    }
    if (input.from > input.to) throw new McpToolError("from is after to");
    if (input.to > today) {
      throw new McpToolError(`to is after today (${today}, UTC)`);
    }
    if (input.from < oldest) {
      throw new McpToolError(
        `The rows go back ${TRIP_METRICS_RETENTION_DAYS} days: from is ${oldest} at the earliest`,
      );
    }
    return { from: input.from, to: input.to };
  }
  return { from: addIsoDays(today, -((input.days ?? 7) - 1)), to: today };
}

export const tripPlannerOpsTool = defineMcpTool({
  name: "trip_planner_ops",
  title: "Trip planner server metrics",
  description:
    "How the trip planner's API routes perform, from the server's own record of every answer of " +
    "/v1/trip-regions/:region/plan, /when (find dates) and /best-days: per region and route the " +
    "requests, errors and error rate; latency percentiles in ms (latencyMs over every 200, " +
    "builtLatencyMs over the 200s that were built rather than served from the answer's Redis " +
    "cache: the optimizer's and the calendar's time); the cache (hit, joined: waited for the same " +
    "build in flight, miss) with hitRate, and the region calendar under the built answers; " +
    "exactShare (the search was proven, not stopped by its time budget); relaxedShare and " +
    "unplacedShare (a rule loosened, a visit left out); hopper and owned-ticket shares; average " +
    "trip length and parks. Then the relaxation codes (blocks_relaxed, streak_relaxed, " +
    "ticket_window_relaxed) with their share, the unplaced reasons (no_open_day, no_day_left, " +
    "no_ticket_day, no_base; /when names none: unknown) by visits and requests, the errors by " +
    "status and reason code (unknown_park, visits_exceed_days, invalid_query, bad_tickets, " +
    "unknown_region …), owned tickets by kind (pass, evening, reserved_day, one_day, " +
    "multi_day), and the built answers' latency by shape: slowest (trip days × parks × bases, " +
    "slowest p90 first) and byLength, byParks, byBases. Optional series per UTC day and route. " +
    "Counted per UTC hour over UTC days, " +
    `kept ${TRIP_METRICS_RETENTION_DAYS} days; each API process writes its rows once a minute, so ` +
    "the last minute is missing (writer says what this process holds and dropped). Not recorded: " +
    "requests the data-key guard refused, answers Cloudflare served from its cache, Parky's " +
    "plan_vacation and find_trip_dates. Every value is the server's own measurement or code: no " +
    "visitor data, no dates, no names.",
  input: {
    days: z
      .number()
      .int()
      .min(1)
      .max(TRIP_METRICS_RETENTION_DAYS)
      .optional()
      .describe(
        "UTC days back from today, today included; 7 when neither days nor from/to is given",
      ),
    from: z
      .string()
      .regex(DAY_PATTERN)
      .optional()
      .describe("First UTC day (YYYY-MM-DD) of a fixed window, with to"),
    to: z
      .string()
      .regex(DAY_PATTERN)
      .optional()
      .describe("Last UTC day (YYYY-MM-DD), with from; today at the latest"),
    region: z
      .enum(REGIONS)
      .optional()
      .describe(
        `Only this region's requests; ${UNKNOWN_REGION} is a slug that names no region`,
      ),
    route: z
      .enum(TRIP_METRIC_ROUTES)
      .optional()
      .describe("Only this route: plan, when (find dates) or best_days"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(10)
      .describe("Rows of slowest"),
    series: z
      .boolean()
      .default(false)
      .describe(
        "Include requests, errors, hit rate and built p90 per UTC day and route",
      ),
  },
  async handler({ days, from, to, region, route, limit, series }, ctx) {
    const window = opsWindow({ days, from, to });
    return ctx.get(TripRequestMetricsService).ops({
      ...window,
      region,
      route,
      limit,
      series,
    });
  },
});
