import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { TRIP_REGIONS } from "../../trip-regions/regions";
import {
  TRIP_METRIC_CALLERS,
  TRIP_METRIC_ROUTES,
  UNKNOWN_REGION,
} from "../../trip-regions/metrics/trip-request-facts";
import {
  TRIP_METRICS_RETENTION_DAYS,
  TripRequestMetricsService,
} from "../../trip-regions/metrics/trip-request-metrics.service";
import { utcDaysInput, utcDaysWindow } from "./utc-window";

const REGIONS = Object.keys(TRIP_REGIONS).concat(UNKNOWN_REGION) as [
  string,
  ...string[],
];

/** The rows reach back as far as they are kept; a week by default. */
const LIMITS = { maxDays: TRIP_METRICS_RETENTION_DAYS, defaultDays: 7 };

/** The window of UTC days the report reads, inside the retention. */
export function opsWindow(
  input: { days?: number; from?: string; to?: string },
  now = new Date(),
): { from: string; to: string } {
  return utcDaysWindow(input, LIMITS, now);
}

export const tripPlannerOpsTool = defineMcpTool({
  name: "trip_planner_ops",
  title: "Trip planner server metrics",
  description:
    "How the trip planner's API routes perform, from the server's own record of every answer of " +
    "/v1/trip-regions/:region/plan, /when (find dates) and /best-days, and of Parky's trip tools " +
    "calling the same services (caller: site or parky): per region, route and caller the " +
    "requests, errors and error rate; latency percentiles in ms (latencyMs over every 200, " +
    "builtLatencyMs over the 200s that were built rather than served from the answer's Redis " +
    "cache: the optimizer's and the calendar's time); the cache (hit, joined: waited for the same " +
    "build in flight, miss) with hitRate, and the region calendar under the built answers; " +
    "exactShare (the search was proven, not stopped by its time budget); relaxedShare and " +
    "unplacedShare (a rule loosened, a visit left out); leftOutShare (a chosen park with no visit " +
    "at all, the planner's parks left out); hopper and owned-ticket shares; in a region with " +
    "bases staySuggestedShare (the answer suggested other stays); average trip length and parks. " +
    "Then the relaxation codes (blocks_relaxed, streak_relaxed, ticket_window_relaxed) with " +
    "their share, the unplaced reasons (no_open_day, no_day_left, no_ticket_day, no_base; /when " +
    "names none: unknown) by visits and requests, the errors by status and reason code " +
    "(unknown_park, visits_exceed_days, invalid_query, bad_tickets, bad_bases, unknown_region …), " +
    "owned tickets by kind (pass, evening, reserved_day, one_day, multi_day), in a region with " +
    "bases the visitor's move buffer (30, 60, 90, 120) and the ends they fixed (none, start, end, " +
    "both), and the built answers' latency by shape: slowest (trip days × parks × bases, slowest " +
    "p90 first) and byLength, byParks, byBases. Optional series per UTC day and route. Counted " +
    `per UTC hour over UTC days, kept ${TRIP_METRICS_RETENTION_DAYS} days; each API process ` +
    "writes its rows once a minute, so the last minute is missing (writer says what this process " +
    "holds and dropped). Not recorded: requests the data-key guard refused, answers Cloudflare " +
    "served from its cache, and the plan of the trip on screen Parky reads for every question. " +
    "Every value is the server's own measurement or code: no visitor data, no dates, no names.",
  input: {
    ...utcDaysInput(LIMITS),
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
    caller: z
      .enum(TRIP_METRIC_CALLERS)
      .optional()
      .describe(
        "Only the route's own requests (site) or Parky's trip tools (parky)",
      ),
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
  async handler({ days, from, to, region, route, caller, limit, series }, ctx) {
    const window = opsWindow({ days, from, to });
    return ctx.get(TripRequestMetricsService).ops({
      ...window,
      region,
      route,
      caller,
      limit,
      series,
    });
  },
});
