import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { isAmbiguous, resolvePark } from "./park-ref";
import {
  DISPLAY_CONSISTENCY_DEFAULT_DAYS,
  DISPLAY_CONSISTENCY_MAX_DAYS,
  DisplayConsistencyService,
} from "../../monitoring/display-consistency/display-consistency.service";

export const displayConsistencyTool = defineMcpTool({
  name: "display_consistency",
  title: "Display consistency",
  description:
    "Whether the places that show a park-day agree: the crowd calendar (grid, day dialog, month " +
    "summary), /best-days, the trip planner's /plan/day, the trip-region calendar, the park " +
    "payload's and the park lists' live level, Parky's park card (todayExpected), his get_days and " +
    "get_park_day, and park.fan's own calendar and best-days relays as its edge serves them to " +
    "visitors. With a park it reads them all now for today and the next days, through the methods " +
    "the endpoints use and the relay URLs the park page asks, and lists per day each surface's " +
    "crowd level and headliner wait, then the mismatches: two surfaces that carry the same " +
    "statistic and show different values, with the statistic named and, for a relay, the age of " +
    "the edge copy (`copy`). `differences` are pairs that measure different things (live now " +
    "against the day's forecast, today so far, last year) and may differ. An unknown or absent " +
    "value is never a mismatch; a surface that could not be read (a relay's timeout, Cloudflare " +
    "challenge or error status) is under `failures`. Without a park: the last run (09:40 and " +
    "23:40 UTC) over the parks in a running season and the most read park pages, with each " +
    "flagged park's mismatches and failures.",
  input: {
    park: z
      .string()
      .min(2)
      .max(120)
      .optional()
      .describe(
        'Park id (uuid) from find_parks, or a slug like "phantasialand"; none for the last daily run',
      ),
    days: z
      .number()
      .int()
      .min(1)
      .max(DISPLAY_CONSISTENCY_MAX_DAYS)
      .default(DISPLAY_CONSISTENCY_DEFAULT_DAYS)
      .describe("Days from today, today included"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(30)
      .describe("Entries per list"),
  },
  async handler({ park: reference, days, limit }, ctx) {
    const service = ctx.get(DisplayConsistencyService);
    if (!reference) {
      const run = await service.latestRun();
      if (!run) {
        return { lastRun: null, note: "no run in the last two days" };
      }
      const { summary } = run;
      return {
        lastRun: {
          checkedAt: summary.checkedAt,
          days: summary.days,
          parksChecked: summary.parks.length,
          mismatches: summary.mismatches,
          failures: summary.failures,
          // A park whose check threw has no result of its own to list below.
          parksFailed: summary.parks
            .filter((park) => park.error)
            .map(({ id, name, error }) => ({ id, name, error })),
        },
        parks: capped(
          run.parks.map((result) => ({
            park: result.park,
            checkedAt: result.checkedAt,
            mismatches: result.mismatches.slice(0, limit),
            failures: result.failures,
          })),
          limit,
        ),
      };
    }
    const found = await resolvePark(ctx, reference);
    if (isAmbiguous(found)) return found;
    const result = await service.check(found, days);
    return {
      ...result,
      mismatches: capped(result.mismatches, limit),
      differences: capped(result.differences, limit),
    };
  },
});
