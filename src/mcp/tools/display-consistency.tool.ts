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
    "get_park_day, and his day tip's stamp (now and as served from its cache). With a park it " +
    "reads them all now for today and the next days, through the methods the endpoints use, and " +
    "lists per day each surface's crowd level, headliner wait and stamp, then the mismatches: two " +
    "surfaces that carry the same statistic and show different values, with the statistic named. " +
    "`differences` are pairs that measure different things (live now against the day's forecast, " +
    "today so far, last year) and may differ. An unknown or absent value is never a mismatch. " +
    "Without a park: the last daily run (09:40 UTC) over the parks in a running season and the " +
    "most read park pages, with each flagged park's mismatches.",
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
      const summary = await service.latestSummary();
      if (!summary) {
        return { lastRun: null, note: "no daily run in the last two days" };
      }
      const flagged = summary.parks.filter(
        (park) => park.mismatches > 0 || park.failures > 0,
      );
      const details = await Promise.all(
        flagged.map((park) => service.latestForPark(park.id)),
      );
      return {
        lastRun: {
          checkedAt: summary.checkedAt,
          days: summary.days,
          parksChecked: summary.parks.length,
          mismatches: summary.mismatches,
        },
        parks: capped(
          details
            .filter((result) => result !== null)
            .map((result) => ({
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
