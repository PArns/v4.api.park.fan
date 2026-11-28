import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { DataQualityMonitorService } from "../../monitoring/data-quality-monitor.service";

export const dataQualityTool = defineMcpTool({
  name: "data_quality",
  title: "Data-quality findings",
  description:
    "The silent-failure detectors behind the admin's data-quality screen: clusters of rides in " +
    "one park that all fell silent on the same day (a dropped feed or a seasonal closure — a " +
    "human decides), jobs that keep failing, rides retired for absence that nobody gave a season, " +
    "and retired rides with a younger live ride right beside them (possibly re-issued under a new " +
    "name), parks with live rides that carry no kind (neither an editor's nor a derived one; " +
    "each ride with id, name and slug, for curate_park_attractions to give it one), " +
    "and how many mismatches and unreadable surfaces the last display-consistency run " +
    "found per park (display_consistency has them). Each list is cut to `limit` with its full " +
    "size in `total`.",
  input: {
    windowDays: z
      .number()
      .int()
      .min(1)
      .max(400)
      .default(14)
      .describe("How far back the silent-cluster detector looks"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(10)
      .describe("Entries per list"),
  },
  async handler({ windowDays, limit }, ctx) {
    const report = await ctx.get(DataQualityMonitorService).report(windowDays);
    return {
      windowDays: report.windowDays,
      silencedClusters: capped(report.silencedClusters, limit),
      failingJobs: capped(report.failingJobs, limit),
      absenceRetiredUnreviewed: capped(report.absenceRetiredUnreviewed, limit),
      reissueCandidates: capped(report.reissueCandidates, limit),
      // The refs carry the names too, so the bare name list stays out.
      attractionsWithoutKind: capped(
        report.attractionsWithoutKind.map(
          ({ parkId, parkName, attractions, withoutKindRefs }) => ({
            parkId,
            parkName,
            attractions,
            withoutKindRefs,
          }),
        ),
        limit,
      ),
      // The last display-consistency run; display_consistency has the details.
      displayConsistency: report.displayConsistency && {
        checkedAt: report.displayConsistency.checkedAt,
        parksChecked: report.displayConsistency.parks.length,
        mismatches: report.displayConsistency.mismatches,
        failures: report.displayConsistency.failures,
        parksFlagged: capped(
          report.displayConsistency.parks.filter(
            (park) => park.mismatches > 0 || park.failures > 0,
          ),
          limit,
        ),
      },
    };
  },
});
