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
    "name). Each list is cut to `limit` with its full size in `total`.",
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
    };
  },
});
