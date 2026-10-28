import { z } from "zod";
import { DataSource } from "typeorm";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { attractionPageUrl } from "./links";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AttractionIntegrationService } from "../../attractions/services/attraction-integration.service";

export const getAttractionTool = defineMcpTool({
  name: "get_attraction",
  title: "Attraction details",
  description:
    "One ride as park.fan shows it now: status and effective status, every queue (standby, single " +
    "rider, virtual queue …) with its wait, live crowd level and trend against its baseline, a " +
    "running outage, whether it has run today, works periods, the curated facts (minimum height, " +
    "gets you wet, single rider, indoor/outdoor, season months), typical waits, downtime reliability " +
    "and, if retired, when and why. Optionally the daily history of the last days. Get the id from " +
    "find_attractions or get_park.",
  input: {
    attractionId: uuidInput().describe(
      "Attraction id from find_attractions or get_park",
    ),
    historyDays: z
      .number()
      .int()
      .min(0)
      .max(30)
      .default(0)
      .describe("Days of daily history to include (0 for none)"),
  },
  async handler({ attractionId, historyDays }, ctx) {
    const attraction = await ctx
      .get(DataSource)
      .getRepository(Attraction)
      .findOne({
        where: { id: attractionId.toLowerCase() },
        relations: { park: { destination: true } },
      });
    if (!attraction?.park) {
      throw new McpToolError(
        `No attraction ${attractionId}; try find_attractions`,
      );
    }

    // The site's own 30-day variant, which is usually cached already.
    const dto = await ctx
      .get(AttractionIntegrationService)
      .buildIntegratedResponse(attraction, 30, false);

    return {
      id: dto.id,
      name: dto.name,
      park: dto.park ? { id: dto.park.id, name: dto.park.name } : null,
      pageUrl: attractionPageUrl(attraction.park, attraction),
      status: dto.status ?? null,
      effectiveStatus: dto.effectiveStatus ?? null,
      queues: (dto.queues ?? []).map((queue) => ({
        type: queue.queueType,
        status: queue.status,
        waitMinutes: queue.waitTime ?? null,
        state: queue.state ?? null,
      })),
      crowdLevel: dto.crowdLevel ?? null,
      trend: dto.trend ?? null,
      baselineMinutes: dto.baseline ?? null,
      outage: dto.outage ?? null,
      notRunToday: dto.notRunToday ?? null,
      worksPeriod: dto.worksPeriod ?? null,
      facts: {
        kind: dto.attractionKind ?? null,
        indoorOutdoor: dto.indoorOutdoor ?? null,
        minimumHeight: dto.minimumHeight ?? null,
        minimumHeightUnit: dto.minimumHeightUnit ?? null,
        maximumHeight: dto.maximumHeight ?? null,
        mayGetWet: dto.mayGetWet ?? null,
        hasSingleRider: dto.hasSingleRider ?? null,
        hasVirtualLine: dto.hasVirtualLine ?? null,
        isSeasonal: dto.isSeasonal ?? null,
        seasonMonths: dto.seasonMonths ?? null,
        isCurrentlyInSeason: dto.isCurrentlyInSeason ?? null,
      },
      statistics: dto.statistics ?? null,
      typicalWaits: dto.typicalWaits
        ? {
            weekday: dto.typicalWaits.weekday,
            weekend: dto.typicalWaits.weekend,
            peak: dto.typicalWaits.peak,
            windowDays: dto.typicalWaits.windowDays,
          }
        : null,
      downtime: dto.downtime ?? null,
      predictionAccuracy: dto.predictionAccuracy ?? null,
      retired: dto.retiredAt
        ? {
            at: dto.retiredAt,
            reason: dto.retiredReason ?? null,
            kind: dto.retiredKind ?? null,
          }
        : null,
      ...(historyDays > 0 && {
        history: (dto.history ?? []).slice(-historyDays),
      }),
    };
  },
});
