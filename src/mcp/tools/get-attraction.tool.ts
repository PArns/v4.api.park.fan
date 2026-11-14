import { z } from "zod";
import { DataSource } from "typeorm";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { attractionPageUrl } from "./links";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AttractionIntegrationService } from "../../attractions/services/attraction-integration.service";
import { rideFacts } from "../../attractions/utils/ride-facts.util";
import { AnalyticsService } from "../../analytics/analytics.service";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";

/** Whether `day` lies inside a works window; an open end runs on. */
function inWindow(
  window: { from?: string | null; to?: string | null },
  day: string,
): boolean {
  return (
    (!window.from || window.from <= day) && (!window.to || window.to >= day)
  );
}

export const getAttractionTool = defineMcpTool({
  name: "get_attraction",
  title: "Attraction details",
  description:
    "One ride as park.fan shows it now: status and effective status, every queue (standby, single " +
    "rider, virtual queue …) with its wait, live crowd level and trend against its baseline, a " +
    "running outage, whether it has run today, works periods, the curated facts (minimum height, " +
    "gets you wet, single rider, indoor/outdoor, season months, land, the paid fast pass, " +
    "headliner), the rope-drop recommendation and today's best visit times where computed, " +
    "the ride profile where one is " +
    "curated (type, track figures in order, launches, inversions, manufacturer, model, opening " +
    "year, top speed, height, length, duration), typical waits, downtime reliability and, if " +
    "retired, when and why. Optionally the daily history of the last days. Get the id from " +
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
    const [dto, headliners] = await Promise.all([
      ctx
        .get(AttractionIntegrationService)
        .buildIntegratedResponse(attraction, 30, false),
      // The same set the park payload's `isHeadliner` reads.
      ctx
        .get(AnalyticsService)
        .getHeadlinerAttractionIds(attraction.parkId)
        .catch(() => null),
    ]);
    const ropeDrop = dto.ropeDrop;

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
      // The window's days are park-local, so "running today" is the park's today.
      worksPeriod: dto.worksPeriod
        ? {
            ...dto.worksPeriod,
            runningToday: inWindow(
              dto.worksPeriod,
              getCurrentDateInTimezone(attraction.park.timezone),
            ),
          }
        : null,
      // Every window, for "when is it closed next" — worksPeriod is just one.
      worksPeriods: dto.worksPeriods ?? [],
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
        // The park season the ride runs in, when one binds it: its days are the window.
        boundSeason: dto.boundSeason ?? null,
        land: dto.land ?? null,
        // Absent is "nobody checked or none sold", never "no fast pass".
        fastPass: dto.fastPass ?? null,
        headliner: headliners ? headliners.has(attraction.id) : null,
      },
      // Whether arriving at opening pays off; times are instants of the next operating day.
      ropeDrop: ropeDrop
        ? {
            worth: ropeDrop.worth,
            strength: ropeDrop.strength,
            confidence: ropeDrop.confidence,
            openWaitMinutes: ropeDrop.openWait,
            busyPeakMinutes: ropeDrop.busyPeak,
            savingsMinutes: ropeDrop.savings,
            rideBy: ropeDrop.rideByUtc,
            bestSlot: ropeDrop.bestSlotUtc,
            bestSlotWaitMinutes: ropeDrop.bestSlotWait,
            betterLateInTheDay: ropeDrop.endOfDayWorth,
          }
        : null,
      bestVisitTimesToday: dto.bestVisitTimes ?? null,
      profile: rideFacts(dto.rideProfile),
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
