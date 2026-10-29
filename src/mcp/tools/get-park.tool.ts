import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { isAmbiguous, resolvePark } from "./park-ref";
import { parkPageUrl } from "./links";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { Park } from "../../parks/entities/park.entity";
import { formatInTimeZone } from "date-fns-tz";
import type {
  ParkAttractionDto,
  ParkWithAttractionsDto,
} from "../../parks/dto/park-with-attractions.dto";

function rideStatus(ride: ParkAttractionDto): string {
  return ride.effectiveStatus ?? ride.status ?? "UNKNOWN";
}

function standbyWait(ride: ParkAttractionDto): number | null {
  const standby = ride.queues?.find((queue) => queue.queueType === "STANDBY");
  return standby?.waitTime ?? null;
}

function summarize(dto: ParkWithAttractionsDto, park: Park, topWaits: number) {
  const rides = dto.attractions ?? [];
  const byStatus: Record<string, number> = {};
  for (const ride of rides) {
    const status = rideStatus(ride);
    byStatus[status] = (byStatus[status] ?? 0) + 1;
  }
  const statistics = dto.analytics?.statistics;

  return {
    id: dto.id,
    name: dto.name,
    city: dto.city,
    country: dto.country,
    timezone: dto.timezone,
    localTime: formatInTimeZone(
      new Date(),
      dto.timezone,
      "yyyy-MM-dd'T'HH:mmXXX",
    ),
    pageUrl: parkPageUrl(park),
    status: dto.status ?? null,
    retiredAt: dto.retiredAt ?? null,
    liveWaitTimes: dto.liveWaitTimes,
    scheduleToday: (dto.schedule ?? []).map((entry) => ({
      date: entry.date,
      type: entry.scheduleType,
      opens: entry.openingTime,
      closes: entry.closingTime,
    })),
    nextSchedule: dto.nextSchedule ?? null,
    crowd: statistics
      ? {
          level: statistics.crowdLevel,
          avgWaitMinutes: statistics.avgWaitTime,
          avgWaitTodayMinutes: statistics.avgWaitToday,
          peakWaitTodayMinutes: statistics.peakWaitToday,
          peakHourLocal: statistics.peakHourLocal,
          occupancyPercent: dto.analytics?.occupancy?.current ?? null,
          comparedToTypicalPercent:
            dto.analytics?.occupancy?.comparedToTypical ?? null,
        }
      : null,
    weatherNow: dto.weather?.now ?? null,
    weatherWarnings: (dto.weather?.warnings ?? []).length,
    rides: {
      total: rides.length,
      byStatus,
      longestWaits: rides
        .filter((ride) => rideStatus(ride) === "OPERATING")
        .map((ride) => ({ ride, wait: standbyWait(ride) }))
        .filter(
          (entry): entry is { ride: ParkAttractionDto; wait: number } =>
            typeof entry.wait === "number",
        )
        .sort((a, b) => b.wait - a.wait)
        .slice(0, topWaits)
        .map(({ ride, wait }) => ({
          id: ride.id,
          name: ride.name,
          waitMinutes: wait,
          crowdLevel: ride.crowdLevel ?? null,
          trend: ride.trend ?? null,
        })),
      down: rides
        .filter((ride) => rideStatus(ride) === "DOWN")
        .map((ride) => ({
          id: ride.id,
          name: ride.name,
          // `startObserved: false` means it began before this timestamp.
          downSince: ride.outage?.startedAt ?? null,
          startObserved: ride.outage?.startObserved ?? null,
          // `closed_gap` is inferred, not reported by the park.
          signal: ride.outage?.signal ?? null,
          operatingMinutesDown: ride.outage?.estimate?.elapsedMinutes ?? null,
        })),
      notRunToday: rides
        .filter((ride) => ride.notRunToday)
        .map((ride) => ({ id: ride.id, name: ride.name })),
    },
    shows: (dto.shows ?? []).length,
    restaurants: (dto.restaurants ?? []).length,
  };
}

export const getParkTool = defineMcpTool({
  name: "get_park",
  title: "Park status",
  description:
    "Live state of one park, as park.fan shows it now: open or closed, today's schedule in the " +
    "park's timezone, crowd level and average/peak waits, current weather, ride counts by status, " +
    "the longest standby waits, rides that are down (since when, and whether the park reported it " +
    "or park.fan inferred it from a gap: signal `down` vs `closed_gap`) and rides that have not run " +
    "today. Pass the park id from find_parks, or its slug when that is unambiguous.",
  input: {
    park: z
      .string()
      .min(2)
      .max(120)
      .describe(
        'Park id (uuid) from find_parks, or a slug like "phantasialand"',
      ),
    longestWaits: z
      .number()
      .int()
      .min(0)
      .max(30)
      .default(10)
      .describe("How many of the longest waits to list"),
  },
  async handler({ park: reference, longestWaits }, ctx) {
    const found = await resolvePark(ctx, reference);
    if (isAmbiguous(found)) return found;
    const park = found;
    const parks = ctx.get(ParksService);

    await parks.loadParkRelations(park);
    const dto = await ctx
      .get(ParkIntegrationService)
      .buildIntegratedResponse(park, false, false);
    return summarize(dto, park, longestWaits);
  },
});
