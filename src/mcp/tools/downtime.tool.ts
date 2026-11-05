import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { DowntimeMeasurementService } from "../../admin/downtime-measurement.service";

const PARK_SLUG = /^[a-z0-9](?:[a-z0-9-]{0,98}[a-z0-9])?$/;

export const downtimeTool = defineMcpTool({
  name: "downtime",
  title: "Ride outages",
  description:
    "Ride outages counted as events, reconstructed from the wait-time history of the given parks: " +
    "per park the rides with outages, outage count, share of single-reading runs and the regime " +
    '("reports" = the feed reports outages, "artefact" = hourly-only feed); per ride the outages, ' +
    "how many are still ongoing, median and longest minutes. For the outages happening right now " +
    "use get_park. Park slugs as in park.fan URLs; a slug can name two parks (disneyland-park), " +
    "the result tells them apart by city.",
  input: {
    parks: z
      .array(z.string().regex(PARK_SLUG))
      .min(1)
      .max(5)
      .describe('Park slugs, e.g. ["phantasialand", "europa-park"]'),
    days: z
      .number()
      .int()
      .min(1)
      .max(90)
      .default(30)
      .describe("Days back from now"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(20)
      .describe("Rides to list, most outages first"),
  },
  async handler({ parks, days, limit }, ctx) {
    const measurement = await ctx
      .get(DowntimeMeasurementService)
      .measure({ parkSlugs: parks, days });
    const rides = capped(measurement.rides, limit);
    return {
      windowDays: measurement.windowDays,
      totals: measurement.totals,
      parks: measurement.parks,
      rides: {
        total: rides.total,
        items: rides.items.map((ride) => ({
          attractionId: ride.attractionId,
          name: ride.attractionName,
          park: ride.parkName,
          city: ride.parkCity,
          outages: ride.outages,
          ongoing: ride.ongoing,
          medianMinutes: ride.medianMinutes,
          longestMinutes: ride.longestMinutes,
        })),
      },
    };
  },
});
