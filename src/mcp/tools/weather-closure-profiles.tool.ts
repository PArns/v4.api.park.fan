import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { WeatherClosureService } from "../../parks/services/weather-closure.service";

export const weatherClosureProfilesTool = defineMcpTool({
  name: "weather_closure_profiles",
  title: "Rides shut by cold or snow",
  description:
    "Which rides the weekly job has learned that cold or snow shuts, best probability first: " +
    "per ride the park, for cold the temperature it usually runs from, the share of cold (or " +
    "snowy) open hours it was shut, its ordinary share in mild weather, the conservative " +
    "probability that a closure under the condition is the weather's, and on how many days " +
    "that rests. `publishable` rides are the ones the public page names (probability at least " +
    "0.9 and shut in at least 15 % of the condition's hours); `totals.near` counts the rest at " +
    "0.75 or more. For whether a ride is shut by the weather right now use get_attraction.",
  input: {
    park: z
      .string()
      .max(100)
      .optional()
      .describe("Only this park, by slug, e.g. europa-park"),
    minProbability: z
      .number()
      .min(0)
      .max(1)
      .default(0.5)
      .describe("Leave out rides whose best probability is below this"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(30)
      .describe("Rides to list"),
  },
  async handler({ park, minProbability, limit }, ctx) {
    const overview = await ctx
      .get(WeatherClosureService)
      .listProfiles(minProbability);
    const rides = capped(
      overview.rides.filter((r) => !park || r.parkSlug === park),
      limit,
    );
    const brief = (p: (typeof overview.rides)[number]["cold"]) =>
      p && {
        usuallyRunsFromC: p.thresholdC,
        closedSharePercent: Math.round(p.closedShare * 100),
        mildClosedSharePercent: Math.round(p.baselineShare * 1000) / 10,
        probability: Math.round(p.attributableShare * 100) / 100,
        days: p.days,
        publishable: p.publishable,
      };
    return {
      bar: overview.bar,
      generatedAt: overview.generatedAt,
      totals: overview.totals,
      rides: {
        total: rides.total,
        items: rides.items.map((r) => ({
          attractionId: r.attractionId,
          name: r.attractionName,
          park: r.parkName,
          parkSlug: r.parkSlug,
          publishable: r.publishable,
          cold: brief(r.cold),
          snow: brief(r.snow),
        })),
      },
    };
  },
});
