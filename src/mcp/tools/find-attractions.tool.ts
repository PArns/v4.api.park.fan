import { z } from "zod";
import { defineMcpTool, uuidInput } from "../mcp-tool";
import { SearchService } from "../../search/search.service";
import { SearchQueryDto } from "../../search/dto/search-query.dto";

export const findAttractionsTool = defineMcpTool({
  name: "find_attractions",
  title: "Find attractions",
  description:
    "Search rides and attractions by name (typo-tolerant). Returns each match's id, name, park " +
    "(id and name), live status and current standby wait in minutes. Optionally only rides of " +
    "one park (its id from find_parks). Pass the id to get_attraction for the details.",
  input: {
    query: z.string().min(2).max(100).describe('Ride name, e.g. "taron"'),
    parkId: uuidInput()
      .optional()
      .describe("Only rides of this park (id from find_parks)"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(20)
      .default(10)
      .describe("Most matches to return"),
  },
  async handler({ query, parkId: rawParkId, limit }, ctx) {
    const parkId = rawParkId?.toLowerCase();
    const result = await ctx.get(SearchService).search(
      Object.assign(new SearchQueryDto(), {
        q: query,
        type: ["attraction"],
        // The park filter runs after the search, so ask for the most it gives.
        limit: parkId ? 20 : limit,
      }),
    );
    const matches = result.results
      .filter((item) => item.type === "attraction")
      .filter((item) => !parkId || item.parentPark?.id === parkId)
      .slice(0, limit);
    return {
      query,
      total: parkId ? matches.length : result.counts.attraction.total,
      attractions: matches.map((ride) => ({
        id: ride.id,
        name: ride.name,
        park: ride.parentPark
          ? { id: ride.parentPark.id, name: ride.parentPark.name }
          : null,
        status: ride.status ?? null,
        waitMinutes: ride.waitTime ?? null,
      })),
    };
  },
});
