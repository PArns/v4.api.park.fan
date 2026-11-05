import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SearchService } from "../../search/search.service";
import { SearchQueryDto } from "../../search/dto/search-query.dto";

export const findParksTool = defineMcpTool({
  name: "find_parks",
  title: "Find parks",
  description:
    "Search theme parks by name, city or country (typo-tolerant, the site's own search). Returns " +
    "for each match its id, name, city, country, live status (OPERATING/CLOSED), live crowd level " +
    "and today's opening hours (ISO timestamps). Pass the id to get_park for rides, waits and outages.",
  input: {
    query: z
      .string()
      .min(2)
      .max(100)
      .describe(
        'Park name, city or country, e.g. "phantasialand" or "orlando"',
      ),
    limit: z
      .number()
      .int()
      .min(1)
      .max(20)
      .default(5)
      .describe("Most matches to return"),
  },
  async handler({ query, limit }, ctx) {
    const result = await ctx.get(SearchService).search(
      Object.assign(new SearchQueryDto(), {
        q: query,
        type: ["park"],
        limit,
      }),
    );
    return {
      query,
      total: result.counts.park.total,
      parks: result.results
        .filter((item) => item.type === "park")
        .map((park) => ({
          id: park.id,
          name: park.name,
          slug: park.slug,
          city: park.city ?? null,
          country: park.country ?? null,
          status: park.status ?? null,
          crowdLevel: park.load ?? null,
          hoursToday: park.parkHours ?? null,
        })),
    };
  },
});
