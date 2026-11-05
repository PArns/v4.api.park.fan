import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { AttractionReviewService } from "../../attractions/services/attraction-review.service";

export const retirementCandidatesTool = defineMcpTool({
  name: "retirement_candidates",
  title: "Retirement candidates",
  description:
    "Rides whose source went quiet and that may have closed for good: park, ride name and id, " +
    "the day it fell silent and the longest wait it ever had. Rides an admin already checked and " +
    "marked as not a case are left out. A candidate is a question, not a verdict.",
  input: {
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(25)
      .describe("Most candidates to list"),
  },
  async handler({ limit }, ctx) {
    const candidates = await ctx
      .get(AttractionReviewService)
      .findRetirementCandidates();
    return capped(candidates, limit);
  },
});
