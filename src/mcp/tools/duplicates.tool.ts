import { z } from "zod";
import { capped, defineMcpTool } from "../mcp-tool";
import { AttractionMergeService } from "../../attractions/services/attraction-merge.service";
import { ParkValidatorService } from "../../parks/services/park-validator.service";

export const duplicatesTool = defineMcpTool({
  name: "duplicates",
  title: "Duplicate candidates",
  description:
    "Rows that look like the same thing twice. `attractions`: base/suffix slug pairs inside one " +
    'park ("taron" and "taron-2") with the row a merge would keep and whether the pair is safe to ' +
    "merge automatically. `parks`: parks that look like the same place, with score, reason, the " +
    "winner a merge would keep and why a human has to review it. Read-only: merging happens in " +
    "the admin UI.",
  input: {
    kind: z
      .enum(["attractions", "parks", "both"])
      .default("both")
      .describe("Which duplicates to list"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(15)
      .describe("Pairs per list"),
  },
  async handler({ kind, limit }, ctx) {
    const [attractions, parks] = await Promise.all([
      kind === "parks"
        ? null
        : ctx.get(AttractionMergeService).findDuplicatePairs(),
      kind === "attractions"
        ? null
        : ctx.get(ParkValidatorService).findDuplicatesWithWinners(),
    ]);
    const summarize = <T extends { safe: boolean }>(pairs: T[]) => ({
      safe: pairs.filter((pair) => pair.safe).length,
      needsReview: pairs.filter((pair) => !pair.safe).length,
      ...capped(pairs, limit),
    });
    return {
      ...(attractions && { attractions: summarize(attractions) }),
      ...(parks && { parks: summarize(parks) }),
    };
  },
});
