import { z } from "zod";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { asToolRefusal, dryRunInput, runAdminRoute } from "../admin-route";
import { AdminController } from "../../admin/admin.controller";
import { AdminCurationService } from "../../admin/content/admin-curation.service";

/**
 * The admin's merges, one pair at a time, with the preview first. A merge
 * deletes the losing row inside one transaction and has no undo, so both
 * default to the dry run (docs/architecture/attraction-status/duplicates.md).
 */

export const mergeAttractionsTool = defineMcpTool({
  name: "merge_attractions",
  title: "Merge two ride rows",
  description:
    "Merges two rows of one ride in one park, as the admin's duplicates page does: the winner " +
    "keeps its history, takes the base slug and what it was missing, the loser's slug becomes " +
    "a redirect and the loser is deleted. No undo. The dry run (default) answers the slug that " +
    "survives, the one that redirects, the columns inherited and the hand-written values the " +
    "merge would destroy. Prove a merge first: the old id no longer listed by the feed, the two " +
    "within one building, the operator's page naming the ride once; distance alone is not " +
    "proof. For a ride the feed re-issued under a new id (duplicates, data_quality re-issue " +
    "candidates) the retired row wins with adoptLoserExternalId. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    winnerId: uuidInput().describe("The row that stays"),
    loserId: uuidInput().describe("The row that is deleted"),
    adoptLoserExternalId: z
      .boolean()
      .default(false)
      .describe(
        "The winner takes the loser's upstream id, the one the feed lists now (a re-issue)",
      ),
    dryRun: dryRunInput(
      "the merge preview: surviving and redirected slug, inherited columns, curations lost",
    ),
  },
  async handler({ winnerId, loserId, adoptLoserExternalId, dryRun }, ctx) {
    const winner = winnerId.toLowerCase();
    const loser = loserId.toLowerCase();
    if (winner === loser) {
      throw new McpToolError("winnerId and loserId are the same ride");
    }
    // The merge service refuses both with an Error the model would not see.
    const curation = ctx.get(AdminCurationService);
    const [kept, removed] = await asToolRefusal(() =>
      Promise.all([
        curation.findAttraction(winner),
        curation.findAttraction(loser),
      ]),
    );
    if (kept.parkId !== removed.parkId) {
      throw new McpToolError(
        `${kept.name} and ${removed.name} are in different parks; merge the parks or neither`,
      );
    }
    const body = {
      winnerId: winner,
      loserId: loser,
      adoptLoserExternalId,
      dryRun,
    };
    const result = await runAdminRoute(
      ctx,
      { method: "POST", path: "merge-duplicate-attractions", body },
      () => ctx.get(AdminController).mergeDuplicateAttractions(body),
    );
    return { dryRun, winner: kept.name, loser: removed.name, result };
  },
});

export const mergeParksTool = defineMcpTool({
  name: "merge_parks",
  title: "Merge two parks",
  description:
    "Merges two park rows of one place, as the admin's duplicates page does: the winner is " +
    "chosen by the merge's own rule (wiki id, more source ids, more rides, older row), whatever " +
    "order the ids come in; rides, schedules and mappings move to it and the other park is " +
    "deleted. No undo. The dry run (default) names the winner and the loser. duplicates lists " +
    "the candidate pairs. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    park1Id: uuidInput().describe("One park of the pair"),
    park2Id: uuidInput().describe("The other park"),
    dryRun: dryRunInput(
      "which park would stay and which would be merged into it",
    ),
  },
  async handler({ park1Id, park2Id, dryRun }, ctx) {
    const body = {
      park1Id: park1Id.toLowerCase(),
      park2Id: park2Id.toLowerCase(),
      dryRun,
    };
    if (body.park1Id === body.park2Id) {
      throw new McpToolError("park1Id and park2Id are the same park");
    }
    return runAdminRoute(
      ctx,
      { method: "POST", path: "merge-duplicate-parks", body },
      () => ctx.get(AdminController).mergeDuplicateParks(body),
    );
  },
});
