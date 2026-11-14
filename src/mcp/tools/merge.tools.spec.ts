import { NotFoundException } from "@nestjs/common";
import { AdminController } from "../../admin/admin.controller";
import { AdminCurationService } from "../../admin/content/admin-curation.service";
import { McpToolError } from "../mcp-tool";
import { mergeAttractionsTool, mergeParksTool } from "./merge.tools";
import { adminContext, callAs, resultText } from "./__fixtures__/admin-context";

const WINNER = "aaaaaaaa-aaaa-4aaa-8aaa-000000000001";
const LOSER = "aaaaaaaa-aaaa-4aaa-8aaa-000000000002";

function rides(parkOfLoser = "p1") {
  const rows: Record<string, { id: string; name: string; parkId: string }> = {
    [WINNER]: { id: WINNER, name: "Taron", parkId: "p1" },
    [LOSER]: { id: LOSER, name: "Taron 2", parkId: parkOfLoser },
  };
  return {
    findAttraction: async (id: string) => {
      if (!rows[id]) throw new NotFoundException(`No attraction ${id}`);
      return rows[id];
    },
  };
}

describe("merge tools", () => {
  it("merge_attractions previews by default and merges only on dryRun false", async () => {
    const mergeDuplicateAttractions = jest.fn(
      async (body: { dryRun: boolean }) =>
        body.dryRun
          ? { dryRun: true, survivingSlug: "taron" }
          : { renamed: false },
    );
    const { ctx, audit } = adminContext("owner", [
      [AdminCurationService, rides()],
      [AdminController, { mergeDuplicateAttractions }],
    ]);
    const preview = await mergeAttractionsTool.handler(
      {
        winnerId: WINNER,
        loserId: LOSER,
        adoptLoserExternalId: true,
        dryRun: true,
      },
      ctx,
    );
    expect(preview).toMatchObject({
      dryRun: true,
      winner: "Taron",
      result: { survivingSlug: "taron" },
    });
    await mergeAttractionsTool.handler(
      {
        winnerId: WINNER,
        loserId: LOSER,
        adoptLoserExternalId: true,
        dryRun: false,
      },
      ctx,
    );
    expect(mergeDuplicateAttractions.mock.calls.map(([body]) => body)).toEqual([
      {
        winnerId: WINNER,
        loserId: LOSER,
        adoptLoserExternalId: true,
        dryRun: true,
      },
      {
        winnerId: WINNER,
        loserId: LOSER,
        adoptLoserExternalId: true,
        dryRun: false,
      },
    ]);
    // The admin's preview is a POST too, and the interceptor records it.
    expect(audit.map((row) => row.action)).toEqual([
      "job.merge-duplicate-attractions",
      "job.merge-duplicate-attractions",
    ]);
  });

  it("merge_attractions refuses one ride twice, rides of two parks and a missing ride", async () => {
    const mergeDuplicateAttractions = jest.fn();
    const twoParks = adminContext("owner", [
      [AdminCurationService, rides("p2")],
      [AdminController, { mergeDuplicateAttractions }],
    ]).ctx;
    const input = { adoptLoserExternalId: false, dryRun: true };
    await expect(
      mergeAttractionsTool.handler(
        { ...input, winnerId: WINNER, loserId: WINNER.toUpperCase() },
        twoParks,
      ),
    ).rejects.toBeInstanceOf(McpToolError);
    await expect(
      mergeAttractionsTool.handler(
        { ...input, winnerId: WINNER, loserId: LOSER },
        twoParks,
      ),
    ).rejects.toThrow(/different parks/);
    await expect(
      mergeAttractionsTool.handler(
        {
          ...input,
          winnerId: WINNER,
          loserId: "aaaaaaaa-aaaa-4aaa-8aaa-000000000009",
        },
        twoParks,
      ),
    ).rejects.toThrow(/No attraction/);
    expect(mergeDuplicateAttractions).not.toHaveBeenCalled();
  });

  it("merge_parks hands the route an explicit dry run, which a manual pair would not default to", async () => {
    const mergeDuplicateParks = jest.fn(async () => ({ merged: 0 }));
    const { ctx } = adminContext("owner", [
      [AdminController, { mergeDuplicateParks }],
    ]);
    await mergeParksTool.handler(
      { park1Id: WINNER, park2Id: LOSER, dryRun: true },
      ctx,
    );
    expect(mergeDuplicateParks).toHaveBeenCalledWith({
      park1Id: WINNER,
      park2Id: LOSER,
      dryRun: true,
    });
  });

  it("is refused to an editor's write token, and names admin:owner", async () => {
    for (const tool of [mergeAttractionsTool, mergeParksTool]) {
      const result = await callAs(tool, "editor", {});
      expect(result.isError).toBe(true);
      expect(resultText(result)).toMatch(
        /needs a token with the admin:owner scope; this one has admin:write/,
      );
    }
  });
});
