import { z } from "zod";
import { AdminController } from "../../admin/admin.controller";
import { AdminCurationService } from "../../admin/content/admin-curation.service";
import { McpToolError } from "../mcp-tool";
import {
  retireAttractionsTool,
  retireParkTool,
  reviewMarkTool,
  unretireAttractionTool,
} from "./retirement.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const RIDE = (n: number) =>
  `aaaaaaaa-aaaa-4aaa-8aaa-${String(n).padStart(12, "0")}`;
const PARK = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

function ride(n: number, overrides: Record<string, unknown> = {}) {
  return {
    id: RIDE(n),
    name: `Ride ${n}`,
    parkId: PARK,
    park: { name: "Movie Park", timezone: "Europe/Berlin" },
    retiredAt: null,
    isSeasonal: false,
    curatedIsSeasonal: null,
    lastListedAt: null,
    absentSince: null,
    ...overrides,
  };
}

const entry = (n: number) => ({
  attractionId: RIDE(n),
  retiredAt: "2026-10-31",
  reason: "Closed for good, https://example.com/news",
});

describe("retirement tools", () => {
  it("retire_attractions counts what it would retire and writes nothing on a dry run", async () => {
    const retireAttractions = jest.fn();
    const rides = new Map([
      [RIDE(1), ride(1)],
      [RIDE(2), ride(2, { curatedIsSeasonal: true })],
    ]);
    const { ctx, audit } = adminContext("owner", [
      [
        AdminCurationService,
        {
          findAttraction: async (id: string) => {
            const row = rides.get(id);
            if (!row) throw new Error("not found");
            return row;
          },
        },
      ],
      [AdminController, { retireAttractions }],
    ]);
    const answer = (await retireAttractionsTool.handler(
      {
        retirements: [entry(1), entry(2), entry(3)],
        dryRun: true,
      },
      ctx,
    )) as { counts: Record<string, number>; warning?: string };
    expect(answer.counts).toEqual({
      asked: 3,
      notFound: 1,
      alreadyRetired: 0,
      seasonKnown: 1,
    });
    expect(answer.warning).toBeUndefined();
    expect(retireAttractions).not.toHaveBeenCalled();
    expect(audit).toEqual([]);
  });

  it("retire_attractions warns about a mass write before it is one", async () => {
    const { ctx } = adminContext("owner", [
      [
        AdminCurationService,
        {
          findAttraction: async (id: string) =>
            ride(Number(id.slice(-2)), { id }),
        },
      ],
    ]);
    const answer = (await retireAttractionsTool.handler(
      {
        retirements: Array.from({ length: 11 }, (_, i) => entry(i + 1)),
        dryRun: true,
      },
      ctx,
    )) as { warning?: string };
    expect(answer.warning).toMatch(/mass write/);
  });

  it("retire_attractions writes through the route and is audited as it", async () => {
    const retireAttractions = jest.fn(async () => ({ retired: 1 }));
    const { ctx, audit } = adminContext("owner", [
      [AdminController, { retireAttractions }],
    ]);
    await retireAttractionsTool.handler(
      { retirements: [entry(1)], dryRun: false },
      ctx,
    );
    expect(retireAttractions).toHaveBeenCalledWith({
      retirements: [entry(1)],
    });
    expect(audit).toEqual([
      expect.objectContaining({ action: "job.retire-attractions" }),
    ]);
  });

  it("retire_attractions refuses an editor, a ride listed twice and a reason without substance", async () => {
    expect(
      (
        await callAs(retireAttractionsTool, "editor", {
          retirements: [entry(1)],
        })
      ).isError,
    ).toBe(true);
    const { ctx } = adminContext("owner", []);
    await expect(
      retireAttractionsTool.handler(
        { retirements: [entry(1), entry(1)], dryRun: true },
        ctx,
      ),
    ).rejects.toBeInstanceOf(McpToolError);
    expect(
      z
        .object(retireAttractionsTool.input)
        .safeParse({ retirements: [{ ...entry(1), reason: "gone" }] }).success,
    ).toBe(false);
  });

  it("retire_park names the park and its rides on a dry run", async () => {
    const retireParks = jest.fn();
    const { ctx } = adminContext("owner", [
      [
        AdminCurationService,
        {
          findPark: async () => ({
            id: PARK,
            name: "Movie Park",
            timezone: "Europe/Berlin",
            retiredAt: null,
          }),
          attractionIdsOf: async () => [RIDE(1), RIDE(2)],
        },
      ],
      [AdminController, { retireParks }],
    ]);
    const answer = await retireParkTool.handler(
      {
        parkId: PARK,
        retiredAt: "2026-10-31",
        reason: "Closed for good, https://example.com",
        dryRun: true,
      },
      ctx,
    );
    expect(answer).toMatchObject({ name: "Movie Park", activeRides: 2 });
    expect(retireParks).not.toHaveBeenCalled();
  });

  it("unretire_attraction says so when the ride was not retired", async () => {
    const { ctx } = adminContext("owner", [
      [
        AdminController,
        { unretireAttraction: async () => ({ restored: false }) },
      ],
    ]);
    await expect(
      unretireAttractionTool.handler({ attractionId: RIDE(1) }, ctx),
    ).rejects.toBeInstanceOf(McpToolError);
  });

  it("review_mark sends the admin's mark and keeps the pair to not_a_duplicate", async () => {
    const recordReviewMarks = jest.fn(async () => ({ recorded: 1 }));
    const { ctx, audit } = adminContext("editor", [
      [AdminController, { recordReviewMarks }],
    ]);
    await reviewMarkTool.handler(
      {
        kind: "not_retired",
        attractionId: RIDE(1).toUpperCase(),
        reason: "Runs on weekends, park site",
        recheckAfter: "2027-03-01",
      },
      ctx,
    );
    expect(recordReviewMarks).toHaveBeenCalledWith({
      marks: [
        {
          kind: "not_retired",
          attractionId: RIDE(1),
          otherAttractionId: null,
          reason: "Runs on weekends, park site",
          recheckAfter: "2027-03-01",
        },
      ],
    });
    expect(audit).toEqual([
      expect.objectContaining({ action: "job.review-marks" }),
    ]);
    await expect(
      reviewMarkTool.handler(
        {
          kind: "not_a_duplicate",
          attractionId: RIDE(1),
          reason: "Two different rides",
        },
        ctx,
      ),
    ).rejects.toBeInstanceOf(McpToolError);
    expect((await callAs(reviewMarkTool, "author", {})).isError).toBe(true);
  });
});
