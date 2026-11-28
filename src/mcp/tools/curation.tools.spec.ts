import { NotFoundException } from "@nestjs/common";
import { getQueueToken } from "@nestjs/bull";
import { z } from "zod";
import { AdminContentController } from "../../admin/content/admin-content.controller";
import { AdminCurationService } from "../../admin/content/admin-curation.service";
import { AdminRideProfileService } from "../../admin/content/admin-ride-profile.service";
import { AdminController } from "../../admin/admin.controller";
import { McpToolError } from "../mcp-tool";
import {
  correctParkLocationTool,
  curateParkAttractionsTool,
  curateParkTool,
  curationFieldsTool,
  deleteRideProfileTool,
  refreshParkTool,
} from "./curation.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const PARK = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA";
const park = PARK.toLowerCase();

describe("curation tools", () => {
  it("curate_park runs the route's handler with the DTO and the token's account", async () => {
    const patchPark = jest.fn(async () => ({
      changed: ["curatedName"],
      auditId: "audit-1",
      fields: [
        {
          key: "curatedName",
          label: "Name",
          type: "text",
          group: "Name",
          syncedValue: "Taron Park",
          curatedValue: "Taron",
          resolvedValue: "Taron",
          overridden: true,
          humanOnly: false,
          defaultValue: null,
        },
        {
          key: "curatedCity",
          label: "City",
          type: "text",
          group: "Ort",
          syncedValue: "Brühl",
          curatedValue: null,
          resolvedValue: "Brühl",
          overridden: false,
          humanOnly: false,
          defaultValue: null,
        },
      ],
    }));
    const { ctx, audit } = adminContext("editor", [
      [AdminContentController, { patchPark }],
    ]);
    const answer = await curateParkTool.handler(
      {
        parkId: PARK,
        fields: { curatedName: "Taron" },
        reason: "The operator's page",
      },
      ctx,
    );
    expect(patchPark).toHaveBeenCalledWith(
      park,
      { fields: { curatedName: "Taron" }, reason: "The operator's page" },
      ctx.admin,
    );
    // The handler writes its own audit row; the tool adds none.
    expect(audit).toEqual([]);
    expect(answer).toEqual({
      changed: ["curatedName"],
      auditId: "audit-1",
      fields: [
        expect.objectContaining({ key: "curatedName", curated: "Taron" }),
      ],
    });
  });

  it("curate_park is refused below editor, and its input wants a reason", async () => {
    const result = await callAs(curateParkTool, "viewer", {
      parkId: PARK,
      fields: { curatedName: "Taron" },
      reason: "x",
    });
    expect(result.isError).toBe(true);
    const input = z.object(curateParkTool.input);
    expect(
      input.safeParse({ parkId: PARK, fields: { curatedName: "T" } }).success,
    ).toBe(false);
    expect(
      input.safeParse({ parkId: PARK, fields: {}, reason: "why" }).success,
    ).toBe(false);
  });

  describe("curate_park_attractions", () => {
    const RIDE_A = "BBBBBBBB-BBBB-4BBB-8BBB-BBBBBBBBBBBB";
    const RIDE_B = "CCCCCCCC-CCCC-4CCC-8CCC-CCCCCCCCCCCC";
    const rows: Record<string, object> = {
      [RIDE_A.toLowerCase()]: {
        name: "Hall of Presidents",
        curatedName: null,
        attractionKind: null,
        derivedAttractionKind: "SHOW",
      },
      [RIDE_B.toLowerCase()]: {
        name: "Astro Orbiter",
        curatedName: null,
        attractionKind: null,
        derivedAttractionKind: null,
      },
    };
    const curation = {
      findAttraction: async (id: string) => {
        if (!rows[id]) throw new NotFoundException(`No attraction ${id}`);
        return { id, ...rows[id] };
      },
    };
    const call = (input: Record<string, unknown>) => {
      const patchParkAttractions = jest.fn(async () => ({ changed: [] }));
      const { ctx } = adminContext("editor", [
        [AdminCurationService, curation],
        [AdminContentController, { patchParkAttractions }],
      ]);
      const parsed = z.object(curateParkAttractionsTool.input).parse({
        parkId: PARK,
        reason: "Data quality",
        ...input,
      });
      return {
        patchParkAttractions,
        answer: curateParkAttractionsTool.handler(parsed, ctx),
      };
    };

    it("sets a kind, GAME included, and clears one with null", async () => {
      const { answer, patchParkAttractions } = call({
        entries: [
          { id: RIDE_A, fields: { attractionKind: "GAME" } },
          { id: RIDE_B, fields: { attractionKind: null } },
        ],
      });
      await answer;
      expect(patchParkAttractions).toHaveBeenCalledWith(
        park,
        expect.objectContaining({
          entries: [
            { id: RIDE_A.toLowerCase(), fields: { attractionKind: "GAME" } },
            { id: RIDE_B.toLowerCase(), fields: { attractionKind: null } },
          ],
        }),
        expect.anything(),
      );
    });

    it("makes a ride's derived kind the editor's beside the named entries", async () => {
      const { answer, patchParkAttractions } = call({
        entries: [{ id: RIDE_B, fields: { attractionKind: "RIDE" } }],
        adoptDerivedKind: [RIDE_A, RIDE_A],
      });
      await answer;
      expect(patchParkAttractions).toHaveBeenCalledWith(
        park,
        expect.objectContaining({
          entries: [
            { id: RIDE_B.toLowerCase(), fields: { attractionKind: "RIDE" } },
            { id: RIDE_A.toLowerCase(), fields: { attractionKind: "SHOW" } },
          ],
        }),
        expect.anything(),
      );
    });

    it("refuses a ride with no derived kind, one sent twice, and an empty call", async () => {
      const none = call({ adoptDerivedKind: [RIDE_B] });
      await expect(none.answer).rejects.toEqual(
        new McpToolError(
          '"Astro Orbiter" has no derived kind to adopt; name its kind in entries',
        ),
      );
      expect(none.patchParkAttractions).not.toHaveBeenCalled();
      await expect(
        call({
          entries: [{ id: RIDE_A, fields: { attractionKind: "SHOW" } }],
          adoptDerivedKind: [RIDE_A],
        }).answer,
      ).rejects.toBeInstanceOf(McpToolError);
      await expect(call({}).answer).rejects.toEqual(
        new McpToolError("Send entries, adoptDerivedKind or both"),
      );
    });

    it("leaves an editor's kind alone", async () => {
      const curated = {
        findAttraction: async (id: string) => ({
          id,
          name: "Monorail",
          curatedName: null,
          attractionKind: "TRANSPORT",
          derivedAttractionKind: "RIDE",
        }),
      };
      const { ctx } = adminContext("editor", [
        [AdminCurationService, curated],
        [AdminContentController, { patchParkAttractions: jest.fn() }],
      ]);
      await expect(
        curateParkAttractionsTool.handler(
          {
            parkId: PARK,
            entries: [],
            adoptDerivedKind: [RIDE_A],
            reason: "x",
          },
          ctx,
        ),
      ).rejects.toEqual(
        new McpToolError(
          '"Monorail" already has an editor\'s kind (TRANSPORT); leave it out',
        ),
      );
    });
  });

  it("curation_fields names a missing row instead of failing", async () => {
    const { ctx } = adminContext("viewer", [
      [
        AdminCurationService,
        {
          findPark: async () => {
            throw new NotFoundException(`No park ${park}`);
          },
        },
      ],
    ]);
    await expect(
      curationFieldsTool.handler(
        { kind: "park", id: PARK, curatedOnly: false },
        ctx,
      ),
    ).rejects.toEqual(new McpToolError(`No park ${park}`));
  });

  it("delete_ride_profile shows the profile and removes nothing on a dry run", async () => {
    const deleteRideProfile = jest.fn();
    const { ctx, audit } = adminContext("editor", [
      [
        AdminRideProfileService,
        { find: async () => ({ elements: ["lift-hill"], types: ["coaster"] }) },
      ],
      [AdminContentController, { deleteRideProfile }],
    ]);
    const answer = await deleteRideProfileTool.handler(
      { attractionId: PARK, reason: "Wrong ride", dryRun: true },
      ctx,
    );
    expect(answer).toMatchObject({
      dryRun: true,
      wouldRemove: { elements: ["lift-hill"] },
    });
    expect(deleteRideProfile).not.toHaveBeenCalled();
    expect(audit).toEqual([]);

    await deleteRideProfileTool.handler(
      { attractionId: PARK, reason: "Wrong ride", dryRun: false },
      ctx,
    );
    expect(deleteRideProfile).toHaveBeenCalledWith(
      park,
      ctx.admin,
      expect.objectContaining({ reason: "Wrong ride" }),
    );
  });

  it("correct_park_location is owner only and audited as the route", async () => {
    expect(
      (await callAs(correctParkLocationTool, "editor", { parkId: PARK }))
        .isError,
    ).toBe(true);
    const correctParkLocation = jest.fn(async () => ({ pathChanged: true }));
    const { ctx, audit } = adminContext("owner", [
      [AdminController, { correctParkLocation }],
      [AdminCurationService, { findPark: async () => ({ id: park }) }],
    ]);
    await correctParkLocationTool.handler({ parkId: PARK, city: "Brühl" }, ctx);
    expect(correctParkLocation.mock.calls).toEqual([[park, { city: "Brühl" }]]);
    expect(audit).toEqual([
      expect.objectContaining({
        action: `job.parks/${park}/correct-location`,
        after: { city: "Brühl" },
      }),
    ]);
    await expect(
      correctParkLocationTool.handler({ parkId: PARK }, ctx),
    ).rejects.toBeInstanceOf(McpToolError);
  });

  it("refresh_park queues the schedule sync, publishes and records it", async () => {
    const order: string[] = [];
    const add = jest.fn(async () => order.push("queue"));
    const curation = {
      findPark: async () => ({ id: park, name: "Phantasialand" }),
      attractionIdsOf: async () => ["r1"],
      publish: jest.fn(async () => order.push("publish")),
    };
    const { ctx, audit } = adminContext("editor", [
      [AdminCurationService, curation],
      [getQueueToken("park-metadata"), { add }],
    ]);
    const answer = await refreshParkTool.handler(
      { parkId: PARK, schedule: true, publish: true },
      ctx,
    );
    expect(add).toHaveBeenCalledWith(
      "sync-park-schedule",
      { parkId: park },
      { removeOnComplete: true },
    );
    expect(curation.publish).toHaveBeenCalledWith(park, ["r1"]);
    expect(order).toEqual(["queue", "publish"]);
    expect(audit).toEqual([
      expect.objectContaining({ action: "park.refresh", entityId: park }),
    ]);
    expect(answer).toMatchObject({ scheduleQueued: true, published: true });
    await expect(
      refreshParkTool.handler(
        { parkId: PARK, schedule: false, publish: false },
        ctx,
      ),
    ).rejects.toBeInstanceOf(McpToolError);
  });
});
