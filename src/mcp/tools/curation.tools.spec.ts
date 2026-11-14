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
