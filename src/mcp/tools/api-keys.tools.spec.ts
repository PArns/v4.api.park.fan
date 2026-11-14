import { NotFoundException } from "@nestjs/common";
import { AdminApiKeysController } from "../../api-keys/admin-api-keys.controller";
import { CreateApiKeyDto } from "../../api-keys/dto/create-api-key.dto";
import { McpToolError } from "../mcp-tool";
import {
  apiKeysTool,
  createApiKeyTool,
  deleteApiKeyTool,
  revokeApiKeyTool,
  updateApiKeyTool,
} from "./api-keys.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const ACTIVE = "cccccccc-cccc-4ccc-8ccc-000000000001";
const REVOKED = "cccccccc-cccc-4ccc-8ccc-000000000002";

function item(id: string, revokedAt: Date | null) {
  return {
    id,
    name: id === ACTIVE ? "Coaster club" : "Old app",
    usage: "non-commercial",
    prefix: "pfk_abcd",
    contact: null,
    note: null,
    requestsPerHour: 1000,
    allowedParkIds: null,
    expiresAt: null,
    createdAt: new Date("2026-01-01T00:00:00Z"),
    lastUsedAt: null,
    revokedAt,
    rotatedAt: null,
    previousExpiresAt: null,
    previousLastUsedAt: null,
    requestsThisHour: 3,
    stats: {
      days: [
        { date: "2026-11-13", requests: 10, limited: 1 },
        { date: "2026-11-14", requests: 5, limited: 0 },
      ],
      months: [{ month: "2026-11", requests: 15 }],
    },
  };
}

function controller() {
  return {
    list: async () => ({
      items: [item(ACTIVE, null), item(REVOKED, new Date("2026-11-01"))],
    }),
    create: jest.fn(async () => ({
      key: "pfk_secret",
      item: item(ACTIVE, null),
    })),
    update: jest.fn(async () => {
      throw new NotFoundException("No such active key");
    }),
    revoke: jest.fn(async () => undefined),
    remove: jest.fn(async () => undefined),
  };
}

describe("API key tools", () => {
  it("api_keys lists without secrets and sums the days", async () => {
    const { ctx } = adminContext("owner", [
      [AdminApiKeysController, controller()],
    ]);
    const answer = (await apiKeysTool.handler(
      { status: "active", withDays: false, limit: 50 },
      ctx,
    )) as { total: number; items: Array<Record<string, unknown>> };
    expect(answer.total).toBe(1);
    expect(answer.items[0]).toMatchObject({
      id: ACTIVE,
      requests30d: 15,
      limited30d: 1,
    });
    expect(answer.items[0]).not.toHaveProperty("days");
  });

  it("create_api_key checks the body as the route does and hands back the key once", async () => {
    const keys = controller();
    const { ctx, audit } = adminContext("owner", [
      [AdminApiKeysController, keys],
    ]);
    const answer = await createApiKeyTool.handler(
      { name: "Coaster club", usage: "non-commercial" },
      ctx,
    );
    expect(keys.create).toHaveBeenCalledWith(
      ctx.admin,
      expect.any(CreateApiKeyDto),
    );
    expect(answer).toMatchObject({ key: "pfk_secret" });
    // The controller writes `api-key.create` itself.
    expect(audit).toEqual([]);
  });

  it("update_api_key hands the route's 404 to the model", async () => {
    const { ctx } = adminContext("owner", [
      [AdminApiKeysController, controller()],
    ]);
    await expect(
      updateApiKeyTool.handler({ id: REVOKED, requestsPerHour: 10 }, ctx),
    ).rejects.toEqual(new McpToolError("No such active key"));
    await expect(updateApiKeyTool.handler({ id: ACTIVE }, ctx)).rejects.toThrow(
      /Name requestsPerHour/,
    );
  });

  it("revoke_api_key shows the key first and revokes on dryRun false", async () => {
    const keys = controller();
    const { ctx } = adminContext("owner", [[AdminApiKeysController, keys]]);
    const preview = await revokeApiKeyTool.handler(
      { id: ACTIVE, secret: "key", dryRun: true },
      ctx,
    );
    expect(preview).toMatchObject({
      dryRun: true,
      item: { name: "Coaster club" },
    });
    expect(keys.revoke).not.toHaveBeenCalled();
    await revokeApiKeyTool.handler(
      { id: ACTIVE, secret: "key", dryRun: false },
      ctx,
    );
    expect(keys.revoke).toHaveBeenCalledWith(ctx.admin, ACTIVE);
  });

  it("delete_api_key refuses a key that is not revoked", async () => {
    const keys = controller();
    const { ctx } = adminContext("owner", [[AdminApiKeysController, keys]]);
    await expect(
      deleteApiKeyTool.handler({ id: ACTIVE, dryRun: false }, ctx),
    ).rejects.toThrow(/not revoked/);
    await deleteApiKeyTool.handler({ id: REVOKED, dryRun: false }, ctx);
    expect(keys.remove).toHaveBeenCalledWith(ctx.admin, REVOKED);
  });

  it("is owner only, like the page", async () => {
    for (const tool of [apiKeysTool, createApiKeyTool, deleteApiKeyTool]) {
      expect((await callAs(tool, "editor", {})).isError).toBe(true);
    }
  });
});
