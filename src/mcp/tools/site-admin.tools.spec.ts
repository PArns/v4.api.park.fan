import { z } from "zod";
import { AdminAssistantController } from "../../assistant/admin-assistant.controller";
import { AssistantRecordsService } from "../../assistant/assistant-records.service";
import { AdminSiteAnalyticsController } from "../../site-analytics/site-analytics.controller";
import { AdminAuthService } from "../../admin/auth/admin-auth.service";
import { McpToolError } from "../mcp-tool";
import {
  adminUsersTool,
  blockEventTool,
  deleteAssistantConversationTool,
  setFeedbackStatusTool,
} from "./site-admin.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";

describe("site admin tools", () => {
  it("set_feedback_status runs the admin route and is audited as it", async () => {
    const setFeedbackStatus = jest.fn(async () => ({ ok: true }));
    const { ctx, audit } = adminContext("editor", [
      [AdminAssistantController, { setFeedbackStatus }],
    ]);
    await setFeedbackStatusTool.handler({ id: ID, status: "done" }, ctx);
    expect(setFeedbackStatus).toHaveBeenCalledWith(ID, { status: "done" });
    expect(audit).toEqual([
      expect.objectContaining({
        action: `job.assistant/feedback/${ID}`,
        after: { status: "done" },
      }),
    ]);
    expect(
      z
        .object(setFeedbackStatusTool.input)
        .safeParse({ id: ID, status: "read" }).success,
    ).toBe(false);
    expect((await callAs(setFeedbackStatusTool, "viewer", {})).isError).toBe(
      true,
    );
  });

  it("delete_assistant_conversation shows the chat first and deletes on dryRun false", async () => {
    const deleteConversation = jest.fn(async () => undefined);
    const chat = {
      id: ID,
      createdAt: new Date("2026-11-14T10:00:00Z"),
      locale: "de",
      messageCount: 4,
      totalTokens: 900,
      costUsd: "0.01",
      rating: null,
      promptVersion: null,
      endedAt: null,
    };
    const { ctx, audit } = adminContext("editor", [
      [AssistantRecordsService, { conversation: async () => chat }],
      [AdminAssistantController, { deleteConversation }],
    ]);
    expect(
      await deleteAssistantConversationTool.handler(
        { id: ID, dryRun: true },
        ctx,
      ),
    ).toMatchObject({ dryRun: true, chat: { id: ID, messages: 4 } });
    expect(deleteConversation).not.toHaveBeenCalled();
    await deleteAssistantConversationTool.handler(
      { id: ID, dryRun: false },
      ctx,
    );
    expect(deleteConversation).toHaveBeenCalledWith(ID);
    expect(audit[0]).toMatchObject({
      action: `job.assistant/conversations/${ID}`,
      entityLabel: `DELETE assistant/conversations/${ID}`,
    });
  });

  it("delete_assistant_conversation names a chat that is gone", async () => {
    const { ctx } = adminContext("editor", [
      [AssistantRecordsService, { conversation: async () => null }],
    ]);
    await expect(
      deleteAssistantConversationTool.handler({ id: ID, dryRun: true }, ctx),
    ).rejects.toBeInstanceOf(McpToolError);
  });

  it("block_event blocks with the account and unblocks", async () => {
    const block = jest.fn(async () => undefined);
    const unblock = jest.fn(async () => undefined);
    const { ctx, audit } = adminContext("editor", [
      [AdminSiteAnalyticsController, { block, unblock }],
    ]);
    await blockEventTool.handler({ name: "chip_shown", blocked: true }, ctx);
    await blockEventTool.handler({ name: "chip_shown", blocked: false }, ctx);
    expect(block).toHaveBeenCalledWith("chip_shown", ctx.admin);
    expect(unblock).toHaveBeenCalledWith("chip_shown");
    expect(audit.map((row) => row.entityLabel)).toEqual([
      "PUT analytics/blocked-events/chip_shown",
      "DELETE analytics/blocked-events/chip_shown",
    ]);
    expect(
      z
        .object(blockEventTool.input)
        .safeParse({ name: "Bad Name", blocked: true }).success,
    ).toBe(false);
  });

  it("admin_users reads accounts and roles for an owner only", async () => {
    const { ctx } = adminContext("owner", [
      [
        AdminAuthService,
        {
          list: async () => [
            {
              id: ID,
              email: "a@park.fan",
              displayName: "A",
              role: "editor",
              isActive: true,
              mustChangePassword: false,
              totpEnabled: true,
              lastLoginAt: null,
              createdAt: "2026-01-01T00:00:00.000Z",
            },
          ],
        },
      ],
    ]);
    expect(await adminUsersTool.handler({}, ctx)).toMatchObject({
      total: 1,
      users: [{ email: "a@park.fan", role: "editor", totp: true }],
    });
    expect(adminUsersTool.writes).toBeUndefined();
    expect((await callAs(adminUsersTool, "editor", {})).isError).toBe(true);
  });
});
