import { z } from "zod";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { dryRunInput } from "../mcp-write";
import { runAdminRoute } from "../admin-route";
import { AdminAssistantController } from "../../assistant/admin-assistant.controller";
import { AssistantRecordsService } from "../../assistant/assistant-records.service";
import { FEEDBACK_STATUSES } from "../../assistant/entities/site-feedback.entity";
import { AdminSiteAnalyticsController } from "../../site-analytics/site-analytics.controller";
import { EVENT_NAME_PATTERN } from "../../site-analytics/site-analytics.hits";
import { AdminAuthService } from "../../admin/auth/admin-auth.service";
import { chatSummary } from "./assistant-records";

/**
 * The admin's smaller writes around the site: the feedback list's status,
 * deleting one of Parky's chats, the site statistic's event block list, and
 * the accounts list, which stays read-only here: no tool changes a role.
 */

export const setFeedbackStatusTool = defineMcpTool({
  name: "set_feedback_status",
  title: "Mark feedback",
  description:
    "Marks one visitor feedback done or archived, or new again, as the admin's feedback list " +
    "does. Ids come from site_feedback.",
  minRole: "editor",
  writes: "change",
  input: {
    id: uuidInput().describe("Feedback id from site_feedback"),
    status: z.enum(FEEDBACK_STATUSES).describe("The new status"),
  },
  async handler({ id, status }, ctx) {
    const feedbackId = id.toLowerCase();
    const body = { status };
    await runAdminRoute(
      ctx,
      { method: "PATCH", path: `assistant/feedback/${feedbackId}`, body },
      () =>
        ctx.get(AdminAssistantController).setFeedbackStatus(feedbackId, body),
    );
    return { id: feedbackId, status };
  },
});

export const deleteAssistantConversationTool = defineMcpTool({
  name: "delete_assistant_conversation",
  title: "Delete a Parky chat",
  description:
    "Deletes one of Parky's chats with its messages, as the admin's delete button does; " +
    "feedback sent from it stays, without the link. For a chat a visitor asked to have removed " +
    "or one that holds what should not be kept. The dry run (default) shows the chat.",
  minRole: "editor",
  writes: "delete",
  input: {
    id: uuidInput().describe("The chat's id from assistant_conversations"),
    dryRun: dryRunInput("the chat that would be deleted"),
  },
  async handler({ id, dryRun }, ctx) {
    const chatId = id.toLowerCase();
    const chat = await ctx.get(AssistantRecordsService).conversation(chatId);
    if (!chat) throw new McpToolError(`No chat ${chatId}`);
    if (dryRun) {
      return { dryRun: true, chat: chatSummary(chat) };
    }
    await runAdminRoute(
      ctx,
      { method: "DELETE", path: `assistant/conversations/${chatId}` },
      () => ctx.get(AdminAssistantController).deleteConversation(chatId),
    );
    return { dryRun: false, deleted: chatId };
  },
});

export const blockEventTool = defineMcpTool({
  name: "block_event",
  title: "Block an event name",
  description:
    "Stops or resumes recording one event name in the site statistic, as the admin's traffic " +
    "page does; every instance applies it within a minute. analytics_events lists the names and " +
    "which are blocked. Blocking drops new events only; what was recorded stays.",
  minRole: "editor",
  writes: "change",
  input: {
    name: z
      .string()
      .regex(
        EVENT_NAME_PATTERN,
        "an event name: lowercase, digits, underscores",
      )
      .describe("The event name"),
    blocked: z
      .boolean()
      .describe("True stops recording it, false records it again"),
  },
  async handler({ name, blocked }, ctx) {
    const controller = ctx.get(AdminSiteAnalyticsController);
    await runAdminRoute(
      ctx,
      {
        method: blocked ? "PUT" : "DELETE",
        path: `analytics/blocked-events/${name}`,
      },
      () =>
        blocked ? controller.block(name, ctx.admin) : controller.unblock(name),
    );
    return { name, blocked };
  },
});

export const adminUsersTool = defineMcpTool({
  name: "admin_users",
  title: "Admin accounts",
  description:
    "The admin accounts with their role (owner, editor, author, viewer), whether they are " +
    "active, use a second factor or still owe a password change, and when they last signed in. " +
    "Read-only: roles and accounts change in the admin's users page only. Owner only.",
  minRole: "owner",
  input: {},
  async handler(_input, ctx) {
    const users = await ctx.get(AdminAuthService).list();
    return {
      total: users.length,
      users: users.map((user) => ({
        id: user.id,
        email: user.email,
        displayName: user.displayName,
        role: user.role,
        active: user.isActive,
        totp: user.totpEnabled,
        mustChangePassword: user.mustChangePassword,
        lastLoginAt: user.lastLoginAt,
        createdAt: user.createdAt,
      })),
    };
  },
});
