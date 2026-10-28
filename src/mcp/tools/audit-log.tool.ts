import { z } from "zod";
import { defineMcpTool, uuidInput } from "../mcp-tool";
import { AdminAuditService } from "../../admin/auth/admin-audit.service";

export const auditLogTool = defineMcpTool({
  name: "audit_log",
  title: "Admin audit log",
  description:
    "Recent admin actions, newest first: who (email), when, the action (e.g. attraction.curate, " +
    "park.season.create, job.sync-parks, auth.login, auth.token.create), what it was done to, the " +
    "changed fields before and after, and the reason and source the admin gave. Filter by entity, " +
    'by action prefix ("park." for every park action) or by account id.',
  input: {
    entityType: z
      .string()
      .max(40)
      .optional()
      .describe(
        "park, attraction, park_season, ride_profile, admin_user, admin_api_token, system …",
      ),
    entityId: z.string().max(80).optional().describe("The id of that entity"),
    action: z
      .string()
      .max(60)
      .optional()
      .describe('Action prefix, e.g. "attraction." or "auth.token"'),
    actorId: uuidInput()
      .optional()
      .describe("Only actions of this admin account"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(20)
      .describe("Entries to return"),
    offset: z
      .number()
      .int()
      .min(0)
      .max(10_000)
      .default(0)
      .describe("Entries to skip"),
  },
  async handler({ entityType, entityId, action, actorId, limit, offset }, ctx) {
    const { entries, total } = await ctx.get(AdminAuditService).list({
      entityType,
      entityId,
      action,
      actorId: actorId?.toLowerCase(),
      limit,
      offset,
    });
    return {
      total,
      offset,
      entries: entries.map((entry) => ({
        id: entry.id,
        at: entry.createdAt,
        actor: entry.actorEmail,
        action: entry.action,
        kind: entry.kind,
        entityType: entry.entityType,
        entityId: entry.entityId,
        entityLabel: entry.entityLabel,
        before: entry.before,
        after: entry.after,
        reason: entry.reason,
        sourceUrl: entry.sourceUrl,
        revertedBy: entry.revertedBy,
      })),
    };
  },
});
