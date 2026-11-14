import { AdminAuditService } from "../admin/auth/admin-audit.service";
import { routeAuditEntry } from "../admin/auth/admin-audit.interceptor";
import type { McpToolContext } from "./mcp-tool";
import { asToolRefusal } from "./mcp-write";

/**
 * How a write tool of the admin's other pages does what the admin does: it
 * runs the admin route's own handler, from the controller the module graph
 * already holds, where that handler is more than one service call (a merge's
 * preview, a job's queue options, an API key's audit row). The handler does
 * the write, the cache eviction and the frontend revalidation; this adds the
 * audit row `AdminAuditInterceptor` writes around a route that does not write
 * its own, and the refusal the model reads (`asToolRefusal`). The body is
 * checked before, with `routeBody`. docs/admin/mcp.md#the-admins-other-pages.
 */

/** One admin route, as its audit row names it. */
export interface AdminRoute {
  method: "POST" | "PUT" | "PATCH" | "DELETE";
  /** The path below `/v1/admin/`, with concrete ids: `retired-attractions/<id>/hidden`. */
  path: string;
  /** What the admin would have sent; the audit row keeps it, credentials redacted. */
  body?: unknown;
  /** The handler writes its own, richer audit row (`@SelfAudited` or no interceptor). */
  selfAudited?: boolean;
}

/**
 * Runs a route's handler as the account behind the token, and records the
 * interceptor's audit row after it succeeds, as the interceptor would: a
 * refused call changed nothing and leaves no row.
 */
export async function runAdminRoute<T>(
  ctx: McpToolContext,
  route: AdminRoute,
  handler: () => Promise<T>,
): Promise<T> {
  const value = await asToolRefusal(handler);
  if (!route.selfAudited) {
    await ctx
      .get(AdminAuditService)
      .record(
        routeAuditEntry(
          ctx.admin,
          route.method,
          route.path,
          route.body ?? null,
        ),
      );
  }
  return value;
}
