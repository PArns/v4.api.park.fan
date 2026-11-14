import { HttpException, ValidationPipe, type Type } from "@nestjs/common";
import { z } from "zod";
import type { ToolAnnotations } from "@modelcontextprotocol/sdk/types.js";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import { routeAuditEntry } from "../admin/auth/admin-audit.interceptor";
import { GLOBAL_VALIDATION_PIPE_OPTIONS } from "../common/pipes/global-validation-pipe.options";
import { McpToolError, type AnyMcpTool, type McpToolContext } from "./mcp-tool";

/**
 * How a write tool does what the admin does: it runs the admin route's own
 * handler, from the controller the module graph already holds, rather than a
 * second copy of it. The handler does the validation the route does, the
 * write, the cache eviction and the frontend revalidation; this file adds
 * what the HTTP layer would have added around it: the body checked by the
 * global ValidationPipe, the audit row `AdminAuditInterceptor` writes for a
 * route that does not write its own, and a refusal the model can read.
 * docs/admin/mcp.md#the-admins-actions.
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

const bodyPipe = new ValidationPipe(GLOBAL_VALIDATION_PIPE_OPTIONS);

/** A body checked and turned into the route's DTO, as the global pipe does it. */
export function routeBody<T>(dto: Type<T>, body: object): Promise<T> {
  return asToolRefusal(
    () =>
      bodyPipe.transform(body, { type: "body", metatype: dto }) as Promise<T>,
  );
}

/**
 * A route's 4xx (bad input, no such row, a conflict) carries a message
 * written for a client, so it reaches the model as a tool error; anything
 * else stays the generic failure `McpServerFactory.call` gives.
 */
export async function asToolRefusal<T>(work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (error) {
    if (error instanceof HttpException && error.getStatus() < 500) {
      throw new McpToolError(refusalText(error));
    }
    throw error;
  }
}

function refusalText(error: HttpException): string {
  const body = error.getResponse();
  if (typeof body === "string") return body;
  const { message } = body as { message?: unknown };
  if (Array.isArray(message)) return message.join("; ");
  return typeof message === "string" ? message : error.message;
}

/** The hints a client shows beside a tool; one that changes or removes rows is destructive. */
export function toolAnnotations(tool: AnyMcpTool): ToolAnnotations {
  return {
    title: tool.title,
    readOnlyHint: !tool.writes,
    destructiveHint: tool.writes === "change" || tool.writes === "delete",
    idempotentHint: tool.writes !== "add",
    openWorldHint: false,
  };
}

/** `dryRun`, on unless the call turns it off: the first call shows, the second writes. */
export function dryRunInput(shows: string) {
  return z
    .boolean()
    .default(true)
    .describe(
      `True (the default) writes nothing and answers ${shows}; false writes.`,
    );
}
