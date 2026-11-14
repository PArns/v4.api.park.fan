import { plainToInstance, type ClassConstructor } from "class-transformer";
import { validate, type ValidationError } from "class-validator";
import { z } from "zod";
import type { ToolAnnotations } from "@modelcontextprotocol/sdk/types.js";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import {
  ADMIN_TOKEN_SCOPES,
  scopeForRole,
  type AdminTokenScope,
} from "../admin/auth/admin-api-token.constants";
import { roleAtLeast } from "../admin/auth/entities/admin-user.entity";
import { clientErrorMessage } from "../common/utils/client-error-message.util";
import { McpToolError, type AnyMcpTool } from "./mcp-tool";

/**
 * The write gate of the admin MCP server, in one file: what a client is told
 * about a tool that writes, who may call it, the route's body check, how a
 * service's refusal reaches the model, and the `dryRun` and `confirm` inputs
 * every write tool shares.
 * A write tool calls the service its admin route calls, so validation, the
 * audit row and the publishing are the route's (docs/admin/mcp.md#writing).
 */

/**
 * The hints a client shows beside a tool. A tool that only adds rows is not
 * destructive; one that changes or removes them is, and a client may ask the
 * person before running it.
 */
export function toolAnnotations(tool: AnyMcpTool): ToolAnnotations {
  return {
    title: tool.title,
    readOnlyHint: !tool.writes,
    destructiveHint: tool.writes === "change" || tool.writes === "delete",
    idempotentHint: tool.writes !== "add",
    openWorldHint: false,
  };
}

/**
 * Why a principal may not call a tool, or null when it may. A token whose
 * scope stops below the tool's role and an account below it are told apart,
 * so the answer names the credential to get: `admin:write` for an editor's
 * tool, `admin:owner` for an owner's.
 */
export function toolRefusal(
  tool: AnyMcpTool,
  admin: AdminPrincipal,
): string | null {
  const minRole = tool.minRole ?? "viewer";
  if (roleAtLeast(admin.role, minRole)) return null;
  const scope = admin.apiToken?.scope;
  const cap =
    scope && scope in ADMIN_TOKEN_SCOPES
      ? ADMIN_TOKEN_SCOPES[scope as AdminTokenScope]
      : null;
  if (cap && !roleAtLeast(cap, minRole)) {
    const needed = scopeForRole(minRole);
    const who = minRole === "owner" ? "An owner" : "An editor or owner";
    return (
      `${tool.name} needs a token with the ${needed} scope; this one has ${scope}. ` +
      `${who} connects again and allows it on the consent page, or creates a ` +
      `personal access token with ${needed}.`
    );
  }
  return `${tool.name} needs the ${minRole} role; this account is ${admin.role}`;
}

/**
 * Runs a write through an admin route's service and hands its refusals to
 * the model. A 4xx (bad input, no such row, a conflict, a rate limit) carries
 * a message written for a client, a ticket product's `problems` by path among
 * it; anything else stays the generic failure `McpServerFactory.call` gives.
 */
export async function asToolRefusal<T>(work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (error) {
    const message = clientErrorMessage(error);
    if (message !== null) throw new McpToolError(message);
    throw error;
  }
}

/**
 * A body checked as the global `ValidationPipe` checks the admin route's: its
 * DTO, implicit conversion on, a field the DTO does not list refused. Throws
 * the failed constraints for the model; answers the DTO as a plain object, as
 * the route hands it to its service or its handler.
 */
export async function routeBody<T extends object>(
  dto: ClassConstructor<T>,
  body: Record<string, unknown>,
): Promise<T & Record<string, unknown>> {
  const instance = plainToInstance(dto, body, {
    enableImplicitConversion: true,
  });
  const errors = await validate(instance, {
    whitelist: true,
    forbidNonWhitelisted: true,
  });
  if (errors.length > 0) {
    throw new McpToolError(
      `Invalid input: ${constraintsOf(errors).join("; ")}`,
    );
  }
  return { ...instance } as T & Record<string, unknown>;
}

function constraintsOf(errors: ValidationError[], path = ""): string[] {
  return errors.flatMap((error) => [
    ...Object.values(error.constraints ?? {}).map((text) =>
      path ? `${path}.${text}` : text,
    ),
    ...constraintsOf(
      error.children ?? [],
      path ? `${path}.${error.property}` : error.property,
    ),
  ]);
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

/** `confirm: true`, without which a delete removes nothing. */
export function confirmInput(removes: string) {
  return z
    .literal(true)
    .describe(
      `Must be true: ${removes}. Read the row first and send this only when the person asked for it.`,
    );
}
