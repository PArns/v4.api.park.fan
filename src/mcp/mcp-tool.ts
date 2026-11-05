import type { Type } from "@nestjs/common";
import { z } from "zod";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import type { AdminRole } from "../admin/auth/entities/admin-user.entity";

/** What a tool handler gets besides its input. */
export interface McpToolContext {
  /** Who is calling, with the role already capped by the token. */
  admin: AdminPrincipal;
  /** Any provider of the application, e.g. `ctx.get(ParksService)`. */
  get<T>(token: Type<T>): T;
}

type ToolInputShape = Record<string, z.ZodType>;

export interface McpToolDefinition<Shape extends ToolInputShape> {
  /** snake_case; what the model calls. */
  name: string;
  /** A few words for a person reading a tool list. */
  title: string;
  /**
   * For the model: what the tool answers, when to reach for it, what it
   * returns and how to get the ids it needs. The model reads nothing else.
   */
  description: string;
  /** Each field with `.describe()`, and with bounds: a model sends what it is allowed to. */
  input: Shape;
  /** The least role that sees the tool; `viewer` when absent. */
  minRole?: AdminRole;
  /**
   * Returns a plain JSON value. Keep it compact: names and numbers the model
   * needs, ids for follow-up calls, lists cut to the limit the input allows.
   */
  handler(
    input: z.infer<z.ZodObject<Shape>>,
    ctx: McpToolContext,
  ): Promise<unknown>;
}

/**
 * A failure the model should read, such as "no park with that id". Any other
 * error a handler throws reaches the model as a generic failure; its message
 * may carry SQL or a hostname and only goes to the log.
 */
export class McpToolError extends Error {}

/** A tool of any input shape, as the registry holds it. */
export type AnyMcpTool = McpToolDefinition<ToolInputShape>;

/**
 * One tool, one file: `export const fooTool = defineMcpTool({ … })`, then one
 * line in `tools/index.ts`. The input type flows from the zod shape into the
 * handler.
 */
export function defineMcpTool<Shape extends ToolInputShape>(
  definition: McpToolDefinition<Shape>,
): AnyMcpTool {
  return definition as unknown as AnyMcpTool;
}

/** A list cut to `limit`, with the size it had, so the model knows there is more. */
export function capped<T>(
  items: readonly T[],
  limit: number,
): { total: number; items: T[] } {
  return { total: items.length, items: items.slice(0, limit) };
}

const UUID_INPUT = /^[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$/;

/**
 * An id argument. A pattern rather than `z.uuid()`, which checks the version
 * bits and would refuse ids this API itself hands out in fixtures and imports;
 * handlers lowercase it before it reaches a uuid column. Describe it last:
 * `.optional()` makes a new schema without the description.
 */
export function uuidInput() {
  return z.string().regex(UUID_INPUT, "must be a uuid");
}
