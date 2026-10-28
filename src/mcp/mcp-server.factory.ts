import { Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import * as packageJson from "../../package.json";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { roleAtLeast } from "../admin/auth/entities/admin-user.entity";
import { McpToolError, type AnyMcpTool, type McpToolContext } from "./mcp-tool";
import { MCP_TOOLS } from "./tools";

/** A tool answer longer than this is cut; the model is told to narrow the call. */
export const MAX_TOOL_RESULT_CHARS = 100_000;

const INSTRUCTIONS =
  "Read-only view of park.fan's admin data: parks and rides with live status, waits and " +
  "outages, page-view popularity, the site statistic and its SEO reports (analytics_*, " +
  "search_report, seo_*), system and ML health, job queues, data-quality findings, " +
  "duplicate and retirement candidates, outage statistics and the admin audit log. Ids are " +
  "UUIDs; find_parks and find_attractions turn names into ids. Timestamps are ISO 8601 and " +
  "park-local times carry their offset. Nothing here changes data.";

/**
 * Builds the MCP server for one request. Stateless: a fresh server per POST,
 * holding only the tools the caller's role may see, so a tool above the role
 * is not even listed. The SDK is used here and in `McpController` only.
 */
@Injectable()
export class McpServerFactory {
  private readonly logger = new Logger("MCP");

  constructor(private readonly moduleRef: ModuleRef) {}

  create(
    admin: AdminPrincipal,
    tools: readonly AnyMcpTool[] = MCP_TOOLS,
  ): McpServer {
    const server = new McpServer(
      { name: "park.fan admin", version: packageJson.version },
      { instructions: INSTRUCTIONS },
    );
    for (const tool of this.visibleTools(admin, tools)) {
      server.registerTool(
        tool.name,
        {
          title: tool.title,
          description: tool.description,
          inputSchema: tool.input,
          annotations: {
            title: tool.title,
            readOnlyHint: true,
            destructiveHint: false,
            idempotentHint: true,
            openWorldHint: false,
          },
        },
        ((input: Record<string, unknown>) =>
          this.call(tool, input, admin)) as never,
      );
    }
    return server;
  }

  visibleTools(
    admin: AdminPrincipal,
    tools: readonly AnyMcpTool[] = MCP_TOOLS,
  ): AnyMcpTool[] {
    return tools.filter((tool) =>
      roleAtLeast(admin.role, tool.minRole ?? "viewer"),
    );
  }

  /** Runs one tool call and turns its value or failure into an MCP result. */
  async call(
    tool: AnyMcpTool,
    input: Record<string, unknown>,
    admin: AdminPrincipal,
  ): Promise<CallToolResult> {
    if (!roleAtLeast(admin.role, tool.minRole ?? "viewer")) {
      return failure(`${tool.name} needs the ${tool.minRole} role`);
    }
    const context: McpToolContext = {
      admin,
      get: (token) => this.moduleRef.get(token, { strict: false }),
    };
    const started = Date.now();
    try {
      const value = await tool.handler(input as never, context);
      this.logger.debug(
        `${tool.name} for ${admin.email} in ${Date.now() - started} ms`,
      );
      return { content: [{ type: "text", text: serialize(value) }] };
    } catch (error) {
      if (error instanceof McpToolError) return failure(error.message);
      this.logger.warn(
        `${tool.name} failed for ${admin.email}: ${error instanceof Error ? error.message : String(error)}`,
      );
      return failure(
        `${tool.name} failed on the server; try again or narrow the call`,
      );
    }
  }
}

function failure(message: string): CallToolResult {
  return { isError: true, content: [{ type: "text", text: message }] };
}

/** Compact JSON, cut to `MAX_TOOL_RESULT_CHARS` with a note the model can act on. */
export function serialize(value: unknown): string {
  const text = JSON.stringify(value ?? null);
  if (text.length <= MAX_TOOL_RESULT_CHARS) return text;
  return (
    text.slice(0, MAX_TOOL_RESULT_CHARS) +
    `\n[truncated: the answer had ${text.length} characters; ask for fewer items or a narrower window]`
  );
}
