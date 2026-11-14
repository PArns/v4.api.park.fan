import { Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import * as packageJson from "../../package.json";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { McpToolError, type AnyMcpTool, type McpToolContext } from "./mcp-tool";
import { toolAnnotations, toolRefusal } from "./mcp-write";
import { MCP_TOOLS } from "./tools";

/** A tool answer longer than this is cut; the model is told to narrow the call. */
export const MAX_TOOL_RESULT_CHARS = 100_000;

const INSTRUCTIONS =
  "park.fan's admin data: parks and rides with live status, waits and " +
  "outages, page-view popularity, the site statistic and its SEO reports (analytics_*, " +
  "search_report, planner_report for the day planner, trip_planner_report for the trip " +
  "planner, seo_*), the trip planner's server metrics (trip_planner_ops), system and ML " +
  "health, job queues, data-quality findings, " +
  "duplicate and retirement candidates, outage statistics, the admin audit log, and Parky's " +
  "chats and the visitors' feedback (assistant_*, site_feedback). Ids are " +
  "UUIDs; find_parks and find_attractions turn names into ids. Timestamps are ISO 8601 and " +
  "park-local times carry their offset.";

/** Appended for a caller that may only read. */
const READ_ONLY = " Nothing here changes data.";

/** Appended for a caller whose token may write. */
const WRITES =
  " With this token, the tools not marked read-only also write, as an editor does in the " +
  "admin, through the same checks. Every write is in the audit log under this account. Write " +
  "only what the person asked for; run a bulk write with dryRun first, and a delete needs confirm.";

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
    const visible = this.visibleTools(admin, tools);
    const server = new McpServer(
      { name: "park.fan admin", version: packageJson.version },
      {
        instructions:
          INSTRUCTIONS +
          (visible.some((tool) => tool.writes) ? WRITES : READ_ONLY),
      },
    );
    for (const tool of visible) {
      server.registerTool(
        tool.name,
        {
          title: tool.title,
          description: tool.description,
          inputSchema: tool.input,
          annotations: toolAnnotations(tool),
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
    return tools.filter((tool) => toolRefusal(tool, admin) === null);
  }

  /** Runs one tool call and turns its value or failure into an MCP result. */
  async call(
    tool: AnyMcpTool,
    input: Record<string, unknown>,
    admin: AdminPrincipal,
  ): Promise<CallToolResult> {
    const refusal = toolRefusal(tool, admin);
    if (refusal) return failure(refusal);
    const context: McpToolContext = {
      admin,
      get: (token) => this.moduleRef.get(token, { strict: false }),
    };
    const started = Date.now();
    try {
      const value = await tool.handler(input as never, context);
      const line = `${tool.name} for ${admin.email} in ${Date.now() - started} ms`;
      // A write's audit row does not say it came through MCP; this line does.
      if (tool.writes) this.logger.log(line);
      else this.logger.debug(line);
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
