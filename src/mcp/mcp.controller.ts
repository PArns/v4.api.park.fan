import {
  Controller,
  Delete,
  Get,
  Logger,
  Post,
  Req,
  Res,
  UseFilters,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiBearerAuth,
  ApiOperation,
  ApiResponse,
  ApiTags,
} from "@nestjs/swagger";
import type { Request, Response } from "express";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import {
  AdminTokenOnly,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { McpAuthChallengeFilter } from "./mcp-auth-challenge.filter";
import { McpServerFactory } from "./mcp-server.factory";

/**
 * The admin MCP server, over Streamable HTTP in stateless mode: every POST
 * carries its own bearer token and gets a server built for that principal,
 * so no session state has to survive between requests or containers.
 *
 * Answers are JSON rather than an SSE stream: every tool is a single read,
 * and JSON goes through the compression middleware and Cloudflare unchanged.
 */
@ApiTags("mcp")
@ApiBearerAuth("admin-api-token")
@Controller("mcp")
@UseGuards(AdminAuthGuard)
@AdminTokenOnly()
@UseFilters(McpAuthChallengeFilter)
@UseInterceptors(NoCdnCacheInterceptor)
export class McpController {
  private readonly logger = new Logger(McpController.name);

  constructor(private readonly servers: McpServerFactory) {}

  @Post()
  @ApiOperation({
    summary: "MCP endpoint (JSON-RPC over Streamable HTTP)",
    description:
      "Needs `Authorization: Bearer` with a personal access token (`pfa_…`) or an OAuth " +
      "access token from /v1/oauth/token. Read-only tools; see docs/admin/mcp.md.",
  })
  @ApiResponse({
    status: 401,
    description:
      "No valid token; `WWW-Authenticate` names the protected-resource metadata",
  })
  async handle(
    @Req() request: Request,
    @Res() response: Response,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<void> {
    const server = this.servers.create(admin);
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
      enableJsonResponse: true,
    });
    response.on("close", () => {
      void transport.close();
      void server.close();
    });
    try {
      await server.connect(transport);
      await transport.handleRequest(request, response, request.body);
    } catch (error) {
      this.logger.error(
        `MCP request failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      if (!response.headersSent) {
        response.status(500).json({
          jsonrpc: "2.0",
          error: { code: -32603, message: "Internal server error" },
          id: null,
        });
      }
    }
  }

  /** No server-initiated stream in stateless mode (Streamable HTTP §2.2.3). */
  @Get()
  @ApiOperation({ summary: "Not offered: answers 405" })
  openStream(@Res() response: Response): void {
    methodNotAllowed(response);
  }

  /** No session to end in stateless mode. */
  @Delete()
  @ApiOperation({ summary: "Not offered: answers 405" })
  endSession(@Res() response: Response): void {
    methodNotAllowed(response);
  }
}

function methodNotAllowed(response: Response): void {
  response
    .status(405)
    .setHeader("Allow", "POST")
    .json({
      jsonrpc: "2.0",
      error: { code: -32000, message: "Method not allowed." },
      id: null,
    });
}
