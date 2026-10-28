import { Module } from "@nestjs/common";
import { McpController } from "./mcp.controller";
import { McpServerFactory } from "./mcp-server.factory";

/**
 * The admin MCP server at `/v1/mcp`. Its tools reach the services they wrap
 * through `ModuleRef`, so this module imports none of them and a new tool
 * needs no wiring here. Authentication is `AdminAuthGuard` from the global
 * `AdminAuthModule`; the OAuth server that issues its tokens is `OAuthModule`.
 */
@Module({
  controllers: [McpController],
  providers: [McpServerFactory],
})
export class McpModule {}
