import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import type { AdminPrincipal } from "../../../admin/auth/admin-principal";
import type { AdminRole } from "../../../admin/auth/entities/admin-user.entity";
import { scopeForRole } from "../../../admin/auth/admin-api-token.constants";
import {
  AdminAuditService,
  type AuditWrite,
} from "../../../admin/auth/admin-audit.service";
import { McpServerFactory } from "../../mcp-server.factory";
import type { AnyMcpTool, McpToolContext } from "../../mcp-tool";

/** The account behind a token, with the narrowest scope that leaves it this role. */
export function principal(role: AdminRole): AdminPrincipal {
  return {
    userId: "11111111-1111-4111-8111-111111111111",
    email: "you@park.fan",
    displayName: "You",
    role,
    sessionToken: null,
    legacy: false,
    ip: "203.0.113.7",
    mustChangePassword: false,
    mustEnrolTotp: false,
    apiToken: {
      id: "t",
      kind: "pat",
      clientName: null,
      scope: scopeForRole(role),
    },
  };
}

/**
 * A tool context whose providers are the given fakes, plus an audit service
 * that keeps every row it is handed, for a spec to read.
 */
export function adminContext(
  role: AdminRole,
  providers: Array<[unknown, unknown]>,
): { ctx: McpToolContext; audit: AuditWrite[] } {
  const audit: AuditWrite[] = [];
  const auditService = {
    record: async (entry: AuditWrite) => {
      audit.push(entry);
      return { id: "a" };
    },
  };
  const byToken = new Map<unknown, unknown>([
    [AdminAuditService, auditService],
    ...providers,
  ]);
  const ctx = {
    admin: principal(role),
    get: (token: unknown) => {
      if (!byToken.has(token)) throw new Error(`no provider ${String(token)}`);
      return byToken.get(token);
    },
  } as unknown as McpToolContext;
  return { ctx, audit };
}

/** A call through the server's gate, as a client makes it, by an account of this role. */
export function callAs(
  tool: AnyMcpTool,
  role: AdminRole,
  input: Record<string, unknown>,
): Promise<CallToolResult> {
  const moduleRef = {
    get: () => {
      throw new Error("the gate let the call through");
    },
  };
  return new McpServerFactory(moduleRef as never).call(
    tool,
    input,
    principal(role),
  );
}

/** The text of a tool result. */
export function resultText(result: CallToolResult): string {
  return (result.content[0] as { text: string }).text;
}
