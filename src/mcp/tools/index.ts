import type { AnyMcpTool } from "../mcp-tool";
import { whoamiTool } from "./whoami.tool";
import { findParksTool } from "./find-parks.tool";
import { getParkTool } from "./get-park.tool";
import { findAttractionsTool } from "./find-attractions.tool";
import { getAttractionTool } from "./get-attraction.tool";
import { pageViewsTool } from "./page-views.tool";
import { systemHealthTool } from "./system-health.tool";
import { queueStatusTool } from "./queue-status.tool";
import { mlOverviewTool } from "./ml-overview.tool";
import { dataQualityTool } from "./data-quality.tool";
import { duplicatesTool } from "./duplicates.tool";
import { retirementCandidatesTool } from "./retirement-candidates.tool";
import { auditLogTool } from "./audit-log.tool";
import { downtimeTool } from "./downtime.tool";

/**
 * Every tool the admin MCP server offers, in the order clients list them.
 * A new tool is a file next to this one and a line here; nothing else
 * registers it. All of v1 is read-only.
 */
export const MCP_TOOLS: readonly AnyMcpTool[] = [
  whoamiTool,
  findParksTool,
  getParkTool,
  findAttractionsTool,
  getAttractionTool,
  pageViewsTool,
  systemHealthTool,
  queueStatusTool,
  mlOverviewTool,
  dataQualityTool,
  duplicatesTool,
  retirementCandidatesTool,
  auditLogTool,
  downtimeTool,
];
