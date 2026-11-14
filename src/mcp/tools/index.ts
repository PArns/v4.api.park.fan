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
import { mlAccuracyTool } from "./ml-accuracy.tool";
import {
  modelComparisonDayTool,
  modelComparisonTool,
} from "./model-comparison.tool";
import { parkForecastTool } from "./park-forecast.tool";
import { dataQualityTool } from "./data-quality.tool";
import { duplicatesTool } from "./duplicates.tool";
import { retirementCandidatesTool } from "./retirement-candidates.tool";
import { auditLogTool } from "./audit-log.tool";
import { assistantConversationsTool } from "./assistant-conversations.tool";
import { assistantConversationTool } from "./assistant-conversation.tool";
import { siteFeedbackTool } from "./site-feedback.tool";
import { assistantToolsReportTool } from "./assistant-tools-report.tool";
import { assistantReportTool } from "./assistant-report.tool";
import { downtimeTool } from "./downtime.tool";
import { analyticsOverviewTool } from "./analytics-overview.tool";
import { analyticsBreakdownTool } from "./analytics-breakdown.tool";
import { analyticsEventsTool } from "./analytics-events.tool";
import { analyticsEventTool } from "./analytics-event.tool";
import { analyticsCrosstabTool } from "./analytics-crosstab.tool";
import { analyticsTrafficCrosstabTool } from "./analytics-traffic-crosstab.tool";
import { analyticsLiveTool } from "./analytics-live.tool";
import { plannerReportTool } from "./planner-report.tool";
import { tripPlannerOpsTool } from "./trip-planner-ops.tool";
import { tripPlannerReportTool } from "./trip-planner-report.tool";
import { searchReportTool } from "./search-report.tool";
import { seoLandingPagesTool } from "./seo-landing-pages.tool";
import { seoTrendsTool } from "./seo-trends.tool";
import { seoNotFoundTool } from "./seo-not-found.tool";
import { seoUncoveredTool } from "./seo-uncovered.tool";
import { gscStatusTool } from "./gsc-status.tool";
import { gscQueriesTool } from "./gsc-queries.tool";
import { gscPagesTool } from "./gsc-pages.tool";
import { gscSeriesTool } from "./gsc-series.tool";
import { gscInspectUrlTool } from "./gsc-inspect-url.tool";
import {
  addRedirectTool,
  correctParkLocationTool,
  curateAttractionTool,
  curateParkAttractionsTool,
  curateParkTool,
  curationFieldsTool,
  deleteRideProfileTool,
  refreshParkTool,
  removeRedirectTool,
  undoCurationTool,
  verifyParkTool,
  writeRideProfileTool,
} from "./curation.tools";
import {
  retireAttractionsTool,
  retiredAttractionsTool,
  retireParkTool,
  reviewMarkTool,
  setRetiredHiddenTool,
  unretireAttractionTool,
} from "./retirement.tools";
import { mergeAttractionsTool, mergeParksTool } from "./merge.tools";
import { runJobTool, runMaintenanceTool } from "./jobs.tools";
import {
  apiKeysTool,
  createApiKeyTool,
  deleteApiKeyTool,
  revokeApiKeyTool,
  rotateApiKeyTool,
  updateApiKeyTool,
} from "./api-keys.tools";
import {
  adminUsersTool,
  blockEventTool,
  deleteAssistantConversationTool,
  setFeedbackStatusTool,
} from "./site-admin.tools";

/**
 * Every tool the admin MCP server offers, in the order clients list them.
 * A new tool is a file next to this one and a line here; nothing else
 * registers it. A tool that writes runs its admin route's handler
 * (`../admin-route.ts`) and carries `writes` and the route's `minRole`.
 */
export const MCP_TOOLS: readonly AnyMcpTool[] = [
  whoamiTool,
  findParksTool,
  getParkTool,
  parkForecastTool,
  findAttractionsTool,
  getAttractionTool,
  pageViewsTool,
  systemHealthTool,
  queueStatusTool,
  mlOverviewTool,
  mlAccuracyTool,
  modelComparisonTool,
  modelComparisonDayTool,
  dataQualityTool,
  duplicatesTool,
  retirementCandidatesTool,
  auditLogTool,
  assistantConversationsTool,
  assistantConversationTool,
  siteFeedbackTool,
  assistantToolsReportTool,
  assistantReportTool,
  downtimeTool,
  analyticsOverviewTool,
  analyticsLiveTool,
  analyticsBreakdownTool,
  analyticsTrafficCrosstabTool,
  analyticsEventsTool,
  analyticsEventTool,
  analyticsCrosstabTool,
  searchReportTool,
  plannerReportTool,
  tripPlannerReportTool,
  tripPlannerOpsTool,
  seoLandingPagesTool,
  seoTrendsTool,
  seoNotFoundTool,
  seoUncoveredTool,
  gscStatusTool,
  gscQueriesTool,
  gscPagesTool,
  gscSeriesTool,
  gscInspectUrlTool,
  // The admin's pages: curation, merges, retirement, jobs, the site, keys, accounts.
  curationFieldsTool,
  curateParkTool,
  curateAttractionTool,
  curateParkAttractionsTool,
  verifyParkTool,
  undoCurationTool,
  writeRideProfileTool,
  deleteRideProfileTool,
  correctParkLocationTool,
  refreshParkTool,
  addRedirectTool,
  removeRedirectTool,
  mergeAttractionsTool,
  mergeParksTool,
  retiredAttractionsTool,
  retireAttractionsTool,
  unretireAttractionTool,
  setRetiredHiddenTool,
  retireParkTool,
  reviewMarkTool,
  runJobTool,
  runMaintenanceTool,
  setFeedbackStatusTool,
  deleteAssistantConversationTool,
  blockEventTool,
  apiKeysTool,
  createApiKeyTool,
  updateApiKeyTool,
  rotateApiKeyTool,
  revokeApiKeyTool,
  deleteApiKeyTool,
  adminUsersTool,
];
