import { Injectable } from "@nestjs/common";
import { EVENT_DIMS } from "./site-analytics.constants";
import {
  refuseFiltersBeyond,
  siteAnalyticsWindow,
  type SiteAnalyticsQuery,
} from "./site-analytics.query";
import { SiteReportRunner } from "./site-analytics-report-runner";
import { echo } from "./site-analytics.service";
import { readEventProps } from "./site-analytics-event-props";
import {
  ASSISTANT_EVENTS,
  assistantFigures,
  type AssistantFigures,
} from "./site-analytics-assistant";

export type SiteAssistantReport = ReturnType<typeof echo> & AssistantFigures;

/**
 * Parky's report over his events: their properties summed over the window
 * (`readEventProps`) and read by `assistantFigures`. Read by the MCP tool
 * `assistant_report`; no admin route serves it yet. docs/admin/mcp.md.
 */
@Injectable()
export class SiteAnalyticsAssistantService {
  constructor(private readonly runner: SiteReportRunner) {}

  async report(
    query: SiteAnalyticsQuery,
    now = new Date(),
  ): Promise<SiteAssistantReport> {
    return this.runner.run(async () => {
      const window = siteAnalyticsWindow(query, now);
      refuseFiltersBeyond(window, "Parky's report", EVENT_DIMS);
      const rows = await readEventProps(
        this.runner,
        window,
        Object.values(ASSISTANT_EVENTS),
      );
      return { ...echo(window), ...assistantFigures(rows) };
    });
  }
}
