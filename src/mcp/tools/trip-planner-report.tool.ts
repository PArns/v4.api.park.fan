import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SiteAnalyticsTripService } from "../../site-analytics/site-analytics-trip.service";
import { TRIP_REGION_SLUGS } from "../../trip-regions/regions";
import {
  HOURLY_FROM_DESCRIPTION,
  eventWindowInput,
  siteQuery,
} from "./site-analytics.inputs";
import {
  SMALL_CELLS_DESCRIPTION,
  SMALL_SERIES_DESCRIPTION,
  foldSmallRows,
  hideSmallInSeries,
  isNarrowed,
} from "./small-cells";
import { VISITOR_DATA_DESCRIPTION, withVisitorData } from "./visitor-data";

export const tripPlannerReportTool = defineMcpTool({
  name: "trip_planner_report",
  title: "Trip planner (vacation) usage",
  description:
    "How park.fan's multi-day trip planner (the region hubs) is used, from its trip_* events and " +
    "ticket_link_clicked, per region or for all (`region`). Not the day planner: that is " +
    "planner_report. The funnel of the dates-first planner (release 2.21.0, the frontend's " +
    "docs/features/orlando-planner.md#the-funnel): started (trip_planner_started, source hero, " +
    "button, link or dates) → requested (trip_plan_requested) → shown (trip_plan_shown) → " +
    "changed (trip_edit_opened, and trip_option_changed by the visitor; Parky's are counted " +
    "beside) → tickets (trip_tickets_opened) → kept (days saved to the day planner, .ics " +
    "exported, link copied). Every step has ofStarted; requested and shown have fromPrevious and " +
    "dropOff (1 − fromPrevious, null where a step outnumbers the one before); changed and " +
    "tickets are optional (a plan that suits needs neither), so they come as ofShown, a share of " +
    "shown, not a drop-off; kept follows shown, with fromPrevious and dropOff against it. " +
    "newFunnelFrom is the first UTC day the dates-first planner's own events were counted; each " +
    "step counts its `before` days by the old planner's events (trip_tickets_answered with yes " +
    "for tickets, no longer sent; nothing for changed) and its `since` days by the new ones, and " +
    "every rate is taken over the days both steps count, so a window across the deploy still " +
    "adds " +
    "up. Counts are events, not visitors: an answer fires once per distinct request. Then: ways " +
    "in (source, via) and the planner tile on the region parks' pages (shown: its section_seen, " +
    "attributed to a region by the park in its page; clicked: trip_tile_clicked by link, plan " +
    "or calendar; clickRate), the requests (mode dates or when, trip length bucket, parks with 0 for " +
    "the headliners, owned tickets, hopper none/some/all, blocks, rest), answers by mode and " +
    "outcome (ok, relaxed with the rule, error with the HTTP status, 0 = no answer), parks left " +
    "out and the fix chosen, cities of a route, find dates (toggles, periods, windows found and " +
    "planned by rank), the edits opened (panel, from) and options changed (option, value, by " +
    "visitor or parky), the stay suggestion, ticket openings (from, first) and entries, hopper, " +
    "the views switched to, the date grid's resort mode, days opened, pins, the " +
    "accuracy note, what was kept and the day planner opened from a trip day, after copying the " +
    "days or from the hub (planner_opened's <region>-trip-day, -trip-first, -hub), ticket " +
    "purchase links by seller kind (official, sponsored, " +
    "plain) and position, surface, mode, locale match, seller and product, attributed to a " +
    "region by the product's catalog (a product no catalog has is in unattributedTicketLinks); " +
    "the regions side by side (a region value park.fan does not have is (other)) and the page " +
    "types the planner was used from (trip_hub, trip_month, trip_best_time from the deploy on, " +
    "other before and elsewhere). An event " +
    "the frontend does not send yet lists nothing. The server's own record of the plan, when " +
    "and best-days answers is trip_planner_ops. UTC days; up to six minutes behind the site; Do " +
    "Not Track, GPC and opted-out readers are missing. " +
    VISITOR_DATA_DESCRIPTION +
    " " +
    SMALL_CELLS_DESCRIPTION +
    " A region counts as narrowing like a filter. " +
    SMALL_SERIES_DESCRIPTION +
    " " +
    HOURLY_FROM_DESCRIPTION,
  input: {
    ...eventWindowInput,
    region: z
      .enum(TRIP_REGION_SLUGS)
      .optional()
      .describe("Only this region's events; every region's when absent"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(20)
      .describe("Entries per breakdown list"),
    series: z
      .boolean()
      .default(false)
      .describe("Include starts, plan requests and plans shown per period"),
  },
  async handler({ region, limit, series, ...input }, ctx) {
    const { head, lists } = await ctx.get(SiteAnalyticsTripService).answer(
      siteQuery(input),
      { region: region ?? null, limit, series },
      {
        narrowed: isNarrowed(input),
        rows: foldSmallRows,
        series: hideSmallInSeries,
      },
    );
    return withVisitorData(head, lists);
  },
});
