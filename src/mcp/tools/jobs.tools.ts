import { z } from "zod";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { dryRunInput } from "../mcp-write";
import { runAdminRoute } from "../admin-route";
import { AdminController } from "../../admin/admin.controller";
import { AdminSearchConsoleController } from "../../search-console/search-console.controller";
import type { McpToolContext } from "../mcp-tool";

/**
 * The triggers on the admin's actions page: each queues the job its admin
 * route queues, with the same options, and is audited as that route is
 * (docs/admin/endpoints.md#sync-and-background-jobs). queue_status shows
 * the job afterwards.
 */

interface JobParams {
  limit?: number;
  sinceHours?: number;
  days?: number;
  dryRun?: boolean;
}

interface Trigger {
  /** What it does, for the tool description. */
  does: string;
  /** The admin route below `/v1/admin/`, for the audit row. */
  path: string;
  /** The one parameter it takes, if any. */
  param?: keyof JobParams;
  run(ctx: McpToolContext, params: JobParams): Promise<unknown>;
}

const admin = (ctx: McpToolContext) => ctx.get(AdminController);

const JOBS = {
  "sync-parks": {
    does: "parks again from every source",
    path: "sync-parks",
    run: (ctx) => admin(ctx).triggerParkSync(),
  },
  "fill-schedule-gaps": {
    does: "missing calendar days and their holiday metadata",
    path: "fill-schedule-gaps",
    run: (ctx) => admin(ctx).triggerScheduleGapFilling(),
  },
  "sync-holidays": {
    does: "school and public holidays",
    path: "sync-holidays",
    run: (ctx) => admin(ctx).triggerHolidaySync(),
  },
  "enrich-parks": {
    does: "country codes, geocoding and influencing regions",
    path: "enrich-parks",
    run: (ctx) => admin(ctx).triggerParkEnrichment(),
  },
  "sync-attraction-details": {
    does: "minimum heights and gets-wet from ThemeParks.wiki; curated values stay",
    path: "sync-attraction-details",
    run: (ctx) => admin(ctx).triggerAttractionDetailsSync(),
  },
  "sync-six-flags-heights": {
    does: "missing minimum heights from the Six Flags sites",
    path: "sync-six-flags-heights",
    run: (ctx) => admin(ctx).triggerSixFlagsHeights(),
  },
  "import-ride-stats": {
    does: "speed, height, length, duration from Wikidata (limit for a trial run)",
    path: "import-ride-stats",
    param: "limit",
    run: (ctx, { limit }) => admin(ctx).triggerRideStats({ limit }),
  },
  "sync-efteling-maintenance": {
    does: "Efteling's maintenance calendar into works periods (dryRun shows the diff in the job result)",
    path: "sync-efteling-maintenance",
    param: "dryRun",
    run: (ctx, { dryRun }) =>
      admin(ctx).triggerEftelingMaintenanceSync({ dryRun }),
  },
  "publish-ride-profiles": {
    does: "evicts and revalidates parks of ride profiles curated in the last sinceHours (24)",
    path: "publish-ride-profiles",
    param: "sinceHours",
    run: (ctx, { sinceHours }) =>
      admin(ctx).triggerPublishRideProfiles({ sinceHours }),
  },
  "detect-seasonal": {
    does: "which rides are seasonal, normally nightly",
    path: "detect-seasonal",
    run: (ctx) => admin(ctx).triggerSeasonalDetection(),
  },
  "rebuild-downtime": {
    does: "outage intervals, exposure days and profiles over the last days (30)",
    path: "rebuild-downtime",
    param: "days",
    run: (ctx, { days }) =>
      admin(ctx).rebuildDowntime(days === undefined ? undefined : String(days)),
  },
  "aggregate-accuracy-stats": {
    does: "forecast accuracy figures",
    path: "aggregate-accuracy-stats",
    run: (ctx) => admin(ctx).triggerAccuracyAggregation(),
  },
  "train-ml-model": {
    does: "a CatBoost training run",
    path: "train-ml-model",
    run: (ctx) => admin(ctx).triggerMLTraining(),
  },
  "pcn-train": {
    does: "the PCN intraday models per park",
    path: "pcn/train",
    run: (ctx) => admin(ctx).triggerPcn("train"),
  },
  "pcn-forecast": {
    does: "today's PCN intraday forecasts",
    path: "pcn/forecast",
    run: (ctx) => admin(ctx).triggerPcn("forecast"),
  },
  "pcn-score": {
    does: "PCN against CatBoost",
    path: "pcn/score",
    run: (ctx) => admin(ctx).triggerPcn("score"),
  },
  "search-console-import": {
    does: "Google Search Console's last days (once a minute at most)",
    path: "search-console/import",
    run: (ctx) => ctx.get(AdminSearchConsoleController).importNow(),
  },
  "search-console-reindex-scan": {
    does: "the reindex scan: sitemap changes against Google's last crawl",
    path: "search-console/reindex/scan",
    run: (ctx) => ctx.get(AdminSearchConsoleController).scanReindex(),
  },
} satisfies Record<string, Trigger>;

type JobName = keyof typeof JOBS;
const JOB_NAMES = Object.keys(JOBS) as [JobName, ...JobName[]];

/** A parameter sent to a job that takes another, or none. */
function strayParam(trigger: Trigger, params: JobParams): string | undefined {
  return (Object.keys(params) as Array<keyof JobParams>).find(
    (key) => params[key] !== undefined && key !== trigger.param,
  );
}

export const runJobTool = defineMcpTool({
  name: "run_job",
  title: "Run a job",
  description:
    "Queues one of the jobs on the admin's actions page now, with the options its admin route " +
    "uses; the answer comes as soon as it is queued, and queue_status shows how it went. Jobs: " +
    JOB_NAMES.map((name) => `${name} (${JOBS[name].does})`).join("; ") +
    ". Most run on their own schedule anyway; trigger one after a fix, not to be safe.",
  minRole: "editor",
  writes: "add",
  input: {
    job: z.enum(JOB_NAMES).describe("Which job"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(10000)
      .optional()
      .describe("import-ride-stats only: most rides to fetch"),
    sinceHours: z
      .number()
      .int()
      .min(1)
      .max(720)
      .optional()
      .describe(
        "publish-ride-profiles only: profiles curated in the last hours",
      ),
    days: z
      .number()
      .int()
      .min(1)
      .max(400)
      .optional()
      .describe("rebuild-downtime only: days back from now"),
    dryRun: z
      .boolean()
      .optional()
      .describe(
        "sync-efteling-maintenance only: compute the diff, write nothing",
      ),
  },
  async handler({ job, ...params }, ctx) {
    const trigger: Trigger = JOBS[job];
    const stray = strayParam(trigger, params);
    if (stray) throw new McpToolError(`${job} takes no ${stray}`);
    const body =
      trigger.param && params[trigger.param] !== undefined
        ? { [trigger.param]: params[trigger.param] }
        : null;
    const queued = await runAdminRoute(
      ctx,
      { method: "POST", path: trigger.path, body },
      () => trigger.run(ctx, params),
    );
    return { job, ...(queued as object) };
  },
});

interface MaintenanceInput {
  fromDate?: string;
  toDate?: string;
  parkId?: string;
  dryRun: boolean;
}

interface Maintenance {
  does: string;
  path: string;
  /** The body the admin sends, built from the input. */
  body?(input: MaintenanceInput): Record<string, unknown>;
  run(ctx: McpToolContext, body: Record<string, unknown>): Promise<unknown>;
}

const MAINTENANCE = {
  "flush-cache": {
    does: "deletes the park cache in Redis; queues and sessions stay, the pages refill on their next read",
    path: "flush-cache",
    run: (ctx) => admin(ctx).flushCache(),
  },
  "cache-reset": {
    does: "the same flush, then a full rebuild: holidays, parks, rides, wait times",
    path: "cache/reset",
    run: (ctx) => admin(ctx).resetCache("true"),
  },
  "dedupe-percentile-aggregates": {
    does: "removes duplicate hourly percentile rows, keeping the newest; may decompress old chunks",
    path: "dedupe-percentile-aggregates",
    run: (ctx) => admin(ctx).triggerDedupePercentileAggregates(),
  },
  "backfill-park-day-operation": {
    does: "judges past days for measured operation, fromDate to toDate, optionally one park",
    path: "backfill-park-day-operation",
    body: ({ fromDate, toDate, parkId }) => ({
      fromDate,
      toDate,
      ...(parkId && { parkId: parkId.toLowerCase() }),
    }),
    run: (ctx, body) => admin(ctx).triggerParkDayOperationBackfill(body),
  },
  "validate-and-repair-parks": {
    does: "checks every park against Queue-Times and Wartezeiten.app; the dry run reports, false repairs the ids",
    path: "validate-and-repair-parks",
    body: ({ dryRun }) => ({ autoFix: !dryRun }),
    run: (ctx, body) => admin(ctx).validateAndRepairParks(body),
  },
} satisfies Record<string, Maintenance>;

type MaintenanceName = keyof typeof MAINTENANCE;
const MAINTENANCE_NAMES = Object.keys(MAINTENANCE) as [
  MaintenanceName,
  ...MaintenanceName[],
];

const DAY = z.string().regex(/^\d{4}-\d{2}-\d{2}$/, "YYYY-MM-DD");

export const runMaintenanceTool = defineMcpTool({
  name: "run_maintenance",
  title: "Run maintenance",
  description:
    "The owner's actions on the admin's actions page, which delete, rewrite or repair: " +
    MAINTENANCE_NAMES.map((name) => `${name} (${MAINTENANCE[name].does})`).join(
      "; ",
    ) +
    ". The dry run (default) says what would run and writes nothing; validate-and-repair-parks " +
    "runs its check and repairs nothing. None of them touches Cloudflare or the frontend's own " +
    "cache. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    op: z.enum(MAINTENANCE_NAMES).describe("Which action"),
    fromDate: DAY.optional().describe(
      "backfill-park-day-operation: first park-local day, YYYY-MM-DD",
    ),
    toDate: DAY.optional().describe(
      "backfill-park-day-operation: last park-local day, inclusive",
    ),
    parkId: uuidInput()
      .optional()
      .describe("backfill-park-day-operation: only this park"),
    dryRun: dryRunInput("what the action would do"),
  },
  async handler({ op, ...input }, ctx) {
    const action: Maintenance = MAINTENANCE[op];
    const backfill = op === "backfill-park-day-operation";
    if (!backfill && (input.fromDate || input.toDate || input.parkId)) {
      throw new McpToolError(`${op} takes no dates or park`);
    }
    if (backfill && !(input.fromDate && input.toDate)) {
      throw new McpToolError(`${op} needs fromDate and toDate`);
    }
    const body = action.body?.(input) ?? null;
    if (input.dryRun && op !== "validate-and-repair-parks") {
      return { dryRun: true, op, wouldRun: action.does, ...(body && { body }) };
    }
    const result = await runAdminRoute(
      ctx,
      { method: "POST", path: action.path, body },
      () => action.run(ctx, body ?? {}),
    );
    return { dryRun: input.dryRun, op, result };
  },
});
