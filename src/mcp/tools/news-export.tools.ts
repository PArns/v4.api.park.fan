import { z } from "zod";
import { capped, defineMcpTool, McpToolError } from "../mcp-tool";
import { dryRunInput } from "../mcp-write";
import { AdminAuditService } from "../../admin/auth/admin-audit.service";
import {
  callSiteRoute,
  SITE_ROUTE_NOT_CONFIGURED,
  siteRouteTarget,
} from "./site-route";

/**
 * The native app's news and guides export on media.park.fan, which a GitHub
 * workflow in the website's repository writes. The website compares it with
 * its own posts and starts the workflow; these tools ask the website, the only
 * side that knows both (park.fan's `docs/features/media-database.md`).
 */

const STATUS_PATH = "/api/news-export/status";
const RUN_PATH = "/api/news-export/run";

/** The website's workflow, by the name its page on GitHub carries. */
const WORKFLOW = "publish-media.yml";

/** The website's `HealthTone`, which its admin tile shows. */
const HEALTH_TONES = ["ok", "warn", "bad", "unknown"] as const;

const timestamp = z
  .string()
  .max(40)
  .refine((value) => Number.isFinite(Date.parse(value)), "a timestamp");

const postRef = z.object({
  slug: z.string().max(300),
  title: z.string().max(500),
  publishedAt: z.string().max(40),
});

const workflowUrl = z.url({ protocol: /^https$/ }).max(500);

/**
 * The website's `NewsExportReport`, checked before a field of it reaches the
 * model: types, bounded strings and lists, a known tone. A field the site adds
 * later is dropped, one it renames fails the check.
 */
const newsExportReport = z.object({
  checkedAt: timestamp,
  deployedAt: timestamp.nullable(),
  workflowUrl,
  feeds: z
    .array(
      z.object({
        feed: z.string().min(1).max(40),
        tone: z.enum(HEALTH_TONES),
        generatedAt: timestamp.nullable(),
        newestExported: postRef.nullable(),
        newestOnSite: postRef.nullable(),
        missing: z
          .array(
            z.object({
              translationKey: z.string().max(300),
              title: z.string().max(500),
              publishedAt: z.string().max(40),
              locales: z.array(z.string().max(10)).max(20),
            }),
          )
          .max(2000),
        behindMinutes: z.number().nonnegative().nullable(),
        unreadable: z
          .array(
            z.object({
              locale: z.string().max(10),
              message: z.string().max(300),
            }),
          )
          .max(50),
      }),
    )
    .max(10),
});

export const newsExportStatusTool = defineMcpTool({
  name: "news_export_status",
  title: "App news export status",
  description:
    "Whether the native app's news and guides export on media.park.fan has caught up with the " +
    "website, as the admin's news-export tile shows it. Per feed (news, guides): the tone the " +
    "tile shows (ok, warn, bad, unknown), when the export last ran (generatedAt), the newest " +
    "post in the export and on the site, the posts the site lists and the export does not " +
    "(missing, newest first, with the locales they are missing in), how many minutes the " +
    "longest-missing one has waited (behindMinutes) and the locales whose list could not be " +
    "read. Asks park.fan's /api/news-export/status, which keeps a list it read for a few " +
    "minutes; checkedAt says when. run_news_export starts a new export.",
  input: {
    limit: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(10)
      .describe("Missing posts listed per feed"),
  },
  async handler({ limit }) {
    if (!siteRouteTarget()) {
      return { configured: false, message: SITE_ROUTE_NOT_CONFIGURED };
    }
    const answer = await callSiteRoute("GET", STATUS_PATH);
    if (answer.status !== 200) {
      throw new McpToolError(
        `park.fan answered ${STATUS_PATH} with HTTP ${answer.status}`,
      );
    }
    const report = newsExportReport.safeParse(answer.body);
    if (!report.success) {
      throw new McpToolError(
        `park.fan answered ${STATUS_PATH} with a report this tool cannot read`,
      );
    }
    const { checkedAt, deployedAt, feeds } = report.data;
    return {
      configured: true,
      checkedAt,
      deployedAt,
      workflowUrl: report.data.workflowUrl,
      feeds: feeds.map((feed) => ({
        feed: feed.feed,
        tone: feed.tone,
        generatedAt: feed.generatedAt,
        behindMinutes:
          feed.behindMinutes === null ? null : Math.round(feed.behindMinutes),
        newestExported: feed.newestExported,
        newestOnSite: feed.newestOnSite,
        missing: capped(feed.missing, limit),
        unreadable: feed.unreadable,
      })),
    };
  },
});

/**
 * What the website answers a run: `{ started: true, workflowUrl }`, or
 * `{ started: false, reason }` with a status other than 2xx. Each field is
 * read on its own, so a page link that fails the check never hides that a
 * run started.
 */
function runAnswerOf(body: unknown) {
  const record =
    body && typeof body === "object" ? (body as Record<string, unknown>) : {};
  return {
    started: record.started === true,
    workflowUrl: workflowUrl.safeParse(record.workflowUrl).data ?? null,
    reason: z.string().max(500).safeParse(record.reason).data ?? null,
  };
}

export const runNewsExportTool = defineMcpTool({
  name: "run_news_export",
  title: "Run the app news export",
  description:
    `Starts the GitHub workflow ${WORKFLOW} in park.fan's repository, which builds the native ` +
    "app's news and guides export and uploads it to media.park.fan; a run takes about 20 " +
    "minutes. Through park.fan's /api/news-export/run. Use it when news_export_status lists " +
    "posts missing that a run should bring, not to be safe: every call starts another run. The " +
    "dry run (default) says what would start and starts nothing. Answers the workflow's page on " +
    "GitHub; news_export_status shows when the export has caught up.",
  minRole: "editor",
  writes: "add",
  input: {
    dryRun: dryRunInput("what would start"),
  },
  async handler({ dryRun }, ctx) {
    const target = siteRouteTarget();
    if (!target) throw new McpToolError(SITE_ROUTE_NOT_CONFIGURED);
    if (dryRun) {
      return {
        dryRun: true,
        wouldStart: WORKFLOW,
        via: `POST ${target.origin}${RUN_PATH}`,
        takesMinutes: 20,
      };
    }
    const answer = await callSiteRoute("POST", RUN_PATH);
    const run = runAnswerOf(answer.body);
    const accepted = answer.status >= 200 && answer.status < 300;
    if (accepted && !run.started) {
      throw new McpToolError(
        `park.fan answered HTTP ${answer.status} without saying ${WORKFLOW} started; ` +
          "look at its runs on GitHub before starting another",
      );
    }
    if (!accepted) {
      throw new McpToolError(
        `park.fan did not start ${WORKFLOW} (HTTP ${answer.status})` +
          (run.reason ? `: ${run.reason}` : ""),
      );
    }
    await ctx.get(AdminAuditService).record({
      actor: ctx.admin,
      action: "news-export.run",
      entityType: "system",
      entityId: null,
      entityLabel: WORKFLOW,
      after: { workflowUrl: run.workflowUrl },
    });
    return {
      dryRun: false,
      started: true,
      workflowUrl: run.workflowUrl,
      takesMinutes: 20,
    };
  },
});
