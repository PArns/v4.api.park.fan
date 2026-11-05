import { z } from "zod";
import { capped, defineMcpTool, McpToolError } from "../mcp-tool";
import { isAmbiguous, resolvePark } from "./park-ref";
import {
  ACCURACY_RETENTION_DAYS,
  ForecastErrorBreakdownService,
  SCORED_MODELS,
  type ErrorRow,
} from "../../ml/services/forecast-error-breakdown.service";

/** Two windows of this length plus the baseline's 4 weeks fit the 90-day retention. */
const MAX_DAYS = 28;

/** Below this many scored slots a park's or ride's MAE is noise. */
const MIN_SLOTS = 30;

const RANKINGS = {
  mae: (row: ErrorRow) => row.recent.mae,
  vsBaseline: (row: ErrorRow) => row.recent.skill,
  change: (row: ErrorRow) => row.maeChange,
} as const;

export const mlAccuracyTool = defineMcpTool({
  name: "ml_accuracy",
  title: "Forecast accuracy",
  description:
    "Where the hourly wait forecasts miss. Per park (or, with `park`, per ride of that park) the " +
    "error against the actual waits in the last `days`: MAE in minutes, bias (forecast minus " +
    "actual, positive = too long), scored slots n, and the weekday-median baseline the ML " +
    "overview's drift watch uses (`baselineMae`; `skill` = MAE ÷ baseline MAE on the same slots, " +
    "below 1 beats it). Each row carries the `days` before for comparison and `maeChange`, so a " +
    "rise in a model's MAE (the totals, as in ml_overview's drift watch) can be traced to the " +
    "parks or rides behind it. Models: catboost (the raw hourly forecast), pcn and chronos (as " +
    "served, where they were). TFT's daily level is not scored per park here; ml_overview has " +
    "its comparison. Ranked by `mae` (default), `vsBaseline` (worst skill first) or `change` " +
    `(biggest rise first). Data reaches back ${ACCURACY_RETENTION_DAYS} days minus the ` +
    `baseline's 4 weeks, so two windows of up to ${MAX_DAYS} days; ` +
    `rows with fewer than ${MIN_SLOTS} slots are left out. Without park ` +
    "it reads about six weeks of scored slots and can take half a minute; results are cached for " +
    "30 minutes, and a timeout asks you to narrow it.",
  input: {
    days: z
      .number()
      .int()
      .min(1)
      .max(MAX_DAYS)
      .default(7)
      .describe(
        "Length of the window; the same length before it is the comparison",
      ),
    park: z
      .string()
      .min(2)
      .max(120)
      .optional()
      .describe(
        "Park id (uuid) from find_parks or an unambiguous slug: list its rides instead of parks",
      ),
    model: z
      .enum(SCORED_MODELS)
      .optional()
      .describe("Only this model; all of them when absent"),
    rankBy: z
      .enum(["mae", "vsBaseline", "change"])
      .default("mae")
      .describe(
        "mae: largest error first; vsBaseline: furthest above the baseline first; change: largest rise first",
      ),
    limit: z.number().int().min(1).max(50).default(15).describe("Rows to list"),
  },
  async handler({ days, park: reference, model, rankBy, limit }, ctx) {
    let park: { id: string; name: string } | undefined;
    if (reference) {
      const found = await resolvePark(ctx, reference);
      if (isAmbiguous(found)) return found;
      park = { id: found.id, name: found.name };
    }

    let breakdown;
    try {
      breakdown = await ctx
        .get(ForecastErrorBreakdownService)
        .breakdown({ days, parkId: park?.id, model });
    } catch (error) {
      if (/statement timeout/i.test(String((error as Error)?.message))) {
        throw new McpToolError(
          "The query ran out of time; pass `park` or a smaller `days`",
        );
      }
      throw error;
    }

    const score = RANKINGS[rankBy];
    const ranked = breakdown.rows
      .filter((row) => row.recent.n >= MIN_SLOTS && score(row) !== null)
      .sort((a, b) => (score(b) as number) - (score(a) as number));
    const rows = capped(ranked, limit);

    return {
      window: breakdown.window,
      days: breakdown.days,
      park,
      rankedBy: rankBy,
      minSlots: MIN_SLOTS,
      totals: breakdown.totals,
      [breakdown.grain === "park" ? "parks" : "rides"]: {
        total: rows.total,
        items: rows.items.map((row) => ({
          id: row.id,
          name: row.name,
          model: row.model,
          n: row.recent.n,
          mae: row.recent.mae,
          bias: row.recent.bias,
          baselineMae: row.recent.baselineMae,
          skill: row.recent.skill,
          previousN: row.previous.n,
          previousMae: row.previous.mae,
          maeChange: row.maeChange,
        })),
      },
    };
  },
});
