import { z } from "zod";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import {
  DAY_ORIGINS,
  ModelComparisonService,
} from "../../parks/services/model-comparison.service";
import type { ModelComparisonOriginId } from "../../ml/dto/model-comparison.dto";
import { ISO_DATE_PATTERN } from "../../common/utils/date.util";

const REGIONS = ["ALL", "EU", "NA", "ASIA", "OTHER"] as const;
const GROUPS = ["live", "today", "planner", "calendar"] as const;

export const modelComparisonTool = defineMcpTool({
  name: "model_comparison",
  title: "Model comparison",
  description:
    "The admin's model comparison matrix: for every question the app answers (`questions`, grouped " +
    "live / today / planner / calendar) and every model (`models`), does the model beat what is " +
    "served today? Each cell has a `state` (today = what serves, better, worse, unclear, pending), " +
    "a German `note`, the three switch `checks` (offline, live, visible), the `live` paired " +
    "difference (model − served in minutes, negative = better, 95 % park-cluster CI, days, " +
    "parks), the `offline` verdict and `expectedBy` for a pending one. A switch needs 14 days and 10 " +
    "parks live. Filter with `group` or `question` (an id from a previous answer); the per-lead " +
    "breakdown (`live.leads`) is only returned for a single question. ml_overview has the summary " +
    "counts only.",
  input: {
    days: z
      .number()
      .int()
      .min(1)
      .max(90)
      .default(28)
      .describe("Scored target days pooled"),
    region: z.enum(REGIONS).default("ALL").describe("Parks of one region"),
    group: z.enum(GROUPS).optional().describe("Only this group's questions"),
    question: z
      .string()
      .max(60)
      .optional()
      .describe("One question id, e.g. live_h0_1, with its per-lead rows"),
  },
  async handler({ days, region, group, question }, ctx) {
    const matrix = await ctx
      .get(ModelComparisonService)
      .getMatrix(days, region);
    let questions = matrix.questions;
    if (group) questions = questions.filter((q) => q.group === group);
    if (question) {
      questions = questions.filter((q) => q.id === question);
      if (questions.length === 0) {
        throw new McpToolError(
          `No question "${question}"; known: ${matrix.questions.map((q) => q.id).join(", ")}`,
        );
      }
    } else {
      // The per-lead rows are a quarter of the matrix; one question at a time.
      questions = questions.map((q) => ({
        ...q,
        cells: Object.fromEntries(
          Object.entries(q.cells).map(([model, cell]) => [
            model,
            cell.live
              ? { ...cell, live: { ...cell.live, leads: undefined } }
              : cell,
          ]),
        ),
      }));
    }
    return { ...matrix, questions };
  },
});

export const modelComparisonDayTool = defineMcpTool({
  name: "model_comparison_day",
  title: "Model comparison: one ride-day",
  description:
    "One real day of the model comparison: every archived forecast curve of one ride from one " +
    "origin (morning of the day, or 1, 3 or 7 days before) against the 15-minute actual waits, " +
    "park-local `slots`, `truth`, each model's `series` and its MAE in `scores`. Without " +
    "`park`, `ride` and `date` all set it answers which dates, parks and rides have archived " +
    "curves (for `date`, or the newest day) and a `suggested` ride-day to start from.",
  input: {
    date: z
      .string()
      .regex(ISO_DATE_PATTERN, "must be YYYY-MM-DD")
      .optional()
      .describe("Park-local day, YYYY-MM-DD"),
    park: uuidInput().optional().describe("Park id"),
    ride: uuidInput().optional().describe("Attraction id"),
    origin: z
      .enum(
        DAY_ORIGINS.map((o) => o.id) as [
          ModelComparisonOriginId,
          ...ModelComparisonOriginId[],
        ],
      )
      .default("morning")
      .describe("When the curves were made: morning, d1, d3 or d7"),
  },
  async handler({ date, park, ride, origin }, ctx) {
    const service = ctx.get(ModelComparisonService);
    if (!date || !park || !ride) return service.getDayOptions(date);
    return service.getDay(park.toLowerCase(), date, ride.toLowerCase(), origin);
  },
});
