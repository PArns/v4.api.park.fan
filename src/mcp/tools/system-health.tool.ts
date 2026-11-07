import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SystemHealthService } from "../../admin/system-health.service";

export const systemHealthTool = defineMcpTool({
  name: "system_health",
  title: "System health",
  description:
    "The admin health dashboard. `infrastructure`: host CPU/RAM/disk and temperatures (the CPU " +
    "package with `temperatureLimits`, amber and red counted down from the chip's TjMax), GPU, " +
    "Postgres (size, connections, slow queries), Redis, data freshness (minutes since the " +
    "newest wait-time row, rows in the last hour, newest weather day) and `siteAnalytics`: the site " +
    "statistic's flush backlog (windows waiting in Redis, the oldest one's age, windows set aside " +
    "after failed writes, the last write, the visits not yet closed) and today's and yesterday's " +
    "dropped or overflowed hits by " +
    "reason; `healthy` false means reports are falling behind. `ml`: CatBoost (ml-service) " +
    "and TFT (nf-service) training status and model quality, and the intraday PCN comparison " +
    "boards (their `verdict` and `count`; the per-day `rows` only with `boardRows`). A " +
    "component that failed to answer shows an `error` field instead.",
  input: {
    section: z
      .enum(["infrastructure", "ml", "all"])
      .default("infrastructure")
      .describe("Which half of the dashboard"),
    boardRows: z
      .number()
      .int()
      .min(0)
      .max(100)
      .default(0)
      .describe("Newest per-day rows to keep in each PCN board"),
  },
  async handler({ section, boardRows }, ctx) {
    const health = await ctx.get(SystemHealthService).getHealth();
    // The boards' 14 days of rows run past the MCP answer limit (131k chars
    // on 2026-11-07), which cut off the rest of the ML half.
    const boards = health.ml as Record<string, unknown> | undefined;
    for (const board of ["pcnComparison", "pcnLeadCurve"]) {
      const value = boards?.[board] as { rows?: unknown[] } | undefined;
      if (boards && Array.isArray(value?.rows)) {
        boards[board] = { ...value, rows: value.rows.slice(0, boardRows) };
      }
    }
    const { ml, timestamp, ...infrastructure } = health;
    if (section === "ml") return { timestamp, ml };
    if (section === "infrastructure") return infrastructure;
    return health;
  },
});
