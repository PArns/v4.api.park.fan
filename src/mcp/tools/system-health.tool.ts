import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { SystemHealthService } from "../../admin/system-health.service";

export const systemHealthTool = defineMcpTool({
  name: "system_health",
  title: "System health",
  description:
    "The admin health dashboard. `infrastructure`: host CPU/RAM/disk and temperatures, GPU, " +
    "Postgres (size, connections, slow queries), Redis, and data freshness (minutes since the " +
    "newest wait-time row, rows in the last hour, newest weather day). `ml`: CatBoost (ml-service) " +
    "and TFT (nf-service) training status and model quality, and the intraday PCN comparison " +
    "boards. A component that failed to answer shows an `error` field instead.",
  input: {
    section: z
      .enum(["infrastructure", "ml", "all"])
      .default("infrastructure")
      .describe("Which half of the dashboard"),
  },
  async handler({ section }, ctx) {
    const health = await ctx.get(SystemHealthService).getHealth();
    const { ml, timestamp, ...infrastructure } = health;
    if (section === "ml") return { timestamp, ml };
    if (section === "infrastructure") return infrastructure;
    return health;
  },
});
