import { defineMcpTool } from "../mcp-tool";
import { MlOverviewService } from "../../admin/ml-overview.service";

export const mlOverviewTool = defineMcpTool({
  name: "ml_overview",
  title: "Forecast models",
  description:
    "The admin's ML overview: every forecast component (CatBoost, TFT, PCN, Chronos …) with what " +
    "it delivers, whether it serves or shadows, its last run, expected cadence and status " +
    "(ok/late/failed/warming); the open model decisions with their progress and verdicts; the " +
    "model comparison summary (better/worse/unclear/pending cells over 28 days); the hourly band " +
    "calibration and the daily drift watch. Some labels are in German.",
  input: {},
  async handler(_input, ctx) {
    return ctx.get(MlOverviewService).getOverview();
  },
});
