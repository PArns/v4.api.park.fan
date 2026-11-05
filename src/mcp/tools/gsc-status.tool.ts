import { defineMcpTool } from "../mcp-tool";
import { SearchConsoleService } from "../../search-console/search-console.service";
import { GSC_DATA_DESCRIPTION, withGscData } from "./gsc-data";

export const gscStatusTool = defineMcpTool({
  name: "gsc_status",
  title: "Search Console status",
  description:
    "Whether Google Search Console is connected to park.fan's admin, the property it reads " +
    "(siteUrl), when the nightly import last succeeded (lastImportAt), when the run in progress " +
    "started (runningSince; null when none is), the first and last day " +
    "with data (coverage; null before the first import) and the rows held. lastError says why " +
    "the last import failed or why the connection is off. Call it first: when configured is " +
    "false the other gsc_ tools have nothing to answer with, and coverage.to is the newest " +
    "day any figure can reach (Google's last two or three days are not final). " +
    GSC_DATA_DESCRIPTION,
  input: {},
  async handler(_input, ctx) {
    const status = await ctx.get(SearchConsoleService).status();
    const { lastError, ...rest } = status;
    return withGscData(rest, { lastError });
  },
});
