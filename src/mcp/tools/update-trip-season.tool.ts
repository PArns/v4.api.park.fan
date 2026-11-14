import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { asToolRefusal, routeBody } from "../mcp-write";
import { SeasonPatchDto } from "../../admin/content/dto/curation.dto";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import {
  seasonInputOf,
  seasonPatchFields,
  seasonView,
} from "./trip-season.inputs";

export const updateTripSeasonTool = defineMcpTool({
  name: "update_trip_season",
  title: "Change a park season",
  description:
    "Changes one season, as the admin's edit form does: only the fields given change, null " +
    "clears an optional one (name, dates, price, hours, links, note), and the result is checked " +
    "whole like a new season. Written to the audit log with the range, status and dates before " +
    "and after, and published. Answers the season as it now is. The id comes from " +
    "list_trip_seasons.",
  minRole: "editor",
  writes: "change",
  input: {
    id: uuidInput().describe("The season's id"),
    ...seasonPatchFields,
  },
  async handler({ id, ...fields }, ctx) {
    const patch = seasonInputOf(fields);
    if (Object.keys(patch).length === 0) {
      throw new McpToolError("Name at least one field to change");
    }
    const body = await routeBody(SeasonPatchDto, patch);
    const season = await asToolRefusal(() =>
      ctx.get(AdminSeasonService).update(id, body, ctx.admin),
    );
    return { updated: seasonView(season) };
  },
});
