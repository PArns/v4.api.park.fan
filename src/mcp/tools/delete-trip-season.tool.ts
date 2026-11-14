import { defineMcpTool, uuidInput } from "../mcp-tool";
import { asToolRefusal, confirmInput } from "../mcp-write";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import { seasonView } from "./trip-season.inputs";

export const deleteTripSeasonTool = defineMcpTool({
  name: "delete_trip_season",
  title: "Delete a park season",
  description:
    "Deletes one season for good, as the admin's delete does: the audit log keeps the whole row " +
    "under this account, enough to type it back in, and the park is published without it. " +
    "Answers the row that was deleted. A season that is only wrong is changed with " +
    "update_trip_season instead; one that will not happen is status cancelled.",
  minRole: "editor",
  writes: "delete",
  input: {
    id: uuidInput().describe("The season's id, from list_trip_seasons"),
    confirm: confirmInput("deletes the season"),
  },
  async handler({ id }, ctx) {
    const season = await asToolRefusal(() =>
      ctx.get(AdminSeasonService).remove(id, ctx.admin),
    );
    return { deleted: seasonView(season) };
  },
});
