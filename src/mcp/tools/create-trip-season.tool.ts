import { defineMcpTool, uuidInput } from "../mcp-tool";
import { asToolRefusal, routeBody } from "../mcp-write";
import { SeasonWriteDto } from "../../admin/content/dto/curation.dto";
import type { SeasonInput } from "../../parks/services/park-season.service";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import {
  seasonCreateFields,
  seasonInputOf,
  seasonView,
} from "./trip-season.inputs";

export const createTripSeasonTool = defineMcpTool({
  name: "create_trip_season",
  title: "Add a park season",
  description:
    "Adds one season or event to a park, as an editor does on the admin's Seasons page: checked " +
    "like the admin's form (a real range, dates inside it, a price with its currency, http(s) " +
    "links, rides of that park), written to the audit log under this account, and published to " +
    "the park page and the trip planner. Answers the season with its id. Check list_trip_seasons " +
    "first: a season of the same park, kind and name over the same days is a duplicate. For " +
    "several researched rows at once use import_trip_seasons.",
  minRole: "editor",
  writes: "add",
  input: {
    park: uuidInput().describe("The park's id; find_parks has it"),
    ...seasonCreateFields,
  },
  async handler({ park, ...fields }, ctx) {
    const body = await routeBody(SeasonWriteDto, seasonInputOf(fields));
    const season = await asToolRefusal(() =>
      ctx
        .get(AdminSeasonService)
        .create(park.toLowerCase(), body as unknown as SeasonInput, ctx.admin),
    );
    return { created: seasonView(season) };
  },
});
