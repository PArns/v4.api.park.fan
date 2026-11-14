import { z } from "zod";
import { DataSource } from "typeorm";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { asToolRefusal, dryRunInput, routeBody } from "../mcp-write";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { SeasonPatchDto } from "../../admin/content/dto/curation.dto";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import {
  PARK_SEASON_BINDING_KINDS,
  type ParkSeason,
} from "../../parks/entities/park-season.entity";
import type { SeasonBinding } from "../../common/types/season-binding.type";

/**
 * How a season binds a ride: `only_during`, the ride runs only on the
 * season's days (`attractionIds`); `not_during`, it does not run on them and
 * runs the rest of the year (`pausedAttractionIds`).
 */
export const RIDE_SEASON_MODES = ["only_during", "not_during"] as const;
export type RideSeasonMode = (typeof RIDE_SEASON_MODES)[number];

/** The season's list a mode edits, and the other one. */
const LIST: Record<
  RideSeasonMode,
  {
    field: "attractionIds" | "pausedAttractionIds";
    other: "attractionIds" | "pausedAttractionIds";
  }
> = {
  only_during: { field: "attractionIds", other: "pausedAttractionIds" },
  not_during: { field: "pausedAttractionIds", other: "attractionIds" },
};

/** The ride as the binding checks read it. */
export interface BindingRide {
  id: string;
  parkId: string;
  curatedIsSeasonal: boolean | null;
  curatedSeasonMonths: number[] | null;
}

/** One season the call names, and what the call does to the mode's list. */
export interface RideSeasonChange {
  season: Pick<
    ParkSeason,
    "id" | "kind" | "name" | "status" | "startDate" | "endDate" | "dates"
  >;
  action: "bind" | "unbind";
  /** False where the list already says so: nothing is written for it. */
  changes: boolean;
  /** The mode's list with the call applied. */
  ids: string[];
}

const BINDING_KINDS: readonly string[] = PARK_SEASON_BINDING_KINDS;

/**
 * Why a season may not be bound or unbound for this ride, or null. A season
 * of another park would name a ride its page does not have; a kind outside
 * `PARK_SEASON_BINDING_KINDS` (a closure, a maintenance window) binds and
 * pauses nothing; a cancelled season binds nothing, though a ride may still
 * leave one; and a ride the season lists the other way is taken out of that
 * list first (unbind with the other mode), never in both.
 */
export function rideSeasonProblem(
  ride: Pick<BindingRide, "id" | "parkId">,
  season: Pick<
    ParkSeason,
    | "id"
    | "parkId"
    | "kind"
    | "status"
    | "attractionIds"
    | "pausedAttractionIds"
  >,
  action: "bind" | "unbind",
  mode: RideSeasonMode = "only_during",
): string | null {
  if (season.parkId !== ride.parkId) {
    return `Season ${season.id} belongs to another park`;
  }
  if (!BINDING_KINDS.includes(season.kind)) {
    return `Season ${season.id} is a ${season.kind} season, which binds no ride (${BINDING_KINDS.join(", ")} do)`;
  }
  if (action === "bind" && season.status === "cancelled") {
    return `Season ${season.id} is cancelled and binds nothing`;
  }
  const other = mode === "only_during" ? "not_during" : "only_during";
  if (
    action === "bind" &&
    (season[LIST[mode].other] ?? []).includes(ride.id.toLowerCase())
  ) {
    return `Season ${season.id} already lists the ride as ${other}; unbind it with mode ${other} first`;
  }
  return null;
}

/** The mode's list of the season with the ride added or taken out, lowercase as stored. */
export function rideSeasonChange(
  ride: Pick<BindingRide, "id">,
  season: RideSeasonChange["season"] &
    Pick<ParkSeason, "attractionIds" | "pausedAttractionIds">,
  action: "bind" | "unbind",
  mode: RideSeasonMode = "only_during",
): RideSeasonChange {
  const id = ride.id.toLowerCase();
  const before = season[LIST[mode].field] ?? [];
  const listed = before.includes(id);
  const ids =
    action === "bind"
      ? listed
        ? before
        : [...before, id]
      : before.filter((a) => a !== id);
  return {
    season: {
      id: season.id,
      kind: season.kind,
      name: season.name,
      status: season.status,
      startDate: season.startDate,
      endDate: season.endDate,
      dates: season.dates,
    },
    action,
    changes: action === "bind" ? !listed : listed,
    ids,
  };
}

/**
 * Why a binding or a pause would not decide the ride's season: its own
 * curated months or a curated `isSeasonal: false` take precedence over any
 * park season (`resolveCuratedFacts`,
 * docs/architecture/attraction-status/seasonality.md).
 */
export function rideBindingWarnings(ride: BindingRide): string[] {
  const warnings: string[] = [];
  if (ride.curatedSeasonMonths && ride.curatedSeasonMonths.length > 0) {
    warnings.push(
      `The ride has curated season months (${ride.curatedSeasonMonths.join(", ")}); they take precedence, so a binding has no effect until they are cleared (curate_attraction curatedSeasonMonths null)`,
    );
  }
  if (ride.curatedIsSeasonal === false) {
    warnings.push(
      "The ride is curated as not seasonal (curatedIsSeasonal false); that takes precedence, so a binding has no effect until it is cleared",
    );
  }
  return warnings;
}

/** A bound or pausing season as a tool shows it. */
export function bindingView(binding: SeasonBinding) {
  return {
    id: binding.id,
    kind: binding.kind,
    name: binding.name,
    status: binding.status,
    startDate: binding.startDate,
    endDate: binding.endDate,
    dates: binding.dates,
  };
}

export const bindRideSeasonsTool = defineMcpTool({
  name: "bind_ride_seasons",
  title: "Bind a ride to park seasons",
  description:
    "Binds one ride to its park's seasons, or unbinds it, from the ride's side, so a season's " +
    "list is never retyped. mode only_during (the default) edits the season's attractionIds: " +
    "the ride runs only on the season's days and reads out of season outside them (a maze " +
    "during Fright Nights, an ice rink during Wintertraum). mode not_during edits its " +
    "pausedAttractionIds: the ride does not run on the season's days and runs the rest of the " +
    "year (River Quest during Wintertraum). Each season must be of the ride's park and of a " +
    "kind that binds (halloween, christmas, winter, summer_nights, special_event); binding to a " +
    "cancelled season is refused, and so is a season that already lists the ride the other " +
    "way. dryRun (the default) answers what would change, the seasons after it and a warning " +
    "where the ride's own curated months or a curated isSeasonal false take precedence; false " +
    "writes each season through the admin's season edit: checked, in the audit log as " +
    "park.season.update, and published with the ride's caches. Season ids from " +
    "list_trip_seasons, the current ones from get_attraction (seasonBindings, seasonPauses).",
  minRole: "editor",
  writes: "change",
  input: {
    attraction: uuidInput().describe(
      "The ride's id, from find_attractions or get_park",
    ),
    bind: z
      .array(uuidInput())
      .max(20)
      .default([])
      .describe("Seasons to bind the ride to in this mode, by id"),
    unbind: z
      .array(uuidInput())
      .max(20)
      .default([])
      .describe("Seasons to take the ride out of in this mode, by id"),
    mode: z
      .enum(RIDE_SEASON_MODES)
      .default("only_during")
      .describe(
        "only_during: the ride runs only during the season (attractionIds); not_during: it " +
          "does not run during it (pausedAttractionIds)",
      ),
    dryRun: dryRunInput("what would change and the seasons after it"),
  },
  async handler({ attraction: attractionId, bind, unbind, mode, dryRun }, ctx) {
    const ids = (list: string[]) => [
      ...new Set(list.map((id) => id.toLowerCase())),
    ];
    const toBind = ids(bind);
    const toUnbind = ids(unbind);
    if (toBind.length + toUnbind.length === 0) {
      throw new McpToolError("Name a season in bind or unbind");
    }
    const both = toBind.filter((id) => toUnbind.includes(id));
    if (both.length > 0) {
      throw new McpToolError(
        `A season cannot be bound and unbound at once: ${both.join(", ")}`,
      );
    }

    // Without an explicit select the row carries `seasonBindings` and
    // `seasonPauses`, the seasons that list it (`season-window.sql.ts`).
    const ride = await ctx
      .get(DataSource)
      .getRepository(Attraction)
      .findOne({ where: { id: attractionId.toLowerCase() } });
    if (!ride) {
      throw new McpToolError(
        `No attraction ${attractionId}; try find_attractions`,
      );
    }

    const seasons = ctx.get(AdminSeasonService);
    const changes: RideSeasonChange[] = [];
    for (const [action, list] of [
      ["bind", toBind],
      ["unbind", toUnbind],
    ] as const) {
      for (const id of list) {
        const season = await asToolRefusal(() => seasons.findOne(id));
        const problem = rideSeasonProblem(ride, season, action, mode);
        if (problem) throw new McpToolError(problem);
        changes.push(rideSeasonChange(ride, season, action, mode));
      }
    }

    const current =
      mode === "only_during" ? ride.seasonBindings : ride.seasonPauses;
    const before = (current ?? []).map(bindingView);
    const unbound = new Set(
      changes.filter((c) => c.action === "unbind").map((c) => c.season.id),
    );
    const after = [
      ...before.filter((s) => !unbound.has(s.id)),
      ...changes
        .filter(
          (c) =>
            c.action === "bind" && !before.some((s) => s.id === c.season.id),
        )
        .map((c) => c.season),
    ].sort((a, b) => a.startDate.localeCompare(b.startDate));
    const warnings = rideBindingWarnings(ride);
    const shown = changes.map(({ season, action, changes: writes }) => ({
      season,
      action,
      changes: writes,
    }));

    if (dryRun) {
      return {
        dryRun: true,
        mode,
        attraction: { id: ride.id, name: ride.name },
        changes: shown,
        seasonsBefore: before,
        seasonsAfter: after,
        ...(warnings.length > 0 && { warnings }),
      };
    }

    const written: string[] = [];
    for (const change of changes.filter((c) => c.changes)) {
      const body = await routeBody(SeasonPatchDto, {
        [LIST[mode].field]: change.ids,
      });
      await asToolRefusal(() =>
        seasons.update(change.season.id, body, ctx.admin),
      );
      written.push(change.season.id);
    }
    return {
      dryRun: false,
      mode,
      attraction: { id: ride.id, name: ride.name },
      changes: shown,
      written,
      seasonsAfter: after,
      ...(warnings.length > 0 && { warnings }),
    };
  },
});
