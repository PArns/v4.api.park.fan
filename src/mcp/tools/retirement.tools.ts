import { z } from "zod";
import { capped, defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import { asToolRefusal, dryRunInput, runAdminRoute } from "../admin-route";
import { AdminController } from "../../admin/admin.controller";
import { AdminCurationService } from "../../admin/content/admin-curation.service";
import {
  AttractionRetirementService,
  isOnParkPage,
  retiredKindOf,
} from "../../attractions/services/attraction-retirement.service";
import { isSeasonalRow } from "../../queues/processors/children-metadata.processor";
import { formatInParkTimezone } from "../../common/utils/date.util";

/**
 * Retirement as the admin does it, one ride or park at a time with a date and
 * a source, its undo, and the review mark that answers a candidate with "not
 * a case". A retirement takes rows out of every list and the sitemap, so it is
 * shown before it is written (docs/rules/automated-retirement-and-identity.md).
 */

const DAY = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/, "YYYY-MM-DD")
  .describe("The last operating day the source states, YYYY-MM-DD");

const RETIRE_REASON = z
  .string()
  .trim()
  .min(10)
  .max(1000)
  .describe(
    "Why, with the URL it was established from; visitors read it on the ride's page",
  );

/** Above this many rides a retirement is a mass write: the dry run says so. */
const MASS_RETIREMENT = 10;

export const retiredAttractionsTool = defineMcpTool({
  name: "retired_attractions",
  title: "Retired rides",
  description:
    "Rides currently retired, newest first, as the admin's retirement page lists them: park, " +
    "when, why, kind (closed or reclassified, a row that was never a ride of its own), whether " +
    "it is hidden from its park page and whether the park page still lists it. The ids are what " +
    "unretire_attraction and set_retired_hidden take.",
  input: {
    parkId: uuidInput().optional().describe("Only this park's rides"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(200)
      .default(50)
      .describe("Most rides to list"),
  },
  async handler({ parkId, limit }, ctx) {
    const rows = await ctx.get(AttractionRetirementService).listRetired();
    const wanted = parkId?.toLowerCase();
    return capped(
      rows
        .filter((row) => !wanted || row.parkId === wanted)
        .map((row) => ({
          attractionId: row.id,
          name: row.name,
          parkId: row.parkId,
          park: row.park?.name ?? null,
          retiredAt: row.retiredAt
            ? formatInParkTimezone(row.retiredAt, row.park?.timezone ?? "UTC")
            : null,
          reason: row.retiredReason,
          kind: retiredKindOf(row),
          hidden: row.retiredHidden,
          onParkPage: isOnParkPage(row),
        })),
      limit,
    );
  },
});

export const retireAttractionsTool = defineMcpTool({
  name: "retire_attractions",
  title: "Retire rides",
  description:
    "Retires rides that closed for good, as the admin's retire button does: each with its last " +
    "day and a reason with the source. The rides leave every list and the sitemap; their pages " +
    "show them as closed. The dry run (default) lists each ride with its park, whether it is " +
    "already retired, and whether its season is known: a seasonal ride missing from the feed is " +
    "out of season, not gone, and is answered with curate_park_attractions " +
    "(curatedIsSeasonal) instead. More than 10 rides is a mass write: count and read them " +
    "first. Owner only; unretire_attraction takes one back. Two rows of one ride are merged " +
    "(merge_attractions), never retired side by side.",
  minRole: "owner",
  writes: "delete",
  input: {
    retirements: z
      .array(
        z.object({
          attractionId: uuidInput().describe("Ride id"),
          retiredAt: DAY,
          reason: RETIRE_REASON,
          kind: z
            .enum(["closed", "reclassified"])
            .optional()
            .describe(
              "closed (default) for a ride that is gone; reclassified for a row that was never " +
                "a ride of its own, which the sync never lifts",
            ),
        }),
      )
      .min(1)
      .max(50)
      .describe("One entry per ride"),
    dryRun: dryRunInput("each ride as it stands now, with the counts"),
  },
  async handler({ retirements, dryRun }, ctx) {
    const requests = retirements.map((entry) => ({
      ...entry,
      attractionId: entry.attractionId.toLowerCase(),
    }));
    const ids = requests.map((entry) => entry.attractionId);
    if (new Set(ids).size !== ids.length) {
      throw new McpToolError("A ride is listed twice");
    }
    if (dryRun) {
      const curation = ctx.get(AdminCurationService);
      const rides = await Promise.all(
        requests.map(async (request) => {
          const ride = await curation
            .findAttraction(request.attractionId)
            .catch(() => null);
          if (!ride)
            return { attractionId: request.attractionId, found: false };
          return {
            attractionId: ride.id,
            found: true,
            name: ride.name,
            park: ride.park?.name ?? null,
            retiredAt: request.retiredAt,
            kind: request.kind ?? "closed",
            alreadyRetired: ride.retiredAt
              ? formatInParkTimezone(
                  ride.retiredAt,
                  ride.park?.timezone ?? "UTC",
                )
              : null,
            seasonKnown: isSeasonalRow(ride),
            lastListedAt: ride.lastListedAt,
            absentSince: ride.absentSince,
          };
        }),
      );
      const found = rides.filter((ride) => ride.found);
      return {
        dryRun: true,
        counts: {
          asked: rides.length,
          notFound: rides.length - found.length,
          alreadyRetired: found.filter(
            (ride) => "alreadyRetired" in ride && ride.alreadyRetired,
          ).length,
          seasonKnown: found.filter(
            (ride) => "seasonKnown" in ride && ride.seasonKnown,
          ).length,
        },
        ...(rides.length > MASS_RETIREMENT && {
          warning:
            `${rides.length} rides at once is a mass write: read each by name, split them by ` +
            "kind (seasonal, a live twin, never had a reading) and retire only those that are gone",
        }),
        rides,
      };
    }
    const body = { retirements: requests };
    return runAdminRoute(
      ctx,
      { method: "POST", path: "retire-attractions", body },
      () => ctx.get(AdminController).retireAttractions(body),
    );
  },
});

export const unretireAttractionTool = defineMcpTool({
  name: "unretire_attraction",
  title: "Undo a retirement",
  description:
    "Takes a ride's retirement back, as the admin's restore button does: it counts as active " +
    "again and its absence runs from now, so the sync does not retire it again at once. Owner " +
    "only, like the admin route.",
  minRole: "owner",
  writes: "change",
  input: {
    attractionId: uuidInput().describe("Ride id from retired_attractions"),
  },
  async handler({ attractionId }, ctx) {
    const id = attractionId.toLowerCase();
    const { restored } = await runAdminRoute(
      ctx,
      { method: "POST", path: `unretire-attraction/${id}` },
      () => ctx.get(AdminController).unretireAttraction(id),
    );
    if (!restored) throw new McpToolError(`Ride ${id} is not retired`);
    return { attractionId: id, restored };
  },
});

export const setRetiredHiddenTool = defineMcpTool({
  name: "set_retired_hidden",
  title: "Hide a closed ride",
  description:
    "Takes a closed ride off its park page's list of closed rides, or puts it back, as the " +
    "admin's retirement page does. The ride's own page and its sitemap entry stay. Only for a " +
    "retired ride. Owner only, like the admin route.",
  minRole: "owner",
  writes: "change",
  input: {
    attractionId: uuidInput().describe("Ride id from retired_attractions"),
    hidden: z.boolean().describe("True hides it, false shows it again"),
  },
  async handler({ attractionId, hidden }, ctx) {
    const id = attractionId.toLowerCase();
    const body = { hidden };
    const { updated } = await runAdminRoute(
      ctx,
      { method: "POST", path: `retired-attractions/${id}/hidden`, body },
      () => ctx.get(AdminController).setRetiredHidden(id, body),
    );
    if (!updated) throw new McpToolError(`Ride ${id} is not retired`);
    return { attractionId: id, hidden };
  },
});

export const retireParkTool = defineMcpTool({
  name: "retire_park",
  title: "Retire a park",
  description:
    "Retires a park that closed for good, as the admin does: the park keeps its page with the " +
    "date and reason, leaves every listing, search and job, and its rides still active are " +
    "retired with it. The dry run (default) names the park and counts those rides. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    parkId: uuidInput().describe("Park id from find_parks"),
    retiredAt: DAY,
    reason: RETIRE_REASON,
    dryRun: dryRunInput(
      "the park and how many active rides would retire with it",
    ),
  },
  async handler({ parkId, retiredAt, reason, dryRun }, ctx) {
    const id = parkId.toLowerCase();
    const curation = ctx.get(AdminCurationService);
    const park = await asToolRefusal(() => curation.findPark(id));
    if (dryRun) {
      return {
        dryRun: true,
        parkId: park.id,
        name: park.name,
        alreadyRetired: park.retiredAt
          ? formatInParkTimezone(park.retiredAt, park.timezone)
          : null,
        activeRides: (await curation.attractionIdsOf(park.id)).length,
        retiredAt,
      };
    }
    const body = { retirements: [{ parkId: park.id, retiredAt, reason }] };
    return runAdminRoute(
      ctx,
      { method: "POST", path: "retire-parks", body },
      () => ctx.get(AdminController).retireParks(body),
    );
  },
});

export const reviewMarkTool = defineMcpTool({
  name: "review_mark",
  title: "Mark a candidate as no case",
  description:
    "Answers a candidate with 'checked, not a case', as the admin's dismiss buttons do, so the " +
    "detector stops asking: not_retired for a retirement candidate that still runs, " +
    "not_a_duplicate for a duplicate or re-issue pair that are two rides (permanent), " +
    "not_a_maze for a ride filed as a Halloween maze that is a scare zone. Give a not_retired " +
    "mark a recheckAfter: a permanent one hides the ride's real retirement forever.",
  minRole: "editor",
  writes: "add",
  input: {
    kind: z
      .enum(["not_retired", "not_a_duplicate", "not_a_maze"])
      .describe("Which question the mark answers"),
    attractionId: uuidInput().describe("The candidate ride"),
    otherAttractionId: uuidInput()
      .optional()
      .describe("not_a_duplicate only: the other ride of the pair"),
    reason: z
      .string()
      .trim()
      .min(10)
      .max(1000)
      .describe("Why it is not a case; the next person asked reads it"),
    recheckAfter: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, "YYYY-MM-DD")
      .optional()
      .describe("Ask again from this day; absent makes the mark permanent"),
  },
  async handler(
    { kind, attractionId, otherAttractionId, reason, recheckAfter },
    ctx,
  ) {
    if ((kind === "not_a_duplicate") !== Boolean(otherAttractionId)) {
      throw new McpToolError(
        "otherAttractionId belongs to not_a_duplicate, and only there",
      );
    }
    const body = {
      marks: [
        {
          kind,
          attractionId: attractionId.toLowerCase(),
          otherAttractionId: otherAttractionId?.toLowerCase() ?? null,
          reason,
          recheckAfter: recheckAfter ?? null,
        },
      ],
    };
    return runAdminRoute(
      ctx,
      { method: "POST", path: "review-marks", body },
      () => ctx.get(AdminController).recordReviewMarks(body),
    );
  },
});
