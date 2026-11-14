import type { Type } from "@nestjs/common";
import { getQueueToken } from "@nestjs/bull";
import type { Queue } from "bull";
import { z } from "zod";
import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import {
  asToolRefusal,
  confirmInput,
  dryRunInput,
  routeBody,
} from "../mcp-write";
import { runAdminRoute } from "../admin-route";
import { AdminController } from "../../admin/admin.controller";
import { AdminAuditService } from "../../admin/auth/admin-audit.service";
import { AdminContentController } from "../../admin/content/admin-content.controller";
import { AdminCurationService } from "../../admin/content/admin-curation.service";
import { AdminRideProfileService } from "../../admin/content/admin-ride-profile.service";
import { AdminRedirectService } from "../../admin/content/admin-redirect.service";
import {
  BulkCurationDto,
  CurationPatchDto,
  RideProfileProvenanceDto,
  RideProfileWriteDto,
  VerifyFieldsDto,
} from "../../admin/content/dto/curation.dto";
import {
  CreateRedirectDto,
  REDIRECT_KINDS,
} from "../../admin/content/dto/redirect.dto";
import {
  ATTRACTION_CURATED_FIELDS,
  PARK_CURATED_FIELDS,
  attractionFieldViews,
  parkFieldViews,
  type CuratedFieldSpec,
  type CuratedFieldView,
} from "../../admin/content/curated-field.spec-list";

/**
 * The admin's park and ride editors: the curated columns beside the synced
 * ones, the check that changed nothing, the undo, the ride profile, a park's
 * location and the redirects from a dead path. Every write runs the content
 * route's handler, which writes the curated cell, its audit row with reason
 * and source, and publishes outside-in (docs/admin/curation.md).
 */

const REASON = z
  .string()
  .trim()
  .min(1)
  .max(2000)
  .describe(
    "Why, in a sentence: it goes into the audit row and is what makes the change reviewable",
  );

const SOURCE_URL = z
  .url({ protocol: /^https?$/ })
  .max(2000)
  .optional()
  .describe(
    "The page the value was established from, ideally the operator's own",
  );

const FIELDS = z
  .record(z.string().max(80), z.unknown())
  .refine((fields) => Object.keys(fields).length > 0, "name at least one field")
  .refine((fields) => Object.keys(fields).length <= 60, "at most 60 fields")
  .describe(
    "Curated fields keyed as curation_fields lists them (curatedName, curatedIsSeasonal, " +
      "curatedSeasonMonths …); null clears a correction and accepts the synced value again, a " +
      "key left out stays as it is",
  );

/** A field descriptor without what the model does not need to choose a value. */
function specView(spec: CuratedFieldSpec) {
  return {
    key: spec.key,
    label: spec.label,
    type: spec.type,
    group: spec.group,
    synced: spec.syncedKey !== null,
    ...(spec.options && { options: spec.options }),
    ...(spec.unit && { unit: spec.unit }),
    ...(spec.min !== undefined && { min: spec.min }),
    ...(spec.max !== undefined && { max: spec.max }),
    ...(spec.maxLength !== undefined && { maxLength: spec.maxLength }),
    ...(spec.hint && { hint: spec.hint }),
  };
}

/** One field as it stands on a row: synced, curated and the value served. */
function valueView(view: CuratedFieldView) {
  return {
    key: view.key,
    label: view.label,
    type: view.type,
    group: view.group,
    synced: view.syncedValue,
    curated: view.curatedValue,
    served: view.resolvedValue,
    overridden: view.overridden,
    ...(view.options && { options: view.options }),
    ...(view.unit && { unit: view.unit }),
    ...(view.min !== undefined && { min: view.min }),
    ...(view.max !== undefined && { max: view.max }),
    ...(view.hint && { hint: view.hint }),
  };
}

/** The fields a write changed, as they stand after it. */
function changedViews(views: CuratedFieldView[], changed: string[]) {
  const keys = new Set(changed);
  return views.filter((view) => keys.has(view.key)).map(valueView);
}

export const curationFieldsTool = defineMcpTool({
  name: "curation_fields",
  title: "Curated fields",
  description:
    "The fields the admin's park or ride editor writes, with type, options and bounds, and with " +
    "an id the values one row has: synced (what the source says), curated (what a person wrote) " +
    "and served (what the API shows). Read it before curate_park, curate_attraction or " +
    "curate_park_attractions, which take these keys. A ride's seasonal flag and months " +
    "(curatedIsSeasonal, curatedSeasonMonths) answer the data-quality 'season or gone?' list.",
  input: {
    kind: z.enum(["park", "attraction"]).describe("Park or ride fields"),
    id: uuidInput()
      .optional()
      .describe("A park or ride id for its values; absent lists the fields"),
    group: z
      .string()
      .max(80)
      .optional()
      .describe("Only the fields of this group, as the answer names them"),
    curatedOnly: z
      .boolean()
      .default(false)
      .describe("With an id: only the fields a person has written"),
  },
  async handler({ kind, id, group, curatedOnly }, ctx) {
    const inGroup = (field: { group: string }) =>
      !group || field.group === group;
    if (!id) {
      const specs =
        kind === "park" ? PARK_CURATED_FIELDS : ATTRACTION_CURATED_FIELDS;
      return { kind, fields: specs.filter(inGroup).map(specView) };
    }
    const curation = ctx.get(AdminCurationService);
    const views = await asToolRefusal(async () =>
      kind === "park"
        ? parkFieldViews(await curation.findPark(id.toLowerCase()))
        : attractionFieldViews(await curation.findAttraction(id.toLowerCase())),
    );
    return {
      kind,
      id: id.toLowerCase(),
      fields: views
        .filter(inGroup)
        .filter(
          (view) =>
            !curatedOnly ||
            (view.curatedValue !== null &&
              JSON.stringify(view.curatedValue) !==
                JSON.stringify(view.defaultValue)),
        )
        .map(valueView),
    };
  },
});

export const curateParkTool = defineMcpTool({
  name: "curate_park",
  title: "Curate a park",
  description:
    "Writes curated fields of one park, as the admin's park editor does: name, abbreviations, " +
    "address, links, fun facts and the rest curation_fields lists. Only the curated cell is " +
    "written, never the synced one; the park and its rides are published again (cache, " +
    "frontend, and once more after the CDN window). A value equal to the stored one writes " +
    "nothing. Answers the fields that changed and the audit id, which undo_curation takes back. " +
    "Names are the operator's own, never a translation.",
  minRole: "editor",
  writes: "change",
  input: {
    parkId: uuidInput().describe("Park id from find_parks"),
    fields: FIELDS,
    reason: REASON,
    sourceUrl: SOURCE_URL,
  },
  async handler({ parkId, fields, reason, sourceUrl }, ctx) {
    const id = parkId.toLowerCase();
    const body = await routeBody(CurationPatchDto, {
      fields,
      reason,
      sourceUrl,
    });
    const result = await runAdminRoute(
      ctx,
      { method: "PATCH", path: `content/parks/${id}`, selfAudited: true },
      () => ctx.get(AdminContentController).patchPark(id, body, ctx.admin),
    );
    return {
      changed: result.changed,
      auditId: result.auditId,
      fields: changedViews(result.fields, result.changed),
    };
  },
});

export const curateAttractionTool = defineMcpTool({
  name: "curate_attraction",
  title: "Curate a ride",
  description:
    "Writes curated fields of one ride, as the admin's ride editor does: name, minimum height, " +
    "gets you wet, single rider, fast pass, seasonal flag and months, works periods, land, kind " +
    "and the rest curation_fields lists. Only the curated cell is written; the park is published " +
    "again. A value equal to the stored one writes nothing. Answers the fields that changed and " +
    "the audit id, which undo_curation takes back. Before renaming, look for a second row of the " +
    "same ride in the park (find_attractions, duplicates): two rows are merged, not renamed.",
  minRole: "editor",
  writes: "change",
  input: {
    attractionId: uuidInput().describe("Ride id from find_attractions"),
    fields: FIELDS,
    reason: REASON,
    sourceUrl: SOURCE_URL,
  },
  async handler({ attractionId, fields, reason, sourceUrl }, ctx) {
    const id = attractionId.toLowerCase();
    const body = await routeBody(CurationPatchDto, {
      fields,
      reason,
      sourceUrl,
    });
    const result = await runAdminRoute(
      ctx,
      { method: "PATCH", path: `content/attractions/${id}`, selfAudited: true },
      () =>
        ctx.get(AdminContentController).patchAttraction(id, body, ctx.admin),
    );
    return {
      changed: result.changed,
      auditId: result.auditId,
      fields: changedViews(result.fields, result.changed),
    };
  },
});

export const curateParkAttractionsTool = defineMcpTool({
  name: "curate_park_attractions",
  title: "Curate several rides",
  description:
    "Writes curated fields on several rides of one park at once, as the admin's fast-pass table " +
    "and the data-quality 'season or gone?' answer do: { curatedIsSeasonal: true, " +
    "curatedSeasonMonths: [9, 10] } for a seasonal ride, { curatedIsSeasonal: false } for one " +
    "that is gone. Every entry is checked before any is written; each ride gets its own audit " +
    "row (undo_curation per ride) and the park is published once. Answers per ride the fields " +
    "that changed and the audit id.",
  minRole: "editor",
  writes: "change",
  input: {
    parkId: uuidInput().describe("The park every ride belongs to"),
    entries: z
      .array(
        z.object({
          id: uuidInput().describe("Ride id, in this park"),
          fields: FIELDS,
        }),
      )
      .min(1)
      .max(100)
      .describe("One entry per ride that changes"),
    reason: REASON,
    sourceUrl: SOURCE_URL,
  },
  async handler({ parkId, entries, reason, sourceUrl }, ctx) {
    const id = parkId.toLowerCase();
    const body = await routeBody(BulkCurationDto, {
      entries: entries.map((entry) => ({
        id: entry.id.toLowerCase(),
        fields: entry.fields,
      })),
      reason,
      sourceUrl,
    });
    return runAdminRoute(
      ctx,
      {
        method: "PATCH",
        path: `content/parks/${id}/attractions`,
        selfAudited: true,
      },
      () =>
        ctx
          .get(AdminContentController)
          .patchParkAttractions(id, body, ctx.admin),
    );
  },
});

export const verifyParkTool = defineMcpTool({
  name: "verify_park",
  title: "Record a park check",
  description:
    "Records that a park's curated values were checked against a source and found right, as the " +
    "admin's 'verified' button does: an audit row (park.verify) with the values as they stand " +
    "and the page compared against. Writes nothing to the park. Use it when a check confirms a " +
    "value; curate_park with an unchanged value leaves no trace.",
  minRole: "editor",
  writes: "add",
  input: {
    parkId: uuidInput().describe("Park id from find_parks"),
    fields: z
      .array(z.string().max(80))
      .min(1)
      .max(60)
      .describe("The curated field keys that were checked"),
    sourceUrl: z
      .url({ protocol: /^https?$/ })
      .max(2000)
      .describe("The page the values were checked against; required"),
    reason: z
      .string()
      .trim()
      .max(2000)
      .optional()
      .describe("What was compared, in a sentence"),
  },
  async handler({ parkId, fields, sourceUrl, reason }, ctx) {
    const id = parkId.toLowerCase();
    const body = await routeBody(VerifyFieldsDto, {
      fields,
      sourceUrl,
      reason,
    });
    return runAdminRoute(
      ctx,
      { method: "POST", path: `content/parks/${id}/verify`, selfAudited: true },
      () => ctx.get(AdminContentController).verifyPark(id, body, ctx.admin),
    );
  },
});

export const undoCurationTool = defineMcpTool({
  name: "undo_curation",
  title: "Undo a curation",
  description:
    "Puts one curation back the way it was, as the admin history's undo does: the fields of that " +
    "audit row get their earlier values, the undo gets its own audit row and the original is " +
    "marked reverted. Takes the auditId a curate_* answer or audit_log gave.",
  minRole: "editor",
  writes: "change",
  input: {
    auditId: uuidInput().describe("The curation's audit id"),
  },
  async handler({ auditId }, ctx) {
    const id = auditId.toLowerCase();
    return runAdminRoute(
      ctx,
      { method: "POST", path: `content/history/${id}/undo`, selfAudited: true },
      () => ctx.get(AdminContentController).undo(id, ctx.admin),
    );
  },
});

const TERM_IDS = z.array(z.string().min(1).max(200)).max(200);

export const writeRideProfileTool = defineMcpTool({
  name: "write_ride_profile",
  title: "Write a ride profile",
  description:
    "Writes a ride's glossary profile, as the admin's ride-profile editor does: the track " +
    "elements in ride order (repeats intended), the ride types, manufacturer, model, opening " +
    "year, inversions and hand-written measurements. It replaces the whole profile, so send " +
    "every value the ride keeps; a value left out is cleared. Term ids are checked against the " +
    "glossary before the write. get_attraction shows the current profile.",
  minRole: "editor",
  writes: "change",
  input: {
    attractionId: uuidInput().describe("Ride id from find_attractions"),
    elements: TERM_IDS.optional().describe(
      "Glossary term ids of the track elements, in ride order",
    ),
    types: TERM_IDS.optional().describe(
      "Glossary term ids of the ride's kind, unordered",
    ),
    manufacturerName: z
      .string()
      .max(200)
      .nullable()
      .optional()
      .describe('Display name, e.g. "Bolliger & Mabillard"'),
    manufacturerTermId: z
      .string()
      .max(200)
      .nullable()
      .optional()
      .describe(
        "Glossary term id of the manufacturer; null shows the name as text",
      ),
    model: z.string().max(200).nullable().optional().describe("Model name"),
    openedYear: z
      .number()
      .int()
      .min(1800)
      .max(2100)
      .nullable()
      .optional()
      .describe("Year it opened"),
    inversions: z
      .number()
      .int()
      .min(0)
      .max(20)
      .nullable()
      .optional()
      .describe("Inversions as the manufacturer counts them"),
    curatedStats: z
      .object({
        topSpeedKmh: z.number().positive().max(300).nullable().optional(),
        heightM: z.number().positive().max(200).nullable().optional(),
        lengthM: z.number().positive().max(10000).nullable().optional(),
        durationSeconds: z.number().positive().max(7200).nullable().optional(),
      })
      .nullable()
      .optional()
      .describe("Measurements that win over Wikidata's: km/h, metres, seconds"),
    reason: REASON,
    sourceUrl: SOURCE_URL,
  },
  async handler({ attractionId, ...profile }, ctx) {
    const id = attractionId.toLowerCase();
    const body = await routeBody(RideProfileWriteDto, profile);
    const saved = await runAdminRoute(
      ctx,
      {
        method: "PUT",
        path: `content/attractions/${id}/ride-profile`,
        selfAudited: true,
      },
      () => ctx.get(AdminContentController).putRideProfile(id, body, ctx.admin),
    );
    return {
      attractionId: id,
      elements: saved.elements,
      types: saved.types,
      manufacturerName: saved.manufacturerName,
      manufacturerTermId: saved.manufacturerTermId,
      model: saved.model,
      openedYear: saved.openedYear,
      inversions: saved.inversions,
      curatedStats: saved.curatedStats,
    };
  },
});

export const deleteRideProfileTool = defineMcpTool({
  name: "delete_ride_profile",
  title: "Delete a ride profile",
  description:
    "Removes a ride's glossary profile, as the admin's ride-profile editor does. Nothing " +
    "regenerates a profile: the dry run shows the one that would go, and the audit row keeps it " +
    "so it can be typed back in.",
  minRole: "editor",
  writes: "delete",
  input: {
    attractionId: uuidInput().describe("Ride id from find_attractions"),
    reason: REASON,
    dryRun: dryRunInput("the profile that would be removed"),
  },
  async handler({ attractionId, reason, dryRun }, ctx) {
    const id = attractionId.toLowerCase();
    const profile = await ctx.get(AdminRideProfileService).find(id);
    if (!profile) throw new McpToolError(`Ride ${id} has no profile`);
    if (dryRun) {
      return {
        dryRun: true,
        attractionId: id,
        wouldRemove: {
          elements: profile.elements,
          types: profile.types,
          manufacturerName: profile.manufacturerName,
          model: profile.model,
          openedYear: profile.openedYear,
          inversions: profile.inversions,
          curatedStats: profile.curatedStats,
        },
      };
    }
    const body = await routeBody(RideProfileProvenanceDto, { reason });
    await runAdminRoute(
      ctx,
      {
        method: "DELETE",
        path: `content/attractions/${id}/ride-profile`,
        selfAudited: true,
      },
      () =>
        ctx.get(AdminContentController).deleteRideProfile(id, ctx.admin, body),
    );
    return { dryRun: false, attractionId: id, removed: true };
  },
});

export const correctParkLocationTool = defineMcpTool({
  name: "correct_park_location",
  title: "Correct a park's location",
  description:
    "Corrects a park's city and coordinates, as the admin's location editor does, for a park a " +
    "failed geocode put in the wrong place. A changed city changes the park's URL: the old path " +
    "is kept as an alias and the frontend is revalidated. Owner only, like the admin route.",
  minRole: "owner",
  writes: "change",
  input: {
    parkId: uuidInput().describe("Park id from find_parks"),
    city: z.string().trim().min(1).max(120).optional().describe("The city"),
    citySlug: z
      .string()
      .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/, "lowercase words joined by hyphens")
      .max(120)
      .optional()
      .describe("The city's URL slug; derived from city when absent"),
    latitude: z.number().min(-90).max(90).optional().describe("Latitude"),
    longitude: z.number().min(-180).max(180).optional().describe("Longitude"),
  },
  async handler({ parkId, ...fields }, ctx) {
    // The body the admin sends: only the values it names.
    const location = Object.fromEntries(
      Object.entries(fields).filter(([, value]) => value !== undefined),
    );
    if (Object.keys(location).length === 0) {
      throw new McpToolError("Name a city or coordinates to correct");
    }
    // The rename service refuses an unknown park with an Error the model would not see.
    const park = await asToolRefusal(() =>
      ctx.get(AdminCurationService).findPark(parkId.toLowerCase()),
    );
    return runAdminRoute(
      ctx,
      {
        method: "POST",
        path: `parks/${park.id}/correct-location`,
        body: location,
      },
      () => ctx.get(AdminController).correctParkLocation(park.id, location),
    );
  },
});

const FROM_PATH = z
  .string()
  .regex(
    /^\/[^\s?#]*$/,
    "a path such as /de/parks/europe/germany/bruehl/phantasialand/old-ride",
  )
  .max(512)
  .describe(
    "The dead path, with or without its locale: /parks/<continent>/<country>/<city>/<park> for " +
      "a park, plus /<ride> for a ride; seo_not_found lists them",
  );

export const addRedirectTool = defineMcpTool({
  name: "add_redirect",
  title: "Redirect a dead path",
  description:
    "Makes a dead park or ride URL answer with a 301 to a park or ride, as the admin's redirect " +
    "button on the not-found list does: a slug alias the park or ride lookup reads. A ride " +
    "redirect takes a path in the ride's current park. Refused when the path is a live page or " +
    "already redirects elsewhere; the same redirect twice writes nothing (created false).",
  minRole: "editor",
  writes: "add",
  input: {
    fromPath: FROM_PATH,
    targetKind: z.enum(REDIRECT_KINDS).describe("What the path should lead to"),
    targetId: uuidInput().describe("The park's or ride's id"),
  },
  async handler({ fromPath, targetKind, targetId }, ctx) {
    const body = await routeBody(CreateRedirectDto, {
      fromPath,
      target: { kind: targetKind, id: targetId.toLowerCase() },
    });
    // The route's handler only sets the status; the service is the write.
    const { created, redirect } = await runAdminRoute(
      ctx,
      { method: "POST", path: "redirects", selfAudited: true },
      () =>
        ctx
          .get(AdminRedirectService)
          .create(body.fromPath, body.target, ctx.admin),
    );
    return { created, redirect };
  },
});

export const removeRedirectTool = defineMcpTool({
  name: "remove_redirect",
  title: "Remove a redirect",
  description:
    "Removes a redirect a person made by hand (add_redirect or the admin), found by the path it " +
    "answers; the path is dead again. One a rename, merge or the sync recorded is refused: it is " +
    "the only trace of a URL that was live.",
  minRole: "editor",
  writes: "delete",
  input: {
    fromPath: FROM_PATH,
    confirm: confirmInput("the redirect is removed and the path is dead again"),
  },
  async handler({ fromPath }, ctx) {
    await runAdminRoute(
      ctx,
      { method: "DELETE", path: "redirects", selfAudited: true },
      () => ctx.get(AdminRedirectService).remove(fromPath, ctx.admin),
    );
    return { removed: fromPath };
  },
});

export const refreshParkTool = defineMcpTool({
  name: "refresh_park",
  title: "Refresh a park",
  description:
    "For one park: fetch its opening hours again from ThemeParks.wiki (the job the calendar " +
    "queues when a park's hours run out), and/or publish it again: evict its cached payloads, " +
    "tell the frontend to revalidate, and again after the CDN window. Use it when a park page " +
    "shows something older than the data. The schedule job runs in the background; queue_status " +
    "shows it.",
  minRole: "editor",
  writes: "add",
  input: {
    parkId: uuidInput().describe("Park id from find_parks"),
    schedule: z
      .boolean()
      .default(false)
      .describe("Queue the opening-hours sync for this park"),
    publish: z
      .boolean()
      .default(true)
      .describe("Evict the park's caches and revalidate its pages"),
  },
  async handler({ parkId, schedule, publish }, ctx) {
    if (!schedule && !publish) {
      throw new McpToolError("Ask for schedule, publish or both");
    }
    const curation = ctx.get(AdminCurationService);
    const park = await asToolRefusal(() =>
      curation.findPark(parkId.toLowerCase()),
    );
    if (schedule) {
      // The job and options CalendarService queues when a park's hours run out.
      await ctx
        .get(getQueueToken("park-metadata") as unknown as Type<Queue>)
        .add(
          "sync-park-schedule",
          { parkId: park.id },
          { removeOnComplete: true },
        );
    }
    if (publish) {
      await curation.publish(park.id, await curation.attractionIdsOf(park.id));
    }
    await ctx.get(AdminAuditService).record({
      actor: ctx.admin,
      action: "park.refresh",
      entityType: "park",
      entityId: park.id,
      entityLabel: park.name,
      after: { schedule, publish },
    });
    return {
      parkId: park.id,
      name: park.name,
      scheduleQueued: schedule,
      published: publish,
    };
  },
});
