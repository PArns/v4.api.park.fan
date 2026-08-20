import type { Attraction } from "../../attractions/entities/attraction.entity";
import type { Park } from "../../parks/entities/park.entity";
import { resolveCuratedFacts } from "../../attractions/utils/curated-attraction-facts.util";
import { resolveCuratedPark } from "../../parks/utils/curated-park-facts.util";

/**
 * The curated fields, described rather than hard-coded into a form.
 *
 * The admin needs to render three values per field — what the sync says, what a
 * human wrote, and what the API actually serves — plus whether the two disagree.
 * That is the same shape for a name, a height and a list of months, and writing
 * it out per field in the UI means the UI holds a second, drifting copy of which
 * columns are curatable and which sync owns each one. So the backend describes
 * them and the frontend renders whatever it is handed: adding a curated column
 * here makes it appear in the editor with no frontend change at all.
 *
 * `syncedKey` is null for a field no sync writes. Those are not "curated" in the
 * two-writers sense — `has_single_rider` has exactly one writer — but they are
 * hand-edited, they belong in the same editor, and the descriptor says so by
 * carrying no upstream value to compare against.
 */

export type CuratedFieldType =
  "text" | "longtext" | "number" | "boolean" | "enum" | "months";

export interface CuratedFieldSpec {
  /** The curated column's TS property name — also the PATCH body key. */
  key: string;
  label: string;
  type: CuratedFieldType;
  /** The sync-owned property this corrects, or null when there is no sync. */
  syncedKey: string | null;
  /** Which resolved value on the entity this field ends up producing. */
  resolvedKey: string | null;
  group: string;
  options?: readonly string[];
  unit?: string;
  min?: number;
  max?: number;
  /** Shown under the input. Say what the field means, not how to type it. */
  hint?: string;
}

export const ATTRACTION_CURATED_FIELDS: readonly CuratedFieldSpec[] = [
  {
    key: "curatedName",
    label: "Name",
    type: "text",
    syncedKey: "name",
    resolvedKey: "name",
    group: "Identity",
    hint:
      "Display name only — the slug and the ride's URL stay as they are. " +
      "There is no redirect table for attractions, so a changed address is a " +
      "permanent 404.",
  },
  {
    key: "curatedLandName",
    label: "Land",
    type: "text",
    syncedKey: "landName",
    resolvedKey: "landName",
    group: "Identity",
    hint: "Queue-Times' land names are missing for whole parks and go stale after a re-theme.",
  },
  {
    key: "curatedAttractionType",
    label: "Type",
    type: "text",
    syncedKey: "attractionType",
    resolvedKey: "attractionType",
    group: "Identity",
    hint: "Upstream's coarse category. The ride-type glossary terms live on the ride profile.",
  },
  {
    key: "curatedMinimumHeight",
    label: "Minimum height",
    type: "number",
    syncedKey: "minimumHeight",
    resolvedKey: "minimumHeight",
    group: "Restrictions",
    unit: "cm",
    min: 0,
    max: 250,
    hint: "0 means there is no minimum at all — not a 0 cm limit. Leave empty to accept upstream.",
  },
  {
    key: "curatedMaximumHeight",
    label: "Maximum height",
    type: "number",
    syncedKey: "maximumHeight",
    resolvedKey: "maximumHeight",
    group: "Restrictions",
    unit: "cm",
    min: 0,
    max: 250,
    hint: "0 means there is no maximum. Upstream carries one for about 2 % of rides.",
  },
  {
    key: "curatedMayGetWet",
    label: "May get wet",
    type: "boolean",
    syncedKey: "mayGetWet",
    resolvedKey: "mayGetWet",
    group: "Restrictions",
    hint: "Upstream fills this for a few dozen of ~7000 rides and is occasionally wrong where it does.",
  },
  {
    key: "curatedIsSeasonal",
    label: "Seasonal",
    type: "boolean",
    syncedKey: "isSeasonal",
    resolvedKey: "isSeasonal",
    group: "Season",
    hint:
      "The nightly detector reports what the feed does. Set false for a ride " +
      "closed for a long refurbishment — that looks identical to a season from " +
      "the outside.",
  },
  {
    key: "curatedSeasonMonths",
    label: "Operating months",
    type: "months",
    syncedKey: "seasonMonths",
    resolvedKey: "seasonMonths",
    group: "Season",
    hint:
      "The detector writes no months for anything watched under 330 days, on " +
      "purpose — derived months would just be the recording window.",
  },
  {
    key: "hasSingleRider",
    label: "Single-rider line",
    type: "boolean",
    syncedKey: null,
    resolvedKey: "hasSingleRider",
    group: "Facilities",
    hint:
      "Whether the queue exists at all, not whether it is open right now. " +
      "Nothing syncs this.",
  },
  {
    key: "openWithPark",
    label: "Open with the park",
    type: "boolean",
    syncedKey: null,
    resolvedKey: "openWithPark",
    group: "Facilities",
    hint:
      "Free-flow attractions — playgrounds, splash pads — that have no queue " +
      "and are accessible whenever the park is open.",
  },
  {
    key: "rcdbId",
    label: "RCDB id",
    type: "number",
    syncedKey: null,
    resolvedKey: "rcdbId",
    group: "Links",
    min: 1,
    hint:
      "rcdb.com/<id>.htm. One id must never sit on two rides — it would point " +
      "a ride page at a different ride, and the Wikidata stats import joins on it.",
  },
];

export const PARK_CURATED_FIELDS: readonly CuratedFieldSpec[] = [
  {
    key: "curatedName",
    label: "Name",
    type: "text",
    syncedKey: "name",
    resolvedKey: "name",
    group: "Identity",
    hint:
      "Display name only. Changing the park's address is a rename, which " +
      "writes a redirect — a different operation.",
  },
  {
    key: "curatedParkType",
    label: "Park type",
    type: "enum",
    syncedKey: "parkType",
    resolvedKey: "parkType",
    group: "Identity",
    options: ["THEME_PARK", "WATER_PARK"],
    hint: "Upstream files water parks inside a combined resort as theme parks.",
  },
  {
    key: "curatedNoWaitTimesReason",
    label: "Wait times unreadable",
    type: "enum",
    syncedKey: null,
    resolvedKey: "noWaitTimesReason",
    group: "Data sources",
    options: ["in_park_app_only", "not_published"],
    hint:
      "Set this only for a park that publishes wait times nowhere we can read. " +
      "It cannot be derived: at 03:00 such a park and a park shut for the night " +
      "look identical.",
  },
  {
    key: "curationNote",
    label: "Curation note",
    type: "longtext",
    syncedKey: null,
    resolvedKey: null,
    group: "Notes",
    hint: "Not shown to visitors. Context for whoever reads this row next.",
  },
];

export interface CuratedFieldView {
  key: string;
  label: string;
  type: CuratedFieldType;
  group: string;
  syncedValue: unknown;
  curatedValue: unknown;
  resolvedValue: unknown;
  /** True when a human value is in force and differs from the synced one. */
  overridden: boolean;
  /** True when there is no sync behind this field at all. */
  humanOnly: boolean;
  options?: readonly string[];
  unit?: string;
  min?: number;
  max?: number;
  hint?: string;
}

function readKey(source: Record<string, unknown>, key: string | null): unknown {
  if (!key) return null;
  const value = source[key];
  return value === undefined ? null : value;
}

function buildViews(
  specs: readonly CuratedFieldSpec[],
  entity: Record<string, unknown>,
  resolved: Record<string, unknown>,
): CuratedFieldView[] {
  return specs.map((spec) => {
    const curatedValue = readKey(entity, spec.key);
    const syncedValue = readKey(entity, spec.syncedKey);
    const resolvedValue = spec.resolvedKey
      ? readKey(resolved, spec.resolvedKey)
      : curatedValue;

    return {
      key: spec.key,
      label: spec.label,
      type: spec.type,
      group: spec.group,
      syncedValue,
      curatedValue,
      resolvedValue,
      // A curated value equal to the synced one is not an override — it is
      // somebody having typed what was already true, and marking it as a
      // correction would put a badge on a row nobody changed.
      overridden:
        curatedValue !== null &&
        curatedValue !== undefined &&
        JSON.stringify(curatedValue) !== JSON.stringify(syncedValue),
      humanOnly: spec.syncedKey === null,
      ...(spec.options ? { options: spec.options } : {}),
      ...(spec.unit ? { unit: spec.unit } : {}),
      ...(spec.min !== undefined ? { min: spec.min } : {}),
      ...(spec.max !== undefined ? { max: spec.max } : {}),
      ...(spec.hint ? { hint: spec.hint } : {}),
    };
  });
}

export function attractionFieldViews(
  attraction: Attraction,
): CuratedFieldView[] {
  return buildViews(
    ATTRACTION_CURATED_FIELDS,
    attraction as unknown as Record<string, unknown>,
    resolveCuratedFacts(attraction) as unknown as Record<string, unknown>,
  );
}

export function parkFieldViews(park: Park): CuratedFieldView[] {
  return buildViews(
    PARK_CURATED_FIELDS,
    park as unknown as Record<string, unknown>,
    resolveCuratedPark(park) as unknown as Record<string, unknown>,
  );
}

/** Every writable key, for validating a PATCH body against the descriptor. */
export const ATTRACTION_CURATED_KEYS = new Set(
  ATTRACTION_CURATED_FIELDS.map((f) => f.key),
);
export const PARK_CURATED_KEYS = new Set(PARK_CURATED_FIELDS.map((f) => f.key));

export const CURATED_FIELD_SPEC_BY_KEY = {
  attraction: new Map(ATTRACTION_CURATED_FIELDS.map((f) => [f.key, f])),
  park: new Map(PARK_CURATED_FIELDS.map((f) => [f.key, f])),
};
