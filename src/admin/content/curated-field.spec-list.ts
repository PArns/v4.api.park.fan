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
  | "text"
  | "longtext"
  | "number"
  | "decimal"
  | "boolean"
  | "enum"
  | "months"
  | "url";

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
  /** Text and url only. A stored value longer than this is somebody pasting a
   *  page into a field, not a fact. */
  maxLength?: number;
  /** Shown under the input. Say what the field means, not how to type it. */
  hint?: string;
  /**
   * What the column holds when nobody has decided anything.
   *
   * `null` for almost everything — a curated column is empty until somebody
   * fills it. `open_with_park` is the exception and the reason this exists: it
   * is `boolean NOT NULL DEFAULT false`, so "not set" is `false`, not null.
   * Two things follow, and both were wrong before this field existed. Clearing
   * the input must write `false` rather than null, or the UPDATE violates the
   * constraint. And a stored `false` must NOT count as an override, or every
   * ride in the catalogue shows a "curated" badge it never earned.
   */
  defaultValue?: unknown;
}

/**
 * `label`, `group` and `hint` are German, and they are the only German strings
 * in this repository.
 *
 * They are not documentation — they are the words the editor renders. The admin
 * at park.fan/admin is deliberately unlocalized German (see its layout: "a
 * surface three people use in one language"), so an English label there is not
 * a language choice, it is a half-finished screen: "Minimum height" over a
 * field, under a heading that says "Kuratierte Felder".
 *
 * Everything else in this file — the comments, the reasoning — stays English
 * like the rest of the codebase.
 */
export const ATTRACTION_CURATED_FIELDS: readonly CuratedFieldSpec[] = [
  {
    key: "curatedName",
    label: "Name",
    type: "text",
    syncedKey: "name",
    resolvedKey: "name",
    group: "Identität",
    hint:
      "Nur der Anzeigename — Slug und Adresse der Bahn bleiben, wie sie sind. " +
      "Für Attraktionen gibt es keine Weiterleitungstabelle, eine geänderte " +
      "Adresse wäre also dauerhaft ein 404.",
  },
  {
    key: "curatedLandName",
    label: "Themenbereich",
    type: "text",
    syncedKey: "landName",
    resolvedKey: "landName",
    group: "Identität",
    hint:
      "Queue-Times fehlen die Bereichsnamen für ganze Parks, und nach einem " +
      "Umbau bleiben sie stehen.",
  },
  {
    key: "curatedAttractionType",
    label: "Art",
    type: "text",
    syncedKey: "attractionType",
    resolvedKey: "attractionType",
    group: "Identität",
    hint:
      "Die grobe Kategorie des Upstreams. Die Glossarbegriffe zur Bahnart " +
      "stehen im Ride-Profil.",
  },
  {
    key: "curatedMinimumHeight",
    label: "Mindestgröße",
    type: "number",
    syncedKey: "minimumHeight",
    resolvedKey: "minimumHeight",
    group: "Einschränkungen",
    unit: "cm",
    min: 0,
    max: 250,
    hint:
      "0 heißt: gar keine Mindestgröße — nicht 0 cm. Leer lassen heißt: der " +
      "Upstream-Wert gilt.",
  },
  {
    key: "curatedMaximumHeight",
    label: "Maximalgröße",
    type: "number",
    syncedKey: "maximumHeight",
    resolvedKey: "maximumHeight",
    group: "Einschränkungen",
    unit: "cm",
    min: 0,
    max: 250,
    hint:
      "0 heißt: keine Obergrenze. Der Upstream führt eine für etwa 2 % der " +
      "Bahnen.",
  },
  {
    key: "curatedMayGetWet",
    label: "Man wird nass",
    type: "boolean",
    syncedKey: "mayGetWet",
    resolvedKey: "mayGetWet",
    group: "Einschränkungen",
    hint:
      "Der Upstream füllt das bei ein paar Dutzend von ~7000 Bahnen — und liegt " +
      "dort gelegentlich daneben.",
  },
  {
    key: "curatedIsSeasonal",
    label: "Saisonal",
    type: "boolean",
    syncedKey: "isSeasonal",
    resolvedKey: "isSeasonal",
    group: "Saison",
    hint:
      'Der nächtliche Detektor beschreibt, was der Feed tut. Auf „nein" setzen ' +
      "bei einer Bahn in langer Revision — von außen sieht die aus wie eine " +
      "Saison.",
  },
  {
    key: "curatedSeasonMonths",
    label: "Betriebsmonate",
    type: "months",
    syncedKey: "seasonMonths",
    resolvedKey: "seasonMonths",
    group: "Saison",
    hint:
      "Unter 330 Tagen Beobachtung schreibt der Detektor absichtlich keine " +
      "Monate — abgeleitete wären nur unser Aufzeichnungsfenster.",
  },
  {
    key: "hasSingleRider",
    label: "Single-Rider-Schlange",
    type: "boolean",
    syncedKey: null,
    resolvedKey: "hasSingleRider",
    group: "Ausstattung",
    hint:
      "Ob es sie überhaupt gibt, nicht ob sie gerade offen ist. Das synchronisiert " +
      "nichts.",
  },
  {
    key: "openWithPark",
    label: "Offen mit dem Park",
    type: "boolean",
    syncedKey: null,
    resolvedKey: "openWithPark",
    group: "Ausstattung",
    // NOT NULL with a default of false — see `defaultValue`.
    defaultValue: false,
    hint:
      "Frei zugängliche Attraktionen — Spielplätze, Wasserspielplätze — ohne " +
      "Warteschlange, offen, solange der Park offen ist.",
  },
  {
    key: "rcdbId",
    label: "RCDB-Id",
    type: "number",
    syncedKey: null,
    resolvedKey: "rcdbId",
    group: "Links",
    min: 1,
    hint:
      "rcdb.com/<id>.htm. Eine Id darf nie an zwei Bahnen hängen — die Bahnseite " +
      "zeigte sonst auf eine andere Bahn, und der Wikidata-Import joint darauf.",
  },
];

export const PARK_CURATED_FIELDS: readonly CuratedFieldSpec[] = [
  {
    key: "curatedName",
    label: "Name",
    type: "text",
    syncedKey: "name",
    resolvedKey: "name",
    group: "Identität",
    hint:
      "Nur der Anzeigename. Die Adresse eines Parks zu ändern ist eine " +
      "Umbenennung, die eine Weiterleitung schreibt — ein anderer Vorgang.",
  },
  {
    key: "curatedParkType",
    label: "Parkart",
    type: "enum",
    syncedKey: "parkType",
    resolvedKey: "parkType",
    group: "Identität",
    options: ["THEME_PARK", "WATER_PARK"],
    hint: "Der Upstream führt Wasserparks innerhalb eines Resorts als Freizeitparks.",
  },
  {
    key: "curatedNoWaitTimesReason",
    label: "Wartezeiten nicht lesbar",
    type: "enum",
    syncedKey: null,
    resolvedKey: "noWaitTimesReason",
    group: "Datenquellen",
    options: ["in_park_app_only", "not_published"],
    hint:
      "Nur für einen Park setzen, der seine Wartezeiten nirgends veröffentlicht, " +
      "wo wir sie lesen können. Ableiten lässt sich das nicht: um 03:00 sieht so " +
      "ein Park aus wie einer, der über Nacht geschlossen hat.",
  },
  {
    key: "curatedWebsite",
    label: "Offizielle Website",
    type: "url",
    syncedKey: null,
    resolvedKey: null,
    group: "Links",
    maxLength: 500,
    hint:
      "Die eigene Startseite des Parks. Eine Adresse für alle sechs Sprachen — " +
      "die meisten Parks antworten in der des Besuchers.",
  },
  {
    key: "curatedTicketsUrl",
    label: "Tickets",
    type: "url",
    syncedKey: null,
    resolvedKey: null,
    group: "Links",
    maxLength: 500,
    hint: "Der Shop, wenn er woanders liegt als die Startseite. Sonst leer lassen.",
  },
  {
    key: "curatedWikipediaUrl",
    label: "Wikipedia",
    type: "url",
    syncedKey: null,
    resolvedKey: null,
    group: "Links",
    maxLength: 500,
    hint: "Beliebige Sprache — der Artikel verlinkt seine Übersetzungen selbst.",
  },
  {
    key: "curatedInstagramUrl",
    label: "Instagram",
    type: "url",
    syncedKey: null,
    resolvedKey: null,
    group: "Links",
    maxLength: 500,
  },
  {
    key: "curatedFacebookUrl",
    label: "Facebook",
    type: "url",
    syncedKey: null,
    resolvedKey: null,
    group: "Links",
    maxLength: 500,
  },
  {
    key: "curatedYoutubeUrl",
    label: "YouTube",
    type: "url",
    syncedKey: null,
    resolvedKey: null,
    group: "Links",
    maxLength: 500,
  },
  {
    key: "curatedStreetAddress",
    label: "Straße",
    type: "text",
    syncedKey: null,
    resolvedKey: null,
    group: "Kontakt",
    maxLength: 200,
    hint: "Straße und Hausnummer. Das Geocoding liefert die Stadt und hört da auf.",
  },
  {
    key: "curatedPostalCode",
    label: "PLZ",
    type: "text",
    syncedKey: null,
    resolvedKey: null,
    group: "Kontakt",
    maxLength: 20,
  },
  {
    key: "curatedPhone",
    label: "Telefon",
    type: "text",
    syncedKey: null,
    resolvedKey: null,
    group: "Kontakt",
    maxLength: 40,
    hint: "So, wie man aus dem Ausland wählt, mit Ländervorwahl.",
  },
  {
    key: "curatedOpenedYear",
    label: "Eröffnet",
    type: "number",
    syncedKey: null,
    resolvedKey: null,
    group: "Eckdaten",
    min: 1550,
    max: 2100,
    hint:
      "Das Jahr der Eröffnung für Besucher. Bakken sagt 1583, deshalb liegt die " +
      "Untergrenze so tief.",
  },
  {
    key: "curatedAreaHectares",
    label: "Fläche",
    type: "decimal",
    syncedKey: null,
    resolvedKey: null,
    group: "Eckdaten",
    unit: "ha",
    min: 0,
    max: 100000,
    hint: "Die für Besucher zugängliche Fläche, nicht der Grundbesitz der Firma.",
  },
  {
    key: "curationNote",
    label: "Notiz",
    type: "longtext",
    syncedKey: null,
    resolvedKey: null,
    group: "Notizen",
    hint: "Sehen Besucher nicht. Kontext für den, der diese Zeile als Nächstes liest.",
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
  /**
   * What "nothing decided" looks like for this field — null for almost
   * everything, `false` for the one NOT NULL column. The editor needs it to
   * know what clearing an input should write.
   */
  defaultValue: unknown;
  options?: readonly string[];
  unit?: string;
  min?: number;
  max?: number;
  maxLength?: number;
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

    const unset = spec.defaultValue ?? null;

    return {
      key: spec.key,
      label: spec.label,
      type: spec.type,
      group: spec.group,
      syncedValue,
      curatedValue,
      resolvedValue,
      defaultValue: unset,
      // Compared against what "nothing decided" looks like, not against null.
      // For a NOT NULL column that is `false`, and comparing against null
      // instead put a "curated" badge on every attraction in the catalogue —
      // `open_with_park` is false on all of them.
      //
      // A curated value equal to the synced one is not an override either: it
      // is somebody having typed what was already true, and marking it as a
      // correction would put a badge on a row nobody changed.
      overridden:
        curatedValue !== null &&
        curatedValue !== undefined &&
        JSON.stringify(curatedValue) !== JSON.stringify(unset) &&
        JSON.stringify(curatedValue) !== JSON.stringify(syncedValue),
      humanOnly: spec.syncedKey === null,
      ...(spec.options ? { options: spec.options } : {}),
      ...(spec.unit ? { unit: spec.unit } : {}),
      ...(spec.min !== undefined ? { min: spec.min } : {}),
      ...(spec.max !== undefined ? { max: spec.max } : {}),
      ...(spec.maxLength !== undefined ? { maxLength: spec.maxLength } : {}),
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
