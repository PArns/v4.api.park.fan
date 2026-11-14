import {
  PARK_SEASON_KINDS,
  PARK_SEASON_STATUSES,
  type ParkSeasonKind,
  type ParkSeasonStatus,
} from "../../parks/entities/park-season.entity";
import { CLOCK_PATTERN, ISO_DATE_PATTERN } from "../../common/utils/date.util";
import type { SeasonInput } from "../../parks/services/park-season.service";
import {
  findRegion,
  TRIP_REGIONS,
  type RegionParkConfig,
} from "../../trip-regions/regions";

/**
 * The rules of a season import, without I/O: what a researched row must hold,
 * which trip-region park its slug means, the admin body it becomes and when it
 * is already on file. `AdminSeasonService.import` runs them; the rows are the
 * shape of `scripts/trip-regions/seasons/*.json`.
 */

/**
 * One researched season: the admin's season body with the park named by its
 * slug (and the trip region, where a slug repeats) and `notes` for `note`.
 * Strings rather than the season's own types, so a wrong value is reported
 * on its row instead of refusing the whole file.
 */
export interface SeasonImportRow {
  region?: string | null;
  parkSlug: string;
  kind: string;
  name?: string | null;
  /** Null while the dates are not announced: the row is reported and waits. */
  startDate: string | null;
  endDate?: string | null;
  dates?: string[] | null;
  status?: string | null;
  separateTicket?: boolean | null;
  priceFrom?: number | string | null;
  priceCurrency?: string | null;
  opensAt?: string | null;
  closesAt?: string | null;
  url?: string | null;
  sourceUrl?: string | null;
  notes?: string | null;
}

/** A region park a row may mean, with the region that lists it. */
export interface RegionParkMatch {
  region: string;
  config: RegionParkConfig;
}

/** What the import answers for one row, as the script printed it. */
export type SeasonImportOutcome =
  "create" | "created" | "exists" | "invalid" | "failed";

/** The row as one line: number, region, park, kind, name and dates. */
export function seasonRowLabel(index: number, row: SeasonImportRow): string {
  return (
    `#${index + 1} ${row.region ? `${row.region}/` : ""}${row.parkSlug} ` +
    `${row.kind} "${row.name ?? ""}" ${row.startDate ?? "?"} – ${row.endDate ?? "?"}`
  );
}

/**
 * What is wrong with a row before any park is looked up, or null. A row
 * without `endDate` (a closure whose reopening is not announced) waits:
 * `park_seasons.end_date` is NOT NULL, and a made-up end would read as a
 * reopening.
 */
export function seasonRowProblem(row: SeasonImportRow): string | null {
  if (!row.parkSlug) return "no parkSlug";
  if (!(PARK_SEASON_KINDS as readonly string[]).includes(row.kind)) {
    return `kind "${row.kind}" is not one of ${PARK_SEASON_KINDS.join(", ")}`;
  }
  if (!row.startDate || !ISO_DATE_PATTERN.test(row.startDate)) {
    return "startDate is not YYYY-MM-DD";
  }
  if (row.endDate == null) {
    return "no endDate: the end is not announced, so the row waits";
  }
  if (!ISO_DATE_PATTERN.test(row.endDate)) return "endDate is not YYYY-MM-DD";
  if (row.endDate < row.startDate) return "endDate is before startDate";
  if (
    row.status &&
    !(PARK_SEASON_STATUSES as readonly string[]).includes(row.status)
  ) {
    return `status "${row.status}" is unknown`;
  }
  for (const field of ["opensAt", "closesAt"] as const) {
    const value = row[field];
    if (value === "24:00") return `${field} is 24:00: write midnight as 00:00`;
    if (value != null && !CLOCK_PATTERN.test(value)) {
      return `${field} is not HH:MM`;
    }
  }
  for (const date of row.dates ?? []) {
    if (
      !ISO_DATE_PATTERN.test(date) ||
      date < row.startDate ||
      date > row.endDate
    ) {
      return `date ${date} is outside ${row.startDate} – ${row.endDate}`;
    }
  }
  return null;
}

/**
 * The region parks a slug may mean: the named region's, else every region's
 * that lists it. Slugs repeat across cities (Disneyland Park is in Anaheim and
 * in Paris), so a slug two regions list needs the row's region. An empty list
 * means no region lists it, and the import looks the slug up in every park.
 */
export function regionParksOf(
  slug: string,
  regionSlug: string | null | undefined,
): { parks: RegionParkMatch[] } | { problem: string } {
  if (regionSlug != null) {
    const region = findRegion(regionSlug);
    if (!region) return { problem: `region ${regionSlug} is no trip region` };
    const config = region.parks.find((park) => park.slug === slug);
    if (!config) return { problem: `${slug} is no park of ${regionSlug}` };
    return { parks: [{ region: region.slug, config }] };
  }
  const parks = Object.values(TRIP_REGIONS).flatMap((region) =>
    region.parks
      .filter((park) => park.slug === slug)
      .map((config) => ({ region: region.slug, config })),
  );
  if (parks.length > 1) {
    return {
      problem: `${slug} is a park of ${parks.map((p) => p.region).join(" and ")}: give the row its region`,
    };
  }
  return { parks };
}

/** The admin's season body for a checked row; `notes` becomes `note`. */
export function seasonRowInput(row: SeasonImportRow): SeasonInput {
  return {
    kind: row.kind as ParkSeasonKind,
    name: row.name ?? null,
    startDate: row.startDate as string,
    endDate: row.endDate as string,
    dates: row.dates && row.dates.length > 0 ? row.dates : null,
    status: (row.status ?? "announced") as ParkSeasonStatus,
    separateTicket: row.separateTicket === true,
    priceFrom: row.priceFrom ?? null,
    priceCurrency: row.priceCurrency ?? null,
    opensAt: row.opensAt ?? null,
    closesAt: row.closesAt ?? null,
    url: row.url ?? null,
    sourceUrl: row.sourceUrl ?? null,
    note: row.notes ?? null,
  };
}

/** A season on file, as far as the duplicate check reads it. */
export interface SeasonOnFile {
  id: string;
  parkId: string;
  kind: string;
  name: string | null;
  startDate: string;
  endDate: string;
}

const nameKey = (name: string | null | undefined): string =>
  (name ?? "").trim().toLowerCase();

/**
 * The season on file a row repeats: same park, kind and name (case and outer
 * spaces aside), dates overlapping. Null when the row is new.
 */
export function seasonOnFile<T extends SeasonOnFile>(
  onFile: readonly T[],
  parkId: string,
  row: SeasonImportRow,
): T | null {
  return (
    onFile.find(
      (season) =>
        season.parkId === parkId &&
        season.kind === row.kind &&
        nameKey(season.name) === nameKey(row.name) &&
        season.startDate <= (row.endDate as string) &&
        season.endDate >= (row.startDate as string),
    ) ?? null
  );
}
