/**
 * The school breaks a visitor from abroad can travel in, per holiday region,
 * read from the `holidays` table: what `GET /v1/trip-regions/:region/school-breaks`
 * lists and what `/when?within=` accepts. One source for both, so the client
 * never guesses a code. Contract: docs/frontend/trip-regions-when.md.
 *
 * Pure: `TripWhenService` reads the rows and the coverage and passes them in.
 *
 * The codes are the `holidays.region` values the sync stored, verbatim. A
 * country listed below offers its visitor regions by name; any other country
 * asked for explicitly offers every code its rows carry, named by the code.
 * The Netherlands offers only the north: OpenHolidays lists a break that all
 * three regions share with the north's subdivisions only, and the sync stores
 * subdivisions, so the middle and south codes lack Christmas, May and some
 * spring and autumn breaks (measured 2026-11-12). Offering them would turn a
 * gap in our data into "no school break".
 */
import { BadRequestException } from "@nestjs/common";
import { addIsoDays } from "../common/utils/date.util";
import type { SchoolCoverage } from "../common/utils/holiday.utils";
import { normalizeRegionCode } from "../common/utils/region.util";
import type { SchoolBreakPeriod, WhenWithin } from "./trip-when";

/** A holiday row as this module reads it. */
export interface SchoolRowLike {
  date: string;
  name: string;
  country: string;
  region: string | null;
  holidayType: string;
  isNationwide: boolean;
}

export interface SchoolBreakRegion {
  code: string;
  name: string;
  country: string;
  /** The last day the stored school data reaches for the region; later breaks are unknown. */
  through: string | null;
  periods: SchoolBreakPeriod[];
}

export interface TripSchoolBreaks {
  regions: SchoolBreakRegion[];
}

/** The visitor regions per country, by the code the holiday table stores. */
export const SCHOOL_BREAK_REGIONS: Record<
  string,
  ReadonlyArray<{ code: string; name: string }>
> = {
  DE: [
    { code: "DE-BW", name: "Baden-Württemberg" },
    { code: "DE-BY", name: "Bayern" },
    { code: "DE-BE", name: "Berlin" },
    { code: "DE-BB", name: "Brandenburg" },
    { code: "DE-HB", name: "Bremen" },
    { code: "DE-HH", name: "Hamburg" },
    { code: "DE-HE", name: "Hessen" },
    { code: "DE-MV", name: "Mecklenburg-Vorpommern" },
    { code: "DE-NI", name: "Niedersachsen" },
    { code: "DE-NW", name: "Nordrhein-Westfalen" },
    { code: "DE-RP", name: "Rheinland-Pfalz" },
    { code: "DE-SL", name: "Saarland" },
    { code: "DE-SN", name: "Sachsen" },
    { code: "DE-ST", name: "Sachsen-Anhalt" },
    { code: "DE-SH", name: "Schleswig-Holstein" },
    { code: "DE-TH", name: "Thüringen" },
  ],
  // Groningen stands for the whole north: Friesland, Drenthe, Overijssel and
  // Noord-Holland carry the same breaks.
  NL: [{ code: "NL-GR", name: "Regio Noord" }],
  GB: [{ code: "GB-ENG", name: "England" }],
  BE: [
    { code: "BE-NL", name: "Vlaamse Gemeenschap" },
    { code: "BE-FR", name: "Fédération Wallonie-Bruxelles" },
  ],
  FR: [
    { code: "FR-ZA", name: "Zone A" },
    { code: "FR-ZB", name: "Zone B" },
    { code: "FR-ZC", name: "Zone C" },
  ],
  AT: [
    { code: "AT-BL", name: "Burgenland" },
    { code: "AT-KÄ", name: "Kärnten" },
    { code: "AT-NÖ", name: "Niederösterreich" },
    { code: "AT-OÖ", name: "Oberösterreich" },
    { code: "AT-SB", name: "Salzburg" },
    { code: "AT-SM", name: "Steiermark" },
    { code: "AT-TI", name: "Tirol" },
    { code: "AT-VA", name: "Vorarlberg" },
    { code: "AT-WI", name: "Wien" },
  ],
};

/** Countries listed without `countries=`, when their data reaches far enough. */
export const DEFAULT_SCHOOL_BREAK_COUNTRIES = Object.keys(SCHOOL_BREAK_REGIONS);

/**
 * A country is left out of the default list when its school data ends sooner
 * than this many days from today: its breaks would read as absent.
 */
export const MIN_COVERAGE_DAYS = 180;

const sameCode = (a: string | null, b: string): boolean =>
  !!a && a.toUpperCase() === b.toUpperCase();

/** 0 = Sunday … 6 = Saturday, of a calendar date. */
const weekday = (date: string): number =>
  new Date(`${date}T12:00:00Z`).getUTCDay();
const isWeekend = (date: string): boolean => [0, 6].includes(weekday(date));

/**
 * A region's breaks: its own rows and the country's nationwide ones, merged
 * into runs of consecutive days, each widened over the weekend before and
 * after it (no school on either). Named by the name most of the run's days
 * carry. Only runs that reach into [from, to] are kept, unclipped.
 */
export function schoolBreakPeriods(
  rows: readonly SchoolRowLike[],
  code: string,
  from: string,
  to: string,
): SchoolBreakPeriod[] {
  const country = code.slice(0, 2).toUpperCase();
  const names = new Map<string, string[]>();
  for (const row of rows) {
    if (row.holidayType !== "school" || row.country !== country) continue;
    if (!(row.isNationwide || !row.region || sameCode(row.region, code))) {
      continue;
    }
    names.set(row.date, [...(names.get(row.date) ?? []), row.name]);
  }
  const dates = [...names.keys()].sort();
  const runs: SchoolBreakPeriod[] = [];
  let start: string | null = null;
  let last: string | null = null;
  let runNames: string[] = [];
  const close = () => {
    if (!start || !last) return;
    let a = start;
    let b = last;
    while (isWeekend(addIsoDays(a, -1))) a = addIsoDays(a, -1);
    while (isWeekend(addIsoDays(b, 1))) b = addIsoDays(b, 1);
    const counts = new Map<string, number>();
    for (const n of runNames) counts.set(n, (counts.get(n) ?? 0) + 1);
    const name = [...counts].sort(
      (x, y) => y[1] - x[1] || x[0].localeCompare(y[0]),
    )[0][0];
    runs.push({ from: a, to: b, name });
  };
  for (const date of dates) {
    // A break that ends on a Friday and one that starts the Monday after are one stretch.
    let joins = !!last;
    for (let d = last ? addIsoDays(last, 1) : date; joins && d < date;) {
      joins = isWeekend(d);
      d = addIsoDays(d, 1);
    }
    if (!joins) {
      close();
      start = date;
      runNames = [];
    }
    last = date;
    runNames.push(...(names.get(date) ?? []));
  }
  close();
  return runs.filter((p) => p.to >= from && p.from <= to);
}

/** The codes a country's rows carry, for a country without curated regions. */
export function storedCodes(rows: readonly SchoolRowLike[]): string[] {
  return [
    ...new Set(
      rows
        .filter((r) => r.holidayType === "school" && r.region)
        .map((r) => r.region as string),
    ),
  ].sort();
}

/**
 * The days to read a country's rows for: the range and three days either
 * side, so a break that ends just before `from` still joins its weekend.
 */
export function schoolRowsSpan(
  from: string,
  to: string,
): { from: string; to: string } {
  return { from: addIsoDays(from, -3), to: addIsoDays(to, 3) };
}

/**
 * The last day a region's own or its country's nationwide school rows reach;
 * null when neither has a row. Unlike the calendar's coverage it never falls
 * back to another region of the country: a code with no rows of its own has
 * no known breaks, not the neighbours'.
 */
export function coveredThrough(
  coverage: SchoolCoverage,
  country: string,
  code: string,
): string | null {
  const region = normalizeRegionCode(code);
  const dates = [
    region ? coverage.regional.get(`${country}|${region}`) : undefined,
    coverage.national.get(country),
  ].filter((d): d is string => !!d);
  return dates.length > 0 ? dates.sort()[dates.length - 1] : null;
}

/** The default countries whose stored school data reaches {@link MIN_COVERAGE_DAYS} ahead. */
export function defaultSchoolBreakCountries(
  coverage: SchoolCoverage,
  today: string,
): string[] {
  const horizon = addIsoDays(today, MIN_COVERAGE_DAYS);
  return DEFAULT_SCHOOL_BREAK_COUNTRIES.filter(
    (c) => (coverage.countryAny.get(c) ?? "") >= horizon,
  );
}

/** `/school-breaks`: every region of the countries, its breaks in [from, to], how far its data reaches. */
export function schoolBreakRegions(
  countries: readonly string[],
  rowsByCountry: ReadonlyMap<string, readonly SchoolRowLike[]>,
  coverage: SchoolCoverage,
  from: string,
  to: string,
): TripSchoolBreaks {
  const regions: SchoolBreakRegion[] = [];
  for (const country of countries) {
    const rows = (rowsByCountry.get(country) ?? []).filter(
      (r) => r.holidayType === "school",
    );
    const listed =
      SCHOOL_BREAK_REGIONS[country] ??
      storedCodes(rows).map((code) => ({ code, name: code }));
    for (const { code, name } of listed) {
      regions.push({
        code,
        name,
        country,
        through: coveredThrough(coverage, country, code),
        periods: schoolBreakPeriods(rows, code, from, to),
      });
    }
  }
  return { regions };
}

/** The country a `within` code belongs to. */
export const countryOfCode = (code: string): string =>
  code.slice(0, 2).toUpperCase();

/** A `within` code as `/school-breaks` lists it for its country; 400 otherwise. */
export function resolveWithin(
  code: string,
  breaks: TripSchoolBreaks,
): WhenWithin {
  const found = breaks.regions.find(
    (r) => r.code.toUpperCase() === code.toUpperCase(),
  );
  if (found) return found;
  const country = countryOfCode(code);
  const known = breaks.regions
    .filter((r) => r.country === country)
    .map((r) => r.code)
    .join(", ");
  throw new BadRequestException(
    known
      ? `within: ${code} is not a school-holiday region; ${country} has ${known}`
      : `within: no school-holiday regions for ${country}`,
  );
}
