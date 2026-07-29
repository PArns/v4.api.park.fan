/**
 * Reads the stat tables off an RCDB ride page.
 *
 * RCDB has no API and serves everything in imperial regardless of cookies, so
 * the numbers are parsed out of the server-rendered page and converted. The
 * conversion is lossless for rides that were built metric: RCDB stores one
 * canonical value and converts for display, so 49.7 mph is 80.0 km/h and
 * 2519.7 ft is 768.0 m — Black Mamba's published figures exactly. Rides built
 * imperial keep a converted value, which is why {@link snap} only rounds to a
 * whole number when the result is within a hair of one.
 *
 * Pure string work, no I/O — the network lives in `rcdb.client.ts` so this can
 * be tested against fixtures.
 */

/** Everything the stat tables carry, in metric. `null` = the page omits it. */
export interface RcdbRideStats {
  /** Track length in metres. */
  lengthM: number | null;
  /** Highest point of the track in metres. */
  heightM: number | null;
  /** Largest single drop in metres. */
  dropM: number | null;
  /** Total elevation change in metres (RCDB's "Δ Elevation"). */
  elevationM: number | null;
  /** Top speed in km/h. */
  topSpeedKmh: number | null;
  /** Ride duration in seconds. */
  durationSeconds: number | null;
  /** Maximum sustained g-force. */
  gForce: number | null;
  /** Steepest descent angle in degrees. */
  verticalAngleDeg: number | null;
  /** Inversions as RCDB counts them. */
  inversions: number | null;
  /** Theoretical throughput in riders per hour. */
  capacityPerHour: number | null;
  /** Riders per train (or per car on single-car trains). */
  ridersPerTrain: number | null;
  /** Who designed the layout, e.g. "Ing.-Büro Stengel GmbH". */
  designer: string | null;
  /** Who erected the ride, when RCDB names someone. */
  builder: string | null;
  /** Who built the trains, e.g. "Vekoma". */
  trainManufacturer: string | null;
  /** Restraint type as RCDB names it, e.g. "Shoulder harness". */
  restraints: string | null;
}

const FT_TO_M = 0.3048;
const MPH_TO_KMH = 1.609344;

/** Longest a plain-text field may be before we treat the parse as junk. */
const MAX_TEXT_LEN = 120;

const EMPTY: RcdbRideStats = {
  lengthM: null,
  heightM: null,
  dropM: null,
  elevationM: null,
  topSpeedKmh: null,
  durationSeconds: null,
  gForce: null,
  verticalAngleDeg: null,
  inversions: null,
  capacityPerHour: null,
  ridersPerTrain: null,
  designer: null,
  builder: null,
  trainManufacturer: null,
  restraints: null,
};

/**
 * Round to `decimals`, then to a whole number when we land within `tolerance`
 * of one.
 *
 * RCDB's imperial display is itself rounded, so converting back overshoots by
 * a hair: 155.3 mph comes out as 249.93 km/h for a ride published as 250. The
 * tolerance recovers the round number that was there before RCDB converted it,
 * without inventing one for a ride genuinely built in mph (55 mph stays
 * 88.5 km/h rather than becoming 89).
 */
function snap(value: number, decimals: number, tolerance: number): number {
  const factor = 10 ** decimals;
  const rounded = Math.round(value * factor) / factor;
  const whole = Math.round(rounded);
  return Math.abs(rounded - whole) <= tolerance ? whole : rounded;
}

/** Strips tags and normalises entities/whitespace to one clean line. */
function text(html: string): string {
  return html
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&#(\d+);/g, (_, code: string) =>
      String.fromCharCode(Number(code)),
    )
    .replace(/\s+/g, " ")
    .trim();
}

/** First number in the string, or null. Handles "2,519.7" and "534.8 ft". */
function num(value: string): number | null {
  const match = /-?\d[\d,]*(?:\.\d+)?/.exec(value);
  if (!match) return null;
  const parsed = Number(match[0].replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Every `<tr><th>Key<td>Value` row of every `stat-tbl` on the page, flattened
 * into one map.
 *
 * The tables are grouped under Tracks / Trains / Details headings, but the keys
 * do not collide across them — "Built by" is only ever the train builder,
 * "Builder" only ever the ride's — so a flat map keeps the callers simple.
 * Later rows win, which matters for pages listing several tracks: RCDB puts the
 * main track last.
 */
export function parseStatRows(html: string): Map<string, string> {
  const rows = new Map<string, string>();
  for (const table of html.matchAll(/<table class=stat-tbl>(.*?)<\/table>/gs)) {
    for (const row of table[1].matchAll(
      /<tr><th>([^<]+)<td>(.*?)(?=<tr>|$)/gs,
    )) {
      const key = text(row[1]);
      const value = text(row[2]);
      if (key && value) rows.set(key, value);
    }
  }
  return rows;
}

/** "1:48" → 108. Also accepts "1:02:30" for the rare long ride. */
function parseDuration(value: string): number | null {
  const parts = value.split(":").map((part) => Number(part.trim()));
  if (parts.length < 2 || parts.some((part) => !Number.isFinite(part)))
    return null;
  return parts.reduce((total, part) => total * 60 + part, 0);
}

/** Feet to metres, or null when the row has no number. */
function feetToMetres(value: string | undefined): number | null {
  const feet = value ? num(value) : null;
  return feet === null ? null : snap(feet * FT_TO_M, 1, 0.05);
}

function plainText(value: string | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  // RCDB concatenates multi-entry cells without separators ("Harry G
  // TraverHarry..."), which is unreadable and not worth guessing at.
  return trimmed.length > 0 && trimmed.length <= MAX_TEXT_LEN ? trimmed : null;
}

/**
 * Parse a ride page.
 *
 * Returns `null` when the page carries no track measurements at all — that is
 * what a manufacturer's MODEL page looks like (rcdb.com/12374.htm is Mack's
 * "Custom - Mega Coaster", with a Details table and nothing else), and writing
 * a row of nulls for one would claim we looked up a ride that does not exist.
 */
export function parseRideStats(html: string): RcdbRideStats | null {
  const rows = parseStatRows(html);

  const stats: RcdbRideStats = {
    ...EMPTY,
    lengthM: feetToMetres(rows.get("Length")),
    heightM: feetToMetres(rows.get("Height")),
    dropM: feetToMetres(rows.get("Drop")),
    elevationM: feetToMetres(rows.get("Δ Elevation")),
    inversions: rows.has("Inversions") ? num(rows.get("Inversions")!) : null,
    gForce: rows.has("G-Force") ? num(rows.get("G-Force")!) : null,
    verticalAngleDeg: rows.has("Vertical Angle")
      ? num(rows.get("Vertical Angle")!)
      : null,
    capacityPerHour: rows.has("Capacity") ? num(rows.get("Capacity")!) : null,
    designer: plainText(rows.get("Designer")),
    builder: plainText(rows.get("Builder") ?? rows.get("Installer")),
    trainManufacturer: plainText(rows.get("Built by")),
    restraints: plainText(rows.get("Restraints")),
  };

  const speed = rows.get("Speed");
  if (speed) {
    const mph = num(speed);
    // Tolerance is wider than for lengths: RCDB rounds mph to one decimal, so
    // a 250 km/h ride comes back as 249.93 and has to survive the trip home.
    if (mph !== null) stats.topSpeedKmh = snap(mph * MPH_TO_KMH, 1, 0.12);
  }

  const duration = rows.get("Duration");
  if (duration) stats.durationSeconds = parseDuration(duration);

  const arrangement = rows.get("Arrangement");
  if (arrangement) {
    // "...for a total of 24 riders per train." / "...a total of 4 riders per car."
    const riders = /total of (\d+) riders/.exec(arrangement);
    if (riders) stats.ridersPerTrain = Number(riders[1]);
  }

  const hasMeasurements =
    stats.lengthM !== null ||
    stats.heightM !== null ||
    stats.topSpeedKmh !== null ||
    stats.dropM !== null;
  return hasMeasurements ? stats : null;
}
