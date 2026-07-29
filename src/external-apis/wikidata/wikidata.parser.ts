/**
 * Builds and reads the SPARQL that fetches ride measurements from Wikidata.
 *
 * Wikidata is the right source here for two reasons. It is CC0, so the numbers
 * can be stored and served — unlike the roller-coaster database we link to,
 * whose terms permit the link and not the data. And it already anchors our
 * catalogue: the `rcdbId` on every attraction came from Wikidata property
 * P2751, which is exactly the key this query joins on.
 *
 * The cost is coverage. Of ~1740 Wikidata items carrying an RCDB id, only
 * ~150-220 state a speed, height or length, so most rides return nothing. That
 * is a fact about the source, not a bug: a ride with no numbers simply keeps
 * none.
 *
 * Pure string work, no I/O — the network lives in `wikidata.client.ts`.
 */

/** A ride's measurements, in metric. `null` = Wikidata does not state it. */
export interface WikidataRideStats {
  /** Wikidata entity id the numbers came from, e.g. "Q319081". */
  entityId: string;
  /** Top speed in km/h. */
  topSpeedKmh: number | null;
  /** Highest point in metres. */
  heightM: number | null;
  /** Track length in metres. */
  lengthM: number | null;
  /** Ride duration in seconds. */
  durationSeconds: number | null;
}

/** One row of a SPARQL JSON result. */
interface Binding {
  [key: string]: { value: string } | undefined;
}

export interface SparqlResults {
  results?: { bindings?: Binding[] };
}

const MS_TO_KMH = 3.6;

/**
 * How many ids go into one query.
 *
 * A `VALUES` block of a few hundred is comfortable for the public endpoint and
 * keeps the whole catalogue to a handful of requests rather than one per ride.
 */
export const IDS_PER_QUERY = 200;

/** Round to `decimals`, dropping a trailing `.0`. */
function round(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function num(binding: Binding, key: string): number | null {
  const raw = binding[key]?.value;
  if (raw === undefined) return null;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * The SPARQL for one batch of RCDB ids.
 *
 * Every measurement is read through `psn:` — the NORMALISED value — so Wikidata
 * converts to SI before we ever see it. A ride entered in mph and one entered
 * in km/h both arrive as metres per second, which is why this file has no unit
 * table and cannot get one wrong.
 */
export function buildStatsQuery(rcdbIds: readonly (string | number)[]): string {
  // Ids come from our own database and are integers, but they are going into a
  // query string: anything that is not digits has no business being there.
  const values = rcdbIds
    .map((id) => String(id))
    .filter((id) => /^\d+$/.test(id))
    .map((id) => `"${id}"`)
    .join(" ");

  return `SELECT ?rcdb ?ride ?speedSi ?heightSi ?lengthSi ?durationSi WHERE {
  VALUES ?rcdb { ${values} }
  ?ride wdt:P2751 ?rcdb .
  OPTIONAL { ?ride p:P2052/psn:P2052 [ wikibase:quantityAmount ?speedSi ] . }
  OPTIONAL { ?ride p:P2048/psn:P2048 [ wikibase:quantityAmount ?heightSi ] . }
  OPTIONAL { ?ride p:P2043/psn:P2043 [ wikibase:quantityAmount ?lengthSi ] . }
  OPTIONAL { ?ride p:P2047/psn:P2047 [ wikibase:quantityAmount ?durationSi ] . }
}`;
}

/**
 * Map a SPARQL result to stats per RCDB id.
 *
 * Rows with no measurement at all are dropped rather than stored as a row of
 * nulls: "Wikidata has an entry for this ride and says nothing about it" and
 * "we imported nothing" are the same outcome, and the second one leaves the
 * ride eligible for a later run that finds something.
 */
export function parseStatsResults(
  results: SparqlResults,
): Map<string, WikidataRideStats> {
  const byRcdbId = new Map<string, WikidataRideStats>();

  for (const binding of results.results?.bindings ?? []) {
    const rcdbId = binding.rcdb?.value;
    const entity = binding.ride?.value;
    if (!rcdbId || !entity) continue;

    const speedSi = num(binding, "speedSi");
    const stats: WikidataRideStats = {
      entityId: entity.split("/").pop() ?? entity,
      // m/s → km/h. 22.2222… comes home as exactly 80.
      topSpeedKmh: speedSi === null ? null : round(speedSi * MS_TO_KMH, 1),
      heightM: num(binding, "heightSi"),
      lengthM: num(binding, "lengthSi"),
      durationSeconds: num(binding, "durationSi"),
    };

    const hasAny =
      stats.topSpeedKmh !== null ||
      stats.heightM !== null ||
      stats.lengthM !== null ||
      stats.durationSeconds !== null;
    if (!hasAny) continue;

    // A ride can have several statements (a rebuild, a differing source). The
    // first row that carries numbers wins — merging them would invent a ride
    // that is one entry's height and another's speed.
    if (!byRcdbId.has(rcdbId)) byRcdbId.set(rcdbId, stats);
  }

  return byRcdbId;
}
