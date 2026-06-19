/**
 * Severe-weather warning types, normalized across sources.
 *
 * First source: MeteoGate (EUMETNET gateway → MeteoAlarm/national services,
 * e.g. DWD for Germany). Others (NWS for the US, …) can be added behind the
 * {@link WeatherWarningSource} interface without touching the service/storage.
 */

/** A geographic area affected by a warning — used to match a park's location. */
export interface WarningArea {
  /** Human-readable area name (CAP areaDesc), e.g. "Stadt Mönchengladbach". */
  description?: string;
  /** CAP geocodes, e.g. { EMMA_ID: "DE114", WARNCELLID: "105116000" }. */
  geocodes?: Record<string, string>;
  /**
   * Bounding box [west, south, east, north] in WGS84 — the index feature's
   * coarse geometry. Good enough for a first park↔warning match; the exact
   * polygon (see {@link geometryUrl}) refines it.
   */
  bbox?: [number, number, number, number];
  /** Link to the exact area polygon (geo+json) for precise point-in-polygon. */
  geometryUrl?: string;
}

/** A weather warning normalized across sources. */
export interface SourceWeatherWarning {
  /** Source identifier, e.g. "meteogate". */
  source: string;
  /** Stable per-warning id (CAP alert id) — used for upsert/dedup. */
  alertId: string;
  /** ISO alpha-2 country the warning was issued for. */
  countryCode: string;

  // --- CAP content (German preferred, English where available) ---
  /** Event type, localized (de), e.g. "Hitzewarnung". */
  event: string;
  eventEn?: string;
  /** CAP category (Met/Geo/Safety/…) or awareness type. */
  category?: string;
  /** CAP severity: Minor | Moderate | Severe | Extreme. */
  severity?: string;
  urgency?: string;
  certainty?: string;
  /** Validity window (ISO 8601). */
  onset?: string;
  expires?: string;
  /** Issue time (ISO 8601). */
  sent?: string;
  headline?: string;
  headlineEn?: string;
  description?: string;
  descriptionEn?: string;
  instruction?: string;
  instructionEn?: string;

  /** Affected areas (for matching to a park). */
  areas: WarningArea[];
}

/**
 * A pluggable severe-weather warning source. Sources are queried by country
 * (every park has a `countryCode`); the caller matches a specific park to a
 * returned warning via the area geometry/geocodes.
 */
export interface WeatherWarningSource {
  /** Source identifier, e.g. "meteogate". */
  readonly name: string;
  /** True if this source issues warnings for the given ISO alpha-2 country. */
  supportsCountry(countryCode: string): boolean;
  /**
   * Active warnings issued for a country, deduplicated by alert id, each with
   * its affected areas. Returns `[]` when there are none or the source is
   * unavailable/unconfigured — warnings are non-critical, so callers fail soft.
   */
  getActiveWarnings(countryCode: string): Promise<SourceWeatherWarning[]>;
}
