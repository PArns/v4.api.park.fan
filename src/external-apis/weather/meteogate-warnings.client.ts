import { Injectable, Logger, Inject } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  SourceWeatherWarning,
  WarningArea,
  WeatherWarningSource,
} from "./weather-warning.types";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";

/**
 * MeteoGate weather-warning client.
 *
 * MeteoGate (api.meteogate.eu) is EUMETNET's open gateway; its `warnings`
 * dataset is the MeteoAlarm/EMMA CAP feed sourced from the national services
 * (DWD for Germany). Covers ~40 European countries.
 *
 * Flow (see weather-warning.types for the normalized shape):
 *  1. GET /warnings/collections/warnings/locations/{ISO}?datetime=now-24h/now
 *     (apikey header) → a GeoJSON index, capped at 100 features/page, with one
 *     feature per (alert × area × info-language). `datetime` is REQUIRED, must
 *     be a range ≤ 24h, and filters by the warning's *sent* time — so a
 *     trailing-24h window returns the currently-published warnings (a future
 *     window yields 204). 204/empty ⇒ no warnings.
 *  2. Dedupe features by `alertId`; each feature's `links` array has the CAP as
 *     JSON (rel="json"), plus the exact area polygon (rel="geometry").
 *  3. Fetch the CAP JSON once per alert; parse the de/en `info` blocks. Build
 *     areas from the index features (bbox + polygon link), enriched with
 *     areaDesc/geocodes from the CAP via the feature's indexInfo/indexArea.
 *
 * Warnings are non-critical: every failure path returns [] (fail soft) so a
 * MeteoGate outage never breaks the weather response.
 */
@Injectable()
export class MeteoGateWarningsClient implements WeatherWarningSource {
  readonly name = "meteogate";
  private readonly logger = new Logger(MeteoGateWarningsClient.name);
  private readonly client: AxiosInstance;
  private readonly baseUrl = "https://api.meteogate.eu/warnings";
  private readonly token?: string;

  // Circuit breaker: opened after an upstream failure so we fail fast (return
  // []) during a MeteoGate outage instead of stalling every weather sync.
  private readonly CIRCUIT_KEY = "ratelimit:meteogate:circuit";
  private readonly CIRCUIT_COOLDOWN = 60; // seconds
  private readonly CACHE_TTL = 10 * 60; // warnings refresh ~15 min; cache 10 min
  private readonly MAX_PAGES = 10; // 10×100 index entries is plenty per country

  // Countries MeteoGate/MeteoAlarm issues warnings for (the /locations list).
  // ISO alpha-2, except the UK is "UK" upstream (ISO "GB" → normalised below).
  private readonly SUPPORTED = new Set([
    "AD",
    "AT",
    "BA",
    "BE",
    "BG",
    "CH",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IL",
    "IS",
    "IT",
    "LT",
    "LU",
    "LV",
    "MD",
    "ME",
    "MK",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "RS",
    "SE",
    "SI",
    "SK",
    "UA",
    "UK",
  ]);

  private readonly inflight = new Map<
    string,
    Promise<SourceWeatherWarning[]>
  >();

  constructor(
    private readonly configService: ConfigService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {
    this.token = this.configService.get<string>("METEOALARM_API_TOKEN");
    this.client = axios.create({
      baseURL: this.baseUrl,
      timeout: 20000,
      headers: this.token
        ? { ...BROWSER_HEADERS, apikey: this.token }
        : { ...BROWSER_HEADERS },
    });
    if (!this.token) {
      this.logger.warn(
        "METEOALARM_API_TOKEN not set — MeteoGate weather warnings disabled.",
      );
    }
  }

  /** ISO alpha-2 → the code MeteoAlarm uses (UK, not GB). */
  private normalizeCountry(countryCode: string): string {
    const cc = (countryCode || "").toUpperCase();
    return cc === "GB" ? "UK" : cc;
  }

  supportsCountry(countryCode: string): boolean {
    return (
      !!this.token && this.SUPPORTED.has(this.normalizeCountry(countryCode))
    );
  }

  async getActiveWarnings(
    countryCode: string,
  ): Promise<SourceWeatherWarning[]> {
    const cc = this.normalizeCountry(countryCode);
    if (!this.supportsCountry(countryCode)) return [];

    const cacheKey = `weather:warnings:meteogate:${cc}`;
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached !== null) return JSON.parse(cached) as SourceWeatherWarning[];
    } catch {
      // cache read failure → fall through to a live fetch
    }

    // Singleflight: one live fetch per country across concurrent callers.
    const existing = this.inflight.get(cc);
    if (existing) return existing;

    const promise = this.fetchCountry(cc)
      .then(async (warnings) => {
        await this.redis
          .set(cacheKey, JSON.stringify(warnings), "EX", this.CACHE_TTL)
          .catch(() => undefined);
        return warnings;
      })
      .catch((err) => {
        this.logger.warn(
          `MeteoGate warnings fetch failed for ${cc}: ${err?.message ?? err}`,
        );
        return [] as SourceWeatherWarning[];
      })
      .finally(() => this.inflight.delete(cc));

    this.inflight.set(cc, promise);
    return promise;
  }

  private async fetchCountry(cc: string): Promise<SourceWeatherWarning[]> {
    if (await this.redis.get(this.CIRCUIT_KEY)) return []; // fail fast, soft

    let features: WarningIndexFeature[];
    try {
      features = await this.fetchIndex(cc);
    } catch (err) {
      await this.redis
        .set(this.CIRCUIT_KEY, "1", "EX", this.CIRCUIT_COOLDOWN)
        .catch(() => undefined);
      throw err;
    }
    if (features.length === 0) return [];

    // Group index features by alert id.
    const byAlert = new Map<string, WarningIndexFeature[]>();
    for (const f of features) {
      const id = f.properties?.alertId;
      if (!id) continue;
      const arr = byAlert.get(id);
      if (arr) arr.push(f);
      else byAlert.set(id, [f]);
    }

    const warnings = await Promise.all(
      Array.from(byAlert.entries()).map(([alertId, feats]) =>
        this.buildWarning(cc, alertId, feats).catch(() => null),
      ),
    );
    return warnings.filter((w): w is SourceWeatherWarning => w !== null);
  }

  /** Fetch the paginated locations index for a country (trailing-24h window). */
  private async fetchIndex(cc: string): Promise<WarningIndexFeature[]> {
    const now = new Date();
    const start = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const datetime = `${start.toISOString()}/${now.toISOString()}`;

    const all: WarningIndexFeature[] = [];
    for (let page = 1; page <= this.MAX_PAGES; page++) {
      const res = await this.client.get(
        `/collections/warnings/locations/${cc}`,
        {
          params: { datetime, page },
          validateStatus: (s) => s === 200 || s === 204,
        },
      );
      // 204 No Content ⇒ no warnings (data is empty).
      const feats: WarningIndexFeature[] =
        res.status === 204 || !res.data ? [] : (res.data.features ?? []);
      all.push(...feats);
      if (feats.length < 100) break; // last page
    }
    return all;
  }

  private async buildWarning(
    cc: string,
    alertId: string,
    feats: WarningIndexFeature[],
  ): Promise<SourceWeatherWarning | null> {
    const capUrl = feats[0].links?.find((l) => l.rel === "json")?.href;
    if (!capUrl) return null;

    // CAP JSON lives on a presigned object store — fetch WITHOUT our apikey.
    const cap = (
      await axios.get(capUrl, {
        timeout: 20000,
        headers: { ...BROWSER_HEADERS },
      })
    ).data as CapAlert;
    if (!cap || cap.status !== "Actual") return null; // skip test/system/cancel

    const byLang = this.indexInfoByLanguage(cap.info ?? []);
    // Prefer German, then English — never the issuing country's local language
    // (a Dutch KNMI warning then reads English for our de/en users, not Dutch).
    const de = byLang.get("de") ?? byLang.get("en") ?? cap.info?.[0];
    const en = byLang.get("en");
    if (!de) return null;

    // MeteoAlarm emits a CAP message per awareness type even when there is NO
    // warning (awareness_level "green", e.g. "Geen waarschuwingen"). Those are
    // all-clears, not warnings — skip them (else a park gets ~7 noise entries).
    if (isNoWarning(de)) return null;

    const areas = this.buildAreas(feats, cap);

    return {
      source: this.name,
      alertId,
      countryCode: cc,
      event: de.event ?? en?.event ?? "",
      eventEn: en?.event,
      category: de.category?.[0],
      severity: de.severity,
      urgency: de.urgency,
      certainty: de.certainty,
      onset: de.onset,
      expires: de.expires,
      sent: cap.sent,
      headline: de.headline,
      headlineEn: en?.headline,
      description: de.description,
      descriptionEn: en?.description,
      instruction: de.instruction,
      instructionEn: en?.instruction,
      areas,
    };
  }

  /** First CAP info block per language prefix (e.g. "de-DE" → "de"). */
  private indexInfoByLanguage(info: CapInfo[]): Map<string, CapInfo> {
    const map = new Map<string, CapInfo>();
    for (const i of info) {
      const lang = (i.language ?? "").slice(0, 2).toLowerCase();
      if (lang && !map.has(lang)) map.set(lang, i);
    }
    return map;
  }

  /**
   * Affected areas for matching a park. Each index feature is one area of the
   * alert; its geometry is a bbox and its `links` hold the exact polygon. We
   * enrich with the CAP areaDesc/geocodes via the feature's indexInfo/indexArea.
   */
  private buildAreas(
    feats: WarningIndexFeature[],
    cap: CapAlert,
  ): WarningArea[] {
    const seen = new Set<string>();
    const areas: WarningArea[] = [];
    for (const f of feats) {
      const bbox = bboxOfPolygon(f.geometry);
      const geometryUrl = f.links?.find((l) => l.rel === "geometry")?.href;
      const key = `${bbox?.join(",") ?? ""}|${geometryUrl ?? ""}`;
      if (seen.has(key)) continue;
      seen.add(key);

      const p = f.properties ?? {};
      const capArea = cap.info?.[p.indexInfo ?? -1]?.area?.[p.indexArea ?? -1];
      areas.push({
        description: capArea?.areaDesc,
        geocodes: capArea?.geocode
          ? Object.fromEntries(
              capArea.geocode
                .filter((g) => g.valueName && g.value)
                .map((g) => [g.valueName, g.value]),
            )
          : undefined,
        bbox,
        geometryUrl,
      });
    }
    return areas;
  }

  /**
   * Fetch the exact area polygon (geo+json) for precise point-in-polygon
   * matching. Cached by the stable object path — the signed URL's query string
   * varies per request, so we key on the path only. Returns the GeoJSON
   * geometry ({ type, coordinates }), or null on any failure.
   */
  async fetchAreaGeometry(
    url: string,
  ): Promise<{ type?: string; coordinates?: unknown } | null> {
    const cacheKey = `weather:warngeom:${url.split("?")[0]}`;
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached !== null) return cached === "" ? null : JSON.parse(cached);
    } catch {
      // cache miss path below
    }
    let geometry: { type?: string; coordinates?: unknown } | null = null;
    try {
      const data = (
        await axios.get(url, {
          timeout: 20000,
          headers: { ...BROWSER_HEADERS },
        })
      ).data as {
        type?: string;
        geometry?: { type?: string; coordinates?: unknown };
        features?: { geometry?: { type?: string; coordinates?: unknown } }[];
        coordinates?: unknown;
      };
      const geom =
        data?.type === "Feature"
          ? data.geometry
          : data?.type === "FeatureCollection"
            ? data.features?.[0]?.geometry
            : (data as { type?: string; coordinates?: unknown });
      geometry = geom?.coordinates ? geom : null;
    } catch {
      geometry = null;
    }
    await this.redis
      .set(cacheKey, geometry ? JSON.stringify(geometry) : "", "EX", 15 * 60)
      .catch(() => undefined);
    return geometry;
  }
}

// --- minimal upstream shapes (only the fields we read) ---

interface WarningIndexFeature {
  geometry?: { type?: string; coordinates?: number[][][] };
  links?: { rel?: string; type?: string; href?: string }[];
  properties?: {
    alertId?: string;
    indexInfo?: number;
    indexArea?: number;
  };
}

interface CapAlert {
  status?: string;
  msgType?: string;
  sent?: string;
  info?: CapInfo[];
}

interface CapInfo {
  language?: string;
  category?: string[];
  event?: string;
  urgency?: string;
  severity?: string;
  certainty?: string;
  onset?: string;
  expires?: string;
  headline?: string;
  description?: string;
  instruction?: string;
  parameter?: { valueName?: string; value?: string }[];
  area?: {
    areaDesc?: string;
    geocode?: { valueName?: string; value?: string }[];
  }[];
}

/**
 * MeteoAlarm tags each CAP message with an `awareness_level` parameter,
 * e.g. "1; green; Minor" (no warning) … "4; red; Extreme". The green level is
 * an all-clear ("Geen waarschuwingen"), not a real warning, so we drop it.
 */
function isNoWarning(info: CapInfo): boolean {
  const level = info.parameter?.find(
    (p) => p.valueName === "awareness_level",
  )?.value;
  return !!level && /green/i.test(level);
}

/** Compute [west, south, east, north] from a (bbox) Polygon's outer ring. */
function bboxOfPolygon(geom?: {
  type?: string;
  coordinates?: number[][][];
}): [number, number, number, number] | undefined {
  const ring = geom?.coordinates?.[0];
  if (!ring || ring.length === 0) return undefined;
  let w = Infinity,
    s = Infinity,
    e = -Infinity,
    n = -Infinity;
  for (const [lon, lat] of ring) {
    if (lon < w) w = lon;
    if (lon > e) e = lon;
    if (lat < s) s = lat;
    if (lat > n) n = lat;
  }
  return Number.isFinite(w) ? [w, s, e, n] : undefined;
}
