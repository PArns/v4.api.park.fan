import { Injectable, Logger, Inject } from "@nestjs/common";
import axios, { AxiosInstance } from "axios";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { SourceWeatherWarning } from "./weather-warning.types";

/**
 * Bright Sky weather-warning client — DWD (Germany) direct.
 *
 * MeteoGate/MeteoAlarm's German feed lags badly and drops the lower awareness
 * levels (verified: it served only expired DE alerts while DWD had an active
 * EXTREME-HEAT warning for Brühl). Bright Sky (api.brightsky.dev) is a free,
 * no-key proxy for DWD's official CAP warnings, so it is the authoritative
 * source for German parks.
 *
 * Unlike MeteoGate (country index + area matching), Bright Sky's `/alerts`
 * endpoint is **point-based**: querying with a park's lat/lon returns exactly
 * the warnings for the DWD warncell containing it — already location-matched,
 * so no bbox/point-in-polygon step is needed. Fail-soft like the other source.
 */
@Injectable()
export class BrightSkyWarningsClient {
  readonly name = "brightsky";
  private readonly logger = new Logger(BrightSkyWarningsClient.name);
  private readonly client: AxiosInstance;

  private readonly CIRCUIT_KEY = "ratelimit:brightsky:circuit";
  private readonly CIRCUIT_COOLDOWN = 60; // seconds
  private readonly CACHE_TTL = 10 * 60; // warnings refresh ~15 min; cache 10 min

  private readonly inflight = new Map<
    string,
    Promise<SourceWeatherWarning[]>
  >();

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {
    this.client = axios.create({
      baseURL: "https://api.brightsky.dev",
      timeout: 20000,
    });
  }

  /**
   * Active DWD warnings for the warncell containing (lat, lon). The warnings
   * are already matched to the location, so the single `area` carries the
   * warncell name/id and no geometry is needed. Returns `[]` on any failure.
   */
  async getActiveWarningsForPoint(
    lat: number,
    lon: number,
  ): Promise<SourceWeatherWarning[]> {
    const key = `${lat.toFixed(3)},${lon.toFixed(3)}`;
    const cacheKey = `weather:warnings:brightsky:${key}`;
    try {
      const cached = await this.redis.get(cacheKey);
      if (cached !== null) return JSON.parse(cached) as SourceWeatherWarning[];
    } catch {
      // cache miss path below
    }

    const existing = this.inflight.get(key);
    if (existing) return existing;

    const promise = this.fetchPoint(lat, lon)
      .then(async (ws) => {
        await this.redis
          .set(cacheKey, JSON.stringify(ws), "EX", this.CACHE_TTL)
          .catch(() => undefined);
        return ws;
      })
      .catch((err) => {
        this.logger.warn(
          `Bright Sky warnings fetch failed for ${key}: ${err?.message ?? err}`,
        );
        return [] as SourceWeatherWarning[];
      })
      .finally(() => this.inflight.delete(key));

    this.inflight.set(key, promise);
    return promise;
  }

  private async fetchPoint(
    lat: number,
    lon: number,
  ): Promise<SourceWeatherWarning[]> {
    if (await this.redis.get(this.CIRCUIT_KEY)) return []; // fail fast, soft

    let data: BrightSkyAlertsResponse;
    try {
      data = (await this.client.get("/alerts", { params: { lat, lon } }))
        .data as BrightSkyAlertsResponse;
    } catch (err) {
      await this.redis
        .set(this.CIRCUIT_KEY, "1", "EX", this.CIRCUIT_COOLDOWN)
        .catch(() => undefined);
      throw err;
    }

    const loc = data?.location ?? {};
    const areaName = loc.name;
    const warncell =
      loc.warn_cell_id != null ? String(loc.warn_cell_id) : undefined;
    const now = Date.now();

    const out: SourceWeatherWarning[] = [];
    for (const a of data?.alerts ?? []) {
      if (a?.status && a.status.toLowerCase() !== "actual") continue;
      if (!a?.expires || Date.parse(a.expires) <= now) continue; // active only
      out.push({
        source: this.name,
        alertId: a.alert_id ?? String(a.id),
        countryCode: "DE",
        event: a.event_de ?? a.event_en ?? "",
        eventEn: a.event_en ?? undefined,
        category: a.category ?? undefined,
        severity: capitalize(a.severity),
        urgency: capitalize(a.urgency),
        certainty: capitalize(a.certainty),
        onset: a.onset ?? undefined,
        expires: a.expires ?? undefined,
        sent: a.effective ?? a.onset ?? undefined,
        headline: a.headline_de ?? undefined,
        headlineEn: a.headline_en ?? undefined,
        description: a.description_de ?? undefined,
        descriptionEn: a.description_en ?? undefined,
        instruction: cleanInstruction(a.instruction_de),
        instructionEn: cleanInstruction(a.instruction_en),
        areas: [
          {
            description: areaName,
            geocodes: warncell ? { WARNCELLID: warncell } : undefined,
          },
        ],
      });
    }
    return out;
  }
}

/** CAP-style capitalization (Bright Sky returns lower-case severity/urgency). */
function capitalize(s?: string | null): string | undefined {
  if (!s) return undefined;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * Drop DWD's auto-translation boilerplate instruction. Their English heat
 * warnings carry no real advice — just a disclaimer ("…automatically generated
 * product. The manually created original text warning is only available in
 * German…"). Showing that as "safety advice" is misleading, so suppress it (the
 * German instruction has the genuine advice and is kept).
 */
function cleanInstruction(s?: string | null): string | undefined {
  if (!s) return undefined;
  const low = s.toLowerCase();
  if (
    low.includes("automatically generated product") ||
    low.includes("only available in german")
  ) {
    return undefined;
  }
  return s;
}

// --- minimal upstream shape (only the fields we read) ---

interface BrightSkyAlertsResponse {
  location?: { name?: string; warn_cell_id?: number | string };
  alerts?: Array<{
    id?: number | string;
    alert_id?: string;
    status?: string;
    category?: string;
    severity?: string;
    urgency?: string;
    certainty?: string;
    onset?: string;
    expires?: string;
    effective?: string;
    event_de?: string;
    event_en?: string;
    headline_de?: string;
    headline_en?: string;
    description_de?: string;
    description_en?: string;
    instruction_de?: string;
    instruction_en?: string;
  }>;
}
