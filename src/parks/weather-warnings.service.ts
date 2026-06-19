import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, MoreThan, Repository } from "typeorm";
import { WeatherWarning } from "./entities/weather-warning.entity";
import { ParksService } from "./parks.service";
import { MeteoGateWarningsClient } from "../external-apis/weather/meteogate-warnings.client";
import { BrightSkyWarningsClient } from "../external-apis/weather/brightsky-warnings.client";
import { SourceWeatherWarning } from "../external-apis/weather/weather-warning.types";
import { bboxContains, pointInPolygon } from "../common/utils/geo.util";

/** A park reduced to what warning-matching needs. */
interface ParkPoint {
  id: string;
  lon: number;
  lat: number;
}

/**
 * Syncs and serves severe-weather warnings per park.
 *
 * Two sources, picked per country:
 * - **Germany → Bright Sky (DWD direct)**: point-based, returns the warncell's
 *   warnings already matched to a park's lat/lon. MeteoGate's German feed lags
 *   and drops low levels, so DWD-direct is authoritative for DE parks.
 * - **Rest of Europe → MeteoGate**: a country index whose areas we match to
 *   each park (bbox pre-filter + exact point-in-polygon).
 *
 * Either way we persist one row per park × alert, deduplicating the hourly
 * "segment" warnings some services emit (same event/area/severity sliced by
 * the hour) into one entry spanning the full window. Warnings are non-critical
 * — failures are logged and skipped, never thrown.
 */
@Injectable()
export class WeatherWarningsService {
  private readonly logger = new Logger(WeatherWarningsService.name);

  constructor(
    @InjectRepository(WeatherWarning)
    private readonly warningRepo: Repository<WeatherWarning>,
    private readonly parksService: ParksService,
    private readonly warningSource: MeteoGateWarningsClient,
    private readonly brightSky: BrightSkyWarningsClient,
  ) {}

  /** Currently-active stored warnings for a park (read path). */
  async getActiveWarnings(parkId: string): Promise<WeatherWarning[]> {
    return this.warningRepo.find({
      where: { parkId, expires: MoreThan(new Date()) },
      order: { severity: "DESC", onset: "ASC" },
    });
  }

  /**
   * Sync warnings for every park whose country the source covers. One upstream
   * query per country, then per-park area matching. Returns a small summary for
   * the cron log.
   */
  async syncWarnings(): Promise<{
    countries: number;
    activeWarnings: number;
    parkRows: number;
  }> {
    const parks = await this.parksService.findAll();

    // Group supported-country parks that have usable coordinates.
    const byCountry = new Map<string, ParkPoint[]>();
    for (const p of parks) {
      const cc = p.countryCode;
      const lat = Number(p.latitude);
      const lon = Number(p.longitude);
      if (
        !cc ||
        !Number.isFinite(lat) ||
        !Number.isFinite(lon) ||
        !this.warningSource.supportsCountry(cc)
      ) {
        continue;
      }
      const arr = byCountry.get(cc);
      const point: ParkPoint = { id: p.id, lon, lat };
      if (arr) arr.push(point);
      else byCountry.set(cc, [point]);
    }

    const now = new Date();
    let activeWarnings = 0;
    let parkRows = 0;

    for (const [cc, countryParks] of byCountry) {
      try {
        let rows: Partial<WeatherWarning>[];
        if (cc === "DE") {
          // DWD direct, point-based: warnings come pre-matched to each park.
          const collected = await this.collectBrightSky(countryParks);
          activeWarnings += collected.active;
          rows = collected.rows;
        } else {
          const warnings = await this.warningSource.getActiveWarnings(cc);
          const active = warnings.filter(
            (w) => w.expires && new Date(w.expires) > now,
          );
          activeWarnings += active.length;
          rows = await this.matchParks(cc, countryParks, active);
        }

        rows = this.dedupeWarnings(rows);
        parkRows += rows.length;

        // Atomic replace: a park's warning set is fully rebuilt each sync so a
        // lifted/expired warning disappears, with no empty window for readers.
        await this.warningRepo.manager.transaction(async (em) => {
          await em.delete(WeatherWarning, {
            parkId: In(countryParks.map((p) => p.id)),
          });
          if (rows.length > 0) await em.insert(WeatherWarning, rows);
        });
      } catch (err) {
        this.logger.warn(
          `Weather-warning sync failed for ${cc}: ${(err as Error)?.message ?? err}`,
        );
      }
    }

    this.logger.log(
      `🌩️  Weather warnings: ${activeWarnings} active across ${byCountry.size} countries → ${parkRows} park rows`,
    );
    return { countries: byCountry.size, activeWarnings, parkRows };
  }

  /** Match each park to the warning areas that contain it → per-park rows. */
  private async matchParks(
    cc: string,
    countryParks: ParkPoint[],
    active: SourceWeatherWarning[],
  ): Promise<Partial<WeatherWarning>[]> {
    // parkId → alertId → row (one row per park × alert, first matching area).
    const matches = new Map<string, Map<string, Partial<WeatherWarning>>>();

    for (const w of active) {
      for (const area of w.areas) {
        const candidates = countryParks.filter((p) =>
          bboxContains(area.bbox, p.lon, p.lat),
        );
        if (candidates.length === 0) continue;

        // Refine with the exact polygon when available; bbox-only otherwise.
        const polygon = area.geometryUrl
          ? await this.warningSource.fetchAreaGeometry(area.geometryUrl)
          : null;

        for (const p of candidates) {
          const inside = polygon ? pointInPolygon(p.lon, p.lat, polygon) : true;
          if (!inside) continue;

          let perPark = matches.get(p.id);
          if (!perPark) {
            perPark = new Map();
            matches.set(p.id, perPark);
          }
          if (!perPark.has(w.alertId)) {
            perPark.set(w.alertId, this.toRow(p.id, w, area.description));
          }
        }
      }
    }

    return Array.from(matches.values()).flatMap((m) => Array.from(m.values()));
  }

  /** Bright Sky (DWD): query each park's point; warnings come pre-matched. */
  private async collectBrightSky(
    parks: ParkPoint[],
  ): Promise<{ rows: Partial<WeatherWarning>[]; active: number }> {
    const results = await Promise.all(
      parks.map((p) =>
        this.brightSky
          .getActiveWarningsForPoint(p.lat, p.lon)
          .then((ws) => ({ p, ws }))
          .catch(() => ({ p, ws: [] as SourceWeatherWarning[] })),
      ),
    );

    const rows: Partial<WeatherWarning>[] = [];
    let active = 0;
    for (const { p, ws } of results) {
      active += ws.length;
      for (const w of ws)
        rows.push(this.toRow(p.id, w, w.areas[0]?.description));
    }
    return { rows, active };
  }

  /**
   * Collapse "segment" duplicates: some services (e.g. KNMI via MeteoGate)
   * slice one warning into many hourly CAP alerts — same event/area/severity,
   * only `expires` walks forward. Keep one row per (park, event, severity,
   * area), using the latest segment as the representative and widening the
   * window to [earliest onset, latest expires].
   */
  private dedupeWarnings(
    rows: Partial<WeatherWarning>[],
  ): Partial<WeatherWarning>[] {
    const groups = new Map<string, Partial<WeatherWarning>>();
    for (const r of rows) {
      const key = `${r.parkId}|${r.event}|${r.severity ?? ""}|${r.area ?? ""}`;
      const cur = groups.get(key);
      if (!cur) {
        groups.set(key, { ...r });
        continue;
      }
      const rExp = r.expires?.getTime() ?? 0;
      const curExp = cur.expires?.getTime() ?? 0;
      const winner = rExp > curExp ? { ...r } : { ...cur };
      const onsets = [cur.onset, r.onset]
        .filter((d): d is Date => d instanceof Date)
        .sort((a, b) => a.getTime() - b.getTime());
      if (onsets.length) winner.onset = onsets[0];
      groups.set(key, winner);
    }
    return Array.from(groups.values());
  }

  private toRow(
    parkId: string,
    w: SourceWeatherWarning,
    areaDesc?: string,
  ): Partial<WeatherWarning> {
    return {
      parkId,
      alertId: w.alertId,
      source: w.source,
      countryCode: w.countryCode,
      event: w.event,
      eventEn: w.eventEn ?? null,
      category: w.category ?? null,
      severity: w.severity ?? null,
      urgency: w.urgency ?? null,
      certainty: w.certainty ?? null,
      onset: w.onset ? new Date(w.onset) : null,
      expires: w.expires ? new Date(w.expires) : null,
      sent: w.sent ? new Date(w.sent) : null,
      headline: w.headline ?? null,
      headlineEn: w.headlineEn ?? null,
      description: w.description ?? null,
      descriptionEn: w.descriptionEn ?? null,
      instruction: w.instruction ?? null,
      instructionEn: w.instructionEn ?? null,
      area: areaDesc ?? null,
    };
  }
}
