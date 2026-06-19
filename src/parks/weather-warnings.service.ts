import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, MoreThan, Repository } from "typeorm";
import { WeatherWarning } from "./entities/weather-warning.entity";
import { ParksService } from "./parks.service";
import { MeteoGateWarningsClient } from "../external-apis/weather/meteogate-warnings.client";
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
 * The source ({@link MeteoGateWarningsClient}) returns a country's active
 * warnings with their affected areas; here we match each park to the area(s)
 * that contain it (bbox pre-filter + exact point-in-polygon) and persist one
 * row per park × alert. Warnings are non-critical — failures are logged and
 * skipped, never thrown.
 */
@Injectable()
export class WeatherWarningsService {
  private readonly logger = new Logger(WeatherWarningsService.name);

  constructor(
    @InjectRepository(WeatherWarning)
    private readonly warningRepo: Repository<WeatherWarning>,
    private readonly parksService: ParksService,
    private readonly warningSource: MeteoGateWarningsClient,
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
        const warnings = await this.warningSource.getActiveWarnings(cc);
        const active = warnings.filter(
          (w) => w.expires && new Date(w.expires) > now,
        );
        activeWarnings += active.length;

        const rows = await this.matchParks(cc, countryParks, active);
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
