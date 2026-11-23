import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, In, IsNull, Not, Repository } from "typeorm";
import { OpenMeteoClient } from "../../external-apis/weather/open-meteo.client";
import { Park } from "../entities/park.entity";
import { WeatherData } from "../entities/weather-data.entity";
import { AttractionWeatherClosureProfile } from "../entities/attraction-weather-closure-profile.entity";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";
import {
  addIsoDays,
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getYesterdayDateInTimezone,
} from "../../common/utils/date.util";
import {
  buildWeatherClosureSamples,
  decideWeatherClosure,
  isPublishable,
  learnWeatherClosureProfiles,
  MIN_PUBLISHED_CLOSED_SHARE_LOWER,
  MIN_PUBLISHED_PROBABILITY,
  RideHourState,
  SNOW_MIN_CM,
  WeatherClosureProfile,
  WeatherClosureVerdict,
} from "../utils/weather-closure.util";
import {
  WEATHER_CLOSURE_CURRENT_WINDOW_SQL,
  WEATHER_CLOSURE_HOURS_SQL,
} from "../utils/weather-closure.sql";

/** A year: one winter behind every profile, and the summer that sets the baseline. */
const LOOKBACK_DAYS = 365;
/** Measured at 1.4 s for Europa-Park on production; generous for the largest parks. */
const JOB_LIMITS: StatementLimits = {
  statementTimeoutMs: 120_000,
  lockTimeoutMs: 5_000,
};
const REQUEST_LIMITS: StatementLimits = {
  statementTimeoutMs: 2_000,
  lockTimeoutMs: 1_000,
};

/** A whole-table read for the admin page; a few thousand rows at most. */
const REQUEST_LIMITS_LIST: StatementLimits = {
  statementTimeoutMs: 10_000,
  lockTimeoutMs: 1_000,
};
/** Below the bar but worth watching. */
const NEAR_PROBABILITY = 0.75;

const WEATHER_CLOSURE_PROFILES_LIST_SQL = `
  SELECT p."attractionId", a.name AS "attractionName", a.slug AS "attractionSlug",
         p."parkId", pk.name AS "parkName", pk.slug AS "parkSlug",
         CASE WHEN pk."continentSlug" IS NOT NULL AND pk."countrySlug" IS NOT NULL
                   AND pk."citySlug" IS NOT NULL
              THEN pk."continentSlug" || '/' || pk."countrySlug" || '/' || pk."citySlug" || '/' || pk.slug
         END AS "parkPath",
         p.cold, p.snow, p."generatedAt"
    FROM attraction_weather_closure_profiles p
    JOIN attractions a ON a.id = p."attractionId"
    JOIN parks pk ON pk.id = p."parkId"
`;

export type WeatherClosureProfileWithFlag = WeatherClosureProfile & {
  publishable: boolean;
};

export interface WeatherClosureOverviewRide {
  attractionId: string;
  attractionName: string;
  attractionSlug: string;
  parkId: string;
  parkName: string;
  parkSlug: string;
  /** `continent/country/city/park`, for the public URL; null when a slug is missing. */
  parkPath: string | null;
  publishable: boolean;
  bestProbability: number;
  cold: WeatherClosureProfileWithFlag | null;
  snow: WeatherClosureProfileWithFlag | null;
}

export interface WeatherClosureOverview {
  bar: { probability: number; closedShareLower: number };
  /** The newest profile's computation; null when the job has never run. */
  generatedAt: string | null;
  /** `near`: not shown, best probability at least 0.75. */
  totals: { rides: number; publishable: number; near: number };
  rides: WeatherClosureOverviewRide[];
}

/**
 * Learns per ride whether cold or snow shuts it, and says so for a ride that
 * is shut during opening hours now. The method is on `weather-closure.util.ts`.
 */
@Injectable()
export class WeatherClosureService {
  private readonly logger = new Logger(WeatherClosureService.name);

  constructor(
    private readonly dataSource: DataSource,
    @InjectRepository(Park) private readonly parkRepository: Repository<Park>,
    @InjectRepository(WeatherData)
    private readonly weatherDataRepository: Repository<WeatherData>,
    @InjectRepository(AttractionWeatherClosureProfile)
    private readonly profileRepository: Repository<AttractionWeatherClosureProfile>,
    private readonly openMeteo: OpenMeteoClient,
  ) {}

  /**
   * Rebuilds every park's profiles, one park at a time: the archive is rate
   * limited and the hour scan decompresses a year of `queue_data` chunks. A
   * park that fails keeps last week's rows.
   */
  async rebuildAll(asOf: Date = new Date()): Promise<{
    parks: number;
    failed: number;
    profiles: number;
    publishable: number;
  }> {
    const parks = await this.parkRepository.find({
      where: {
        retiredAt: IsNull(),
        latitude: Not(IsNull()),
        longitude: Not(IsNull()),
        timezone: Not(IsNull()),
      },
      select: {
        id: true,
        slug: true,
        timezone: true,
        latitude: true,
        longitude: true,
      },
    });
    const totals = { parks: 0, failed: 0, profiles: 0, publishable: 0 };
    for (const park of parks) {
      try {
        const r = await this.rebuildPark(park, asOf);
        totals.parks += 1;
        totals.profiles += r.profiles;
        totals.publishable += r.publishable;
      } catch (error) {
        totals.failed += 1;
        this.logger.warn(
          `Weather-closure profiles for ${park.slug} failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
    this.logger.log(
      `Weather-closure profiles: ${totals.parks} parks, ${totals.failed} failed, ` +
        `${totals.profiles} rides with a profile, ${totals.publishable} publishable`,
    );
    return totals;
  }

  async rebuildPark(
    park: Pick<Park, "id" | "timezone" | "latitude" | "longitude">,
    asOf: Date,
  ): Promise<{ profiles: number; publishable: number }> {
    const from = new Date(asOf.getTime() - LOOKBACK_DAYS * 24 * 3600 * 1000);
    // The scan first: a park with no opening hours or no readings must not
    // spend archive quota.
    const rows = await queryWithLimits<
      Array<{ attractionId: string; hourStart: Date | string; closed: boolean }>
    >(
      this.dataSource,
      WEATHER_CLOSURE_HOURS_SQL,
      [park.id, from, asOf, park.timezone],
      JOB_LIMITS,
    );
    const profiles: AttractionWeatherClosureProfile[] = [];
    let publishable = 0;
    if (rows.length > 0) {
      const weather = await this.openMeteo.fetchHourlyArchive(
        park.latitude!,
        park.longitude!,
        formatInParkTimezone(from, park.timezone),
        addIsoDays(getCurrentDateInTimezone(park.timezone), -1),
      );
      const states: RideHourState[] = rows.map((r) => ({
        attractionId: r.attractionId,
        hourStart: new Date(r.hourStart),
        closed: r.closed,
      }));
      const samples = buildWeatherClosureSamples(states, weather, (d) =>
        formatInParkTimezone(d, park.timezone),
      );
      for (const [attractionId, list] of samples) {
        const learned = learnWeatherClosureProfiles(list);
        if (!learned.cold && !learned.snow) continue;
        if (isPublishable(learned.cold) || isPublishable(learned.snow)) {
          publishable += 1;
        }
        profiles.push(
          this.profileRepository.create({
            attractionId,
            parkId: park.id,
            cold: learned.cold,
            snow: learned.snow,
            generatedAt: asOf,
          }),
        );
      }
    }

    await this.dataSource.transaction(async (manager) => {
      await manager.delete(AttractionWeatherClosureProfile, {
        parkId: park.id,
      });
      if (profiles.length > 0) await manager.save(profiles, { chunk: 500 });
    });
    return { profiles: profiles.length, publishable };
  }

  /**
   * Every stored profile with its ride and park, for the admin page and the
   * MCP tool: which rides the weather shuts, how close each is to the bar.
   *
   * @param minProbability - Leave out rides whose best profile is below this.
   */
  async listProfiles(minProbability = 0.5): Promise<WeatherClosureOverview> {
    const rows: Array<{
      attractionId: string;
      attractionName: string;
      attractionSlug: string;
      parkId: string;
      parkName: string;
      parkSlug: string;
      parkPath: string | null;
      cold: WeatherClosureProfile | null;
      snow: WeatherClosureProfile | null;
      generatedAt: Date | string;
    }> = await queryWithLimits(
      this.dataSource,
      WEATHER_CLOSURE_PROFILES_LIST_SQL,
      [],
      REQUEST_LIMITS_LIST,
    );
    const withFlag = (p: WeatherClosureProfile | null) =>
      p ? { ...p, publishable: isPublishable(p) } : null;
    let generatedAt: Date | null = null;
    let publishable = 0;
    let near = 0;
    const rides: WeatherClosureOverviewRide[] = [];
    for (const r of rows) {
      const at = new Date(r.generatedAt);
      if (!generatedAt || at > generatedAt) generatedAt = at;
      const cold = withFlag(r.cold);
      const snow = withFlag(r.snow);
      const shown = !!(cold?.publishable || snow?.publishable);
      const best = Math.max(
        cold?.attributableShare ?? 0,
        snow?.attributableShare ?? 0,
      );
      if (shown) publishable += 1;
      else if (best >= NEAR_PROBABILITY) near += 1;
      if (best < minProbability && !shown) continue;
      rides.push({
        attractionId: r.attractionId,
        attractionName: r.attractionName,
        attractionSlug: r.attractionSlug,
        parkId: r.parkId,
        parkName: r.parkName,
        parkSlug: r.parkSlug,
        parkPath: r.parkPath,
        publishable: shown,
        bestProbability: best,
        cold,
        snow,
      });
    }
    rides.sort((a, b) => b.bestProbability - a.bestProbability);
    return {
      bar: {
        probability: MIN_PUBLISHED_PROBABILITY,
        closedShareLower: MIN_PUBLISHED_CLOSED_SHARE_LOWER,
      },
      generatedAt: generatedAt?.toISOString() ?? null,
      totals: { rides: rows.length, publishable, near },
      rides,
    };
  }

  /**
   * The weather verdict for each of the given rides, which the caller has
   * already found shut during opening hours (an `outage` or `notRunToday`,
   * CLOSED or DOWN).
   *
   * Reads the profiles first and stops there for the usual park with none
   * that clear the bar, so the forecast and the window are only asked about
   * when a verdict is possible. The forecast comes straight from the client,
   * not from `WeatherService.getHourlyForecast`, which falls back to hours
   * synthesised from the daily min and max: a made-up curve must not name an
   * opening time. Never throws: a line under a badge is a nicety, the page is
   * not.
   */
  async getVerdicts(
    park: Pick<Park, "id" | "timezone">,
    attractionIds: string[],
    asOf: Date = new Date(),
  ): Promise<Map<string, WeatherClosureVerdict>> {
    const out = new Map<string, WeatherClosureVerdict>();
    if (attractionIds.length === 0) return out;
    try {
      const profiles = (
        await this.profileRepository.find({
          where: { attractionId: In(attractionIds) },
        })
      ).filter((p) => isPublishable(p.cold) || isPublishable(p.snow));
      if (profiles.length === 0) return out;

      const window = await queryWithLimits<Array<{ closesAt: Date | string }>>(
        this.dataSource,
        WEATHER_CLOSURE_CURRENT_WINDOW_SQL,
        [park.id, asOf, new Date(asOf.getTime() + 60_000), park.timezone],
        REQUEST_LIMITS,
      );
      if (window.length === 0) return out;
      const closesAt = new Date(window[0].closesAt);

      const coords = await this.parkRepository.findOne({
        where: { id: park.id },
        select: { id: true, latitude: true, longitude: true },
      });
      if (coords?.latitude == null || coords.longitude == null) return out;
      const forecast = await this.openMeteo.getHourlyForecast(
        coords.latitude,
        coords.longitude,
      );
      const hours = forecast.hours.map((h) => ({
        time: new Date(h.time),
        temperatureC: h.temperature,
        snowfallCm: h.snowfall,
      }));
      const yesterday = await this.weatherDataRepository.findOne({
        where: {
          parkId: park.id,
          date: getYesterdayDateInTimezone(park.timezone) as unknown as Date,
        },
        select: { parkId: true, snowfallSum: true },
      });
      const snowedYesterday =
        Number(yesterday?.snowfallSum ?? 0) >= SNOW_MIN_CM;

      for (const profile of profiles) {
        const verdict = decideWeatherClosure(
          profile,
          hours,
          snowedYesterday,
          asOf,
          closesAt,
        );
        if (verdict) out.set(profile.attractionId, verdict);
      }
    } catch (error) {
      this.logger.warn(
        `Weather-closure lookup failed for park ${park.id}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
    return out;
  }
}
