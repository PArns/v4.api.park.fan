import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, In, IsNull, Not, Repository } from "typeorm";
import { OpenMeteoClient } from "../../external-apis/weather/open-meteo.client";
import { Park } from "../entities/park.entity";
import { WeatherData } from "../entities/weather-data.entity";
import { AttractionWeatherClosureProfile } from "../entities/attraction-weather-closure-profile.entity";
import { WeatherService } from "../weather.service";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
  getYesterdayDateInTimezone,
} from "../../common/utils/date.util";
import {
  buildWeatherClosureSamples,
  decideWeatherClosure,
  learnWeatherClosureProfiles,
  MIN_PUBLISHED_PROBABILITY,
  RideHourState,
  WeatherClosureVerdict,
} from "../utils/weather-closure.util";
import {
  WEATHER_CLOSURE_CURRENT_WINDOW_SQL,
  WEATHER_CLOSURE_HOURS_SQL,
} from "../utils/weather-closure.sql";

/** A year: one winter behind every profile, and the summer that sets the baseline. */
const LOOKBACK_DAYS = 365;
/** Measured at 3–4 s per park on production; generous for the largest parks. */
const JOB_LIMITS: StatementLimits = {
  statementTimeoutMs: 120_000,
  lockTimeoutMs: 5_000,
};
const REQUEST_LIMITS: StatementLimits = {
  statementTimeoutMs: 2_000,
  lockTimeoutMs: 1_000,
};

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
    private readonly weatherService: WeatherService,
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
    const today = getCurrentDateInTimezone(park.timezone);
    const from = new Date(asOf.getTime() - LOOKBACK_DAYS * 24 * 3600 * 1000);
    const weather = await this.openMeteo.fetchHourlyArchive(
      park.latitude!,
      park.longitude!,
      from.toISOString().slice(0, 10),
      addIsoDays(today, -1),
    );
    const rows = await queryWithLimits<
      Array<{ attractionId: string; hourStart: Date | string; closed: boolean }>
    >(
      this.dataSource,
      WEATHER_CLOSURE_HOURS_SQL,
      [park.id, from, asOf, park.timezone],
      JOB_LIMITS,
    );
    const states: RideHourState[] = rows.map((r) => ({
      attractionId: r.attractionId,
      hourStart: new Date(r.hourStart),
      closed: r.closed,
    }));

    const samples = buildWeatherClosureSamples(states, weather);
    const profiles: AttractionWeatherClosureProfile[] = [];
    let publishable = 0;
    for (const [attractionId, list] of samples) {
      const learned = learnWeatherClosureProfiles(list);
      if (!learned.cold && !learned.snow) continue;
      if (
        [learned.cold, learned.snow].some(
          (p) => p && p.attributableShare >= MIN_PUBLISHED_PROBABILITY,
        )
      ) {
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

    await this.dataSource.transaction(async (manager) => {
      await manager.delete(AttractionWeatherClosureProfile, {
        parkId: park.id,
      });
      if (profiles.length > 0) await manager.save(profiles, { chunk: 500 });
    });
    return { profiles: profiles.length, publishable };
  }

  /**
   * The weather verdict for each of the given rides, which the caller has
   * already found shut during opening hours (an `outage` or `notRunToday`).
   *
   * Reads the profiles first and stops there for the usual park with none
   * that clear the bar, so the forecast and the window are only asked about
   * when a verdict is possible. Never throws: a line under a badge is a
   * nicety, the page is not.
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
      ).filter((p) =>
        [p.cold, p.snow].some(
          (x) => x && x.attributableShare >= MIN_PUBLISHED_PROBABILITY,
        ),
      );
      if (profiles.length === 0) return out;

      const window = await queryWithLimits<Array<{ closesAt: Date | string }>>(
        this.dataSource,
        WEATHER_CLOSURE_CURRENT_WINDOW_SQL,
        [park.id, asOf, new Date(asOf.getTime() + 60_000), park.timezone],
        REQUEST_LIMITS,
      );
      if (window.length === 0) return out;
      const closesAt = new Date(window[0].closesAt);

      const forecast = await this.weatherService.getHourlyForecast(park.id);
      const hours = forecast.map((h) => ({
        time: new Date(h.time),
        temperatureC: h.temperature ?? null,
        snowfallCm: h.snowfall ?? null,
      }));
      const yesterday = await this.weatherDataRepository.findOne({
        where: {
          parkId: park.id,
          date: getYesterdayDateInTimezone(park.timezone) as unknown as Date,
        },
        select: { parkId: true, snowfallSum: true },
      });
      const snowedYesterday = Number(yesterday?.snowfallSum ?? 0) > 0;

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
