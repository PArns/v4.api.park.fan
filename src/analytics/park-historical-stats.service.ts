import { Injectable, Inject, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { subDays, subYears } from "date-fns";
import { formatInTimeZone } from "date-fns-tz";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { safeJsonParse } from "../common/utils/json.util";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { Park } from "../parks/entities/park.entity";
import {
  ParkHistoricalStatsDto,
  MonthStatDto,
  DayOfWeekStatDto,
  TopAttractionStatDto,
} from "./dto/park-historical-stats.dto";
import {
  ParkHourlyProfileDto,
  HourlyProfileAttractionDto,
} from "./dto/park-hourly-profile.dto";
import { RideDayCurveDto } from "./dto/ride-day-curve.dto";
import { CrowdLevel } from "../common/types/crowd-level.type";
import { rateOrUnknown } from "../common/utils/crowd-level.util";

/** Response schema version — bump when the contract changes (see DTO). */
const SCHEMA_VERSION = 3;
/** Default minimum sample days before the section is considered displayable. */
const DEFAULT_MIN_SAMPLE_DAYS = 30;
/** Drop noisy single-sample hours from the aggregate scan. */
const MIN_SAMPLES_PER_HOUR = 2;

/**
 * Measured days an attraction needs before it may appear in `topAttractions`.
 *
 * The ranking is by average P90, and an average over one day is not an
 * average: Walibi Hollands Sky Diver was measured on a single day, came out
 * at P90 75, and outranked every coaster in the park — a ride the park's own
 * top-ten table then led with. Twenty days is the same floor the per-ride
 * `typicalWaits` aggregate uses for its `displayable` gate, so the two
 * surfaces refuse the same thin evidence.
 */
const DEFAULT_MIN_ATTRACTION_DAYS = 20;

/**
 * How many ranked rides the day curve may choose between.
 *
 * Enough that a closed headliner is not the end of the search, small enough that
 * the underlying profile stays the lean thing it was built to be.
 */
const DAY_CURVE_CANDIDATES = 8;

/** Hourly-profile schema version (see ParkHourlyProfileDto). */
const HOURLY_SCHEMA_VERSION = 4;

/**
 * An hour needs this many measured days across the window to be drawn at all.
 * An absolute floor for small or young parks; the ratio below does the real work.
 */
const MIN_DAYS_PER_HOUR = 10;

/**
 * …and it needs this share of the best-observed hour's day count.
 *
 * An absolute floor cannot decide this on its own, because "the park was open"
 * is not a fixed number of days: Europa-Park's Winterzauber runs 11:00–20:00 for
 * about six weeks, so a flat threshold either keeps 20:00 (drawing a winter-only
 * hour as part of a normal day) or drops it together with hours a small park
 * only ever measures forty times. Measuring each hour against the hours the park
 * is *always* open scales to both. Same shape as the weekday-sample ratio in the
 * frontend's quietest-day rule, and for the same reason.
 */
const MIN_HOUR_DAY_RATIO = 0.4;

/**
 * …and this share of the rides in the table must actually report it.
 *
 * The day-count tests above ask the BEST-observed ride whether an hour exists,
 * so one ride is enough to mint a column. Europa-Park then opened at 07:00 and
 * 08:00 with seven of eight rows empty: that is the hotel guests' early entry
 * through one queue, not an hour of the park's day. A column the table cannot
 * fill is width taken from a matrix that has to fit on a phone.
 */
const MIN_HOUR_RIDE_RATIO = 0.5;

/** One operating day's headliner-only values (peak + typical wait). */
interface DayValue {
  month: number;
  dow: number;
  dayValueP90: number;
  dayValueP50: number;
}

@Injectable()
export class ParkHistoricalStatsService {
  private readonly logger = new Logger(ParkHistoricalStatsService.name);
  private readonly CACHE_TTL = 24 * 60 * 60; // 24 hours

  constructor(
    @InjectRepository(QueueDataAggregate)
    private readonly aggregateRepo: Repository<QueueDataAggregate>,
    @Inject(REDIS_CLIENT)
    private readonly redis: Redis,
  ) {}

  /**
   * Redis key for a cached historical-stats payload. topN / minSampleDays
   * change the payload, so they're part of the key (v2: occupancy-relative
   * avgCrowdLevel + additive meta fields).
   */
  private statsCacheKey(
    parkId: string,
    years: number,
    topN: number,
    minSampleDays: number,
    minAttractionDays: number,
  ): string {
    return `park:historical-stats:v3:${parkId}:${years}:${topN}:${minSampleDays}:${minAttractionDays}`;
  }

  async getParkHistoricalStats(
    park: Park,
    years: number,
    topN = 10,
    minSampleDays = DEFAULT_MIN_SAMPLE_DAYS,
    minAttractionDays = DEFAULT_MIN_ATTRACTION_DAYS,
  ): Promise<ParkHistoricalStatsDto> {
    const cacheKey = this.statsCacheKey(
      park.id,
      years,
      topN,
      minSampleDays,
      minAttractionDays,
    );
    const cached = safeJsonParse<ParkHistoricalStatsDto>(
      await this.redis.get(cacheKey),
    );
    if (cached) return cached;

    const result = await this.compute(
      park,
      years,
      topN,
      minSampleDays,
      minAttractionDays,
    );
    await this.redis.set(
      cacheKey,
      JSON.stringify(result),
      "EX",
      this.CACHE_TTL,
    );
    return result;
  }

  /**
   * Read-only accessor for the cached per-weekday aggregate, keyed like the
   * default `/stats` request (years=2, topN=10, minSampleDays=30) that the
   * endpoint + warmup populate.
   *
   * Returns `null` WITHOUT computing when the cache is cold — callers on a
   * latency-sensitive background path (the best-days precompute) must never
   * trigger the heavy 2-year percentile scan just to enrich an optional field.
   */
  async getCachedByDayOfWeek(park: Park): Promise<DayOfWeekStatDto[] | null> {
    const cacheKey = this.statsCacheKey(
      park.id,
      2,
      10,
      DEFAULT_MIN_SAMPLE_DAYS,
      DEFAULT_MIN_ATTRACTION_DAYS,
    );
    const cached = safeJsonParse<ParkHistoricalStatsDto>(
      await this.redis.get(cacheKey),
    );
    return cached?.byDayOfWeek ?? null;
  }

  private async compute(
    park: Park,
    years: number,
    topN: number,
    minSampleDays: number,
    minAttractionDays: number,
  ): Promise<ParkHistoricalStatsDto> {
    // Use park timezone so "yesterday" and the start boundary are correct
    // for parks in any UTC offset. date-fns subDays operates in wall-clock
    // time, then formatInTimeZone renders the result in the park's calendar.
    const now = new Date();
    const endStr = formatInTimeZone(
      subDays(now, 1),
      park.timezone,
      "yyyy-MM-dd",
    );
    const startStr = formatInTimeZone(
      subYears(subDays(now, 1), years),
      park.timezone,
      "yyyy-MM-dd",
    );

    // Headliner-only, matching the calendar's crowd-level semantic (a park's
    // crowd level is its headliners, not the family-ride dilution).
    const headlinerIds = await this.getHeadlinerIds(park.id);

    const [dayValues, topAttrRaw] = await Promise.all([
      this.queryHeadlinerDayValues(
        park.id,
        park.timezone,
        startStr,
        endStr,
        headlinerIds,
      ),
      this.queryTopAttractions(
        park.id,
        startStr,
        endStr,
        topN,
        minAttractionDays,
      ),
    ]);

    // typical-day-peak = median over operating days of the day_value
    // (AVG-of-headliner daily peaks). Computed from the SAME source as the
    // numerators below, so a statistically typical day ≈ 100% = moderate.
    const typicalDayPeak = this.median(dayValues.map((d) => d.dayValueP90));

    const byMonth: MonthStatDto[] = this.groupAvg(
      dayValues,
      (d) => d.month,
    ).map((g) => ({
      month: g.key,
      avgCrowdScore: this.toCrowdScore(g.avgP50),
      avgCrowdLevel: this.toCrowdLevel(g.avgP90, typicalDayPeak),
      avgWaitP50: roundToNearest5Minutes(g.avgP50),
      avgWaitP90: roundToNearest5Minutes(g.avgP90),
      sampleDays: g.sampleDays,
    }));

    const byDayOfWeek: DayOfWeekStatDto[] = this.groupAvg(
      dayValues,
      (d) => d.dow,
    ).map((g) => ({
      dayOfWeek: g.key,
      avgCrowdScore: this.toCrowdScore(g.avgP50),
      avgCrowdLevel: this.toCrowdLevel(g.avgP90, typicalDayPeak),
      avgWaitP50: roundToNearest5Minutes(g.avgP50),
      avgWaitP90: roundToNearest5Minutes(g.avgP90),
      sampleDays: g.sampleDays,
    }));

    // Every minute figure this service emits goes through
    // `roundToNearest5Minutes`, the same helper the calendar, the PCN serving
    // path and the typical-waits job use. Parks post their waits in five-minute
    // steps, so a stored p50 is always a multiple of five — but PERCENTILE_CONT
    // interpolates between two of them and AVG() across days blurs the rest, and
    // the table then printed 51, 53, 47: readings no park has ever displayed and
    // no visitor can be shown. The crowd score and level a few lines up keep the
    // RAW averages deliberately: they are ratios against the typical day peak,
    // and coarsening the input before dividing would move rides across a tier
    // boundary for no reason.
    const topAttractions: TopAttractionStatDto[] = topAttrRaw.map((r, i) => ({
      attractionSlug: r.slug as string,
      attractionName: r.name as string,
      avgWaitP50: roundToNearest5Minutes(Number(r.avg_p50)),
      avgWaitP90: roundToNearest5Minutes(Number(r.avg_p90)),
      sampleDays: Number(r.sample_days),
      rank: i + 1,
      land: (r.land as string | null) ?? null,
      attractionType: (r.attraction_type as string | null) ?? null,
    }));

    const totalSampleDays = dayValues.length;

    return {
      byMonth,
      byDayOfWeek,
      topAttractions,
      meta: {
        parkSlug: park.slug,
        dataFrom: startStr,
        dataTo: endStr,
        totalSampleDays,
        windowYears: years,
        displayable: totalSampleDays >= minSampleDays,
        generatedAt: new Date().toISOString(),
        schemaVersion: SCHEMA_VERSION,
        minAttractionDays,
      },
    };
  }

  /**
   * The park's day shape, ride by ride: what each queue typically looks like
   * at every hour it is open.
   *
   * A projection, not a slice of an existing payload. The attraction detail
   * endpoint carries the same information for one ride in ~53 KB, 45 % of it a
   * `schedule` nobody renders — eight rides would cost 424 KB for a table that
   * fits in 2 KB. Reads the same hourly pre-aggregation as everything else
   * here, so the numbers agree with the top-attraction ranking by construction.
   */
  async getParkHourlyProfile(
    park: Park,
    years: number,
    topN: number,
    minAttractionDays = DEFAULT_MIN_ATTRACTION_DAYS,
  ): Promise<ParkHourlyProfileDto> {
    const cacheKey = `park:hourly-profile:v4:${park.id}:${years}:${topN}:${minAttractionDays}`;
    const cached = safeJsonParse<ParkHourlyProfileDto>(
      await this.redis.get(cacheKey),
    );
    if (cached) return cached;

    const result = await this.computeHourlyProfile(
      park,
      years,
      topN,
      minAttractionDays,
    );
    await this.redis.set(
      cacheKey,
      JSON.stringify(result),
      "EX",
      this.CACHE_TTL,
    );
    return result;
  }

  private async computeHourlyProfile(
    park: Park,
    years: number,
    topN: number,
    minAttractionDays: number,
  ): Promise<ParkHourlyProfileDto> {
    const now = new Date();
    const endStr = formatInTimeZone(
      subDays(now, 1),
      park.timezone,
      "yyyy-MM-dd",
    );
    const startStr = formatInTimeZone(
      subYears(subDays(now, 1), years),
      park.timezone,
      "yyyy-MM-dd",
    );

    const rows = await this.queryHourlyProfile(
      park.id,
      park.timezone,
      startStr,
      endStr,
      topN,
      minAttractionDays,
    );

    // Which hours the table has columns for. Decided across the whole park, not
    // per ride: a matrix whose rows each start at a different hour is not a
    // matrix. An hour survives when enough DAYS reported it — a park that
    // stayed open to 20:00 on four August evenings has a 20:00 bucket, and
    // drawing it would suggest the park is open then.
    //
    // `hour_days` counts the days THAT HOUR was measured, which is the only
    // number this test can be made of. Reading the ride's window-wide
    // `sample_days` here instead is what shipped 21:00–23:00 columns for
    // Europa-Park at 58–68 minutes: every hour inherited ~157 and the filter
    // never removed anything.
    const daysPerHour = new Map<number, number>();
    for (const r of rows) {
      const hour = Number(r.hour_of_day);
      daysPerHour.set(
        hour,
        Math.max(daysPerHour.get(hour) ?? 0, Number(r.hour_days)),
      );
    }
    const bestHourDays = Math.max(0, ...daysPerHour.values());
    const hours = [...daysPerHour.entries()]
      .filter(
        ([, days]) =>
          days >= MIN_DAYS_PER_HOUR &&
          days >= bestHourDays * MIN_HOUR_DAY_RATIO,
      )
      .map(([hour]) => hour)
      .sort((a, b) => a - b);

    // Rank by the ride's busiest hour rather than its all-day average: this
    // table exists to show WHEN a queue happens, so a ride with one sharp
    // morning spike belongs above one that idles at a flat ten minutes.
    const byAttraction = new Map<
      string,
      {
        slug: string;
        name: string;
        land: string | null;
        p25: Map<number, number>;
        p50: Map<number, number>;
        p90: Map<number, number>;
        sampleDays: number;
      }
    >();
    for (const r of rows) {
      const slug = r.slug as string;
      const entry = byAttraction.get(slug) ?? {
        slug,
        name: r.name as string,
        land: (r.land as string | null) ?? null,
        p25: new Map<number, number>(),
        p50: new Map<number, number>(),
        p90: new Map<number, number>(),
        sampleDays: 0,
      };
      const hour = Number(r.hour_of_day);
      // A ride that reported an hour on a handful of days gets a gap in that
      // cell rather than a number. It happens to rides that opened mid-season
      // and to the odd hour a single ride stayed open for, and one such cell
      // next to a column of well-measured ones reads as a comparable value.
      if (Number(r.hour_days) >= MIN_DAYS_PER_HOUR) {
        // Stored RAW. Rounding happens once, on the way out — the ordering
        // decisions below (which ride ranks where, which hour is its peak) read
        // these, and five-minute buckets create ties that a raw comparison does
        // not have: a ride reading 51 at 11:00 and 53 at 12:00 peaks at noon,
        // but rounded both are 50 and the first hour wins by accident.
        entry.p25.set(hour, Number(r.p25));
        entry.p50.set(hour, Number(r.p50));
        entry.p90.set(hour, Number(r.p90));
      }
      entry.sampleDays = Math.max(entry.sampleDays, Number(r.sample_days));
      byAttraction.set(slug, entry);
    }

    // Rank and cut first, so the coverage test below asks the rides the table
    // will actually show rather than the up-to-60 the SQL over-fetched.
    const ranked = [...byAttraction.values()]
      .map((a) => ({
        ...a,
        peak: Math.max(-1, ...[...a.p50.values()]),
      }))
      .filter((a) => a.peak >= 0)
      .sort((a, b) => b.peak - a.peak)
      .slice(0, topN);

    // An hour half the table cannot fill is not an hour of the park's day.
    const visibleHours = hours.filter(
      (h) =>
        ranked.filter((a) => a.p50.has(h)).length >=
        ranked.length * MIN_HOUR_RIDE_RATIO,
    );

    const attractions: HourlyProfileAttractionDto[] = ranked.map((a) => {
      // Recomputed against the trimmed axis: a peak that fell outside it would
      // point at a column the response no longer carries. Decided on the raw
      // values, before the five-minute rounding flattens neighbouring hours.
      let peakHour: number | null = null;
      let peakValue = -1;
      for (const h of visibleHours) {
        const v = a.p50.get(h);
        if (v != null && v > peakValue) {
          peakValue = v;
          peakHour = h;
        }
      }
      const round = (v: number | undefined) =>
        v == null ? null : roundToNearest5Minutes(v);
      return {
        attractionSlug: a.slug,
        attractionName: a.name,
        land: a.land,
        p25: visibleHours.map((h) => round(a.p25.get(h))),
        p50: visibleHours.map((h) => round(a.p50.get(h))),
        p90: visibleHours.map((h) => round(a.p90.get(h))),
        peakHour,
        sampleDays: a.sampleDays,
      };
    });

    const totalSampleDays = attractions.reduce(
      (max, a) => Math.max(max, a.sampleDays),
      0,
    );

    return {
      hours: visibleHours,
      attractions,
      meta: {
        parkSlug: park.slug,
        dataFrom: startStr,
        dataTo: endStr,
        windowYears: years,
        totalSampleDays,
        // Two independent ways to have nothing worth drawing: no ride cleared
        // the sample floor, or the park's hours are so ragged that no single
        // hour was measured on enough days to be a column.
        displayable: visibleHours.length >= 3 && attractions.length > 0,
        generatedAt: new Date().toISOString(),
        schemaVersion: HOURLY_SCHEMA_VERSION,
      },
    };
  }

  /** Day-curve schema version (see RideDayCurveDto). */
  /**
   * Bumped to 2 when `predicted` was added.
   *
   * Unlike the hourly profile's version this one is NOT part of a Redis key —
   * the day curve is not stored, `HttpCacheInterceptor` only sets headers, so a
   * response without the new field ages out of Cloudflare within its five
   * minutes on its own. The number is here for the caller: it is the one field
   * that says "the shape changed", and leaving it at 1 while the shape changed
   * makes it worthless for exactly the case it exists for.
   */
  private readonly DAY_CURVE_SCHEMA_VERSION = 2;

  /**
   * One ride's day: the historical shape, today's readings, and the forecast for
   * the hours still to come.
   *
   * Three sources, because they genuinely are three. The percentiles come from
   * the nightly rollup; TODAY cannot, because that rollup is computed for
   * yesterday and back, so today is bucketed live out of raw queue data; and the
   * forecast comes from the hourly prediction table, most recent prediction per
   * hour.
   *
   * The ride is picked here rather than by the caller, and picked to be
   * SHOWABLE: the profile's own ranking, but preferring a ride that actually
   * reported something today. That is what stops a chart headed "today" from
   * landing on a ride that is closed, under refurbishment, or out of season,
   * without anybody maintaining a second list of which rides those are.
   */
  async getRideDayCurve(
    park: Park,
    attractionSlug?: string,
  ): Promise<RideDayCurveDto | null> {
    const profile = await this.getParkHourlyProfile(
      park,
      1,
      DAY_CURVE_CANDIDATES,
      DEFAULT_MIN_ATTRACTION_DAYS,
    );
    if (!profile.meta.displayable || profile.attractions.length === 0) {
      return null;
    }

    const todayStr = formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd");
    const todayRows = await this.queryTodayByHour(
      park.id,
      park.timezone,
      todayStr,
    );

    // slug -> hour -> measured wait
    const todayBySlug = new Map<string, Map<number, number>>();
    for (const r of todayRows) {
      const slug = r.slug as string;
      const byHour = todayBySlug.get(slug) ?? new Map<number, number>();
      byHour.set(Number(r.hour_of_day), roundToNearest5Minutes(Number(r.wait)));
      todayBySlug.set(slug, byHour);
    }

    const ride = attractionSlug
      ? profile.attractions.find((a) => a.attractionSlug === attractionSlug)
      : // The ranking is by busiest hour; the first one that reported today wins,
        // and the plain ranking is the fallback when the whole park is quiet or
        // shut (at 03:00 nothing has reported, and that is not an error).
        (profile.attractions.find((a) => todayBySlug.has(a.attractionSlug)) ??
        profile.attractions[0]);
    if (!ride) return null;

    const todayByHour =
      todayBySlug.get(ride.attractionSlug) ?? new Map<number, number>();
    const today = profile.hours.map((h) => todayByHour.get(h) ?? null);

    const [forecastRows, mae] = await Promise.all([
      this.queryHourlyForecast(
        ride.attractionSlug,
        park.id,
        park.timezone,
        todayStr,
      ),
      this.queryAttractionMae(ride.attractionSlug, park.id),
    ]);
    const latestByHour = new Map<number, number>();
    const firstByHour = new Map<number, number>();
    for (const r of forecastRows) {
      const hour = Number(r.hour_of_day);
      if (r.last_predicted != null) {
        latestByHour.set(
          hour,
          roundToNearest5Minutes(Number(r.last_predicted)),
        );
      }
      if (r.first_predicted != null) {
        firstByHour.set(
          hour,
          roundToNearest5Minutes(Number(r.first_predicted)),
        );
      }
    }

    // An hour that has already been measured is not a forecast any more. Leaving
    // both in would draw the model's guess on top of the fact.
    const forecast = profile.hours.map((h, i) =>
      today[i] != null ? null : (latestByHour.get(h) ?? null),
    );

    // What the model said BEFORE each hour happened — every hour it has an
    // opinion about, measured or not. This is the series that lets a reader
    // check the model rather than take its word for it, and it is the reason
    // `queryHourlyForecast` returns the oldest row per hour as well as the
    // newest: scoring a prediction against reality only means something if the
    // prediction was written first.
    const predicted = profile.hours.map((h) => firstByHour.get(h) ?? null);

    return {
      hours: profile.hours,
      attractionSlug: ride.attractionSlug,
      attractionName: ride.attractionName,
      p25: ride.p25,
      p50: ride.p50,
      p90: ride.p90,
      today,
      forecast,
      predicted,
      forecastError: mae,
      measuredToday: today.some((v) => v != null),
      sampleDays: ride.sampleDays,
      timezone: park.timezone,
      generatedAt: new Date().toISOString(),
      schemaVersion: this.DAY_CURVE_SCHEMA_VERSION,
    };
  }

  /**
   * Today's measured wait per hour for every ride in the park.
   *
   * Raw `queue_data`, not `queue_data_aggregates`: the rollup runs nightly for
   * the completed day, so it has no row for today at all. One park-day of
   * five-minute readings is small, and taking the whole park in one query is
   * what lets the caller CHOOSE a ride that has data instead of discovering the
   * one it picked is shut.
   *
   * Averaged per hour and rounded to five on the way out, matching how every
   * other wait time on this API is presented. STANDBY only — a single-rider or
   * Lightning Lane line is a different queue and would drag the average away
   * from the number a visitor sees on the sign.
   */
  /**
   * A NOTE ON THE JOIN, because this file joins `attractions` to three tables and
   * they do NOT agree on the column type:
   *
   *   queue_data.attractionId              uuid  → join `a.id` directly
   *   wait_time_predictions.attractionId   uuid  → join `a.id` directly
   *   queue_data_aggregates.attractionId   TEXT  → join `a.id::text`
   *
   * The aggregates table is the odd one out, and it is also the one every older
   * query in this file reads — so `a.id::text = …` looks like the house style
   * and is wrong the moment it is copied onto a raw-queue or prediction join.
   * Postgres answers that with `operator does not exist: text = uuid`, a 500 at
   * request time that neither the build nor any unit test sees, because the SQL
   * is only parsed when it runs. It shipped exactly that way.
   */
  private async queryTodayByHour(
    parkId: string,
    timezone: string,
    todayStr: string,
  ): Promise<Record<string, unknown>[]> {
    return this.aggregateRepo.manager.query(
      `SELECT a.slug,
              EXTRACT(HOUR FROM (qd.timestamp AT TIME ZONE $2))::int AS hour_of_day,
              AVG(qd."waitTime") AS wait
       FROM queue_data qd
       JOIN attractions a ON a.id = qd."attractionId"
       WHERE a."parkId" = $1::uuid
         AND qd.timestamp >= ($3::date::timestamp AT TIME ZONE $2)
         AND qd.timestamp <  (($3::date + INTERVAL '1 day')::timestamp AT TIME ZONE $2)
         AND qd."queueType" = 'STANDBY'
         AND qd.status = 'OPERATING'
         AND qd."waitTime" IS NOT NULL
       GROUP BY a.slug, hour_of_day
       ORDER BY a.slug, hour_of_day`,
      [parkId, timezone, todayStr],
    );
  }

  /**
   * The hourly forecast for one ride, for today.
   *
   * The model re-predicts through the day, so one hour holds several rows, and
   * WHICH of them is the answer depends on the question:
   *
   * - "what do you expect for 20:00" wants the NEWEST prediction — everything
   *   learned since this morning is in it.
   * - "what did you say 14:00 would be" wants the OLDEST — the one made before
   *   the hour happened. Answering that with a row written after the fact would
   *   let the model mark its own homework.
   *
   * So both come back per hour and the caller picks. Grouping by `predictedTime`
   * rather than `DISTINCT ON` because two aggregates over the same group is
   * exactly what `array_agg(… ORDER BY …)` is for.
   */
  private async queryHourlyForecast(
    attractionSlug: string,
    parkId: string,
    timezone: string,
    todayStr: string,
  ): Promise<Record<string, unknown>[]> {
    return this.aggregateRepo.manager.query(
      `SELECT EXTRACT(HOUR FROM (p."predictedTime" AT TIME ZONE $3))::int AS hour_of_day,
              (array_agg(p."predictedWaitTime" ORDER BY p."createdAt" ASC))[1]  AS first_predicted,
              (array_agg(p."predictedWaitTime" ORDER BY p."createdAt" DESC))[1] AS last_predicted
       FROM wait_time_predictions p
       JOIN attractions a ON a.id = p."attractionId"
       WHERE a.slug = $1
         AND a."parkId" = $2::uuid
         AND p."predictionType" = 'hourly'
         AND p."predictedTime" >= ($4::date::timestamp AT TIME ZONE $3)
         AND p."predictedTime" <  (($4::date + INTERVAL '1 day')::timestamp AT TIME ZONE $3)
       GROUP BY p."predictedTime"
       ORDER BY p."predictedTime"`,
      [attractionSlug, parkId, timezone, todayStr],
    );
  }

  /**
   * The ride's own mean absolute error, for a caller drawing the forecast as a
   * band. Per ride rather than the model-wide figure: the band belongs to this
   * curve, and a headliner and a carousel are not predicted equally well.
   */
  private async queryAttractionMae(
    attractionSlug: string,
    parkId: string,
  ): Promise<number | null> {
    const rows: Array<{ mae: number | null }> =
      await this.aggregateRepo.manager.query(
        `SELECT s.mae
         FROM attraction_accuracy_stats s
         JOIN attractions a ON a.id::text = s.attraction_id::text
         WHERE a.slug = $1 AND a."parkId" = $2::uuid
         LIMIT 1`,
        [attractionSlug, parkId],
      );
    const mae = rows[0]?.mae;
    return mae == null ? null : Number(mae);
  }

  /**
   * One row per (attraction, hour-of-day): the median and busy wait that ride
   * shows at that hour, plus how many days went into it.
   *
   * The hour bucket is read in the PARK's timezone, so 10:00 means 10:00 to a
   * visitor standing there — reading it in UTC shifts Gardaland's morning by
   * two hours in summer and one in winter, i.e. by a different amount inside
   * the same window.
   */
  private async queryHourlyProfile(
    parkId: string,
    timezone: string,
    startDate: string,
    endDate: string,
    topN: number,
    minAttractionDays: number,
  ): Promise<Record<string, unknown>[]> {
    return this.aggregateRepo.manager.query(
      `WITH eligible AS (
         SELECT qda."attractionId"                    AS aid,
                COUNT(DISTINCT (qda.hour AT TIME ZONE $2)::date)::int AS sample_days
         FROM queue_data_aggregates qda
         WHERE qda."parkId" = $1
           AND qda.hour >= $3::date
           AND qda.hour <  ($4::date + INTERVAL '1 day')
           AND qda."sampleCount" >= $5
         GROUP BY qda."attractionId"
         HAVING COUNT(DISTINCT (qda.hour AT TIME ZONE $2)::date) >= $6
         ORDER BY AVG(qda.p90) DESC
         LIMIT $7
       )
       SELECT
         a.slug,
         COALESCE(a.curated_name, a.name)           AS name,
         COALESCE(a.curated_land_name, a.land_name) AS land,
         EXTRACT(HOUR FROM (qda.hour AT TIME ZONE $2))::int AS hour_of_day,
         AVG(qda.p25)                               AS p25,
         AVG(qda.p50)                               AS p50,
         AVG(qda.p90)                               AS p90,
         -- Days THIS HOUR was measured for THIS ride. Distinct from
         -- e.sample_days, which counts the ride's measured days across the
         -- whole window and is the same number for all 24 of its hours.
         COUNT(DISTINCT (qda.hour AT TIME ZONE $2)::date)::int AS hour_days,
         e.sample_days
       FROM queue_data_aggregates qda
       JOIN eligible e   ON e.aid = qda."attractionId"
       JOIN attractions a ON a.id::text = qda."attractionId"
       WHERE qda."parkId" = $1
         AND qda.hour >= $3::date
         AND qda.hour <  ($4::date + INTERVAL '1 day')
         AND qda."sampleCount" >= $5
       GROUP BY a.id, a.slug, name, land, hour_of_day, e.sample_days
       ORDER BY a.slug, hour_of_day`,
      [
        parkId,
        timezone,
        startDate,
        endDate,
        MIN_SAMPLES_PER_HOUR,
        minAttractionDays,
        // Over-fetch: the SQL ranks by all-day average, the projection re-ranks
        // by peak hour, and a ride that only spikes at rope drop must not be
        // cut before that re-rank can see it.
        Math.min(topN * 3, 60),
      ],
    );
  }

  /**
   * Headliner attraction IDs for the park (as text, to match the text
   * `attractionId` column on queue_data_aggregates). Empty ⇒ caller falls back
   * to all attractions, mirroring the calendar's headliner fallback.
   */
  private async getHeadlinerIds(parkId: string): Promise<string[]> {
    const rows: Array<{ id: string }> = await this.aggregateRepo.manager.query(
      `SELECT "attractionId"::text AS id
       FROM headliner_attractions
       WHERE "parkId" = $1::uuid`,
      [parkId],
    );
    return rows.map((r) => r.id);
  }

  /**
   * One row per operating day: the headliner-only day_value = AVG across
   * headliners of that ride's daily peak (P90 of the day's hourly P90s) and
   * the day's typical wait (AVG of hourly P50s). Computed from the hourly
   * pre-aggregation (queue_data_aggregates), restricted to headliners.
   */
  private async queryHeadlinerDayValues(
    parkId: string,
    timezone: string,
    startDate: string,
    endDate: string,
    headlinerIds: string[],
  ): Promise<DayValue[]> {
    const rows: Array<Record<string, unknown>> =
      await this.aggregateRepo.manager.query(
        `WITH per_attraction_day AS (
           SELECT
             (qda.hour AT TIME ZONE $2)::date                     AS day,
             qda."attractionId"                                   AS aid,
             PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qda.p90) AS day_peak,
             AVG(qda.p50)                                         AS day_typical
           FROM queue_data_aggregates qda
           WHERE qda."parkId" = $1
             AND qda.hour >= $3::date
             AND qda.hour <  ($4::date + INTERVAL '1 day')
             AND qda."sampleCount" >= $5
             AND ($6::text[] IS NULL OR qda."attractionId" = ANY($6))
           GROUP BY day, aid
         )
         SELECT
           EXTRACT(MONTH FROM day)::int AS month,
           EXTRACT(DOW FROM day)::int   AS dow,
           AVG(day_peak)                AS day_value_p90,
           AVG(day_typical)             AS day_value_p50
         FROM per_attraction_day
         GROUP BY day
         ORDER BY day`,
        [
          parkId,
          timezone,
          startDate,
          endDate,
          MIN_SAMPLES_PER_HOUR,
          headlinerIds.length > 0 ? headlinerIds : null,
        ],
      );

    return rows.map((r) => ({
      month: Number(r.month),
      dow: Number(r.dow),
      dayValueP90: Number(r.day_value_p90),
      dayValueP50: Number(r.day_value_p50),
    }));
  }

  private async queryTopAttractions(
    parkId: string,
    startDate: string,
    endDate: string,
    topN: number,
    minAttractionDays: number,
  ): Promise<Record<string, unknown>[]> {
    // queue_data_aggregates uses camelCase columns: "parkId", "attractionId", p50, p90.
    // p50/p90 are lowercase (single-word) in the entity, so no quoting needed there.
    //
    // The HAVING is the whole reason a ride with one measured day no longer
    // leads the ranking (see DEFAULT_MIN_ATTRACTION_DAYS). It is deliberately
    // NOT a WHERE on the joined rows: the threshold is about how many days the
    // ride was watched, which only exists after the GROUP BY.
    //
    // Name, land and type all prefer the curated column. `land_name` comes
    // from Queue-Times, is missing for whole parks and goes stale when a land
    // is re-themed; the curated one is the correction and wins wherever it is
    // set. `attractionType` is one of the few unquoted camelCase columns on
    // this table — every neighbour here is snake_case, so it needs the quotes.
    return this.aggregateRepo.manager.query(
      `SELECT
         a.slug,
         COALESCE(a.curated_name, a.name)                        AS name,
         COALESCE(a.curated_land_name, a.land_name)              AS land,
         COALESCE(a.curated_attraction_type, a."attractionType") AS attraction_type,
         AVG(qda.p50)                               AS avg_p50,
         AVG(qda.p90)                               AS avg_p90,
         COUNT(DISTINCT DATE(qda.hour))::int         AS sample_days
       FROM queue_data_aggregates qda
       JOIN attractions a ON a.id::text = qda."attractionId"
       WHERE qda."parkId" = $1
         AND qda.hour >= $2::date
         AND qda.hour <  ($3::date + INTERVAL '1 day')
       GROUP BY a.id, a.slug, name, land, attraction_type
       HAVING COUNT(DISTINCT DATE(qda.hour)) >= $5
       ORDER BY avg_p90 DESC
       LIMIT $4`,
      [parkId, startDate, endDate, topN, minAttractionDays],
    );
  }

  /** Linear-interpolation median over an array (0 when empty). */
  private median(values: number[]): number {
    if (values.length === 0) return 0;
    const sorted = [...values].sort((a, b) => a - b);
    const mid = (sorted.length - 1) / 2;
    const lo = Math.floor(mid);
    const hi = Math.ceil(mid);
    return lo === hi ? sorted[lo] : (sorted[lo] + sorted[hi]) / 2;
  }

  /** Group day-values by a key (month / day-of-week) and average each bucket. */
  private groupAvg(
    rows: DayValue[],
    keyOf: (d: DayValue) => number,
  ): Array<{
    key: number;
    avgP50: number;
    avgP90: number;
    sampleDays: number;
  }> {
    const buckets = new Map<number, { p50: number; p90: number; n: number }>();
    for (const d of rows) {
      const k = keyOf(d);
      const e = buckets.get(k) ?? { p50: 0, p90: 0, n: 0 };
      e.p50 += d.dayValueP50;
      e.p90 += d.dayValueP90;
      e.n += 1;
      buckets.set(k, e);
    }
    return [...buckets.entries()]
      .map(([key, e]) => ({
        key,
        avgP50: e.p50 / e.n,
        avgP90: e.p90 / e.n,
        sampleDays: e.n,
      }))
      .sort((a, b) => a.key - b.key);
  }

  /**
   * Maps average P50 wait time to a 1.0–5.0 crowd score.
   * 10 min → 1.0, 50 min → 5.0 (linear). Clamped to [1.0, 5.0].
   *
   * Kept for backwards compatibility (sorting/tooltips). Prefer avgCrowdLevel
   * for display — it is occupancy-relative and consistent across endpoints.
   */
  private toCrowdScore(avgWaitP50: number): number {
    const raw = avgWaitP50 / 10;
    return Math.round(Math.min(Math.max(raw, 1.0), 5.0) * 10) / 10;
  }

  /**
   * Maps a period's average daily-peak wait to a CrowdLevel, occupancy-relative
   * to the park's typical-day-peak (100% = a statistically typical day). Same
   * 6-tier thresholds + headliner-only definition as the calendar, so the two
   * surfaces stay on one scale. Emits "unknown" when there's no baseline yet
   * (park not ratable — < 30 operating days), rather than a made-up "moderate".
   */
  private toCrowdLevel(avgWaitP90: number, typicalDayPeak: number): CrowdLevel {
    return rateOrUnknown(avgWaitP90, typicalDayPeak);
  }
}
