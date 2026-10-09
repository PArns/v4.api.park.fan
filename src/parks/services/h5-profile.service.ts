import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { Park } from "../entities/park.entity";
import {
  buildH5Profile,
  H5_WINDOW_DAYS,
  H5RideProfile,
  historyDaysOf,
} from "../../common/utils/h5-profile.util";
import {
  StatementLimits,
  queryWithLimits,
} from "../../common/utils/statement-limits.util";
import { safeJsonParse } from "../../common/utils/json.util";

/**
 * Two reads of one park's last 56 days: ~60 rollup rows a day and one schedule
 * row a day. Neither touches `queue_data` or `queue_data_aggregates`.
 */
const READ_LIMITS: StatementLimits = {
  statementTimeoutMs: 20_000,
  lockTimeoutMs: 2_000,
  idleInTransactionTimeoutMs: 10_000,
};

/**
 * Six hours, not a day: the rollup writes yesterday's row in the early
 * morning, and a profile cached before it would otherwise miss that day until
 * tomorrow. Bumping the version prefix invalidates every cached profile.
 */
const CACHE_TTL_SECONDS = 6 * 3600;
const CACHE_PREFIX = "plan-day:h5:v2";

/**
 * How long a failed build (an error, a statement timeout) is remembered, in
 * process. Without it every request for the park re-ran the two statements
 * while the database was the reason they failed — the single-flight only
 * collapses requests that arrive DURING a build, not the ones after it.
 */
export const H5_NEGATIVE_CACHE_MS = 45_000;

/** The composite index the window read wants; built out of band, see below. */
const PARK_DATE_INDEX = "idx_attraction_hourly_history_park_date";

/**
 * Every ride's H5 profile for one park, as of today (PAR-834).
 *
 * Built from `attraction_hourly_history` — the nightly 15-minute rollup — and
 * the park's published windows in `schedule_entries`, never from `queue_data`:
 * the rollup already holds one row per ride and day, so a park's 56 days are a
 * few thousand small rows rather than a scan of the compressed hypertable. The
 * aggregation runs here rather than in SQL so the arithmetic is the one the
 * unit tests pin (`h5-profile.util.ts`).
 *
 * Cached per park and park-local date in Redis, with one in-flight build per
 * key, so the planner's first request of the day for a park pays for it and
 * every other one (and the forward archive's d0–d90 walk) reads the cache.
 */
@Injectable()
export class H5ProfileService implements OnModuleInit {
  private readonly logger = new Logger(H5ProfileService.name);
  private readonly inFlight = new Map<
    string,
    Promise<Map<string, H5RideProfile>>
  >();
  /** key → until when the last failure is replayed instead of retried. */
  private readonly failedUntil = new Map<
    string,
    { until: number; error: Error }
  >();

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  onModuleInit(): void {
    // Fire and forget, like the trigram and ride-profile indexes: the read is
    // correct without it (the single-column parkId index serves it).
    void this.ensureParkDateIndex();
  }

  /**
   * `(parkId, date)` on `attraction_hourly_history`, built CONCURRENTLY so the
   * table's nightly writer is never blocked, and declared `synchronize: false`
   * on the entity so TypeORM neither builds it blocking at boot nor drops it.
   * A concurrent build that died leaves an INVALID index that `IF NOT EXISTS`
   * would skip forever, so an invalid one is dropped and rebuilt. Bounded by a
   * session statement timeout; on failure the read simply keeps using the
   * single-column index. Manual equivalent:
   * `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_attraction_hourly_history_park_date
   *    ON attraction_hourly_history ("parkId", date);`
   */
  private async ensureParkDateIndex(): Promise<void> {
    const runner = this.dataSource.createQueryRunner();
    try {
      await runner.connect();
      await runner.query(`SET statement_timeout = '10min'`);
      await runner.query(`SET lock_timeout = '30s'`);
      const state: Array<{ valid: boolean }> = await runner.query(
        `SELECT i.indisvalid AS valid FROM pg_class c
           JOIN pg_index i ON i.indexrelid = c.oid
          WHERE c.relname = $1`,
        [PARK_DATE_INDEX],
      );
      if (state[0]?.valid === true) return;
      if (state[0]?.valid === false) {
        await runner.query(
          `DROP INDEX CONCURRENTLY IF EXISTS ${PARK_DATE_INDEX}`,
        );
      }
      await runner.query(
        `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${PARK_DATE_INDEX}
           ON attraction_hourly_history ("parkId", date)`,
      );
    } catch (err) {
      this.logger.warn(
        `${PARK_DATE_INDEX} not built: ${(err as Error).message}`,
      );
    } finally {
      await runner.query(`RESET statement_timeout`).catch(() => undefined);
      await runner.query(`RESET lock_timeout`).catch(() => undefined);
      await runner.release();
    }
  }

  /**
   * @param localToday the park-local date the window ends before
   * @throws when a read fails — the caller decides what an outage costs. A
   *         failure is replayed for {@link H5_NEGATIVE_CACHE_MS} rather than
   *         retried on every request.
   */
  async getProfiles(
    park: Park,
    localToday: string,
  ): Promise<Map<string, H5RideProfile>> {
    const key = `${CACHE_PREFIX}:${park.id}:${localToday}`;
    const failed = this.failedUntil.get(key);
    if (failed) {
      if (failed.until > Date.now()) throw failed.error;
      this.failedUntil.delete(key);
    }
    const cached = safeJsonParse<Record<string, H5RideProfile>>(
      await this.redis.get(key).catch(() => null),
    );
    if (cached) return new Map(Object.entries(cached));

    const pending = this.inFlight.get(key);
    if (pending) return pending;

    const build = this.build(park, localToday)
      .catch((error: Error) => {
        this.failedUntil.set(key, {
          until: Date.now() + H5_NEGATIVE_CACHE_MS,
          error,
        });
        throw error;
      })
      .then(async (profiles) => {
        await this.redis
          .set(
            key,
            JSON.stringify(Object.fromEntries(profiles)),
            "EX",
            CACHE_TTL_SECONDS,
          )
          .catch(() => undefined);
        return profiles;
      })
      .finally(() => this.inFlight.delete(key));
    this.inFlight.set(key, build);
    return build;
  }

  private async build(
    park: Park,
    localToday: string,
  ): Promise<Map<string, H5RideProfile>> {
    const started = Date.now();
    const [windowRows, historyRows] = await Promise.all([
      queryWithLimits<
        Array<{
          d: string;
          open_min: number | string;
          close_min: number | string;
        }>
      >(
        this.dataSource,
        // The service-day window exactly as the benchmark's truth defines it:
        // earliest OPERATING opening to latest closing, each entry sane on its
        // own (closing after opening, at most 24 h later). Minutes are counted
        // from the service date's own park-local midnight, so a closing past
        // midnight comes back above 1440.
        `SELECT se.date::text AS d,
                extract(epoch FROM (min(se."openingTime") AT TIME ZONE $3)
                                   - se.date::timestamp) / 60 AS open_min,
                extract(epoch FROM (max(se."closingTime") AT TIME ZONE $3)
                                   - se.date::timestamp) / 60 AS close_min
           FROM schedule_entries se
          WHERE se."parkId" = $1
            AND se.date >= $2::date - $4::int AND se.date < $2::date
            AND se."scheduleType" = 'OPERATING' AND se."attractionId" IS NULL
            AND se."openingTime" IS NOT NULL AND se."closingTime" IS NOT NULL
            AND se."closingTime" > se."openingTime"
            AND se."closingTime" - se."openingTime" <= interval '24 hours'
          GROUP BY se.date`,
        [park.id, localToday, park.timezone, H5_WINDOW_DAYS],
        READ_LIMITS,
      ),
      queryWithLimits<Array<{ id: string; d: string; s: string | null }>>(
        this.dataSource,
        // One compact string per ride-day ("09:15=35,09:30=40,…") instead of
        // the jsonb: the slot's p90 and sample count are not needed, and a
        // park's 56 days of jsonb is ten times the bytes.
        `SELECT h."attractionId"::text AS id, h.date::text AS d,
                string_agg((x->>'time_slot') || '=' || (x->>'avgWait'), ',') AS s
           FROM attraction_hourly_history h
          CROSS JOIN LATERAL jsonb_array_elements(h.slots) x
          WHERE h."parkId" = $1
            AND h.date >= $2::date - $3::int AND h.date < $2::date
          GROUP BY h."attractionId", h.date`,
        [park.id, localToday, H5_WINDOW_DAYS],
        READ_LIMITS,
      ),
    ]);

    const windows = new Map<string, { openMin: number; closeMin: number }>();
    for (const w of windowRows) {
      const openMin = Number(w.open_min);
      const closeMin = Number(w.close_min);
      if (Number.isFinite(openMin) && Number.isFinite(closeMin)) {
        windows.set(w.d, { openMin, closeMin });
      }
    }

    // ride → date → readings
    const byRide = new Map<
      string,
      Map<string, Array<{ minute: number; wait: number }>>
    >();
    for (const row of historyRows) {
      const readings = H5ProfileService.parseSlots(row.s);
      if (readings.length === 0) continue;
      const days = byRide.get(row.id) ?? new Map();
      days.set(row.d, readings);
      byRide.set(row.id, days);
    }

    const out = new Map<string, H5RideProfile>();
    for (const [rideId, days] of byRide) {
      const profile = buildH5Profile(historyDaysOf(days, windows));
      if (profile) out.set(rideId, profile);
    }
    this.logger.debug(
      `H5 profiles for ${park.slug}: ${out.size} rides from ${historyRows.length} ride-days and ${windows.size} windows in ${Date.now() - started} ms`,
    );
    return out;
  }

  /** `"09:15=35,09:30=40"` → minutes since that date's midnight and waits. */
  static parseSlots(s: string | null): Array<{ minute: number; wait: number }> {
    if (!s) return [];
    const out: Array<{ minute: number; wait: number }> = [];
    for (const part of s.split(",")) {
      const [time, value] = part.split("=");
      const [hh, mm] = (time ?? "").split(":").map(Number);
      const wait = Number(value);
      if (
        !Number.isFinite(hh) ||
        !Number.isFinite(mm) ||
        !Number.isFinite(wait)
      )
        continue;
      out.push({ minute: hh * 60 + mm, wait });
    }
    return out;
  }
}
