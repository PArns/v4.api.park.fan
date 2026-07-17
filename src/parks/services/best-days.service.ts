import { Injectable, Logger, Inject } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { CacheKeys } from "../../common/cache/cache-keys";
import { safeJsonParse } from "../../common/utils/json.util";
import { SingleFlight } from "../../common/utils/single-flight.util";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { parseDateRange } from "../../common/utils/date-parsing.util";
import { CalendarService } from "./calendar.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { Park } from "../entities/park.entity";
import { CalendarDay } from "../dto/integrated-calendar.dto";
import {
  BestDaysResponse,
  BestDayEntry,
  BestDaysByDayOfWeek,
} from "../dto/best-days-calendar.dto";

/** Rolling window length, in days, from "today" (park timezone). */
const BEST_DAYS_WINDOW = 90;

/**
 * Snapshot TTL. Must outlive the refresh cadence (calendar warmup runs every
 * 12h) so a single skipped run never leaves the endpoint cold — 26h spans a
 * full missed daily cycle. Redis persists across deploys, so this also keeps
 * best-days warm through a redeploy.
 */
const SNAPSHOT_TTL = 26 * 60 * 60;

/**
 * Best-Days Service
 *
 * Owns the lean, precomputed best-days projection (the exact shape the frontend
 * keeps today, derived from the full `/calendar` response):
 *
 * - `precomputeForPark` materializes a `today → +90d` snapshot into Redis. It
 *   runs from the background calendar warmup, where the calendar month caches
 *   are already warm — so it reuses them and never pays the cold ML path.
 * - `getBestDays` serves the endpoint with a single Redis GET + in-memory
 *   slice. It NEVER triggers a calendar/ML rebuild, which is what lets the
 *   endpoint hit its p99 < 300 ms SLO cold and warm and lets the frontend drop
 *   its SSR seed timeout guard.
 */
@Injectable()
export class BestDaysService {
  private readonly logger = new Logger(BestDaysService.name);
  // Collapses a cache-miss stampede: many /best-days reads for the same park within the
  // on-demand rebuild window trigger ONE precompute, not N (the build is ~2s cold).
  private readonly rebuildFlight = new SingleFlight();

  constructor(
    private readonly calendarService: CalendarService,
    private readonly parkHistoricalStatsService: ParkHistoricalStatsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Serve the best-days projection from the materialized snapshot.
   *
   * @param park   Resolved park entity
   * @param fromStr Window start (YYYY-MM-DD, park tz). Defaults to today.
   * @param toStr   Window end (YYYY-MM-DD, park tz). Defaults to today + 90d.
   *
   * On a cache miss (snapshot never materialized / brand-new park) it returns a
   * graceful empty payload — the frontend already degrades to its own fetch —
   * rather than lazily rebuilding, honouring the "no ML compute on request" SLO.
   */
  async getBestDays(
    park: Park,
    fromStr?: string,
    toStr?: string,
  ): Promise<BestDaysResponse> {
    const tz = park.timezone || "UTC";
    const windowFrom = fromStr || getCurrentDateInTimezone(tz);
    // Only an explicit `to` upper-bounds the slice. The default window is the
    // whole snapshot from windowFrom onward — bounding it by a locally computed
    // "today + 90d" risks a boundary off-by-one silently dropping the last day.
    const windowTo = toStr;

    let snapshot = safeJsonParse<BestDaysResponse>(
      await this.redis.get(CacheKeys.bestDays(park.id)),
    );

    if (!snapshot) {
      // No snapshot — the 12h warmup hasn't materialized it yet (or it lapsed / the
      // warmup is failing). Rebuild ON-DEMAND (single-flighted) rather than serving
      // empty: an empty 200 gets CDN-cached for an hour AND leaves "Prognose heute"
      // blank, so this self-heals independently of the background warmup. The rebuild
      // is the same working 90-day build precomputeForPark does; it stores the snapshot.
      snapshot = safeJsonParse<BestDaysResponse>(
        await this.rebuildFlight
          .run(park.id, async () => {
            await this.precomputeForPark(park);
            return this.redis.get(CacheKeys.bestDays(park.id));
          })
          .catch(() => null),
      );
    }

    if (!snapshot) {
      // Rebuild failed too — degrade gracefully (200 + empty days).
      return {
        meta: {
          slug: park.slug,
          timezone: tz,
          hasOperatingSchedule: false,
          windowFrom,
          windowTo: windowTo ?? windowFrom,
        },
        days: [],
      };
    }

    const days = snapshot.days.filter(
      (d) => d.date >= windowFrom && (!windowTo || d.date <= windowTo),
    );

    return {
      meta: {
        ...snapshot.meta,
        // Reflect the actually-served window (the stored snapshot may start a
        // day earlier once "today" has advanced past its computedAt).
        windowFrom: days[0]?.date ?? windowFrom,
        windowTo: days[days.length - 1]?.date ?? windowTo ?? windowFrom,
      },
      days,
      byDayOfWeek: snapshot.byDayOfWeek,
    };
  }

  /**
   * Materialize the `today → +90d` best-days snapshot for one park.
   *
   * Reuses the (already warm, during the calendar warmup) calendar month caches
   * via `buildCalendarResponse`, projects each day to the lean shape, enriches
   * with the cached `/stats` weekday aggregate (best-effort), and stores it.
   *
   * @returns the park slug on success (for batched revalidation), or null on
   *   failure — never throws, so it can't break the warmup batch.
   */
  async precomputeForPark(park: Park): Promise<string | null> {
    try {
      const tz = park.timezone || "UTC";
      const { fromDate, toDate } = parseDateRange(undefined, undefined, {
        timezone: tz,
        defaultFromDaysAgo: 0,
        defaultToDaysAhead: BEST_DAYS_WINDOW,
      });
      const windowFrom = formatInParkTimezone(fromDate, tz);
      const windowTo = formatInParkTimezone(toDate, tz);

      // "none" is the includeHourly variant the frontend calendar/best-days
      // clients read and the warmup caches — so this assembles from warm month
      // caches instead of rebuilding.
      const calendar = await this.calendarService.buildCalendarResponse(
        park,
        fromDate,
        toDate,
        "none",
      );

      const byDayOfWeek = await this.loadByDayOfWeek(park);

      const snapshot: BestDaysResponse = {
        meta: {
          slug: park.slug,
          timezone: tz,
          hasOperatingSchedule: calendar.meta.hasOperatingSchedule,
          computedAt: new Date().toISOString(),
          windowFrom,
          windowTo,
        },
        days: calendar.days.map((d) => this.projectDay(d)),
        ...(byDayOfWeek ? { byDayOfWeek } : {}),
      };

      await this.redis.set(
        CacheKeys.bestDays(park.id),
        JSON.stringify(snapshot),
        "EX",
        SNAPSHOT_TTL,
      );

      return park.slug;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.logger.debug(
        `Best-days precompute skipped for ${park.slug}: ${msg}`,
      );
      return null;
    }
  }

  /** Project a full calendar day down to the lean best-days entry. */
  private projectDay(d: CalendarDay): BestDayEntry {
    return {
      date: d.date,
      status: d.status,
      crowdLevel: d.crowdLevel,
      predictedCrowdLevel: d.predictedCrowdLevel,
      isHoliday: d.isHoliday,
      isSchoolVacation: d.isSchoolVacation,
      isBridgeDay: d.isBridgeDay,
    };
  }

  /**
   * Best-effort weekday aggregate from the cached `/stats` payload. Read-only:
   * never triggers the heavy 2-year percentile scan (returns null when cold),
   * so the optional field never slows the precompute.
   */
  private async loadByDayOfWeek(
    park: Park,
  ): Promise<BestDaysByDayOfWeek[] | undefined> {
    try {
      const dow =
        await this.parkHistoricalStatsService.getCachedByDayOfWeek(park);
      if (!dow || dow.length === 0) return undefined;
      return dow.map((d) => ({
        dayOfWeek: d.dayOfWeek,
        avgCrowdScore: d.avgCrowdScore,
        sampleDays: d.sampleDays,
      }));
    } catch {
      return undefined;
    }
  }
}
