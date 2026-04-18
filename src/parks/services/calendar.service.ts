import { Injectable, Logger, Inject } from "@nestjs/common";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { MLService } from "../../ml/ml.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { Park } from "../entities/park.entity";
import { ScheduleEntry, ScheduleType } from "../entities/schedule-entry.entity";
import {
  IntegratedCalendarResponse,
  CalendarDay,
  OperatingHours,
  CalendarEvent,
  InfluencingHoliday,
  WeatherSummary,
  HourlyPrediction,
} from "../dto/integrated-calendar.dto";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getTomorrowDateInTimezone,
} from "../../common/utils/date.util";
import {
  calculateHolidayInfo,
  HolidayEntry,
} from "../../common/utils/holiday.utils";
import { normalizeRegionCode } from "../../common/utils/region.util";
import { WeatherData } from "../entities/weather-data.entity";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { Holiday } from "../../holidays/entities/holiday.entity";
import { PredictionDto } from "../../ml/dto";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { ParkStatus } from "../../common/types/status.type";

const SCHEDULE_REFRESH_GAP_DAYS = 5; // Trigger refresh if schedule ends < 5 days from requested date

/**
 * Calendar Service
 *
 * Combines Schedule, Weather, ML Predictions, Holidays, and Events into a unified API.
 */
@Injectable()
export class CalendarService {
  private readonly logger = new Logger(CalendarService.name);
  private readonly CALENDAR_CACHE_TTL = 30 * 60; // 30 minutes (reduced from 1h)

  constructor(
    private readonly parksService: ParksService,
    private readonly weatherService: WeatherService,
    private readonly mlService: MLService,
    private readonly analyticsService: AnalyticsService,
    private readonly holidaysService: HolidaysService,
    private readonly attractionsService: AttractionsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    @InjectQueue("park-metadata") private readonly parkMetadataQueue: Queue,
  ) {}

  /**
   * Build integrated calendar response
   *
   * @param park - Park entity
   * @param fromDate - Start date (park timezone)
   * @param toDate - End date (park timezone)
   * @param includeHourly - Which days should include hourly predictions
   * @returns Complete calendar response
   */
  async buildCalendarResponse(
    park: Park,
    fromDate: Date,
    toDate: Date,
    includeHourly:
      | "today+tomorrow"
      | "today"
      | "none"
      | "all" = "today+tomorrow",
  ): Promise<IntegratedCalendarResponse> {
    const fromStr = formatInParkTimezone(fromDate, park.timezone);
    const toStr = formatInParkTimezone(toDate, park.timezone);
    const today = getCurrentDateInTimezone(park.timezone);

    // Try per-month cache: assemble from calendar:month:{parkId}:YYYY-MM:{includeHourly}
    const monthsInRange = this.getMonthsInRange(
      fromDate,
      toDate,
      park.timezone,
    );
    const monthCacheKeys = monthsInRange.map(
      (ym) => `calendar:month:${park.id}:${ym}:${includeHourly}`,
    );
    const monthCached = await Promise.all(
      monthCacheKeys.map((k) => this.redis.get(k)),
    );
    if (monthCached.every((v) => v != null)) {
      const allDays: CalendarDay[] = [];
      for (const raw of monthCached) {
        const days = JSON.parse(raw!) as CalendarDay[];
        allDays.push(...days);
      }
      allDays.sort((a, b) => a.date.localeCompare(b.date));
      const daysInRange = allDays
        .filter((d) => d.date >= fromStr && d.date <= toStr)
        .map((d) => ({ ...d, status: d.status ?? "UNKNOWN" }));
      const response: IntegratedCalendarResponse = {
        meta: {
          slug: park.slug,
          timezone: park.timezone,
          hasOperatingSchedule:
            allDays.length > 0 &&
            allDays.some((d) => d.hours && !d.hours.isInferred),
        },
        days: daysInRange,
      };
      this.logger.debug(
        `Calendar assembled from ${monthsInRange.length} month cache(s): ${fromStr}–${toStr}`,
      );
      return response;
    }
    this.logger.debug(
      `Building calendar for ${park.slug} from ${fromStr} to ${toStr}`,
    );

    // Only fetch hourly ML predictions when the range actually contains today or tomorrow,
    // or when includeHourly="all". Skipping this for pure future-month requests (e.g. July)
    // eliminates an unnecessary sequential ML HTTP call that was causing slowness.
    const tomorrow = getTomorrowDateInTimezone(park.timezone);
    const needsHourly =
      includeHourly === "all" ||
      (includeHourly !== "none" && fromStr <= tomorrow && toStr >= today);

    // Fetch all data in parallel (including hourly ML when needed)
    const results = await Promise.all([
      this.parksService.getSchedule(park.id, fromDate, toDate).catch((err) => {
        this.logger.warn(
          `Schedule unavailable for ${park.slug}: ${err.message}`,
        );
        return [];
      }),
      this.weatherService
        .getWeatherData(park.id, fromDate, toDate, park.timezone)
        .catch((err) => {
          this.logger.warn(
            `Weather data unavailable for ${park.slug}: ${err.message}`,
          );
          return [];
        }),
      this.mlService.getParkPredictions(park.id, "daily").catch((err) => {
        this.logger.warn(
          `ML predictions unavailable for ${park.slug}: ${err.message}`,
        );
        return { predictions: [] };
      }),
      (() => {
        const countries = [
          park.countryCode,
          ...(park.influencingRegions || []).map((r) => r.countryCode),
        ];
        const uniqueCountries = [...new Set(countries)];

        // Extend holiday fetch range by +/- 1 day to support bridge day detection
        const extendedFromDate = new Date(fromDate);
        extendedFromDate.setDate(fromDate.getDate() - 1);
        const extendedToDate = new Date(toDate);
        extendedToDate.setDate(toDate.getDate() + 1);

        return Promise.all(
          uniqueCountries.map((cc) =>
            this.holidaysService.getHolidays(
              cc,
              formatInParkTimezone(extendedFromDate, park.timezone),
              formatInParkTimezone(extendedToDate, park.timezone),
            ),
          ),
        )
          .then((res) => res.flat())
          .catch((err) => {
            this.logger.warn(
              `Holidays data partially unavailable for ${park.slug}: ${err.message}`,
            );
            return [];
          });
      })(),
      this.parksService.getOperatingDateRange(park.id, park.timezone),
      needsHourly
        ? this.mlService
            .getParkPredictions(park.id, "hourly")
            .then((r) => r.predictions)
            .catch(() => [] as PredictionDto[])
        : Promise.resolve([] as PredictionDto[]),
      this.parksService.isParkSeasonal(park.id),
      this.parksService.getDerivedHistoricalHours(
        park.id,
        fromStr,
        toStr,
        park.timezone,
      ),
    ]);

    const schedules = results[0] as ScheduleEntry[];
    const weatherData = results[1] as WeatherData[];
    const mlPredictions = results[2] as { predictions: PredictionDto[] };
    const holidays = results[3] as Holiday[];
    const operatingDateRange = results[4] as {
      minDate: string | null;
      maxDate: string | null;
    };
    const hourlyPredictionsList = results[5] as PredictionDto[];
    const isSeasonal = results[6] as boolean;
    const derivedHistoricalHours = results[7] as Map<
      string,
      { openingTime: string; closingTime: string }
    >;

    // Derive booleans / range info from operatingDateRange
    const parkHasOperatingSchedule =
      operatingDateRange.minDate !== null &&
      operatingDateRange.maxDate !== null;

    // On-demand schedule refresh: if requested range has little/no schedule data, trigger
    // a background sync so next request may get updated opening hours (e.g. when source publishes new months).
    this.requestScheduleRefreshIfNeeded(
      park,
      fromDate,
      toDate,
      schedules,
    ).catch((err) =>
      this.logger.warn(
        `Schedule refresh check failed for ${park.slug}: ${err.message}`,
      ),
    );

    // Pre-compute predicted crowd levels for future days using headliner median / P50 baseline
    // (mirrors calculateCrowdLevelForDate logic for historical days)
    const [allHeadliners, p50Baseline] = await Promise.all([
      this.analyticsService.getHeadlinerAttractions(park.id),
      this.analyticsService.getP50BaselineFromCache(park.id),
    ]);
    const headlinerIdSet = new Set(allHeadliners.map((h) => h.attractionId));
    const predictedCrowdLevels = this.buildPredictedCrowdLevels(
      mlPredictions.predictions,
      headlinerIdSet,
      p50Baseline,
    );

    // Batch Redis MGET for crowd level cache to avoid N round-trips per historical day
    const historicalDateStrs: string[] = [];
    const walk = new Date(fromDate);
    while (walk <= toDate) {
      const d = formatInParkTimezone(walk, park.timezone);
      if (d <= today) historicalDateStrs.push(d);
      walk.setDate(walk.getDate() + 1);
    }
    const crowdLevelKeys =
      historicalDateStrs.length > 0
        ? historicalDateStrs.map(
            (d) => `analytics:crowdlevel:park:${park.id}:${d}`,
          )
        : [];
    const crowdLevelRaw =
      crowdLevelKeys.length > 0 ? await this.redis.mget(...crowdLevelKeys) : [];
    const prefetchedCrowdLevels = new Map<
      string,
      { crowdLevel: CrowdLevel | "closed"; peakLoad?: CrowdLevel | "closed" }
    >();
    crowdLevelRaw.forEach((raw, i) => {
      if (!raw || i >= historicalDateStrs.length) return;
      try {
        const parsed = JSON.parse(raw) as {
          crowdLevel: CrowdLevel;
          peakCrowdLevel?: CrowdLevel;
        };
        prefetchedCrowdLevels.set(historicalDateStrs[i], {
          crowdLevel: parsed.crowdLevel,
          peakLoad: parsed.peakCrowdLevel,
        });
      } catch {
        // ignore invalid cache
      }
    });

    // For today: override crowdLevel with real-time occupancy (current spot wait) so the calendar
    // matches the park overview. calculateCrowdLevelForDate uses P50 of the whole day so far
    // (morning + afternoon averaged), while the park overview uses the current spot wait —
    // causing "moderate" on the calendar vs "high" in the overview on busy afternoons.
    const occupancyRaw = await this.redis.get(`park:occupancy:${park.id}`);
    if (occupancyRaw) {
      try {
        const occ = JSON.parse(occupancyRaw) as { current: number };
        if (typeof occ.current === "number" && occ.current > 0) {
          const liveCrowdLevel = this.analyticsService.determineCrowdLevel(
            occ.current,
          ) as CrowdLevel;
          const existing = prefetchedCrowdLevels.get(today);
          prefetchedCrowdLevels.set(today, {
            crowdLevel: liveCrowdLevel,
            peakLoad: existing?.peakLoad, // keep daily peak from historical cache
          });
        }
      } catch {
        // ignore — fall back to calculateCrowdLevelForDate path
      }
    }

    // Collect all dates in the range
    const datesToBuild: Date[] = [];
    const currentDate = new Date(fromDate);
    while (currentDate <= toDate) {
      datesToBuild.push(new Date(currentDate));
      currentDate.setDate(currentDate.getDate() + 1);
    }

    // Build all calendar days in parallel
    const days = await Promise.all(
      datesToBuild.map((date) => {
        const dateStr = formatInParkTimezone(date, park.timezone);
        return this.buildCalendarDay(
          park,
          date,
          schedules,
          weatherData,
          mlPredictions.predictions,
          holidays,
          includeHourly,
          today,
          hourlyPredictionsList,
          prefetchedCrowdLevels,
          parkHasOperatingSchedule,
          operatingDateRange,
          predictedCrowdLevels,
          isSeasonal,
          derivedHistoricalHours.get(dateStr) || null,
        );
      }),
    );

    // Build response
    const response: IntegratedCalendarResponse = {
      meta: {
        slug: park.slug,
        timezone: park.timezone,
        hasOperatingSchedule: parkHasOperatingSchedule,
      },
      days,
    };

    // Cache full months only (per-month keys); no full-range key — overlapping requests reuse months
    await this.cacheFullMonths(
      park.id,
      park.timezone,
      days,
      includeHourly,
      today,
    );

    return response;
  }

  /** Returns ["YYYY-MM", ...] for all months overlapping [fromDate, toDate] in park timezone. */
  private getMonthsInRange(
    fromDate: Date,
    toDate: Date,
    timezone: string,
  ): string[] {
    const seen = new Set<string>();
    const cur = new Date(fromDate);
    const to = new Date(toDate);
    while (cur <= to) {
      const ym = formatInParkTimezone(cur, timezone).slice(0, 7); // YYYY-MM
      seen.add(ym);
      cur.setDate(cur.getDate() + 1);
    }
    return [...seen].sort();
  }

  /**
   * Cache full months so overlapping ranges can reuse (e.g. Feb 1–15 and Feb 10–28 both use Feb).
   * Only caches months where we have all days (1st through last day of that month).
   */
  private async cacheFullMonths(
    parkId: string,
    timezone: string,
    days: CalendarDay[],
    includeHourly: string,
    today: string,
  ): Promise<void> {
    const byMonth = new Map<string, CalendarDay[]>();
    for (const d of days) {
      const ym = d.date.slice(0, 7); // YYYY-MM
      if (!byMonth.has(ym)) byMonth.set(ym, []);
      byMonth.get(ym)!.push(d);
    }
    const writes: Promise<void>[] = [];
    for (const [ym, monthDays] of byMonth) {
      monthDays.sort((a, b) => a.date.localeCompare(b.date));
      const [y, m] = ym.split("-").map(Number);
      const lastDay = new Date(y, m, 0).getDate(); // last day of month
      if (monthDays.length !== lastDay) continue;
      if (monthDays[0].date !== `${ym}-01`) continue;
      if (
        monthDays[monthDays.length - 1].date !==
        `${ym}-${String(lastDay).padStart(2, "0")}`
      )
        continue;
      const key = `calendar:month:${parkId}:${ym}:${includeHourly}`;
      const ttl = today.startsWith(ym) ? 5 * 60 : this.CALENDAR_CACHE_TTL;
      writes.push(
        this.redis
          .set(key, JSON.stringify(monthDays), "EX", ttl)
          .then(() =>
            this.logger.debug(`Cached calendar month: ${key} (TTL: ${ttl}s)`),
          ),
      );
    }
    await Promise.all(writes);
  }

  /**
   * If the requested calendar range has little or no schedule data (e.g. Efteling March+),
   * trigger a background schedule sync for this park so that when the source publishes
   * new opening hours, the next request can get them.
   * Rate-limited to once per 12h per park. Only triggers when the gap is at least SCHEDULE_REFRESH_GAP_DAYS.
   * UNKNOWN entries (from fillScheduleGaps) count as "we have data until X" so we don't trigger
   * when we already have holiday placeholders for the requested range.
   */
  private async requestScheduleRefreshIfNeeded(
    park: Park,
    fromDate: Date,
    toDate: Date,
    schedules: ScheduleEntry[],
  ): Promise<void> {
    const today = getCurrentDateInTimezone(park.timezone);
    const toStr = formatInParkTimezone(toDate, park.timezone);
    if (toStr <= today) return; // No future dates in range

    // Only count OPERATING/CLOSED entries (real API data), not UNKNOWN placeholders from
    // fillScheduleGaps. UNKNOWN means "no data from source yet" — if the requested range
    // only has UNKNOWN entries, we still want to trigger a refresh to check for new data.
    const realSchedules = schedules.filter(
      (s) => s.scheduleType !== ScheduleType.UNKNOWN,
    );
    const maxScheduleDate =
      realSchedules.length > 0
        ? formatInParkTimezone(
            new Date(
              Math.max(
                ...realSchedules.map((s) =>
                  (typeof s.date === "string"
                    ? new Date(`${s.date}T12:00:00Z`)
                    : s.date
                  ).getTime(),
                ),
              ),
            ),
            park.timezone,
          )
        : null;
    const cutoff = formatInParkTimezone(
      (() => {
        const d = new Date(toDate);
        d.setDate(d.getDate() - SCHEDULE_REFRESH_GAP_DAYS);
        return d;
      })(),
      park.timezone,
    );
    const rangeNeedsData = !maxScheduleDate || maxScheduleDate < cutoff;

    if (!rangeNeedsData) return;

    // Rate limiting: once per 12h
    const cacheKey = `calendar:refresh-check:${park.id}`;
    const wasChecked = await this.redis.get(cacheKey);
    if (wasChecked) return;

    await this.redis.set(cacheKey, "1", "EX", 12 * 60 * 60);

    // Trigger sync job
    await this.parkMetadataQueue.add(
      "sync-park-schedule",
      { parkId: park.id },
      { removeOnComplete: true },
    );
    this.logger.debug(
      `Triggered on-demand schedule refresh for ${park.slug} (range needs data until ${toStr})`,
    );
  }

  /**
   * Builds a single calendar day entry
   *
   * @param park - Park entity
   * @param date - The date to build (park local midnight)
   * @param schedules - Pre-fetched schedule list
   * @param weatherData - Pre-fetched weather list
   * @param mlPredictions - Pre-fetched daily ML predictions
   * @param holidays - Pre-fetched holiday list (local + influencing)
   * @param includeHourly - Current includeHourly config
   * @param today - Current date string (YYYY-MM-DD)
   * @param hourlyPredictionsPreFetched - Hourly ML predictions for the park (fetched once per calendar build to avoid N+1)
   * @param prefetchedCrowdLevels - Crowd level cache (Redis MGET) for historical dates to avoid N round-trips
   */
  private async buildCalendarDay(
    park: Park,
    date: Date,
    schedules: ScheduleEntry[],
    weatherData: WeatherData[],
    mlPredictions: PredictionDto[],
    holidays: Holiday[],
    includeHourly: string,
    today: string,
    hourlyPredictionsPreFetched: PredictionDto[] = [],
    prefetchedCrowdLevels: Map<
      string,
      { crowdLevel: CrowdLevel | "closed"; peakLoad?: CrowdLevel | "closed" }
    > = new Map(),
    parkHasOperatingSchedule: boolean = false,
    operatingDateRange: {
      minDate: string | null;
      maxDate: string | null;
    } = { minDate: null, maxDate: null },
    predictedCrowdLevels: Map<
      string,
      { crowdLevel: CrowdLevel; peakLoad: CrowdLevel }
    > = new Map(),
    isSeasonal: boolean = false,
    derivedHours: { openingTime: string; closingTime: string } | null = null,
  ): Promise<CalendarDay> {
    const dateStr = formatInParkTimezone(date, park.timezone);
    // Find schedule for this day
    const schedule = schedules.find(
      (s) => formatInParkTimezone(s.date, park.timezone) === dateStr,
    );

    // Find ML prediction for this day
    const mlPrediction = mlPredictions.find(
      (p) => p.predictedTime?.split("T")[0] === dateStr,
    );

    // Find weather for this day
    const weather = weatherData.find(
      (w) =>
        formatInParkTimezone(
          typeof w.date === "string" ? new Date(`${w.date}T12:00:00Z`) : w.date,
          park.timezone,
        ) === dateStr,
    );

    // Build holiday map for calculateHolidayInfo utility (includes ±1 day for bridge day detection)
    const holidayMap = new Map<string, string | HolidayEntry>();
    const extendedHolidays = [...holidays];

    for (const h of extendedHolidays) {
      // Normalize to noon UTC to prevent timezone shifts (YYYY-MM-DD from DB)
      const normalizedDate = new Date(h.date);
      normalizedDate.setUTCHours(12, 0, 0, 0);
      const hDateStr = formatInParkTimezone(normalizedDate, park.timezone);

      // Use normalized region codes for consistent matching
      const normalizedParkRegion = normalizeRegionCode(park.regionCode);
      const normalizedHolidayRegion = normalizeRegionCode(h.region);
      const isLocal =
        h.country === park.countryCode &&
        (h.isNationwide ||
          !h.region ||
          normalizedHolidayRegion === normalizedParkRegion);

      if (isLocal) {
        // Add to holidayMap with type information
        const existing = holidayMap.get(hDateStr);
        const hType = h.holidayType;

        if (!existing || typeof existing === "string") {
          holidayMap.set(hDateStr, {
            name: h.localName || h.name || "",
            type: hType,
            allTypes: [hType],
          });
        } else {
          // Aggregate types
          if (!existing.allTypes) existing.allTypes = [existing.type];
          if (!existing.allTypes.includes(hType)) {
            existing.allTypes.push(hType);
          }

          // Prioritize public holidays for the main entry (useful for bridge day logic)
          if (hType === "public" || hType === "bank") {
            existing.name = h.localName || h.name || "";
            existing.type = hType;
          }
        }
      }
    }

    // Use utility function to calculate holiday info including bridge days
    const holidayInfo = calculateHolidayInfo(date, holidayMap, park.timezone);
    const isHoliday = holidayInfo.isPublicHoliday; // Only public holidays, not school
    const isBridgeDay = holidayInfo.isBridgeDay;
    const isSchoolVacation = holidayInfo.isSchoolHoliday;

    // Build events array and influencing holidays
    const events: CalendarEvent[] = [];
    const influencingHolidays: InfluencingHoliday[] = [];
    const seenEvents = new Set<string>();

    const dayHolidays = holidays.filter((h) => {
      const normalizedDate = new Date(h.date);
      normalizedDate.setUTCHours(12, 0, 0, 0);
      return formatInParkTimezone(normalizedDate, park.timezone) === dateStr;
    });

    for (const h of dayHolidays) {
      const type = h.holidayType === "school" ? "school-holiday" : "holiday";
      const key = `${type}:${h.name}`;

      // Use normalized region codes for consistent matching
      const normalizedParkRegion = normalizeRegionCode(park.regionCode);
      const normalizedHolidayRegion = normalizeRegionCode(h.region);
      const isLocal =
        h.country === park.countryCode &&
        (h.isNationwide ||
          !h.region ||
          normalizedHolidayRegion === normalizedParkRegion);

      if (isLocal) {
        if (!seenEvents.has(key)) {
          seenEvents.add(key);
          events.push({
            name: h.name,
            type: type as "holiday" | "school-holiday",
            isNationwide: h.isNationwide,
          });
        }
      } else {
        // Influencing but not local
        influencingHolidays.push({
          name: h.name,
          source: {
            countryCode: h.country,
            regionCode: h.region
              ? normalizeRegionCode(h.region.split("-").pop() ?? null)
              : null,
          },
          holidayType: h.holidayType,
        });
      }
    }

    // status = ParkStatus (OPERATING | CLOSED | UNKNOWN); UNKNOWN = no schedule data yet
    let status: ParkStatus =
      schedule?.scheduleType === ScheduleType.OPERATING
        ? "OPERATING"
        : schedule?.scheduleType === ScheduleType.CLOSED
          ? "CLOSED"
          : schedule?.scheduleType === ScheduleType.UNKNOWN
            ? "UNKNOWN"
            : "UNKNOWN";

    // Recover status from ride activity for past days
    const isHistorical = dateStr <= today;
    const isStrictlyPast = dateStr < today;
    let isEstimated = false;

    if (isHistorical && status === "UNKNOWN" && !parkHasOperatingSchedule) {
      if (derivedHours) {
        status = "OPERATING";
        isEstimated = true;
      } else if (isStrictlyPast) {
        // No activity detected on a past day: it was CLOSED
        status = "CLOSED";
        isEstimated = true;
      }
      // If it is 'today' and no activity yet, we keep it as UNKNOWN (allowing predictions below)
    }

    // Seasonal Closure detection (Gap-fill)
    if (
      status === "UNKNOWN" &&
      operatingDateRange.minDate &&
      operatingDateRange.maxDate
    ) {
      // 1. Classic Gap (between two operating dates)
      if (
        dateStr > operatingDateRange.minDate &&
        dateStr < operatingDateRange.maxDate
      ) {
        status = "CLOSED";
      }

      // 2. Off-season (before first known or after last known operating date)
      // Only infer CLOSED if the park has a history of seasonal closures.
      // This prevents killing future crowd predictions for year-round parks
      // that just haven't published next month's hours yet.
      if (isSeasonal) {
        if (
          dateStr < operatingDateRange.minDate ||
          dateStr > operatingDateRange.maxDate
        ) {
          status = "CLOSED";
        }
      }
    }

    // Compute crowd level for the day (needed even when no schedule, to infer open/closed)
    let inferredCrowdLevel: CrowdLevel | "closed";
    let peakLoad: CrowdLevel | "closed" | undefined;

    if (isHistorical) {
      const prefetched = prefetchedCrowdLevels.get(dateStr);
      if (prefetched !== undefined) {
        inferredCrowdLevel = prefetched.crowdLevel;
        peakLoad = prefetched.peakLoad;
      } else {
        // Always query real usage data for today/past days (calculateCrowdLevelForDate
        // has its own Redis cache: 30 min for today, 24h for historical).
        // ML prediction is only the last resort when genuinely no data exists.
        const crowdData =
          await this.analyticsService.calculateCrowdLevelForDate(
            park.id,
            "park",
            dateStr,
            park.timezone,
          );
        inferredCrowdLevel = crowdData.hasData
          ? crowdData.crowdLevel
          : predictedCrowdLevels.get(dateStr)?.crowdLevel ||
            mlPrediction?.crowdLevel ||
            "moderate";

        peakLoad = crowdData.hasData
          ? crowdData.peakCrowdLevel
          : predictedCrowdLevels.get(dateStr)?.peakLoad ||
            mlPrediction?.crowdLevel || // Fallback to normal crowdLevel if no P90
            "moderate";
      }
    } else {
      const predicted = predictedCrowdLevels.get(dateStr);
      inferredCrowdLevel =
        predicted?.crowdLevel || mlPrediction?.crowdLevel || "moderate";

      peakLoad = predicted?.peakLoad || mlPrediction?.crowdLevel || "moderate";
    }

    // Future UNKNOWN (no schedule): show ML crowd prediction; strictly past CLOSED → closed
    let crowdLevel: CrowdLevel | "closed";
    if (status === "OPERATING") {
      crowdLevel = inferredCrowdLevel;
    } else if (status === "UNKNOWN" && !isStrictlyPast) {
      crowdLevel = inferredCrowdLevel; // future day (or today) without schedule: still show prediction
    } else {
      crowdLevel = "closed";
      peakLoad = "closed";
    }

    // Build operating hours
    let hours: OperatingHours | undefined = undefined;
    if (schedule && schedule.scheduleType === ScheduleType.OPERATING) {
      hours = {
        openingTime: schedule.openingTime?.toISOString() || "",
        closingTime: schedule.closingTime?.toISOString() || "",
        type: schedule.scheduleType,
        isInferred: false,
      };
    } else if (isHistorical && derivedHours) {
      // Use reconstructed hours for the past
      hours = {
        openingTime: derivedHours.openingTime,
        closingTime: derivedHours.closingTime,
        type: ScheduleType.OPERATING,
        isInferred: true,
      };
    }

    // Build weather summary
    const weatherSummary: WeatherSummary | undefined = weather
      ? {
          condition: weather.weatherCode
            ? getWeatherDescription(weather.weatherCode)
            : "unknown",
          tempMin: Number(weather.temperatureMin) ?? 0,
          tempMax: Number(weather.temperatureMax) ?? 0,
          rainChance: Number(weather.precipitationSum) ?? 0,
          icon: weather.weatherCode ?? 0,
        }
      : undefined;

    const day: CalendarDay = {
      date: dateStr,
      status,
      hours,
      crowdLevel,
      peakLoad,
      weather: weatherSummary,
      isToday: dateStr === today,
      isEstimated,
      isHoliday,
      isBridgeDay,
      isSchoolVacation,
      recommendation: this.computeRecommendation(
        crowdLevel,
        status,
        isHoliday,
        isSchoolVacation,
        isBridgeDay,
        influencingHolidays.length,
        weatherSummary || null,
      ),
      events: events.length > 0 ? events : undefined,
      influencingHolidays:
        influencingHolidays.length > 0 ? influencingHolidays : undefined,
    };

    // Add hourly data if requested (uses pre-fetched list to avoid N+1 ML calls)
    if (this.shouldIncludeHourly(date, includeHourly, park.timezone)) {
      day.hourly = this.buildHourlyPredictionsFromList(
        park,
        dateStr,
        hourlyPredictionsPreFetched,
      );
    }

    return day;
  }

  /**
   * Helper to build hourly predictions for a single day from a pre-fetched list
   */
  private buildHourlyPredictionsFromList(
    park: Park,
    dateStr: string,
    allPredictions: PredictionDto[],
  ): HourlyPrediction[] | undefined {
    const dailyPreds = allPredictions.filter((p) =>
      p.predictedTime.startsWith(dateStr),
    );
    if (dailyPreds.length === 0) return undefined;

    // Group by hour
    const hoursMap = new Map<string, PredictionDto[]>();
    for (const p of dailyPreds) {
      const hour = p.predictedTime.substring(11, 13); // HH
      if (!hoursMap.has(hour)) hoursMap.set(hour, []);
      hoursMap.get(hour)!.push(p);
    }

    // Aggregate (median)
    const result: HourlyPrediction[] = [];
    for (const [hour, preds] of hoursMap) {
      const waits = preds.map((p) => p.predictedWaitTime);
      waits.sort((a, b) => a - b);
      const median = waits[Math.floor(waits.length / 2)];

      // Use a generic P50 baseline (25m) for hourly mapping
      const { rating } = this.analyticsService.getLoadRating(median, 25);

      result.push({
        hour: parseInt(hour, 10),
        crowdLevel: rating,
        predictedWaitTime: Math.round(median),
      });
    }

    return result.sort((a, b) => a.hour - b.hour);
  }

  /**
   * Decides if hourly data should be included for a specific date
   */
  private shouldIncludeHourly(
    date: Date,
    includeHourly: string,
    timezone: string,
  ): boolean {
    if (includeHourly === "none") return false;
    if (includeHourly === "all") return true;

    const dateStr = formatInParkTimezone(date, timezone);
    const today = getCurrentDateInTimezone(timezone);

    if (includeHourly === "today") return dateStr === today;

    if (includeHourly === "today+tomorrow") {
      const tomorrow = getTomorrowDateInTimezone(timezone);
      return dateStr === today || dateStr === tomorrow;
    }

    return false;
  }

  /**
   * Aggregate per-attraction ML predictions into a date → CrowdLevel map for future days.
   *
   * Mirrors calculateCrowdLevelForDate: median of headliner wait times / park P50 baseline.
   * Falls back to all attractions if no headliners are defined.
   */
  private buildPredictedCrowdLevels(
    predictions: PredictionDto[],
    headlinerIdSet: Set<string>,
    p50Baseline: number,
  ): Map<string, { crowdLevel: CrowdLevel; peakLoad: CrowdLevel }> {
    const result = new Map<
      string,
      { crowdLevel: CrowdLevel; peakLoad: CrowdLevel }
    >();
    const datesMap = new Map<string, PredictionDto[]>();

    // 1. Group by date
    for (const p of predictions) {
      const date = p.predictedTime.split("T")[0];
      if (!datesMap.has(date)) datesMap.set(date, []);
      datesMap.get(date)!.push(p);
    }

    // 2. Aggregate per date
    for (const [date, dailyPreds] of datesMap) {
      // Filter to headliners
      const headliners =
        headlinerIdSet.size > 0
          ? dailyPreds.filter((p) => headlinerIdSet.has(p.attractionId))
          : dailyPreds;

      if (headliners.length === 0) continue;

      // Logic identical to calculateCrowdLevelForDate:
      // a) Median wait time
      const waits = headliners.map((p) => p.predictedWaitTime);
      waits.sort((a, b) => a - b);
      const medianWait = waits[Math.floor(waits.length / 2)];

      // b) P90 Peak Load proxy (simplification: 90th percentile of predicted waits)
      const p90Wait = waits[Math.floor(waits.length * 0.9)];

      // c) Map to crowd level using P50 baseline
      const { rating: crowdLevel } = this.analyticsService.getLoadRating(
        medianWait,
        p50Baseline,
      );
      const { rating: peakLoad } = this.analyticsService.getLoadRating(
        p90Wait,
        p50Baseline,
      );

      result.set(date, { crowdLevel, peakLoad });
    }

    return result;
  }

  /**
   * Compute visit recommendation score (0-5)
   * Factors:
   *   Crowd Level: very_low(0) to extreme(5)
   *   +2  public holiday (local)
   *   +1  school vacation (local)
   *   +1  bridge day
   *   +1  heavy influencing holidays from neighboring regions
   *   +1  rain likely (rainChance > 60%) — poor visitor experience
   *
   * Final score → recommendation:
   *   0-1 → highly_recommended
   *   2   → recommended
   *   3   → neutral
   *   4   → avoid
   *   ≥5  → strongly_avoid
   */
  private computeRecommendation(
    crowdLevel: CrowdLevel | "closed",
    status: string,
    isHoliday: boolean,
    isSchoolVacation: boolean,
    isBridgeDay: boolean,
    influencingHolidayCount: number,
    weather: WeatherSummary | null,
  ):
    | "highly_recommended"
    | "recommended"
    | "neutral"
    | "avoid"
    | "strongly_avoid"
    | "closed" {
    if (crowdLevel === "closed" || status === "CLOSED") return "closed";

    const crowdScore: Record<string, number> = {
      very_low: 0,
      low: 1,
      moderate: 2,
      high: 3,
      very_high: 4,
      extreme: 5,
    };

    let score = crowdScore[crowdLevel] ?? 2;

    if (isHoliday) score += 2;
    if (isSchoolVacation) score += 1;
    if (isBridgeDay) score += 1;
    if (influencingHolidayCount >= 2) score += 1;
    if (weather && weather.rainChance > 60) score += 1;

    if (score <= 1) return "highly_recommended";
    if (score === 2) return "recommended";
    if (score === 3) return "neutral";
    if (score === 4) return "avoid";
    return "strongly_avoid";
  }
}
