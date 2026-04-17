import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { MLService } from "../../ml/ml.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { AttractionsService } from "../../attractions/attractions.service";
import {
  IntegratedCalendarResponse,
  CalendarDay,
  OperatingHours,
  WeatherSummary,
  CalendarEvent,
  HourlyPrediction,
} from "../dto/integrated-calendar.dto";
import { Park } from "../entities/park.entity";
import { ScheduleEntry, ScheduleType } from "../entities/schedule-entry.entity";
import { ParkStatus } from "../../common/types/status.type";
import { CrowdLevel } from "../../common/types/crowd-level.type";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import { InfluencingHoliday } from "../dto/schedule-item.dto";
import { WeatherData } from "../entities/weather-data.entity";
import { Holiday } from "../../holidays/entities/holiday.entity";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  getTomorrowDateInTimezone,
} from "../../common/utils/date.util";
import { normalizeRegionCode } from "../../common/utils/region.util";
import {
  calculateHolidayInfo,
  HolidayEntry,
} from "../../common/utils/holiday.utils";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";

/** TTL for "schedule refresh requested" rate-limit key (avoid hammering ThemeParks API). */
const SCHEDULE_REFRESH_RATE_LIMIT_TTL_SEC = 12 * 60 * 60; // 12 hours (was 6h – less aggressive)
/** Only trigger on-demand refresh when requested range ends this many days beyond our last schedule date. */
const SCHEDULE_REFRESH_GAP_DAYS = 14; // was 7 – avoid triggering for small gaps

/**
 * Calendar Service
 *
 * Orchestrates data from multiple sources to build integrated calendar responses.
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
    const [
      schedules,
      weatherData,
      mlPredictions,
      holidays,
      operatingDateRange,
      hourlyPredictionsList,
      isSeasonal,
    ] = await Promise.all([
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
    ]);

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
      allHeadliners,
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

    // Collect all dates in the range
    const datesToBuild: Date[] = [];
    const currentDate = new Date(fromDate);
    while (currentDate <= toDate) {
      datesToBuild.push(new Date(currentDate));
      currentDate.setDate(currentDate.getDate() + 1);
    }

    // Build all calendar days in parallel
    const days = await Promise.all(
      datesToBuild.map((date) =>
        this.buildCalendarDay(
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
        ),
      ),
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

    const rateLimitKey = `schedule:refresh:requested:${park.id}`;
    const wasSet = await this.redis.set(
      rateLimitKey,
      "1",
      "EX",
      SCHEDULE_REFRESH_RATE_LIMIT_TTL_SEC,
      "NX",
    );
    if (wasSet === "OK") {
      await this.parkMetadataQueue.add(
        "sync-park-schedule",
        { parkId: park.id },
        { removeOnComplete: true },
      );
      this.logger.debug(
        `Triggered on-demand schedule refresh for ${park.slug} (range needs data until ${toStr})`,
      );
    }
  }

  /**
   * Build a single calendar day
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
    const isHistorical = dateStr <= today;
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

    // Past + Today: only infer OPERATING from crowd level when park has NO OPERATING schedule at all.
    // Parks with OPERATING entries: keep UNKNOWN for days without schedule (gap-fill UNKNOWN).
    if (
      isHistorical &&
      status === "UNKNOWN" &&
      inferredCrowdLevel !== "closed" &&
      !parkHasOperatingSchedule
    ) {
      status = "OPERATING";
    }

    // Future UNKNOWN (no schedule): show ML crowd prediction; past/today non-OPERATING or CLOSED → closed
    let crowdLevel: CrowdLevel | "closed";
    if (status === "OPERATING") {
      crowdLevel = inferredCrowdLevel;
    } else if (status === "UNKNOWN" && !isHistorical) {
      crowdLevel = inferredCrowdLevel; // future day without schedule: still show prediction
    } else {
      crowdLevel = "closed";
      peakLoad = "closed";
    }

    // Build operating hours
    let hours: OperatingHours | null = null;
    if (schedule && schedule.scheduleType === ScheduleType.OPERATING) {
      hours = {
        openingTime: schedule.openingTime?.toISOString() || "",
        closingTime: schedule.closingTime?.toISOString() || "",
        type: schedule.scheduleType,
        isInferred: false,
      };
    } else if (status === "OPERATING" && mlPrediction) {
      hours = await this.inferOperatingHours(park, date);
    }

    // Build weather summary
    const weatherSummary: WeatherSummary | null = weather
      ? {
          condition: weather.weatherCode
            ? getWeatherDescription(weather.weatherCode)
            : "Unknown",
          icon: weather.weatherCode || 0,
          tempMin: weather.temperatureMin || 0,
          tempMax: weather.temperatureMax || 0,
          rainChance: Math.round(
            (weather.precipitationSum ?? 0) > 0
              ? Math.min(((weather.precipitationSum ?? 0) / 10) * 100, 100)
              : 0,
          ),
        }
      : null;

    // Compute visit recommendation using crowd level + contextual signals
    const recommendation = this.computeRecommendation(
      crowdLevel,
      status,
      isHoliday,
      isSchoolVacation,
      isBridgeDay,
      influencingHolidays.length,
      weatherSummary,
    );

    // Build calendar day
    const day: CalendarDay = {
      date: dateStr,
      status,
      isToday: dateStr === today,
      hours: hours || undefined,
      crowdLevel,
      peakLoad,
      recommendation,
      weather: weatherSummary || undefined,
      events,
      isHoliday,
      isBridgeDay,
      isSchoolVacation,
      influencingHolidays:
        influencingHolidays.length > 0 ? influencingHolidays : undefined,
    };

    // Add hourly data if requested (uses pre-fetched list to avoid N+1 ML calls)
    if (this.shouldIncludeHourly(date, includeHourly, park.timezone)) {
      day.hourly = this.buildHourlyPredictionsFromList(
        date,
        status,
        park.timezone,
        hourlyPredictionsPreFetched,
      );
    }

    return day;
  }

  /**
   * Determine if hourly data should be included for this date
   */
  private shouldIncludeHourly(
    date: Date,
    includeHourly: string,
    timezone: string,
  ): boolean {
    const todayStr = getCurrentDateInTimezone(timezone);
    const tomorrowStr = getTomorrowDateInTimezone(timezone);
    const dateStr = formatInParkTimezone(date, timezone);

    switch (includeHourly) {
      case "today+tomorrow":
        return dateStr === todayStr || dateStr === tomorrowStr;
      case "today":
        return dateStr === todayStr;
      case "all":
        return true;
      case "none":
        return false;
      default:
        return false;
    }
  }

  /**
   * Build hourly predictions for a day from pre-fetched list (avoids N+1 ML calls per calendar build).
   */
  private buildHourlyPredictionsFromList(
    date: Date,
    dayStatus: ParkStatus,
    timezone: string,
    hourlyPredictions: PredictionDto[],
  ): HourlyPrediction[] {
    if (dayStatus !== "OPERATING" || !hourlyPredictions.length) {
      return [];
    }
    const dateStr = formatInParkTimezone(date, timezone);
    const hourlyData = hourlyPredictions.filter((p) =>
      p.predictedTime?.startsWith(dateStr),
    );
    return hourlyData.map((p) => {
      const hour = new Date(p.predictedTime).getHours();
      return {
        hour,
        crowdLevel: (p.crowdLevel === "closed"
          ? "very_low"
          : p.crowdLevel) as CrowdLevel,
        predictedWaitTime: roundToNearest5Minutes(p.predictedWaitTime || 30),
        probability: p.confidence,
      };
    });
  }

  /**
   * Infer operating hours based on ML predictions and historical data
   */
  private async inferOperatingHours(
    park: Park,
    date: Date,
  ): Promise<OperatingHours> {
    // Simple heuristic: use standard theme park hours
    const dayOfWeek = date.getDay();
    const isWeekend = dayOfWeek === 0 || dayOfWeek === 6;

    const openingHour = isWeekend ? 9 : 10;
    const closingHour = isWeekend ? 20 : 18;

    const dateStr = formatInParkTimezone(date, park.timezone);
    const openingTime = new Date(
      `${dateStr}T${String(openingHour).padStart(2, "0")}:00:00`,
    );
    const closingTime = new Date(
      `${dateStr}T${String(closingHour).padStart(2, "0")}:00:00`,
    );

    return {
      openingTime: openingTime.toISOString(),
      closingTime: closingTime.toISOString(),
      type: ScheduleType.OPERATING,
      isInferred: true,
    };
  }

  /**
   * Aggregate per-attraction ML predictions into a date → CrowdLevel map for future days.
   *
   * Mirrors calculateCrowdLevelForDate: median of headliner wait times / park P50 baseline.
   * Falls back to all attractions if no headliners are defined.
   */
  private buildPredictedCrowdLevels(
    predictions: PredictionDto[],
    headlinerIds: Set<string>,
    p50Baseline: number,
    allHeadliners: any[] = [],
  ): Map<string, { crowdLevel: CrowdLevel; peakLoad: CrowdLevel }> {
    const map = new Map<
      string,
      { crowdLevel: CrowdLevel; peakLoad: CrowdLevel }
    >();
    if (predictions.length === 0) return map;

    // Calculate P90 baseline from headliners
    const validHeadliners = allHeadliners.filter(
      (h) => Number(h.p90Wait548d) > 0,
    );
    const p90Baseline =
      validHeadliners.length > 0
        ? validHeadliners.reduce((sum, h) => sum + Number(h.p90Wait548d), 0) /
          validHeadliners.length
        : p50Baseline * 1.5; // Fallback heuristic if p90 missing

    // Use headliners only; fall back to all attractions if none defined
    const filtered =
      headlinerIds.size > 0
        ? predictions.filter((p) => headlinerIds.has(p.attractionId))
        : predictions;

    // Group predicted wait times by date
    const byDate = new Map<string, number[]>();
    for (const p of filtered) {
      const date = p.predictedTime?.split("T")[0];
      if (!date || p.predictedWaitTime == null) continue;
      if (!byDate.has(date)) byDate.set(date, []);
      byDate.get(date)!.push(p.predictedWaitTime);
    }

    for (const [date, waits] of byDate) {
      if (waits.length === 0) continue;

      // Median of headliner predicted waits
      const sorted = [...waits].sort((a, b) => a - b);
      const mid = Math.floor(sorted.length / 2);
      const median =
        sorted.length % 2 === 0
          ? (sorted[mid - 1] + sorted[mid]) / 2
          : sorted[mid];

      // P90 of headliner predicted waits
      const p90Index = Math.ceil(sorted.length * 0.9) - 1;
      const p90Value = sorted[Math.max(0, p90Index)];

      const pct =
        p50Baseline > 0 ? Math.round((median / p50Baseline) * 100) : 100;

      const peakPct =
        p90Baseline > 0 ? Math.round((p90Value / p90Baseline) * 100) : pct;

      map.set(date, {
        crowdLevel: this.analyticsService.determineCrowdLevel(pct),
        peakLoad: this.analyticsService.determineCrowdLevel(peakPct),
      });
    }

    return map;
  }

  /**
   * Map average wait time to crowd level (DEPRECATED - kept for reference only)
   * DO NOT USE - Use AnalyticsService.calculateCrowdLevelForDate() instead
   *
   * This method used absolute thresholds which don't adapt to park-specific baselines.
   * All crowd level calculations MUST use P90-relative percentages.
   */
  // private mapWaitTimeToCrowdLevel(avgWaitTime: number): CrowdLevel {
  //   if (avgWaitTime <= 15) return "very_low";
  //   if (avgWaitTime <= 30) return "low";
  //   if (avgWaitTime <= 45) return "moderate";
  //   if (avgWaitTime <= 60) return "high";
  //   return "very_high";
  // }

  /**
   * Compute visit recommendation combining crowd level with contextual signals.
   *
   * Scoring (higher = worse):
   *   crowd level base: very_low=0, low=1, moderate=2, high=3, very_high=4, extreme=5
   *   +2  public holiday (crowds significantly higher than predicted)
   *   +1  school vacation
   *   +1  bridge day
   *   +1  ≥2 influencing holidays from neighboring regions
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
