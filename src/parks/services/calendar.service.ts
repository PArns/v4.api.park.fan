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
import { ShowsService } from "../../shows/shows.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import {
  IntegratedCalendarResponse,
  CalendarDay,
  OperatingHours,
  WeatherSummary,
  CalendarEvent,
  HourlyPrediction,
  ShowTime,
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
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  isSameDayInTimezone,
} from "../../common/utils/date.util";
import { normalizeRegionCode } from "../../common/utils/region.util";
import {
  calculateHolidayInfo,
  HolidayEntry,
} from "../../common/utils/holiday.utils";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";
import { addDays, parseISO } from "date-fns";

import { ParkDailyStats } from "../../stats/entities/park-daily-stats.entity";
import { StatsService } from "../../stats/stats.service";

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
  private readonly CALENDAR_CACHE_TTL = 60 * 60; // 1 hour

  constructor(
    private readonly parksService: ParksService,
    private readonly weatherService: WeatherService,
    private readonly mlService: MLService,
    private readonly analyticsService: AnalyticsService,
    private readonly holidaysService: HolidaysService,
    private readonly attractionsService: AttractionsService,
    private readonly showsService: ShowsService,
    private readonly queueDataService: QueueDataService,
    private readonly statsService: StatsService,
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
          parkId: park.id,
          slug: park.slug,
          timezone: park.timezone,
          generatedAt: new Date().toISOString(),
          requestRange: { from: fromStr, to: toStr },
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

    // Fetch all data in parallel
    const [
      schedules,
      weatherData,
      mlPredictions,
      holidays,
      refurbishments,
      historicalQueueData,
      dailyStats,
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
      this.getRefurbishmentsList(park.id).catch((err) => {
        this.logger.warn(
          `Refurbishments data unavailable for ${park.slug}: ${err.message}`,
        );
        return [];
      }),
      (async () => {
        // Optimized: Only fetch queue data if range includes today or past
        // And use optimized date-range query instead of full park dump
        const rangeStartStr = formatInParkTimezone(fromDate, park.timezone);
        if (rangeStartStr > today) {
          return []; // Future only request - no queue data needed
        }

        try {
          // Add 1 day buffer to end date to ensure we cover full day in UTC
          const queryEnd = new Date(toDate);
          queryEnd.setDate(queryEnd.getDate() + 1);

          return await this.queueDataService.findHistoricalDataForDateRange(
            park.id,
            fromDate,
            queryEnd,
          );
        } catch (err: any) {
          this.logger.debug(
            `Historical queue data unavailable for ${park.slug}: ${err.message}`,
          );
          return [];
        }
      })(),
      this.statsService
        .getDailyStats(
          park.id,
          formatInParkTimezone(fromDate, park.timezone),
          formatInParkTimezone(toDate, park.timezone),
        )
        .catch((err) => {
          this.logger.warn(
            `Stats unavailable for ${park.slug}: ${err.message}`,
          );
          return [];
        }),
    ]);

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

    // Fetch hourly ML predictions once (used for today+tomorrow when includeHourly) to avoid N+1
    const hourlyPredictionsList: PredictionDto[] =
      includeHourly !== "none"
        ? (
            await this.mlService
              .getParkPredictions(park.id, "hourly")
              .catch(() => ({ predictions: [] }))
          ).predictions
        : [];

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
    const prefetchedCrowdLevels = new Map<string, CrowdLevel | "closed">();
    crowdLevelRaw.forEach((raw, i) => {
      if (!raw || i >= historicalDateStrs.length) return;
      try {
        const parsed = JSON.parse(raw) as { crowdLevel: CrowdLevel };
        prefetchedCrowdLevels.set(historicalDateStrs[i], parsed.crowdLevel);
      } catch {
        // ignore invalid cache
      }
    });

    // Build calendar days
    const days: CalendarDay[] = [];
    const currentDate = new Date(fromDate);

    while (currentDate <= toDate) {
      const dayData = await this.buildCalendarDay(
        park,
        currentDate,
        schedules,
        weatherData,
        mlPredictions.predictions,
        holidays,
        refurbishments,
        includeHourly,
        historicalQueueData,
        dailyStats as ParkDailyStats[], // Pass stats
        today,
        hourlyPredictionsList,
        prefetchedCrowdLevels,
      );
      days.push(dayData);
      currentDate.setDate(currentDate.getDate() + 1);
    }

    // Build response
    const response: IntegratedCalendarResponse = {
      meta: {
        parkId: park.id,
        slug: park.slug,
        timezone: park.timezone,
        generatedAt: new Date().toISOString(),
        requestRange: {
          from: formatInParkTimezone(fromDate, park.timezone),
          to: formatInParkTimezone(toDate, park.timezone),
        },
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
      await this.redis.set(key, JSON.stringify(monthDays), "EX", ttl);
      this.logger.debug(`Cached calendar month: ${key} (TTL: ${ttl}s)`);
    }
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

    // Include all types (OPERATING, CLOSED, UNKNOWN): UNKNOWN placeholders from fillScheduleGaps
    // mean we already have "something" for that date, so don't trigger refresh for that range.
    const maxScheduleDate =
      schedules.length > 0
        ? formatInParkTimezone(
            new Date(Math.max(...schedules.map((s) => s.date.getTime()))),
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
    refurbishments: string[],
    includeHourly: string,
    historicalQueueData: QueueData[],
    parkStats: ParkDailyStats[],
    today: string,
    hourlyPredictionsPreFetched: PredictionDto[] = [],
    prefetchedCrowdLevels: Map<string, CrowdLevel | "closed"> = new Map(),
  ): Promise<CalendarDay> {
    const dateStr = formatInParkTimezone(date, park.timezone);
    // today is passed as argument
    const tomorrow = formatInParkTimezone(
      addDays(parseISO(today), 1),
      park.timezone,
    );

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
          typeof w.date === "string" ? parseISO(w.date) : w.date,
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
    const status: ParkStatus =
      schedule?.scheduleType === ScheduleType.OPERATING
        ? "OPERATING"
        : schedule?.scheduleType === ScheduleType.CLOSED
          ? "CLOSED"
          : schedule?.scheduleType === ScheduleType.UNKNOWN
            ? "UNKNOWN"
            : "UNKNOWN";

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

    // Map crowd level based on date
    const isHistorical = dateStr <= today;
    let crowdLevel: CrowdLevel | "closed";

    if (status !== "OPERATING") {
      crowdLevel = "closed";
    } else if (isHistorical) {
      const prefetched = prefetchedCrowdLevels.get(dateStr);
      if (prefetched !== undefined) {
        crowdLevel = prefetched;
      } else {
        const cachedStat = this.findCachedStat(dateStr, parkStats);
        if (cachedStat && cachedStat.p90WaitTime !== null) {
          const crowdData =
            await this.analyticsService.calculateCrowdLevelForDate(
              park.id,
              "park",
              dateStr,
              park.timezone,
            );
          crowdLevel = crowdData.crowdLevel;
        } else {
          const dayQueueData = historicalQueueData.filter(
            (q) =>
              formatInParkTimezone(q.timestamp, park.timezone) === dateStr &&
              q.waitTime !== null &&
              q.waitTime > 0,
          );
          if (dayQueueData.length > 0) {
            const crowdData =
              await this.analyticsService.calculateCrowdLevelForDate(
                park.id,
                "park",
                dateStr,
                park.timezone,
              );
            crowdLevel = crowdData.crowdLevel;
          } else {
            crowdLevel = mlPrediction?.crowdLevel || "moderate";
          }
        }
      }
    } else {
      // Future date: use ML prediction
      crowdLevel = mlPrediction?.crowdLevel || "moderate";
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

    // Build calendar day
    const day: CalendarDay = {
      date: dateStr,
      status,
      isToday: dateStr === today,
      isTomorrow: dateStr === tomorrow,
      hours: hours || undefined,
      crowdLevel,
      crowdScore: undefined, // Deprecated: use crowdLevel instead
      weather: weatherSummary || undefined,
      events,
      isHoliday,
      isBridgeDay,
      isSchoolVacation,
      influencingHolidays:
        influencingHolidays.length > 0 ? influencingHolidays : undefined,
    };

    // Add refurbishments if any
    if (refurbishments.length > 0) {
      day.refurbishments = refurbishments;
    }

    // Add ML-generated recommendation
    if (status === "OPERATING") {
      const advisoryKeys = this.generateAdvisoryKeys(
        mlPrediction?.crowdLevel,
        isHoliday,
        isBridgeDay,
        weatherSummary,
      );
      day.advisoryKeys = advisoryKeys;
      day.recommendation = this.generateRecommendationString(advisoryKeys);
      // Note: Show times are available via dedicated /parks/:id/shows endpoint
    }

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
    const today = parseISO(getCurrentDateInTimezone(timezone));
    const tomorrow = addDays(today, 1);

    switch (includeHourly) {
      case "today+tomorrow":
        return (
          isSameDayInTimezone(date, today, timezone) ||
          isSameDayInTimezone(date, tomorrow, timezone)
        );
      case "today":
        return isSameDayInTimezone(date, today, timezone);
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
   * Map ML crowd score to CrowdLevel enum
   * Note: This is a legacy method for score-based systems
   * New code should use determineCrowdLevel with P90-relative percentages
   */
  private mapCrowdLevel(score: number): CrowdLevel {
    if (score <= 40) return "very_low";
    if (score <= 70) return "low";
    if (score <= 85) return "moderate";
    if (score <= 95) return "high";
    return "very_high";
  }

  /**
   * Get list of attractions under refurbishment
   * Note: Returns empty array for now - attraction status is derived from live queue data
   */
  private async getRefurbishmentsList(_parkId: string): Promise<string[]> {
    // TODO: Implement refurbishment detection from queue data status
    // Attractions don't have a persistent status field - it's determined by live data
    return [];
  }

  /**
   * Generate advisory keys for localization
   */
  private generateAdvisoryKeys(
    crowdData?: number | string,
    isHoliday?: boolean,
    isBridgeDay?: boolean,
    weather?: WeatherSummary | null,
  ): string[] {
    const keys: string[] = [];

    // Convert crowd level to score if needed
    let crowdScore: number | undefined;
    if (typeof crowdData === "number") {
      crowdScore = crowdData;
    } else if (typeof crowdData === "string") {
      const levelMap: Record<string, number> = {
        very_low: 30,
        low: 55,
        moderate: 80,
        high: 110,
        very_high: 140,
        extreme: 170,
      };
      crowdScore = levelMap[crowdData] || 80;
    }

    if (crowdScore !== undefined) {
      if (crowdScore < 30) {
        keys.push("lowCrowds");
      } else if (crowdScore > 75) {
        keys.push("highCrowds");
        if (isHoliday || isBridgeDay) {
          keys.push("visitWeekday");
        }
      } else if (crowdScore > 50 && crowdScore <= 75) {
        keys.push("moderateCrowds");
      } else if (crowdScore >= 30 && crowdScore <= 50) {
        keys.push("goodCrowds");
      }
    }

    // Weather-based recommendations
    if (weather) {
      if (weather.rainChance > 60) {
        keys.push("rainLikely");
      } else if (weather.tempMax > 30) {
        keys.push("hotDay");
      } else if (weather.tempMax < 5) {
        keys.push("coldDay");
      }
    }

    if (keys.length === 0) {
      keys.push("goodDay");
    }

    return keys;
  }

  /**
   * Generate legacy recommendation string from keys
   */
  private generateRecommendationString(keys: string[]): string {
    const map: Record<string, string> = {
      lowCrowds: "Low crowds expected - excellent day to visit",
      highCrowds: "High crowds expected - arrive early",
      visitWeekday: "Consider visiting on a weekday instead",
      moderateCrowds: "Moderate crowds expected",
      goodCrowds: "Good crowd levels for most attractions",
      rainLikely: "Rain likely - indoor attractions recommended",
      hotDay: "Hot day -stay hydrated",
      coldDay: "Cold weather - dress warmly",
      goodDay: "Good day for a park visit",
    };

    return keys.map((k) => map[k] || k).join(". ");
  }

  /**
   * Get show times for a specific day
   */
  /**
   * Get show times for a specific target date
   * Projects current/stale data to the target date if the show is Operating
   */
  private async getShowTimes(
    parkId: string,
    targetDate: Date,
  ): Promise<ShowTime[]> {
    try {
      // Get current show status for this park
      const showStatusMap =
        await this.showsService.findCurrentStatusByPark(parkId);

      const showTimes: ShowTime[] = [];

      // Extract showtimes from each show's live data
      const now = new Date();
      const STALE_THRESHOLD_MS = 24 * 60 * 60 * 1000; // 24 hours

      // We need to project times to the target date
      // targetDate might be Today, Tomorrow, etc.
      // We assume the daily schedule is consistent if status is OPERATING
      // (Static Schedule Assumption)

      // Fetch park to get timezone (needed for accurate date string construction)
      // Optimization: Pass timezone in or fetch once preferably
      // For now we get it from the show entity if relations are loaded

      for (const [_showId, liveData] of showStatusMap.entries()) {
        if (!liveData.showtimes || !Array.isArray(liveData.showtimes)) {
          continue;
        }

        const isOperating = liveData.status === "OPERATING";

        // Check for stale data
        // If Operating, we are more lenient (allow stale data projection)
        if (liveData.lastUpdated && !isOperating) {
          const lastUpdated = new Date(liveData.lastUpdated);
          const age = now.getTime() - lastUpdated.getTime();
          if (age > STALE_THRESHOLD_MS) {
            this.logger.debug(
              `Skipping stale show data for ${liveData.show?.name} (Age: ${Math.round(age / 1000 / 60 / 60)}h) - Not Operating`,
            );
            continue;
          }
        }

        // Find timezone from show relation (loaded in findCurrentStatusByPark)
        const timezone = liveData.show?.park?.timezone || "UTC";
        const targetDateStr = formatInParkTimezone(targetDate, timezone);

        for (const showtime of liveData.showtimes) {
          if (showtime.startTime) {
            // Project to Target Date
            // Extract Time Part from original ISO: T11:00:00+01:00
            const iso = showtime.startTime;
            const timePart = iso.substring(10); // Start at 'T' (index 10)

            // Construct new ISO for Target Date
            const newIso = targetDateStr + timePart;

            // For EndTime
            let newEndIso: string | undefined;
            if (showtime.endTime) {
              const endIso = showtime.endTime;
              const endTimePart = endIso.substring(10);
              newEndIso = targetDateStr + endTimePart;
            }

            // Determine best name to show
            let showName = liveData.show?.name || "Show";
            if (
              showName === "Show" &&
              showtime.type &&
              showtime.type !== "Performance"
            ) {
              showName = showtime.type;
            }

            // Only add if we confirmed operating or data is fresh
            // If stale but operating -> Project
            // If fresh -> Project (to target date, because data might be for "Today")
            showTimes.push({
              name: showName,
              time: newIso,
              endTime: newEndIso,
            });
          }
        }
      }

      // Sort by time
      showTimes.sort((a, b) => a.time.localeCompare(b.time));

      return showTimes;
    } catch (error) {
      this.logger.warn(
        `Failed to fetch show times for park ${parkId}: ${error}`,
      );
      return [];
    }
  }

  /**
   * Calculate average wait time from queue data
   */
  private calculateP90WaitTime(queueData: QueueData[]): number {
    const waitTimes = queueData
      .filter(
        (q) =>
          q.waitTime !== null && q.waitTime > 0 && q.queueType === "STANDBY",
      ) // Ensure STANDBY and valid
      .map((q) => q.waitTime!)
      .sort((a, b) => a - b);

    if (waitTimes.length === 0) return 0;

    const percentileIndex = Math.ceil(waitTimes.length * 0.9) - 1;
    return waitTimes[percentileIndex];
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
   * Helper to find cached stat for a specific date
   */
  private findCachedStat(
    dateStr: string,
    stats: ParkDailyStats[],
  ): ParkDailyStats | undefined {
    return stats.find((s) => s.date === dateStr);
  }
}
