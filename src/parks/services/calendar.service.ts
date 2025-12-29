import { Injectable, Logger, Inject } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { MLService } from "../../ml/ml.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
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
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
  isSameDayInTimezone,
} from "../../common/utils/date.util";
import { getWeatherDescription } from "../../common/constants/wmo-weather-codes.constant";
import { addDays, parseISO } from "date-fns";

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
    private readonly holidaysService: HolidaysService,
    private readonly attractionsService: AttractionsService,
    private readonly showsService: ShowsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
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
    const cacheKey = `calendar:${park.id}:${formatInParkTimezone(fromDate, park.timezone)}:${formatInParkTimezone(toDate, park.timezone)}:${includeHourly}`;

    // Try cache first
    const cached = await this.redis.get(cacheKey);
    if (cached) {
      this.logger.debug(`Cache hit for calendar: ${cacheKey}`);
      return JSON.parse(cached);
    }

    this.logger.debug(
      `Building calendar for ${park.slug} from ${formatInParkTimezone(fromDate, park.timezone)} to ${formatInParkTimezone(toDate, park.timezone)}`,
    );

    // Fetch all data in parallel
    const [schedules, weatherData, mlPredictions, holidays, refurbishments] =
      await Promise.all([
        this.parksService.getSchedule(park.id, fromDate, toDate),
        this.weatherService
          .getWeatherData(park.id, fromDate, toDate)
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

          return Promise.all(
            uniqueCountries.map((cc) =>
              this.holidaysService.getHolidays(cc, fromDate, toDate),
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
      ]);

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

    // Cache the response
    await this.redis.set(
      cacheKey,
      JSON.stringify(response),
      "EX",
      this.CALENDAR_CACHE_TTL,
    );
    this.logger.debug(`Cached calendar response: ${cacheKey}`);

    return response;
  }

  /**
   * Build a single calendar day
   */
  private async buildCalendarDay(
    park: Park,
    date: Date,
    schedules: ScheduleEntry[],
    weatherData: any[],
    mlPredictions: any[],
    holidays: any[],
    refurbishments: string[],
    includeHourly: string,
  ): Promise<CalendarDay> {
    const dateStr = formatInParkTimezone(date, park.timezone);
    const today = getCurrentDateInTimezone(park.timezone);
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
      (p) => p.date?.split("T")[0] === dateStr,
    );

    // Find weather for this day
    const weather = weatherData.find(
      (w) =>
        formatInParkTimezone(
          typeof w.date === "string" ? parseISO(w.date) : w.date,
          park.timezone,
        ) === dateStr,
    );

    // Build events array (deduplicated) and separate local/influencing holidays
    const events: CalendarEvent[] = [];
    const influencingHolidays: any[] = [];
    const seenEvents = new Set<string>();

    const dayHolidays = holidays.filter(
      (h) => formatInParkTimezone(h.date, park.timezone) === dateStr,
    );

    let localHolidayFound = false;
    let localBridgeDayFound = false;
    let localSchoolVacationFound = false;

    for (const h of dayHolidays) {
      const type = h.holidayType === "school" ? "school-holiday" : "holiday";
      const key = `${type}:${h.name}`;

      const isLocal =
        h.country === park.countryCode &&
        (h.isNationwide ||
          !h.region ||
          h.region === park.regionCode ||
          (park.regionCode && h.region.endsWith(`-${park.regionCode}`)));

      if (isLocal) {
        if (!seenEvents.has(key)) {
          seenEvents.add(key);
          events.push({
            name: h.name,
            type: type as any,
            isNationwide: h.isNationwide,
          });
        }

        localHolidayFound = true;
        if (h.holidayType === "school") {
          localSchoolVacationFound = true;
        }
        if (h.metadata?.isBridgeDay) {
          localBridgeDayFound = true;
        }
      } else {
        // Influencing but not local
        influencingHolidays.push({
          name: h.name,
          source: {
            countryCode: h.country,
            regionCode: h.region ? h.region.split("-").pop() : null,
          },
          holidayType: h.holidayType,
        });
      }
    }

    const isHoliday = localHolidayFound;
    const isBridgeDay = localBridgeDayFound;
    const isSchoolVacation = localSchoolVacationFound;

    // Determine park status
    const status: ParkStatus =
      schedule?.scheduleType === ScheduleType.OPERATING
        ? "OPERATING"
        : schedule?.scheduleType === ScheduleType.CLOSED
          ? "CLOSED"
          : mlPrediction?.crowdLevel === "closed"
            ? "CLOSED"
            : "CLOSED";

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

    // Map crowd level
    const crowdLevel: CrowdLevel | "closed" =
      status === "CLOSED"
        ? "closed"
        : this.mapCrowdLevel(mlPrediction?.crowdScore || 50);

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
            (weather.precipitationSum || 0) > 0
              ? Math.min((weather.precipitationSum / 10) * 100, 100)
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
      crowdScore:
        status === "CLOSED" ? undefined : mlPrediction?.crowdScore || undefined,
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
        mlPrediction?.crowdScore || mlPrediction?.crowdLevel,
        isHoliday,
        isBridgeDay,
        weatherSummary,
      );
      day.advisoryKeys = advisoryKeys;
      day.recommendation = this.generateRecommendationString(advisoryKeys);
      day.showTimes = await this.getShowTimes(park.id, date);
    }

    // Add hourly data if requested
    if (this.shouldIncludeHourly(date, includeHourly, park.timezone)) {
      day.hourly = await this.buildHourlyPredictions(
        park,
        date,
        mlPrediction,
        status,
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
   * Build hourly predictions for a day
   */
  private async buildHourlyPredictions(
    park: Park,
    date: Date,
    mlPrediction: any,
    dayStatus: ParkStatus,
  ): Promise<HourlyPrediction[]> {
    if (dayStatus === "CLOSED") {
      return [];
    }

    // Get hourly predictions from ML service (if available)
    try {
      const hourlyPredictions = await this.mlService.getParkPredictions(
        park.id,
        "hourly",
      );

      const dateStr = formatInParkTimezone(date, park.timezone);
      const hourlyData = hourlyPredictions.predictions.filter((p) =>
        p.predictedTime?.startsWith(dateStr),
      );

      return hourlyData.map((p) => {
        const hour = new Date(p.predictedTime).getHours();
        return {
          hour,
          crowdLevel: (p.crowdLevel === "closed"
            ? "very_low"
            : p.crowdLevel) as CrowdLevel,
          predictedWaitTime: Math.round(p.predictedWaitTime || 30),
          probability: p.confidence,
        };
      });
    } catch (_error) {
      this.logger.warn(
        `Hourly predictions unavailable for ${park.slug} on ${formatInParkTimezone(date, park.timezone)}`,
      );
      return [];
    }
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
   */
  private mapCrowdLevel(score: number): CrowdLevel {
    if (score <= 20) return "very_low";
    if (score <= 40) return "low";
    if (score <= 60) return "moderate";
    if (score <= 80) return "high";
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
        very_low: 20,
        low: 35,
        moderate: 50,
        high: 70,
        very_high: 85,
        extreme: 95,
      };
      crowdScore = levelMap[crowdData] || 50;
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
}
