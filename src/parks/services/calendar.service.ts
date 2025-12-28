import { Injectable, Logger, Inject } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { ParksService } from "../parks.service";
import { WeatherService } from "../weather.service";
import { MLService } from "../../ml/ml.service";
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
        this.holidaysService
          .getHolidays(park.countryCode, fromDate, toDate)
          .catch((err) => {
            this.logger.warn(
              `Holidays data unavailable for ${park.countryCode}: ${err.message}`,
            );
            return [];
          }),
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

    // Check if holiday
    const isHoliday = await this.holidaysService.isHoliday(
      date,
      park.countryCode,
      park.regionCode,
      park.timezone,
    );

    // Check if bridge day
    const isBridgeDay = await this.holidaysService.isBridgeDay(
      date,
      park.countryCode,
      park.regionCode,
      park.timezone,
    );

    // Determine park status
    const status: ParkStatus =
      schedule?.scheduleType === ScheduleType.OPERATING
        ? "OPERATING"
        : schedule?.scheduleType === ScheduleType.CLOSED
          ? "CLOSED"
          : mlPrediction?.crowdLevel === "closed"
            ? "CLOSED"
            : "OPERATING"; // Default optimistic

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
      // Infer hours from ML (park likely open but no official schedule)
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

    // Build events array
    const events: CalendarEvent[] = [];
    const dayHolidays = holidays.filter(
      (h) => formatInParkTimezone(h.date, park.timezone) === dateStr,
    );
    dayHolidays.forEach((h) => {
      events.push({
        type: "holiday",
        name: h.name,
        isNationwide: h.isNationwide,
      });
    });

    // Build calendar day
    const day: CalendarDay = {
      date: dateStr,
      status,
      isToday: dateStr === today,
      isTomorrow: dateStr === tomorrow,
      hours: hours || undefined,
      crowdLevel: (crowdLevel || "moderate") as CrowdLevel,
      crowdScore:
        status === "CLOSED" ? undefined : mlPrediction?.crowdScore || undefined,
      weather: weatherSummary || undefined,
      events,
      isHoliday,
      isBridgeDay,
      isSchoolVacation: false, // TODO: Implement school vacation detection
    };

    // Add refurbishments if any
    if (refurbishments.length > 0) {
      day.refurbishments = refurbishments;
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
}
