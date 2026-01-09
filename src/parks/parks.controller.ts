import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
  Inject,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiExtraModels,
  getSchemaPath,
  ApiParam,
  ApiQuery,
} from "@nestjs/swagger";
import { ParksService } from "./parks.service";
import { WeatherService } from "./weather.service";
import { ParkIntegrationService } from "./services/park-integration.service";
import { ParkEnrichmentService } from "./services/park-enrichment.service";
import { CalendarService } from "./services/calendar.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AttractionIntegrationService } from "../attractions/services/attraction-integration.service";
import { ShowsService } from "../shows/shows.service";
import { RestaurantsService } from "../restaurants/restaurants.service";
import { QueueDataService } from "../queue-data/queue-data.service";
import { AnalyticsService } from "../analytics/analytics.service";
import { MLService } from "../ml/ml.service";
import { PredictionAccuracyService } from "../ml/services/prediction-accuracy.service";
import { QueueType } from "../external-apis/themeparks/themeparks.types";
import { ParkResponseDto } from "./dto/park-response.dto";
import { ParkWithAttractionsDto } from "./dto/park-with-attractions.dto";
import { ParkQueryDto } from "./dto/park-query.dto";
import { ParkDailyPredictionDto } from "./dto/park-daily-prediction.dto";
import { WeatherResponseDto } from "./dto/weather-response.dto";
import { WeatherItemDto } from "./dto/weather-item.dto";
import { ScheduleResponseDto } from "./dto/schedule-response.dto";
import { ScheduleItemDto } from "./dto/schedule-item.dto";
import { IntegratedCalendarResponse } from "./dto/integrated-calendar.dto";
import { AttractionResponseDto } from "../attractions/dto/attraction-response.dto";
import { PaginatedResponseDto } from "../common/dto/pagination.dto";
import { MissingGeocodeResponseDto } from "./dto/missing-geocode-response.dto";
import { ParkWaitTimesResponseDto } from "../queue-data/dto/park-wait-times-response.dto";
import { Park } from "./entities/park.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

/**
 * Parks Controller
 *
 * Provides REST API endpoints for accessing park data.
 *
 * Endpoints:
 * - GET /parks - List all parks
 * - GET /parks/:slug - Get specific park with attractions
 */
@ApiTags("parks")
@Controller("parks")
export class ParksController {
  private readonly TTL_INTEGRATED_RESPONSE = 5 * 60; // 5 minutes for real-time data

  constructor(
    private readonly parksService: ParksService,
    private readonly weatherService: WeatherService,
    private readonly attractionsService: AttractionsService,
    private readonly attractionIntegrationService: AttractionIntegrationService,
    private readonly showsService: ShowsService,
    private readonly restaurantsService: RestaurantsService,
    private readonly queueDataService: QueueDataService,
    private readonly analyticsService: AnalyticsService,
    private readonly mlService: MLService,
    private readonly predictionAccuracyService: PredictionAccuracyService,
    private readonly parkIntegrationService: ParkIntegrationService,
    private readonly parkEnrichmentService: ParkEnrichmentService,
    private readonly calendarService: CalendarService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * GET /v1/parks
   *
   * Returns all parks with optional filtering and sorting.
   * Now supports pagination with default limit of 10 items.
   */
  @Get()
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live park status
  @ApiOperation({
    summary: "List all parks",
    description:
      "Returns a paginated list of all parks with status and analytics.",
  })
  @ApiExtraModels(PaginatedResponseDto, ParkResponseDto)
  @ApiResponse({
    status: 200,
    description: "List of parks",
    schema: {
      allOf: [
        { $ref: getSchemaPath(PaginatedResponseDto) },
        {
          properties: {
            data: {
              type: "array",
              items: { $ref: getSchemaPath(ParkResponseDto) },
            },
          },
        },
      ],
    },
  })
  async findAll(@Query() query: ParkQueryDto): Promise<{
    data: ParkResponseDto[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrevious: boolean;
    };
  }> {
    const { data: parks, total } =
      await this.parksService.findAllWithFilters(query);
    const mappedParks =
      await this.parkEnrichmentService.enrichParksWithLiveData(parks);

    return {
      data: mappedParks,
      pagination: {
        page: query.page || 1,
        limit: query.limit || 10,
        total,
        totalPages: Math.ceil(total / (query.limit || 10)),
        hasNext: (query.page || 1) < Math.ceil(total / (query.limit || 10)),
        hasPrevious: (query.page || 1) > 1,
      },
    };
  }

  /**
   * GET /v1/parks/debug/missing-geocode
   *
   * Debug endpoint to list all parks with incomplete geographic data.
   * Shows parks that have latitude/longitude but missing continent, country, or city.
   *
   * @returns Parks with incomplete geocoding data
   */
  @Get("debug/missing-geocode")
  @ApiOperation({
    summary: "List parks with missing geocode",
    description: "Debug endpoint for identifying data issues.",
  })
  @ApiResponse({
    status: 200,
    description: "List of parks with incomplete geocoding data",
    type: [MissingGeocodeResponseDto],
  })
  async findParksWithMissingGeocode(): Promise<MissingGeocodeResponseDto[]> {
    const parks = await this.parksService.findAll();

    const incomplete = parks.filter(
      (park) =>
        park.latitude &&
        park.longitude &&
        (!park.continent || !park.country || !park.city),
    );

    return incomplete.map((park) => ({
      id: park.id,
      name: park.name,
      slug: park.slug,
      latitude: park.latitude,
      longitude: park.longitude,
      continent: park.continent,
      continentSlug: park.continentSlug,
      country: park.country,
      countrySlug: park.countrySlug,
      city: park.city,
      citySlug: park.citySlug,
      geocodingAttemptedAt: park.geocodingAttemptedAt,
      missing: {
        continent: !park.continent,
        country: !park.country,
        city: !park.city,
      },
    }));
  }

  /**
   * GET /v1/parks/:continent
   *
   * Returns all parks in a specific continent.
   *
   * @param continent - Continent slug (e.g., "europe", "north-america")
   * @throws NotFoundException if continent not found
   */
  @Get(":continent")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live park status
  @ApiOperation({
    summary: "Get parks by continent",
    description:
      "Returns a list of all parks in a specific continent with status and analytics.",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiResponse({
    status: 200,
    description: "List of parks in continent",
    type: [ParkResponseDto],
  })
  @ApiResponse({ status: 404, description: "Continent not found" })
  async findByContinent(
    @Param("continent") continent: string,
  ): Promise<ParkResponseDto[]> {
    const parks = await this.parksService.findByContinent(continent);

    if (parks.length === 0) {
      throw new NotFoundException(
        `Continent with slug "${continent}" not found`,
      );
    }

    // Return list of parks with live data
    return this.mapToResponseWithStatus(parks);
  }

  /**
   * GET /v1/parks/:continent/:country
   *
   * Returns all parks in a specific country.
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @throws NotFoundException if no parks found
   */
  @Get(":continent/:country")
  @ApiOperation({
    summary: "List parks in country",
    description: "Returns all parks operating in a specific country.",
  })
  @ApiResponse({
    status: 200,
    description: "List of parks",
    type: ParkResponseDto,
    isArray: true,
  })
  @ApiResponse({ status: 404, description: "No parks found" })
  async getParksByCountry(
    @Param("continent") continent: string,
    @Param("country") country: string,
  ): Promise<ParkResponseDto[]> {
    const parks = await this.parksService.findByCountry(continent, country);

    if (parks.length === 0) {
      throw new NotFoundException(`No parks found in ${country}, ${continent}`);
    }

    return this.mapToResponseWithStatus(parks);
  }

  /**
   * GET /v1/parks/:continent/:country/:city
   *
   * Returns all parks in a specific city.
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @param city - City slug
   * @throws NotFoundException if no parks found
   */
  @Get(":continent/:country/:city")
  @ApiOperation({
    summary: "List parks in city",
    description: "Returns all parks operating in a specific city.",
  })
  @ApiResponse({
    status: 200,
    description: "List of parks",
    type: ParkResponseDto,
    isArray: true,
  })
  @ApiResponse({ status: 404, description: "No parks found" })
  async getParksByCity(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
  ): Promise<ParkResponseDto[]> {
    const parks = await this.parksService.findByCity(continent, country, city);

    if (parks.length === 0) {
      throw new NotFoundException(
        `No parks found in ${city}, ${country}, ${continent}`,
      );
    }

    return this.mapToResponseWithStatus(parks);
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/calendar
   *
   * Returns integrated calendar via geographic path.
   *
   * **Timezone Handling:**
   * - All dates in the response are in the park's local timezone
   * - The `meta.timezone` field specifies the IANA timezone (e.g., "Europe/Berlin")
   * - Query parameters `from` and `to` are interpreted in the park's timezone
   * - `isToday` and `isTomorrow` are calculated based on park's local time
   *
   * **Hourly Predictions:**
   * - By default, hourly predictions are included for today and tomorrow
   * - Use `includeHourly` parameter to customize this behavior
   * - Hourly data includes crowd levels and predicted wait times per hour
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @param city - City slug
   * @param parkSlug - Park slug
   * @param from - Start date (YYYY-MM-DD, optional, default: today in park timezone)
   * @param to - End date (YYYY-MM-DD, optional, default: today + 30 days in park timezone)
   * @param includeHourly - Which days should include hourly predictions (default: "today+tomorrow")
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid or range > 90 days
   */
  @Get(":continent/:country/:city/:parkSlug/calendar")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache
  @ApiOperation({
    summary: "Get integrated calendar (geographic path)",
    description:
      "Returns unified calendar data combining schedule, weather forecasts, ML predictions, holidays, and events. " +
      "All dates are in the park's local timezone. Includes hourly predictions for today/tomorrow by default. " +
      "Cache TTL: 1 hour (HTTP + Redis) for fresh hourly predictions.",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiQuery({
    name: "from",
    required: false,
    description:
      "Start date (YYYY-MM-DD) in park's local timezone. Defaults to today.",
    example: "2025-12-28",
  })
  @ApiQuery({
    name: "to",
    required: false,
    description:
      "End date (YYYY-MM-DD) in park's local timezone. Defaults to from + 30 days. Max range: 90 days.",
    example: "2025-12-30",
  })
  @ApiQuery({
    name: "includeHourly",
    required: false,
    enum: ["today+tomorrow", "today", "all", "none"],
    description:
      "Controls which days include hourly predictions. " +
      "'today+tomorrow' (default): hourly data for today and tomorrow. " +
      "'today': only today. " +
      "'all': all days in range. " +
      "'none': no hourly data.",
    example: "today+tomorrow",
  })
  @ApiExtraModels(IntegratedCalendarResponse)
  @ApiResponse({
    status: 200,
    description: "Integrated calendar data",
    type: IntegratedCalendarResponse,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  @ApiResponse({ status: 400, description: "Invalid date range or format" })
  async getCalendarByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
    @Query("includeHourly")
    includeHourly?: "today+tomorrow" | "today" | "none" | "all",
  ): Promise<IntegratedCalendarResponse> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    // Parse date range with timezone awareness
    const { parseDateRange, validateDateRange } =
      await import("../common/utils/date-parsing.util");
    const { fromDate, toDate } = parseDateRange(from, to, {
      timezone: park.timezone,
      defaultFromDaysAgo: 0,
      defaultToDaysAhead: 30,
    });

    // Validate range (max 90 days)
    validateDateRange(fromDate, toDate, 90);

    return this.calendarService.buildCalendarResponse(
      park,
      fromDate,
      toDate,
      includeHourly || "today+tomorrow",
    );
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/weather/forecast
   *
   * Returns 16-day weather forecast for a park via geographic path.
   *
   * @param continent - Continent slug (e.g., "europe", "north-america")
   * @param country - Country slug (e.g., "germany", "united-states")
   * @param city - City slug (e.g., "rust", "orlando")
   * @param parkSlug - Park slug (e.g., "europa-park", "magic-kingdom")
   * @throws NotFoundException if park not found
   */
  @Get(":continent/:country/:city/:parkSlug/weather/forecast")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache
  @ApiOperation({
    summary: "Get weather forecast (geo)",
    description:
      "Returns 16-day weather forecast for a park via geographic path. " +
      "Cache TTL: 1 hour (HTTP + Redis).",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiResponse({
    status: 200,
    description: "Weather forecast",
    type: WeatherResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getWeatherForecastByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
  ): Promise<WeatherResponseDto> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    // Use helper method for consistent response
    return this.buildWeatherForecastResponse(park);
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/weather
   *
   * Returns weather data for a park within a date range via geographic path.
   * Includes historical, current, and forecast data.
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @param city - City slug
   * @param parkSlug - Park slug
   * @param from - Start date (YYYY-MM-DD, optional, defaults to 30 days ago)
   * @param to - End date (YYYY-MM-DD, optional, defaults to today)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid
   */
  @Get(":continent/:country/:city/:parkSlug/weather")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache
  @ApiOperation({
    summary: "Get weather history/forecast (geo)",
    description:
      "Returns weather data within a date range via geographic path. " +
      "Includes historical, current, and forecast data. Cache TTL: 1 hour (HTTP + Redis).",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiQuery({
    name: "from",
    required: false,
    description:
      "Start date (YYYY-MM-DD) in park's local timezone. Defaults to 30 days ago.",
    example: "2025-12-01",
  })
  @ApiQuery({
    name: "to",
    required: false,
    description:
      "End date (YYYY-MM-DD) in park's local timezone. Defaults to today.",
    example: "2025-12-31",
  })
  @ApiResponse({
    status: 200,
    description: "Weather data",
    type: WeatherResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  @ApiResponse({ status: 400, description: "Invalid date format" })
  async getWeatherByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
  ): Promise<WeatherResponseDto> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    // Parse date range with timezone awareness
    const { parseDateRange } =
      await import("../common/utils/date-parsing.util");
    const { fromDate, toDate } = parseDateRange(from, to, {
      timezone: park.timezone,
      defaultFromDaysAgo: 30,
      defaultToDaysAhead: 0, // Default 'to' is today
    });

    const weatherData = await this.weatherService.getWeatherData(
      park.id,
      fromDate,
      toDate,
    );

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
      },
      weather: weatherData.map((w) => WeatherItemDto.fromEntity(w)),
    };
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/schedule
   *
   * Returns park operating schedule with optional date range filtering via geographic path.
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @param city - City slug
   * @param parkSlug - Park slug
   * @param from - Start date (YYYY-MM-DD, optional)
   * @param to - End date (YYYY-MM-DD, optional)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid
   */
  @Get(":continent/:country/:city/:parkSlug/schedule")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache
  @ApiOperation({
    summary: "Get schedule range (geo)",
    description:
      "Returns schedule data within a date range via geographic path (default: 30 days). " +
      "Cache TTL: 1 hour (HTTP + Redis).",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiQuery({
    name: "from",
    required: false,
    description:
      "Start date (YYYY-MM-DD) in park's local timezone. Defaults to today.",
    example: "2025-12-28",
  })
  @ApiQuery({
    name: "to",
    required: false,
    description:
      "End date (YYYY-MM-DD) in park's local timezone. Defaults to from + 30 days.",
    example: "2025-12-30",
  })
  @ApiResponse({
    status: 200,
    description: "Schedule data",
    type: ScheduleResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  @ApiResponse({ status: 400, description: "Invalid date format" })
  async getScheduleByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
  ): Promise<ScheduleResponseDto> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    // Parse date range with timezone awareness
    const { parseDateRange } =
      await import("../common/utils/date-parsing.util");
    const { fromDate, toDate } = parseDateRange(from, to, {
      timezone: park.timezone,
      defaultFromDaysAgo: 0, // Default from: today
      defaultToDaysAhead: 30, // Default to: 30 days ahead
    });

    const schedules = await this.parksService.getSchedule(
      park.id,
      fromDate,
      toDate,
    );

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
      },
      schedule: schedules.map((s) => ScheduleItemDto.fromEntity(s)),
    };
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/wait-times
   *
   * Returns current wait times for a park via geographic path.
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @param city - City slug
   * @param parkSlug - Park slug
   * @param queueType - Optional queue type filter
   * @throws NotFoundException if park not found
   */
  @Get(":continent/:country/:city/:parkSlug/wait-times")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live wait times
  @ApiOperation({
    summary: "Get wait times (geo)",
    description:
      "Returns current wait times for all attractions in a park via geographic path. " +
      "Cached for 2 minutes.",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiQuery({
    name: "queueType",
    required: false,
    description: "Optional queue type filter (e.g., 'STANDBY', 'RETURN_TIME')",
    enum: ["STANDBY", "RETURN_TIME", "SINGLE_RIDER", "PAID_FASTPASS"],
  })
  @ApiResponse({
    status: 200,
    description: "Wait times data",
    type: ParkWaitTimesResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getParkWaitTimesByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("queueType") queueType?: QueueType,
  ): Promise<ParkWaitTimesResponseDto> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    return this.parkIntegrationService.getParkWaitTimesResponse(
      park,
      queueType,
    );
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/predictions/yearly
   *
   * Returns yearly crowd predictions via geographic path.
   */
  @Get(":continent/:country/:city/:parkSlug/predictions/yearly")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60)) // 24 hours HTTP cache
  @ApiOperation({
    summary: "Get yearly crowd predictions (geo)",
    description:
      "Returns daily crowd predictions for the entire year (365 days) via geographic path. " +
      "Useful for long-term trip planning and identifying best times to visit.",
  })
  @ApiExtraModels(ParkDailyPredictionDto)
  @ApiResponse({
    status: 200,
    description: "Yearly predictions retrieved",
    schema: {
      type: "object",
      properties: {
        park: {
          type: "object",
          properties: {
            id: { type: "string", format: "uuid" },
            name: { type: "string", example: "Disneyland Park" },
            slug: { type: "string", example: "disneyland-park" },
          },
        },
        predictions: {
          type: "array",
          items: { $ref: getSchemaPath("ParkDailyPredictionDto") },
          description:
            "Daily predictions for up to 365 days (or fewer if off-season days filtered)",
        },
        generatedAt: {
          type: "string",
          format: "date-time",
          example: "2024-01-15T10:00:00.000Z",
          description: "Timestamp when predictions were generated",
        },
      },
    },
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getYearlyPredictionsByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
  ) {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    return this.buildYearlyPredictionsResponse(park);
  }

  /**
   * Helper: Build yearly predictions response
   * Extracts duplicated logic from both yearly predictions endpoints
   */
  private async buildYearlyPredictionsResponse(park: Park) {
    const predictions = await this.mlService.getParkPredictionsYearly(park.id);
    const dailyPredictions =
      await this.parkIntegrationService.aggregateDailyPredictions(
        predictions.predictions,
      );

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
      },
      predictions: dailyPredictions,
      generatedAt: new Date().toISOString(),
    };
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/attractions
   *
   * Returns all attractions in a specific park via geographic path.
   *
   * @param continent - Continent slug (e.g., "europe", "north-america")
   * @param country - Country slug (e.g., "germany", "united-states")
   * @param city - City slug (e.g., "rust", "orlando")
   * @param parkSlug - Park slug (e.g., "europa-park", "magic-kingdom")
   * @param page - Page number (optional, default: 1)
   * @param limit - Items per page (optional, default: 10)
   * @throws NotFoundException if park not found
   */
  @Get(":continent/:country/:city/:parkSlug/attractions")
  @ApiOperation({
    summary: "List park attractions (geo)",
    description:
      "Returns a paginated list of all attractions for a specific park via geographic path.",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiQuery({
    name: "page",
    required: false,
    description: "Page number (default: 1)",
    example: 1,
  })
  @ApiQuery({
    name: "limit",
    required: false,
    description: "Items per page (default: 10)",
    example: 10,
  })
  @ApiExtraModels(PaginatedResponseDto, AttractionResponseDto)
  @ApiResponse({
    status: 200,
    description: "List of attractions",
    schema: {
      allOf: [
        { $ref: getSchemaPath(PaginatedResponseDto) },
        {
          properties: {
            data: {
              type: "array",
              items: { $ref: getSchemaPath(AttractionResponseDto) },
            },
          },
        },
      ],
    },
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getAttractionsInParkByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("page") page: number = 1,
    @Query("limit") limit: number = 10,
  ): Promise<{
    data: AttractionResponseDto[];
    pagination: {
      page: number;
      limit: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrevious: boolean;
    };
  }> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    // Use findAllWithFilters with geo parameters to ensure geo filtering is applied
    // This ensures that any additional query parameters respect the geo context
    const { data: attractions, total } =
      await this.attractionsService.findAllWithFilters({
        park: parkSlug,
        continentSlug: continent,
        countrySlug: country,
        citySlug: city,
        page,
        limit,
      });

    const mappedAttractions = attractions.map((attraction) =>
      AttractionResponseDto.fromEntity(attraction),
    );

    return {
      data: mappedAttractions,
      pagination: {
        page: Number(page),
        limit: Number(limit),
        total,
        totalPages: Math.ceil(total / limit),
        hasNext: page < Math.ceil(total / limit),
        hasPrevious: page > 1,
      },
    };
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/rides/:rideSlug
   *
   * Returns a specific ride (attraction) within a park via full geographic path.
   * This is an alias for the attractions route, using the common term "rides".
   *
   * @param continent - Continent slug (e.g., "europe", "north-america")
   * @param country - Country slug (e.g., "germany", "united-states")
   * @param city - City slug (e.g., "rust", "orlando")
   * @param parkSlug - Park slug (e.g., "europa-park", "magic-kingdom")
   * @param rideSlug - Ride (attraction) slug (e.g., "taron", "space-mountain")
   * @throws NotFoundException if park or ride not found
   */
  @Get(":continent/:country/:city/:parkSlug/rides/:rideSlug")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live wait times
  @ApiOperation({
    summary: "Get ride details (geo)",
    description:
      "Returns details for a specific ride (attraction) in a park via full geographic path. " +
      "This is an alias for the attractions route using the common term 'rides'. " +
      "This is the primary route with full integration support. Cached for 2 minutes.",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiParam({
    name: "rideSlug",
    description: "Ride (attraction) slug (e.g., 'taron', 'space-mountain')",
    example: "taron",
  })
  @ApiResponse({
    status: 200,
    description: "Ride details with full integration",
    type: AttractionResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park or ride not found" })
  async getRideByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Param("rideSlug") rideSlug: string,
  ): Promise<AttractionResponseDto> {
    // Delegate to attraction route (rides are attractions)
    return this.getAttractionByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
      rideSlug,
    );
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/attractions/:attractionSlug
   *
   * Returns a specific attraction within a park via full geographic path (primary route).
   * This is the preferred route with full integration support.
   *
   * @param continent - Continent slug (e.g., "europe", "north-america")
   * @param country - Country slug (e.g., "germany", "united-states")
   * @param city - City slug (e.g., "rust", "orlando")
   * @param parkSlug - Park slug (e.g., "europa-park", "magic-kingdom")
   * @param attractionSlug - Attraction slug (e.g., "taron", "space-mountain")
   * @throws NotFoundException if park or attraction not found
   */
  @Get(":continent/:country/:city/:parkSlug/attractions/:attractionSlug")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live wait times
  @ApiOperation({
    summary: "Get attraction details (geo)",
    description:
      "Returns details for a specific attraction in a park via full geographic path. " +
      "This is the primary route with full integration support. Cached for 2 minutes.",
  })
  @ApiParam({
    name: "continent",
    description: "Continent slug (e.g., 'europe', 'north-america')",
    example: "europe",
  })
  @ApiParam({
    name: "country",
    description: "Country slug (e.g., 'germany', 'united-states')",
    example: "germany",
  })
  @ApiParam({
    name: "city",
    description: "City slug (e.g., 'bruhl', 'orlando')",
    example: "bruhl",
  })
  @ApiParam({
    name: "parkSlug",
    description: "Park slug (e.g., 'phantasialand', 'magic-kingdom')",
    example: "phantasialand",
  })
  @ApiParam({
    name: "attractionSlug",
    description: "Attraction slug (e.g., 'taron', 'space-mountain')",
    example: "taron",
  })
  @ApiResponse({
    status: 200,
    description: "Attraction details with full integration",
    type: AttractionResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park or attraction not found" })
  async getAttractionByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Param("attractionSlug") attractionSlug: string,
  ): Promise<AttractionResponseDto> {
    const attraction = await this.attractionsService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
      attractionSlug,
    );

    if (!attraction) {
      throw new NotFoundException(
        `Attraction with slug "${attractionSlug}" not found in park "${parkSlug}" at ${city}, ${country}, ${continent}`,
      );
    }

    // Return with full integration (live data, forecasts, ML predictions, etc.)
    return this.attractionIntegrationService.buildIntegratedResponse(
      attraction,
    );
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug
   *
   * Returns a specific park by geographic path.
   *
   * @param continent - Continent slug (e.g., "europe", "north-america")
   * @param country - Country slug (e.g., "germany", "united-states")
   * @param city - City slug (e.g., "rust", "orlando")
   * @param parkSlug - Park slug (e.g., "europa-park", "magic-kingdom-park")
   * @throws NotFoundException if park not found
   */
  @Get(":continent/:country/:city/:parkSlug")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live park status
  @ApiOperation({
    summary: "Get park by location",
    description: "Returns a specific park by its geographic structure.",
  })
  @ApiResponse({
    status: 200,
    description: "Park details",
    type: ParkWithAttractionsDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getParkByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
  ): Promise<ParkWithAttractionsDto> {
    const park = await this.parksService.findByGeographicPath(
      continent,
      country,
      city,
      parkSlug,
    );

    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }

    // Return with integrated live data
    return this.parkIntegrationService.buildIntegratedResponse(park);
  }

  /**
   * Helper: Build weather forecast response
   * Extracts duplicated logic from both weather forecast endpoints
   */
  private async buildWeatherForecastResponse(
    park: Park,
  ): Promise<WeatherResponseDto> {
    // Get forecast data (next 16 days) using park's timezone
    const { getCurrentDateInTimezone } =
      await import("../common/utils/date.util");
    const { fromZonedTime } = await import("date-fns-tz");

    const todayStr = getCurrentDateInTimezone(park.timezone);
    const today = fromZonedTime(`${todayStr}T00:00:00`, park.timezone);
    const futureDate = new Date(today);
    futureDate.setDate(futureDate.getDate() + 16);

    const weatherData = await this.weatherService.getWeatherData(
      park.id,
      today,
      futureDate,
    );

    // Filter to only forecast data
    const forecastData = weatherData.filter((w) => w.dataType === "forecast");

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
      },
      weather: forecastData.map((w) => WeatherItemDto.fromEntity(w)),
    };
  }

  /**
   * Helper: Map parks to response DTOs with status and analytics
   * @deprecated Use ParkEnrichmentService.enrichParksWithLiveData instead
   */
  private async mapToResponseWithStatus(
    parks: Park[],
  ): Promise<ParkResponseDto[]> {
    return this.parkEnrichmentService.enrichParksWithLiveData(parks);
  }
}
