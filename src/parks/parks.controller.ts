import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
  BadRequestException,
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
  async findParksWithMissingGeocode(): Promise<any[]> {
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

    // Parse and validate date parameters
    let fromDate: Date;
    let toDate: Date;

    if (from) {
      fromDate = new Date(from);
      if (isNaN(fromDate.getTime())) {
        throw new BadRequestException(
          'Invalid "from" date format. Use YYYY-MM-DD.',
        );
      }
    } else {
      fromDate = new Date();
    }
    fromDate.setHours(0, 0, 0, 0);

    if (to) {
      toDate = new Date(to);
      if (isNaN(toDate.getTime())) {
        throw new BadRequestException(
          'Invalid "to" date format. Use YYYY-MM-DD.',
        );
      }
    } else {
      // Default: 30 days ahead
      toDate = new Date(fromDate);
      toDate.setDate(toDate.getDate() + 30);
    }
    toDate.setHours(23, 59, 59, 999);

    // Validate range
    const daysDiff = Math.ceil(
      (toDate.getTime() - fromDate.getTime()) / (1000 * 60 * 60 * 24),
    );
    if (daysDiff > 90) {
      throw new BadRequestException(
        "Date range too large. Maximum allowed: 90 days.",
      );
    }

    return this.calendarService.buildCalendarResponse(
      park,
      fromDate,
      toDate,
      includeHourly || "today+tomorrow",
    );
  }

  /**
   * GET /v1/parks/:slug/weather/forecast
   *
   * Returns 16-day weather forecast for a park.
   *
   * @param slug - Park slug
   * @throws NotFoundException if park not found
   */
  @Get(":slug/weather/forecast")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache (weather updates every 12h, but we want faster cache invalidation)
  @ApiOperation({
    summary: "Get weather forecast",
    description: "Returns 16-day weather forecast.",
  })
  @ApiResponse({
    status: 200,
    description: "Weather forecast",
    type: WeatherResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getWeatherForecast(
    @Param("slug") slug: string,
  ): Promise<WeatherResponseDto> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Get forecast data (next 16 days)
    const today = new Date();
    today.setHours(0, 0, 0, 0);
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
   * GET /v1/parks/:slug/weather
   *
   * Returns weather data for a park within a date range.
   * Includes historical, current, and forecast data.
   *
   * @param slug - Park slug
   * @param from - Start date (YYYY-MM-DD, optional, defaults to 30 days ago)
   * @param to - End date (YYYY-MM-DD, optional, defaults to today)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid
   */
  @Get(":slug/weather")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache
  @ApiOperation({
    summary: "Get weather history/forecast",
    description: "Returns weather data within a date range.",
  })
  @ApiResponse({
    status: 200,
    description: "Weather data",
    type: WeatherResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getWeather(
    @Param("slug") slug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
  ): Promise<WeatherResponseDto> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Parse date parameters
    let fromDate: Date;
    let toDate: Date;

    if (from) {
      fromDate = new Date(from);
      if (isNaN(fromDate.getTime())) {
        throw new BadRequestException(
          'Invalid "from" date format. Use YYYY-MM-DD.',
        );
      }
    } else {
      // Default: 30 days ago
      fromDate = new Date();
      fromDate.setDate(fromDate.getDate() - 30);
    }
    fromDate.setHours(0, 0, 0, 0);

    if (to) {
      toDate = new Date(to);
      if (isNaN(toDate.getTime())) {
        throw new BadRequestException(
          'Invalid "to" date format. Use YYYY-MM-DD.',
        );
      }
    } else {
      // Default: today
      toDate = new Date();
    }
    toDate.setHours(23, 59, 59, 999);

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
   * GET /v1/parks/:slug/schedule/:date
   *
   * Returns schedule for a specific date.
   *
   * @param slug - Park slug
   * GET /v1/parks/:slug/schedule
   *
   * Returns park operating schedule with optional date range filtering.
   *
   * @param slug - Park slug
   * @param from - Start date (YYYY-MM-DD, optional)
   * @param to - End date (YYYY-MM-DD, optional)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid
   */
  @Get(":slug/schedule")
  @UseInterceptors(new HttpCacheInterceptor(60 * 60)) // 1 hour HTTP cache
  @ApiOperation({
    summary: "Get schedule range",
    description:
      "Returns schedule data within a date range (default: 30 days).",
  })
  @ApiResponse({
    status: 200,
    description: "Schedule data",
    type: ScheduleResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getSchedule(
    @Param("slug") slug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
  ): Promise<ScheduleResponseDto> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Parse date parameters
    let fromDate: Date;
    let toDate: Date;

    if (from) {
      fromDate = new Date(from);
      if (isNaN(fromDate.getTime())) {
        throw new BadRequestException(
          'Invalid "from" date format. Use YYYY-MM-DD.',
        );
      }
    } else {
      // Default: today
      fromDate = new Date();
    }
    fromDate.setHours(0, 0, 0, 0);

    if (to) {
      toDate = new Date(to);
      if (isNaN(toDate.getTime())) {
        throw new BadRequestException(
          'Invalid "to" date format. Use YYYY-MM-DD.',
        );
      }
    } else {
      // Default: 30 days ahead
      toDate = new Date(fromDate);
      toDate.setDate(toDate.getDate() + 30);
    }
    toDate.setHours(23, 59, 59, 999);

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
   * GET /v1/parks/:slug/predictions/yearly
   *
   * Returns yearly crowd predictions (365 days) for trip planning.
   */
  @Get(":slug/predictions/yearly")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60)) // 24 hours HTTP cache
  @ApiOperation({
    summary: "Get yearly crowd predictions",
    description:
      "Returns daily crowd predictions for the entire year (365 days). " +
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
  async getYearlyPredictions(@Param("slug") slug: string) {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park not found: ${slug}`);
    }

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
   * GET /v1/parks/:slug/attractions
   *
   * Returns all attractions in a specific park.
   *
   * @param slug - Park slug
   * @throws NotFoundException if park not found
   */
  @Get(":slug/attractions")
  @ApiOperation({
    summary: "List park attractions",
    description:
      "Returns a paginated list of all attractions for a specific park.",
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
  async getAttractionsInPark(
    @Param("slug") slug: string,
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
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Get paginated attractions for this park
    const { data: attractions, total } =
      await this.attractionsService.findByParkId(park.id, page, limit);

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
   * GET /v1/parks/:slug/attractions/:attractionSlug
   *
   * Returns a specific attraction within a park (hierarchical route).
   *
   * @param slug - Park slug
   * @param attractionSlug - Attraction slug
   * @throws NotFoundException if park or attraction not found
   */
  @Get(":slug/attractions/:attractionSlug")
  @ApiOperation({
    summary: "Get attraction details",
    description: "Returns details for a specific attraction in a park.",
  })
  @ApiResponse({
    status: 200,
    description: "Attraction details",
    type: AttractionResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park or attraction not found" })
  async getAttractionInPark(
    @Param("slug") slug: string,
    @Param("attractionSlug") attractionSlug: string,
  ): Promise<AttractionResponseDto> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    const attraction = await this.attractionsService.findBySlugInPark(
      park.id,
      attractionSlug,
    );

    if (!attraction) {
      throw new NotFoundException(
        `Attraction with slug "${attractionSlug}" not found in park "${park.name}"`,
      );
    }

    return AttractionResponseDto.fromEntity(attraction);
  }

  /**
   * GET /v1/parks/:slug/wait-times
   *
   * Returns current wait times for all attractions in a park.
   * This route MUST come before geographic routes to avoid conflicts.
   *
   * @param slug - Park slug
   * @param queueType - Optional queue type filter
   * @throws NotFoundException if park not found
   */
  @Get(":slug/wait-times")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live wait times
  @ApiOperation({
    summary: "Get current wait times",
    description: "Returns live wait times for all attractions in a park.",
  })
  @ApiResponse({ status: 200, description: "Wait times data" })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getParkWaitTimes(
    @Param("slug") slug: string,
    @Query("queueType") queueType?: QueueType,
  ): Promise<{ park: any; attractions: any[] }> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Check timezone-aware park status
    const statusMap = await this.parksService.getBatchParkStatus([park.id]);
    const parkStatus = statusMap.get(park.id) || "CLOSED";

    // If park is CLOSED, return empty wait times (no stale data)
    if (parkStatus === "CLOSED") {
      return {
        park: {
          id: park.id,
          name: park.name,
          slug: park.slug,
          timezone: park.timezone,
          status: "CLOSED",
        },
        attractions: [],
      };
    }

    // Park is OPERATING - fetch live wait times
    const waitTimes = await this.queueDataService.findWaitTimesByPark(
      park.id,
      queueType,
    );

    // Group by attraction
    const attractionsMap = new Map<string, any>();

    for (const queueData of waitTimes) {
      const attractionId = queueData.attraction.id;

      if (!attractionsMap.has(attractionId)) {
        attractionsMap.set(attractionId, {
          attraction: {
            id: queueData.attraction.id,
            name: queueData.attraction.name,
            slug: queueData.attraction.slug,
          },
          queues: [],
        });
      }

      const queueDto = {
        queueType: queueData.queueType,
        status: queueData.status,
        waitTime: queueData.waitTime ?? null,
        state: queueData.state ?? null,
        returnStart: queueData.returnStart
          ? queueData.returnStart.toISOString()
          : null,
        returnEnd: queueData.returnEnd
          ? queueData.returnEnd.toISOString()
          : null,
        price: queueData.price ?? null,
        allocationStatus: queueData.allocationStatus ?? null,
        currentGroupStart: queueData.currentGroupStart ?? null,
        currentGroupEnd: queueData.currentGroupEnd ?? null,
        estimatedWait: queueData.estimatedWait ?? null,
        lastUpdated: (
          queueData.lastUpdated || queueData.timestamp
        ).toISOString(),
      };

      attractionsMap.get(attractionId)!.queues.push(queueDto);
    }

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
        status: parkStatus,
      },
      attractions: Array.from(attractionsMap.values()),
    };
  }

  /**
   * GET /v1/parks/:continent/:country/:city/:parkSlug/wait-times
   *
   * Returns current wait times for a park via geographic path.
   * This route MUST come before the generic geographic route.
   *
   * @param continent - Continent slug
   * @param country - Country slug
   * @param city - City slug
   * @param parkSlug - Park slug
   * @param queueType - Optional queue type filter
   * @throws NotFoundException if park not found
   */
  @Get(":continent/:country/:city/:parkSlug/wait-times")
  @ApiOperation({
    summary: "Get wait times (geo)",
    description: "Returns wait times via geographic path.",
  })
  @ApiResponse({ status: 200, description: "Wait times data" })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getParkWaitTimesByGeographicPath(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("queueType") queueType?: QueueType,
  ): Promise<{ park: any; attractions: any[] }> {
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

    // Check timezone-aware park status
    const statusMap = await this.parksService.getBatchParkStatus([park.id]);
    const parkStatus = statusMap.get(park.id) || "CLOSED";

    // If park is CLOSED, return empty wait times (no stale data)
    if (parkStatus === "CLOSED") {
      return {
        park: {
          id: park.id,
          name: park.name,
          slug: park.slug,
          timezone: park.timezone,
          status: "CLOSED",
        },
        attractions: [],
      };
    }

    // Park is OPERATING - fetch live wait times
    const waitTimes = await this.queueDataService.findWaitTimesByPark(
      park.id,
      queueType,
    );

    // Group by attraction
    const attractionsMap = new Map<string, any>();

    for (const queueData of waitTimes) {
      const attractionId = queueData.attraction.id;

      if (!attractionsMap.has(attractionId)) {
        attractionsMap.set(attractionId, {
          attraction: {
            id: queueData.attraction.id,
            name: queueData.attraction.name,
            slug: queueData.attraction.slug,
          },
          queues: [],
        });
      }

      const queueDto = {
        queueType: queueData.queueType,
        status: queueData.status,
        waitTime: queueData.waitTime ?? null,
        state: queueData.state ?? null,
        returnStart: queueData.returnStart
          ? queueData.returnStart.toISOString()
          : null,
        returnEnd: queueData.returnEnd
          ? queueData.returnEnd.toISOString()
          : null,
        price: queueData.price ?? null,
        allocationStatus: queueData.allocationStatus ?? null,
        currentGroupStart: queueData.currentGroupStart ?? null,
        currentGroupEnd: queueData.currentGroupEnd ?? null,
        estimatedWait: queueData.estimatedWait ?? null,
        lastUpdated: (
          queueData.lastUpdated || queueData.timestamp
        ).toISOString(),
      };

      attractionsMap.get(attractionId)!.queues.push(queueDto);
    }

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
        status: parkStatus,
      },
      attractions: Array.from(attractionsMap.values()),
    };
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
   * GET /v1/parks/:slugOrContinent
   *
   * Ambiguous route that handles both:
   * - Individual park by slug (e.g., "magic-kingdom-park")
   * - All parks in continent (e.g., "europe", "north-america")
   *
   * Strategy: Try park first, if not found, try continent.
   *
   * @param slugOrContinent - Park slug OR continent slug
   * @throws NotFoundException if neither park nor continent found
   */
  @Get(":slugOrContinent")
  @ApiOperation({
    summary: "Get park or continent",
    description:
      "Smart route that returns either a specific park or a list of parks in a continent.",
  })
  @ApiResponse({
    status: 200,
    description: "Park or list of parks",
    type: ParkWithAttractionsDto,
  })
  @ApiResponse({ status: 404, description: "Not found" })
  async findOneOrContinent(
    @Param("slugOrContinent") slugOrContinent: string,
  ): Promise<ParkWithAttractionsDto | ParkResponseDto[]> {
    // First, try to find a park with this slug
    const park = await this.parksService.findBySlug(slugOrContinent);

    if (park) {
      // Found a park! Return it with integrated live data
      return this.parkIntegrationService.buildIntegratedResponse(park);
    }

    // Not a park, try as continent
    const parks = await this.parksService.findByContinent(slugOrContinent);

    if (parks.length > 0) {
      // Found continent! Return list of parks
      return this.mapToResponseWithStatus(parks);
    }

    // Not found as park OR continent
    throw new NotFoundException(
      `Park or continent with slug "${slugOrContinent}" not found`,
    );
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
