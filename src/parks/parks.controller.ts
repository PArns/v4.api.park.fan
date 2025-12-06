import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
  BadRequestException,
  Inject,
} from "@nestjs/common";
import { ParksService } from "./parks.service";
import { WeatherService } from "./weather.service";
import { ParkIntegrationService } from "./services/park-integration.service";
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
import { WeatherResponseDto } from "./dto/weather-response.dto";
import { WeatherItemDto } from "./dto/weather-item.dto";
import { ScheduleResponseDto } from "./dto/schedule-response.dto";
import { ScheduleItemDto } from "./dto/schedule-item.dto";
import { AttractionResponseDto } from "../attractions/dto/attraction-response.dto";
import { Park } from "./entities/park.entity";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";

/**
 * Parks Controller
 *
 * Provides REST API endpoints for accessing park data.
 *
 * Endpoints:
 * - GET /parks - List all parks
 * - GET /parks/:slug - Get specific park with attractions
 */
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
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * GET /v1/parks
   *
   * Returns all parks globally with optional filtering and sorting.
   *
   * @param query - Filter and sort options (continent, country, city, sort)
   */
  @Get()
  async findAll(@Query() query: ParkQueryDto): Promise<ParkResponseDto[]> {
    const parks = await this.parksService.findAllWithFilters(query);
    return this.mapToResponseWithStatus(parks);
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
   * GET /v1/parks/:slug/weather/forecast
   *
   * Returns 16-day weather forecast for a park.
   *
   * @param slug - Park slug
   * @throws NotFoundException if park not found
   */
  @Get(":slug/weather/forecast")
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
   * @param date - Date (YYYY-MM-DD)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid
   */
  @Get(":slug/schedule/:date")
  async getScheduleForDate(
    @Param("slug") slug: string,
    @Param("date") date: string,
  ): Promise<ScheduleResponseDto> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Parse date parameter
    const targetDate = new Date(date);
    if (isNaN(targetDate.getTime())) {
      throw new BadRequestException("Invalid date format. Use YYYY-MM-DD.");
    }
    targetDate.setHours(0, 0, 0, 0);

    // Get schedule for this specific date
    const endDate = new Date(targetDate);
    endDate.setHours(23, 59, 59, 999);

    const scheduleData = await this.parksService.getSchedule(
      park.id,
      targetDate,
      endDate,
    );

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
      },
      schedule: scheduleData.map((s) => ScheduleItemDto.fromEntity(s)),
    };
  }

  /**
   * GET /v1/parks/:slug/schedule
   *
   * Returns schedule data for a park within a date range.
   *
   * @param slug - Park slug
   * @param from - Start date (YYYY-MM-DD, optional, defaults to today)
   * @param to - End date (YYYY-MM-DD, optional, defaults to 30 days ahead)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if date format invalid
   */
  @Get(":slug/schedule")
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

    const scheduleData = await this.parksService.getSchedule(
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
      schedule: scheduleData.map((s) => ScheduleItemDto.fromEntity(s)),
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
  async getAttractionsInPark(
    @Param("slug") slug: string,
  ): Promise<AttractionResponseDto[]> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Get all attractions for this park
    const attractions = await this.attractionsService.findByParkId(park.id);

    return attractions.map((attraction) =>
      AttractionResponseDto.fromEntity(attraction),
    );
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
  async getParkWaitTimes(
    @Param("slug") slug: string,
    @Query("queueType") queueType?: QueueType,
  ): Promise<{ park: any; attractions: any[] }> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Delegate to QueueDataService
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
        timestamp: queueData.timestamp.toISOString(),
      };

      attractionsMap.get(attractionId)!.queues.push(queueDto);
    }

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
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

    // Delegate to QueueDataService
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
        timestamp: queueData.timestamp.toISOString(),
      };

      attractionsMap.get(attractionId)!.queues.push(queueDto);
    }

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
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
   * Helper: Map parks to response DTOs with status
   */
  private async mapToResponseWithStatus(
    parks: Park[],
  ): Promise<ParkResponseDto[]> {
    if (parks.length === 0) {
      return [];
    }

    const parkIds = parks.map((p) => p.id);
    const statusMap = await this.parksService.getBatchParkStatus(parkIds);

    return parks.map((park) => {
      const dto = ParkResponseDto.fromEntity(park);
      dto.status = statusMap.get(park.id);
      return dto;
    });
  }
}
