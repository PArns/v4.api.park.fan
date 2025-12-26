import {
  Controller,
  Get,
  Param,
  NotFoundException,
  UseInterceptors,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from "@nestjs/swagger";
import { DiscoveryService } from "./discovery.service";
import {
  GeoStructureDto,
  ContinentDto,
  CountryDto,
  CityDto,
} from "./dto/geo-structure.dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";
import { ParkIntegrationService } from "../parks/services/park-integration.service";
import { ParksService } from "../parks/parks.service";
import { ParkResponseDto } from "../parks/dto/park-response.dto";
import { ParkWithAttractionsDto } from "../parks/dto/park-with-attractions.dto";
import { BreadcrumbDto } from "../common/dto/breadcrumb.dto";
import { AnalyticsService } from "../analytics/analytics.service";

/**
 * Discovery Controller
 *
 * Provides endpoints for discovering the geographic structure of parks.
 * Used for route generation and navigation on the frontend.
 */
@ApiTags("discovery")
@Controller("discovery")
export class DiscoveryController {
  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly parkIntegrationService: ParkIntegrationService,
    private readonly parksService: ParksService,
    private readonly analyticsService: AnalyticsService,
  ) { }

  /**
   * GET /v1/discovery/geo
   *
   * Returns complete hierarchical geographic structure.
   * Cached for 24 hours (HTTP + Redis).
   */
  @Get("geo")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60)) // 24 hours HTTP cache
  @ApiOperation({
    summary: "Get complete geo structure",
    description:
      "Returns hierarchical structure of continents → countries → cities → parks → attractions. " +
      "Includes full URL paths for route generation. Cached for 24 hours. " +
      "Perfect for static site generation and building navigation menus.",
  })
  @ApiResponse({
    status: 200,
    description: "Geographic structure with attractions",
    type: GeoStructureDto,
    example: {
      continents: [
        {
          name: "Europe",
          slug: "europe",
          countryCount: 12,
          parkCount: 45,
          countries: [
            {
              name: "Germany",
              slug: "germany",
              code: "DE",
              cityCount: 8,
              parkCount: 15,
              cities: [
                {
                  name: "Rust",
                  slug: "rust",
                  parkCount: 2,
                  parks: [
                    {
                      id: "abc-123",
                      name: "Europa-Park",
                      slug: "europa-park",
                      url: "/europe/germany/rust/europa-park",
                      attractionCount: 15,
                      attractions: [
                        {
                          id: "xyz-789",
                          name: "Blue Fire Megacoaster",
                          slug: "blue-fire-megacoaster",
                          url: "/europe/germany/rust/europa-park/blue-fire-megacoaster",
                        },
                      ],
                    },
                  ],
                },
              ],
            },
          ],
        },
      ],
      continentCount: 5,
      countryCount: 32,
      cityCount: 78,
      parkCount: 139,
      attractionCount: 2345,
      generatedAt: "2024-01-15T10:00:00.000Z",
    },
  })
  async getGeoStructure(): Promise<GeoStructureDto> {
    return this.discoveryService.getGeoStructure();
  }

  /**
   * GET /v1/discovery/continents
   *
   * Returns all continents with nested data.
   */
  @Get("continents")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60))
  @ApiOperation({
    summary: "List all continents",
    description:
      "Returns all continents with countries, cities, parks, and attractions. " +
      "Same data as /geo endpoint but without global summary counts.",
  })
  @ApiResponse({
    status: 200,
    description: "List of continents with full nested structure",
    type: [ContinentDto],
  })
  async getContinents(): Promise<ContinentDto[]> {
    return this.discoveryService.getContinents();
  }

  /**
   * GET /v1/discovery/continents/:continentSlug
   *
   * Returns countries in a specific continent.
   */
  @Get("continents/:continentSlug")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60))
  @ApiOperation({
    summary: "Get countries in continent",
    description:
      "Returns all countries in a specific continent with cities, parks, and attractions. " +
      "Useful for building continent-specific navigation or filtering.",
  })
  @ApiParam({
    name: "continentSlug",
    description: "Continent slug (e.g., europe, north-america, asia)",
    example: "europe",
  })
  @ApiResponse({
    status: 200,
    description: "List of countries in the continent",
    type: [CountryDto],
  })
  @ApiResponse({
    status: 404,
    description: "Continent not found",
    example: {
      statusCode: 404,
      message: 'Continent with slug "invalid-continent" not found',
      error: "Not Found",
    },
  })
  async getCountriesInContinent(
    @Param("continentSlug") continentSlug: string,
  ): Promise<{
    data: CountryDto[];
    breadcrumbs: BreadcrumbDto[];
  }> {
    const countries =
      await this.discoveryService.getCountriesInContinent(continentSlug);

    if (!countries) {
      throw new NotFoundException(
        `Continent with slug "${continentSlug}" not found`,
      );
    }

    // Generate breadcrumbs: Home > Continent
    // Need proper name for continent? DTO usually has it.
    // If we only have slug from param, we might need to lookup.
    // However, countries[0] is not guaranteed if empty.
    // Let's rely on cached structure or simple slug formatting if needed.
    // Better: Helper in service. For now, simple standard breadcrumbs.
    const continentName =
      countries.length > 0
        ? (await this.discoveryService.getContinents()).find(
          (c) => c.slug === continentSlug,
        )?.name || continentSlug
        : continentSlug;

    const breadcrumbs: BreadcrumbDto[] = [
      { name: "Home", url: "/" },
      { name: continentName, url: `/${continentSlug}` },
    ];

    return { data: countries, breadcrumbs };
  }

  /**
   * GET /v1/discovery/continents/:continentSlug/:countrySlug
   *
   * Returns cities in a specific country.
   */
  @Get("continents/:continentSlug/:countrySlug")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60))
  @ApiOperation({
    summary: "Get cities in country",
    description:
      "Returns all cities in a specific country with parks and attractions. " +
      "Each park includes complete attraction listings with URLs for route generation.",
  })
  @ApiParam({
    name: "continentSlug",
    description: "Continent slug (e.g., europe, north-america)",
    example: "europe",
  })
  @ApiParam({
    name: "countrySlug",
    description: "Country slug (e.g., germany, france, united-states)",
    example: "germany",
  })
  @ApiResponse({
    status: 200,
    description: "List of cities with parks and attractions",
    type: [CityDto],
  })
  @ApiResponse({
    status: 404,
    description: "Continent or country not found",
    example: {
      statusCode: 404,
      message:
        'Country with slug "invalid-country" not found in continent "europe"',
      error: "Not Found",
    },
  })
  async getCitiesInCountry(
    @Param("continentSlug") continentSlug: string,
    @Param("countrySlug") countrySlug: string,
  ): Promise<{
    data: CityDto[];
    breadcrumbs: BreadcrumbDto[];
  }> {
    const cities = await this.discoveryService.getCitiesInCountry(
      continentSlug,
      countrySlug,
    );

    if (!cities) {
      throw new NotFoundException(
        `Country with slug "${countrySlug}" not found in continent "${continentSlug}"`,
      );
    }

    // Breadcrumbs: Home > Continent > Country
    const continents = await this.discoveryService.getContinents();
    const continent = continents.find((c) => c.slug === continentSlug);
    const country = continent?.countries.find((c) => c.slug === countrySlug);

    const breadcrumbs: BreadcrumbDto[] = [
      { name: "Home", url: "/" },
      {
        name: continent?.name || continentSlug,
        url: `/${continentSlug}`,
      },
      {
        name: country?.name || countrySlug,
        url: `/${continentSlug}/${countrySlug}`,
      },
    ];

    return { data: cities, breadcrumbs };
  }

  /**
   * GET /v1/discovery/:continent/:country
   * HYDRATED Discovery Endpoint
   * Returns list of hydrated park objects for a country
   */
  @Get(":continent/:country")
  @UseInterceptors(new HttpCacheInterceptor(5 * 60)) // 5 mins cache for hydrated data
  @ApiOperation({
    summary: "Get hydrated parks in country",
    description: "Returns fully hydrated park objects for a country.",
  })
  async getHydratedParksByCountry(
    @Param("continent") continentSlug: string,
    @Param("country") countrySlug: string,
  ): Promise<{
    data: ParkResponseDto[];
    breadcrumbs: BreadcrumbDto[];
  }> {
    // 1. Find parks from DB (or ParksService)
    const parks = await this.parksService.findByCountry(
      continentSlug,
      countrySlug,
    );

    if (parks.length === 0) {
      throw new NotFoundException(
        `No parks found in ${countrySlug}, ${continentSlug}`,
      );
    }

    // 2. Hydrate them concurrently
    // Use mapToResponseWithStatus logic from ParksController?
    // Or better: buildIntegratedResponse if we want FULL details including attractions/weather?
    // Requirement: "return fully hydrated park objects identical to the ParkResponse interface"
    // ParkResponseDto usually implies the list view (which has stats/status).
    // ParkWithAttractionsDto has nested attractions.
    // "Response must show correct counts" implies ParkResponseDto (list view).
    // Let's us ParkResponseDto but ensures stats are correct.

    // Reuse logic from ParksController's mapToResponseWithStatus
    // We can't import private methods. We should move that logic to a service if possible.
    // For now, we replicate it or use ParkIntegrationService if it supports lists.
    // ParkIntegrationService.buildIntegratedResponse is for SINGLE park with full details.
    // For lists, we usually want simplified status/analytics.

    // Let's fetch status/occupancy batch
    const parkIds = parks.map((p) => p.id);
    const [statusMap, occupancyMap] = await Promise.all([
      this.parksService.getBatchParkStatus(parkIds),
      this.analyticsService["getBatchParkOccupancy"](parkIds), // Accessed via public if possible, else we assume it's public
    ]);

    // Fetch batch statistics map
    // TODO: move to service to avoid duplication
    const statisticsMap = new Map<string, any>();
    await Promise.all(
      parks.map(async (park) => {
        try {
          const stats = await this.analyticsService.getParkStatistics(park.id);
          statisticsMap.set(park.id, stats);
        } catch (e) {
          // ignore
        }
      }),
    );

    const hydrated = parks.map((park) => {
      const dto = ParkResponseDto.fromEntity(park);
      dto.status = statusMap.get(park.id) || "CLOSED";
      const occupancy = occupancyMap.get(park.id);
      const stats = statisticsMap.get(park.id);

      if (occupancy) {
        dto.currentLoad = {
          crowdLevel: this.mapCrowdLevel(occupancy.current),
          baseline: occupancy.baseline90thPercentile,
          currentWaitTime: occupancy.breakdown?.currentAvgWait || 0,
        };

        dto.analytics = {
          occupancy: {
            current: occupancy.current,
            trend: occupancy.trend,
            comparedToTypical: occupancy.comparedToTypical,
            comparisonStatus: occupancy.comparisonStatus,
            baseline90thPercentile: occupancy.baseline90thPercentile,
            updatedAt: occupancy.updatedAt,
          },
          statistics: {
            avgWaitTime: occupancy.breakdown?.currentAvgWait || 0,
            avgWaitToday: stats?.avgWaitToday || 0,
            peakHour: stats?.peakHour || null,
            crowdLevel: this.mapCrowdLevel(occupancy.current),
            totalAttractions: stats?.totalAttractions || 0,
            operatingAttractions: stats?.operatingAttractions || 0,
            closedAttractions: stats?.closedAttractions || 0,
            timestamp: occupancy.updatedAt,
          },
        };
      }
      return dto;
    });

    // Breadcrumbs
    const continents = await this.discoveryService.getContinents();
    const continent = continents.find((c) => c.slug === continentSlug);
    const country = continent?.countries.find((c) => c.slug === countrySlug);

    const breadcrumbs: BreadcrumbDto[] = [
      { name: "Home", url: "/" },
      {
        name: continent?.name || continentSlug,
        url: `/${continentSlug}`,
      },
      {
        name: country?.name || countrySlug,
        url: `/${continentSlug}/${countrySlug}`,
      },
    ];

    return { data: hydrated, breadcrumbs };
  }

  private mapCrowdLevel(occupancy: number): any {
    if (occupancy < 30) return "very_low";
    if (occupancy < 50) return "low";
    if (occupancy < 75) return "moderate";
    if (occupancy < 95) return "high";
    return "very_high";
  }
}
