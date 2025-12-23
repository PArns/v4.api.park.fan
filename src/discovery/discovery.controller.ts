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

/**
 * Discovery Controller
 *
 * Provides endpoints for discovering the geographic structure of parks.
 * Used for route generation and navigation on the frontend.
 */
@ApiTags("discovery")
@Controller("discovery")
export class DiscoveryController {
  constructor(private readonly discoveryService: DiscoveryService) {}

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
  ): Promise<CountryDto[]> {
    const countries =
      await this.discoveryService.getCountriesInContinent(continentSlug);

    if (!countries) {
      throw new NotFoundException(
        `Continent with slug "${continentSlug}" not found`,
      );
    }

    return countries;
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
  ): Promise<CityDto[]> {
    const cities = await this.discoveryService.getCitiesInCountry(
      continentSlug,
      countrySlug,
    );

    if (!cities) {
      throw new NotFoundException(
        `Country with slug "${countrySlug}" not found in continent "${continentSlug}"`,
      );
    }

    return cities;
  }
}
