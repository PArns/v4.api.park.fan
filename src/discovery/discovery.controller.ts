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
      "Returns hierarchical structure of continents → countries → cities → parks. " +
      "Includes full URL paths for route generation. Cached for 24 hours.",
  })
  @ApiResponse({
    status: 200,
    description: "Geographic structure",
    type: GeoStructureDto,
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
    description: "Returns all continents with countries, cities, and parks.",
  })
  @ApiResponse({
    status: 200,
    description: "List of continents",
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
      "Returns all countries in a specific continent with cities and parks.",
  })
  @ApiParam({
    name: "continentSlug",
    description: "Continent slug",
    example: "europe",
  })
  @ApiResponse({
    status: 200,
    description: "List of countries",
    type: [CountryDto],
  })
  @ApiResponse({
    status: 404,
    description: "Continent not found",
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
    description: "Returns all cities in a specific country with parks.",
  })
  @ApiParam({
    name: "continentSlug",
    description: "Continent slug",
    example: "europe",
  })
  @ApiParam({
    name: "countrySlug",
    description: "Country slug",
    example: "germany",
  })
  @ApiResponse({
    status: 200,
    description: "List of cities",
    type: [CityDto],
  })
  @ApiResponse({
    status: 404,
    description: "Continent or country not found",
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
