import { Controller, Get, Query, UseInterceptors } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse, ApiQuery } from "@nestjs/swagger";
import { SearchService } from "./search.service";
import { SearchQueryDto } from "./dto/search-query.dto";
import { SearchResultDto } from "./dto/search-result.dto";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

@ApiTags("search")
@Controller("search")
export class SearchController {
  constructor(private readonly searchService: SearchService) {}

  @Get()
  @UseInterceptors(new HttpCacheInterceptor(60)) // 1 minute - matches Redis TTL
  @ApiOperation({
    summary: "Intelligent search across all entities",
    description:
      "Search parks, attractions, shows, and restaurants by name, city, country, or continent. " +
      "Returns enriched results with coordinates, wait times, park hours, show times, and more. " +
      "Cached for 1 minute for optimal performance.",
  })
  @ApiQuery({
    name: "q",
    description: "Search query (min 2 characters)",
    example: "disney",
  })
  @ApiQuery({
    name: "type",
    required: false,
    description: "Filter by entity type",
    enum: ["park", "attraction", "show", "restaurant"],
    isArray: true,
    example: "park,attraction",
  })
  @ApiResponse({
    status: 200,
    description: "Search results with counts and enriched metadata",
    type: SearchResultDto,
  })
  @ApiResponse({
    status: 400,
    description: "Invalid search query (too short)",
  })
  async search(@Query() query: SearchQueryDto): Promise<SearchResultDto> {
    return this.searchService.search(query);
  }
}
