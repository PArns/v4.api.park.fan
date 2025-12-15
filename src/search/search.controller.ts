import { Controller, Get, Query } from "@nestjs/common";
import { ApiTags } from "@nestjs/swagger";
import { SearchService } from "./search.service";
import { SearchQueryDto } from "./dto/search-query.dto";
import { SearchResultDto } from "./dto/search-result.dto";

@ApiTags("search")
@Controller("search")
export class SearchController {
  constructor(private readonly searchService: SearchService) {}

  @Get()
  async search(@Query() query: SearchQueryDto): Promise<SearchResultDto> {
    return this.searchService.search(query);
  }
}
