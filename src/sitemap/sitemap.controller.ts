import { Controller, Get, UseInterceptors } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { SitemapService, AttractionSitemapItem } from "./sitemap.service";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";

@ApiTags("sitemap")
@Controller("sitemap")
export class SitemapController {
  constructor(private readonly sitemapService: SitemapService) {}

  /**
   * GET /v1/sitemap/attractions
   *
   * Returns minimal routing data for all attractions.
   * Intended for sitemap generation and static path pre-rendering.
   * No analytics, no status — only url and slug.
   */
  @Get("attractions")
  @UseInterceptors(new HttpCacheInterceptor(24 * 60 * 60)) // 24h HTTP cache
  @ApiOperation({
    summary: "Get sitemap entries for all attractions",
    description:
      "Returns a flat array of { url, slug } for every attraction with complete geo data. " +
      "Designed for sitemap generation and static route pre-rendering. " +
      "Cached for 24 hours — attractions are added/removed infrequently.",
  })
  @ApiResponse({
    status: 200,
    description: "Flat list of attraction routing data",
    schema: {
      type: "array",
      items: {
        type: "object",
        properties: {
          url: {
            type: "string",
            example:
              "/v1/parks/europe/germany/rust/europa-park/attractions/blue-fire-megacoaster",
          },
          slug: {
            type: "string",
            example: "blue-fire-megacoaster",
          },
        },
        required: ["url", "slug"],
      },
    },
  })
  async getAttractionsSitemap(): Promise<AttractionSitemapItem[]> {
    return this.sitemapService.getAttractionsSitemap();
  }
}
