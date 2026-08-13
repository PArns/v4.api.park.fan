import { Controller, Get, Header } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { ApiCatalog, AppService } from "./app.service";

/**
 * RFC 9727 §2: a HEAD on the catalog must answer with the api-catalog relation, and §3 lets
 * any response carry it. The docs page at / does, so a crawler that lands there is one hop
 * from the machine-readable version.
 */
const API_CATALOG_LINK =
  '</.well-known/api-catalog>; rel="api-catalog"; type="application/linkset+json"';

@ApiTags("root")
@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  @Header("Content-Type", "text/html; charset=utf-8")
  @Header("Cache-Control", "public, max-age=3600")
  @Header("Link", API_CATALOG_LINK)
  @ApiOperation({
    summary: "API Documentation",
    description: "Returns the API documentation as HTML from README.md",
  })
  @ApiResponse({
    status: 200,
    description: "HTML rendered README documentation",
    type: String,
  })
  async getRoot(): Promise<string> {
    return this.appService.getReadmeAsHtml();
  }

  @Get("robots.txt")
  @Header("Content-Type", "text/plain; charset=utf-8")
  @Header("Cache-Control", "public, max-age=86400")
  @ApiOperation({
    summary: "robots.txt",
    description:
      "Keeps crawlers off the JSON API surface and the Swagger UI. Without it every " +
      "crawler probe logged a 404 warning.",
  })
  @ApiResponse({
    status: 200,
    description: "Plain-text robots directives",
    type: String,
  })
  getRobotsTxt(): string {
    return this.appService.getRobotsTxt();
  }

  /**
   * Like robots.txt, this only works while the path stays in `setGlobalPrefix`'s exclude list:
   * RFC 9727 fixes it at the host root, and a catalog that quietly moved to
   * /v1/.well-known/api-catalog would be a path no agent ever asks for.
   */
  @Get(".well-known/api-catalog")
  @Header(
    "Content-Type",
    'application/linkset+json; profile="https://www.rfc-editor.org/info/rfc9727"',
  )
  @Header("Cache-Control", "public, max-age=3600")
  @Header("Link", API_CATALOG_LINK)
  @ApiOperation({
    summary: "API catalog (RFC 9727)",
    description:
      "Linkset naming this API's OpenAPI description, documentation and health endpoint, " +
      "for agents that discover APIs by probing the well-known URI.",
  })
  @ApiResponse({
    status: 200,
    description: "RFC 9264 Linkset describing the APIs published here",
  })
  getApiCatalog(): ApiCatalog {
    return this.appService.getApiCatalog();
  }
}
