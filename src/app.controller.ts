import { Controller, Get, Header } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { AppService } from "./app.service";

@ApiTags("root")
@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  @Header("Content-Type", "text/html; charset=utf-8")
  @Header("Cache-Control", "public, max-age=3600")
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
}
