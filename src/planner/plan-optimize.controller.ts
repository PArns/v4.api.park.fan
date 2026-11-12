import {
  Body,
  Controller,
  Header,
  HttpCode,
  HttpStatus,
  Param,
  Post,
} from "@nestjs/common";
import {
  ApiBody,
  ApiOperation,
  ApiParam,
  ApiResponse,
  ApiTags,
} from "@nestjs/swagger";
import { RequiresApiKey } from "../api-keys/requires-api-key.decorator";
import {
  PlanOptimizeRequestDto,
  PlanOptimizeResponseDto,
} from "./dto/plan-optimize.dto";
import { PlanOptimizeService } from "./plan-optimize.service";

/**
 * The trip planner's day optimizer, one route next to `/plan/day`. Its own controller so the parks
 * controller keeps its GET routes and their caching to itself; this answer is one visitor's plan
 * and is never cached anywhere.
 */
@ApiTags("parks")
@Controller("parks")
export class PlanOptimizeController {
  constructor(private readonly planOptimizeService: PlanOptimizeService) {}

  /**
   * POST /v1/parks/:continent/:country/:city/:parkSlug/plan/optimize
   */
  @Post(":continent/:country/:city/:parkSlug/plan/optimize")
  @HttpCode(HttpStatus.OK)
  @RequiresApiKey()
  @Header("Cache-Control", "no-store")
  @ApiOperation({
    summary: "Optimize one planned day",
    description:
      "The single-day trip planner's optimizer. Send the day as the planner holds it " +
      "(entries with park-local start minutes, free blocks, shows, ticked-off rides) and get " +
      "back the order with the least queueing: the same entries with new start minutes, and " +
      "for `headliners` the park's headliners added. The server plans against its own " +
      "`/plan/day` for that date; no forecast is uploaded. A day that cannot hold everything " +
      "asked for answers `needs-fit` with the fit assistant's opening answer, and `fit` " +
      "answers that assistant for one choice. Never cached. As on every route, a null field " +
      "is left out of the JSON: a stop without `entryId` is a ride being added.",
  })
  @ApiParam({ name: "continent", example: "europe" })
  @ApiParam({ name: "country", example: "germany" })
  @ApiParam({ name: "city", example: "bruehl" })
  @ApiParam({ name: "parkSlug", example: "phantasialand" })
  @ApiBody({ type: PlanOptimizeRequestDto })
  @ApiResponse({ status: 200, type: PlanOptimizeResponseDto })
  @ApiResponse({
    status: 400,
    description:
      "Malformed body: an impossible date, too many entries, a duplicate id, an unknown field, or a fit-only field on another mode.",
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  @ApiResponse({
    status: 503,
    description:
      "Every optimizer thread is busy and the queue is full; retry shortly.",
  })
  optimize(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Body() body: PlanOptimizeRequestDto,
  ): Promise<PlanOptimizeResponseDto> {
    return this.planOptimizeService.optimize(
      { continent, country, city, parkSlug },
      body,
    );
  }
}
