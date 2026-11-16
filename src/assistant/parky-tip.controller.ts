import {
  BadRequestException,
  Controller,
  Get,
  NotFoundException,
  Param,
  ParseEnumPipe,
  Query,
  Res,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import {
  ApiOperation,
  ApiParam,
  ApiQuery,
  ApiResponse,
  ApiTags,
} from "@nestjs/swagger";
import type { Response } from "express";
import { ApiPermission } from "../api-keys/api-permission.decorator";
import { isRealIsoDate } from "../common/utils/date.util";
import { ParksService } from "../parks/parks.service";
import { ASSISTANT_LOCALES, type AssistantLocale } from "./assistant-locales";
import {
  PARKY_TIP_HORIZON_DAYS,
  ParkyTipService,
  type ParkyTip,
} from "./parky-tip.service";

/** Six hours at the edge; the Redis copy lives twelve. */
const TIP_CACHE_CONTROL =
  "public, max-age=21600, s-maxage=21600, stale-while-revalidate=3600";

/** Parky's tip for one day of a park's crowd calendar (docs/frontend/assistant.md). */
@ApiTags("parks")
@Controller("parks")
export class ParkyTipController {
  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly tips: ParkyTipService,
  ) {}

  @Get(":continent/:country/:city/:parkSlug/parky-tip")
  @ApiPermission("forecasts.crowd", { parkRequired: true })
  @ApiOperation({
    summary: "Parky's tip for one calendar day (geo)",
    description:
      "One sentence in Parky's voice about the day, written by Claude from the calendar's " +
      "entries for the day and three days on each side, and a verdict from the day's crowd " +
      "level (very_low/low good, moderate meh, high and above bad). 204 when there is nothing " +
      `to say: a closed day, no forecast, a date before today or more than ${PARKY_TIP_HORIZON_DAYS} ` +
      "days ahead in park time, Parky switched off or the day's token budget spent. " +
      "Cache: public, max-age=21600, stale-while-revalidate=3600.",
  })
  @ApiParam({ name: "continent", example: "europe" })
  @ApiParam({ name: "country", example: "germany" })
  @ApiParam({ name: "city", example: "bruhl" })
  @ApiParam({ name: "parkSlug", example: "phantasialand" })
  @ApiQuery({
    name: "date",
    required: true,
    description: "The day (YYYY-MM-DD) in the park's timezone.",
    example: "2026-12-05",
  })
  @ApiQuery({ name: "locale", required: true, enum: ASSISTANT_LOCALES })
  @ApiResponse({
    status: 200,
    description: "`{ date, verdict: good|meh|bad, text, promptVersion }`",
  })
  @ApiResponse({ status: 204, description: "Nothing to say for this day" })
  @ApiResponse({ status: 400, description: "Invalid date or locale" })
  @ApiResponse({ status: 404, description: "Park not found" })
  async tip(
    @Param("continent") continent: string,
    @Param("country") country: string,
    @Param("city") city: string,
    @Param("parkSlug") parkSlug: string,
    @Query("date") date: string | undefined,
    @Query(
      "locale",
      new ParseEnumPipe(ASSISTANT_LOCALES, {
        exceptionFactory: () =>
          new BadRequestException(
            `locale must be one of: ${ASSISTANT_LOCALES.join(", ")}`,
          ),
      }),
    )
    locale: AssistantLocale,
    @Res({ passthrough: true }) res: Response,
  ): Promise<ParkyTip | undefined> {
    if (!isRealIsoDate(date)) {
      throw new BadRequestException("date must be a real YYYY-MM-DD day");
    }
    const park = await this.moduleRef
      .get(ParksService, { strict: false })
      .findByGeographicPath(continent, country, city, parkSlug);
    if (!park) {
      throw new NotFoundException(
        `Park with slug "${parkSlug}" not found in ${city}, ${country}, ${continent}`,
      );
    }
    const tip = await this.tips.tip(park, date, locale);
    if (!tip) {
      res.status(204);
      return undefined;
    }
    res.setHeader("Cache-Control", TIP_CACHE_CONTROL);
    return tip;
  }
}
