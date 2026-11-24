import {
  BadRequestException,
  Controller,
  Get,
  Param,
  ParseEnumPipe,
  Query,
  Res,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { ApiOperation, ApiQuery, ApiResponse, ApiTags } from "@nestjs/swagger";
import type { Response } from "express";
import { ApiPermission } from "../api-keys/api-permission.decorator";
import {
  getCurrentDateInTimezone,
  isRealIsoDate,
  secondsUntilEndOfDayInTimezone,
} from "../common/utils/date.util";
import { ParksService } from "../parks/parks.service";
import { ASSISTANT_LOCALES, type AssistantLocale } from "./assistant-locales";
import {
  PARKY_TIP_HORIZON_DAYS,
  ParkyTipService,
  type ParkyTip,
} from "./parky-tip.service";
import { ApiParkPath, parkNotFound } from "../parks/park-path";

/** Six hours, the tip's own window, where the park's day lasts that long. */
const TIP_MAX_AGE_SECONDS = 21_600;

/**
 * Nothing to say, for a few minutes: most of the 204s pass on their own (the day's budget, a failed
 * or refused call, Parky switched back on), and a dialog kept empty for hours would hide the tip
 * long after it could be written. A closed day costs the same calendar read again, no model call.
 */
export const TIP_NONE_CACHE_CONTROL = "public, max-age=300, s-maxage=300";

/**
 * The tip counts its day from the park's today ("tomorrow", "on Saturday"), so a cached copy must
 * not outlive park-local midnight; only a window that ends before it may be served stale.
 */
export function tipCacheControl(secondsLeftToday: number): string {
  const seconds = Math.min(TIP_MAX_AGE_SECONDS, secondsLeftToday);
  return seconds < TIP_MAX_AGE_SECONDS
    ? `public, max-age=${seconds}, s-maxage=${seconds}`
    : `public, max-age=${seconds}, s-maxage=${seconds}, stale-while-revalidate=3600`;
}

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
      "entries for the day and three days on each side, a verdict from the day's crowd " +
      "level (very_low/low good, moderate meh, high and above bad), and that level " +
      "(`crowdLevel`), as the calendar has it now: once the forecast moves the day or one of " +
      "the days around it, the sentence is written again. 204 when " +
      `there is nothing to say: a closed day, no forecast, a date before today or more than ${PARKY_TIP_HORIZON_DAYS} ` +
      "days ahead in park time, Parky switched off or the day's token budget spent; " +
      "cached five minutes. The sentence names days counted from the park's today (today, " +
      "tomorrow, Saturday): a `today` that is not the park's answers 204 with no-store and " +
      "writes nothing. Cache: public, max-age=21600, stale-while-revalidate=3600, cut to " +
      "park-local midnight.",
  })
  @ApiParkPath()
  @ApiQuery({
    name: "date",
    required: true,
    description: "The day (YYYY-MM-DD) in the park's timezone.",
    example: "2026-12-05",
  })
  @ApiQuery({ name: "locale", required: true, enum: ASSISTANT_LOCALES })
  @ApiQuery({
    name: "today",
    required: false,
    description:
      "The park's today (YYYY-MM-DD) as the caller counts it, where the caller keys its own " +
      "cache by it. Any other day than the park's answers 204 with no-store.",
    example: "2026-11-17",
  })
  @ApiResponse({
    status: 200,
    description:
      "`{ date, verdict: good|meh|bad, crowdLevel: very_low|low|moderate|high|very_high|extreme, todayCrowdLevel?, text, promptVersion }`; " +
      "`todayCrowdLevel` only on the tip for today, the level so far its sentence was written from.",
  })
  @ApiResponse({ status: 204, description: "Nothing to say for this day" })
  @ApiResponse({ status: 400, description: "Invalid date, today or locale" })
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
    @Query("today") asked: string | undefined,
    @Res({ passthrough: true }) res: Response,
  ): Promise<ParkyTip | undefined> {
    if (!isRealIsoDate(date)) {
      throw new BadRequestException("date must be a real YYYY-MM-DD day");
    }
    if (asked !== undefined && !isRealIsoDate(asked)) {
      throw new BadRequestException("today must be a real YYYY-MM-DD day");
    }
    const park = await this.moduleRef
      .get(ParksService, { strict: false })
      .findByGeographicPath(continent, country, city, parkSlug);
    if (!park) throw parkNotFound(continent, country, city, parkSlug);
    const today = getCurrentDateInTimezone(park.timezone);
    // A caller whose clock is already past the park's midnight, or not there yet, would keep a
    // sentence counted from one day ("tomorrow …") under the other day's key.
    if (asked !== undefined && asked !== today) {
      res.status(204);
      res.setHeader("Cache-Control", "no-store");
      return undefined;
    }
    const tip = await this.tips.tip(park, date, locale, today);
    if (!tip) {
      res.status(204);
      res.setHeader("Cache-Control", TIP_NONE_CACHE_CONTROL);
      return undefined;
    }
    res.setHeader(
      "Cache-Control",
      tipCacheControl(secondsUntilEndOfDayInTimezone(park.timezone)),
    );
    return tip;
  }
}
