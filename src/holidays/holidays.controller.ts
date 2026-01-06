import {
  Controller,
  Get,
  Query,
  Param,
  NotFoundException,
  BadRequestException,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { HolidaysService } from "./holidays.service";
import { ParksService } from "../parks/parks.service";
import { HolidayResponseDto } from "./dto/holiday-response.dto";
import { HolidayItemDto } from "./dto/holiday-item.dto";

/**
 * Holidays Controller
 *
 * Provides REST API endpoints for accessing holiday data.
 *
 * Endpoints:
 * - GET /holidays - Query holidays by country and date range
 * - GET /parks/:slug/holidays - Get holidays for a specific park's country
 */
@ApiTags("holidays")
@Controller()
export class HolidaysController {
  constructor(
    private readonly holidaysService: HolidaysService,
    private readonly parksService: ParksService,
  ) {}

  /**
   * GET /v1/holidays
   *
   * Query holidays by country, date range, region, and type.
   *
   * @param country - ISO 3166-1 alpha-2 code (e.g., "US", "GB") - Optional
   * @param from - Start date (YYYY-MM-DD, optional, defaults to today)
   * @param to - End date (YYYY-MM-DD, optional, defaults to 1 year ahead)
   * @param region - Region filter (optional, e.g., "US-FL")
   * @param type - Holiday type filter (optional: public, observance, school, bank)
   * @param limit - Max number of results (optional, default: 100, max: 1000)
   * @throws BadRequestException if parameters invalid
   */
  @Get("holidays")
  @ApiOperation({
    summary: "Query holidays",
    description:
      "Retrieve holidays filtering by country, date range, region, and type.",
  })
  @ApiResponse({
    status: 200,
    description: "Holidays retrieved successfully",
    type: HolidayResponseDto,
  })
  async getHolidays(
    @Query("country") country?: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
    @Query("region") region?: string,
    @Query("type") type?: "public" | "observance" | "school" | "bank",
    @Query("limit") limit?: number,
  ): Promise<HolidayResponseDto> {
    // Validate country code format if provided
    if (country && !/^[A-Z]{2}$/.test(country)) {
      throw new BadRequestException(
        'Country must be a valid ISO 3166-1 alpha-2 code (e.g., "US", "GB")',
      );
    }

    // Parse limit
    const resultLimit = limit ? Math.min(parseInt(String(limit)), 1000) : 100;

    // Parse date range with timezone awareness
    // Note: Holidays are country-specific, not park-specific
    // We use UTC as default timezone since holidays are calendar-based
    const { parseDateRange } =
      await import("../common/utils/date-parsing.util");
    const { fromDate, toDate } = parseDateRange(from, to, {
      timezone: "UTC", // Holidays are calendar dates, not park-specific
      defaultFromDaysAgo: 0,
      defaultToDaysAhead: 365, // 1 year
    });

    // Get holidays from service
    let holidays = country
      ? await this.holidaysService.getHolidays(country, fromDate, toDate)
      : await this.holidaysService.getAllHolidays(fromDate, toDate);

    // Apply region filter if specified
    if (region) {
      holidays = holidays.filter(
        (h) => h.region === region || h.region === null,
      );
    }

    // Apply type filter if specified
    if (type) {
      holidays = holidays.filter((h) => h.holidayType === type);
    }

    // Apply limit
    holidays = holidays.slice(0, resultLimit);

    return {
      holidays: holidays.map((h) => HolidayItemDto.fromEntity(h)),
    };
  }

  /**
   * GET /v1/parks/:slug/holidays
   *
   * Get holidays for a specific park's country.
   * Region is automatically injected from park data.
   *
   * @param slug - Park slug
   * @param year - Year (optional, defaults to current year)
   * @throws NotFoundException if park not found
   * @throws BadRequestException if park has no country data
   */
  @Get("parks/:slug/holidays")
  @ApiOperation({
    summary: "Get holidays for a park",
    description: "Returns holidays for the country where the park is located.",
  })
  @ApiResponse({
    status: 200,
    description: "Holidays retrieved successfully",
    type: HolidayResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getParkHolidays(
    @Param("slug") slug: string,
    @Query("year") year?: number,
  ): Promise<HolidayResponseDto> {
    const park = await this.parksService.findBySlug(slug);

    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    if (!park.countryCode) {
      throw new BadRequestException(
        `Park "${park.name}" has no country code data available`,
      );
    }

    // Parse year parameter
    const targetYear = year ? parseInt(String(year)) : new Date().getFullYear();

    if (isNaN(targetYear) || targetYear < 1900 || targetYear > 2100) {
      throw new BadRequestException("Invalid year parameter");
    }

    // Date range: full year
    const fromDate = new Date(targetYear, 0, 1); // January 1st
    const toDate = new Date(targetYear, 11, 31, 23, 59, 59, 999); // December 31st

    // Get holidays for park's country
    let holidays = await this.holidaysService.getHolidays(
      park.countryCode,
      fromDate,
      toDate,
    );

    // Filter by Region if available (e.g., DE-BW for Europa-Park)
    // Nager.Date returns region specific holidays with a region code
    if (park.regionCode) {
      const parkRegion = park.regionCode; // e.g., "DE-BW"
      holidays = holidays.filter(
        (h) => h.isNationwide || h.region === parkRegion,
      );
    }

    return {
      holidays: holidays.map((h) => HolidayItemDto.fromEntity(h)),
    };
  }
}
