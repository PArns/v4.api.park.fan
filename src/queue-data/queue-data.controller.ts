import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
  BadRequestException,
  UseInterceptors,
} from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from "@nestjs/swagger";
import { QueueDataService } from "./queue-data.service";
import { AttractionsService } from "../attractions/attractions.service";
import { ParksService } from "../parks/parks.service";
import { HttpCacheInterceptor } from "../common/interceptors/cache.interceptor";
import { WaitTimesResponseDto } from "./dto/wait-times-response.dto";
import { StatusResponseDto } from "./dto/status-response.dto";
import {
  ForecastResponseDto,
  ForecastItemDto,
} from "./dto/forecast-response.dto";
import {
  ParkWaitTimesResponseDto,
  AttractionWaitTimesDto,
} from "./dto/park-wait-times-response.dto";
import { QueueDataItemDto } from "./dto/queue-data-item.dto";
import { PaginationDto } from "../common/dto/pagination.dto";
import { QueueType } from "../external-apis/themeparks/themeparks.types";
import { ForecastData } from "./entities/forecast-data.entity";

/**
 * Queue Data Controller
 *
 * Handles endpoints for wait times, forecasts, and real-time status
 */
@ApiTags("queue-data")
@Controller()
export class QueueDataController {
  constructor(
    private readonly queueDataService: QueueDataService,
    private readonly attractionsService: AttractionsService,
    private readonly parksService: ParksService,
  ) {}

  /**
   * GET /v1/attractions/:slug/wait-times
   *
   * Returns historical and current wait time data for an attraction
   */
  @Get("attractions/:slug/wait-times")
  @ApiOperation({
    summary: "Get attraction wait times",
    description:
      "Returns historical and current wait time data for a specific attraction.",
  })
  @ApiParam({
    name: "slug",
    description: "Attraction slug",
    example: "space-mountain",
  })
  @ApiResponse({
    status: 200,
    description: "Wait times retrieved successfully",
    type: WaitTimesResponseDto,
  })
  @ApiResponse({ status: 404, description: "Attraction not found" })
  async getWaitTimes(
    @Param("slug") slug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
    @Query("queueType") queueType?: QueueType,
    @Query("page") page?: number,
    @Query("limit") limit?: number,
  ): Promise<WaitTimesResponseDto> {
    // Find attraction
    const attraction = await this.attractionsService.findBySlug(slug);
    if (!attraction) {
      throw new NotFoundException(`Attraction with slug "${slug}" not found`);
    }

    // Parse date parameters
    let fromDate: Date | undefined;
    let toDate: Date | undefined;

    if (from) {
      fromDate = new Date(from);
      if (isNaN(fromDate.getTime())) {
        throw new BadRequestException(
          'Invalid "from" date format. Use ISO 8601.',
        );
      }
    }

    if (to) {
      toDate = new Date(to);
      if (isNaN(toDate.getTime())) {
        throw new BadRequestException(
          'Invalid "to" date format. Use ISO 8601.',
        );
      }
    }

    // Query wait times
    const { data: waitTimes, total } =
      await this.queueDataService.findWaitTimesByAttraction(attraction.id, {
        from: fromDate,
        to: toDate,
        queueType,
        page: page ? parseInt(String(page)) : 1,
        limit: limit ? parseInt(String(limit)) : 10,
      });

    // Build response
    const response: WaitTimesResponseDto = {
      attraction: {
        id: attraction.id,
        name: attraction.name,
        slug: attraction.slug,
      },
      park: {
        id: attraction.park.id,
        name: attraction.park.name,
        slug: attraction.park.slug,
        timezone: attraction.park.timezone,
      },
      waitTimes: waitTimes.map((qd) => QueueDataItemDto.fromEntity(qd)),
      pagination: new PaginationDto(
        page ? parseInt(String(page)) : 1,
        limit ? parseInt(String(limit)) : 10,
        total,
      ),
    };

    return response;
  }

  /**
   * GET /v1/attractions/:slug/status
   *
   * Returns current real-time status for an attraction
   */
  @Get("attractions/:slug/status")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live status
  @ApiOperation({
    summary: "Get attraction live status",
    description:
      "Returns current real-time status only (no historical data) for an attraction.",
  })
  @ApiParam({
    name: "slug",
    description: "Attraction slug",
    example: "space-mountain",
  })
  @ApiResponse({
    status: 200,
    description: "Status retrieved successfully",
    type: StatusResponseDto,
  })
  @ApiResponse({ status: 404, description: "Attraction not found" })
  async getStatus(@Param("slug") slug: string): Promise<StatusResponseDto> {
    // Find attraction
    const attraction = await this.attractionsService.findBySlug(slug);
    if (!attraction) {
      throw new NotFoundException(`Attraction with slug "${slug}" not found`);
    }

    // Get current status
    const currentQueues =
      await this.queueDataService.findCurrentStatusByAttraction(attraction.id);

    if (currentQueues.length === 0) {
      throw new NotFoundException(`No status data available for "${slug}"`);
    }

    // Determine overall status (use the first queue's status as overall)
    const overallStatus = currentQueues[0].status;
    const lastUpdated =
      currentQueues[0].lastUpdated || currentQueues[0].timestamp;

    // Build response
    const response: StatusResponseDto = {
      attraction: {
        id: attraction.id,
        name: attraction.name,
        slug: attraction.slug,
      },
      park: {
        id: attraction.park.id,
        name: attraction.park.name,
        slug: attraction.park.slug,
        timezone: attraction.park.timezone,
      },
      status: overallStatus,
      queues: currentQueues.map((qd) => QueueDataItemDto.fromEntity(qd)),
      lastUpdated: lastUpdated.toISOString(),
    };

    return response;
  }

  /**
   * GET /v1/attractions/:slug/forecast
   *
   * Get wait time forecasts for a specific attraction
   */
  @Get("attractions/:slug/forecast")
  @UseInterceptors(new HttpCacheInterceptor(120))
  @ApiOperation({
    summary: "Get attraction forecasts",
    description:
      "Returns future wait time predictions for a specific attraction.",
  })
  @ApiParam({
    name: "slug",
    description: "Attraction slug",
    example: "space-mountain",
  })
  @ApiResponse({
    status: 200,
    description: "Forecasts retrieved successfully",
    type: ForecastResponseDto,
  })
  @ApiResponse({ status: 404, description: "Attraction not found" })
  async getAttractionForecast(
    @Param("slug") slug: string,
    @Query("hours") hoursParam?: string,
  ): Promise<ForecastResponseDto> {
    const attraction = await this.attractionsService.findBySlug(slug);

    if (!attraction) {
      throw new NotFoundException(`Attraction with slug "${slug}" not found`);
    }

    // Parse hours param for forecast horizon (default: 24)
    const hours = hoursParam ? parseInt(hoursParam) : 24;

    if (hours < 1 || hours > 168) {
      throw new BadRequestException("Hours must be between 1 and 168 (1 week)");
    }

    // Get forecasts
    const forecasts = await this.queueDataService.findForecastsByAttraction(
      attraction.id,
      hours,
    );

    // Build response
    const response: ForecastResponseDto = {
      attraction: {
        id: attraction.id,
        name: attraction.name,
        slug: attraction.slug,
      },
      park: {
        id: attraction.park.id,
        name: attraction.park.name,
        slug: attraction.park.slug,
        timezone: attraction.park.timezone,
      },
      forecasts: forecasts.map((f) => this.mapForecastToDto(f)),
    };

    return response;
  }

  /**
   * GET /v1/parks/:slug/wait-times
   *
   * Get current wait times for all attractions in a park (grouped by attraction)
   * NOTE: This endpoint has park status checking - querying closed parks returns empty wait times
   */
  @Get("parks/:slug/wait-times")
  @UseInterceptors(new HttpCacheInterceptor(120)) // 2 minutes - live wait times
  @ApiOperation({
    summary: "Get park wait times",
    description:
      "Returns current wait times for all attractions in a park, grouped by attraction. " +
      "Closed parks return empty wait times. Cached for 2 minutes.",
  })
  @ApiParam({
    name: "slug",
    description: "Park slug",
    example: "magic-kingdom",
  })
  @ApiResponse({
    status: 200,
    description: "Wait times retrieved successfully",
    type: ParkWaitTimesResponseDto,
  })
  @ApiResponse({ status: 404, description: "Park not found" })
  async getParkWaitTimes(
    @Param("slug") slug: string,
    @Query("queueType") queueType?: QueueType,
  ): Promise<ParkWaitTimesResponseDto> {
    // Find park
    const park = await this.parksService.findBySlug(slug);
    if (!park) {
      throw new NotFoundException(`Park with slug "${slug}" not found`);
    }

    // Get wait times for all attractions
    const waitTimes = await this.queueDataService.findWaitTimesByPark(
      park.id,
      queueType,
    );

    // Group by attraction
    const attractionsMap = new Map<string, AttractionWaitTimesDto>();

    for (const queueData of waitTimes) {
      const attractionId = queueData.attraction.id;

      if (!attractionsMap.has(attractionId)) {
        attractionsMap.set(attractionId, {
          attraction: {
            id: queueData.attraction.id,
            name: queueData.attraction.name,
            slug: queueData.attraction.slug,
          },
          queues: [],
        });
      }

      attractionsMap
        .get(attractionId)!
        .queues.push(QueueDataItemDto.fromEntity(queueData));
    }

    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
      },
      attractions: Array.from(attractionsMap.values()),
    };
  }

  /**
   * Map Forecast Data entity to DTO
   */
  private mapForecastToDto(forecast: ForecastData): ForecastItemDto {
    return {
      predictedTime: forecast.predictedTime.toISOString(),
      predictedWaitTime: forecast.predictedWaitTime,
      confidencePercentage: forecast.confidencePercentage,
      source: forecast.source,
    };
  }
}
