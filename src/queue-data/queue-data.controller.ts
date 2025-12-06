import {
  Controller,
  Get,
  Param,
  Query,
  NotFoundException,
  BadRequestException,
} from "@nestjs/common";
import { QueueDataService } from "./queue-data.service";
import { AttractionsService } from "../attractions/attractions.service";
import { ParksService } from "../parks/parks.service";
import { WaitTimesResponseDto } from "./dto/wait-times-response.dto";
import { StatusResponseDto } from "./dto/status-response.dto";
import {
  ForecastResponseDto,
  ForecastItemDto,
} from "./dto/forecast-response.dto";
import { QueueDataItemDto } from "./dto/queue-data-item.dto";
import { PaginationDto } from "../common/dto/pagination.dto";
import { QueueType } from "../external-apis/themeparks/themeparks.types";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";

/**
 * Queue Data Controller
 *
 * Handles endpoints for wait times, forecasts, and real-time status
 */
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
        limit: limit ? parseInt(String(limit)) : 50,
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
      waitTimes: waitTimes.map(this.mapQueueDataToDto),
      pagination: new PaginationDto(
        page ? parseInt(String(page)) : 1,
        limit ? parseInt(String(limit)) : 50,
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
      queues: currentQueues.map((q) => ({
        queueType: q.queueType,
        status: q.status,
        waitTime: q.waitTime ?? undefined,
        state: q.state ?? undefined,
        returnStart: q.returnStart ? q.returnStart.toISOString() : undefined,
        returnEnd: q.returnEnd ? q.returnEnd.toISOString() : undefined,
        price: q.price ?? undefined,
        allocationStatus: q.allocationStatus ?? undefined,
        currentGroupStart: q.currentGroupStart ?? undefined,
        currentGroupEnd: q.currentGroupEnd ?? undefined,
        estimatedWait: q.estimatedWait ?? undefined,
      })),
      lastUpdated: lastUpdated.toISOString(),
    };

    return response;
  }

  /**
   * GET /v1/attractions/:slug/forecasts
   *
   * Returns wait time predictions for an attraction
   */
  @Get("attractions/:slug/forecasts")
  async getForecasts(
    @Param("slug") slug: string,
    @Query("hours") hours?: number,
  ): Promise<ForecastResponseDto> {
    // Find attraction
    const attraction = await this.attractionsService.findBySlug(slug);
    if (!attraction) {
      throw new NotFoundException(`Attraction with slug "${slug}" not found`);
    }

    // Parse hours parameter
    const hoursAhead = hours ? parseInt(String(hours)) : 24;
    if (isNaN(hoursAhead) || hoursAhead < 1 || hoursAhead > 168) {
      throw new BadRequestException(
        'Invalid "hours" parameter. Must be between 1 and 168.',
      );
    }

    // Query forecasts
    const forecasts = await this.queueDataService.findForecastsByAttraction(
      attraction.id,
      hoursAhead,
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
      forecasts: forecasts.map(this.mapForecastToDto),
    };

    return response;
  }

  /**
   * GET /v1/parks/:slug/wait-times
   *
   * Returns current wait times for all attractions in a park
   */
  @Get("parks/:slug/wait-times")
  async getParkWaitTimes(
    @Param("slug") slug: string,
    @Query("queueType") queueType?: QueueType,
  ): Promise<{ park: any; attractions: any[] }> {
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
    const attractionsMap = new Map<string, any>();

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
        .queues.push(this.mapQueueDataToDto(queueData));
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
   * Map QueueData entity to DTO
   */
  private mapQueueDataToDto(queueData: QueueData): QueueDataItemDto {
    return {
      queueType: queueData.queueType,
      status: queueData.status,
      waitTime: queueData.waitTime ?? null,
      state: queueData.state ?? null,
      returnStart: queueData.returnStart
        ? queueData.returnStart.toISOString()
        : null,
      returnEnd: queueData.returnEnd ? queueData.returnEnd.toISOString() : null,
      price: queueData.price ?? null,
      allocationStatus: queueData.allocationStatus ?? null,
      currentGroupStart: queueData.currentGroupStart ?? null,
      currentGroupEnd: queueData.currentGroupEnd ?? null,
      estimatedWait: queueData.estimatedWait ?? null,
      lastUpdated: (queueData.lastUpdated || queueData.timestamp).toISOString(),
      timestamp: queueData.timestamp.toISOString(),
    };
  }

  /**
   * Map ForecastData entity to DTO
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
