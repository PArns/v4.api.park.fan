import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  Post,
  Query,
  Req,
  UseInterceptors,
} from "@nestjs/common";
import { ApiOperation, ApiQuery, ApiResponse, ApiTags } from "@nestjs/swagger";
import { Request } from "express";
import { getClientIp } from "../common/utils/request.util";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { frontendAttractionPath } from "../common/utils/frontend-url.util";
import { getNoLiveWaitTimesReason } from "../parks/data/live-wait-time-sources";
import { PushService } from "../push/push.service";
import { PushFollowWriteRateLimitService } from "../push/push-follow-write-rate-limit.service";
import {
  RideAlertsService,
  MAX_RIDE_ALERTS_PER_SUBSCRIPTION,
} from "./ride-alerts.service";
import { RideAlert } from "./entities/ride-alert.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import {
  CreateRideAlertDto,
  DeleteRideAlertDto,
  RideAlertResponseDto,
} from "./dto/ride-alert.dto";

/**
 * A visitor's wait-time alerts — which attraction, and how many minutes.
 *
 * Same anonymous shape as `push` and `trips`: the endpoint IS the identity,
 * there is no account to check against, and every write is rate-limited by
 * `PushFollowWriteRateLimitService` because the global throttler is bypassed
 * for our own frontend, which is where every real write comes from.
 */
@ApiTags("ride-alerts")
@Controller("push/ride-alerts")
@UseInterceptors(new NoCdnCacheInterceptor())
export class RideAlertsController {
  constructor(
    private readonly rideAlerts: RideAlertsService,
    private readonly pushService: PushService,
    private readonly rateLimit: PushFollowWriteRateLimitService,
  ) {}

  @Get()
  @ApiOperation({
    summary: "A browser's ride alerts",
    description: "404 when the endpoint has no push subscription at all.",
  })
  @ApiQuery({ name: "endpoint", required: true })
  @ApiResponse({ status: 200, type: [RideAlertResponseDto] })
  @ApiResponse({
    status: 404,
    description: "No subscription for this endpoint.",
  })
  async list(
    @Query("endpoint") endpoint: string,
  ): Promise<RideAlertResponseDto[]> {
    const subscription = await this.subscriptionOrThrow(endpoint);
    const alerts = await this.rideAlerts.listForSubscription(subscription.id);
    return alerts.map((alert) =>
      RideAlertsController.present(alert, alert.attraction),
    );
  }

  @Post()
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Watch an attraction's wait time",
    description:
      "An upsert on (endpoint, attractionId): re-sending for a ride already " +
      "watched replaces the threshold and re-arms the alert.",
  })
  @ApiResponse({ status: 200, type: RideAlertResponseDto })
  @ApiResponse({
    status: 400,
    description:
      "Malformed threshold, or this park's wait times can never be read " +
      "(see `noLiveWaitTimesReason`) — an alert against it could never fire.",
  })
  @ApiResponse({
    status: 404,
    description: "No subscription for this endpoint, or no such attraction.",
  })
  @ApiResponse({
    status: 429,
    description: "Too many writes from this address.",
  })
  async create(
    @Body() body: CreateRideAlertDto,
    @Req() request: Request,
  ): Promise<RideAlertResponseDto> {
    await this.guard(request);
    const subscription = await this.subscriptionOrThrow(body?.endpoint);

    const found = await this.rideAlerts.findAttractionForAlert(
      body?.attractionId,
    );
    if (!found) {
      throw new HttpException("Attraction not found", HttpStatus.NOT_FOUND);
    }

    // A ride whose park we cannot read wait times for will never satisfy any
    // threshold — the same "never accept a subscription that can never
    // notify" rule the push and trip endpoints already apply.
    const unreadable = getNoLiveWaitTimesReason(
      found.park.citySlug,
      found.park.slug,
    );
    if (unreadable) {
      throw new BadRequestException(
        `This park's wait times cannot be read (${unreadable}) — an alert here would never fire`,
      );
    }

    const existing = await this.rideAlerts.find(
      subscription.id,
      found.attraction.id,
    );
    if (!existing) {
      const count = await this.rideAlerts.countForSubscription(subscription.id);
      if (count >= MAX_RIDE_ALERTS_PER_SUBSCRIPTION) {
        throw new BadRequestException(
          `A browser may watch at most ${MAX_RIDE_ALERTS_PER_SUBSCRIPTION} rides`,
        );
      }
    }

    const alert = await this.rideAlerts.upsert(
      subscription.id,
      found.attraction.id,
      body.thresholdMinutes,
    );
    return RideAlertsController.present(alert, found.attraction);
  }

  @Delete()
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Stop watching an attraction",
    description:
      "Idempotent — removing an alert that is not there is not an error.",
  })
  @ApiResponse({ status: 204 })
  async remove(
    @Body() body: DeleteRideAlertDto,
    @Req() request: Request,
  ): Promise<void> {
    await this.guard(request);
    const subscription = await this.subscriptionOrThrow(body?.endpoint);
    await this.rideAlerts.remove(subscription.id, body?.attractionId);
  }

  private async subscriptionOrThrow(endpoint: string) {
    const subscription = await this.pushService.findByEndpoint(endpoint);
    if (!subscription) {
      throw new HttpException(
        "No push subscription for this endpoint",
        HttpStatus.NOT_FOUND,
      );
    }
    return subscription;
  }

  private async guard(request: Request): Promise<void> {
    const verdict = await this.rateLimit.check(
      getClientIp(request),
      "ride-alert",
    );
    if (verdict.allowed) return;
    throw new HttpException(
      {
        statusCode: HttpStatus.TOO_MANY_REQUESTS,
        message: "Too many ride-alert writes from this address",
        retryAfterSeconds: verdict.retryAfterSeconds,
      },
      HttpStatus.TOO_MANY_REQUESTS,
    );
  }

  private static present(
    alert: RideAlert,
    attraction: Attraction,
  ): RideAlertResponseDto {
    const park = attraction.park;
    return {
      attractionId: alert.attractionId,
      attractionName: attraction.name,
      attractionSlug: attraction.slug,
      parkId: park.id,
      parkName: park.name,
      parkSlug: park.slug,
      path: frontendAttractionPath(park, { slug: attraction.slug }),
      thresholdMinutes: alert.thresholdMinutes,
      armed: alert.armed,
      createdAt: alert.createdAt.toISOString(),
    };
  }
}
