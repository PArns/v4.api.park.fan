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
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { frontendAttractionPath } from "../common/utils/frontend-url.util";
import { getNoLiveWaitTimesReason } from "../parks/data/live-wait-time-sources";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../attractions/utils/curated-attraction-facts.util";
import { PushFollowAccessGuard } from "../push/push-follow-access.guard";
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
    private readonly access: PushFollowAccessGuard,
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
    const subscription = await this.access.subscriptionOrThrow(endpoint);
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
    await this.access.writeGuard(request, "ride-alert");
    const subscription = await this.access.subscriptionOrThrow(body?.endpoint);

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
    await this.access.writeGuard(request, "ride-alert");
    const subscription = await this.access.subscriptionOrThrow(body?.endpoint);
    await this.rideAlerts.remove(subscription.id, body?.attractionId);
  }

  private static present(
    alert: RideAlert,
    attraction: Attraction,
  ): RideAlertResponseDto {
    const park = attraction.park;
    // The same curated name the notification itself uses
    // (`RideAlertsService.checkAndNotify` reads it through the identical
    // helper) — the raw `attraction.name` would name a curated ride
    // differently in the list than in the banner it is about.
    const facts = resolveCuratedFacts(attraction);
    return {
      attractionId: alert.attractionId,
      attractionName: facts.name,
      attractionSlug: attraction.slug,
      parkId: park.id,
      parkName: park.name,
      parkSlug: park.slug,
      path: frontendAttractionPath(park, { slug: attraction.slug }),
      // Accepted at write time regardless (a visitor may reasonably alert on
      // a winter ride in August, ahead of a trip) — see `create`'s own
      // comment. Surfaced here so the caller can say so rather than let a
      // dormant alert look identical to a live one.
      outOfSeason: isCurrentlyInSeason(facts) === false,
      // A ride retired AFTER this alert was created — `findAttractionForAlert`
      // refuses a NEW alert on one already retired, but nothing removes an
      // existing alert when its ride is retired later, and the FK cascade
      // only fires if the attraction row itself is deleted, which retirement
      // is not. Same "surface it rather than let it look live" reasoning.
      retired: attraction.retiredAt !== null,
      thresholdMinutes: alert.thresholdMinutes,
      armed: alert.armed,
      createdAt: alert.createdAt.toISOString(),
    };
  }
}
