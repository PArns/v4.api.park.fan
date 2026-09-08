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
import { frontendShowsPath } from "../common/utils/frontend-url.util";
import { PushFollowAccessGuard } from "../push/push-follow-access.guard";
import {
  ShowFollowsService,
  MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION,
} from "./show-follows.service";
import { Show } from "../shows/entities/show.entity";
import { ShowFollow } from "./entities/show-follow.entity";
import {
  CreateShowFollowDto,
  DeleteShowFollowDto,
  ShowFollowResponseDto,
} from "./dto/show-follow.dto";

/**
 * A visitor's followed shows — same anonymous shape as `ride-alerts`: the
 * endpoint is the identity, and every write is rate-limited by
 * `PushFollowWriteRateLimitService` for the reason documented there.
 */
@ApiTags("show-follows")
@Controller("push/show-follows")
@UseInterceptors(new NoCdnCacheInterceptor())
export class ShowFollowsController {
  constructor(
    private readonly showFollows: ShowFollowsService,
    private readonly access: PushFollowAccessGuard,
  ) {}

  @Get()
  @ApiOperation({
    summary: "A browser's followed shows",
    description: "404 when the endpoint has no push subscription at all.",
  })
  @ApiQuery({ name: "endpoint", required: true })
  @ApiResponse({ status: 200, type: [ShowFollowResponseDto] })
  @ApiResponse({
    status: 404,
    description: "No subscription for this endpoint.",
  })
  async list(
    @Query("endpoint") endpoint: string,
  ): Promise<ShowFollowResponseDto[]> {
    const subscription = await this.access.subscriptionOrThrow(endpoint);
    const follows = await this.showFollows.listForSubscription(subscription.id);
    return follows.map((follow) =>
      ShowFollowsController.present(follow, follow.show),
    );
  }

  @Post()
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Follow a show",
    description:
      "An upsert on (endpoint, showId) — following an already-followed show " +
      "is a no-op.",
  })
  @ApiResponse({ status: 200, type: ShowFollowResponseDto })
  @ApiResponse({
    status: 404,
    description: "No subscription for this endpoint, or no such show.",
  })
  @ApiResponse({
    status: 429,
    description: "Too many writes from this address.",
  })
  async create(
    @Body() body: CreateShowFollowDto,
    @Req() request: Request,
  ): Promise<ShowFollowResponseDto> {
    await this.access.writeGuard(request, "show-follow");
    const subscription = await this.access.subscriptionOrThrow(body?.endpoint);

    const found = await this.showFollows.findShowForFollow(body?.showId);
    if (!found) {
      throw new HttpException("Show not found", HttpStatus.NOT_FOUND);
    }

    const existing = await this.showFollows.find(
      subscription.id,
      found.show.id,
    );
    if (!existing) {
      const count = await this.showFollows.countForSubscription(
        subscription.id,
      );
      if (count >= MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION) {
        throw new BadRequestException(
          `A browser may follow at most ${MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION} shows`,
        );
      }
    }

    const follow = await this.showFollows.upsert(
      subscription.id,
      found.show.id,
    );
    return ShowFollowsController.present(follow, found.show);
  }

  @Delete()
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({
    summary: "Unfollow a show",
    description:
      "Idempotent — unfollowing a show that is not followed is not an error.",
  })
  @ApiResponse({ status: 204 })
  async remove(
    @Body() body: DeleteShowFollowDto,
    @Req() request: Request,
  ): Promise<void> {
    await this.access.writeGuard(request, "show-follow");
    const subscription = await this.access.subscriptionOrThrow(body?.endpoint);
    await this.showFollows.remove(subscription.id, body?.showId);
  }

  private static present(
    follow: ShowFollow,
    show: Show,
  ): ShowFollowResponseDto {
    const park = show.park;
    return {
      showId: follow.showId,
      showName: show.name,
      showSlug: show.slug,
      parkId: park.id,
      parkName: park.name,
      parkSlug: park.slug,
      path: frontendShowsPath(park),
      createdAt: follow.createdAt.toISOString(),
    };
  }
}
