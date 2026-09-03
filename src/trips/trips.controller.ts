import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  Param,
  Post,
  Put,
  Req,
  UseInterceptors,
} from "@nestjs/common";
import { ApiOperation, ApiParam, ApiResponse, ApiTags } from "@nestjs/swagger";
import { Request } from "express";
import { getClientIp } from "../common/utils/request.util";
import { NoCdnCacheInterceptor } from "../common/interceptors/no-cdn-cache.interceptor";
import { TripsService } from "./trips.service";
import { TripWriteRateLimitService } from "./trip-write-rate-limit.service";
import { checkTripPayload } from "./trip-payload.util";
import { TripResponseDto, TripWriteDto } from "./dto/trip.dto";
import { Trip } from "./entities/trip.entity";

/**
 * Stored plans.
 *
 * The first unauthenticated write surface in this API, and everything unusual
 * about it follows from that:
 *
 *  - The id is the credential (see `TripsService`), so there is no owner check
 *    anywhere in this file and there is nothing to check against.
 *  - Writes are counted by `TripWriteRateLimitService` rather than by
 *    `@Throttle`, because the global guard skips our own frontend and the
 *    frontend is where every real write comes from.
 *  - The payload has to look like a plan and has to be small, or this is a file
 *    host with a good domain in front of it.
 *
 * Nothing here is CDN-cacheable. A trip is one visitor's, changes on their
 * schedule, and a shared edge copy would hand the next reader somebody else's
 * plan or an old version of their own.
 */
@ApiTags("trips")
@Controller("trips")
@UseInterceptors(new NoCdnCacheInterceptor())
export class TripsController {
  constructor(
    private readonly tripsService: TripsService,
    private readonly rateLimit: TripWriteRateLimitService,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Store a plan",
    description:
      "Returns the trip's id, which is its only credential — anyone holding it " +
      "can read and overwrite the trip. Rate-limited per address.",
  })
  @ApiResponse({ status: 201, type: TripResponseDto })
  @ApiResponse({
    status: 400,
    description: "The payload is not a plan, or is too large.",
  })
  @ApiResponse({
    status: 429,
    description: "Too many trips created from this address.",
  })
  async create(
    @Body() body: TripWriteDto,
    @Req() request: Request,
  ): Promise<TripResponseDto> {
    await this.guard(request, "create");
    const payload = this.validated(body);
    return TripsController.present(await this.tripsService.create(payload));
  }

  @Get(":id")
  @ApiOperation({
    summary: "Read a stored plan",
    description:
      "404 for an id that does not exist AND for one that has expired: whether " +
      "the sweep has run yet is not the caller's business, and a trip that " +
      "answers differently on two days for the same reason is worse than one " +
      "that is simply gone.",
  })
  @ApiParam({ name: "id", example: "n7Qk2Fd3Xb9pLmZa" })
  @ApiResponse({ status: 200, type: TripResponseDto })
  @ApiResponse({ status: 404, description: "No such trip." })
  async read(@Param("id") id: string): Promise<TripResponseDto> {
    const trip = await this.tripsService.find(TripsController.tripId(id));
    if (!trip) throw new HttpException("Trip not found", HttpStatus.NOT_FOUND);
    return TripsController.present(trip);
  }

  @Put(":id")
  @ApiOperation({
    summary: "Replace a stored plan",
    description:
      "A full replace, never a merge: the browser holds the whole plan and is " +
      "the only writer that knows what was deleted. 404 rather than creating a " +
      "trip at a caller-chosen id — that would let an attacker pick their own " +
      "ids, and with them overwrite a trip by guessing one.",
  })
  @ApiParam({ name: "id", example: "n7Qk2Fd3Xb9pLmZa" })
  @ApiResponse({ status: 200, type: TripResponseDto })
  @ApiResponse({
    status: 400,
    description: "The payload is not a plan, or is too large.",
  })
  @ApiResponse({ status: 404, description: "No such trip." })
  @ApiResponse({
    status: 429,
    description: "Too many writes from this address.",
  })
  async update(
    @Param("id") id: string,
    @Body() body: TripWriteDto,
    @Req() request: Request,
  ): Promise<TripResponseDto> {
    await this.guard(request, "update");
    const payload = this.validated(body);
    const trip = await this.tripsService.update(
      TripsController.tripId(id),
      payload,
    );
    if (!trip) throw new HttpException("Trip not found", HttpStatus.NOT_FOUND);
    return TripsController.present(trip);
  }

  /** The limiter, before any work. Throws 429 with a `Retry-After` figure. */
  private async guard(
    request: Request,
    kind: "create" | "update",
  ): Promise<void> {
    const verdict = await this.rateLimit.check(getClientIp(request), kind);
    if (verdict.allowed) return;
    throw new HttpException(
      {
        statusCode: HttpStatus.TOO_MANY_REQUESTS,
        message: "Too many trip writes from this address",
        retryAfterSeconds: verdict.retryAfterSeconds,
      },
      HttpStatus.TOO_MANY_REQUESTS,
    );
  }

  /** The payload, or a 400 that names the reason without echoing the payload. */
  private validated(body: TripWriteDto): Record<string, unknown> {
    const verdict = checkTripPayload(body?.payload);
    if (!verdict.ok) {
      throw new BadRequestException(`Not a trip payload: ${verdict.reason}`);
    }
    return body.payload;
  }

  /**
   * The id as this API will look one up.
   *
   * An id that cannot exist is answered as 404 rather than passed to the
   * database: the column is a 32-character varchar and every id this service
   * mints is 16 base64url characters, so anything else is a probe. 404 and not
   * 400 because the caller learns the same thing either way — there is no trip
   * there — and this route's whole authorisation model is that an id is a
   * secret, so it should say as little as possible about which ids are shaped
   * like real ones.
   */
  private static tripId(id: string): string {
    if (!/^[A-Za-z0-9_-]{1,32}$/.test(id ?? "")) {
      throw new HttpException("Trip not found", HttpStatus.NOT_FOUND);
    }
    return id;
  }

  private static present(trip: Trip): TripResponseDto {
    return {
      id: trip.id,
      payload: trip.payload,
      expiresAt: trip.expiresAt.toISOString(),
      updatedAt: trip.updatedAt.toISOString(),
    };
  }
}
