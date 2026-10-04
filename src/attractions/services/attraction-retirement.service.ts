import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Not, Repository } from "typeorm";
import { Redis } from "ioredis";
import { Attraction } from "../entities/attraction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";

/**
 * The exact `retired_reason` the children sync writes when an entity is
 * reclassified upstream, and the marker that lets it undo itself.
 *
 * It has to be an exact string rather than a prefix or a substring, because
 * the sync only un-retires rows carrying *this* reason: a retirement entered
 * by a human through `POST /admin/retire-attractions` must survive every
 * nightly run, and a fuzzy match would eventually swallow one.
 *
 * **It is also user-facing**, so it reads as a sentence and not as a note to
 * the next developer: `AttractionResponseDto` serves `retiredReason` on the
 * public attraction detail endpoint. Issue numbers, file paths and internals
 * belong in the docblock of the method that writes it, not in here.
 *
 * ⚠️ **An edit here changes what {@link isReclassifiedUpstreamReason} matches,
 * so the previous value moves into {@link RECLASSIFIED_UPSTREAM_REASONS} in
 * the same commit.** Without that, every row already retired under the old
 * wording is stranded: the un-retire check no longer recognises it and the
 * retire filter skips it because `retiredAt` is set. A spec pins the literal,
 * so the wording cannot be changed without reading this first.
 *
 * The pin is a mitigation and not a fix — the copy and the marker are one
 * string, which is also why this sentence cannot be localized. Decoupling
 * them needs a column of its own (`retired_by`, say), and that is a schema
 * change this issue did not ask for.
 */
export const RECLASSIFIED_UPSTREAM_REASON =
  "ThemeParks.wiki lists this entity as a show or a restaurant rather than an " +
  "attraction, so it is no longer tracked as a ride. The date is when this was " +
  "noticed, not when the reclassification happened. " +
  "Source: https://api.themeparks.wiki/";

/**
 * The exact `retired_reason` the children sync writes when an attraction's id
 * has been missing from its park's `/children` for
 * `ABSENT_UPSTREAM_RETIRE_DAYS` while the park itself kept syncing.
 *
 * It belongs to the same family as {@link RECLASSIFIED_UPSTREAM_REASON} on
 * purpose (PO decision, 2026-10-02): nothing is known to have closed, so the
 * row must not read as "closed permanently", and `syncAttraction` has to lift
 * the retirement by itself if the id ever comes back. Both follow from it
 * being listed in {@link RECLASSIFIED_UPSTREAM_REASONS}. The price is that
 * `retiredKind: "reclassified"` now covers two cases, "the wiki calls it a show
 * now" and "the wiki stopped listing it"; only this string tells them apart.
 *
 * Same obligations as the other one: an exact string, user-facing, and pinned
 * by a spec — a rewording moves the old value into the list below.
 */
export const ABSENT_UPSTREAM_REASON =
  "ThemeParks.wiki no longer lists this entity among its park's attractions, " +
  "so it is no longer tracked as a ride. The date is when this was noticed, " +
  "not when it left the list. Source: https://api.themeparks.wiki/";

/**
 * Every wording the children sync has ever written for a retirement it may
 * undo again, newest first: entities reclassified as a show or a restaurant,
 * and entities that left the park's `/children` altogether. The un-retire
 * check accepts all of them, so a row retired under an older text still comes
 * back when the wiki lists the entity as an attraction again, and
 * `retiredKindOf`, the sitemap and `detect-seasonal` treat all of them as
 * "not closed".
 */
export const RECLASSIFIED_UPSTREAM_REASONS: readonly string[] = [
  RECLASSIFIED_UPSTREAM_REASON,
  ABSENT_UPSTREAM_REASON,
];

/** True for a retirement this sync wrote, under any wording it has used. */
export function isReclassifiedUpstreamReason(
  reason: string | null | undefined,
): boolean {
  return reason != null && RECLASSIFIED_UPSTREAM_REASONS.includes(reason);
}

/**
 * The two things `retiredAt` can mean, as the API tells them apart.
 *
 * - `closed` — a human retired the ride because it stopped operating for good
 *   (`POST /admin/retire-attractions`). The frontend renders its page as
 *   "closed permanently since …" and keeps it in the sitemap.
 * - `reclassified` — the children sync retired the row because ThemeParks.wiki
 *   now lists the entity as a show or a restaurant, or no longer lists it in
 *   the park at all ({@link ABSENT_UPSTREAM_REASON}). Nothing is known to have
 *   closed, so nothing may say so.
 */
export type RetiredKind = "closed" | "reclassified";

export const RETIRED_KIND_VALUES: readonly RetiredKind[] = [
  "closed",
  "reclassified",
];

/**
 * How long a closed ride stays on its park's page after it closed.
 *
 * A year covers the season after the closure, when visitors still look for the
 * ride where it used to be. After that the park page is about the park as it
 * is, and the ride answers only on its own URL: its page and its sitemap entry
 * stay, because the ranking they earned does not expire with that interest.
 * `retiredHidden` takes a ride off sooner; nothing puts one back later.
 */
export const CLOSED_RIDE_PARK_PAGE_DAYS = 365;

/** The oldest closing date still listed on a park page at `now`. */
export function closedRideParkPageCutoff(now: Date = new Date()): Date {
  return new Date(now.getTime() - CLOSED_RIDE_PARK_PAGE_DAYS * 86_400_000);
}

/**
 * Whether a retired row is listed on its park's page: a closure, not hidden by
 * an editor, and closed within {@link CLOSED_RIDE_PARK_PAGE_DAYS}. The query
 * in `AttractionsService.findClosedForParkPage` and the admin list both ask
 * this, so the two cannot disagree.
 */
export function isOnParkPage(
  row: {
    retiredAt: Date | null;
    retiredReason: string | null;
    retiredHidden?: boolean | null;
  },
  now: Date = new Date(),
): boolean {
  return (
    retiredKindOf(row) === "closed" &&
    row.retiredHidden !== true &&
    row.retiredAt!.getTime() > closedRideParkPageCutoff(now).getTime()
  );
}

/**
 * Which retirement a row carries, or null while it is not retired.
 *
 * Served so that no client has to match {@link RECLASSIFIED_UPSTREAM_REASON}
 * itself: that string is the sync's marker and its wording may change, and
 * {@link RECLASSIFIED_UPSTREAM_REASONS} is the one place that keeps track.
 */
export function retiredKindOf(row: {
  retiredAt: Date | null;
  retiredReason: string | null;
}): RetiredKind | null {
  if (!row.retiredAt) return null;
  return isReclassifiedUpstreamReason(row.retiredReason)
    ? "reclassified"
    : "closed";
}

export interface RetirementRequest {
  attractionId: string;
  /** The day it stopped existing, where a source states one. */
  retiredAt: string;
  /** Why, and the URL it was established from. */
  reason: string;
}

export interface RetirementResult {
  attractionId: string;
  name: string;
  parkId: string;
  retiredAt: string;
}

/**
 * Retires attractions that no longer exist, and cleans up after itself.
 *
 * Retirement is a plain column write, so on its own it would leave the park's
 * cached payload and the frontend's advertised slug in place for up to 24h plus
 * the CDN's stale-while-revalidate window — the same trap the attraction merge
 * had to solve. Doing the write through here keeps eviction and revalidation
 * attached to it, which matters because ~73 retirements are queued and doing
 * that by hand 73 times is how one gets forgotten.
 */
@Injectable()
export class AttractionRetirementService {
  private readonly logger = new Logger(AttractionRetirementService.name);

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly revalidationService: RevalidationService,
  ) {}

  async retire(requests: RetirementRequest[]): Promise<RetirementResult[]> {
    const results: RetirementResult[] = [];
    const touchedParks = new Set<string>();

    for (const request of requests) {
      const attraction = await this.attractionRepository.findOne({
        where: { id: request.attractionId },
      });
      if (!attraction) {
        this.logger.warn(`Attraction ${request.attractionId} not found`);
        continue;
      }

      await this.attractionRepository.update(attraction.id, {
        retiredAt: new Date(request.retiredAt),
        retiredReason: request.reason,
      });

      touchedParks.add(attraction.parkId);
      results.push({
        attractionId: attraction.id,
        name: attraction.name,
        parkId: attraction.parkId,
        retiredAt: request.retiredAt,
      });
      this.logger.log(
        `🪦 Retired "${attraction.name}" as of ${request.retiredAt} — ${request.reason}`,
      );
    }

    for (const parkId of touchedParks) {
      await invalidateParkCaches(this.redis, parkId).catch((e) =>
        this.logger.warn(
          `Cache eviction failed for park ${parkId}: ${(e as Error)?.message ?? e}`,
        ),
      );
    }

    if (results.length > 0) {
      // The frontend holds the park payload for a day, and that payload is
      // what decides whether a ride page renders live or as closed
      // permanently. Without this it keeps rendering the ride as running.
      await this.revalidationService
        .revalidateTags(["geo", "parks", "attractions"])
        .catch((e) =>
          this.logger.warn(
            `Revalidation failed: ${(e as Error)?.message ?? e}`,
          ),
        );
    }

    return results;
  }

  /**
   * Undo — a retirement is a claim about the world, and claims can be wrong.
   *
   * `absentSince` goes with it. A person saying the ride exists is better
   * evidence than the feed not listing it, so the absence starts over rather
   * than resuming where it left off: without this, the next children sync
   * would read a clock older than `ABSENT_UPSTREAM_RETIRE_DAYS` and retire the
   * row again within the hour. It used to be the pollution of `updatedAt` that
   * bought those 60 days by accident; now it is stated (PAR-656).
   */
  async unretire(attractionId: string): Promise<boolean> {
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
    });
    if (!attraction || attraction.retiredAt === null) return false;

    await this.attractionRepository.update(attractionId, {
      retiredAt: null,
      retiredReason: null,
      retiredHidden: false,
      absentSince: null,
    });
    await invalidateParkCaches(this.redis, attraction.parkId).catch(() => {});
    await this.revalidationService
      .revalidateTags(["geo", "parks", "attractions"])
      .catch(() => {});

    this.logger.log(`↩️  Un-retired "${attraction.name}"`);
    return true;
  }

  /**
   * Show or hide a closed ride on its park's page. The ride page and the
   * sitemap entry are not affected — see `Attraction.retiredHidden`.
   *
   * Refused for a row that is not retired: the flag would sit there unseen and
   * hide the ride the day somebody retires it.
   */
  async setRetiredHidden(
    attractionId: string,
    hidden: boolean,
  ): Promise<boolean> {
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
    });
    if (!attraction || attraction.retiredAt === null) return false;

    await this.attractionRepository.update(attractionId, {
      retiredHidden: hidden,
    });
    await invalidateParkCaches(this.redis, attraction.parkId).catch(() => {});
    await this.revalidationService.revalidateTags(["parks"]).catch(() => {});

    this.logger.log(
      `${hidden ? "🙈 Hid" : "👁️  Showed"} "${attraction.name}" on its park page`,
    );
    return true;
  }

  /** Everything currently retired, newest first — the audit view. */
  async listRetired(): Promise<Attraction[]> {
    return this.attractionRepository.find({
      where: { retiredAt: Not(IsNull()) },
      relations: ["park"],
      order: { retiredAt: "DESC" },
    });
  }
}
