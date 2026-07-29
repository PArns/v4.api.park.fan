import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  AttractionRideProfile,
  type RideStats,
} from "../entities/attraction-ride-profile.entity";
import { RcdbClient } from "../../external-apis/rcdb/rcdb.client";
import type { TouchedPark } from "./ride-profile.service";

/** Pause between ride pages. A coaster's length does not change; there is no hurry. */
const REQUEST_GAP_MS = 1_200;

/** Rows imported more recently than this are left alone on a re-run. */
const REFRESH_AFTER_DAYS = 90;

export interface RideStatsImportResult {
  /** Rides whose stats were written. */
  written: number;
  /** Rides skipped because their stats are still fresh. */
  fresh: number;
  /** Rides whose RCDB id yielded nothing (gone, or a model page). */
  missing: number;
  touchedParks: TouchedPark[];
}

interface Candidate {
  profileId: string;
  parkId: string;
  rcdbId: number;
  slug: string;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Imports measured ride facts from RCDB onto the curated ride profiles.
 *
 * Nothing upstream carries these numbers: ThemeParks.wiki gives rider heights
 * and stops there, so a ride's length, drop, speed and duration exist nowhere
 * in this system. RCDB has them for every ride we already store an `rcdbId`
 * for — which is the reason this can be targeted rather than a crawl: we ask
 * only for pages we already have an id for, one at a time.
 *
 * Re-runnable and self-limiting. Rows imported within {@link REFRESH_AFTER_DAYS}
 * are skipped, so a second run costs a handful of requests rather than five
 * hundred, and an interrupted run resumes where it stopped.
 */
@Injectable()
export class RideStatsService {
  private readonly logger = new Logger(RideStatsService.name);

  constructor(
    @InjectRepository(AttractionRideProfile)
    private readonly repo: Repository<AttractionRideProfile>,
    private readonly rcdb: RcdbClient,
  ) {}

  /**
   * Fetch and store stats for every curated ride that has an RCDB id.
   *
   * @param limit optional cap for a trial run — the first N candidates only.
   */
  async import(limit?: number): Promise<RideStatsImportResult> {
    const result: RideStatsImportResult = {
      written: 0,
      fresh: 0,
      missing: 0,
      touchedParks: [],
    };

    const cutoff = new Date(
      Date.now() - REFRESH_AFTER_DAYS * 24 * 60 * 60 * 1000,
    );

    // The rcdbId lives on the attraction, the stats on its profile row — one
    // join rather than 500 lookups, and it drops rides with no id up front.
    const candidates: Candidate[] = await this.repo.manager
      .createQueryBuilder()
      .select([
        'profile."attractionId" AS profileid',
        'profile."parkId" AS parkid',
        'attraction."rcdbId" AS rcdbid',
        "attraction.slug AS slug",
      ])
      .from(AttractionRideProfile, "profile")
      .innerJoin(
        "attractions",
        "attraction",
        'attraction.id = profile."attractionId"',
      )
      .where('attraction."rcdbId" IS NOT NULL')
      .andWhere(
        '(profile."stats_updated_at" IS NULL OR profile."stats_updated_at" < :cutoff)',
        { cutoff },
      )
      .orderBy("attraction.slug", "ASC")
      .getRawMany<{
        profileid: string;
        parkid: string;
        rcdbid: number;
        slug: string;
      }>()
      .then((rows) =>
        rows.map((row) => ({
          profileId: row.profileid,
          parkId: row.parkid,
          rcdbId: Number(row.rcdbid),
          slug: row.slug,
        })),
      );

    const targets =
      typeof limit === "number" ? candidates.slice(0, limit) : candidates;
    this.logger.log(
      `🎢 RCDB stat import: ${targets.length} ride(s) to fetch (${REQUEST_GAP_MS}ms apart)`,
    );

    const byPark = new Map<string, string[]>();

    for (const [index, target] of targets.entries()) {
      const stats = await this.rcdb.fetchRideStats(target.rcdbId);

      if (!stats) {
        // A dead id or a manufacturer's model page. Left unstamped on purpose:
        // the next run retries it, which is what we want for an id that is
        // wrong in our seed rather than gone from theirs.
        result.missing++;
        this.logger.warn(
          `🎢 No stats for ${target.slug} (rcdb ${target.rcdbId}) — id gone, or not a ride page`,
        );
      } else {
        const payload: RideStats = {
          ...stats,
          source: "rcdb",
          sourceId: target.rcdbId,
        };
        await this.repo.update(target.profileId, {
          stats: payload,
          statsUpdatedAt: new Date(),
        });
        result.written++;
        const ids = byPark.get(target.parkId) ?? [];
        ids.push(target.profileId);
        byPark.set(target.parkId, ids);
      }

      // No pause after the last one — the job should not sit idle at the end.
      if (index < targets.length - 1) await sleep(REQUEST_GAP_MS);
    }

    result.fresh = candidates.length - targets.length;
    result.touchedParks = [...byPark].map(([parkId, attractionIds]) => ({
      parkId,
      attractionIds,
    }));

    this.logger.log(
      `🎢 RCDB stat import done: ${result.written} written, ${result.missing} without stats`,
    );
    return result;
  }
}
