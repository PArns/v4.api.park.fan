import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import {
  AttractionRideProfile,
  type RideStats,
} from "../entities/attraction-ride-profile.entity";
import { WikidataClient } from "../../external-apis/wikidata/wikidata.client";
import { IDS_PER_QUERY } from "../../external-apis/wikidata/wikidata.parser";
import type { TouchedPark } from "./ride-profile.service";

/** Rows imported more recently than this are left alone on a re-run. */
const REFRESH_AFTER_DAYS = 90;

/** Pause between batches. Three queries is not a load; be polite anyway. */
const BATCH_GAP_MS = 1_000;

export interface RideStatsImportResult {
  /** Rides whose stats were written. */
  written: number;
  /** Rides Wikidata states no measurement for. */
  withoutData: number;
  touchedParks: TouchedPark[];
}

interface Candidate {
  attractionId: string;
  parkId: string;
  rcdbId: number;
  slug: string;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Imports ride measurements from Wikidata onto the curated ride profiles.
 *
 * Nothing upstream carries these numbers: ThemeParks.wiki gives rider heights
 * and stops there, so a ride's speed, height, length and duration exist nowhere
 * in this system.
 *
 * Wikidata is joined on the RCDB id we already store — which came from Wikidata
 * in the first place (property P2751) — and is CC0, so the values can be stored
 * and served. Coverage is thin: most rides have an entry and no measurements.
 * A ride with nothing is left unstamped so a later run picks it up if someone
 * fills it in.
 */
@Injectable()
export class RideStatsService {
  private readonly logger = new Logger(RideStatsService.name);

  constructor(
    @InjectRepository(AttractionRideProfile)
    private readonly repo: Repository<AttractionRideProfile>,
    private readonly wikidata: WikidataClient,
  ) {}

  /**
   * Fetch and store stats for every curated ride that has an RCDB id.
   *
   * @param limit optional cap for a trial run — the first N candidates only.
   */
  async import(limit?: number): Promise<RideStatsImportResult> {
    const result: RideStatsImportResult = {
      written: 0,
      withoutData: 0,
      touchedParks: [],
    };

    const cutoff = new Date(
      Date.now() - REFRESH_AFTER_DAYS * 24 * 60 * 60 * 1000,
    );

    // Raw table names throughout: this is hand-written SQL and TypeORM should
    // pass it through rather than rewrite property names inside it. Note
    // `rcdb_id` — the entity property is `rcdbId`, the column is not.
    const candidates: Candidate[] = await this.repo.manager
      .createQueryBuilder()
      .select([
        'profile."attractionId" AS attractionid',
        'profile."parkId" AS parkid',
        "attraction.rcdb_id AS rcdbid",
        "attraction.slug AS slug",
      ])
      .from("attraction_ride_profiles", "profile")
      .innerJoin(
        "attractions",
        "attraction",
        'attraction.id = profile."attractionId"',
      )
      .where("attraction.rcdb_id IS NOT NULL")
      .andWhere(
        "(profile.stats_updated_at IS NULL OR profile.stats_updated_at < :cutoff)",
        { cutoff },
      )
      .orderBy("attraction.slug", "ASC")
      .getRawMany<{
        attractionid: string;
        parkid: string;
        rcdbid: number;
        slug: string;
      }>()
      .then((rows) =>
        rows.map((row) => ({
          attractionId: row.attractionid,
          parkId: row.parkid,
          rcdbId: Number(row.rcdbid),
          slug: row.slug,
        })),
      );

    const targets =
      typeof limit === "number" ? candidates.slice(0, limit) : candidates;
    const batches = Math.ceil(targets.length / IDS_PER_QUERY);
    this.logger.log(
      `🎢 Wikidata stat import: ${targets.length} ride(s) in ${batches} quer${
        batches === 1 ? "y" : "ies"
      }`,
    );

    const byPark = new Map<string, string[]>();

    for (let i = 0; i < targets.length; i += IDS_PER_QUERY) {
      const batch = targets.slice(i, i + IDS_PER_QUERY);
      const found = await this.wikidata.fetchRideStats(
        batch.map((target) => target.rcdbId),
      );

      for (const target of batch) {
        const stats = found.get(String(target.rcdbId));
        if (!stats) {
          // Left unstamped on purpose: Wikidata is edited by people, and a ride
          // with nothing today may have a speed next month.
          result.withoutData++;
          continue;
        }

        const payload: RideStats = {
          topSpeedKmh: stats.topSpeedKmh,
          heightM: stats.heightM,
          lengthM: stats.lengthM,
          durationSeconds: stats.durationSeconds,
          source: "wikidata",
          sourceId: stats.entityId,
        };
        await this.repo.update(target.attractionId, {
          stats: payload,
          statsUpdatedAt: new Date(),
        });
        result.written++;
        const ids = byPark.get(target.parkId) ?? [];
        ids.push(target.attractionId);
        byPark.set(target.parkId, ids);
      }

      if (i + IDS_PER_QUERY < targets.length) await sleep(BATCH_GAP_MS);
    }

    result.touchedParks = [...byPark].map(([parkId, attractionIds]) => ({
      parkId,
      attractionIds,
    }));

    this.logger.log(
      `🎢 Wikidata stat import done: ${result.written} written, ${result.withoutData} with no measurements`,
    );
    return result;
  }
}
