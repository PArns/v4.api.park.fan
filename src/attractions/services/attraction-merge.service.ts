import { Inject, Injectable, Logger } from "@nestjs/common";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { Attraction } from "../entities/attraction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import {
  ATTRACTION_DEPENDENCIES,
  applyMergeDependencies,
} from "../../parks/utils/merge-dependencies";
import {
  resolveSurvivingSlug,
  isSafeToAutoMerge,
  chooseDuplicateWinner,
  DuplicateCandidate,
} from "../utils/attraction-merge.util";

interface DuplicatePairRow {
  park_id: string;
  park_name: string;
  base_id: string;
  base_slug: string;
  base_name: string;
  base_qt: string | null;
  base_geo: boolean;
  base_recent: string;
  base_total: string;
  base_created: string;
  suffix_id: string;
  suffix_slug: string;
  suffix_name: string;
  suffix_qt: string | null;
  suffix_geo: boolean;
  suffix_recent: string;
  suffix_total: string;
  suffix_created: string;
}

export interface DuplicatePairReport {
  parkId: string;
  parkName: string;
  baseSlug: string;
  suffixSlug: string;
  winnerId: string;
  loserId: string;
  survivingSlug: string;
  safe: boolean;
  reason: string;
}

export interface DuplicateBatchReport {
  dryRun: boolean;
  totalPairs: number;
  planned: DuplicatePairReport[];
  skipped: DuplicatePairReport[];
  merged: number;
  failed: Array<{ pair: DuplicatePairReport; error: string }>;
}

export interface AttractionMergeResult {
  winnerId: string;
  loserId: string;
  name: string;
  parkId: string;
  survivingSlug: string;
  renamed: boolean;
}

/**
 * Collapses two rows that describe the same ride inside one park.
 *
 * `ParkMergeService` only merges across parks, so duplicates created by the
 * externalId-only sync matching had no way to be cleaned up. The dependent-row
 * handling is shared with the park merge via `ATTRACTION_DEPENDENCIES`, which
 * a rollback-only cold run against production verified end to end: queue_data
 * is fully preserved and no orphans are left behind.
 */
@Injectable()
export class AttractionMergeService {
  private readonly logger = new Logger(AttractionMergeService.name);

  constructor(
    private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly revalidationService: RevalidationService,
  ) {}

  async mergeAttractions(
    winnerId: string,
    loserId: string,
  ): Promise<AttractionMergeResult> {
    if (winnerId === loserId) {
      throw new Error(`Cannot merge attraction ${winnerId} into itself`);
    }

    this.logger.log(`🔀 Merging attraction ${loserId} → ${winnerId}`);

    const result = await this.dataSource.transaction(async (manager) => {
      const winner = await manager.findOne(Attraction, {
        where: { id: winnerId },
      });
      const loser = await manager.findOne(Attraction, {
        where: { id: loserId },
      });

      if (!winner || !loser) {
        throw new Error(
          `Attraction not found (winner: ${!!winner}, loser: ${!!loser})`,
        );
      }

      if (winner.parkId !== loser.parkId) {
        throw new Error(
          `Attractions must live in the same park (${winner.parkId} vs ${loser.parkId}) — use ParkMergeService to merge across parks`,
        );
      }

      // queue_data and friends are compressed hypertables; the default
      // decompression cap aborts a merge of any real size.
      await manager.query(
        "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0",
      );

      await applyMergeDependencies(
        manager,
        ATTRACTION_DEPENDENCIES,
        winnerId,
        loserId,
      );

      await manager.query(
        "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 100000",
      );

      await manager.delete(Attraction, loserId);

      // Only now is the base slug free: (parkId, slug) is unique, so the
      // rename has to follow the delete.
      const survivingSlug = resolveSurvivingSlug(winner.slug, loser.slug);
      const renamed = survivingSlug !== winner.slug;

      // The two sources fill in different columns — across the real duplicate
      // pairs the queue-times id sits only on the second row 33 times and the
      // coordinates 29 times. Harvest whatever the survivor lacks, never
      // overwriting what it already has.
      const inherited = this.inheritMissingMetadata(winner, loser);

      if (renamed || Object.keys(inherited).length > 0) {
        await manager.update(Attraction, winnerId, {
          ...inherited,
          ...(renamed ? { slug: survivingSlug } : {}),
        });
      }

      return {
        winnerId,
        loserId,
        name: winner.name,
        parkId: winner.parkId,
        survivingSlug,
        renamed,
      };
    });

    await invalidateParkCaches(this.redis, result.parkId, [winnerId, loserId]);
    // The park detail deduplicated these at read time, but the sitemap did
    // not — both surfaces have to be rebuilt or the removed slug keeps being
    // advertised for up to 24h plus the CDN's stale-while-revalidate window.
    await this.revalidationService.revalidateTags([
      "geo",
      "parks",
      "attractions",
    ]);

    this.logger.log(
      `✅ Merged "${result.name}" — surviving slug "${result.survivingSlug}"`,
    );

    return result;
  }

  /**
   * Finds every base/suffix slug pair inside a park and decides, per pair,
   * whether it is provably one ride.
   *
   * The queue-row counts are windowed (7 and 90 days) — counting the full
   * hypertable for a few hundred attractions is far too expensive, and recency
   * is what the winner rule actually cares about.
   */
  async findDuplicatePairs(): Promise<DuplicatePairReport[]> {
    const rows: DuplicatePairRow[] = await this.dataSource.query(`
      WITH pairs AS (
        SELECT b.id AS base_id, b.slug AS base_slug, b.name AS base_name,
               b.queue_times_entity_id AS base_qt,
               (b.latitude IS NOT NULL) AS base_geo, b."createdAt" AS base_created,
               s.id AS suffix_id, s.slug AS suffix_slug, s.name AS suffix_name,
               s.queue_times_entity_id AS suffix_qt,
               (s.latitude IS NOT NULL) AS suffix_geo, s."createdAt" AS suffix_created,
               b."parkId" AS park_id
        FROM attractions s
        JOIN attractions b
          ON b."parkId" = s."parkId"
         AND b.slug = regexp_replace(s.slug, '-[0-9]+$', '')
        WHERE s.slug ~ '-[0-9]+$'
      )
      SELECT p.*, pk.name AS park_name,
        (SELECT count(*) FROM queue_data q WHERE q."attractionId" = p.base_id
           AND q."timestamp" > now() - interval '7 days')  AS base_recent,
        (SELECT count(*) FROM queue_data q WHERE q."attractionId" = p.base_id
           AND q."timestamp" > now() - interval '90 days') AS base_total,
        (SELECT count(*) FROM queue_data q WHERE q."attractionId" = p.suffix_id
           AND q."timestamp" > now() - interval '7 days')  AS suffix_recent,
        (SELECT count(*) FROM queue_data q WHERE q."attractionId" = p.suffix_id
           AND q."timestamp" > now() - interval '90 days') AS suffix_total
      FROM pairs p
      JOIN parks pk ON pk.id = p.park_id
      ORDER BY pk.name, p.base_slug
    `);

    return rows.map((row) => {
      const base: DuplicateCandidate = {
        id: row.base_id,
        slug: row.base_slug,
        name: row.base_name,
        queueTimesEntityId: row.base_qt,
        hasCoordinates: row.base_geo,
        recentQueueRows: Number(row.base_recent),
        totalQueueRows: Number(row.base_total),
        createdAt: new Date(row.base_created),
      };
      const suffix: DuplicateCandidate = {
        id: row.suffix_id,
        slug: row.suffix_slug,
        name: row.suffix_name,
        queueTimesEntityId: row.suffix_qt,
        hasCoordinates: row.suffix_geo,
        recentQueueRows: Number(row.suffix_recent),
        totalQueueRows: Number(row.suffix_total),
        createdAt: new Date(row.suffix_created),
      };

      const safe = isSafeToAutoMerge(base, suffix);
      const { winnerId, loserId } = chooseDuplicateWinner(base, suffix);
      const winner = winnerId === base.id ? base : suffix;
      const loser = winnerId === base.id ? suffix : base;

      return {
        parkId: row.park_id,
        parkName: row.park_name,
        baseSlug: base.slug,
        suffixSlug: suffix.slug,
        winnerId,
        loserId,
        survivingSlug: resolveSurvivingSlug(winner.slug, loser.slug),
        safe,
        reason: safe
          ? "same ride (name or queue-times id match)"
          : `names differ and no shared queue-times id — "${base.name}" vs "${suffix.name}"; review by hand`,
      };
    });
  }

  /**
   * Merges every pair that is provably one ride, one transaction per pair.
   *
   * Pairs needing review are never touched: a shared slug stem alone is not
   * evidence, and at least one real pair holds two genuinely different rides.
   * A failing pair is recorded and the run continues, so one bad row cannot
   * block the rest.
   */
  async mergeDuplicates(
    options: { dryRun?: boolean; limit?: number } = {},
  ): Promise<DuplicateBatchReport> {
    const { dryRun = true, limit } = options;
    const pairs = await this.findDuplicatePairs();

    const safe = pairs.filter((p) => p.safe);
    const skipped = pairs.filter((p) => !p.safe);
    const planned = typeof limit === "number" ? safe.slice(0, limit) : safe;

    const report: DuplicateBatchReport = {
      dryRun,
      totalPairs: pairs.length,
      planned,
      skipped,
      merged: 0,
      failed: [],
    };

    if (dryRun) {
      this.logger.log(
        `🔎 Dry run: ${planned.length} pair(s) would be merged, ${skipped.length} need review`,
      );
      return report;
    }

    for (const pair of planned) {
      try {
        await this.mergeAttractions(pair.winnerId, pair.loserId);
        report.merged++;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.logger.error(
          `❌ Merge failed for ${pair.baseSlug} in ${pair.parkName}: ${message}`,
        );
        report.failed.push({ pair, error: message });
      }
    }

    this.logger.log(
      `✅ Batch done: ${report.merged} merged, ${report.failed.length} failed, ${skipped.length} left for review`,
    );
    return report;
  }

  /**
   * Columns worth carrying over from the losing row. Deliberately excludes
   * `externalId` (unique, and the survivor keeps its own identity) and `slug`
   * (handled by resolveSurvivingSlug).
   */
  private static readonly INHERITABLE_COLUMNS = [
    "queueTimesEntityId",
    "latitude",
    "longitude",
    "landName",
    "landExternalId",
    "minimumHeight",
    "maximumHeight",
    "mayGetWet",
    "curatedMayGetWet",
    "hasSingleRider",
    "rcdbId",
    "seasonMonths",
  ] as const;

  private inheritMissingMetadata(
    winner: Attraction,
    loser: Attraction,
  ): Partial<Attraction> {
    const inherited: Record<string, unknown> = {};

    for (const column of AttractionMergeService.INHERITABLE_COLUMNS) {
      const winnerValue = winner[column];
      const loserValue = loser[column];
      if (
        (winnerValue === null || winnerValue === undefined) &&
        loserValue !== null &&
        loserValue !== undefined
      ) {
        inherited[column] = loserValue;
      }
    }

    return inherited as Partial<Attraction>;
  }
}
