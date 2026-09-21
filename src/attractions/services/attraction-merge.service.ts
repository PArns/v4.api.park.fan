import { Inject, Injectable, Logger } from "@nestjs/common";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { Attraction } from "../entities/attraction.entity";
import { worksPeriodEndsBeforeItBegins } from "../utils/curated-out-of-service.util";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import {
  ATTRACTION_DEPENDENCIES,
  applyMergeDependencies,
  planWinnerAuthoritative,
} from "../../parks/utils/merge-dependencies";
import {
  resolveSurvivingSlug,
  resolveSurvivingName,
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
  base_external: string;
  base_geo: boolean;
  base_recent: string;
  base_total: string;
  base_created: string;
  suffix_id: string;
  suffix_slug: string;
  suffix_name: string;
  suffix_qt: string | null;
  suffix_external: string;
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
 * A hand-written row the merge would delete, named while it still exists.
 *
 * There is no feed behind these rows and no job that would rebuild one, so the
 * report carries the row itself rather than a count: somebody who decides to
 * go ahead anyway has to be able to type it back in afterwards.
 */
export interface DroppedCuration {
  /** Table the row lives in. */
  table: string;
  /**
   * Which attraction loses its row. The SURVIVOR's own row can be the one that
   * goes: where both sides hold a curated ride profile, the richer of the two
   * wins and the survivor's is not automatically the richer (PAR-179).
   */
  from: "winner" | "loser";
  /**
   * The row as stored, so the loss is recoverable by hand.
   *
   * For a dependent table that is the raw row as the planner read it, column
   * names and all. For `attractions` it is the handful of curated columns that
   * stay behind, under the names `AdminCurationService` takes them back under —
   * typing `curatedOutOfServiceFrom` into the editor is what recovery looks
   * like there, and `curated_out_of_service_from` is not a key it accepts.
   */
  row: Record<string, unknown>;
}

/** The same answer as a merge, minus the merge. See `previewMerge`. */
export interface AttractionMergePreview extends AttractionMergeResult {
  dryRun: true;
  /** The slug that would stop resolving once the losing row is deleted. */
  removedSlug: string;
  /** Columns the survivor would take from the row about to disappear. */
  inheritedColumns: string[];
  /**
   * Curated values the merge would destroy: a dependent row it deletes, and
   * the curated columns on the losing attraction itself that the survivor
   * does not inherit. Empty for almost every pair — very few rides carry a
   * curated profile or a works period at all.
   */
  droppedCurations: DroppedCuration[];
}

/** A column the merge can read: null and undefined both mean "nothing here". */
function isSet(value: unknown): boolean {
  return value !== null && value !== undefined;
}

/**
 * What a merge would write onto the survivor, and what it would leave behind.
 *
 * Both halves come out of one pass because they are the same decision read
 * twice: a column set the survivor does not inherit is a column set the DELETE
 * takes with it. Splitting them into a rule for the merge and a rule for the
 * rehearsal is how the two drift.
 */
interface MetadataInheritancePlan {
  /** Columns the survivor takes from the losing row. */
  inherited: Partial<Attraction>;
  /**
   * Per column set, the values the losing row holds that reach the survivor
   * nowhere — under the entity's own names, because a curator types those.
   * Empty for the ordinary pair: almost no ride carries a curated window, and
   * a loser holding none loses none.
   */
  droppedColumnSets: Record<string, unknown>[];
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

  /**
   * What `mergeAttractions` would do, without doing any of it.
   *
   * The admin has a "Probelauf" button next to every duplicate pair, and the
   * endpoint behind it documents `dryRun` as defaulting to true — but the
   * single-pair branch handed the ids straight to `mergeAttractions`, which
   * takes no such flag. So a rehearsal deleted the losing row for real, inside
   * a transaction with no undo, and then rendered the outcome as a preview.
   * The button was not even gated on the pair being safe, so the pairs most
   * likely to be rehearsed were the ones the detector had refused to merge.
   *
   * Everything reported here is derived by the same functions the real merge
   * uses, so the preview cannot drift from the act — `resolveSurvivingName` and
   * `resolveSurvivingSlug` for the naming, `planMetadataInheritance` for the
   * columns that travel and the ones that stay behind, and
   * `planWinnerAuthoritative` for the curated rows that would cease to exist.
   *
   * The last of those was missing until PAR-179, and it was the one that
   * mattered: a rehearsal that reports a slug and a handful of inherited
   * columns, and stays silent about a fourteen-element ride layout it is about
   * to delete, understates the only irreversible thing the merge does.
   */
  async previewMerge(
    winnerId: string,
    loserId: string,
  ): Promise<AttractionMergePreview> {
    if (winnerId === loserId) {
      throw new Error(`Cannot merge attraction ${winnerId} into itself`);
    }

    const attractions = this.dataSource.getRepository(Attraction);
    const [winner, loser] = await Promise.all([
      attractions.findOne({ where: { id: winnerId } }),
      attractions.findOne({ where: { id: loserId } }),
    ]);

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

    const survivingName = resolveSurvivingName(winner.name, loser.name);
    const survivingSlug = resolveSurvivingSlug(
      winner.slug,
      loser.slug,
      survivingName,
    );

    const plan = this.planMetadataInheritance(winner, loser);

    return {
      dryRun: true,
      winnerId,
      loserId,
      parkId: winner.parkId,
      name: survivingName,
      survivingSlug,
      renamed: survivingSlug !== winner.slug,
      removedSlug: loser.slug,
      inheritedColumns: Object.keys(plan.inherited),
      droppedCurations: [
        ...(await this.findDroppedCurations(winnerId, loserId)),
        // The attraction's own curated columns, which `findDroppedCurations`
        // cannot see: it reads `ATTRACTION_DEPENDENCIES`, and that list is
        // about rows in other tables. `from` is always the loser here —
        // a survivor holding part of the set keeps all of its own.
        ...plan.droppedColumnSets.map((row) => ({
          table: "attractions",
          from: "loser" as const,
          row,
        })),
      ],
    };
  }

  /**
   * The curated rows `applyMergeDependencies` would delete for this pair.
   *
   * Reads `ATTRACTION_DEPENDENCIES` rather than naming a table, so a
   * `winner-authoritative` entry added to that list is reported here without a
   * second edit — the drift this whole method exists to close would otherwise
   * reopen with the next curated table somebody declares.
   *
   * Dependent rows only. A curated column on the attraction row itself never
   * appears in that list, so the works period is reported by
   * `planMetadataInheritance` instead and joins this list in `previewMerge`.
   *
   * Only `winner-authoritative` entries are asked. `discard` rows are derived
   * and the nightly jobs rewrite them from the history that has just moved onto
   * the survivor, so naming them would bury the one line that matters under
   * five that do not — the baselines, the rope drop and the typical waits are
   * deliberately out of scope (PAR-179).
   *
   * A `custom` entry is skipped too, and there that is a gap rather than a
   * decision: `attraction_review_marks` drops two kinds of hand-written verdict
   * (PAR-149) and this rehearsal names neither. One of them is the mark saying
   * a person already established that these two are DIFFERENT rides — the most
   * useful thing this preview could put in front of somebody about to merge
   * them, and the one thing it does not say. Closing it wants a read-only half
   * for the strategy, the way `planWinnerAuthoritative` is the twin of
   * `applyWinnerAuthoritative`, plus a decision about whether such a mark
   * should stop the merge rather than annotate it: PAR-239. Until then the
   * merge logs what it drops and the preview does not.
   *
   * Outside a transaction, unlike the merge: this is a read, and the pair can
   * change between the rehearsal and the act either way.
   */
  private async findDroppedCurations(
    winnerId: string,
    loserId: string,
  ): Promise<DroppedCuration[]> {
    const dropped: DroppedCuration[] = [];

    for (const dep of ATTRACTION_DEPENDENCIES) {
      if (dep.strategy !== "winner-authoritative") continue;

      const decision = await planWinnerAuthoritative(
        this.dataSource,
        dep,
        winnerId,
        loserId,
      );

      if (!decision.droppedFrom) continue;
      for (const row of decision.dropped) {
        dropped.push({ table: dep.table, from: decision.droppedFrom, row });
      }
    }

    return dropped;
  }

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

      // `external_entity_mapping` is not in `ATTRACTION_DEPENDENCIES` and
      // never was: it is keyed on `internal_entity_id` across every entity
      // type at once, so the list cannot carry it and every caller moves it
      // itself, before the dependencies. The other two merge paths do
      // (`ParksService.consolidateMergedEntities`,
      // `ParkMergeService.consolidateEntityData`); this one did not, and the
      // table has no FK, so the DELETE below left the row behind in silence
      // rather than raising 23503.
      //
      // The cost is not housekeeping. The unique index is on
      // `(external_source, external_entity_id)` alone, so the stranded row
      // keeps holding the upstream's id — and `ChildrenMetadataProcessor`
      // reads that index to decide whether a mapping is needed
      // (`createMapping`), finds one, and creates nothing. The replacement
      // attraction then has no Queue-Times mapping, `WaitTimesProcessor`
      // resolves nothing for it (its `externalId` fallback covers
      // `themeparks-wiki` only) and `reconcileMissingAttractions` writes it a
      // permanent `system-reconciliation` CLOSED series while the feed reports
      // the ride as open. Measured on 2026-09-18: 201 stranded rows, 38 of
      // them sitting on the id of a live attraction, 30 in one park.
      //
      // No conflict delete, for the same reason the other two paths give: a
      // pair the winner already holds cannot also sit on the loser, because
      // that index is unique across the whole table.
      await manager.query(
        `UPDATE external_entity_mapping SET "internal_entity_id" = $1 WHERE "internal_entity_id" = $2`,
        [winnerId, loserId],
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

      // The two sources fill in different columns — across the real duplicate
      // pairs the queue-times id sits only on the second row 33 times and the
      // coordinates 29 times. Harvest whatever the survivor lacks, never
      // overwriting what it already has.
      //
      // Planned before the DELETE rather than after it, so the warning about
      // the curated values nobody will carry is written while the row holding
      // them still exists. The plan reads the two entities already in hand and
      // issues no statement, so the position costs nothing either way.
      const { inherited, droppedColumnSets } = this.planMetadataInheritance(
        winner,
        loser,
      );
      this.logDroppedColumnSets(loser, droppedColumnSets);

      await manager.delete(Attraction, loserId);

      // Only now is the base slug free: (parkId, slug) is unique, so the
      // rename has to follow the delete.
      const survivingSlug = resolveSurvivingSlug(
        winner.slug,
        loser.slug,
        resolveSurvivingName(winner.name, loser.name),
      );
      const survivingName = resolveSurvivingName(winner.name, loser.name);
      const renamed = survivingSlug !== winner.slug;
      const rewordedName = survivingName !== winner.name;

      // `lastMergedAt` is unconditional, unlike everything beside it: the merge
      // happened whether or not the survivor changed its name or inherited a
      // column, and what depends on this stamp is the reconstruction skipping a
      // ride whose history is now two interleaved series.
      await manager.update(Attraction, winnerId, {
        ...inherited,
        ...(renamed ? { slug: survivingSlug } : {}),
        ...(rewordedName ? { name: survivingName } : {}),
        lastMergedAt: new Date(),
      });

      return {
        winnerId,
        loserId,
        name: survivingName,
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
      WITH slug_pairs AS (
        -- The original rule: "foo" next to "foo-2". Finds duplicates the sync
        -- created by appending a counter.
        -- Ordered by id, not by which row holds the base slug. The two rules
        -- below find many of the same pairs, and a UNION only removes exactly
        -- equal tuples — so without a canonical order the same two rows appear
        -- twice with their roles swapped. That inflated the count from 53 real
        -- pairs to 63 rows, and each mirrored twin would have been merged a
        -- second time against a row the first merge had already deleted.
        -- Which row is "base" carries no meaning anyway: chooseDuplicateWinner
        -- decides the survivor and is symmetric.
        SELECT LEAST(b.id, s.id) AS base_id, GREATEST(b.id, s.id) AS suffix_id,
               b."parkId" AS park_id
        FROM attractions s
        JOIN attractions b
          ON b."parkId" = s."parkId"
         AND b.slug = regexp_replace(s.slug, '-[0-9]+$', '')
        WHERE s.slug ~ '-[0-9]+$'
          AND b.retired_at IS NULL AND s.retired_at IS NULL
      ),
      qt_pairs AS (
        -- Two rows in one park carrying the SAME Queue-Times id. That is not a
        -- resemblance, it is an identity: Queue-Times issues one id per ride.
        --
        -- The slug rule cannot see these, because their slugs share no stem.
        -- Energylandia is the worked example: Queue-Times publishes the park's
        -- own map numbers inside the ride names — "Draken (155)", "Frutti Loop
        -- (39)" — while ThemeParks.wiki publishes them without. Name matching
        -- therefore fails, and we ended up with "draken" beside "draken-rc",
        -- eight times in that park alone.
        SELECT LEAST(a.id, b.id) AS base_id, GREATEST(a.id, b.id) AS suffix_id,
               a."parkId" AS park_id
        FROM attractions a
        JOIN attractions b
          ON b."parkId" = a."parkId"
         AND b.queue_times_entity_id = a.queue_times_entity_id
         AND b.id <> a.id
        WHERE a.queue_times_entity_id IS NOT NULL
          AND a.retired_at IS NULL AND b.retired_at IS NULL
        GROUP BY 1, 2, 3
      ),
      pairs AS (
        SELECT b.id AS base_id, b.slug AS base_slug, b.name AS base_name,
               b.queue_times_entity_id AS base_qt,
               b."externalId" AS base_external,
               (b.latitude IS NOT NULL) AS base_geo, b."createdAt" AS base_created,
               s.id AS suffix_id, s.slug AS suffix_slug, s.name AS suffix_name,
               s.queue_times_entity_id AS suffix_qt,
               s."externalId" AS suffix_external,
               (s.latitude IS NOT NULL) AS suffix_geo, s."createdAt" AS suffix_created,
               b."parkId" AS park_id
        FROM (
          SELECT base_id, suffix_id, park_id FROM slug_pairs
          UNION
          SELECT base_id, suffix_id, park_id FROM qt_pairs
        ) candidate_pairs
        JOIN attractions b ON b.id = candidate_pairs.base_id
        JOIN attractions s ON s.id = candidate_pairs.suffix_id
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
      -- Pairs a human has already established are two different attractions.
      -- Without this, Cedar Creek keeps being offered for merge against Cedar
      -- Creek Mine Ride — a lazy river against a roller coaster — until
      -- somebody stops reading carefully.
      LEFT JOIN attraction_review_marks m
             ON m.kind = 'not_a_duplicate'
            AND m.attraction_id = LEAST(p.base_id, p.suffix_id)
            AND m.other_attraction_id = GREATEST(p.base_id, p.suffix_id)
      WHERE m.id IS NULL
      ORDER BY pk.name, p.base_slug
    `);

    return rows.map((row) => {
      const base: DuplicateCandidate = {
        id: row.base_id,
        slug: row.base_slug,
        name: row.base_name,
        externalId: row.base_external,
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
        externalId: row.suffix_external,
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
        survivingSlug: resolveSurvivingSlug(
          winner.slug,
          loser.slug,
          winner.name,
        ),
        safe,
        reason: safe
          ? "same ride (names agree once the map number is stripped)"
          : base.externalId.startsWith("qt-ride-") ===
              suffix.externalId.startsWith("qt-ride-")
            ? `same name but two ids from one source — "${base.name}"; upstream calls these two things, review by hand`
            : `names differ — "${base.name}" vs "${suffix.name}"; review by hand`,
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
   * Columns worth carrying over from the losing row, one at a time.
   * Deliberately excludes `externalId` (unique, and the survivor keeps its own
   * identity) and `slug` (handled by resolveSurvivingSlug).
   *
   * Every `curated_*` column has to be on this list or in
   * `INHERITABLE_COLUMN_SETS`, and stay there. A merge deletes the losing row,
   * so a curation that lived only there is gone with no trace and nothing to
   * notice it by — the value simply reverts to whatever the sync last wrote,
   * months after anybody remembers deciding otherwise. Add a curated column to
   * the entity, add it here — or to a set below, if it only means anything
   * beside its neighbours. The spec in `curated-field.spec-list.spec.ts` holds
   * the descriptors against both lists, because this sentence on its own is
   * what the works period was missed by.
   *
   * `openWithPark` is on neither list because it cannot be on this one: the
   * column is NOT NULL, so the winner's value is never absent and the test
   * below never fires. It travels on `INHERITABLE_DEFAULTED_COLUMNS` instead.
   *
   * `curatedIsSeasonal` and `curatedSeasonMonths` stay here rather than moving
   * into a set, although the resolver treats them as one statement. Column by
   * column is what a merge wants for them: a winner curated seasonal but
   * without months should take the loser's months, and a set would refuse. The
   * one crossed pair it can build — a curated `false` next to inherited months
   * — never reaches a reader, because `resolveCuratedFacts` drops the months
   * whenever the resolved seasonality is false.
   */
  static readonly INHERITABLE_COLUMNS = [
    "queueTimesEntityId",
    "latitude",
    "longitude",
    "landName",
    "landExternalId",
    "minimumHeight",
    "maximumHeight",
    "mayGetWet",
    "curatedMayGetWet",
    "curatedMinimumHeight",
    "curatedMaximumHeight",
    "curatedName",
    "curatedLandName",
    "curatedAttractionType",
    "curatedIsSeasonal",
    "curatedSeasonMonths",
    "retiredAt",
    "retiredReason",
    "hasSingleRider",
    "hasFastPass",
    "fastPassName",
    "fastPassPrice",
    "rcdbId",
    "seasonMonths",
  ] as const;

  /**
   * Groups of columns that only ever travel together.
   *
   * The works period is three columns holding one statement, and two rules
   * guard it on the way in: `AdminCurationService` rejects an end before its
   * start, and clears "that end is only an estimate" whenever the end goes
   * away. Column-by-column inheritance breaks both. A winner with a start and
   * no end takes the loser's end and carries a window that ends before it
   * begins — the pair the endpoint refuses, and since PAR-287 one that is
   * served to readers as `worksPeriod`. A winner with no dates at all takes
   * the loser's bare `toUncertain` and hedges a date it does not have.
   *
   * So the set moves together or not at all: the winner inherits the columns
   * the loser holds, and only when it holds none of the three itself. That is
   * all three for a window with both dates and an estimate flag, and one for
   * the ordinary open-ended one: `AdminCurationService` clears the flag
   * whenever the end date goes, so a start with no end carries a null the
   * filter never picks up. What `settle` drops below is the flag that stayed —
   * written by hand, past the endpoint that would have cleared it.
   *
   * `settle` then asks of the window that is about to travel what the endpoint
   * asks of one being typed. It has to, and not because the endpoint is
   * careless: these columns are reachable by hand, and unlike a value the
   * merge merely reads, this one is written onto a row that outlives it. A
   * window stating nothing (no date at all) stays behind; so does an inverted
   * one, which is exactly the state the set exists to prevent and would be no
   * better for having arrived whole. A stale estimate flag is dropped rather
   * than made to sink the dates beside it — that is the endpoint's own
   * normalisation, and the dates are a real curation worth carrying.
   *
   * Whatever stays behind on any of those routes is named rather than dropped
   * in silence: `planMetadataInheritance` returns it, the rehearsal lists it
   * and the merge logs it. Refusing a window is the right rule and still
   * deletes a hand-written one (PAR-301).
   */
  private static readonly INHERITABLE_COLUMN_SETS: readonly {
    readonly columns: readonly (keyof Attraction)[];
    /** The values to write, or null to leave the whole set behind. */
    readonly settle: (
      carried: Partial<Attraction>,
    ) => Partial<Attraction> | null;
  }[] = [
    {
      columns: [
        "curatedOutOfServiceFrom",
        "curatedOutOfServiceTo",
        "curatedOutOfServiceToUncertain",
      ],
      settle: (carried) => {
        const from = carried.curatedOutOfServiceFrom ?? null;
        const to = carried.curatedOutOfServiceTo ?? null;

        // It has to be a window at all. `AdminCurationService` refuses this
        // pair outright, so a row carrying it never came through it.
        if (worksPeriodEndsBeforeItBegins(from, to)) return null;

        // The flag qualifies the end date and means nothing without one. The
        // endpoint clears it and keeps the dates; so does this.
        //
        // This is also what leaves a window of nothing but a flag behind: with
        // the flag dropped there is nothing left to assign, so "a bare
        // toUncertain is not a window" needs no rule of its own — and a rule
        // no mutation can turn red would read like one that does something.
        if (!isSet(to) && "curatedOutOfServiceToUncertain" in carried) {
          const { curatedOutOfServiceToUncertain: _dropped, ...rest } = carried;
          return rest;
        }
        return carried;
      },
    },
  ];

  /**
   * Columns whose "nobody decided anything" is a value rather than an absence.
   *
   * `INHERITABLE_COLUMNS` above asks whether the winner's cell is empty, which
   * is the right question for a curated column that starts as null. It is the
   * wrong one for `open_with_park`: the column is `boolean NOT NULL DEFAULT
   * false`, so the winner always holds a value and the test never fires. A
   * merge whose loser was marked free-flow and whose winner sat on the default
   * dropped the statement with nothing to notice it by.
   *
   * Reading that stored `false` as "nobody said" is not a new decision — it is
   * the one the editor already runs on. `ATTRACTION_CURATED_FIELDS` gives
   * `openWithPark` a `defaultValue` of `false`, described as what the column
   * holds when nobody has decided anything, and `attractionFieldViews` scores
   * `overridden` against that value rather than against null, which is why the
   * catalogue does not wear a curated badge on every row. So `unset` here is
   * that same value, and `curated-field.spec-list.spec.ts` holds the two
   * against each other: declaring a different one is red, not silent.
   *
   * The rule stays one-directional on purpose. A loser's `false` never
   * overwrites a winner's `true`, because a `false` is the absence of a
   * statement and an absence may not unset one (`docs/rules/absent-facts.md`).
   *
   * Measured before the change (production, 2026-09-18): of the 47 duplicate
   * pairs `findDuplicatePairs` offers, none holds `open_with_park` on either
   * side, so no pair on that list loses anything today. The pair this guards
   * is the hand-named one — `POST /admin/merge-duplicate-attractions` takes a
   * `winnerId`/`loserId` of its caller's choosing and never consults that list.
   */
  static readonly INHERITABLE_DEFAULTED_COLUMNS = [
    { column: "openWithPark", unset: false },
  ] as const satisfies readonly {
    column: keyof Attraction;
    unset: unknown;
  }[];

  /** The set columns, flat — for the spec that holds both lists against the descriptors. */
  static readonly INHERITABLE_SET_COLUMNS: readonly string[] =
    AttractionMergeService.INHERITABLE_COLUMN_SETS.flatMap(
      (set) => set.columns as readonly string[],
    );

  /**
   * Which columns travel, and which ones the DELETE takes with it.
   *
   * The second half exists because a refused set is silent in a way a refused
   * single column is not: the works period is the one curation whose loss the
   * survivor cannot even hint at, since a survivor holding a window of its own
   * is exactly the case in which the loser's is refused. Three routes end
   * there, and the report does not distinguish them — for the operator the
   * question is only which values will be gone:
   *
   * - the survivor holds any of the set, so the loser's is refused whole (the
   *   common one, and the reason PAR-297's rule is right rather than a gap);
   * - `settle` refuses the window as inverted;
   * - `settle` drops a `toUncertain` that arrived without its date.
   */
  private planMetadataInheritance(
    winner: Attraction,
    loser: Attraction,
  ): MetadataInheritancePlan {
    const inherited: Record<string, unknown> = {};
    const droppedColumnSets: Record<string, unknown>[] = [];

    for (const column of AttractionMergeService.INHERITABLE_COLUMNS) {
      if (!isSet(winner[column]) && isSet(loser[column])) {
        inherited[column] = loser[column];
      }
    }

    for (const {
      column,
      unset,
    } of AttractionMergeService.INHERITABLE_DEFAULTED_COLUMNS) {
      const loserValue = loser[column];
      if (winner[column] === unset && isSet(loserValue) && loserValue !== unset)
        inherited[column] = loserValue;
    }

    for (const set of AttractionMergeService.INHERITABLE_COLUMN_SETS) {
      const carried: Record<string, unknown> = {};
      for (const column of set.columns.filter((c) => isSet(loser[c]))) {
        carried[column] = loser[column];
      }

      // The losing row states nothing, so there is nothing to carry and
      // nothing to report. Asked before the survivor is, because a preview
      // that named a loss on every pair holding a window would be read as
      // noise and stop being read at all.
      if (Object.keys(carried).length === 0) continue;

      const settled = set.columns.some((column) => isSet(winner[column]))
        ? null
        : set.settle(carried as Partial<Attraction>);

      if (settled !== null) Object.assign(inherited, settled);

      const dropped = Object.fromEntries(
        Object.entries(carried).filter(
          ([column]) => settled === null || !(column in settled),
        ),
      );
      if (Object.keys(dropped).length > 0) droppedColumnSets.push(dropped);
    }

    return { inherited: inherited as Partial<Attraction>, droppedColumnSets };
  }

  /**
   * The last record of a curated window that is about to cease to exist.
   *
   * Called from the merge and never from the rehearsal, which is the whole
   * reason the plan above returns the values instead of logging them itself:
   * a warning inside the shared function would fire on every "Probelauf" and
   * describe a deletion that is not happening. `ParkMergeService` strikes the
   * same bargain on the park side (`logDroppedCuration`).
   */
  private logDroppedColumnSets(
    loser: Attraction,
    droppedColumnSets: Record<string, unknown>[],
  ): void {
    for (const dropped of droppedColumnSets) {
      this.logger.warn(
        `🗑️  Deleting "${loser.name}" (${loser.id}) with curated values the survivor will not carry: ` +
          Object.entries(dropped)
            .map(([column, value]) => `${column}=${JSON.stringify(value)}`)
            .join(", "),
      );
    }
  }
}
