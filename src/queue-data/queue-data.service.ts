import { Injectable, Logger, Inject } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { randomUUID } from "crypto";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { Repository, Between, LessThanOrEqual, MoreThanOrEqual } from "typeorm";
import { QueueData } from "./entities/queue-data.entity";
import { ForecastData } from "./entities/forecast-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import {
  EntityLiveResponse,
  QueueType,
} from "../external-apis/themeparks/themeparks.types";
import { ParksService } from "../parks/parks.service";
import {
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";

/** One attraction's already-fetched live payload, as handed to the batch writer. */
export interface LiveDataBatchItem {
  attractionId: string;
  liveData: EntityLiveResponse;
  source?: string;
}

/** A queue_data row the upstream payload could produce, before the delta check. */
interface QueueCandidate {
  attractionId: string;
  queueType: QueueType;
  data: Partial<QueueData>;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Postgres `foreign_key_violation`. */
const FK_VIOLATION = "23503";

function isForeignKeyViolation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === FK_VIOLATION
  );
}

/**
 * How long an attraction stays on the skip list after its row was rejected for
 * not existing.
 *
 * Long enough that a dead id costs one failed insert an hour rather than one
 * every poll cycle, short enough that a re-created attraction starts recording
 * again on its own. Nothing clears this list deliberately: the point is that it
 * heals without anybody watching it.
 */
const ORPHAN_TTL_MS = 60 * 60 * 1000;

/**
 * Revives a cached "latest" entry. A corrupt entry counts as a cache MISS
 * (→ DB lookup) rather than throwing.
 */
function parseCachedLatest(raw: string | null): Partial<QueueData> | null {
  if (!raw) return null;
  try {
    const p = JSON.parse(raw);
    return {
      ...p,
      returnStart: p.returnStart ? new Date(p.returnStart) : null,
      returnEnd: p.returnEnd ? new Date(p.returnEnd) : null,
      timestamp: p.timestamp ? new Date(p.timestamp) : null,
    };
  } catch {
    return null;
  }
}

/**
 * Queue Data Service
 *
 * Handles storing wait times and live data for attractions.
 * Supports all 6 queue types from ThemeParks.wiki.
 *
 * Delta Strategy:
 * - Only store when waitTime changes by > 5 minutes
 * - Always store status changes (OPERATING → CLOSED, etc.)
 * - Store all virtual queue updates (return time windows)
 */
@Injectable()
export class QueueDataService {
  private readonly logger = new Logger(QueueDataService.name);

  /**
   * Attractions whose queue rows the database rejects because the attraction is
   * gone, and when each was last rejected.
   *
   * The live feed keeps reporting a ride for a while after it is deleted or
   * merged away here, and every one of those rows fails the `queue_data` foreign
   * key. That failure is not free: the whole poll's bulk INSERT aborts on the
   * first bad row, so a sixty-ride park falls back to sixty single-row inserts
   * every five minutes, and TimescaleDB logs the constraint violation each time.
   * One dead id was costing a park its bulk write for as long as upstream kept
   * listing it.
   *
   * In-process rather than Redis on purpose: it is a hint, not a fact. Losing it
   * on a restart costs exactly one more failed insert, which is the price of the
   * first cycle anyway, and it keeps the hot path free of another round-trip.
   */
  private readonly orphanedAttractions = new Map<string, number>();
  private readonly LATEST_CACHE_TTL = 10 * 60; // 10 min — covers 2 sync cycles

  constructor(
    @InjectRepository(QueueData)
    private queueDataRepository: Repository<QueueData>,
    @InjectRepository(ForecastData)
    private forecastDataRepository: Repository<ForecastData>,
    @InjectRepository(Attraction)
    private attractionRepository: Repository<Attraction>,
    private parksService: ParksService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  private latestCacheKey(attractionId: string, queueType: QueueType): string {
    return `parkfan:queue:latest:${attractionId}:${queueType}`;
  }

  private attractionTimezoneCacheKey(attractionId: string): string {
    return `parkfan:attraction:tz:${attractionId}`;
  }

  /**
   * Fetch historical queue data for a specific park and date range.
   * Optimized for calendar generation.
   *
   * @param parkId - Park ID
   * @param fromDate - Start date
   * @param toDate - End date
   * @returns List of queue data entries
   */
  async findHistoricalDataForDateRange(
    parkId: string,
    fromDate: Date,
    toDate: Date,
  ): Promise<QueueData[]> {
    return this.queueDataRepository
      .createQueryBuilder("qd")
      .innerJoin("qd.attraction", "a")
      .where("a.parkId = :parkId", { parkId })
      .andWhere("qd.timestamp >= :from", { from: fromDate })
      .andWhere("qd.timestamp <= :to", { to: toDate })
      .andWhere("qd.waitTime IS NOT NULL")
      .andWhere("qd.waitTime > 0")
      .orderBy("qd.timestamp", "ASC")
      .getMany();
  }

  /**
   * Saves queue data for an attraction from ThemeParks.wiki live API response.
   *
   * @param attractionId - Our internal attraction ID (UUID)
   * @param liveData - Live data from ThemeParks.wiki API
   * @param source - Optional data source name (e.g., 'queue-times')
   */
  async saveLiveData(
    attractionId: string,
    liveData: EntityLiveResponse,
    source?: string,
  ): Promise<number> {
    const saved = await this.saveLiveDataBatch([
      { attractionId, liveData, source },
    ]);
    return saved.get(attractionId) ?? 0;
  }

  /**
   * Batched variant of {@link saveLiveData} for a whole poll cycle.
   *
   * The single-attraction path costs 1 Redis GET per queue type (plus a DB
   * SELECT on a cache miss) and 1 INSERT + 1 Redis SET per written row — all
   * strictly sequential, so a 60-ride park meant ~180 round-trips every 5
   * minutes. This collapses an entire batch into:
   *   1 MGET + at most 2 SELECTs (latest-on-miss, timezones)
   *   + 1 bulk INSERT + 1 Redis pipeline.
   *
   * The delta decision itself is unchanged — {@link isSignificantChange} holds
   * the exact rules the per-attraction path used, and `saveLiveData` now runs
   * through this same code so the two can never drift apart.
   *
   * NOTE: upstream fetching stays sequential/rate-limited by the caller; this
   * only batches what we do with an ALREADY fetched response.
   *
   * @returns attractionId → number of rows written for it
   */
  async saveLiveDataBatch(
    items: LiveDataBatchItem[],
  ): Promise<Map<string, number>> {
    const savedByAttraction = new Map<string, number>();
    if (items.length === 0) return savedByAttraction;

    // One timestamp for the whole batch: these rows all describe the same
    // upstream poll, and a shared value keeps the written entries comparable.
    const now = new Date();

    // Collapse to one candidate per (attraction, queueType). The sequential
    // path compared a repeated entity against the row it had just written; here
    // both would evaluate against the same "latest" and write twice, so the
    // last one wins instead. dedupePollEntities already guards the poll path —
    // this keeps the invariant true for any caller.
    const byKey = new Map<string, QueueCandidate>();
    for (const item of items) {
      for (const candidate of this.buildQueueCandidates(
        item.attractionId,
        item.liveData,
        item.source,
      )) {
        byKey.set(
          `${candidate.attractionId}:${candidate.queueType}`,
          candidate,
        );
      }
    }
    const candidates = [...byKey.values()];
    if (candidates.length === 0) return savedByAttraction;

    // ── 1. Latest known state per (attraction, queueType) — Redis first ──
    const cachedRaw = await this.redis
      .mget(
        ...candidates.map((c) =>
          this.latestCacheKey(c.attractionId, c.queueType),
        ),
      )
      .catch(() => [] as (string | null)[]);

    const latestPerCandidate: (Partial<QueueData> | null)[] = candidates.map(
      (_, i) => parseCachedLatest(cachedRaw[i] ?? null),
    );

    // ── 2. Cache misses in ONE DISTINCT ON query (was: one SELECT each) ──
    const missIdx = latestPerCandidate
      .map((v, i) => (v === null ? i : -1))
      .filter((i) => i >= 0);

    if (missIdx.length > 0) {
      const missAttractionIds = [
        ...new Set(missIdx.map((i) => candidates[i].attractionId)),
      ];
      // Same 24h bound the per-attraction lookup used — it lets TimescaleDB
      // exclude old chunks instead of walking each attraction's full history.
      const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      try {
        const rows = await this.queueDataRepository
          .createQueryBuilder("qd")
          .where("qd.attractionId IN (:...ids)", { ids: missAttractionIds })
          .andWhere("qd.timestamp >= :since", { since: oneDayAgo })
          .distinctOn(["qd.attractionId", "qd.queueType"])
          .orderBy("qd.attractionId", "ASC")
          .addOrderBy("qd.queueType", "ASC")
          .addOrderBy("qd.timestamp", "DESC")
          .getMany();

        const dbLatest = new Map<string, QueueData>(
          rows.map((r) => [`${r.attractionId}:${r.queueType}`, r]),
        );
        for (const i of missIdx) {
          const c = candidates[i];
          latestPerCandidate[i] =
            dbLatest.get(`${c.attractionId}:${c.queueType}`) ?? null;
        }
      } catch (error) {
        // A failed lookup must not silently swallow the poll: treat every miss
        // as "no previous data" (→ save), which is the safe direction.
        this.logger.error(
          `Batch latest-queue-data lookup failed: ${errorMessage(error)}`,
        );
      }
    }

    // ── 3. Timezones (only needed where a previous row exists) ──
    const tzMap = await this.resolveTimezones([
      ...new Set(
        candidates
          .filter((_, i) => latestPerCandidate[i]?.timestamp)
          .map((c) => c.attractionId),
      ),
    ]);

    // ── 4. Delta decision (pure, no I/O) ──
    const toInsert: QueueData[] = [];
    for (let i = 0; i < candidates.length; i++) {
      const c = candidates[i];
      const latest = latestPerCandidate[i];
      const timezone = tzMap.get(c.attractionId) ?? "UTC";
      if (!this.isSignificantChange(latest, c.data, c.queueType, timezone)) {
        continue;
      }
      const entry = this.queueDataRepository.create(c.data);
      // Set explicitly instead of relying on the @BeforeInsert hook so the
      // written row is identical whether it goes through insert() or save().
      entry.id = randomUUID();
      entry.timestamp = now;
      toInsert.push(entry);
    }

    if (toInsert.length === 0) return savedByAttraction;

    // ── 5. One bulk INSERT (was: one save() per row) ──
    const writable = this.withoutOrphans(toInsert);
    if (writable.length === 0) return savedByAttraction;
    const written = await this.insertQueueRows(writable);
    if (written.length === 0) return savedByAttraction;

    for (const entry of written) {
      savedByAttraction.set(
        entry.attractionId,
        (savedByAttraction.get(entry.attractionId) ?? 0) + 1,
      );
    }

    // ── 6. One pipeline for all cache writes (was: one SET per row) ──
    const pipeline = this.redis.pipeline();
    for (const entry of written) {
      pipeline.set(
        this.latestCacheKey(entry.attractionId, entry.queueType),
        JSON.stringify(entry),
        "EX",
        this.LATEST_CACHE_TTL,
      );
    }
    await pipeline
      .exec()
      .catch((e) =>
        this.logger.debug(
          `Redis latest-cache pipeline failed for ${written.length} entr${
            written.length === 1 ? "y" : "ies"
          }: ${errorMessage(e)}`,
        ),
      );

    return savedByAttraction;
  }

  /**
   * Bulk-insert with a per-row fallback: one malformed row must not cost the
   * whole park's poll (the per-attraction path failed in isolation, and this
   * keeps that property).
   */
  /**
   * Drops rows for attractions the database has already rejected as missing,
   * and forgets an id once its entry has aged out so a re-created attraction
   * records again without a deploy.
   */
  private withoutOrphans(rows: QueueData[]): QueueData[] {
    if (this.orphanedAttractions.size === 0) return rows;
    const cutoff = Date.now() - ORPHAN_TTL_MS;
    for (const [id, at] of this.orphanedAttractions) {
      if (at < cutoff) this.orphanedAttractions.delete(id);
    }
    if (this.orphanedAttractions.size === 0) return rows;
    return rows.filter((r) => !this.orphanedAttractions.has(r.attractionId));
  }

  private async insertQueueRows(rows: QueueData[]): Promise<QueueData[]> {
    try {
      await this.queueDataRepository.insert(rows);
      return rows;
    } catch (error) {
      this.logger.warn(
        `Bulk queue_data insert of ${rows.length} row(s) failed (${errorMessage(
          error,
        )}) — retrying row by row`,
      );
      const written: QueueData[] = [];
      for (const row of rows) {
        try {
          await this.queueDataRepository.insert(row);
          written.push(row);
        } catch (rowError) {
          // An attraction that no longer exists is a different kind of failure
          // from a transient one, and the only one worth remembering: it will
          // fail again in five minutes, and again after that.
          if (isForeignKeyViolation(rowError)) {
            const first = !this.orphanedAttractions.has(row.attractionId);
            this.orphanedAttractions.set(row.attractionId, Date.now());
            if (first) {
              this.logger.warn(
                `Attraction ${row.attractionId} is not in the attractions table — ` +
                  `skipping its queue rows for the next hour. Upstream is still ` +
                  `reporting a ride that was deleted or merged away here.`,
              );
            }
            continue;
          }
          this.logger.error(
            `❌ Failed to save ${row.queueType} queue data for ${row.attractionId}: ${errorMessage(rowError)}`,
          );
        }
      }
      return written;
    }
  }

  /**
   * attractionId → park timezone, Redis-first with a single DB fallback query.
   */
  private async resolveTimezones(
    attractionIds: string[],
  ): Promise<Map<string, string>> {
    const tzMap = new Map<string, string>();
    if (attractionIds.length === 0) return tzMap;

    const cached = await this.redis
      .mget(...attractionIds.map((id) => this.attractionTimezoneCacheKey(id)))
      .catch(() => [] as (string | null)[]);

    const missing: string[] = [];
    attractionIds.forEach((id, i) => {
      const tz = cached[i];
      if (tz) tzMap.set(id, tz);
      else missing.push(id);
    });

    if (missing.length === 0) return tzMap;

    try {
      const rows = await this.attractionRepository
        .createQueryBuilder("a")
        .innerJoin("a.park", "p")
        .select("a.id", "id")
        .addSelect("p.timezone", "timezone")
        .where("a.id IN (:...ids)", { ids: missing })
        .getRawMany<{ id: string; timezone: string | null }>();

      const pipeline = this.redis.pipeline();
      for (const row of rows) {
        if (!row.timezone) continue;
        tzMap.set(row.id, row.timezone);
        pipeline.set(
          this.attractionTimezoneCacheKey(row.id),
          row.timezone,
          "EX",
          3600,
        );
      }
      await pipeline.exec().catch(() => undefined);
    } catch (error) {
      // Falling back to UTC only affects the "new park-local day" check, which
      // the >60min heartbeat rule covers shortly after anyway.
      this.logger.debug(`Timezone lookup failed: ${errorMessage(error)}`);
    }

    return tzMap;
  }

  /**
   * Maps one upstream live payload to the queue_data rows it could produce.
   * Pure — no I/O, so both the single and the batch path share it verbatim.
   */
  private buildQueueCandidates(
    attractionId: string,
    liveData: EntityLiveResponse,
    source?: string,
  ): QueueCandidate[] {
    if (!liveData.queue) {
      // Record any explicit status change even without queue data.
      // Covers: CLOSED/DOWN/REFURBISHMENT (no queue expected) AND OPERATING for
      // attractions without posted wait times (walk-throughs, free-flow rides).
      if (!liveData.status) return [];
      return [
        {
          attractionId,
          queueType: QueueType.STANDBY,
          data: {
            attractionId,
            queueType: QueueType.STANDBY,
            status: liveData.status,
            dataSource: source || "themeparks-wiki",
            lastUpdated: liveData.lastUpdated
              ? new Date(liveData.lastUpdated)
              : new Date(),
            // Non-OPERATING statuses have 0 wait; OPERATING without queue has no posted time
            waitTime: liveData.status !== "OPERATING" ? 0 : undefined,
          },
        },
      ];
    }

    const candidates: QueueCandidate[] = [];
    const queueTypes = Object.keys(liveData.queue) as QueueType[];

    for (const queueType of queueTypes) {
      const queueInfo =
        liveData.queue[queueType as keyof typeof liveData.queue];

      if (!queueInfo) continue;

      // Prepare queue data based on type
      const queueData: Partial<QueueData> = {
        attractionId,
        queueType,
        status: liveData.status,
        dataSource: source || "themeparks-wiki",
        lastUpdated: liveData.lastUpdated
          ? new Date(liveData.lastUpdated)
          : null,
      };

      // Map fields based on queue type
      switch (queueType) {
        case QueueType.STANDBY:
        case QueueType.SINGLE_RIDER:
        case QueueType.PAID_STANDBY:
          if ("waitTime" in queueInfo) {
            const raw = queueInfo.waitTime;
            if (typeof raw === "number" && raw > 360) {
              this.logger.warn(
                `Discarding implausible wait time ${raw} min for attraction ${attractionId}`,
              );
              // Leave queueData.waitTime unset (null) — status change still saved
            } else {
              queueData.waitTime = raw;
            }
          }
          break;

        case QueueType.RETURN_TIME:
        case QueueType.PAID_RETURN_TIME:
          if ("state" in queueInfo) {
            queueData.state = queueInfo.state;
            queueData.returnStart = queueInfo.returnStart
              ? new Date(queueInfo.returnStart)
              : undefined;
            queueData.returnEnd = queueInfo.returnEnd
              ? new Date(queueInfo.returnEnd)
              : undefined;
          }
          if (
            queueType === QueueType.PAID_RETURN_TIME &&
            "price" in queueInfo
          ) {
            queueData.price = queueInfo.price;
          }
          break;

        case QueueType.BOARDING_GROUP:
          if ("allocationStatus" in queueInfo) {
            queueData.allocationStatus = queueInfo.allocationStatus;
            queueData.currentGroupStart = queueInfo.currentGroupStart;
            queueData.currentGroupEnd = queueInfo.currentGroupEnd;
            queueData.estimatedWait = queueInfo.estimatedWait;
          }
          break;
      }

      candidates.push({ attractionId, queueType, data: queueData });
    }

    return candidates;
  }

  /**
   * Delta strategy: Only save if data has changed significantly.
   *
   * Save when:
   * - No previous data exists
   * - Wait time changed by > 5 minutes
   * - Status changed (OPERATING → CLOSED, etc.)
   * - Virtual queue return time windows changed
   */
  private isSignificantChange(
    latest: Partial<QueueData> | null,
    newData: Partial<QueueData>,
    queueType: QueueType,
    timezone: string,
  ): boolean {
    // No previous data → save
    if (!latest) {
      return true;
    }

    // Status changed → save
    if (latest.status !== newData.status) {
      return true;
    }

    // Wait time changed → save
    if (newData.waitTime !== undefined && latest.waitTime !== undefined) {
      if (Number(newData.waitTime) !== Number(latest.waitTime)) {
        return true;
      }
    }

    // Return time window changed → save
    if (
      queueType === QueueType.RETURN_TIME ||
      queueType === QueueType.PAID_RETURN_TIME
    ) {
      if (newData.returnStart && latest.returnStart) {
        if (
          latest.returnStart?.getTime() !== newData.returnStart?.getTime() ||
          latest.returnEnd?.getTime() !== newData.returnEnd?.getTime()
        ) {
          return true;
        }
      } else if (newData.returnStart !== latest.returnStart) {
        // One is null/undefined, the other is not -> changed
        // (handling null vs undefined equality manually if needed, but here simple mismatch is enough trigger?)
        // Actually, be careful: undefined (new) vs null (old) should be false if both mean "no value".
        const newStart = newData.returnStart ?? null;
        const oldStart = latest.returnStart ?? null;
        if (newStart !== oldStart) return true;
      }
    }

    // Boarding group allocation changed → save
    if (queueType === QueueType.BOARDING_GROUP) {
      // Normalize to null for comparison
      const newStatus = newData.allocationStatus ?? null;
      const oldStatus = latest.allocationStatus ?? null;
      if (newStatus !== oldStatus) return true;

      const newStart = newData.currentGroupStart ?? null;
      const oldStart = latest.currentGroupStart ?? null;
      if (newStart !== oldStart) return true;

      const newEnd = newData.currentGroupEnd ?? null;
      const oldEnd = latest.currentGroupEnd ?? null;
      if (newEnd !== oldEnd) return true;
    }

    // Date changed → save (ensure at least one data point per day)
    // This fixes the issue where "Closed" status persists from yesterday and we ignore today's "Closed" update
    if (latest.timestamp) {
      // `timezone` is resolved once per batch (Redis-first, single DB fallback)
      // in resolveTimezones() — the caller passes the park timezone in, or "UTC"
      // when it could not be determined.
      //
      // Guarded: a corrupt cached timestamp or timezone must not throw. This
      // check runs inside a whole park's batch now, so an unguarded throw would
      // cost every ride in that park its reading — the per-attraction path only
      // ever lost one. The >60min heartbeat rule below still forces a write.
      try {
        const latestDateStr = formatInParkTimezone(latest.timestamp, timezone);
        const currentDateStr = getCurrentDateInTimezone(timezone);
        if (latestDateStr !== currentDateStr) {
          return true;
        }
      } catch {
        // fall through to the heartbeat rule
      }
    }

    // Heartbeat: Force save if last update was > 60 minutes ago
    // This ensures we have recent data ("Yes, it's still closed") even if nothing changed
    if (latest.timestamp) {
      const now = new Date();
      const lastUpdate = new Date(latest.timestamp);
      const diffMinutes = (now.getTime() - lastUpdate.getTime()) / (1000 * 60);
      if (diffMinutes > 60) {
        return true;
      }
    }

    // No significant change
    return false;
  }

  /**
   * Find wait times for an attraction with optional date range and queue type filtering
   *
   * @param attractionId - Attraction ID
   * @param options - Query options (from, to, queueType, page, limit)
   * @returns Queue data with pagination info
   */
  async findWaitTimesByAttraction(
    attractionId: string,
    options: {
      from?: Date;
      to?: Date;
      queueType?: QueueType;
      page?: number;
      limit?: number;
    } = {},
  ): Promise<{ data: QueueData[]; total: number }> {
    const { from, to, queueType, page = 1, limit = 10 } = options;

    const whereClause: Record<string, unknown> = { attractionId };

    // Add date range filter
    if (from && to) {
      whereClause.timestamp = Between(from, to);
    } else if (from) {
      whereClause.timestamp = MoreThanOrEqual(from);
    } else if (to) {
      whereClause.timestamp = LessThanOrEqual(to);
    }

    // Add queue type filter
    if (queueType) {
      whereClause.queueType = queueType;
    }

    // Query with pagination
    const [data, total] = await this.queueDataRepository.findAndCount({
      where: whereClause,
      relations: ["attraction", "attraction.park"],
      order: { timestamp: "DESC" },
      skip: (page - 1) * limit,
      take: limit,
    });

    return { data, total };
  }

  /**
   * Find current status for an attraction (most recent queue data)
   *
   * Uses park opening hours to determine valid data cutoff instead of fixed time window.
   * If park is open today, filters data from today's opening time.
   * Falls back to maxAgeMinutes if no schedule available.
   *
   * @param attractionId - Attraction ID
   * @param maxAgeMinutes - Fallback maximum age in minutes (optional, default: 6 hours)
   * @returns Most recent queue data for all queue types
   */
  async findCurrentStatusByAttraction(
    attractionId: string,
    maxAgeMinutes?: number,
  ): Promise<QueueData[]> {
    // Get attraction to find park
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId },
      select: ["parkId"],
    });

    let cutoff: Date | undefined;
    if (attraction?.parkId) {
      cutoff = await this.getValidDataCutoff(attraction.parkId, maxAgeMinutes);
    } else if (maxAgeMinutes) {
      // Fallback if no park found
      cutoff = new Date(Date.now() - maxAgeMinutes * 60 * 1000);
    }

    // Use DISTINCT ON optimization to get latest record per queueType efficiently
    // This replaces N queries (one per queue type) with a single query
    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .where("qd.attractionId = :attractionId", { attractionId })
      .distinctOn(["qd.queueType"])
      .orderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC");

    if (cutoff) {
      query.andWhere("qd.timestamp >= :cutoff", { cutoff });
    }

    return query.getMany();
  }

  /**
   * Find current status for multiple attractions (batch query)
   *
   * Note: This method doesn't use park-specific opening hours due to performance.
   * It uses maxAgeMinutes as a simple fallback. For park-specific filtering,
   * use findCurrentStatusByPark instead.
   *
   * @param attractionIds - Array of Attraction IDs
   * @param maxAgeMinutes - Maximum age of queue data in minutes (optional, default: 6 hours)
   * @returns Map of attractionId -> QueueData[]
   */
  async findCurrentStatusByAttractionIds(
    attractionIds: string[],
    maxAgeMinutes?: number,
  ): Promise<Map<string, QueueData[]>> {
    if (attractionIds.length === 0) {
      return new Map();
    }

    // Use DISTINCT ON optimization like findCurrentStatusByPark
    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .where("qd.attractionId IN (:...attractionIds)", { attractionIds })
      .distinctOn(["qd.attractionId", "qd.queueType"])
      .orderBy("qd.attractionId", "ASC")
      .addOrderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC");

    // For batch queries, use simple time-based filter
    // (park-specific filtering would require N schedule lookups)
    const fallbackMinutes = maxAgeMinutes ?? 6 * 60; // Default: 6 hours
    const cutoff = new Date(Date.now() - fallbackMinutes * 60 * 1000);
    query.andWhere("qd.timestamp >= :cutoff", { cutoff });

    const queueData = await query.getMany();

    const result = new Map<string, QueueData[]>();
    for (const data of queueData) {
      if (!result.has(data.attractionId)) {
        result.set(data.attractionId, []);
      }
      result.get(data.attractionId)!.push(data);
    }

    return result;
  }

  /**
   * Find current status for all attractions in a park (bulk query optimization)
   *
   * This is a performance-optimized version that fetches queue data for all attractions
   * in a single query instead of N queries (one per attraction).
   *
   * Uses park opening hours to determine valid data cutoff instead of fixed time window.
   * If park is open today, filters data from today's opening time.
   * Falls back to maxAgeMinutes if no schedule available.
   *
   * Uses PostgreSQL DISTINCT ON to get latest record per (attractionId, queueType) efficiently.
   * Requires composite index on (attractionId, queueType, timestamp) for optimal performance.
   *
   * @param parkId - Park ID
   * @param maxAgeMinutes - Fallback maximum age in minutes (optional, default: 6 hours)
   * @returns Map of attractionId -> QueueData[] (current status for all queue types)
   */
  async findCurrentStatusByPark(
    parkId: string,
    maxAgeMinutes?: number,
  ): Promise<Map<string, QueueData[]>> {
    // Get valid data cutoff based on park opening hours
    const cutoff = await this.getValidDataCutoff(parkId, maxAgeMinutes);

    // Use DISTINCT ON to get latest timestamp for each (attractionId, queueType) combination
    // This replaces the O(n²) correlated subquery with a single index scan
    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .innerJoin("qd.attraction", "attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .distinctOn(["qd.attractionId", "qd.queueType"])
      .orderBy("qd.attractionId", "ASC")
      .addOrderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC"); // Latest first within each group

    if (cutoff) {
      query.andWhere("qd.timestamp >= :cutoff", { cutoff });
    }

    const queueData = await query.getMany();

    // Group by attractionId
    const result = new Map<string, QueueData[]>();
    for (const data of queueData) {
      if (!result.has(data.attractionId)) {
        result.set(data.attractionId, []);
      }
      result.get(data.attractionId)!.push(data);
    }

    return result;
  }

  /**
   * Get valid data cutoff based on park opening hours
   *
   * Uses park timezone to determine "today" and checks if park is scheduled to operate today.
   * If park has an OPERATING schedule for today (even if not yet open), uses today's opening time.
   * This ensures we keep all data from when the park opens, even if > 6 hours.
   *
   * **Today's opening time may only widen this window, never narrow it.** A queue row is
   * written when a value changes, plus an hourly heartbeat, so the current reading for a ride
   * that has not moved carries a timestamp from BEFORE the park opened — and cutting the window
   * at the opening time throws exactly that reading away. Phantasialand opens at 09:00 and its
   * source is polled roughly hourly: between 09:00 and the poll that landed at 09:23, not one
   * of its 40 attractions had a row inside the window. The park payload then took its "no live
   * data" branch for all of them, and for an open park that branch means an optimistically
   * OPERATING ride (`statusWithoutLiveData`) — which is how an ice rink read "geöffnet" in
   * August while the source it comes from had said CLOSED, 40 minutes earlier and every hour
   * before that. The floor keeps the feed's last word, and its last word is the answer.
   *
   * The floor is the same `maxAgeMinutes` window used when there is no schedule, so a reading
   * still expires: a feed that fell silent yesterday evening stays out, and the ride falls
   * through to the optimistic branch exactly as before.
   *
   * Falls back to maxAgeMinutes (default: 6 hours) if:
   * - No schedule available
   * - Park is not scheduled to operate today
   * - Schedule lookup fails
   *
   * @param parkId - Park ID
   * @param maxAgeMinutes - Fallback maximum age in minutes (optional, default: 6 hours)
   * @returns Cutoff date or undefined if no filter should be applied
   */
  private async getValidDataCutoff(
    parkId: string,
    maxAgeMinutes?: number,
  ): Promise<Date | undefined> {
    const fallbackMinutes = maxAgeMinutes ?? 6 * 60; // Default: 6 hours
    const fallbackCutoff = new Date(Date.now() - fallbackMinutes * 60 * 1000);

    try {
      // Get today's schedule (uses park timezone internally)
      const todaySchedule = await this.parksService.getTodaySchedule(parkId);

      // Find today's OPERATING schedule
      // openingTime and closingTime are stored as UTC timestamps
      const operatingSchedule = todaySchedule.find(
        (s) => s.scheduleType === "OPERATING" && s.openingTime,
      );

      if (operatingSchedule?.openingTime) {
        // Park is scheduled to operate today - keep everything since it opened,
        // and never less than the age window (see above: the opening time is a
        // floor under how much history we keep, not a ceiling over it).
        // openingTime is already a UTC timestamp, so we can use it directly.
        const opening = new Date(operatingSchedule.openingTime);
        return opening < fallbackCutoff ? opening : fallbackCutoff;
      }

      // No schedule or park closed today - use fallback
      return fallbackCutoff;
    } catch (error) {
      // If schedule lookup fails, use fallback
      this.logger.warn(
        `Failed to get schedule for park ${parkId}, using fallback:`,
        error,
      );
      return fallbackCutoff;
    }
  }

  /**
   * Get prioritized queue data for attractions (STANDBY preferred, fallback to others)
   *
   * Returns one QueueData per attraction, prioritizing STANDBY queue type.
   * Falls back to other queue types if STANDBY not available.
   * This ensures rides with only virtual queues (RETURN_TIME, BOARDING_GROUP, etc.) are counted.
   *
   * @param parkId - Park ID
   * @param maxAgeMinutes - Maximum age of queue data in minutes (optional)
   * @returns Map of attractionId -> QueueData (single prioritized queue per attraction)
   */
  async findPrioritizedStatusByPark(
    parkId: string,
    maxAgeMinutes?: number,
  ): Promise<Map<string, QueueData>> {
    // Get all queue types
    const allQueues = await this.findCurrentStatusByPark(parkId, maxAgeMinutes);

    const result = new Map<string, QueueData>();

    for (const [attractionId, queues] of allQueues.entries()) {
      if (queues.length === 0) continue;

      // Prioritize STANDBY, fallback to first available queue type
      const standby = queues.find((q) => q.queueType === QueueType.STANDBY);
      result.set(attractionId, standby || queues[0]);
    }

    return result;
  }

  /**
   * Find forecasts for an attraction
   *
   * @param attractionId - Attraction ID
   * @param hours - Number of hours ahead to fetch (default: 24)
   * @returns Forecast data
   */
  async findForecastsByAttraction(
    attractionId: string,
    hours: number = 24,
  ): Promise<ForecastData[]> {
    const now = new Date();
    const futureTime = new Date(now.getTime() + hours * 60 * 60 * 1000);

    return this.forecastDataRepository.find({
      where: {
        attractionId,
        predictedTime: Between(now, futureTime),
      },
      relations: ["attraction", "attraction.park"],
      order: { predictedTime: "ASC" },
    });
  }

  /**
   * Find current wait times for all attractions in a park
   *
   * @param parkId - Park ID
   * @param queueType - Optional queue type filter
   * @returns Most recent queue data for each attraction in the park
   */
  async findWaitTimesByPark(
    parkId: string,
    queueType?: QueueType,
  ): Promise<QueueData[]> {
    // Latest reading per (attractionId, queueType) via DISTINCT ON + a
    // park-opening-hours cutoff for TimescaleDB chunk exclusion. Replaces a
    // correlated `MAX(timestamp)` subquery that re-scanned the ENTIRE queue_data
    // hypertable (all chunks, incl. compressed) once per attraction — the same
    // pattern findCurrentStatusByPark already uses.
    const cutoff = await this.getValidDataCutoff(parkId);

    const query = this.queueDataRepository
      .createQueryBuilder("qd")
      .innerJoinAndSelect("qd.attraction", "attraction")
      .innerJoinAndSelect("attraction.park", "park")
      .where("park.id = :parkId", { parkId })
      .distinctOn(["qd.attractionId", "qd.queueType"])
      .orderBy("qd.attractionId", "ASC")
      .addOrderBy("qd.queueType", "ASC")
      .addOrderBy("qd.timestamp", "DESC"); // latest first within each group

    if (queueType) {
      query.andWhere("qd.queueType = :queueType", { queueType });
    }
    if (cutoff) {
      query.andWhere("qd.timestamp >= :cutoff", { cutoff });
    }

    const rows = await query.getMany();

    // DISTINCT ON forces ordering by (attractionId, queueType); restore the
    // display order (attraction name) in memory. The set is one row per
    // attraction/queueType for a single park, so this is cheap.
    rows.sort((a, b) =>
      (a.attraction?.name ?? "").localeCompare(b.attraction?.name ?? ""),
    );

    return rows;
  }
  /**
   * Find forecasts for all attractions in a park
   *
   * @param parkId - Park ID
   * @param hours - Number of hours ahead to fetch (default: 24)
   * @returns Forecast data grouped by attraction
   */
  async findForecastsByPark(
    parkId: string,
    hours: number = 24,
  ): Promise<ForecastData[]> {
    const now = new Date();
    const futureTime = new Date(now.getTime() + hours * 60 * 60 * 1000);

    return this.forecastDataRepository
      .createQueryBuilder("forecast")
      .innerJoinAndSelect("forecast.attraction", "attraction")
      .innerJoinAndSelect("attraction.park", "park")
      .where("park.id = :parkId", { parkId })
      .andWhere("forecast.predictedTime BETWEEN :now AND :futureTime", {
        now,
        futureTime,
      })
      .orderBy("forecast.predictedTime", "ASC")
      .getMany();
  }
}
