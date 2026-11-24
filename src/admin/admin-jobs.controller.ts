import {
  BadRequestException,
  Body,
  HttpCode,
  HttpException,
  HttpStatus,
  Inject,
  Logger,
  Param,
  Post,
  Query,
  applyDecorators,
} from "@nestjs/common";
import { ApiOperation, ApiQuery, ApiResponse } from "@nestjs/swagger";
import { InjectQueue } from "@nestjs/bull";
import type { JobOptions, Queue } from "bull";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { QueryInt } from "../common/pipes/query-int.pipe";
import { isRealIsoDate } from "../common/utils/date.util";
import { deleteKeysByPatterns } from "../common/cache/delete-keys-by-patterns";
import { AdminApi } from "./auth/admin-api.decorator";
import { AdminMinRole } from "./auth/admin-auth.decorators";
import type { AdminRole } from "./auth/entities/admin-user.entity";
import { PARK_CACHE_FLUSH_PATTERNS } from "./flush-cache-patterns";
import { SYNC_EFTELING_MAINTENANCE_JOB } from "./content/efteling-maintenance-sync.processor";
import { readBodyFlag } from "./read-body-flag";

/** What a job trigger answers: the job it queued, or the one already queued. */
export interface QueuedJob {
  message: string;
  jobId: string;
}

/**
 * A route on the admin's actions page that queues one job: `POST`, answered
 * with 202 as soon as the job is queued, and at least `editor`.
 */
function JobTrigger(
  path: string,
  doc: { summary: string; description: string; role?: AdminRole },
) {
  return applyDecorators(
    Post(path),
    AdminMinRole(doc.role ?? "editor"),
    HttpCode(HttpStatus.ACCEPTED),
    ApiOperation({ summary: doc.summary, description: doc.description }),
    ApiResponse({ status: 202, description: "Job queued" }),
  );
}

/**
 * The admin's actions page: every route that queues a background job, and
 * the two that delete the park cache.
 *
 * Apart from `AdminController` because these routes share nothing with the
 * read routes there but the prefix: they need the Bull queues and Redis, and
 * nothing else. The MCP `run_job` and `run_maintenance` tools call these
 * handlers, so a job queued from either side gets the same options.
 */
@AdminApi("admin")
export class AdminJobsController {
  private readonly logger = new Logger(AdminJobsController.name);

  constructor(
    @InjectQueue("holidays") private readonly holidaysQueue: Queue,
    @InjectQueue("park-metadata") private readonly parkMetadataQueue: Queue,
    @InjectQueue("park-enrichment")
    private readonly parkEnrichmentQueue: Queue,
    @InjectQueue("ml-training") private readonly mlTrainingQueue: Queue,
    @InjectQueue("nf-training") private readonly nfTrainingQueue: Queue,
    @InjectQueue("wait-times") private readonly waitTimesQueue: Queue,
    @InjectQueue("downtime") private readonly downtimeQueue: Queue,
    @InjectQueue("children-metadata") private readonly childrenQueue: Queue,
    @InjectQueue("six-flags-heights")
    private readonly sixFlagsHeightsQueue: Queue,
    @InjectQueue("ride-stats") private readonly rideStatsQueue: Queue,
    @InjectQueue("manual-metadata")
    private readonly manualMetadataQueue: Queue,
    @InjectQueue("prediction-accuracy") private readonly accuracyQueue: Queue,
    @InjectQueue("analytics") private readonly analyticsQueue: Queue,
    @InjectQueue("pcn-shadow") private readonly pcnShadowQueue: Queue,
    @InjectQueue("park-day-operation")
    private readonly parkDayOperationQueue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** Useful after a change to how holidays are stored. */
  @JobTrigger("sync-holidays", {
    summary: "Trigger holiday sync",
    description:
      "Manually triggers a complete resync of all holidays from Nager.Date API",
  })
  triggerHolidaySync(): Promise<QueuedJob> {
    return this.enqueue(
      this.holidaysQueue,
      "fetch-holidays",
      { priority: 10 },
      "Holiday sync job queued",
    );
  }

  /** Useful for testing duplicate detection and matching improvements. */
  @JobTrigger("sync-parks", {
    summary: "Trigger park metadata sync",
    description:
      "Manually triggers a complete resync of all parks from all sources (Wiki, Queue-Times, Wartezeiten)",
  })
  triggerParkSync(): Promise<QueuedJob> {
    return this.enqueue(
      this.parkMetadataQueue,
      "sync-all-parks",
      { priority: 10 },
      "Park metadata sync job queued",
    );
  }

  @JobTrigger("fill-schedule-gaps", {
    summary: "Fill schedule gaps",
    description:
      "Triggers schedule gap filling to update holiday/bridge day metadata",
  })
  triggerScheduleGapFilling(): Promise<QueuedJob> {
    return this.enqueue(
      this.parkMetadataQueue,
      "fill-all-gaps",
      { priority: 5 },
      "Schedule gap filling job queued",
    );
  }

  /**
   * Writes only the upstream columns: hand-made corrections live in
   * `curated_may_get_wet` and are never touched here.
   */
  @JobTrigger("sync-attraction-details", {
    summary: "Trigger attraction detail sync",
    description:
      "Syncs minimum rider heights from ThemeParks.wiki entity documents (~1 request per attraction). Curated corrections are not overwritten.",
  })
  triggerAttractionDetailsSync(): Promise<QueuedJob> {
    return this.enqueue(
      this.childrenQueue,
      "sync-attraction-details",
      { priority: 10 },
      "Attraction detail sync job queued",
    );
  }

  @JobTrigger("enrich-parks", {
    summary: "Trigger park enrichment",
    description:
      "Manually triggers park enrichment to set countryCode from country names and update influencing regions",
  })
  triggerParkEnrichment(): Promise<QueuedJob> {
    return this.enqueue(
      this.parkEnrichmentQueue,
      "enrich-all",
      { priority: 10 },
      "Park enrichment job queued",
    );
  }

  @JobTrigger("train-ml-model", {
    summary: "Trigger ML training",
    description: "Manually triggers ML model training - takes 1-2 minutes",
  })
  triggerMLTraining(): Promise<QueuedJob> {
    return this.enqueue(
      this.mlTrainingQueue,
      "train-model",
      { priority: 10 },
      "ML training job queued",
    );
  }

  /**
   * Queues a TFT training as the 03:00 UTC cron does. The job skips itself
   * while a training is in flight, so a second click starts nothing.
   */
  @JobTrigger("train-nf", {
    summary: "Trigger TFT training",
    description:
      "Queues a TFT (nf-service) training run, about 40–50 minutes; skipped while one is in flight",
  })
  triggerTftTraining(): Promise<QueuedJob> {
    return this.enqueue(
      this.nfTrainingQueue,
      "train-nf",
      // One attempt, as the cron: a retry would stack a second training on the shared host.
      { priority: 10, attempts: 1 },
      "TFT training job queued",
    );
  }

  /**
   * Queues the calendar warmup as the 08:00 and 20:00 UTC cron does: it evicts
   * and rebuilds every park's forecast months and best-days, then rings the
   * frontend. Needed after a deploy or training that changes served forecasts
   * (docs/deployment/coolify.md). Two warmups at once saturate the database's
   * disk, so while one is running or waiting this answers with that job
   * instead of queueing another.
   */
  @JobTrigger("warmup-calendar", {
    summary: "Trigger calendar warmup",
    description:
      "Queues the calendar warmup and forecast ring; returns the running one instead while a warmup is active or waiting",
  })
  async triggerCalendarWarmup(): Promise<QueuedJob> {
    const pending = (
      await this.parkMetadataQueue.getJobs(["active", "waiting"])
    ).find((job) => job?.name === "warmup-calendar-daily");
    if (pending) {
      return {
        message: "Calendar warmup already running or waiting",
        jobId: pending.id.toString(),
      };
    }
    return this.enqueue(
      this.parkMetadataQueue,
      "warmup-calendar-daily",
      { priority: 10 },
      "Calendar warmup queued",
    );
  }

  /**
   * `train` is the bring-up trigger: the nightly cron runs at 08:30 UTC, so
   * the first models do not exist until then otherwise. `forecast` and `score`
   * are for testing.
   */
  @JobTrigger("pcn/:action", {
    summary: "Trigger a PCN shadow job (train | forecast | score)",
    description:
      "Enqueues the matching pcn-shadow job. 'train' (per-park GP-STGNN) is the " +
      "bring-up trigger; 'forecast' writes pcn_forecasts; 'score' writes the board.",
  })
  triggerPcn(@Param("action") action: string): Promise<QueuedJob> {
    const jobName = {
      train: "train-pcn",
      forecast: "forecast-pcn",
      score: "score-pcn",
    }[action];
    if (!jobName) {
      throw new BadRequestException(
        `Unknown PCN action '${action}' (expected train | forecast | score)`,
      );
    }
    return this.enqueue(
      this.pcnShadowQueue,
      jobName,
      { priority: 10 },
      `PCN ${action} job queued`,
    );
  }

  @JobTrigger("aggregate-accuracy-stats", {
    summary: "Trigger accuracy stats aggregation",
    description:
      "Manually triggers the aggregation of prediction accuracy metrics",
  })
  triggerAccuracyAggregation(): Promise<QueuedJob> {
    return this.enqueue(
      this.accuracyQueue,
      "aggregate-stats",
      { priority: 5 },
      "Accuracy aggregation job queued",
    );
  }

  /**
   * For after a fix that changes the CLOSED-status signal, so the seasonal
   * flags follow without waiting for the nightly run.
   */
  @JobTrigger("detect-seasonal", {
    summary: "Trigger seasonal detection",
    description:
      "Manually triggers the seasonal attraction/show detection job (normally daily at 2:30am)",
  })
  triggerSeasonalDetection(): Promise<QueuedJob> {
    return this.enqueue(
      this.analyticsQueue,
      "detect-seasonal",
      { priority: 5 },
      "Seasonal detection job queued",
    );
  }

  /**
   * Judges a range of past park-local days for measured operation (PAR-697),
   * the fill the nightly 4:45 job never does for days before its first run.
   *
   * One job per call, so the caller chooses the portion: a Coolify deploy
   * renews Postgres and kills whatever is writing, and the portion is the
   * damage radius (G-134). Month slices over the `queue_data` retention window
   * are the intended use; re-running a slice is harmless, the rows upsert.
   */
  @JobTrigger("backfill-park-day-operation", {
    summary: "Backfill measured-operation verdicts",
    description:
      "Queues backfill-park-day-operation for { fromDate, toDate } (YYYY-MM-DD, park-local, inclusive), optionally for one parkId.",
    role: "owner",
  })
  triggerParkDayOperationBackfill(
    @Body() body: { fromDate?: string; toDate?: string; parkId?: string },
  ): Promise<QueuedJob> {
    if (!isRealIsoDate(body?.fromDate) || !isRealIsoDate(body?.toDate)) {
      throw new BadRequestException(
        "fromDate and toDate are required as YYYY-MM-DD",
      );
    }
    if (body.fromDate > body.toDate) {
      throw new BadRequestException("fromDate must not be after toDate");
    }
    return this.enqueue(
      this.parkDayOperationQueue,
      "backfill-park-day-operation",
      { priority: 5 },
      `Measured-operation backfill queued for ${body.fromDate} → ${body.toDate}`,
      {
        fromDate: body.fromDate,
        toDate: body.toDate,
        ...(body.parkId ? { parkId: body.parkId } : {}),
      },
    );
  }

  /**
   * One-time cleanup of the duplicated (attractionId, hour) buckets the old
   * random-uuid upsert left in queue_data_aggregates; the deterministic id
   * prevents new ones. Run in a controlled window — it may decompress old
   * TimescaleDB chunks.
   */
  @JobTrigger("dedupe-percentile-aggregates", {
    summary: "Dedupe percentile aggregates",
    description:
      "Removes historical duplicate (attractionId, hour) rows from queue_data_aggregates, keeping the latest. Idempotent.",
    role: "owner",
  })
  triggerDedupePercentileAggregates(): Promise<QueuedJob> {
    return this.enqueue(
      this.analyticsQueue,
      "dedupe-percentile-aggregates",
      { priority: 5 },
      "Percentile-aggregate dedupe job queued",
    );
  }

  /**
   * The curation is hand-written SQL now, and SQL cannot evict a Redis key or
   * ping the frontend. Without this, a corrected ride surfaces only as the
   * caches age out — `park:integrated` up to 6h, the Cloudflare copy 900s on
   * top, and the frontend pinning whatever it read for a day. Call it after a
   * curation session; it evicts, revalidates, and schedules the second sweep
   * past the CDN window.
   */
  @JobTrigger("publish-ride-profiles", {
    summary: "Publish hand-curated ride profiles",
    description:
      "Evicts the park caches for every ride profile whose `seeded_at` falls " +
      "in the last `sinceHours` (default 24) and tells the frontend to " +
      "revalidate. Set `seeded_at = now()` when you edit a row.",
  })
  triggerPublishRideProfiles(
    @Body() body?: { sinceHours?: number },
  ): Promise<QueuedJob> {
    return this.enqueue(
      this.manualMetadataQueue,
      "publish-ride-profiles",
      { priority: 1 },
      "Ride profile publish queued",
      { sinceHours: body?.sinceHours },
    );
  }

  /** Weekly on its own (PAR-1245); this runs it now. */
  @JobTrigger("sync-efteling-maintenance", {
    summary: "Sync Efteling's maintenance calendar into works periods",
    description:
      "Reads efteling.com's in-maintenance page and adds, moves or removes the " +
      "works periods the sync owns (`source: efteling-maintenance-calendar`). " +
      "Hand-written windows are never touched. Pass `dryRun: true` to see the " +
      "diff in the job result without writing.",
  })
  triggerEftelingMaintenanceSync(
    @Body() body?: { dryRun?: unknown },
  ): Promise<QueuedJob> {
    return this.enqueue(
      this.manualMetadataQueue,
      SYNC_EFTELING_MAINTENANCE_JOB,
      { priority: 1, removeOnComplete: 100 },
      "Efteling maintenance sync queued",
      { dryRun: readBodyFlag(body?.dryRun) === true },
    );
  }

  /**
   * Speed, height, length and duration — none of which exists anywhere else in
   * this system. Joined on the RCDB id we already hold (which came from
   * Wikidata property P2751), a few hundred rides per SPARQL query, so the
   * whole catalogue is a handful of requests. Rows imported in the last 90 days
   * are skipped, and rides Wikidata states nothing for stay eligible for a
   * later run.
   */
  @JobTrigger("import-ride-stats", {
    summary: "Import ride measurements from Wikidata",
    description:
      "Queues a fetch for every curated ride that has an rcdbId and no recent " +
      "import. Pass `limit` for a trial run. Re-runnable and self-limiting.",
  })
  triggerRideStats(@Body() body?: { limit?: number }): Promise<QueuedJob> {
    return this.enqueue(
      this.rideStatsQueue,
      "import-stats",
      { priority: 1 },
      "Ride stat import queued",
      { limit: body?.limit },
    );
  }

  /**
   * ThemeParks.wiki carries no minimumHeight for the Six Flags and former
   * Cedar Fair parks, so this reads it off the parks' own ride pages. Runs
   * weekly on its own; this is for filling the gap on demand.
   */
  @JobTrigger("sync-six-flags-heights", {
    summary: "Fill missing ride heights from the Six Flags sites",
    description:
      "Queues a sweep over every attraction still missing a minimumHeight in " +
      "a Six Flags or former Cedar Fair park. Gap-filling only — an existing " +
      "height is never overwritten.",
  })
  triggerSixFlagsHeights(): Promise<QueuedJob> {
    return this.enqueue(
      this.sixFlagsHeightsQueue,
      "sync-heights",
      { priority: 10 },
      "Six Flags height sync queued",
    );
  }

  /**
   * Rebuild the outage intervals, the exposure days and the profiles.
   *
   * Normally the 5:00 AM cron. Exposed for a targeted repair and for the first
   * run, which has no history to be incremental about.
   */
  @Post("rebuild-downtime")
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Run the downtime reconstruction now",
    description:
      "Rebuilds attraction_outages, attraction_exposure_days and the profiles. " +
      "Publishes nothing on its own: whether a ride shows a figure is decided " +
      "by the gates in DowntimeProfileService.",
  })
  @ApiQuery({
    name: "days",
    required: false,
    description:
      "Trailing window in days, counted back from now. Omitted, the job's own " +
      "DEFAULT_WINDOW_DAYS of 30 applies — not 120. Clamped to 1-400.",
    example: 30,
  })
  async rebuildDowntime(
    @QueryInt({ name: "days", min: 1, max: 400 }) days?: number,
  ): Promise<{ message: string; queue: string }> {
    await this.downtimeQueue.add(
      "reconstruct-downtime",
      { windowDays: days },
      // One attempt. A staged fill's statements get deadlines that grow with
      // the scan (up to ~2.2 h each, PAR-820); the default three attempts
      // would turn one fill that hits them into ~6.7 h of retries.
      { priority: 60, attempts: 1 },
    );
    return { message: "Downtime reconstruction queued", queue: "downtime" };
  }

  /**
   * Clears only park-related cached data (schedules, wait times, analytics,
   * etc.) while preserving Bull queue jobs and system caches. Bull shares this
   * Redis under `<BULL_PREFIX>:<queue>:*` (default prefix `parkfan`), so the
   * pattern list names each cache family under `parkfan:` explicitly — see
   * flush-cache-patterns.ts and its spec.
   */
  @Post("flush-cache")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Flush park cache",
    description:
      "Clears park-related cached data (schedules, wait times, analytics) without affecting queue jobs",
  })
  @ApiResponse({ status: 200, description: "Park cache flushed successfully" })
  async flushCache(): Promise<{ message: string; keysDeleted: number }> {
    const keysDeleted = await deleteKeysByPatterns(
      this.redis,
      PARK_CACHE_FLUSH_PATTERNS,
    );
    return { message: "Park cache flushed successfully", keysDeleted };
  }

  /**
   * Deletes the park cache (the same PARK_CACHE_FLUSH_PATTERNS as
   * `POST /v1/admin/flush-cache`, walked with SCAN) and queues a full rebuild.
   * It used to run FLUSHALL, which also deleted every Bull queue's jobs and
   * `repeat` entries (so no cron ran again until the API restarted) and every
   * admin session. Bull keys (`<BULL_PREFIX>:<queue>:*`), admin sessions and
   * the state families listed in flush-cache-patterns.ts stay.
   *
   * Use when the discovery structure is out of sync, after a major schema
   * change, or when the cache holds stale data. The rebuild runs by priority:
   * holidays (100), parks (90), attractions/shows/restaurants (80), live data
   * (70).
   */
  @Post("cache/reset")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Complete cache reset and rebuild",
    description:
      "Deletes the park cache (same key patterns as flush-cache) and triggers the complete data rebuild pipeline. " +
      "Bull queue jobs, repeat (cron) entries and admin sessions are kept. " +
      "Requires explicit confirmation via ?confirm=true query parameter.",
  })
  @ApiResponse({
    status: 200,
    description: "Park cache deleted and rebuild jobs triggered",
  })
  @ApiResponse({
    status: 400,
    description:
      "Confirmation required. Add ?confirm=true to confirm the cache reset.",
  })
  async resetCache(@Query("confirm") confirm?: string): Promise<{
    message: string;
    flushed: string;
    keysDeleted: number;
    jobsTriggered: string[];
  }> {
    if (confirm !== "true") {
      throw new HttpException(
        {
          statusCode: HttpStatus.BAD_REQUEST,
          message:
            "Cache reset requires explicit confirmation. Add ?confirm=true to confirm.",
          warning:
            "This operation will delete the park cache and rebuild it. This cannot be undone.",
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    this.logger.warn(
      "⚠️  Cache reset: deleting the park cache (Bull queues are kept)",
    );
    const keysDeleted = await deleteKeysByPatterns(
      this.redis,
      PARK_CACHE_FLUSH_PATTERNS,
    );

    const rebuild: Array<[Queue, string, number]> = [
      [this.holidaysQueue, "fetch-holidays", 100],
      [this.parkMetadataQueue, "sync-all-parks", 90],
      [this.childrenQueue, "fetch-all-children", 80],
      [this.waitTimesQueue, "fetch-wait-times", 70],
    ];
    for (const [queue, name, priority] of rebuild) {
      await queue.add(name, {}, { priority });
    }

    return {
      message: "Complete cache reset and rebuild started",
      flushed: "park cache (PARK_CACHE_FLUSH_PATTERNS)",
      keysDeleted,
      jobsTriggered: rebuild.map(([, name]) => name),
    };
  }

  private async enqueue(
    queue: Queue,
    name: string,
    options: JobOptions,
    message: string,
    data: object = {},
  ): Promise<QueuedJob> {
    const job = await queue.add(name, data, options);
    return { message, jobId: job.id.toString() };
  }
}
