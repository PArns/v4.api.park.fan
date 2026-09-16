import {
  Controller,
  Get,
  Post,
  HttpCode,
  HttpStatus,
  Inject,
  Body,
  Param,
  Query,
  HttpException,
  BadRequestException,
  Logger,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiSecurity,
  ApiBody,
  ApiQuery,
} from "@nestjs/swagger";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { In } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  RideProfileAuditService,
  RideProfileTermAudit,
} from "../attractions/services/ride-profile-audit.service";
import { ParkValidatorService } from "../parks/services/park-validator.service";
import { ParkRepairService } from "../parks/services/park-repair.service";
import {
  AttractionMergeService,
  DuplicatePairReport,
} from "../attractions/services/attraction-merge.service";
import { ParkRenameService } from "../parks/services/park-rename.service";
import { ParkMergeService } from "../parks/services/park-merge.service";
import { determineMergeWinner } from "../parks/utils/park-merge.util";
import { SystemHealthService } from "./system-health.service";
import { DowntimeMeasurementService } from "./downtime-measurement.service";
import { AdminAuthGuard } from "./auth/admin-auth.guard";
import { AdminMinRole } from "./auth/admin-auth.decorators";
import { AdminAuditInterceptor } from "./auth/admin-audit.interceptor";
import { DataQualityMonitorService } from "../monitoring/data-quality-monitor.service";
import {
  AttractionRetirementService,
  RetirementRequest,
} from "../attractions/services/attraction-retirement.service";
import {
  AttractionReviewService,
  ReviewMarkRequest,
} from "../attractions/services/attraction-review.service";

/**
 * One pair of `POST merge-duplicate-parks`, on either side of the gate.
 *
 * In `planned` it is a merge that ran or would run; in `skipped` it is a pair
 * the detector found and `autoDetect` refuses to act on, with `reviewReason`
 * saying what a human has to settle. The winner is resolved either way, so the
 * operator can send that one pair back as a manual merge without working out
 * which row survives.
 *
 * Both lists belong to `autoDetect`, with one exception: a manual pair asked
 * for as a dry run returns itself in `planned`, because that is the whole
 * answer. A manual pair that really merges reports in `results` and leaves
 * both lists empty — it was never a plan, it was an instruction.
 *
 * `score` is the pair's name similarity, and it is `null` for a manual pair:
 * two ids typed into a form were never scored by anything.
 */
export interface ParkMergePlanEntry {
  winnerId: string;
  winnerName: string;
  loserId: string;
  loserName: string;
  score: number | null;
  reason: string;
  reviewReason: string | null;
}

/**
 * Read a flag out of a body that nothing validates, and refuse to guess.
 *
 * `merge-duplicate-parks` takes an inline body rather than a DTO, so the
 * `ValidationPipe` has no metatype to work with and coerces nothing — and Nest
 * parses `application/x-www-form-urlencoded` out of the box, where every value
 * arrives as a string. `dryRun=true` from a curl one-liner is therefore
 * `"true"`, and `"true" === true` is false: a strict comparison reads the one
 * request that asked in as many words for a dry run as permission to delete.
 * Which is, to the letter, the trap the attraction endpoint sprang once.
 *
 * A value that is neither is `undefined` here and a 400 at the call site. On an
 * endpoint that deletes parks, a `dryRun: "yes"` nobody can interpret must not
 * be interpreted — in either direction. The one value that is not a 400 is
 * `null`, which the call site reads as the absent key it stands for in JSON.
 */
function readBodyFlag(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  if (value === "true") return true;
  if (value === "false") return false;
  return undefined;
}

/**
 * Admin Controller
 *
 * Every endpoint here needs a signed-in administrator — see `AdminAuthGuard`,
 * which is on the class rather than on each method so that a new endpoint is
 * protected by default and has to opt OUT to be public.
 *
 * That is a change of posture, not a tightening of one. Until it, these
 * endpoints were documented as "protected in production via Cloudflare", which
 * describes traffic arriving through Cloudflare and says nothing about traffic
 * that does not: this application never checked the `pass` parameter it
 * advertised, in any environment, so anything able to reach the origin could
 * merge parks, retire attractions or flush every cache. Cloudflare's rule stays
 * where it is; this is the second lock, on the inside of the same door.
 *
 * Roles follow what an action costs to get wrong. Reads need any session.
 * Job triggers and curation need `editor`. The handful that cannot be undone —
 * merges, retirements, repair, a full cache reset — need `owner`.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminController {
  private readonly logger = new Logger(AdminController.name);

  constructor(
    @InjectQueue("holidays") private holidaysQueue: Queue,
    @InjectQueue("park-metadata") private parkMetadataQueue: Queue,
    @InjectQueue("park-enrichment") private parkEnrichmentQueue: Queue,
    @InjectQueue("ml-training") private mlTrainingQueue: Queue,
    @InjectQueue("wait-times") private waitTimesQueue: Queue,
    @InjectQueue("downtime") private downtimeQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue,
    @InjectQueue("six-flags-heights") private sixFlagsHeightsQueue: Queue,
    @InjectQueue("ride-stats") private rideStatsQueue: Queue,
    @InjectQueue("manual-metadata") private manualMetadataQueue: Queue,
    @InjectQueue("prediction-accuracy") private accuracyQueue: Queue,
    @InjectQueue("analytics") private analyticsQueue: Queue,
    @InjectQueue("pcn-shadow") private pcnShadowQueue: Queue,
    @InjectQueue("shape-shadow") private shapeShadowQueue: Queue,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly parkValidatorService: ParkValidatorService,
    private readonly parkRepairService: ParkRepairService,
    private readonly parkMergeService: ParkMergeService,
    private readonly systemHealth: SystemHealthService,
    private readonly downtimeMeasurement: DowntimeMeasurementService,
    private readonly attractionMergeService: AttractionMergeService,
    private readonly parkRenameService: ParkRenameService,
    private readonly rideProfileAudit: RideProfileAuditService,
    private readonly dataQualityMonitor: DataQualityMonitorService,
    private readonly retirementService: AttractionRetirementService,
    private readonly reviewService: AttractionReviewService,
  ) {}

  /**
   * System health dashboard data.
   *
   * Aggregates host (CPU/RAM/disk), Postgres, Redis, and both ML services —
   * CatBoost (ml-service) + TFT (nf-service): training status/progress + model
   * quality + the TFT-vs-CatBoost scoreboard. One JSON for a monitoring UI.
   */
  /**
   * On-demand view of the two silent-failure detectors that run daily at 06:45.
   *
   * `windowDays` exists so the cluster detector can be pointed at history: it
   * defaults to a short window on purpose, so a reported cluster falls quiet by
   * itself after a couple of weeks rather than nagging forever. Ask for 120
   * days to see the known incidents (Europa-Park 2026-06-07 and friends).
   */
  /**
   * Retire attractions that no longer exist.
   *
   * Not a plain UPDATE, because retirement has to drag the caches and the
   * frontend sitemap along with it — a removed slug otherwise stays advertised
   * for up to 24h plus the CDN window. Each entry carries the date and the
   * source it was established from; a retirement is a claim about the world.
   */
  /**
   * Record that a candidate has been investigated and is not a case.
   *
   * Both detectors are behavioural, so every candidate returns tomorrow however
   * carefully it was checked. Without this the same questions get re-asked
   * until somebody answers one carelessly — and a merge cannot be undone.
   *
   * `recheckAfter` matters: leave it null only when the answer cannot change.
   * Shock Wave has stood unused since March 2026 with no announcement either
   * way; a permanent mark there would hide its eventual retirement forever.
   */
  @Post("review-marks")
  @AdminMinRole("editor")
  @ApiOperation({ summary: "Mark candidates as investigated and not a case" })
  async recordReviewMarks(@Body() body: { marks: ReviewMarkRequest[] }) {
    return { recorded: await this.reviewService.record(body.marks ?? []) };
  }

  @Get("retirement-candidates")
  @ApiOperation({
    summary: "Attractions whose feed went quiet, minus those already cleared",
  })
  async getRetirementCandidates() {
    const candidates = await this.reviewService.findRetirementCandidates();
    return { total: candidates.length, candidates };
  }

  @Post("retire-attractions")
  @AdminMinRole("owner")
  @ApiOperation({ summary: "Retire attractions that no longer exist" })
  async retireAttractions(@Body() body: { retirements: RetirementRequest[] }) {
    const retired = await this.retirementService.retire(body.retirements ?? []);
    return { retired: retired.length, attractions: retired };
  }

  @Post("unretire-attraction/:id")
  @AdminMinRole("owner")
  @ApiOperation({ summary: "Undo a retirement" })
  async unretireAttraction(@Param("id") id: string) {
    return { restored: await this.retirementService.unretire(id) };
  }

  @Get("retired-attractions")
  @ApiOperation({ summary: "Everything currently retired" })
  async listRetiredAttractions() {
    const rows = await this.retirementService.listRetired();
    return {
      total: rows.length,
      attractions: rows.map((a) => ({
        id: a.id,
        name: a.name,
        park: a.park?.name ?? null,
        retiredAt: a.retiredAt,
        reason: a.retiredReason,
      })),
    };
  }

  @Get("data-quality")
  @ApiOperation({ summary: "Silenced attraction clusters and failing jobs" })
  async getDataQuality(@Query("windowDays") windowDays?: string) {
    const days = Math.min(Math.max(Number(windowDays) || 14, 1), 400);
    const [silencedClusters, failingJobs] = await Promise.all([
      this.dataQualityMonitor.findSilencedClusters(days),
      this.dataQualityMonitor.findFailingJobs(),
    ]);
    return { windowDays: days, silencedClusters, failingJobs };
  }

  /**
   * Phase 0 of the downtime work. Reads, counts, writes nothing.
   *
   * Answers in EVENTS, which is the unit nobody had: `downCount` on the history
   * endpoint counts distinct clock hours carrying a DOWN reading, across the
   * whole park-local calendar day and with no opening-hours bound, so it reads
   * 16 on a ten-hour park day. Every threshold in
   * `docs/analytics/ride-downtime.md` is provisional until this has run.
   */
  @Get("downtime-measurement")
  @ApiOperation({
    summary: "Count ride outages as events (phase 0, read-only)",
    description:
      "Reconstructs outage intervals from queue_data over a window and reports " +
      "event counts, duration histograms, the capability census and the " +
      "per-park artefact signature. Publishes nothing and writes nothing.",
  })
  @ApiQuery({
    name: "parks",
    required: false,
    description:
      "Comma-separated park slugs. Omit for every park that could report an " +
      "outage at all.",
    example: "phantasialand,europa-park",
  })
  @ApiQuery({
    name: "days",
    required: false,
    description: "Lookback in days. Default 90, clamped to 1-365.",
    example: 90,
  })
  async getDowntimeMeasurement(
    @Query("parks") parks?: string,
    @Query("days") days?: string,
  ): Promise<Record<string, unknown>> {
    const parkSlugs = (parks ?? "")
      .split(",")
      .map((slug) => slug.trim())
      .filter(Boolean);
    const measurement = await this.downtimeMeasurement.measure({
      parkSlugs,
      days: Number(days) || undefined,
    });
    return measurement as unknown as Record<string, unknown>;
  }

  /**
   * Phase 5: how much of the DOWN signal our own merge deletes before storage.
   *
   * The one number that decides whether the probability question can ever be
   * reopened. It answers nothing until `raw_status` has existed for the whole
   * window, and the response says so rather than reporting a small figure that
   * reads as good news.
   */
  @Get("downtime-erasure")
  @ApiOperation({
    summary: "Count the DOWN readings the conflict resolver rewrote",
    description:
      "Reads queue_data.raw_status and reports, per park, how many DOWN and " +
      "CLOSED readings were overwritten with OPERATING — in rows AND in the " +
      "minutes they carried, because an overridden state is stable afterwards " +
      "and writes no further row. Read-only.",
  })
  @ApiQuery({
    name: "days",
    required: false,
    description: "Lookback in days. Default 30, clamped to 1-365.",
    example: 30,
  })
  async getDowntimeErasure(
    @Query("days") days?: string,
  ): Promise<Record<string, unknown>> {
    const measurement = await this.downtimeMeasurement.measureErasure(
      Number(days) || undefined,
    );
    return measurement as unknown as Record<string, unknown>;
  }

  /**
   * Rebuild the outage intervals, the exposure days and the profiles.
   *
   * Normally the 5:00 AM cron. Exposed for a targeted repair and for the first
   * run, which has no history to be incremental about.
   */
  @Post("rebuild-downtime")
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
    @Query("days") days?: string,
  ): Promise<Record<string, unknown>> {
    await this.downtimeQueue.add(
      "reconstruct-downtime",
      { windowDays: Number(days) || undefined },
      { priority: 60 },
    );
    return { message: "Downtime reconstruction queued", queue: "downtime" };
  }

  @Get("system-health")
  @ApiOperation({
    summary: "System health + ML training/quality dashboard",
    description:
      "Host (CPU/RAM/disk), Postgres, Redis stats + CatBoost & TFT training status/" +
      "progress, model quality, and the TFT-vs-CatBoost comparison scoreboard.",
  })
  @ApiResponse({ status: 200, description: "Aggregated system + ML health" })
  async getSystemHealth(): Promise<Record<string, unknown>> {
    return this.systemHealth.getHealth();
  }

  /**
   * Bull queue job counts per queue.
   */
  @Get("queue-status")
  @ApiOperation({ summary: "Bull queue job counts per queue" })
  @ApiResponse({
    status: 200,
    description: "Active/pending/failed/delayed counts per queue",
  })
  async getQueueStatus(): Promise<Record<string, unknown>> {
    const queues = [
      { name: "wait-times", queue: this.waitTimesQueue },
      { name: "park-metadata", queue: this.parkMetadataQueue },
      { name: "ml-training", queue: this.mlTrainingQueue },
      { name: "prediction-accuracy", queue: this.accuracyQueue },
      { name: "analytics", queue: this.analyticsQueue },
      { name: "holidays", queue: this.holidaysQueue },
      { name: "park-enrichment", queue: this.parkEnrichmentQueue },
      { name: "children-metadata", queue: this.childrenQueue },
      // The queues you can trigger by hand were missing from this list, which
      // is exactly when you want it: after firing one of those endpoints there
      // was no way to see whether the job was waiting, failed or done.
      { name: "manual-metadata", queue: this.manualMetadataQueue },
      { name: "six-flags-heights", queue: this.sixFlagsHeightsQueue },
      { name: "ride-stats", queue: this.rideStatsQueue },
    ];
    const results = await Promise.all(
      queues.map(async ({ name, queue }) => ({
        name,
        active: await queue.getActiveCount(),
        pending: await queue.getWaitingCount(),
        failed: await queue.getFailedCount(),
        delayed: await queue.getDelayedCount(),
        completed: await queue.getCompletedCount(),
      })),
    );
    return { timestamp: new Date().toISOString(), queues: results };
  }

  /**
   * Why the recent jobs on a queue failed.
   *
   * `queue-status` counts failures; Bull knows the reason and the stack, and
   * keeps them in Redis where nothing could read them. A job that dies on a
   * server you cannot tail is otherwise a number with no explanation.
   */
  @Get("queue-failures")
  @ApiOperation({
    summary: "Failure reasons for the most recent failed jobs on a queue",
    description:
      "Pass `queue` (e.g. manual-metadata) and optionally `limit` (default 3). " +
      "Returns each failed job's name, reason and first stack frames.",
  })
  @ApiResponse({ status: 200, description: "Recent failures" })
  async getQueueFailures(
    @Query("queue") queueName: string,
    @Query("limit") limit?: string,
  ): Promise<Record<string, unknown>> {
    const queues: Record<string, Queue> = {
      "wait-times": this.waitTimesQueue,
      "park-metadata": this.parkMetadataQueue,
      "children-metadata": this.childrenQueue,
      "manual-metadata": this.manualMetadataQueue,
      "six-flags-heights": this.sixFlagsHeightsQueue,
      "ride-stats": this.rideStatsQueue,
      analytics: this.analyticsQueue,
      "prediction-accuracy": this.accuracyQueue,
    };
    const queue = queues[queueName];
    if (!queue) {
      return {
        error: `Unknown queue "${queueName}"`,
        known: Object.keys(queues),
      };
    }

    const count = Math.min(Number(limit) || 3, 20);
    const jobs = await queue.getFailed(0, count - 1);
    return {
      queue: queueName,
      failures: jobs.map((job) => ({
        id: job.id,
        name: job.name,
        data: job.data as unknown,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason,
        // First frames only: the interesting line is always near the top and
        // a full stack turns this response into a wall.
        stack: (job.stacktrace ?? []).join("\n").split("\n").slice(0, 12),
      })),
    };
  }

  /**
   * Focused model-comparison board (daily TFT-vs-CatBoost + intraday PCN-vs-CatBoost).
   *
   * Unlike /system-health (host/gpu/postgres/redis + ML-service round-trips), this is
   * just the two scoreboard SQL reads + a computed PCN−CatBoost verdict per segment, so
   * a UI can poll it cheaply. `days` bounds the intraday window (default 14).
   */
  @Get("ml-comparison")
  @ApiOperation({
    summary: "Model comparison board (daily + intraday shadow) + verdict",
    description:
      "Light, pollable board: model_comparisons (TFT vs CatBoost) + " +
      "pcn_intraday_comparisons (PCN vs CatBoost, all segments × lead buckets) with a " +
      "computed PCN−CatBoost MAE delta per segment.",
  })
  @ApiResponse({ status: 200, description: "Comparison boards + verdict" })
  async getMlComparison(
    @Query("days") days?: string,
  ): Promise<Record<string, unknown>> {
    const n = Math.min(Math.max(parseInt(days ?? "14", 10) || 14, 1), 90);
    return this.systemHealth.comparisonBoard(n);
  }

  /**
   * Manually trigger holiday sync
   *
   * Forces a complete resync of all holidays from Nager.Date API.
   * Useful after code changes to holiday storage logic.
   */
  @Post("sync-holidays")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger holiday sync",
    description:
      "Manually triggers a complete resync of all holidays from Nager.Date API",
  })
  @ApiResponse({
    status: 202,
    description: "Holiday sync job queued successfully",
  })
  async triggerHolidaySync(): Promise<{ message: string; jobId: string }> {
    const job = await this.holidaysQueue.add(
      "fetch-holidays",
      {},
      { priority: 10 },
    );
    return {
      message: "Holiday sync job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger park metadata sync
   *
   * Forces a complete resync of all parks from all sources (Wiki, Queue-Times, Wartezeiten).
   * Useful for testing duplicate detection and matching improvements.
   */
  @Post("sync-parks")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger park metadata sync",
    description:
      "Manually triggers a complete resync of all parks from all sources (Wiki, Queue-Times, Wartezeiten)",
  })
  @ApiResponse({
    status: 202,
    description: "Park metadata sync job queued successfully",
  })
  async triggerParkSync(): Promise<{ message: string; jobId: string }> {
    const job = await this.parkMetadataQueue.add(
      "sync-all-parks",
      {},
      { priority: 10 },
    );
    return {
      message: "Park metadata sync job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger schedule gap filling for all parks
   *
   * Updates holiday/bridge day metadata in schedule entries.
   */
  @Post("fill-schedule-gaps")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Fill schedule gaps",
    description:
      "Triggers schedule gap filling to update holiday/bridge day metadata",
  })
  @ApiResponse({
    status: 202,
    description: "Schedule gap filling job queued successfully",
  })
  async triggerScheduleGapFilling(): Promise<{
    message: string;
    jobId: string;
  }> {
    const job = await this.parkMetadataQueue.add(
      "fill-all-gaps",
      {},
      { priority: 5 },
    );
    return {
      message: "Schedule gap filling job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger attraction detail sync
   *
   * Syncs minimumHeight, maximumHeight and mayGetWet from ThemeParks.wiki
   * per-entity documents. It writes only the upstream columns — hand-made
   * corrections live in `curated_may_get_wet` and are never touched here.
   */
  @Post("sync-attraction-details")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger attraction detail sync",
    description:
      "Syncs minimum rider heights from ThemeParks.wiki entity documents (~1 request per attraction). Curated corrections are not overwritten.",
  })
  @ApiResponse({
    status: 202,
    description: "Attraction detail sync job queued successfully",
  })
  async triggerAttractionDetailsSync(): Promise<{
    message: string;
    jobId: string;
  }> {
    const job = await this.childrenQueue.add(
      "sync-attraction-details",
      {},
      { priority: 10 },
    );
    return {
      message: "Attraction detail sync job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger park enrichment
   *
   * Enriches all parks with ISO country codes and influencing regions.
   * Useful for fixing missing countryCode fields.
   */
  @Post("enrich-parks")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger park enrichment",
    description:
      "Manually triggers park enrichment to set countryCode from country names and update influencing regions",
  })
  @ApiResponse({
    status: 202,
    description: "Park enrichment job queued successfully",
  })
  async triggerParkEnrichment(): Promise<{ message: string; jobId: string }> {
    const job = await this.parkEnrichmentQueue.add(
      "enrich-all",
      {},
      { priority: 10 },
    );
    return {
      message: "Park enrichment job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger ML model training
   *
   * Forces a complete model retraining with latest data.
   */
  @Post("train-ml-model")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger ML training",
    description: "Manually triggers ML model training - takes 1-2 minutes",
  })
  @ApiResponse({
    status: 202,
    description: "ML training job queued successfully",
  })
  async triggerMLTraining(): Promise<{ message: string; jobId: string }> {
    const job = await this.mlTrainingQueue.add(
      "train-model",
      {},
      { priority: 10 },
    );
    return {
      message: "ML training job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger the PCN intraday shadow jobs (queue → pcn-shadow processor →
   * pcn-service). `train` is needed for bring-up (the nightly cron is 08:30 UTC, so the
   * first models otherwise don't exist until then); `forecast`/`score` are for testing.
   */
  @Post("pcn/:action")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger a PCN shadow job (train | forecast | score)",
    description:
      "Enqueues the matching pcn-shadow job. 'train' (per-park GP-STGNN) is the " +
      "bring-up trigger; 'forecast' writes pcn_forecasts; 'score' writes the board.",
  })
  @ApiResponse({ status: 202, description: "PCN job queued" })
  async triggerPcn(
    @Param("action") action: string,
  ): Promise<{ message: string; jobId: string }> {
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
    const job = await this.pcnShadowQueue.add(jobName, {}, { priority: 10 });
    return { message: `PCN ${action} job queued`, jobId: job.id.toString() };
  }

  /**
   * Manually trigger the Shape day-curve shadow jobs (queue → shape-shadow processor →
   * shape-service). `build` is the bring-up trigger (persist profiles); then `forecast`
   * writes shape_forecasts and `score` writes the shape_comparisons board.
   */
  @Post("shape/:action")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger a Shape shadow job (build | forecast | score)",
    description:
      "Enqueues the matching shape-shadow job. 'build' persists the additive+smooth " +
      "profiles; 'forecast' writes shape_forecasts; 'score' writes the board.",
  })
  @ApiResponse({ status: 202, description: "Shape job queued" })
  async triggerShape(
    @Param("action") action: string,
  ): Promise<{ message: string; jobId: string }> {
    const jobName = {
      build: "build-shape",
      forecast: "forecast-shape",
      score: "score-shape",
    }[action];
    if (!jobName) {
      throw new BadRequestException(
        `Unknown Shape action '${action}' (expected build | forecast | score)`,
      );
    }
    const job = await this.shapeShadowQueue.add(jobName, {}, { priority: 10 });
    return { message: `Shape ${action} job queued`, jobId: job.id.toString() };
  }

  /**
   * Manually trigger prediction accuracy aggregation
   *
   * Refreshes MAE/MAPE/R2 metrics for all attractions.
   */
  @Post("aggregate-accuracy-stats")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger accuracy stats aggregation",
    description:
      "Manually triggers the aggregation of prediction accuracy metrics",
  })
  @ApiResponse({
    status: 202,
    description: "Accuracy aggregation job queued successfully",
  })
  async triggerAccuracyAggregation(): Promise<{
    message: string;
    jobId: string;
  }> {
    const job = await this.accuracyQueue.add(
      "aggregate-stats",
      {},
      { priority: 5 },
    );
    return {
      message: "Accuracy aggregation job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Manually trigger seasonal attraction detection
   *
   * Re-runs the detect-seasonal analytics job (normally scheduled daily at
   * 2:30am) to re-evaluate which attractions/shows should be flagged as
   * seasonal. Useful after deploying fixes that affect the CLOSED-status
   * signal (e.g. reverse-reconciliation for attractions no longer reported
   * by any upstream source).
   */
  @Post("detect-seasonal")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Trigger seasonal detection",
    description:
      "Manually triggers the seasonal attraction/show detection job (normally daily at 2:30am)",
  })
  @ApiResponse({
    status: 202,
    description: "Seasonal detection job queued successfully",
  })
  async triggerSeasonalDetection(): Promise<{
    message: string;
    jobId: string;
  }> {
    const job = await this.analyticsQueue.add(
      "detect-seasonal",
      {},
      { priority: 5 },
    );
    return {
      message: "Seasonal detection job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * One-time cleanup of historical duplicate percentile-aggregate rows.
   *
   * Collapses each duplicated (attractionId, hour) bucket in
   * queue_data_aggregates to a single (latest) row. These duplicates came
   * from the old random-uuid upsert; the deterministic-id fix prevents new
   * ones, this clears the backlog. Idempotent and safe to re-run. Run in a
   * controlled window — it may decompress old TimescaleDB chunks.
   */
  @Post("dedupe-percentile-aggregates")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Dedupe percentile aggregates",
    description:
      "Removes historical duplicate (attractionId, hour) rows from queue_data_aggregates, keeping the latest. Idempotent.",
  })
  @ApiResponse({
    status: 202,
    description: "Dedupe job queued successfully",
  })
  async triggerDedupePercentileAggregates(): Promise<{
    message: string;
    jobId: string;
  }> {
    const job = await this.analyticsQueue.add(
      "dedupe-percentile-aggregates",
      {},
      { priority: 5 },
    );
    return {
      message: "Percentile-aggregate dedupe job queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Flush park-related Redis cache
   *
   * Clears only park-related cached data (schedules, wait times, analytics, etc.)
   * while preserving Bull queue jobs and system caches.
   */
  @Post("flush-cache")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Flush park cache",
    description:
      "Clears park-related cached data (schedules, wait times, analytics) without affecting queue jobs",
  })
  @ApiResponse({
    status: 200,
    description: "Park cache flushed successfully",
  })
  async flushCache(): Promise<{ message: string; keysDeleted: number }> {
    // Park-related cache key patterns. Every entry must match a prefix that
    // is actually written somewhere (see CacheKeys + inline keys) — the old
    // list carried six prefixes that no code ever writes (parks:*,
    // wait-times:*, occupancy:*, predictions:*, show:*, restaurant:*) while
    // the real calendar/ML/favorites caches survived the flush.
    //
    // Deliberately NOT flushed (state, not cache): popularity:* (ranking),
    // downtime:* (open downtime tracking), prediction:deviation:* (deviation
    // tracking), ratelimit:* (circuit breakers), ml:accuracy:* and
    // ml:last-accuracy-check (job markers).
    const patterns = [
      "schedule:*",
      "park:*", // integrated, occupancy, statistics, baselines, …
      "attraction:*", // integrated, history, baselines, ropedrop, last-seen
      "calendar:*", // month caches + refresh-check markers
      "analytics:*",
      "holiday:*",
      "weather:*", // forecasts + sync:done markers (flush ⇒ next sync re-runs)
      "search:*",
      "discovery:*",
      "favorites:*",
      "parkfan:*", // latest queue snapshot + attraction tz cache
      "accuracy:*", // prediction accuracy badges
      "ml:park:*", // serving predictions (daily/hourly/yearly)
      "ml:tft-daily:*",
      "ml:active-attractions:*",
      "ml:dashboard:*", // ML dashboard snapshot (5min cache)
      "location:*", // /nearby shared park-coordinate index
    ];

    let totalDeleted = 0;

    // Fan out the KEYS scans in parallel — they're independent and
    // each hits a different namespace. The old loop serialised them at
    // ~one round-trip per pattern; Promise.all collapses that to a
    // single batch wall-time.
    const keysPerPattern = await Promise.all(
      patterns.map((p) => this.redis.keys(p)),
    );
    const allKeys = keysPerPattern.flat();

    // Single pipelined DEL for every key that matched — one network
    // round-trip regardless of total key count, vs. 14× DEL before.
    if (allKeys.length > 0) {
      const pipeline = this.redis.pipeline();
      for (const key of allKeys) pipeline.del(key);
      await pipeline.exec();
      totalDeleted = allKeys.length;
    }

    return {
      message: "Park cache flushed successfully",
      keysDeleted: totalDeleted,
    };
  }

  /**
   * Complete Cache Reset and Rebuild
   *
   * ⚠️ WARNING: Performs FLUSHALL on Redis, clearing ALL cache data.
   * Queue jobs are NOT affected (separate storage mechanism).
   *
   * SECURITY: This operation is protected by Cloudflare in production and requires
   * explicit confirmation via `confirm=true` query parameter to prevent accidental execution.
   *
   * Use when:
   * - Discovery structure is corrupted or out of sync
   * - Major database schema changes occurred
   * - Cache contains stale/invalid data
   *
   * Pipeline order (by priority):
   * 1. Holidays (100) - Base geographic/temporal metadata
   * 2. Parks (90) - Park metadata, geocoding, matching
   * 3. Children (80) - Attractions, Shows, Restaurants
   * 4. Live Data (70) - Current wait times and schedules
   */
  @Post("cache/reset")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Complete cache reset and rebuild",
    description:
      "⚠️ SECURITY: Performs FLUSHALL on Redis and triggers complete data rebuild pipeline. " +
      "Requires explicit confirmation via ?confirm=true query parameter. Use with extreme caution.",
  })
  @ApiResponse({
    status: 200,
    description: "Cache completely flushed and rebuild jobs triggered",
  })
  @ApiResponse({
    status: 400,
    description:
      "Confirmation required. Add ?confirm=true to confirm FLUSHALL operation.",
  })
  async resetCache(@Query("confirm") confirm?: string): Promise<{
    message: string;
    flushed: string;
    jobsTriggered: string[];
  }> {
    // SECURITY: Require explicit confirmation to prevent accidental FLUSHALL
    if (confirm !== "true") {
      throw new HttpException(
        {
          statusCode: HttpStatus.BAD_REQUEST,
          message:
            "FLUSHALL operation requires explicit confirmation. Add ?confirm=true to confirm.",
          warning:
            "This operation will delete ALL Redis cache data. This cannot be undone.",
        },
        HttpStatus.BAD_REQUEST,
      );
    }

    // Perform complete Redis flush
    // SECURITY: This is a dangerous operation, but protected by Cloudflare in production
    this.logger.warn(
      "⚠️  Executing FLUSHALL on Redis - all cache data will be deleted",
    );
    await this.redis.flushall();

    // Trigger complete rebuild pipeline
    const jobsTriggered: string[] = [];

    await this.holidaysQueue.add("fetch-holidays", {}, { priority: 100 });
    jobsTriggered.push("fetch-holidays");

    await this.parkMetadataQueue.add("sync-all-parks", {}, { priority: 90 });
    jobsTriggered.push("sync-all-parks");

    await this.childrenQueue.add("fetch-all-children", {}, { priority: 80 });
    jobsTriggered.push("fetch-all-children");

    await this.waitTimesQueue.add("fetch-wait-times", {}, { priority: 70 });
    jobsTriggered.push("fetch-wait-times");

    return {
      message: "Complete cache reset and rebuild started",
      flushed: "ALL (FLUSHALL executed)",
      jobsTriggered,
    };
  }

  /**
   * Validate and repair park data
   *
   * Validates all parks against external APIs (Queue-Times, Wartezeiten.app)
   * and optionally repairs found issues automatically.
   */
  @Post("validate-and-repair-parks")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Validate and repair parks",
    description:
      "Validates all parks against Queue-Times and Wartezeiten.app APIs. " +
      "Detects mismatched IDs, missing IDs, and duplicates. " +
      "Optionally repairs issues automatically if autoFix=true.",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        autoFix: {
          type: "boolean",
          description: "Automatically repair found issues",
          default: false,
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description:
      "Validation complete. Returns report with found issues and repair results.",
  })
  async validateAndRepairParks(
    @Body() body: { autoFix?: boolean } = {},
  ): Promise<{
    validation: {
      mismatchedQtIds: number;
      mismatchedWzIds: number;
      missingQtIds: number;
      missingWzIds: number;
      duplicates: number;
      summary: {
        totalParks: number;
        parksWithQtId: number;
        parksWithWzId: number;
        issuesFound: number;
      };
    };
    repair?: {
      fixedQtMismatches: number;
      fixedWzMismatches: number;
      addedQtIds: number;
      addedWzIds: number;
      mergedDuplicates: number;
      errors: number;
    };
    report: {
      mismatchedQtIds: Array<{
        parkId: string;
        parkName: string;
        currentQtId: string;
        reason: string;
      }>;
      mismatchedWzIds: Array<{
        parkId: string;
        parkName: string;
        currentWzId: string;
        reason: string;
      }>;
      missingQtIds: Array<{
        parkId: string;
        parkName: string;
        suggestedQtId: string;
      }>;
      missingWzIds: Array<{
        parkId: string;
        parkName: string;
        suggestedWzId: string;
      }>;
      duplicates: Array<{
        park1: { id: string; name: string };
        park2: { id: string; name: string };
        score: number;
        reason: string;
      }>;
    };
  }> {
    const autoFix = body.autoFix === true;

    // Run validation
    const validationReport = await this.parkValidatorService.validateAll();

    let repairResult = null;

    if (autoFix) {
      // Auto-fix mismatched QT IDs (but we need to determine correct IDs first)
      // For now, we'll only fix missing IDs and note mismatches for manual review
      const qtFixes: Array<{ parkId: string; correctQtId: string }> = [];
      const wzFixes: Array<{ parkId: string; correctWzId: string }> = [];
      const qtAdditions: Array<{ parkId: string; qtId: string }> = [];
      const wzAdditions: Array<{ parkId: string; wzId: string }> = [];

      // Add missing IDs
      for (const missing of validationReport.missingQtIds) {
        qtAdditions.push({
          parkId: missing.parkId,
          qtId: missing.suggestedQtId,
        });
      }

      for (const missing of validationReport.missingWzIds) {
        wzAdditions.push({
          parkId: missing.parkId,
          wzId: missing.suggestedWzId,
        });
      }

      // Note: Mismatched IDs require manual review to determine correct IDs
      // We don't auto-fix them as it could cause data loss

      // Perform repairs
      const [qtResult, wzResult, qtAddResult, wzAddResult] = await Promise.all([
        qtFixes.length > 0
          ? this.parkRepairService.fixMismatchedQueueTimesIds(qtFixes)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
        wzFixes.length > 0
          ? this.parkRepairService.fixMismatchedWartezeitenIds(wzFixes)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
        qtAdditions.length > 0
          ? this.parkRepairService.addMissingQueueTimesIds(qtAdditions)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
        wzAdditions.length > 0
          ? this.parkRepairService.addMissingWartezeitenIds(wzAdditions)
          : Promise.resolve({
              fixedQtMismatches: 0,
              fixedWzMismatches: 0,
              addedQtIds: 0,
              addedWzIds: 0,
              mergedDuplicates: 0,
              errors: [],
            }),
      ]);

      repairResult = {
        fixedQtMismatches:
          qtResult.fixedQtMismatches + wzResult.fixedQtMismatches,
        fixedWzMismatches:
          qtResult.fixedWzMismatches + wzResult.fixedWzMismatches,
        addedQtIds: qtAddResult.addedQtIds,
        addedWzIds: wzAddResult.addedWzIds,
        mergedDuplicates: 0, // Merges require manual confirmation
        errors: [
          ...qtResult.errors,
          ...wzResult.errors,
          ...qtAddResult.errors,
          ...wzAddResult.errors,
        ],
      };
    }

    return {
      validation: {
        mismatchedQtIds: validationReport.mismatchedQtIds.length,
        mismatchedWzIds: validationReport.mismatchedWzIds.length,
        missingQtIds: validationReport.missingQtIds.length,
        missingWzIds: validationReport.missingWzIds.length,
        duplicates: validationReport.duplicates.length,
        summary: validationReport.summary,
      },
      repair: repairResult
        ? {
            ...repairResult,
            errors: repairResult.errors.length,
          }
        : undefined,
      report: {
        mismatchedQtIds: validationReport.mismatchedQtIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          currentQtId: m.currentQtId,
          reason: m.reason,
        })),
        mismatchedWzIds: validationReport.mismatchedWzIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          currentWzId: m.currentWzId,
          reason: m.reason,
        })),
        missingQtIds: validationReport.missingQtIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          suggestedQtId: m.suggestedQtId,
        })),
        missingWzIds: validationReport.missingWzIds.map((m) => ({
          parkId: m.parkId,
          parkName: m.parkName,
          suggestedWzId: m.suggestedWzId,
        })),
        duplicates: validationReport.duplicates.map((d) => ({
          park1: d.park1,
          park2: d.park2,
          score: d.score,
          reason: d.reason,
        })),
      },
    };
  }

  /**
   * Publish ride profiles curated directly in the database.
   *
   * The curation is hand-written SQL now, and SQL cannot evict a Redis key or
   * ping the frontend. Without this, a corrected ride surfaces only as the
   * caches age out — `park:integrated` up to 6h, the Cloudflare copy 900s on
   * top, and the frontend pinning whatever it read for a day. Call it after a
   * curation session; it evicts, revalidates, and schedules the second sweep
   * past the CDN window.
   */
  @Post("publish-ride-profiles")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Publish hand-curated ride profiles",
    description:
      "Evicts the park caches for every ride profile whose `seeded_at` falls " +
      "in the last `sinceHours` (default 24) and tells the frontend to " +
      "revalidate. Set `seeded_at = now()` when you edit a row.",
  })
  @ApiResponse({ status: 202, description: "Job queued" })
  async triggerPublishRideProfiles(
    @Body() body?: { sinceHours?: number },
  ): Promise<{ message: string; jobId: string }> {
    const job = await this.manualMetadataQueue.add(
      "publish-ride-profiles",
      { sinceHours: body?.sinceHours },
      { priority: 1 },
    );
    return { message: "Ride profile publish queued", jobId: job.id.toString() };
  }

  /**
   * Check that every glossary term id the curation stores still resolves.
   *
   * Replaces the CI check that went away with the ride-profile seed. Both
   * sides are needed and only one is in this repo, so the frontend publishes
   * the ids that resolve to a page and this diffs them against the ids the
   * database actually stores. Synchronous: it is one HTTP call plus one query
   * over a few hundred rows, and the answer is the point of calling it.
   */
  @Get("ride-profile-term-audit")
  @ApiOperation({
    summary: "Find ride-profile term ids the glossary no longer defines",
    description:
      "A broken id is invisible at runtime — the ride page drops it and the " +
      "layout just reads short. Names every offending id and the rides it " +
      "damages.",
  })
  @ApiResponse({ status: 200, description: "Audit result" })
  async getRideProfileTermAudit(): Promise<RideProfileTermAudit> {
    return this.rideProfileAudit.audit();
  }

  /**
   * Import ride measurements from Wikidata.
   *
   * Speed, height, length and duration — none of which exists anywhere else in
   * this system. Joined on the RCDB id we already hold (which came from
   * Wikidata property P2751), a few hundred rides per SPARQL query, so the
   * whole catalogue is a handful of requests. Rows imported in the last 90 days
   * are skipped, and rides Wikidata states nothing for stay eligible for a
   * later run.
   */
  @Post("import-ride-stats")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Import ride measurements from Wikidata",
    description:
      "Queues a fetch for every curated ride that has an rcdbId and no recent " +
      "import. Pass `limit` for a trial run. Re-runnable and self-limiting.",
  })
  @ApiResponse({ status: 202, description: "Job queued" })
  async triggerRideStats(
    @Body() body?: { limit?: number },
  ): Promise<{ message: string; jobId: string }> {
    const job = await this.rideStatsQueue.add(
      "import-stats",
      { limit: body?.limit },
      { priority: 1 },
    );
    return { message: "Ride stat import queued", jobId: job.id.toString() };
  }

  /**
   * Trigger the Six Flags ride-height sync.
   *
   * ThemeParks.wiki carries no minimumHeight for the Six Flags and former
   * Cedar Fair parks, so this reads it off the parks' own ride pages. Runs
   * weekly on its own; this is for filling the gap on demand.
   */
  @Post("sync-six-flags-heights")
  @AdminMinRole("editor")
  @HttpCode(HttpStatus.ACCEPTED)
  @ApiOperation({
    summary: "Fill missing ride heights from the Six Flags sites",
    description:
      "Queues a sweep over every attraction still missing a minimumHeight in " +
      "a Six Flags or former Cedar Fair park. Gap-filling only — an existing " +
      "height is never overwritten.",
  })
  @ApiResponse({ status: 202, description: "Job queued" })
  async triggerSixFlagsHeights(): Promise<{ message: string; jobId: string }> {
    const job = await this.sixFlagsHeightsQueue.add(
      "sync-heights",
      {},
      { priority: 10 },
    );
    return {
      message: "Six Flags height sync queued",
      jobId: job.id.toString(),
    };
  }

  /**
   * Correct a park's real-world location.
   *
   * A failed geocode can put a park on the wrong continent, and a merge may
   * legitimately keep that row when it holds the better data lineage. Changing
   * the city changes the public path, so this records an alias for the old
   * path and triggers frontend revalidation instead of silently breaking
   * already-indexed URLs.
   */
  @Post("parks/:id/correct-location")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Correct a park's city and/or coordinates",
    description:
      "Updates city/citySlug/latitude/longitude. A changed city records a " +
      "slug alias for the old path so indexed URLs keep resolving, and " +
      "revalidates the frontend.",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        city: { type: "string" },
        citySlug: {
          type: "string",
          description: "Derived from city if omitted",
        },
        latitude: { type: "number" },
        longitude: { type: "number" },
      },
    },
  })
  @ApiResponse({ status: 200, description: "Location corrected" })
  async correctParkLocation(
    @Param("id") id: string,
    @Body()
    body: {
      city?: string;
      citySlug?: string;
      latitude?: number;
      longitude?: number;
    },
  ): Promise<{
    message: string;
    parkId: string;
    city: string | null;
    citySlug: string | null;
    latitude: number | null;
    longitude: number | null;
    pathChanged: boolean;
  }> {
    const { park, pathChanged } = await this.parkRenameService.correctLocation(
      id,
      body,
    );
    return {
      message: pathChanged
        ? "Location corrected; old path kept alive as an alias"
        : "Location corrected",
      parkId: park.id,
      city: park.city,
      citySlug: park.citySlug,
      latitude: park.latitude,
      longitude: park.longitude,
      pathChanged,
    };
  }

  /**
   * List duplicate attraction rows inside a park.
   *
   * These come from the sync keying on a source-scoped externalId, so the same
   * ride ends up as "x" and "x-2". Pairs are classified: a shared slug stem is
   * NOT on its own evidence — one real pair holds two genuinely different
   * rides — so only pairs whose names match (ignoring punctuation) or that
   * share a Queue-Times id are marked safe to merge automatically.
   */
  @Get("duplicate-attractions")
  @ApiOperation({
    summary: "List duplicate attraction rows within parks",
    description:
      "Reports base/suffix slug pairs per park with the recommended winner, " +
      "the slug that would survive, and whether the pair is safe to auto-merge.",
  })
  @ApiResponse({ status: 200, description: "Duplicate pairs with a verdict" })
  async listDuplicateAttractions(): Promise<{
    total: number;
    safe: number;
    needsReview: number;
    pairs: DuplicatePairReport[];
  }> {
    const pairs = await this.attractionMergeService.findDuplicatePairs();
    return {
      total: pairs.length,
      safe: pairs.filter((p) => p.safe).length,
      needsReview: pairs.filter((p) => !p.safe).length,
      pairs,
    };
  }

  /**
   * The park duplicates, and which row a merge would keep.
   *
   * A read, which the park side did not have: detection lived inside
   * `POST merge-duplicate-parks` behind `autoDetect: true`, and that flag
   * merged everything it found in the same call. So the only way to ask "what
   * would you merge" was to merge it, and the admin's "search" button ended up
   * sending `autoDetect: false` with no ids — a combination the endpoint
   * answers with its own usage message and nothing else. That flag is now a
   * dry run unless `dryRun: false` says otherwise and merges only `safe`
   * pairs, so it can answer the question too; this route stays the read,
   * because asking it should not need the `owner` role the merge does.
   *
   * `winnerId` is resolved by `determineMergeWinner`, the same function the
   * merge uses, because the order two ids are typed in carries no weight and
   * an operator who believes otherwise deletes the wrong park.
   *
   * `safe` and `needsReview` are counted apart, like the attraction listing
   * does, so the two sets are readable without going pair by pair: the safe
   * ones are what `autoDetect` would merge, the others are the work.
   */
  @Get("duplicate-parks")
  @ApiOperation({
    summary: "List parks that look like the same place, with the likely winner",
  })
  @ApiResponse({ status: 200, description: "Duplicate park pairs" })
  async listDuplicateParks(): Promise<{
    total: number;
    safe: number;
    needsReview: number;
    pairs: Array<{
      park1: { id: string; name: string; city: string | null };
      park2: { id: string; name: string; city: string | null };
      score: number;
      reason: string;
      winnerId: string | null;
      loserId: string | null;
      safe: boolean;
      reviewReason: string | null;
    }>;
  }> {
    const duplicates = await this.parkValidatorService.findDuplicates();
    if (duplicates.length === 0)
      return { total: 0, safe: 0, needsReview: 0, pairs: [] };

    // One query for every park involved, rather than two per pair.
    const parkRepo = this.parkValidatorService.getParkRepository();
    const ids = Array.from(
      new Set(duplicates.flatMap((d) => [d.park1.id, d.park2.id])),
    );
    const parks = await parkRepo.find({ where: { id: In(ids) } });
    const parkById = new Map(parks.map((park) => [park.id, park]));

    return {
      total: duplicates.length,
      safe: duplicates.filter((d) => d.safe).length,
      needsReview: duplicates.filter((d) => !d.safe).length,
      pairs: duplicates.map((duplicate) => {
        const park1 = parkById.get(duplicate.park1.id);
        const park2 = parkById.get(duplicate.park2.id);
        const verdict =
          park1 && park2 ? determineMergeWinner(park1, park2) : null;
        return {
          park1: duplicate.park1,
          park2: duplicate.park2,
          score: duplicate.score,
          reason: duplicate.reason,
          winnerId: verdict?.winnerId ?? null,
          loserId: verdict?.loserId ?? null,
          safe: duplicate.safe,
          reviewReason: duplicate.reviewReason,
        };
      }),
    };
  }

  /**
   * Merge duplicate attraction rows.
   *
   * Defaults to a dry run — pass dryRun:false to actually write. Pairs flagged
   * for review are never merged, whatever the flags say.
   */
  @Post("merge-duplicate-attractions")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Merge duplicate attraction rows within parks",
    description:
      "Merges pairs proven to be the same ride, one transaction each. The " +
      "surviving row takes the base slug (the URL in the sitemap) and inherits " +
      "any metadata it was missing. Defaults to a dry run.",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        dryRun: {
          type: "boolean",
          description: "Report what would happen without writing",
          default: true,
        },
        limit: {
          type: "number",
          description: "Merge at most this many pairs (useful for a first run)",
        },
        winnerId: {
          type: "string",
          description:
            "Merge exactly one pair: the row to keep. Honours `dryRun` like " +
            "the automatic path — send `dryRun: false` to actually merge.",
        },
        loserId: {
          type: "string",
          description: "Merge exactly one pair: the row to remove",
        },
      },
    },
  })
  @ApiResponse({ status: 200, description: "Merge outcome" })
  async mergeDuplicateAttractions(
    @Body()
    body: {
      dryRun?: boolean;
      limit?: number;
      winnerId?: string;
      loserId?: string;
    } = {},
  ): Promise<unknown> {
    if (body.winnerId && body.loserId) {
      // `dryRun` governs this branch too. It did not: the pair went straight
      // to `mergeAttractions`, which takes no such flag — so the admin's
      // "Probelauf" button deleted the losing row for real, inside a
      // transaction with no undo, and then displayed the outcome as a preview.
      // The flag defaults to true here for the same reason it does below: the
      // destructive reading of an unset parameter is the wrong one, and this
      // endpoint's own documentation already promised the safe one.
      if (body.dryRun !== false) {
        return this.attractionMergeService.previewMerge(
          body.winnerId,
          body.loserId,
        );
      }
      return this.attractionMergeService.mergeAttractions(
        body.winnerId,
        body.loserId,
      );
    }

    return this.attractionMergeService.mergeDuplicates({
      dryRun: body.dryRun !== false,
      limit: body.limit,
    });
  }

  /**
   * Merge duplicate parks
   *
   * Identifies and merges duplicate parks, or merges specific parks if IDs are provided.
   *
   * **`autoDetect` picks its own victims, so it defaults to a dry run** and
   * merges only pairs `findDuplicates` marks `safe` — `dryRun: false` buys the
   * write, never the pairs a human still has to look at. Before PAR-247 it had
   * neither: every pair the detector returned was merged inside one call, in a
   * transaction with no undo, and the only way to ask what it would do was to
   * let it do it.
   *
   * **The manual pair keeps its default**, which is a real merge. Two ids typed
   * into a form are the human judgement `autoDetect` lacks, and the admin's
   * merge button sends exactly that body with no `dryRun` — a default flipped
   * here would be a button that silently stops working. It does honour an
   * explicit `dryRun: true`, because the alternative is the trap the attraction
   * endpoint already sprang once: a request that says "Probelauf", deletes the
   * row, and prints the outcome as a preview.
   */
  @Post("merge-duplicate-parks")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Merge duplicate parks",
    description:
      "Identifies duplicate parks automatically or merges specific parks if park1Id and park2Id are provided. " +
      "Winner is determined by priority (Wiki-ID, more Entity-IDs, more Child-Entities, older park). " +
      "autoDetect defaults to a dry run and never merges a pair marked for review.",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        park1Id: {
          type: "string",
          description: "First park ID (optional, for manual merge)",
        },
        park2Id: {
          type: "string",
          description: "Second park ID (optional, for manual merge)",
        },
        autoDetect: {
          type: "boolean",
          description: "Automatically detect and merge all duplicates",
          default: false,
        },
        // No `default` here: it is true on the autoDetect path and false on
        // the manual one, and a schema can only publish one of them — which
        // would be a machine-readable claim that is wrong for half the calls.
        dryRun: {
          type: "boolean",
          description:
            "With autoDetect: report what would be merged without writing. " +
            "Defaults to true there, so a real run needs dryRun:false. With " +
            "park1Id/park2Id it defaults to false (that pair is a deliberate " +
            "act) and dryRun:true previews the winner.",
        },
      },
    },
  })
  @ApiResponse({
    status: 200,
    description: "Merge operation completed",
  })
  async mergeDuplicateParks(
    @Body()
    body: {
      park1Id?: string;
      park2Id?: string;
      autoDetect?: boolean;
      dryRun?: boolean;
    } = {},
  ): Promise<{
    message: string;
    dryRun: boolean;
    merged: number;
    planned: ParkMergePlanEntry[];
    skipped: ParkMergePlanEntry[];
    results: Array<{
      winnerId: string;
      winnerName: string;
      loserId: string;
      loserName: string;
      migratedAttractions: number;
      migratedShows: number;
      migratedRestaurants: number;
      migratedScheduleEntries: number;
      migratedMappings: number;
    }>;
    errors: Array<{ parkId: string; error: string }>;
  }> {
    const results: Array<{
      winnerId: string;
      winnerName: string;
      loserId: string;
      loserName: string;
      migratedAttractions: number;
      migratedShows: number;
      migratedRestaurants: number;
      migratedScheduleEntries: number;
      migratedMappings: number;
    }> = [];
    const errors: Array<{ parkId: string; error: string }> = [];
    const planned: ParkMergePlanEntry[] = [];
    const skipped: ParkMergePlanEntry[] = [];

    // Both flags decide whether rows get deleted, so an uninterpretable value
    // is refused rather than read as one side of the question.
    const autoDetectFlag = readBodyFlag(body.autoDetect);
    const dryRunFlag = readBodyFlag(body.dryRun);
    for (const [name, raw, parsed] of [
      ["autoDetect", body.autoDetect, autoDetectFlag],
      ["dryRun", body.dryRun, dryRunFlag],
    ] as const) {
      if (raw !== undefined && raw !== null && parsed === undefined) {
        throw new BadRequestException(
          `${name} must be a boolean (true or false)`,
        );
      }
    }

    if (autoDetectFlag) {
      // Nothing is written unless the caller asks for it in as many words.
      const dryRun = dryRunFlag !== false;

      // Auto-detect duplicates
      const duplicates = await this.parkValidatorService.findDuplicates();

      if (duplicates.length === 0) {
        return {
          message: "No duplicates found",
          dryRun,
          merged: 0,
          planned: [],
          skipped: [],
          results: [],
          errors: [],
        };
      }

      // Use repair service to handle merges
      const mergePairs: Array<{ winnerId: string; loserId: string }> = [];
      const parkRepo = this.parkValidatorService.getParkRepository();

      // Pre-fetch every park involved in any duplicate pair in a single
      // query. The old loop did 2× findOne() per duplicate — for 10
      // dupes that's 20 round-trips; this batch is one.
      const allIds = Array.from(
        new Set(
          duplicates.flatMap((d) => [d.park1.id, d.park2.id]).filter(Boolean),
        ),
      );
      const parks = allIds.length
        ? await parkRepo.find({ where: { id: In(allIds) } })
        : [];
      const parkById = new Map(parks.map((p) => [p.id, p]));

      for (const duplicate of duplicates) {
        // Determine winner based on priority
        const park1 = parkById.get(duplicate.park1.id);
        const park2 = parkById.get(duplicate.park2.id);

        if (!park1 || !park2) {
          errors.push({
            parkId: duplicate.park1.id || duplicate.park2.id,
            error: "Park not found",
          });
          continue;
        }

        const verdict = determineMergeWinner(park1, park2);
        const winner = verdict.winnerId === park1.id ? park1 : park2;
        const loser = verdict.winnerId === park1.id ? park2 : park1;
        const entry: ParkMergePlanEntry = {
          winnerId: winner.id,
          winnerName: winner.name,
          loserId: loser.id,
          loserName: loser.name,
          score: duplicate.score,
          reason: duplicate.reason,
          reviewReason: duplicate.reviewReason,
        };

        // A pair a human still has to look at is reported and left alone —
        // `dryRun: false` is permission to write, not permission to decide.
        if (!duplicate.safe) {
          skipped.push(entry);
          continue;
        }

        planned.push(entry);
        mergePairs.push(verdict);
      }

      if (dryRun) {
        return {
          message: `Dry run: ${planned.length} pair(s) would be merged, ${skipped.length} need review`,
          dryRun: true,
          merged: 0,
          planned,
          skipped,
          results: [],
          errors,
        };
      }

      // Use repair service to perform merges
      const repairResult =
        await this.parkRepairService.repairDuplicates(mergePairs);

      // Convert repair result to response format. The names come from
      // `planned`, which was built from the park rows themselves — the entry
      // and its merge pair are pushed together, so the two lists line up, and
      // `repairDuplicates` returns one verdict per pair in that same order.
      //
      // The verdict is read BY POSITION and never searched for by id.
      // `repairResult.errors` is keyed by park, and one run can hold the same
      // row as the loser of two pairs: three rows for one park give the
      // detector (1,2), (1,3) and (2,3), the first merge deletes the shared
      // row, the second fails on it, and the failure carries exactly the
      // `loserId` the successful pair also has. An id search therefore reads
      // the second pair's failure onto the first, dropping a park that really
      // was deleted out of `results` — the one list that says what this call
      // did — and undercounting `merged`.
      //
      // The position carries the names, so the ids are checked rather than
      // trusted: this endpoint deletes parks, and a verdict read against the
      // wrong entry would report a real deletion under another park's name,
      // which is worse than reporting nothing. The two ids in the verdict make
      // that answerable here instead of only at the call site that built both
      // lists.
      //
      // The walk goes over `planned` and not over the verdicts, so that a list
      // which is too SHORT is a reported error rather than a quiet tail of
      // pairs nobody accounts for — which would be this issue's own bug in a
      // new place. Surplus verdicts are reported after the loop, so neither
      // direction can go missing.
      planned.forEach((pair, index) => {
        const verdict = repairResult.pairs[index];

        if (
          !verdict ||
          pair.winnerId !== verdict.winnerId ||
          pair.loserId !== verdict.loserId
        ) {
          errors.push({
            parkId: pair.loserId,
            error: `No merge verdict lined up with the planned pair ${pair.winnerId} / ${pair.loserId} at position ${index}; whether it merged is unknown`,
          });
          return;
        }

        if (!verdict.merged) {
          return;
        }

        // Note: We don't have detailed migration counts from repairDuplicates
        // The merge service logs them, but repairDuplicates doesn't return them
        // For now, we'll use placeholder values
        results.push({
          winnerId: pair.winnerId,
          winnerName: pair.winnerName,
          loserId: pair.loserId,
          loserName: pair.loserName,
          migratedAttractions: 0, // Would need to enhance repairDuplicates to return this
          migratedShows: 0,
          migratedRestaurants: 0,
          migratedScheduleEntries: 0,
          migratedMappings: 0,
        });
      });

      for (const surplus of repairResult.pairs.slice(planned.length)) {
        errors.push({
          parkId: surplus.loserId,
          error: `Merge verdict for ${surplus.winnerId} / ${surplus.loserId} has no planned pair; it is left out of the results`,
        });
      }

      // A skipped pair can share a row with a pair that just merged: three
      // rows for one park give A–B safe and B–C for review, and B is gone by
      // the time the operator reads the list. The entry still names it, so the
      // "send it back as a manual pair" this list is for would answer "Park
      // not found".
      //
      // Read off `planned` rather than `results`, i.e. every row a merge was
      // ATTEMPTED on, which is what the warning is about: a failed attempt is
      // the case where the row is gone and this run is the reason. In the same
      // trio, the second pair fails precisely BECAUSE the first one deleted the
      // row they share, and `results` holds only the first. The wider set costs
      // a warning on a row that is still there, and the warning says re-run
      // detection, which is true either way.
      //
      // Until PAR-268, `results` was unusable here for a second reason — it
      // dropped the successful pair of exactly this trio. It is per-pair now,
      // and `planned` is still the set this warning wants.
      const touchedByAMerge = new Set(planned.map((p) => p.loserId));
      for (const entry of skipped) {
        if (
          !touchedByAMerge.has(entry.winnerId) &&
          !touchedByAMerge.has(entry.loserId)
        ) {
          continue;
        }
        entry.reviewReason = `a merge in this run was attempted on one of these rows, which may no longer exist — re-run detection before acting${
          entry.reviewReason ? ` (${entry.reviewReason})` : ""
        }`;
      }

      // Add errors from repair result
      errors.push(...repairResult.errors);
    } else if (body.park1Id && body.park2Id) {
      // Manual merge - determine winner
      const parkRepo = this.parkValidatorService.getParkRepository();
      const park1 = await parkRepo.findOne({
        where: { id: body.park1Id },
      });
      const park2 = await parkRepo.findOne({
        where: { id: body.park2Id },
      });

      if (!park1 || !park2) {
        return {
          message: "One or both parks not found",
          dryRun: dryRunFlag === true,
          merged: 0,
          planned: [],
          skipped: [],
          results: [],
          errors: [
            {
              parkId: body.park1Id || body.park2Id,
              error: "Park not found",
            },
          ],
        };
      }

      const { winnerId, loserId } = determineMergeWinner(park1, park2);
      const winner = winnerId === park1.id ? park1 : park2;
      const loser = winnerId === park1.id ? park2 : park1;

      // Only an explicit `true` previews here — see the method's docblock for
      // why this default is the other way round from `autoDetect`'s.
      if (dryRunFlag === true) {
        // A preview that promises what the write would refuse is worse than no
        // preview: `mergeParks` rejects one id on both sides, so the dry run
        // has to reject it too, in the same words.
        if (winnerId === loserId) {
          return {
            message: `Refusing to merge park ${winnerId} into itself`,
            dryRun: true,
            merged: 0,
            planned: [],
            skipped: [],
            results: [],
            errors: [
              {
                parkId: winnerId,
                error: `Refusing to merge park ${winnerId} into itself`,
              },
            ],
          };
        }

        return {
          message: `Dry run: "${loser.name}" would be merged into "${winner.name}"`,
          dryRun: true,
          merged: 0,
          planned: [
            {
              winnerId,
              winnerName: winner.name,
              loserId,
              loserName: loser.name,
              score: null,
              reason: "manual pair",
              reviewReason: null,
            },
          ],
          skipped: [],
          results: [],
          errors: [],
        };
      }

      try {
        const mergeResult = await this.parkMergeService.mergeParks(
          winnerId,
          loserId,
        );

        if (mergeResult.success) {
          results.push({
            winnerId: mergeResult.winnerId,
            winnerName: mergeResult.winnerName,
            loserId: mergeResult.loserId,
            loserName: mergeResult.loserName,
            migratedAttractions: mergeResult.migratedAttractions,
            migratedShows: mergeResult.migratedShows,
            migratedRestaurants: mergeResult.migratedRestaurants,
            migratedScheduleEntries: mergeResult.migratedScheduleEntries,
            migratedMappings: mergeResult.migratedMappings,
          });
        } else {
          errors.push({
            parkId: loserId,
            error: mergeResult.errors.join(", "),
          });
        }
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        errors.push({ parkId: loserId, error: errorMessage });
      }
    } else {
      return {
        message:
          "Either autoDetect=true or both park1Id and park2Id must be provided",
        dryRun: false,
        merged: 0,
        planned: [],
        skipped: [],
        results: [],
        errors: [],
      };
    }

    return {
      message: `Merged ${results.length} duplicate park(s)`,
      dryRun: false,
      merged: results.length,
      planned,
      skipped,
      results,
      errors,
    };
  }
}
