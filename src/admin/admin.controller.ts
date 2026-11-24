import {
  Get,
  Post,
  HttpCode,
  HttpStatus,
  Body,
  Param,
  Query,
  BadRequestException,
  NotFoundException,
} from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiBody, ApiQuery } from "@nestjs/swagger";
import {
  RideProfileAuditService,
  RideProfileTermAudit,
} from "../attractions/services/ride-profile-audit.service";
import {
  ParkValidatorService,
  type DuplicatePairVerdict,
} from "../parks/services/park-validator.service";
import { ParkRepairService } from "../parks/services/park-repair.service";
import {
  AttractionMergeService,
  DuplicatePairReport,
} from "../attractions/services/attraction-merge.service";
import { ParkRenameService } from "../parks/services/park-rename.service";
import { SystemHealthService } from "./system-health.service";
import { ForecastArchiveScoringService } from "../parks/services/forecast-archive-scoring.service";
import {
  DAY_ORIGINS,
  ModelComparisonService,
} from "../parks/services/model-comparison.service";
import {
  ModelComparisonDayDto,
  ModelComparisonDayOptionsDto,
  ModelComparisonOriginId,
  ModelComparisonResponseDto,
} from "../ml/dto/model-comparison.dto";
import {
  SCORE_REGIONS,
  type ScoreRegion,
} from "../ml/utils/forecast-archive-scoring.util";
import { DowntimeMeasurementService } from "./downtime-measurement.service";
import { MlOverviewService } from "./ml-overview.service";
import { AdminQueueStatusService } from "./admin-queue-status.service";
import { MlOverviewDto } from "../ml/dto/ml-overview.dto";
import { AdminApi } from "./auth/admin-api.decorator";
import { AdminMinRole, CurrentAdmin } from "./auth/admin-auth.decorators";
import type { AdminPrincipal } from "./auth/admin-principal";
import { McpToolError } from "../mcp/mcp-tool";
import { DataQualityMonitorService } from "../monitoring/data-quality-monitor.service";
import {
  DISPLAY_CONSISTENCY_DEFAULT_DAYS,
  DISPLAY_CONSISTENCY_MAX_DAYS,
  DisplayConsistencyService,
} from "../monitoring/display-consistency/display-consistency.service";
import { QueryInt } from "../common/pipes/query-int.pipe";
import { ISO_DATE_PATTERN } from "../common/utils/date.util";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import {
  AttractionRetirementService,
  RetirementRequest,
  isOnParkPage,
  retiredKindOf,
} from "../attractions/services/attraction-retirement.service";
import {
  AttractionReviewService,
  ReviewMarkRequest,
} from "../attractions/services/attraction-review.service";
import {
  ParkDuplicateMergeService,
  listBySafety,
  type DuplicateListing,
  type ParkMergeRequest,
  type ParkMergeResponse,
} from "./park-duplicate-merge.service";
import {
  toParkValidationResponse,
  type ParkValidationResponse,
} from "./park-validation-response";

/** What the `region` query of the two forecast boards takes. */
const SCORE_REGION_DESCRIPTION = `${SCORE_REGIONS[0]} (default), ${SCORE_REGIONS.slice(1).join(", ")}`;

/** A value of the score's region axis, `ALL` when absent; anything else is a 400. */
function parseScoreRegion(region?: string): ScoreRegion {
  const upper = (region ?? "ALL").toUpperCase();
  const known = SCORE_REGIONS.find((r) => r === upper);
  if (!known) {
    throw new BadRequestException(
      `region must be ${SCORE_REGIONS.slice(0, -1).join(", ")} or ${SCORE_REGIONS[SCORE_REGIONS.length - 1]}`,
    );
  }
  return known;
}

/**
 * Admin Controller
 *
 * Every endpoint here needs a signed-in administrator — see `AdminAuthGuard`,
 * which `@AdminApi` puts on the class rather than on each method so that a new
 * endpoint is protected by default and has to opt OUT to be public.
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
 * Curation needs `editor`. The handful that cannot be undone — merges,
 * retirements, repair — need `owner`. The routes that queue jobs or delete the
 * cache are in `AdminJobsController`, under the same prefix.
 */
@AdminApi("admin")
export class AdminController {
  constructor(
    private readonly parkValidatorService: ParkValidatorService,
    private readonly parkRepairService: ParkRepairService,
    private readonly parkDuplicateMerge: ParkDuplicateMergeService,
    private readonly attractionMergeService: AttractionMergeService,
    private readonly parkRenameService: ParkRenameService,
    private readonly rideProfileAudit: RideProfileAuditService,
    private readonly dataQualityMonitor: DataQualityMonitorService,
    private readonly retirementService: AttractionRetirementService,
    private readonly reviewService: AttractionReviewService,
    private readonly systemHealth: SystemHealthService,
    private readonly downtimeMeasurement: DowntimeMeasurementService,
    private readonly queueStatus: AdminQueueStatusService,
    private readonly displayConsistency: DisplayConsistencyService,
    private readonly forecastArchiveScoring: ForecastArchiveScoringService,
    private readonly modelComparison: ModelComparisonService,
    private readonly mlOverview: MlOverviewService,
  ) {}

  // ── retirement ─────────────────────────────────────────────────────────────

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

  /**
   * Retired rows with a younger live row right beside them, for the admin to
   * merge or dismiss (PAR-686). Same list as `reissueCandidates` on
   * `data-quality`; its own route so the duplicates screen does not pull the
   * whole data-quality report to show one section.
   */
  @Get("reissue-candidates")
  @ApiOperation({
    summary:
      "Absence-retired rides with a younger live ride within 30 m — possible re-issues under a new name",
  })
  async getReissueCandidates() {
    const candidates = await this.dataQualityMonitor.findReissueCandidates();
    return {
      total: candidates.length,
      namesMatch: candidates.filter((c) => c.namesMatch).length,
      candidates,
    };
  }

  @Get("retirement-candidates")
  @ApiOperation({
    summary: "Attractions whose feed went quiet, minus those already cleared",
  })
  async getRetirementCandidates() {
    const candidates = await this.reviewService.findRetirementCandidates();
    return { total: candidates.length, candidates };
  }

  /**
   * Retire attractions that no longer exist.
   *
   * Not a plain UPDATE, because retirement has to drag the caches and the
   * frontend sitemap along with it — a removed slug otherwise stays advertised
   * for up to 24h plus the CDN window. Each entry carries the date and the
   * source it was established from; a retirement is a claim about the world.
   */
  @Post("retire-attractions")
  @AdminMinRole("owner")
  @ApiOperation({ summary: "Retire attractions that no longer exist" })
  async retireAttractions(@Body() body: { retirements: RetirementRequest[] }) {
    const retired = await this.retirementService.retire(body.retirements ?? []);
    return { retired: retired.length, attractions: retired };
  }

  @Post("retire-parks")
  @AdminMinRole("owner")
  @ApiOperation({
    summary: "Retire a park that closed for good (and its still-active rides)",
    description:
      "`retiredAt` is the operator's last operating day; `reason` is shown to " +
      "users and should carry the source. The park keeps its page and leaves " +
      "every listing, search and job loop.",
  })
  async retireParks(
    @Body()
    body: {
      retirements?: { parkId: string; retiredAt: string; reason: string }[];
    },
  ) {
    const parks = [];
    for (const r of body?.retirements ?? []) {
      parks.push(
        await this.retirementService.retirePark(
          r.parkId,
          r.retiredAt,
          r.reason,
        ),
      );
    }
    return { retired: parks.length, parks };
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
        kind: retiredKindOf(a),
        hidden: a.retiredHidden,
        // False for a reclassification, a hidden ride, and a closure older
        // than CLOSED_RIDE_PARK_PAGE_DAYS.
        onParkPage: isOnParkPage(a),
      })),
    };
  }

  @Post("retired-attractions/:id/hidden")
  @AdminMinRole("owner")
  @ApiOperation({
    summary:
      "Show or hide a closed ride on its park page (ride page and sitemap stay)",
  })
  async setRetiredHidden(
    @Param("id") id: string,
    @Body() body: { hidden?: unknown },
  ) {
    if (typeof body?.hidden !== "boolean") {
      throw new BadRequestException("`hidden` must be true or false");
    }
    return {
      updated: await this.retirementService.setRetiredHidden(id, body.hidden),
    };
  }

  // ── monitoring ─────────────────────────────────────────────────────────────

  /**
   * On-demand view of the two silent-failure detectors that run daily at 06:45.
   *
   * `windowDays` exists so the cluster detector can be pointed at history: it
   * defaults to a short window on purpose, so a reported cluster falls quiet by
   * itself after a couple of weeks rather than nagging forever. Ask for 120
   * days to see the known incidents (Europa-Park 2026-06-07 and friends).
   */
  @Get("data-quality")
  @ApiOperation({
    summary:
      "Silenced attraction clusters, failing jobs, and absence-retired rides nobody has given a season",
  })
  @ApiQuery({
    name: "windowDays",
    required: false,
    description: "Cluster window in days, 1-400, default 14",
  })
  async getDataQuality(
    @QueryInt({ name: "windowDays", fallback: 14, min: 1, max: 400 })
    windowDays: number,
  ) {
    return this.dataQualityMonitor.report(windowDays);
  }

  /**
   * The last display-consistency run: per park the mismatches between surfaces
   * that must show the same value and the surfaces it could not read, and each
   * flagged park's details. docs/analytics/display-consistency.md.
   */
  @Get("display-consistency")
  @ApiOperation({
    summary:
      "The last display-consistency run: parks whose surfaces disagree about a park-day, or could not be read",
  })
  async getDisplayConsistency() {
    return (
      (await this.displayConsistency.latestRun()) ?? {
        summary: null,
        parks: [],
      }
    );
  }

  /**
   * Reads one park's days as every surface shows them now and compares them.
   * Writes nothing: the caches it reads through are the ones the public
   * endpoints fill on a miss.
   */
  @Get("display-consistency/check")
  @ApiOperation({
    summary:
      "Compare what every surface shows for a park's next days (calendar, best-days, /plan/day, trip region, park payload, Parky, park.fan's calendar and best-days relays)",
  })
  @ApiQuery({
    name: "park",
    required: true,
    description: "Park id, slug or name",
  })
  @ApiQuery({
    name: "days",
    required: false,
    description: `Days from today, today included (1–${DISPLAY_CONSISTENCY_MAX_DAYS}, default ${DISPLAY_CONSISTENCY_DEFAULT_DAYS})`,
  })
  async checkDisplayConsistency(
    @CurrentAdmin() admin: AdminPrincipal,
    @Query("park") reference?: string,
    @QueryInt({
      name: "days",
      fallback: DISPLAY_CONSISTENCY_DEFAULT_DAYS,
      min: 1,
      max: DISPLAY_CONSISTENCY_MAX_DAYS,
    })
    days: number = DISPLAY_CONSISTENCY_DEFAULT_DAYS,
  ) {
    if (!reference?.trim()) throw new BadRequestException("park is required");
    const found = await this.displayConsistency
      .findPark(reference, admin)
      .catch((error: unknown) => {
        if (error instanceof McpToolError) {
          throw new NotFoundException(`No park "${reference}"`);
        }
        throw error;
      });
    if ("ambiguous" in found) return found;
    return this.displayConsistency.check(found, days);
  }

  /**
   * Phase 0 of the downtime work. Reads, counts, writes nothing.
   *
   * Answers in EVENTS, which is the unit nobody had: `downCount` on the history
   * endpoint counts distinct clock hours carrying a DOWN reading, across the
   * whole park-local calendar day and with no opening-hours bound, so it reads
   * 16 on a ten-hour park day. Every threshold in
   * `docs/analytics/ride-downtime/publishing.md#ride-gates` is provisional until
   * this has run.
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
    @QueryInt({ name: "days", min: 1, max: 365 }) days?: number,
  ): Promise<Record<string, unknown>> {
    const parkSlugs = (parks ?? "")
      .split(",")
      .map((slug) => slug.trim())
      .filter(Boolean);
    const measurement = await this.downtimeMeasurement.measure({
      parkSlugs,
      days,
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
    @QueryInt({ name: "days", min: 1, max: 365 }) days?: number,
  ): Promise<Record<string, unknown>> {
    const measurement = await this.downtimeMeasurement.measureErasure(days);
    return measurement as unknown as Record<string, unknown>;
  }

  /**
   * Aggregates host (CPU/RAM/disk), Postgres, Redis, and both ML services —
   * CatBoost (ml-service) + TFT (nf-service): training status/progress + model
   * quality + the TFT-vs-CatBoost scoreboard. One JSON for a monitoring UI.
   */
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

  @Get("queue-status")
  @ApiOperation({ summary: "Bull queue job counts per queue" })
  @ApiResponse({
    status: 200,
    description: "Active/pending/failed/delayed counts per queue",
  })
  async getQueueStatus(): Promise<Record<string, unknown>> {
    return {
      timestamp: new Date().toISOString(),
      queues: await this.queueStatus.counts(),
    };
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
  @ApiQuery({ name: "queue", required: true })
  @ApiQuery({ name: "limit", required: false, description: "1-20, default 3" })
  @ApiResponse({ status: 200, description: "Recent failures" })
  async getQueueFailures(
    @Query("queue") queueName: string,
    @QueryInt({ name: "limit", fallback: 3, min: 1, max: 20 }) limit: number,
  ): Promise<Record<string, unknown>> {
    const failures = await this.queueStatus.failures(queueName, limit);
    if (!failures) {
      return {
        error: `Unknown queue "${queueName}"`,
        known: this.queueStatus.inspectableQueues(),
      };
    }
    return { queue: queueName, failures };
  }

  // ── forecasts and models ───────────────────────────────────────────────────

  /**
   * Focused model-comparison board (intraday PCN-vs-CatBoost + the PCN lead curve).
   *
   * Unlike /system-health (host/gpu/postgres/redis + ML-service round-trips), this is
   * just the scoreboard SQL reads + a computed PCN−CatBoost verdict per segment, so
   * a UI can poll it cheaply. The daily TFT-vs-CatBoost and Shape sections were
   * retired in PAR-870.
   */
  @Get("ml-comparison")
  @ApiOperation({
    summary: "Intraday comparison board (PCN shadow) + verdict",
    description:
      "Light, pollable board: pcn_intraday_comparisons (PCN vs CatBoost, all " +
      "segments × lead buckets) with a computed PCN−CatBoost MAE delta per " +
      "segment, plus the PCN lead curve vs persistence.",
  })
  @ApiQuery({ name: "days", required: false, description: "1-90, default 14" })
  @ApiResponse({ status: 200, description: "Comparison boards + verdict" })
  async getMlComparison(
    @QueryInt({ name: "days", fallback: 14, min: 1, max: 90 }) days: number,
  ): Promise<Record<string, unknown>> {
    return this.systemHealth.comparisonBoard(days);
  }

  /**
   * The forward archive's board (PAR-831): how the curves users were SERVED
   * at each lead turned out, per use case x lead x source x segment, pooled
   * over the last `days` scored target dates. Read-only and cached for 2 h —
   * the scores move once a day.
   */
  @Get("forecast-archive")
  @ApiOperation({
    summary: "Forward archive scores (served curves vs truth, per lead)",
    description:
      "UC1/UC2 (served 15-min curve, slot leads h0-1 … h24-48 and ride-day " +
      "leads d0-d2), UC3 (plan/day d0-d7), UC3S (composer A/B shadow, PAR-834), " +
      "D6 (crowd bucket per park-day), " +
      "D9 (coverage). Counters are pooled before dividing. `chronos`: the " +
      "Chronos-2 shadow (PAR-852, never served) per use case × lead — own " +
      "MAE plus the paired difference vs served / persistence / H5 with a " +
      "95 % park-cluster bootstrap CI. See docs/ml/forward-archive/scoring.md#admin-board.",
  })
  @ApiQuery({ name: "days", required: false, description: "1-90, default 14" })
  @ApiQuery({
    name: "region",
    required: false,
    description: SCORE_REGION_DESCRIPTION,
  })
  async getForecastArchive(
    @QueryInt({ name: "days", fallback: 14, min: 1, max: 90 }) days: number,
    @Query("region") region?: string,
  ): Promise<Record<string, unknown>> {
    return this.forecastArchiveScoring.getBoard(days, parseScoreRegion(region));
  }

  /**
   * The model comparison (PAR-855): for every question the app answers, does
   * each model beat what is served today? Live from the forward archive's
   * paired rows (park-cluster bootstrap CI, gate 14 days / 10 parks), mixed
   * with the versioned offline verdicts. Read-only, cached 2 h per scoring run.
   */
  @Get("model-comparison")
  @ApiOperation({
    summary: "Model comparison matrix (question × model vs served)",
    description:
      "Cells: today | better | worse | unclear | pending, with the live " +
      "paired difference (model − served, minutes, negative = better) and its " +
      "95 % park-cluster CI, the offline verdict, the three switch checks and " +
      "when the outstanding one reports. See docs/ml/forward-archive/model-comparison.md.",
  })
  @ApiQuery({ name: "days", required: false, description: "1-90, default 28" })
  @ApiQuery({
    name: "region",
    required: false,
    description: SCORE_REGION_DESCRIPTION,
  })
  async getModelComparison(
    @QueryInt({ name: "days", fallback: 28, min: 1, max: 90 }) days: number,
    @Query("region") region?: string,
  ): Promise<ModelComparisonResponseDto> {
    return this.modelComparison.getMatrix(days, parseScoreRegion(region));
  }

  /**
   * The ML overview (PAR-861): what is up for decision (derived from the
   * model comparison matrix and src/ml/data/ml-decisions.json), the matrix
   * summary, and whether every ML component ran when it should have.
   * Read-only, bounded index lookups, cached 5 min.
   */
  @Get("ml-overview")
  @ApiOperation({
    summary: "ML overview: open decisions, matrix summary, component status",
    description:
      "decisions: ready | waiting_live | open | later | owner, with the live " +
      "progress of their least-measured matrix cell and the earliest date the " +
      "gate can be met. components: last run, expected cadence and " +
      "ok | late | failed | warming | unknown. See docs/ml/ml-overview.md.",
  })
  async getMlOverview(): Promise<MlOverviewDto> {
    return this.mlOverview.getOverview();
  }

  /** Dates, parks and rides the day comparison can show (PAR-855). */
  @Get("model-comparison/day/options")
  @ApiOperation({
    summary: "Model comparison: days, parks and rides with archived curves",
  })
  @ApiQuery({ name: "date", required: false, description: "YYYY-MM-DD" })
  async getModelComparisonDayOptions(
    @Query("date") date?: string,
  ): Promise<ModelComparisonDayOptionsDto> {
    if (date !== undefined && !ISO_DATE_PATTERN.test(date)) {
      throw new BadRequestException("date must be YYYY-MM-DD");
    }
    return this.modelComparison.getDayOptions(date);
  }

  /**
   * One real day (PAR-855): every archived curve of one ride from one origin
   * against the 15-min truth, park-local, with the MAE of each.
   */
  @Get("model-comparison/day")
  @ApiOperation({
    summary: "Model comparison: one ride-day, curves vs truth",
  })
  @ApiQuery({ name: "parkId", required: true })
  @ApiQuery({ name: "date", required: true, description: "YYYY-MM-DD" })
  @ApiQuery({ name: "attractionId", required: true })
  @ApiQuery({
    name: "origin",
    required: false,
    description: "morning (default), d1, d3, d7",
  })
  async getModelComparisonDay(
    @Query("parkId") parkId?: string,
    @Query("date") date?: string,
    @Query("attractionId") attractionId?: string,
    @Query("origin") origin?: string,
  ): Promise<ModelComparisonDayDto> {
    if (!isCanonicalUuid(parkId)) {
      throw new BadRequestException("parkId must be a UUID");
    }
    if (!isCanonicalUuid(attractionId)) {
      throw new BadRequestException("attractionId must be a UUID");
    }
    if (!date || !ISO_DATE_PATTERN.test(date)) {
      throw new BadRequestException("date must be YYYY-MM-DD");
    }
    const o = (origin ?? "morning") as ModelComparisonOriginId;
    if (!DAY_ORIGINS.some((x) => x.id === o)) {
      throw new BadRequestException("origin must be morning, d1, d3 or d7");
    }
    return this.modelComparison.getDay(parkId, date, attractionId, o);
  }

  // ── parks and rides ────────────────────────────────────────────────────────

  /**
   * Validates all parks against external APIs (Queue-Times, Wartezeiten.app)
   * and, with `autoFix`, adds the ids found missing. Mismatched ids and
   * duplicates are reported, never repaired here: both need a human.
   */
  @Post("validate-and-repair-parks")
  @AdminMinRole("owner")
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Validate and repair parks",
    description:
      "Validates all parks against Queue-Times and Wartezeiten.app APIs. " +
      "Detects mismatched IDs, missing IDs, and duplicates. " +
      "With autoFix=true, adds the missing IDs; mismatches and duplicates are only reported.",
  })
  @ApiBody({
    schema: {
      type: "object",
      properties: {
        autoFix: {
          type: "boolean",
          description: "Add the missing IDs the validation found",
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
  ): Promise<ParkValidationResponse> {
    const report = await this.parkValidatorService.validateAll();
    const repair =
      body.autoFix === true
        ? await this.parkRepairService.addMissingIds(report)
        : null;
    return toParkValidationResponse(report, repair);
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
  async listDuplicateAttractions(): Promise<
    DuplicateListing<DuplicatePairReport>
  > {
    return listBySafety(await this.attractionMergeService.findDuplicatePairs());
  }

  /** The park duplicates and which row a merge would keep; see `ParkDuplicateMergeService.list`. */
  @Get("duplicate-parks")
  @ApiOperation({
    summary: "List parks that look like the same place, with the likely winner",
  })
  @ApiResponse({ status: 200, description: "Duplicate park pairs" })
  async listDuplicateParks(): Promise<DuplicateListing<DuplicatePairVerdict>> {
    return this.parkDuplicateMerge.list();
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
        adoptLoserExternalId: {
          type: "boolean",
          description:
            "Single pair only: the kept row takes the removed row's upstream id. " +
            "For a ride the feed re-issued under a new id (PAR-686).",
          default: false,
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
      adoptLoserExternalId?: boolean;
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
        { adoptLoserExternalId: body.adoptLoserExternalId === true },
      );
    }

    return this.attractionMergeService.mergeDuplicates({
      dryRun: body.dryRun !== false,
      limit: body.limit,
    });
  }

  /**
   * Merge duplicate parks: every safe pair the detector finds (`autoDetect`,
   * a dry run unless `dryRun: false`), or one pair by id (a real merge unless
   * `dryRun: true`). The rules and why the two defaults differ are on
   * `ParkDuplicateMergeService`.
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
    @Body() body: ParkMergeRequest = {},
  ): Promise<ParkMergeResponse> {
    return this.parkDuplicateMerge.merge(body);
  }
}
