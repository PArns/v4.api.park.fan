import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, IsNull, LessThan } from "typeorm";
import { PredictionLeadSnapshot } from "../entities/prediction-lead-snapshot.entity";
import { PredictionDto } from "../dto/prediction-response.dto";
import { AnalyticsService } from "../../analytics/analytics.service";
import { Park } from "../../parks/entities/park.entity";
import { formatInParkTimezone } from "../../common/utils/date.util";

/** Rows scored per run. The backlog drains over a few nights rather than
 *  holding one transaction open across the whole catalogue. */
const SCORING_BATCH = 5000;

/**
 * The lead-time error curve, built forward.
 *
 * See {@link PredictionLeadSnapshot} for why this cannot be answered from the data
 * already in the database. In short: daily predictions are never scored, and the
 * nightly de-duplication overwrites them until only the last one survives, so the
 * relationship between "how far ahead did we say this" and "how wrong were we" has
 * to be recorded as it happens.
 *
 * Two jobs, both cheap:
 *
 * - {@link snapshotPark} runs right after the nightly daily-prediction run stores
 *   its rows, and copies the ones landing on a bucket distance.
 * - {@link scoreDueSnapshots} runs later and fills in what the day actually did.
 */
@Injectable()
export class PredictionLeadSnapshotService {
  private readonly logger = new Logger(PredictionLeadSnapshotService.name);

  /**
   * The lead distances sampled, in days.
   *
   * Chosen to be dense where the curve is expected to move and sparse where it
   * probably flattens: the interesting question is whether a week out is
   * materially worse than tomorrow, not whether day 58 differs from day 59. Six
   * buckets also keep the table at roughly 1,000 headliners × 6 rows a night.
   *
   * 60 is the last one because that is where the stored daily horizon ends
   * (`MLService.deduplicatePredictions`); a bucket beyond it would never be
   * written and would read as a gap in the curve rather than as out of range.
   */
  static readonly LEAD_BUCKETS = [1, 3, 7, 14, 30, 60] as const;

  constructor(
    @InjectRepository(PredictionLeadSnapshot)
    private readonly snapshotRepository: Repository<PredictionLeadSnapshot>,
    private readonly analyticsService: AnalyticsService,
  ) {}

  /**
   * Record this run's predictions for the bucket distances.
   *
   * Called once per park per nightly run, with the predictions that were just
   * stored. `now` is passed in rather than read here so a whole batch shares one
   * reference point — a run that straddles midnight would otherwise put two
   * different lead distances on the same night's work.
   */
  async snapshotPark(
    park: Park,
    predictions: PredictionDto[],
    now: Date,
  ): Promise<number> {
    const headliners = await this.analyticsService
      .getHeadlinerAttractions(park.id)
      .catch(() => []);
    if (headliners.length === 0) return 0;

    const headlinerIds = new Set(headliners.map((h) => h.attractionId));

    // Bucket distance is measured in the park's own calendar days, because
    // `targetDate` is a park-local day and the predictions carry instants. Doing
    // this in UTC puts a park at UTC+10 one day off for half the year.
    const todayLocal = formatInParkTimezone(now, park.timezone);
    const wanted = new Map<string, number>();
    for (const lead of PredictionLeadSnapshotService.LEAD_BUCKETS) {
      wanted.set(this.addDays(todayLocal, lead), lead);
    }

    const rows: PredictionLeadSnapshot[] = [];
    for (const p of predictions) {
      if (p.predictionType !== "daily") continue;
      if (!headlinerIds.has(p.attractionId)) continue;

      const targetDate = formatInParkTimezone(
        new Date(p.predictedTime),
        park.timezone,
      );
      const lead = wanted.get(targetDate);
      if (lead === undefined) continue;

      const row = new PredictionLeadSnapshot();
      row.attractionId = p.attractionId;
      row.targetDate = targetDate;
      row.leadDays = lead;
      row.predictedWaitTime = p.predictedWaitTime;
      row.uncertaintyMinutes = p.uncertaintyMinutes ?? null;
      row.actualWaitTime = null;
      row.absoluteError = null;
      row.scoredAt = null;
      row.modelVersion = p.modelVersion;
      rows.push(row);
    }

    if (rows.length === 0) return 0;

    // orIgnore, not upsert: a re-run on the same night must not overwrite the row
    // that already recorded what we said at that distance. The first answer at a
    // given lead is the one the curve is about.
    await this.snapshotRepository
      .createQueryBuilder()
      .insert()
      .into(PredictionLeadSnapshot)
      .values(rows)
      .orIgnore()
      .execute();

    return rows.length;
  }

  /**
   * Fill in what actually happened, for target dates that have passed.
   *
   * `actualWaitTime` is the day's P90 for the ride — the same statistic
   * {@link AnalyticsService.getHeadlinerDailyPeaks} serves the calendar's past
   * days with, and the one the prediction is comparable to: predict.py collapses
   * the peak-window hours to a per-day MAX, so scoring against a daily average
   * would measure the change of statistic rather than the model's error.
   *
   * A row whose day produced no usable readings is still marked `scoredAt`, with
   * `actualWaitTime` left NULL. Without that the job would re-examine every closed
   * day, every out-of-season ride and every feed gap on every run, forever.
   *
   * The due rows are read ONCE and then grouped, rather than queried per park:
   * `targetDate` is a park-local day and the rows carry an attraction rather than
   * a park, so a per-park query would have to scan the same unscored set 213 times
   * over. The cutoff is therefore global and deliberately conservative — a day is
   * only offered for scoring once it is over in every timezone on earth, which
   * costs one day of latency and removes the whole class of "scored a day that had
   * not finished yet somewhere".
   */
  async scoreDueSnapshots(
    parks: Park[],
    now: Date,
  ): Promise<{ scored: number; withActual: number }> {
    // UTC-12 is the last timezone to finish a calendar day. Anything strictly
    // before yesterday there is over everywhere.
    const safeCutoff = this.addDays(
      new Date(now.getTime() - 12 * 3_600_000).toISOString().slice(0, 10),
      0,
    );

    const due = await this.snapshotRepository.find({
      where: { scoredAt: IsNull(), targetDate: LessThan(safeCutoff) },
      take: SCORING_BATCH,
    });
    if (due.length === 0) return { scored: 0, withActual: 0 };

    // attraction → park, built from the headliner sets the snapshots came from.
    // One Redis read per park, and they are the same reads the calendar makes.
    const parkOf = new Map<string, Park>();
    for (const park of parks) {
      const headliners = await this.analyticsService
        .getHeadlinerAttractions(park.id)
        .catch(() => []);
      for (const h of headliners) parkOf.set(h.attractionId, park);
    }

    const byPark = new Map<string, PredictionLeadSnapshot[]>();
    for (const row of due) {
      const park = parkOf.get(row.attractionId);
      // A ride that has since stopped being a headliner, or whose park is gone.
      // Mark it seen so it does not come back every night; there is nothing to
      // score it against through this path.
      if (!park) {
        row.scoredAt = now;
        continue;
      }
      const list = byPark.get(park.id) ?? [];
      list.push(row);
      byPark.set(park.id, list);
    }

    let scored = 0;
    let withActual = 0;

    for (const [parkId, rows] of byPark) {
      const park = parkOf.get(rows[0].attractionId)!;
      const dates = [...new Set(rows.map((r) => r.targetDate))].sort();
      const peaks = await this.analyticsService
        .getHeadlinerDailyPeaks(
          [...new Set(rows.map((r) => r.attractionId))],
          dates[0],
          dates[dates.length - 1],
          park.timezone,
        )
        .catch((err: Error) => {
          this.logger.warn(
            `Lead-snapshot scoring: peaks unavailable for park ${parkId}: ${err.message}`,
          );
          return null;
        });
      // Leave these unscored — a failed query is not evidence about the day, and
      // marking them would lose them permanently.
      if (peaks === null) continue;

      for (const row of rows) {
        const hit = (peaks.get(row.targetDate) ?? []).find(
          (e) => e.attractionId === row.attractionId,
        );
        const actual = hit ? Math.round(hit.peak) : null;

        row.actualWaitTime = actual;
        row.absoluteError =
          actual === null ? null : Math.abs(row.predictedWaitTime - actual);
        row.scoredAt = now;
        scored++;
        if (actual !== null) withActual++;
      }
    }

    // Includes the orphan rows marked above, which are scored in the sense that
    // they will not be looked at again.
    const touched = due.filter((r) => r.scoredAt !== null);
    if (touched.length > 0) {
      await this.snapshotRepository.save(touched, { chunk: 500 });
    }

    return { scored, withActual };
  }

  /** `YYYY-MM-DD` plus n calendar days, without touching a timezone. */
  private addDays(isoDate: string, days: number): string {
    const d = new Date(`${isoDate}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() + days);
    return d.toISOString().slice(0, 10);
  }
}
