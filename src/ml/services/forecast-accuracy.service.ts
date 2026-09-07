import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { ForecastAccuracyProfile } from "../entities/forecast-accuracy-profile.entity";

/** The lead buckets, as upper edges in days. `null` means past the last one. */
const LEAD_BUCKETS = [
  { key: "d1", maxDays: 1 },
  { key: "d7", maxDays: 7 },
  { key: "d30", maxDays: 30 },
  { key: "d60", maxDays: 60 },
] as const;

/** Fewer comparisons than this and the figure is not worth publishing. */
const MIN_SAMPLE = 500;

/**
 * How wrong the daily forecast typically is — measured, and served so a planner
 * can say "give or take a quarter of an hour" without inventing the quarter.
 *
 * See {@link ForecastAccuracyProfile} for why the answer needs two axes and why
 * the band is the predicted level rather than the realised one.
 *
 * The measurement is retrospective and possible only because `tft_forecasts`
 * keeps every origin: for a target day it holds what was said 1, 7, 30 and 60
 * days out, so the error curve can be computed from history instead of waiting
 * for one to accumulate. CatBoost has no such record — its daily rows are
 * rewritten until only the last survives — which is why nothing past 60 days can
 * be answered here yet.
 */
@Injectable()
export class ForecastAccuracyService {
  private readonly logger = new Logger(ForecastAccuracyService.name);

  constructor(
    @InjectRepository(ForecastAccuracyProfile)
    private readonly repository: Repository<ForecastAccuracyProfile>,
  ) {}

  /**
   * Recompute the whole profile from the last 45 days of forecasts.
   *
   * 45 days rather than a year: these numbers follow the model, and a window
   * long enough to be stable is also long enough to average two model versions
   * into one figure. One statement, not one per park — a per-park breakdown
   * splits 2.5 M comparisons into a few hundred each and starts measuring noise.
   */
  async rebuild(): Promise<number> {
    const rows: Array<{
      predicted_band: string;
      lead_bucket: string;
      sample_size: string;
      mae: string;
      mean_actual: string;
    }> = await this.repository.manager.query(
      `WITH truth AS (
         SELECT qda."attractionId"::uuid AS aid,
                (qda.hour AT TIME ZONE p.timezone)::date AS day,
                max(qda.p90)::float AS actual
           FROM queue_data_aggregates qda
           JOIN parks p ON p.id::text = qda."parkId"
          WHERE qda.hour >= current_date - 45 AND qda.hour < current_date
            AND qda."sampleCount" >= 2
          GROUP BY 1, 2
         HAVING max(qda.p90) > 0
       ), j AS (
         SELECT CASE WHEN f.predicted_peak >= 60 THEN 'busy'
                     WHEN f.predicted_peak >= 30 THEN 'mid'
                     ELSE 'quiet' END AS predicted_band,
                CASE WHEN (f.target_date - f.forecast_date) <= 1  THEN 'd1'
                     WHEN (f.target_date - f.forecast_date) <= 7  THEN 'd7'
                     WHEN (f.target_date - f.forecast_date) <= 30 THEN 'd30'
                     ELSE 'd60' END AS lead_bucket,
                abs(f.predicted_peak - t.actual) AS err,
                t.actual
           FROM tft_forecasts f
           JOIN truth t ON t.aid = f.attraction_id AND t.day = f.target_date
          WHERE f.target_date >= current_date - 45
            AND f.target_date < current_date
            AND f.target_date > f.forecast_date
            AND (f.target_date - f.forecast_date) <= 60
       )
       SELECT predicted_band, lead_bucket,
              count(*)::text     AS sample_size,
              avg(err)::text     AS mae,
              avg(actual)::text  AS mean_actual
         FROM j
        GROUP BY 1, 2
       HAVING count(*) >= ${MIN_SAMPLE}`,
    );

    const computedAt = new Date();
    const entities = rows.map((r) => ({
      predictedBand: r.predicted_band,
      leadBucket: r.lead_bucket,
      sampleSize: Number(r.sample_size),
      mae: Math.round(Number(r.mae) * 10) / 10,
      meanActual: Math.round(Number(r.mean_actual) * 10) / 10,
      computedAt,
    }));

    // Replaced wholesale rather than upserted: a combination that stops being
    // measurable has to disappear, or a caller keeps being handed a figure from
    // a model that no longer exists.
    await this.repository.manager.transaction(async (tx) => {
      await tx.clear(ForecastAccuracyProfile);
      if (entities.length > 0) {
        await tx.insert(ForecastAccuracyProfile, entities);
      }
    });

    this.logger.log(
      `📐 Forecast accuracy profile rebuilt: ${entities.length} cell(s)`,
    );
    return entities.length;
  }

  /**
   * The whole profile, keyed `band|leadBucket`.
   *
   * Small enough (nine cells) that a caller reads all of it and looks up per
   * ride; there is nothing to page and nothing to filter.
   */
  async getProfile(): Promise<Map<string, ForecastAccuracyProfile>> {
    const rows = await this.repository.find();
    return new Map(rows.map((r) => [`${r.predictedBand}|${r.leadBucket}`, r]));
  }

  /** The bucket key a lead distance falls in, or null past the last bucket. */
  static bucketFor(leadDays: number): string | null {
    if (leadDays < 0) return null;
    const hit = LEAD_BUCKETS.find((b) => leadDays <= b.maxDays);
    return hit ? hit.key : null;
  }

  /** The band a predicted wait falls in. */
  static bandFor(predictedWait: number): string {
    if (predictedWait >= 60) return "busy";
    if (predictedWait >= 30) return "mid";
    return "quiet";
  }
}
