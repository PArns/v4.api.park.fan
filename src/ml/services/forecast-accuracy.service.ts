import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { ForecastAccuracyProfile } from "../entities/forecast-accuracy-profile.entity";

/**
 * The lead buckets, as upper edges in days. `null` means past the last one.
 *
 * The same six distances `PredictionLeadSnapshotService.LEAD_BUCKETS` samples
 * forward, and for the same reason: dense where the curve moves, sparse where it
 * flattens. They were four here (`d1/d7/d30/d60`) while the forward archive
 * already used six, so one question had two answers depending on which table was
 * asked — `d3` and `d14` close that.
 *
 * Adding them costs nothing but resolution. Measured over the 45-day window on
 * 2026-09-11, every one of the 18 cells clears {@link MIN_SAMPLE} by an order of
 * magnitude (the smallest, `busy|d1`, holds 6,132 comparisons), so the two new
 * columns are published rather than suppressed.
 */
const LEAD_BUCKETS = [
  { key: "d1", maxDays: 1 },
  { key: "d3", maxDays: 3 },
  { key: "d7", maxDays: 7 },
  { key: "d14", maxDays: 14 },
  { key: "d30", maxDays: 30 },
  { key: "d60", maxDays: 60 },
] as const;

/** Fewer comparisons than this and the figure is not worth publishing. */
const MIN_SAMPLE = 500;

/**
 * How wrong the daily forecast typically is — measured, and served so a planner
 * can say "give or take a quarter of an hour" without inventing the quarter.
 *
 * Two figures per cell, and they answer different questions. {@link
 * ForecastAccuracyProfile.mae} is the typical miss, served as `/plan/day`'s
 * `expectedError`. {@link ForecastAccuracyProfile.uncertaintyP95} is the upper
 * 95th percentile of the same residuals, served as `uncertaintyMinutes` on the
 * days the TFT answers — where there is no trained quantile to read one from.
 * Keeping both is the point: a band that equalled the MAE would put the same
 * number under two names, and the two names promise different things.
 *
 * See {@link ForecastAccuracyProfile} for why the answer needs two axes and why
 * the band is the predicted level rather than the realised one.
 *
 * The measurement is retrospective and possible only because `tft_forecasts`
 * keeps every origin: for a target day it holds what was said at every distance
 * from 1 to 60 days out — all 60 densely populated, 17.8 M rows on 2026-09-11 —
 * so the error curve can be computed from history instead of waiting for one to
 * accumulate. CatBoost has no such record — its daily rows are
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
      uncertainty_p95: string;
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
                     WHEN (f.target_date - f.forecast_date) <= 3  THEN 'd3'
                     WHEN (f.target_date - f.forecast_date) <= 7  THEN 'd7'
                     WHEN (f.target_date - f.forecast_date) <= 14 THEN 'd14'
                     WHEN (f.target_date - f.forecast_date) <= 30 THEN 'd30'
                     ELSE 'd60' END AS lead_bucket,
                abs(f.predicted_peak - t.actual) AS err,
                -- SIGNED, and in this direction: the band is served as "the wait
                -- may be this much LONGER", so the tail that matters is the one
                -- where the day beat the forecast.
                (t.actual - f.predicted_peak) AS resid,
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
              avg(actual)::text  AS mean_actual,
              (percentile_cont(0.95) WITHIN GROUP (ORDER BY resid))::text
                                 AS uncertainty_p95
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
      // NULL below zero, not clamped to zero. The percentile is of a SIGNED
      // residual, so a cell that systematically over-forecasts produces a
      // negative one — and that is not "a band of width zero", it is a
      // different fact about the cell ("it essentially never runs long"). Zero
      // is a measurement here and travels as one, which is exactly why a
      // negative must not be rounded up into it: the band would read as
      // maximum certainty, and `dailyConfidence` would hand its model term a
      // 100, on the cell that supports the claim least. NULL puts it on the
      // "no cell" path instead, where a caller already reads it as not known.
      //
      // Deliberately NOT floored at the MAE either: that compares a signed
      // percentile against a mean of absolute values, and for such a cell it
      // would publish the MAE as an upward band the data does not support.
      //
      // Never observed in production — the smallest measured value is 25.3 on
      // `quiet|d1`, the smallest p95/MAE ratio 2.1x.
      uncertaintyP95:
        Number(r.uncertainty_p95) < 0
          ? null
          : Math.round(Number(r.uncertainty_p95) * 10) / 10,
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
   * Small enough (18 cells: three bands × six lead buckets) that a caller reads
   * all of it and looks up per ride; there is nothing to page and nothing to
   * filter.
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

  /**
   * The measured cell for a ride, or undefined when this distance has none.
   *
   * Looks up `band|bucket` and, if that cell is absent, WIDENS to the next
   * coarser bucket that is present. Two reasons it is a method rather than a
   * string built at the call site:
   *
   * 1. **The stored grid and this code are deployed separately.** `rebuild()`
   *    replaces the table wholesale once a night, so between a deploy that adds a
   *    bucket and the next nightly run the table still holds the previous set. A
   *    plain lookup returns nothing for the new keys, and `/plan/day` would drop
   *    from a measured figure to `unmeasured` for up to a day — at distances that
   *    reported one before the deploy. That is a silent regression, and it is
   *    exactly what adding `d3` and `d14` would have caused.
   * 2. A bucket that stops being measurable disappears from the table by design
   *    (see `rebuild()`), so an absent cell is a normal state and not an error.
   *
   * Widening rather than narrowing is the safe direction: the answer errs towards
   * "at least this wrong" rather than understating — the same reasoning
   * `PredictionLeadSnapshotService` applies from the other side. It never reaches
   * past the last bucket, so past 60 days the answer stays undefined.
   *
   * FOR `uncertaintyP95` THE SAME WIDENING IS ALL BUT MONOTONE, AND THE EXCEPTION
   * IS NAMED HERE RATHER THAN ROUNDED AWAY. Measured 2026-09-13 over 45 days, the
   * band rises with lead in fifteen of the eighteen steps; the one that falls is
   * `busy` from `d1` (46.16) to `d3` (45.85), by 0.31 minutes. So a `busy|d1`
   * lookup that has to widen understates by a third of a minute rather than
   * overstating — inside the rounding the field itself applies, and far inside
   * the 500-comparison sampling noise of the cell. It is recorded because the MAE
   * invariant below is stated as a fact and this one cannot be.
   *
   * THAT INVARIANT RESTS ON TWO THINGS, and both are measured rather than assumed:
   *
   * 1. **MAE rises with lead in every band** — 8.6→12.5 quiet, 12.9→16.5 mid,
   *    21.5→25.5 busy over 1→60 days. A coarser bucket is a longer distance, so
   *    its mean is the larger one. Were the curve non-monotone this would not
   *    follow.
   * 2. **The bucket list only ever gets refined, never re-cut.** `d3` and `d14`
   *    split existing buckets; they do not move `d1/d7/d30/d60`'s edges. So a
   *    stale coarse bucket shares its LOWER edge with the ideal bucket and extends
   *    only further out — its mean is taken over a superset reaching away from the
   *    distance asked about, never towards it.
   *
   * Verified against production on 2026-09-11 for the two cells this change adds,
   * both compared inside ONE 45-day window (comparing across windows is how this
   * invariant first looked violated):
   *
   * ```
   *   band    new d3   widens to old d7      new d14   widens to old d30
   *   busy     21.63          22.47           24.31          24.83
   *   mid      13.22          13.49           14.65          15.26
   *   quiet     8.70           8.88            9.89          10.44
   * ```
   *
   * A future bucket that re-cuts rather than refines the existing edges would
   * break point 2, and this method would then need to compare edges rather than
   * walk the list.
   */
  static lookup(
    profile: Map<string, ForecastAccuracyProfile>,
    predictedWait: number,
    leadDays: number,
  ): ForecastAccuracyProfile | undefined {
    const band = ForecastAccuracyService.bandFor(predictedWait);
    if (leadDays < 0) return undefined;
    for (const bucket of LEAD_BUCKETS) {
      if (leadDays > bucket.maxDays) continue;
      const cell = profile.get(`${band}|${bucket.key}`);
      if (cell) return cell;
    }
    return undefined;
  }
}
