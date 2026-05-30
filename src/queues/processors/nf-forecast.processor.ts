import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Job } from "bull";
import axios from "axios";
import { ModelComparison } from "../../ml/entities/model-comparison.entity";

const NF_URL = process.env.NF_SERVICE_URL || "http://nf-service:8000";
const SCORE_LOOKBACK_DAYS = 14; // re-score the last N matured days each run (idempotent)

/**
 * NeuralForecast (TFT) training + the TFT-vs-CatBoost forward scoreboard.
 *
 * - train-nf: run AFTER the CatBoost 06:00 cron (07:30) so the two PyTorch/CatBoost
 *   training spikes never overlap on the shared host. Triggers nf /train, polls to
 *   completion, then nf /forecast (which persists forward forecasts to tft_forecasts).
 * - score-comparison: score each model's genuine FORWARD daily-peak forecast (made
 *   before the target date) against the realised actual daily P90 peak, per target
 *   date, into model_comparisons. Fair by construction (no holdout leakage).
 */
@Processor("nf-training")
export class NfForecastProcessor {
  private readonly logger = new Logger(NfForecastProcessor.name);

  constructor(
    @InjectRepository(ModelComparison)
    private readonly comparisonRepo: Repository<ModelComparison>,
  ) {}

  @Process("train-nf")
  async handleTrainNf(
    _job: Job,
  ): Promise<{ status: string; version?: string }> {
    this.logger.log(`🧠 Triggering TFT training via ${NF_URL}/train`);
    try {
      // Overlap guard: a TFT train can run up to ~90 min. If one is still in
      // flight (long run, manual trigger, or a re-fire), skip rather than stack a
      // second training on the shared host. nf-service also rejects with 409.
      const pre = (
        await axios.get(`${NF_URL}/train/status`, { timeout: 15000 })
      ).data;
      if (pre?.is_training) {
        this.logger.warn(
          "TFT training already in progress — skipping this run.",
        );
        return { status: "skipped" };
      }

      let start;
      try {
        start = await axios.post(`${NF_URL}/train`, {}, { timeout: 30000 });
      } catch (e: any) {
        if (e?.response?.status === 409) {
          this.logger.warn(
            "TFT training already in progress (409) — skipping.",
          );
          return { status: "skipped" };
        }
        throw e;
      }
      const version = start.data?.version;
      this.logger.log(`TFT training started: ${version}`);

      // Poll to completion (TFT on CPU can take a while; generous bound).
      const pollSeconds = 30;
      const maxAttempts = (90 * 60) / pollSeconds; // up to 90 min
      let attempts = 0;
      while (attempts < maxAttempts) {
        await new Promise((r) => setTimeout(r, pollSeconds * 1000));
        attempts++;
        const st = (
          await axios.get(`${NF_URL}/train/status`, { timeout: 15000 })
        ).data;
        if (st.status === "completed") {
          this.logger.log(`✅ TFT training completed: ${st.version}`);
          break;
        }
        if (st.status === "failed") {
          throw new Error(`TFT training failed: ${st.error}`);
        }
        if (attempts % 4 === 0) {
          this.logger.log(`TFT training… (${attempts}/${maxAttempts})`);
        }
      }

      // Forecast + persist forward records for the scoreboard.
      this.logger.log("Running TFT forecast (persists tft_forecasts)…");
      const fc = await axios.post(
        `${NF_URL}/forecast`,
        {},
        { timeout: 300000 },
      );
      this.logger.log(
        `✅ TFT forecast cached: ${fc.data?.rows} rows, ${fc.data?.persisted} persisted`,
      );
      return { status: "ok", version };
    } catch (e: any) {
      this.logger.error(`TFT train/forecast failed: ${e?.message ?? e}`);
      throw e;
    }
  }

  @Process("score-comparison")
  async handleScoreComparison(_job: Job): Promise<{ scored: number }> {
    this.logger.log("📊 Scoring TFT vs CatBoost forward forecasts…");

    // tft_forecasts is owned by nf-service; ensure it exists so a first run (before
    // any TFT forecast) doesn't error on a missing relation.
    await this.comparisonRepo.query(`
      CREATE TABLE IF NOT EXISTS tft_forecasts (
        attraction_id  uuid NOT NULL,
        target_date    date NOT NULL,
        forecast_date  date NOT NULL,
        predicted_peak double precision NOT NULL,
        model_version  text,
        created_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (attraction_id, target_date, forecast_date)
      )`);

    // Symmetric durable CatBoost snapshot — the fix for a CLEAN comparison.
    // wait_time_predictions gets deduplicated daily (deduplicatePredictions),
    // which DELETES CatBoost's fresh forward records, so only stale ones survive
    // → its scoreboard lead inflates to ~5d while TFT (durable tft_forecasts)
    // scores at lead ~1d, on a different/smaller population. We mirror tft_forecasts:
    // snapshot today's freshest daily-peak predictions into an immutable forward
    // table, so CatBoost ALSO gets genuine lead-1 records and both models are scored
    // on the SAME (attraction, target_date) intersection at a matched lead.
    await this.snapshotCatboostDaily();

    const actualsCte = `
      actuals AS (
        SELECT qd."attractionId" aid,
               DATE(qd.timestamp AT TIME ZONE p.timezone) d,
               PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qd."waitTime") p90
        FROM queue_data qd
        JOIN attractions a ON a.id = qd."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE qd.timestamp >= NOW() - ($1 || ' days')::interval
          AND qd.status = 'OPERATING' AND qd."queueType" = 'STANDBY'
          AND qd."waitTime" >= 5
        GROUP BY 1, 2
        HAVING COUNT(*) >= 3
      )`;

    // Matched scoreboard: both models' freshest genuine-forward forecast per
    // (attraction, target_date), INNER-joined so they score the SAME population,
    // then joined to the realised daily P90. n + meanActual are identical across
    // the two emitted rows by construction; leads align because both snapshots are
    // now durable (freshest-before-target ≈ lead 1 for each).
    const rows = await this.comparisonRepo.query(
      `
      WITH ${actualsCte},
      cat AS (
        SELECT DISTINCT ON (f.attraction_id, f.target_date)
          f.attraction_id aid, f.target_date d, f.predicted_peak pred,
          (f.target_date - f.forecast_date) lead
        FROM catboost_daily_forecasts f
        WHERE f.target_date >= (CURRENT_DATE - $1::int)
          AND f.forecast_date < f.target_date
        ORDER BY f.attraction_id, f.target_date, f.forecast_date DESC
      ),
      tft AS (
        SELECT DISTINCT ON (f.attraction_id, f.target_date)
          f.attraction_id aid, f.target_date d, f.predicted_peak pred,
          (f.target_date - f.forecast_date) lead
        FROM tft_forecasts f
        WHERE f.target_date >= (CURRENT_DATE - $1::int)
          AND f.forecast_date < f.target_date
        ORDER BY f.attraction_id, f.target_date, f.forecast_date DESC
      ),
      scored AS (
        SELECT c.d, act.p90,
               c.pred cat_pred, c.lead cat_lead,
               t.pred tft_pred, t.lead tft_lead,
               (h."attractionId" IS NOT NULL) is_hdlnr
        FROM cat c
        JOIN tft t   ON t.aid = c.aid AND t.d = c.d
        JOIN actuals act ON act.aid = c.aid AND act.d = c.d
        LEFT JOIN headliner_attractions h ON h."attractionId" = c.aid
        WHERE c.d < CURRENT_DATE
      )
      -- Segment each matched day into all / busy (P90>=40) / headliner via a lateral
      -- segment list + per-segment predicate, so overall MAE never hides TFT's tail edge.
      SELECT seg.segment, s.d::text "targetDate", COUNT(*)::int n,
             AVG(s.p90) "meanActual",
             AVG(ABS(s.cat_pred - s.p90)) cat_mae, AVG(s.cat_pred - s.p90) cat_bias,
             AVG(s.cat_pred) cat_mean, ROUND(AVG(s.cat_lead))::int cat_lead,
             AVG(ABS(s.tft_pred - s.p90)) tft_mae, AVG(s.tft_pred - s.p90) tft_bias,
             AVG(s.tft_pred) tft_mean, ROUND(AVG(s.tft_lead))::int tft_lead
      FROM scored s
      CROSS JOIN LATERAL (VALUES ('all'), ('busy'), ('headliner')) seg(segment)
      WHERE seg.segment = 'all'
         OR (seg.segment = 'busy' AND s.p90 >= 40)
         OR (seg.segment = 'headliner' AND s.is_hdlnr)
      GROUP BY seg.segment, s.d`,
      [SCORE_LOOKBACK_DAYS],
    );

    const entities: ModelComparison[] = [];
    for (const r of rows) {
      entities.push(
        this.comparisonRepo.create({
          targetDate: r.targetDate,
          model: "catboost",
          segment: r.segment,
          n: r.n,
          mae: Number(r.cat_mae),
          bias: Number(r.cat_bias),
          meanActual: Number(r.meanActual),
          meanPred: Number(r.cat_mean),
          avgLeadDays: r.cat_lead,
        }),
        this.comparisonRepo.create({
          targetDate: r.targetDate,
          model: "tft",
          segment: r.segment,
          n: r.n,
          mae: Number(r.tft_mae),
          bias: Number(r.tft_bias),
          meanActual: Number(r.meanActual),
          meanPred: Number(r.tft_mean),
          avgLeadDays: r.tft_lead,
        }),
      );
    }
    if (entities.length) await this.comparisonRepo.save(entities);

    this.logger.log(
      `✅ Scored ${rows.length} matched day(s) (TFT vs CatBoost, same population) into model_comparisons`,
    );
    return { scored: entities.length };
  }

  /**
   * Snapshot today's freshest CatBoost daily-peak predictions into the durable
   * catboost_daily_forecasts table (mirror of tft_forecasts). Immutable per
   * forecast_date: re-running on the same day overwrites only today's snapshot,
   * past forecast_dates are preserved as genuine forward records for scoring.
   *
   * Only predictions CREATED in the last 26h are captured (so forecast_date=today
   * is honest — if generate-daily didn't run today, we snapshot nothing rather than
   * mislabel a stale forecast as lead-1). Horizon capped at +45d to mirror TFT's
   * 30-day surface and keep the table bounded (CatBoost daily itself spans 365d).
   * predictedWaitTime is already the per-day MAX over DAILY_PEAK_HOURS (≈ P90 peak).
   */
  private async snapshotCatboostDaily(): Promise<void> {
    await this.comparisonRepo.query(`
      CREATE TABLE IF NOT EXISTS catboost_daily_forecasts (
        attraction_id  uuid NOT NULL,
        target_date    date NOT NULL,
        forecast_date  date NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')::date,
        predicted_peak double precision NOT NULL,
        model_version  text,
        created_at     timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (attraction_id, target_date, forecast_date)
      )`);

    const res = await this.comparisonRepo.query(`
      INSERT INTO catboost_daily_forecasts
        (attraction_id, target_date, forecast_date, predicted_peak, model_version)
      SELECT DISTINCT ON (wp."attractionId", DATE(wp."predictedTime" AT TIME ZONE p.timezone))
        wp."attractionId",
        DATE(wp."predictedTime" AT TIME ZONE p.timezone),
        (now() AT TIME ZONE 'UTC')::date,
        wp."predictedWaitTime"::float,
        'catboost'
      FROM wait_time_predictions wp
      JOIN attractions a ON a.id = wp."attractionId"
      JOIN parks p ON p.id = a."parkId"
      WHERE wp."predictionType" = 'daily'
        AND wp."createdAt" >= NOW() - INTERVAL '26 hours'
        AND DATE(wp."predictedTime" AT TIME ZONE p.timezone)
            BETWEEN CURRENT_DATE + 1 AND CURRENT_DATE + 45
      ORDER BY wp."attractionId",
               DATE(wp."predictedTime" AT TIME ZONE p.timezone),
               wp."createdAt" DESC
      ON CONFLICT (attraction_id, target_date, forecast_date)
      DO UPDATE SET predicted_peak = EXCLUDED.predicted_peak,
                    model_version  = EXCLUDED.model_version,
                    created_at     = now()`);

    // node-postgres returns rowCount on the driver result; TypeORM .query passes it
    // through as an array, so log defensively.
    const n = Array.isArray(res) ? res.length : (res?.rowCount ?? 0);
    this.logger.log(
      `📸 CatBoost daily snapshot: upserted ${n} forward record(s)`,
    );

    // Bootstrap backfill (idempotent): seed the durable table from the CatBoost
    // forward records that are STILL alive in wait_time_predictions (those the
    // daily dedup hasn't deleted yet), keyed by their real createdAt date as
    // forecast_date. Without this the matched scoreboard would be empty for the
    // first ~1-2 days until fresh lead-1 snapshots mature. DO NOTHING on conflict
    // so a genuine same-day snapshot above is never overwritten by a stale record.
    await this.comparisonRepo.query(
      `
      INSERT INTO catboost_daily_forecasts
        (attraction_id, target_date, forecast_date, predicted_peak, model_version)
      SELECT DISTINCT ON (wp."attractionId",
                          DATE(wp."predictedTime" AT TIME ZONE p.timezone),
                          DATE(wp."createdAt" AT TIME ZONE 'UTC'))
        wp."attractionId",
        DATE(wp."predictedTime" AT TIME ZONE p.timezone),
        DATE(wp."createdAt" AT TIME ZONE 'UTC'),
        wp."predictedWaitTime"::float,
        'catboost-backfill'
      FROM wait_time_predictions wp
      JOIN attractions a ON a.id = wp."attractionId"
      JOIN parks p ON p.id = a."parkId"
      WHERE wp."predictionType" = 'daily'
        AND wp."predictedTime" >= NOW() - ($1 || ' days')::interval
        -- Upper bound: the backfill only seeds the matured comparison window; the
        -- fresh snapshot above owns the future. Without this it captured the full
        -- 365-day daily horizon × every forecast_date (millions of rows of bloat).
        AND DATE(wp."predictedTime" AT TIME ZONE p.timezone) <= CURRENT_DATE + 1
        AND DATE(wp."createdAt" AT TIME ZONE p.timezone)
            < DATE(wp."predictedTime" AT TIME ZONE p.timezone)
      ON CONFLICT (attraction_id, target_date, forecast_date) DO NOTHING`,
      [SCORE_LOOKBACK_DAYS],
    );
  }
}
