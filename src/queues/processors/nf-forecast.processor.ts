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
  async handleTrainNf(_job: Job): Promise<{ status: string; version?: string }> {
    this.logger.log(`🧠 Triggering TFT training via ${NF_URL}/train`);
    try {
      // Overlap guard: a TFT train can run up to ~90 min. If one is still in
      // flight (long run, manual trigger, or a re-fire), skip rather than stack a
      // second training on the shared host. nf-service also rejects with 409.
      const pre = (await axios.get(`${NF_URL}/train/status`, { timeout: 15000 }))
        .data;
      if (pre?.is_training) {
        this.logger.warn("TFT training already in progress — skipping this run.");
        return { status: "skipped" };
      }

      let start;
      try {
        start = await axios.post(`${NF_URL}/train`, {}, { timeout: 30000 });
      } catch (e: any) {
        if (e?.response?.status === 409) {
          this.logger.warn("TFT training already in progress (409) — skipping.");
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
        const st = (await axios.get(`${NF_URL}/train/status`, { timeout: 15000 }))
          .data;
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
      const fc = await axios.post(`${NF_URL}/forecast`, {}, { timeout: 300000 });
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

    // CatBoost: its daily prediction (14:00 local) as the peak proxy. Pick the
    // freshest forecast made strictly before the target date (genuine forward).
    const catRows = await this.comparisonRepo.query(
      `
      WITH ${actualsCte},
      cat AS (
        SELECT DISTINCT ON (wp."attractionId", DATE(wp."predictedTime" AT TIME ZONE p.timezone))
          wp."attractionId" aid,
          DATE(wp."predictedTime" AT TIME ZONE p.timezone) d,
          wp."predictedWaitTime"::float pred,
          (DATE(wp."predictedTime" AT TIME ZONE p.timezone)
           - DATE(wp."createdAt" AT TIME ZONE p.timezone)) lead
        FROM wait_time_predictions wp
        JOIN attractions a ON a.id = wp."attractionId"
        JOIN parks p ON p.id = a."parkId"
        WHERE wp."predictionType" = 'daily'
          AND wp."predictedTime" >= NOW() - ($1 || ' days')::interval
          AND DATE(wp."createdAt" AT TIME ZONE p.timezone)
              < DATE(wp."predictedTime" AT TIME ZONE p.timezone)
        ORDER BY wp."attractionId",
                 DATE(wp."predictedTime" AT TIME ZONE p.timezone),
                 wp."createdAt" DESC
      )
      SELECT c.d::text "targetDate", COUNT(*)::int n,
             AVG(ABS(c.pred - act.p90)) mae,
             AVG(c.pred - act.p90) bias,
             AVG(act.p90) "meanActual", AVG(c.pred) "meanPred",
             ROUND(AVG(c.lead))::int "avgLeadDays"
      FROM cat c JOIN actuals act ON act.aid = c.aid AND act.d = c.d
      WHERE c.d < CURRENT_DATE
      GROUP BY c.d`,
      [SCORE_LOOKBACK_DAYS],
    );

    // TFT: persisted forward daily-peak forecast; freshest made before target.
    const tftRows = await this.comparisonRepo.query(
      `
      WITH ${actualsCte},
      tft AS (
        SELECT DISTINCT ON (f.attraction_id, f.target_date)
          f.attraction_id aid, f.target_date d, f.predicted_peak pred,
          (f.target_date - f.forecast_date) lead
        FROM tft_forecasts f
        WHERE f.target_date >= (CURRENT_DATE - $1::int)
          AND f.forecast_date < f.target_date
        ORDER BY f.attraction_id, f.target_date, f.forecast_date DESC
      )
      SELECT t.d::text "targetDate", COUNT(*)::int n,
             AVG(ABS(t.pred - act.p90)) mae,
             AVG(t.pred - act.p90) bias,
             AVG(act.p90) "meanActual", AVG(t.pred) "meanPred",
             ROUND(AVG(t.lead))::int "avgLeadDays"
      FROM tft t JOIN actuals act ON act.aid = t.aid AND act.d = t.d
      WHERE t.d < CURRENT_DATE
      GROUP BY t.d`,
      [SCORE_LOOKBACK_DAYS],
    );

    const toEntity = (r: any, model: "catboost" | "tft"): ModelComparison =>
      this.comparisonRepo.create({
        targetDate: r.targetDate,
        model,
        n: r.n,
        mae: Number(r.mae),
        bias: Number(r.bias),
        meanActual: Number(r.meanActual),
        meanPred: Number(r.meanPred),
        avgLeadDays: r.avgLeadDays,
      });

    const rows = [
      ...catRows.map((r: any) => toEntity(r, "catboost")),
      ...tftRows.map((r: any) => toEntity(r, "tft")),
    ];
    if (rows.length) await this.comparisonRepo.save(rows);

    this.logger.log(
      `✅ Scored ${catRows.length} CatBoost + ${tftRows.length} TFT day(s) into model_comparisons`,
    );
    return { scored: rows.length };
  }
}
