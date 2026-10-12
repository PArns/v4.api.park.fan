import { Process, Processor } from "@nestjs/bull";
import { getNfServiceUrl } from "../../config/ml-services.config";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import axios from "axios";

const nfServiceUrl = getNfServiceUrl;
/** How long this job waits for a run; also the age past which an in-flight run is hung. */
const TRAIN_DEADLINE_MINUTES = 90;

/** nf-service `GET /train/status` (the runner's status file). */
interface NfTrainStatus {
  is_training?: boolean;
  status?: string;
  version?: string | null;
  error?: string | null;
  started_at?: string;
  overdue?: boolean;
  forecast_date?: string;
  info?: {
    rows?: number;
    persisted?: number;
    chunks?: { total?: number; ok?: number; skipped?: number };
  };
}

/**
 * NeuralForecast (TFT) training.
 *
 * - train-nf: runs at 03:00 UTC, well BEFORE the CatBoost 06:00 cron, so the two
 *   training spikes never overlap on the shared host. Triggers nf /train and polls
 *   until THIS run completes; the runner persists tft_forecasts itself. A timeout or
 *   any other end state fails the job (attempts: 1) and persists nothing.
 *
 * The TFT-vs-CatBoost forward scoreboard (`score-comparison`, writing
 * `catboost_daily_forecasts` + `model_comparisons`) lived here until PAR-870
 * (2026-10-12): nobody read it any more once the admin page moved to the model
 * comparison (PAR-862), so the job and both tables were retired.
 */
@Processor("nf-training")
export class NfForecastProcessor {
  private readonly logger = new Logger(NfForecastProcessor.name);

  @Process("train-nf")
  async handleTrainNf(
    _job: Job,
  ): Promise<{ status: string; version?: string }> {
    this.logger.log(`🧠 Triggering TFT training via ${nfServiceUrl()}/train`);
    try {
      // Overlap guard: a TFT train can run up to ~90 min. If one is still in
      // flight (long run, manual trigger, or a re-fire), skip rather than stack a
      // second training on the shared host. nf-service also rejects with 409.
      const pre: NfTrainStatus = (
        await axios.get(`${nfServiceUrl()}/train/status`, { timeout: 15000 })
      ).data;
      if (pre?.is_training) {
        // A run older than our own deadline is not "in progress", it is hung.
        // Skipping quietly would report success night after night while the
        // reader's three-day window runs out and TFT drops off the calendar
        // (PAR-814: on 2026-10-09 a runner sat at is_training from 03:00 until
        // a container restart at 07:31).
        const startedMs = pre.started_at ? Date.parse(pre.started_at) : NaN;
        const ageMin = (Date.now() - startedMs) / 60_000;
        if (pre.overdue || !(ageMin <= TRAIN_DEADLINE_MINUTES)) {
          throw new Error(
            `TFT run ${pre.version} has been "training" since ${pre.started_at ?? "an unknown time"} — ` +
              `past the ${TRAIN_DEADLINE_MINUTES}-min deadline, treating it as hung`,
          );
        }
        this.logger.warn(
          "TFT training already in progress — skipping this run.",
        );
        return { status: "skipped" };
      }

      let start;
      try {
        start = await axios.post(
          `${nfServiceUrl()}/train`,
          {},
          { timeout: 30000 },
        );
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

      // Poll to completion (generous bound: a run takes ~11 min on the GPU).
      const pollSeconds = 30;
      const maxAttempts = (TRAIN_DEADLINE_MINUTES * 60) / pollSeconds;
      let attempts = 0;
      let done: NfTrainStatus | null = null;
      while (attempts < maxAttempts) {
        await new Promise((r) => setTimeout(r, pollSeconds * 1000));
        attempts++;
        const st: NfTrainStatus = (
          await axios.get(`${nfServiceUrl()}/train/status`, { timeout: 15000 })
        ).data;
        if (st.status === "failed") {
          throw new Error(`TFT training failed: ${st.error}`);
        }
        if (st.status === "completed" || !st.is_training) {
          // Only THIS run's completion counts. A status that stopped training
          // under another version, or settled on anything but "completed"
          // (nf-service restarted mid-run -> "idle", "reset on startup"), means
          // this run produced nothing.
          if (st.status !== "completed" || st.version !== version) {
            throw new Error(
              `TFT run ${version} ended without completing: status=${st.status}, ` +
                `version=${st.version}, error=${st.error ?? "none"}`,
            );
          }
          done = st;
          break;
        }
        if (attempts % 4 === 0) {
          this.logger.log(`TFT training… (${attempts}/${maxAttempts})`);
        }
      }

      // The runner persists tft_forecasts itself, under the forecast_date of
      // the run that produced them. Nothing is re-persisted from here: on a
      // timeout this job used to fall through to nf /forecast, which wrote the
      // PREVIOUS night's cached parquet under today's forecast_date (PAR-814,
      // 2026-10-09). A missing day is safe: the readers accept a forecast up
      // to three days old and fall back to the previous forecast_date.
      if (!done) {
        throw new Error(
          `TFT run ${version} did not complete within ${(maxAttempts * pollSeconds) / 60} min — ` +
            "no forecast persisted for today",
        );
      }

      if (done.info?.persisted === 0) {
        this.logger.warn(
          `TFT run ${version} completed but persisted 0 rows — no forecast for ${done.forecast_date ?? "today"}`,
        );
      }
      const chunks = done.info?.chunks;
      if (chunks?.skipped) {
        this.logger.warn(
          `TFT run ${version}: ${chunks.skipped} of ${chunks.total} park chunk(s) failed and were skipped — ` +
            "their parks keep the previous forecast_date",
        );
      }
      this.logger.log(
        `✅ TFT training completed: ${version} — ${done.info?.rows ?? "?"} rows, ` +
          `${done.info?.persisted ?? "?"} persisted for ${done.forecast_date ?? "?"}`,
      );
      return { status: "ok", version };
    } catch (e: any) {
      this.logger.error(`TFT train/forecast failed: ${e?.message ?? e}`);
      throw e;
    }
  }
}
