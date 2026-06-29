import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import axios from "axios";
import { getPcnServiceUrl } from "../../config/ml-services.config";

const pcnServiceUrl = getPcnServiceUrl;

/**
 * PCN (Park-Crowd Nowcaster) intraday SHADOW orchestration.
 *
 * Thin HTTP triggers for the Python pcn-service (which owns all the ML + DB work);
 * mirrors NfForecastProcessor. PCN runs in the shadow — it only writes pcn_forecasts /
 * pcn_intraday_comparisons; CatBoost stays the served champion until the shadow shows a
 * busy/headliner win (design doc §12).
 *
 * - train-pcn: nightly per-park GP-STGNN training (GPU). Scheduled at 08:30 UTC, after
 *   TFT (starts 03:00, ~2.5h) and CatBoost (starts 06:00, ~45min) have both finished, so
 *   the GPU training spikes never overlap on the shared host.
 *   Overlap-guarded + polled to completion.
 * - forecast-pcn: every 15 min — re-infer with the current state → durable pcn_forecasts
 *   (the going-forward shadow snapshot). Fire-and-trigger (the service runs it async).
 * - score-pcn: hourly — score matured forecasts vs actuals + CatBoost → the segmented
 *   pcn_intraday_comparisons board.
 */
@Processor("pcn-shadow")
export class PcnShadowProcessor {
  private readonly logger = new Logger(PcnShadowProcessor.name);

  @Process("train-pcn")
  async handleTrainPcn(
    _job: Job,
  ): Promise<{ status: string; version?: string }> {
    this.logger.log(`🧠 Triggering PCN training via ${pcnServiceUrl()}/train`);
    try {
      const pre = (
        await axios.get(`${pcnServiceUrl()}/train/status`, { timeout: 15000 })
      ).data;
      if (pre?.is_training) {
        this.logger.warn(
          "PCN training already in progress — skipping this run.",
        );
        return { status: "skipped" };
      }

      let start;
      try {
        start = await axios.post(
          `${pcnServiceUrl()}/train`,
          {},
          { timeout: 30000 },
        );
      } catch (e: any) {
        if (e?.response?.status === 409) {
          this.logger.warn(
            "PCN training already in progress (409) — skipping.",
          );
          return { status: "skipped" };
        }
        throw e;
      }
      const version = start.data?.version;
      this.logger.log(`PCN training started: ${version}`);

      // Poll to completion (per-park GP-STGNN over ~150 parks; generous bound).
      const pollSeconds = 30;
      const maxAttempts = (90 * 60) / pollSeconds; // up to 90 min
      let attempts = 0;
      while (attempts < maxAttempts) {
        await new Promise((r) => setTimeout(r, pollSeconds * 1000));
        attempts++;
        const st = (
          await axios.get(`${pcnServiceUrl()}/train/status`, { timeout: 15000 })
        ).data;
        if (st.status === "completed") {
          this.logger.log(`✅ PCN training completed: ${st.version}`);
          return { status: "ok", version };
        }
        if (st.status === "failed") {
          throw new Error(`PCN training failed: ${st.error}`);
        }
        if (attempts % 4 === 0) {
          this.logger.log(`PCN training… (${attempts}/${maxAttempts})`);
        }
      }
      this.logger.warn(
        "PCN training poll timed out — leaving it to finish out-of-band.",
      );
      return { status: "timeout", version };
    } catch (e: any) {
      this.logger.error(`PCN train failed: ${e?.message ?? e}`);
      throw e;
    }
  }

  @Process("forecast-pcn")
  async handleForecastPcn(_job: Job): Promise<{ status: string }> {
    try {
      const res = await axios.post(
        `${pcnServiceUrl()}/forecast`,
        {},
        { timeout: 30000 },
      );
      this.logger.log(`PCN forecast triggered: ${res.data?.status ?? "ok"}`);
      return { status: "ok" };
    } catch (e: any) {
      if (e?.response?.status === 409) {
        this.logger.warn("PCN forecast already running (409) — skipping.");
        return { status: "skipped" };
      }
      this.logger.error(`PCN forecast trigger failed: ${e?.message ?? e}`);
      throw e;
    }
  }

  @Process("score-pcn")
  async handleScorePcn(_job: Job): Promise<{ status: string }> {
    try {
      const res = await axios.post(
        `${pcnServiceUrl()}/score`,
        {},
        { timeout: 30000 },
      );
      this.logger.log(`PCN scoring triggered: ${res.data?.status ?? "ok"}`);
      return { status: "ok" };
    } catch (e: any) {
      if (e?.response?.status === 409) {
        this.logger.warn("PCN scoring already running (409) — skipping.");
        return { status: "skipped" };
      }
      this.logger.error(`PCN scoring trigger failed: ${e?.message ?? e}`);
      throw e;
    }
  }
}
