import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import axios from "axios";
import { getShapeServiceUrl } from "../../config/ml-services.config";

const shapeServiceUrl = getShapeServiceUrl;

/**
 * Shape (Level×Shape day-curve expander) SHADOW orchestration.
 *
 * Thin HTTP triggers for the Python shape-service (which owns the ML + DB work); mirrors
 * PcnShadowProcessor. Shape runs in the shadow — it only writes shape_forecasts /
 * shape_comparisons; CatBoost stays the served champion. Cheap (CPU, pandas), so the polls
 * are short.
 *
 * - build-shape: nightly — assemble + persist each park's additive+smooth profiles.
 * - forecast-shape: daily — render the next N days' curves → durable shape_forecasts.
 * - score-shape: daily — score matured forecasts vs actuals + CatBoost → the board.
 */
@Processor("shape-shadow")
export class ShapeShadowProcessor {
  private readonly logger = new Logger(ShapeShadowProcessor.name);

  private async pollToCompletion(
    kind: string,
    maxMinutes: number,
  ): Promise<string> {
    const pollSeconds = 20;
    const maxAttempts = (maxMinutes * 60) / pollSeconds;
    for (let i = 0; i < maxAttempts; i++) {
      await new Promise((r) => setTimeout(r, pollSeconds * 1000));
      const st = (
        await axios.get(`${shapeServiceUrl()}/status`, { timeout: 15000 })
      ).data?.[kind];
      if (st?.status === "completed") return "ok";
      if (st?.status === "failed")
        throw new Error(`shape ${kind} failed: ${st?.error}`);
    }
    this.logger.warn(`shape ${kind} poll timed out — finishing out-of-band.`);
    return "timeout";
  }

  private async trigger(
    kind: string,
    endpoint: string,
    maxMinutes: number,
  ): Promise<{ status: string }> {
    try {
      const pre = (
        await axios.get(`${shapeServiceUrl()}/status`, { timeout: 15000 })
      ).data?.[kind];
      if (pre?.running) {
        this.logger.warn(`shape ${kind} already running — skipping.`);
        return { status: "skipped" };
      }
      try {
        await axios.post(
          `${shapeServiceUrl()}/${endpoint}`,
          {},
          { timeout: 30000 },
        );
      } catch (e: any) {
        if (e?.response?.status === 409) {
          this.logger.warn(`shape ${kind} already running (409) — skipping.`);
          return { status: "skipped" };
        }
        throw e;
      }
      const status = await this.pollToCompletion(kind, maxMinutes);
      this.logger.log(`shape ${kind}: ${status}`);
      return { status };
    } catch (e: any) {
      this.logger.error(`shape ${kind} failed: ${e?.message ?? e}`);
      throw e;
    }
  }

  @Process("build-shape")
  async handleBuild(_job: Job) {
    return this.trigger("build", "build", 30);
  }

  @Process("forecast-shape")
  async handleForecast(_job: Job) {
    return this.trigger("forecast", "forecast", 30);
  }

  @Process("score-shape")
  async handleScore(_job: Job) {
    return this.trigger("score", "score", 20);
  }
}
