import { Injectable, Logger } from "@nestjs/common";
import axios from "axios";
import { getNfServiceUrl } from "../../config/ml-services.config";

/** Bench variant: `live` = `chronos2` (intraday), `grid` = `chronos2_grid` (planner). */
export type ChronosVariant = "live" | "grid";

/** One ride and service day as nf-service returns it (15-min steps). */
export interface ChronosCurve {
  attractionId: string;
  /** Park-local service date. */
  date: string;
  /** UTC instant of the first step. */
  slotStart: string;
  slotMinutes: number;
  /** Which covariates rested on data for this day. */
  covariates: { holidays: "full" | "short" | "none"; schedule: string };
  q50: (number | null)[];
  q80: (number | null)[];
  q95: (number | null)[];
}

export interface ChronosResponse {
  status: "ok" | "warming" | "no_data";
  revision?: string;
  benchName?: string;
  curves: ChronosCurve[];
}

/**
 * The API's side of the Chronos-2 SHADOW forecast (PAR-852): calls nf-service
 * `POST /chronos/forecast` for the forward archive. Nothing here reaches a user
 * surface — the only caller is `ForecastArchiveService`.
 *
 * It must never cost the archive anything else it captures:
 *
 * - **Kill switch** `CHRONOS_SHADOW_ENABLED` (default on; `false` = no call).
 * - **Timeout** per call, and any error is swallowed into `null`.
 * - **Circuit breaker**: after {@link MAX_FAILURES_PER_RUN} failures in one
 *   hourly run the remaining parks skip the call, so a hung service costs a
 *   run at most three timeouts instead of one per park — the capture's
 *   15-minute lock budget stays intact.
 * - **Logged once per run**: the first failure as a warning, the count in the
 *   run summary (`endRun`), never one line per park.
 */
@Injectable()
export class ChronosShadowService {
  private readonly logger = new Logger(ChronosShadowService.name);

  /** A park's call: data read (cached) plus one GPU pass — seconds when warm. */
  static readonly TIMEOUT_MS = 45_000;
  static readonly MAX_FAILURES_PER_RUN = 3;

  private failures = 0;
  private warming = 0;
  private skipped = 0;
  private loggedThisRun = false;

  /** `CHRONOS_SHADOW_ENABLED` — on unless set to `false` / `0` / `off`. */
  static enabled(): boolean {
    const v = (process.env.CHRONOS_SHADOW_ENABLED ?? "true").toLowerCase();
    return !["false", "0", "off", "no"].includes(v);
  }

  /** Starts a run: resets the breaker and the once-per-run log. */
  beginRun(): void {
    this.failures = 0;
    this.warming = 0;
    this.skipped = 0;
    this.loggedThisRun = false;
  }

  /** Ends a run; logs one summary line when anything went wrong. */
  endRun(): { failures: number; warming: number; skipped: number } {
    const out = {
      failures: this.failures,
      warming: this.warming,
      skipped: this.skipped,
    };
    if (out.failures > 0) {
      this.logger.warn(
        `Chronos shadow: ${out.failures} call(s) failed, ${out.skipped} skipped this run`,
      );
    }
    return out;
  }

  /**
   * The shadow curves for one park at one origin, or null (switched off,
   * breaker open, service error, still warming up). Never throws.
   */
  async forecast(
    parkId: string,
    originAt: Date,
    variant: ChronosVariant,
    keepMinutes?: number,
  ): Promise<ChronosResponse | null> {
    if (!ChronosShadowService.enabled()) return null;
    if (this.failures >= ChronosShadowService.MAX_FAILURES_PER_RUN) {
      this.skipped++;
      return null;
    }
    try {
      const res = await axios.post<ChronosResponse>(
        `${getNfServiceUrl()}/chronos/forecast`,
        {
          park_id: parkId,
          origin_at: originAt.toISOString(),
          variant,
          keep_minutes: keepMinutes ?? null,
        },
        { timeout: ChronosShadowService.TIMEOUT_MS },
      );
      const body = res.data;
      if (!body || !Array.isArray(body.curves)) return null;
      if (body.status === "warming") this.warming++;
      return body.status === "ok" ? body : null;
    } catch (err) {
      // 503 = switched off in nf-service or a TFT training run holds the GPU:
      // an answer, not a fault, and it costs no time — the breaker ignores it.
      if (axios.isAxiosError(err) && err.response?.status === 503) {
        this.skipped++;
        return null;
      }
      this.failures++;
      if (!this.loggedThisRun) {
        this.loggedThisRun = true;
        this.logger.warn(
          `Chronos shadow unavailable (${variant}, park ${parkId}): ${(err as Error).message} — ` +
            `the other archive captures are unaffected`,
        );
      }
      return null;
    }
  }
}
