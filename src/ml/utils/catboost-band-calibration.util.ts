/**
 * Split-conformal calibration of CatBoost's HOURLY band (PAR-1060).
 *
 * `uncertaintyMinutes` is `q0.95 − q0.5`: a ONE-sided upper half-width, read as
 * "the wait stays at or below the served wait plus this, 95 % of the time" —
 * the reading the forward archive's D8 scores (`truth ≤ pred + band`) and the
 * one the TFT band is calibrated to. Measured, it holds 53–57 % (PAR-167). The
 * fix is one multiplier `k` per (lead bucket, predicted-wait segment), fitted on
 * the last 28 days of scored slots so that P(actual − served ≤ k·band) = 0.95.
 *
 * The SQL side ships a histogram of the conformity score
 * `s = (actual − predicted) / band`, binned upward to {@link SCORE_BIN}; the
 * fit is here so it can be tested on a fixture. Semantics:
 * docs/ml/quantile-serving-and-calibration.md § "CatBoost hourly band".
 */
import { ForecastAccuracyService } from "../services/forecast-accuracy.service";

/** The coverage the band is named for. */
export const BAND_TARGET_COVERAGE = 0.95;
/** A cell below this many scored slots borrows the pooled factor. */
export const BAND_MIN_SAMPLE = 500;
/** Histogram bin width of the conformity score; k is a multiple of it, rounded up. */
export const SCORE_BIN = 0.05;
/** Scores beyond ±(this × SCORE_BIN) are clamped into the outermost bin. */
export const SCORE_BIN_CLAMP = 400;
export const BAND_CALIBRATION_WINDOW_DAYS = 28;
export const BAND_CALIBRATION_KEY = "ml:band-calibration:catboost-hourly:v1";
/** Rebuilt daily; three days survive two failed runs before the band reverts to raw. */
export const BAND_CALIBRATION_TTL_SECONDS = 3 * 24 * 3600;

export type BandLeadBucket = "h0-2" | "h2-6" | "h6-24" | "h24+";

export function bandLeadBucket(leadMinutes: number): BandLeadBucket {
  if (leadMinutes <= 120) return "h0-2";
  if (leadMinutes <= 360) return "h2-6";
  if (leadMinutes <= 1440) return "h6-24";
  return "h24+";
}

/** One histogram row: `bin` is the score's upper edge in units of SCORE_BIN. */
export interface ScoreHistogramRow {
  lead: string;
  segment: string;
  bin: number;
  n: number;
}

export interface BandCalibrationCell {
  /** Scored slots in the cell itself. */
  n: number;
  /** The factor served for this cell (own or pooled); null = band served raw. */
  k: number | null;
  /** `cell` = fitted on its own slots, `pooled` = too few, borrowed from its
   *  lead bucket's pool (never from another lead bucket). */
  source: "cell" | "pooled";
  /** In-sample share with actual − predicted ≤ band (the raw band). */
  coverageRaw: number;
  /** In-sample share with actual − predicted ≤ k·band; null without k. */
  coverageCalibrated: number | null;
}

export interface BandCalibrationTable {
  computedAt: string;
  target: number;
  windowDays: number;
  minSample: number;
  /**
   * Pooled over segments, PER LEAD BUCKET. A lead bucket never borrows from
   * another: `prediction_accuracy` keeps the latest write per slot, so nearly
   * every row is short-lead, and a short-lead factor on a long-lead band would
   * leave it under-covered while claiming it is calibrated (PAR-1060 review).
   * A lead bucket without its own pool serves the raw band.
   */
  pooledByLead: Record<string, Omit<BandCalibrationCell, "source">>;
  /** Keyed `lead|segment`. */
  cells: Record<string, BandCalibrationCell>;
}

/** Smallest bin edge whose cumulative share reaches the target, plus coverage at k and at 1. */
function fit(bins: Map<number, number>): {
  n: number;
  k: number | null;
  coverageRaw: number;
  coverageCalibrated: number | null;
} {
  const sorted = [...bins.entries()].sort((a, b) => a[0] - b[0]);
  const n = sorted.reduce((s, [, c]) => s + c, 0);
  if (n === 0) return { n, k: null, coverageRaw: 0, coverageCalibrated: null };
  const rawBin = Math.round(1 / SCORE_BIN);
  let covered = 0;
  let atRaw = 0;
  let kBin: number | null = null;
  let atK = 0;
  for (const [bin, c] of sorted) {
    covered += c;
    if (bin <= rawBin) atRaw = covered;
    if (kBin === null && covered >= BAND_TARGET_COVERAGE * n) {
      kBin = bin;
      atK = covered;
    }
  }
  // A non-positive factor means the cell over-forecasts so far that 95 % of
  // actuals sit below the median: that is a bias, not a narrow band — no k.
  const k = kBin !== null && kBin > 0 ? +(kBin * SCORE_BIN).toFixed(2) : null;
  return {
    n,
    k,
    coverageRaw: atRaw / n,
    coverageCalibrated: k === null ? null : atK / n,
  };
}

export function fitBandCalibration(
  rows: ScoreHistogramRow[],
  now: Date = new Date(),
): BandCalibrationTable {
  const add = (
    m: Map<string, Map<number, number>>,
    key: string,
    r: ScoreHistogramRow,
  ) => {
    const bins = m.get(key) ?? new Map<number, number>();
    bins.set(r.bin, (bins.get(r.bin) ?? 0) + r.n);
    m.set(key, bins);
  };
  const byCell = new Map<string, Map<number, number>>();
  const byLead = new Map<string, Map<number, number>>();
  for (const r of rows) {
    add(byCell, `${r.lead}|${r.segment}`, r);
    add(byLead, r.lead, r);
  }
  const pooledByLead: BandCalibrationTable["pooledByLead"] = {};
  for (const [lead, bins] of byLead) {
    const f = fit(bins);
    pooledByLead[lead] =
      f.n >= BAND_MIN_SAMPLE ? f : { ...f, k: null, coverageCalibrated: null };
  }
  const cells: Record<string, BandCalibrationCell> = {};
  for (const [key, bins] of byCell) {
    const pooled = pooledByLead[key.split("|")[0]];
    const own = fit(bins);
    if (own.n >= BAND_MIN_SAMPLE && own.k !== null) {
      cells[key] = { ...own, source: "cell" };
    } else {
      // Coverage of the pooled k on THIS cell's slots, so the page shows what it buys here.
      let atK = 0;
      if (pooled.k !== null) {
        for (const [bin, c] of bins)
          if (bin * SCORE_BIN <= pooled.k + 1e-9) atK += c;
      }
      cells[key] = {
        n: own.n,
        k: pooled.k,
        source: "pooled",
        coverageRaw: own.coverageRaw,
        coverageCalibrated:
          pooled.k === null || own.n === 0 ? null : atK / own.n,
      };
    }
  }
  return {
    computedAt: now.toISOString(),
    target: BAND_TARGET_COVERAGE,
    windowDays: BAND_CALIBRATION_WINDOW_DAYS,
    minSample: BAND_MIN_SAMPLE,
    pooledByLead,
    cells,
  };
}

/** The factor for one slot: its cell, else its lead bucket's pool, else null (serve raw). */
export function bandFactor(
  table: BandCalibrationTable | null,
  leadMinutes: number,
  predictedWait: number,
): number | null {
  // Optional chaining: the value comes from Redis and may not be a table at all.
  if (!table?.cells) return null;
  const lead = bandLeadBucket(leadMinutes);
  const cell =
    table.cells[`${lead}|${ForecastAccuracyService.bandFor(predictedWait)}`];
  const k = cell ? cell.k : table.pooledByLead?.[lead]?.k;
  return typeof k === "number" && k > 0 ? k : null;
}

/**
 * Multiplies `uncertaintyMinutes` in place on the hourly rows given — the
 * caller passes CatBoost-served rows only (never PCN, whose band is null, nor
 * Chronos, which sets its own). Whole minutes, as the raw field is.
 */
export function applyBandCalibration(
  preds: Array<{
    predictionType: string;
    predictedTime: string;
    predictedWaitTime: number;
    uncertaintyMinutes?: number | null;
  }>,
  table: BandCalibrationTable | null,
  nowMs: number = Date.now(),
): number {
  if (!table) return 0;
  let applied = 0;
  for (const p of preds) {
    if (p.predictionType !== "hourly" || p.uncertaintyMinutes == null) continue;
    const lead = (Date.parse(p.predictedTime) - nowMs) / 60_000;
    const k = bandFactor(table, Math.max(0, lead), p.predictedWaitTime);
    if (k === null) continue;
    p.uncertaintyMinutes = Math.round(p.uncertaintyMinutes * k);
    applied++;
  }
  return applied;
}
