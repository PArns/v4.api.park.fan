/**
 * Drift watch (PAR-1059), pure part: per served model, the last 7 days
 * against the 28 before — quantile coverage and the MAE relative to the
 * weekday median. The thresholds are here and in docs/ml/ml-overview.md §2a.
 * No alert is sent (owner decision "vorerst keine Alarme"); the result is a
 * status on the ML overview.
 */
import {
  MlDriftModelDto,
  MlDriftStatus,
  MlDriftWatchDto,
  MlDriftWindowDto,
} from "../dto/ml-overview.dto";
import { fmtDe } from "./model-comparison.util";

export const DRIFT_THRESHOLDS = {
  /** Fewer scored slots than this in either window: `no_data`, never a verdict. */
  minSlots: 1000,
  /** q0.8 coverage moving more than 5 percentage points. */
  coverageQ80Pp: 5,
  /** q0.95 coverage moving more than 3 points (a narrower tail, a tighter bound). */
  coverageQ95Pp: 3,
  /**
   * The MAE ÷ weekday-median ratio rising by more than 10 % (e.g. 0.80 →
   * 0.88). A ratio, not the MAE: a busier week raises every MAE, the
   * model's and the baseline's alike.
   */
  skillRise: 0.1,
} as const;

/** The window sums one SQL row carries per model and window. */
export interface DriftWindowSums {
  n: number;
  sae: number;
  /** Slots that have a weekday median, and both error sums on them. */
  nBase: number;
  saeOnBase: number;
  saeBase: number;
  /** Slots with a stored quantile, and how many came in at or under it. */
  nQ: number;
  hitQ80: number;
  hitQ95: number;
}

export const DRIFT_MODEL_LABELS: Record<string, string> = {
  catboost: "CatBoost (Stundenwerte, mit Quantilen)",
  pcn: "PCN (ausgeliefert)",
  chronos: "Chronos-2 (ausgeliefert)",
};

const r3 = (v: number) => Math.round(v * 1000) / 1000;

export function driftWindow(s: DriftWindowSums | undefined): MlDriftWindowDto {
  if (!s || s.n === 0) {
    return {
      n: 0,
      mae: null,
      baselineMae: null,
      skill: null,
      coverageQ80: null,
      coverageQ95: null,
    };
  }
  return {
    n: s.n,
    mae: r3(s.sae / s.n),
    baselineMae: s.nBase ? r3(s.saeBase / s.nBase) : null,
    skill: s.nBase && s.saeBase > 0 ? r3(s.saeOnBase / s.saeBase) : null,
    coverageQ80: s.nQ ? r3(s.hitQ80 / s.nQ) : null,
    coverageQ95: s.nQ ? r3(s.hitQ95 / s.nQ) : null,
  };
}

const pp = (v: number) =>
  `${fmtDe(Math.round(v * 1000) / 10, true)} Prozentpunkte`;

/** One model's status from its two windows. */
export function driftModel(
  model: string,
  recentSums: DriftWindowSums | undefined,
  previousSums: DriftWindowSums | undefined,
): MlDriftModelDto {
  const recent = driftWindow(recentSums);
  const previous = driftWindow(previousSums);
  const base = {
    model,
    label: DRIFT_MODEL_LABELS[model] ?? model,
    recent,
    previous,
  };
  const t = DRIFT_THRESHOLDS;
  if (recent.n < t.minSlots || previous.n < t.minSlots) {
    return { ...base, status: "no_data", reasons: [] };
  }
  const reasons: string[] = [];
  const cov = (
    a: number | null,
    b: number | null,
    limitPp: number,
    name: string,
  ) => {
    if (a === null || b === null) return;
    if (Math.abs(a - b) * 100 > limitPp) {
      reasons.push(
        `Abdeckung ${name}: ${pp(a - b)} gegenüber den 4 Wochen davor (erlaubt ±${limitPp})`,
      );
    }
  };
  cov(recent.coverageQ80, previous.coverageQ80, t.coverageQ80Pp, "q0,8");
  cov(recent.coverageQ95, previous.coverageQ95, t.coverageQ95Pp, "q0,95");
  if (
    recent.skill !== null &&
    previous.skill !== null &&
    recent.skill > previous.skill * (1 + t.skillRise)
  ) {
    reasons.push(
      `Fehler gegenüber dem Wochentag-Median: ${dec2(recent.skill)} statt ${dec2(previous.skill)} (erlaubt bis +${t.skillRise * 100} %)`,
    );
  }
  return { ...base, status: reasons.length ? "drift" : "ok", reasons };
}

const dec2 = (v: number) => v.toFixed(2).replace(".", ",");

const RANK: Record<MlDriftStatus, number> = { no_data: 0, ok: 1, drift: 2 };

export function buildDriftWatch(
  sums: Map<string, { recent?: DriftWindowSums; previous?: DriftWindowSums }>,
  computedAt: Date,
): MlDriftWatchDto {
  const models = Object.keys(DRIFT_MODEL_LABELS).map((m) =>
    driftModel(m, sums.get(m)?.recent, sums.get(m)?.previous),
  );
  const status = models.reduce<MlDriftStatus>(
    (w, m) => (RANK[m.status] > RANK[w] ? m.status : w),
    "no_data",
  );
  return { computedAt: computedAt.toISOString(), status, models };
}
