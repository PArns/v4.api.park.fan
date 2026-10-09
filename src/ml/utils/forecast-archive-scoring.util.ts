/**
 * Pure scoring of the forward archive (PAR-831) against BENCH-SPEC truth.
 *
 * Everything here is arithmetic on values already in memory, so the spec can
 * pin it without a database: {@link buildTruthSlots} turns one ride-day of
 * `queue_data` change-log rows into 15-min truth slots, {@link scoreParkDay}
 * compares every archived curve of one park and one target date against them,
 * and {@link scoreCrossParkCrowd} adds the D6 pairs that need every park of
 * the date at once.
 *
 * Every counter is ADDITIVE so the board can pool days by summing:
 *
 * | counter | meaning |
 * |---|---|
 * | `n`, `sae`, `se` | slots compared, Σ\|pred − truth\|, Σ(pred − truth) |
 * | `nBand`, `nBandCov` | slots with a served band, of those with truth ≤ pred + band (D8) |
 * | `rd`, `hitTop2`, `hit30`, `regret` | ride-days scored for best time, hits, Σ regret minutes (D3) |
 * | `nRho`, `sRho` | ride-days with a defined slot Spearman, Σ rho |
 * | `nPeak`, `sPeakAe`, `sPeakE` | ride-days with dayPeak + truth P90, Σ\|err\|, Σerr |
 * | `nStated`, `sStated`, `sStatedAe` | of those, the ones with a served expectedError, Σ stated, Σ realised \|err\| (D8) |
 * | `nRank`, `sRank`, `pairs`, `pairsOk` | park-days with a dayPeak rank Spearman, Σ rho; ride pairs, correctly ordered (D4 ordering) |
 * | `truthRides`, `truthRidesOffered`, `offered`, `offeredNotOperating`, `parkDays`, `parkDaysEmpty` | coverage (D9) |
 * | `n`, `exact`, `within1`, `busyTrue`, `busyPred`, `busyBoth`, `unknownPred`, `crossPairs`, `crossPairsOk` | crowd bucket per park-day (D6) |
 * | `d1Sugg`, `d1SuggOk`, `d1None`, `d1NoneWorse` | next-best-ride suggestions and how many held; the base rate among non-suggestions (D1) |
 *
 * UC3 and D9 rows are written twice: under the slot / plan source and under
 * `level_<tft|catboost|climatology|mixed|none>`, the model behind the day
 * level, so the board can split the horizon by level source.
 */
import { determineCrowdLevel } from "../../common/utils/crowd-level.util";
import {
  ARCHIVE_SOURCE_CODES,
  ArchiveOriginKind,
  ArchiveSourceCode,
  ArchiveSurface,
} from "../entities/forecast-archive-curve.entity";

export const TRUTH_SLOT_MS = 15 * 60_000;
/** queue_data is a change log: a reading older than this is not in force. */
export const MAX_STALENESS_MS = 3 * 3_600_000;
export const MIN_TRUTH_WAIT = 5;
/** BENCH-SPEC ex-ante busy: ride's 56-day q90 at or above this. */
export const BUSY_Q90_MINUTES = 45;
/** Fewest scored slots a ride-day needs before best-time / P90 mean anything. */
export const MIN_RIDE_DAY_SLOTS = 4;
/** dayPeak pairs closer than this in truth are not an ordering question. */
export const MIN_PAIR_GAP_MINUTES = 5;
/** D1 (BENCH-SPEC): suggest when live is this far below the forecast … */
export const D1_GAP_MINUTES = 10;
/** … anywhere within this window after the origin. */
export const D1_WINDOW_MINUTES = 120;
/** Truth slots the D1 window needs before the outcome is judged. */
const D1_MIN_TRUTH_SLOTS = 2;
const THIRTY_MIN_MS = 30 * 60_000;

export interface TruthReading {
  timestamp: number;
  status: string;
  waitTime: number | null;
}

export interface OperatingWindow {
  open: number;
  close: number;
}

/**
 * 15-min truth slots for one ride-day (BENCH-SPEC "Truth"): the slot's value is
 * the STANDBY reading in force at its midpoint — the last row at or before it,
 * no older than 3 h — and the slot counts only when that row says OPERATING
 * with a wait of at least 5 and the midpoint lies inside a published OPERATING
 * window. Slots are on the UTC quarter-hour grid, which is the park-local one
 * for every timezone offset that is a multiple of 15 minutes.
 *
 * `readings` must be sorted by timestamp ascending.
 */
export function buildTruthSlots(
  readings: TruthReading[],
  windows: OperatingWindow[],
): Map<number, number> {
  const out = new Map<number, number>();
  if (readings.length === 0) return out;
  const sorted = [...windows].sort((a, b) => a.open - b.open);
  for (const w of sorted) {
    let i = 0;
    const first = Math.floor(w.open / TRUTH_SLOT_MS) * TRUTH_SLOT_MS;
    for (let slot = first; slot < w.close; slot += TRUTH_SLOT_MS) {
      const mid = slot + TRUTH_SLOT_MS / 2;
      if (mid < w.open || mid >= w.close) continue;
      while (i + 1 < readings.length && readings[i + 1].timestamp <= mid) i++;
      const r = readings[i];
      if (r.timestamp > mid || mid - r.timestamp > MAX_STALENESS_MS) continue;
      if (r.status !== "OPERATING") continue;
      if (r.waitTime === null || r.waitTime < MIN_TRUTH_WAIT) continue;
      out.set(slot, r.waitTime);
    }
  }
  return out;
}

/** Linear-interpolated percentile (Postgres `percentile_cont`). */
export function percentileCont(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const s = [...values].sort((a, b) => a - b);
  const pos = (s.length - 1) * p;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return s[lo] + (s[hi] - s[lo]) * (pos - lo);
}

function averageRanks(xs: number[]): number[] {
  const idx = xs.map((v, i) => [v, i] as const).sort((a, b) => a[0] - b[0]);
  const ranks = new Array<number>(xs.length);
  let k = 0;
  while (k < idx.length) {
    let j = k;
    while (j + 1 < idx.length && idx[j + 1][0] === idx[k][0]) j++;
    const r = (k + j) / 2 + 1;
    for (let m = k; m <= j; m++) ranks[idx[m][1]] = r;
    k = j + 1;
  }
  return ranks;
}

/** Spearman rank correlation with average ranks for ties; null if undefined. */
export function spearman(xs: number[], ys: number[]): number | null {
  if (xs.length !== ys.length || xs.length < 3) return null;
  const rx = averageRanks(xs);
  const ry = averageRanks(ys);
  const mean = (a: number[]) => a.reduce((s, v) => s + v, 0) / a.length;
  const mx = mean(rx);
  const my = mean(ry);
  let num = 0;
  let dx = 0;
  let dy = 0;
  for (let i = 0; i < rx.length; i++) {
    num += (rx[i] - mx) * (ry[i] - my);
    dx += (rx[i] - mx) ** 2;
    dy += (ry[i] - my) ** 2;
  }
  if (dx === 0 || dy === 0) return null;
  return num / Math.sqrt(dx * dy);
}

/**
 * Lead bucket of one 15-min slot, by minutes after the origin. Leads are never
 * pooled in a headline number (BENCH-SPEC), so the buckets are narrow where
 * the decision changes: UC1 is everything up to two hours.
 */
export function slotLeadBucket(
  leadMinutes: number,
): { useCase: "UC1" | "UC2"; lead: string } | null {
  if (leadMinutes < 0) return null;
  if (leadMinutes <= 60) return { useCase: "UC1", lead: "h0-1" };
  if (leadMinutes <= 120) return { useCase: "UC1", lead: "h1-2" };
  if (leadMinutes <= 360) return { useCase: "UC2", lead: "h2-6" };
  if (leadMinutes <= 720) return { useCase: "UC2", lead: "h6-12" };
  if (leadMinutes <= 1440) return { useCase: "UC2", lead: "h12-24" };
  if (leadMinutes <= 2880) return { useCase: "UC2", lead: "h24-48" };
  return null;
}

const CROWD_ORDINALS: Record<string, number> = {
  very_low: 0,
  low: 1,
  moderate: 2,
  high: 3,
  very_high: 4,
  extreme: 5,
};
/** Bucket index, or null for `unknown` / absent (which are counted apart). */
export function crowdOrdinal(level: string | null | undefined): number | null {
  if (!level) return null;
  return CROWD_ORDINALS[level] ?? null;
}
const BUSY_ORDINAL = CROWD_ORDINALS.high;

/** Region of a park for the score's region axis (BENCH-SPEC: EU / NA / Asia). */
export function regionOf(continentSlug: string | null | undefined): string {
  switch (continentSlug) {
    case "europe":
      return "EU";
    case "north-america":
      return "NA";
    case "asia":
      return "ASIA";
    default:
      return "OTHER";
  }
}

/** Additive counters keyed by region × use case × lead × source × segment. */
export class ScoreAccumulator {
  private readonly rows = new Map<string, Record<string, number>>();

  add(
    region: string,
    useCase: string,
    lead: string,
    source: string,
    segment: string,
    counters: Record<string, number>,
    alsoAll = true,
  ): void {
    const regions = region === "ALL" || !alsoAll ? [region] : [region, "ALL"];
    for (const r of regions) {
      const key = [r, useCase, lead, source, segment].join("|");
      const row = this.rows.get(key) ?? {};
      for (const [k, v] of Object.entries(counters)) {
        if (!Number.isFinite(v)) continue;
        row[k] = (row[k] ?? 0) + v;
      }
      this.rows.set(key, row);
    }
  }

  entries(): Array<{
    region: string;
    useCase: string;
    lead: string;
    source: string;
    segment: string;
    sums: Record<string, number>;
  }> {
    return [...this.rows.entries()].map(([key, sums]) => {
      const [region, useCase, lead, source, segment] = key.split("|");
      return { region, useCase, lead, source, segment, sums };
    });
  }
}

export interface ArchivedCurve {
  attractionId: string;
  surface: ArchiveSurface;
  originKind: ArchiveOriginKind;
  originAt: number;
  leadDays: number;
  slotStart: number;
  slotMinutes: number;
  waits: (number | null)[];
  sources: string;
  bands: (number | null)[] | null;
  dayPeak: number | null;
  expectedError: number | null;
  rideQ90: number | null;
  isHeadliner: boolean;
  /** park_hourly: live STANDBY wait at the origin (D1 anchor). */
  liveWait?: number | null;
  /** plan_day: model behind the day level. */
  levelSource?: string | null;
}

export interface ArchivedParkDay {
  originAt: number;
  leadDays: number;
  tier: string | null;
  crowdLevel: string | null;
  predictedCrowdLevel: string | null;
  levelSource?: string | null;
}

export interface ParkDayScoringInput {
  region: string;
  curves: ArchivedCurve[];
  parkDays: ArchivedParkDay[];
  /** attraction → slot start (ms) → truth wait. Every ride of the park. */
  truth: Map<string, Map<number, number>>;
  /** Whether the park published an OPERATING window for the date at all. */
  hadWindows: boolean;
  headlinerIds: ReadonlySet<string>;
  /** Park's typical-day-peak baseline; ≤ 0 = not ratable (D6 skipped). */
  typicalDayPeak: number;
}

/** One park-day's crowd bucket, for the cross-park pairs of D6. */
export interface CrowdObservation {
  region: string;
  lead: string;
  source: string;
  predicted: number;
  truth: number;
  truthRatio: number;
}

function sourceName(code: string | undefined): string {
  return ARCHIVE_SOURCE_CODES[code as ArchiveSourceCode] ?? "unknown";
}

function segmentsOf(curve: ArchivedCurve): string[] {
  const out = ["all"];
  if (curve.rideQ90 !== null && curve.rideQ90 >= BUSY_Q90_MINUTES) {
    out.push("busy");
  }
  if (curve.isHeadliner) out.push("headliner");
  return out;
}

interface Compared {
  slot: number;
  pred: number;
  truth: number;
  src: string;
  band: number | null;
}

/** The archived value covering a truth slot, if the curve has one. */
export function valueAt(
  curve: ArchivedCurve,
  slotStartMs: number,
): { wait: number; src: string; band: number | null } | null {
  const step = curve.slotMinutes * 60_000;
  const idx = Math.floor((slotStartMs - curve.slotStart) / step);
  if (idx < 0 || idx >= curve.waits.length) return null;
  const wait = curve.waits[idx];
  if (wait === null || wait === undefined) return null;
  return {
    wait,
    src: sourceName(curve.sources[idx]),
    band: curve.bands?.[idx] ?? null,
  };
}

/** Day-level lead label. */
export function dayLead(leadDays: number): string {
  return `d${leadDays}`;
}

/**
 * Best-time decision of one ride-day (D3): did the slots the curve calls the
 * quietest turn out to be the quietest? Ties in the prediction (an hourly
 * product serves four equal quarter-hours) resolve to the earliest slot, which
 * is what a "go at" recommendation reads first.
 */
export function bestTimeOutcome(compared: Compared[]): {
  hitTop2: boolean;
  hit30: boolean;
  regret: number;
} | null {
  if (compared.length < MIN_RIDE_DAY_SLOTS) return null;
  const byPred = [...compared].sort(
    (a, b) => a.pred - b.pred || a.slot - b.slot,
  );
  const trueMin = Math.min(...compared.map((c) => c.truth));
  const top2 = byPred.slice(0, 2);
  const best = byPred[0];
  return {
    hitTop2: top2.some((c) => c.truth === trueMin),
    hit30: compared.some(
      (c) =>
        c.truth === trueMin && Math.abs(c.slot - best.slot) <= THIRTY_MIN_MS,
    ),
    regret: best.truth - trueMin,
  };
}

function rideDayTruthP90(
  slots: Map<number, number> | undefined,
): number | null {
  if (!slots || slots.size < MIN_RIDE_DAY_SLOTS) return null;
  return percentileCont([...slots.values()], 0.9);
}

/**
 * D1, next-best-ride (BENCH-SPEC decision table): the frontend suggests a ride
 * when its live wait is at least {@link D1_GAP_MINUTES} below its forecast
 * somewhere in the next {@link D1_WINDOW_MINUTES}. The suggestion was right
 * when the ride actually got that much worse within the same window.
 *
 * Every judged origin lands in `D1|h0-2|all` (suggestions AND the base rate
 * among non-suggestions, so precision can be read against chance); a
 * suggestion is also counted under the lead bucket and source of the first
 * forecast slot that triggered it.
 */
export function scoreNextBestRide(
  curve: ArchivedCurve,
  live: number,
  truthSlots: Map<number, number>,
  segments: string[],
  region: string,
  acc: ScoreAccumulator,
): void {
  const end = curve.originAt + D1_WINDOW_MINUTES * 60_000;
  const future = [...truthSlots.entries()].filter(
    ([slot]) => slot > curve.originAt && slot <= end,
  );
  if (future.length < D1_MIN_TRUTH_SLOTS) return;
  const worse = future.some(([, v]) => v >= live + D1_GAP_MINUTES);

  let trigger: { lead: string; src: string } | null = null;
  const step = curve.slotMinutes * 60_000;
  for (let i = 0; i < curve.waits.length; i++) {
    const slot = curve.slotStart + i * step;
    if (slot <= curve.originAt || slot > end) continue;
    const w = curve.waits[i];
    if (w === null || w === undefined || w < live + D1_GAP_MINUTES) continue;
    const bucket = slotLeadBucket((slot - curve.originAt) / 60_000);
    trigger = {
      lead: bucket?.lead ?? "h0-2",
      src: sourceName(curve.sources[i]),
    };
    break;
  }

  const counters: Record<string, number> = trigger
    ? { d1Sugg: 1, d1SuggOk: worse ? 1 : 0 }
    : { d1None: 1, d1NoneWorse: worse ? 1 : 0 };
  for (const seg of segments) {
    acc.add(region, "D1", "h0-2", "all", seg, counters);
    if (trigger) {
      acc.add(region, "D1", trigger.lead, trigger.src, seg, counters);
    }
  }
}

/**
 * Scores every archived curve of one park and one target date. Returns the
 * park-day crowd observations for {@link scoreCrossParkCrowd}.
 */
export function scoreParkDay(
  input: ParkDayScoringInput,
  acc: ScoreAccumulator,
): CrowdObservation[] {
  const { region, truth } = input;

  // Per plan_day origin: the rides offered and their dayPeak vs truth P90.
  const planByOrigin = new Map<
    number,
    {
      leadDays: number;
      peaks: Array<{ pred: number; truth: number }>;
      rides: Set<string>;
    }
  >();

  for (const curve of input.curves) {
    const truthSlots = truth.get(curve.attractionId);
    const compared: Compared[] = [];
    if (truthSlots) {
      for (const [slot, value] of truthSlots) {
        const v = valueAt(curve, slot);
        if (!v) continue;
        compared.push({
          slot,
          pred: v.wait,
          truth: value,
          src: v.src,
          band: v.band,
        });
      }
    }
    compared.sort((a, b) => a.slot - b.slot);
    const segments = segmentsOf(curve);
    const levelKey =
      curve.surface === "plan_day" && curve.levelSource
        ? `level_${curve.levelSource}`
        : null;

    // ---- slot metrics: MAE, bias, band coverage ----
    for (const c of compared) {
      let useCase: string;
      let lead: string;
      if (curve.surface === "park_hourly") {
        const bucket = slotLeadBucket((c.slot - curve.originAt) / 60_000);
        if (!bucket) continue;
        useCase = bucket.useCase;
        lead = bucket.lead;
      } else {
        useCase = "UC3";
        lead = dayLead(curve.leadDays);
      }
      const err = c.pred - c.truth;
      const counters: Record<string, number> = {
        n: 1,
        sae: Math.abs(err),
        se: err,
      };
      if (c.band !== null) {
        counters.nBand = 1;
        counters.nBandCov = c.truth <= c.pred + c.band ? 1 : 0;
      }
      for (const seg of segments) {
        acc.add(region, useCase, lead, c.src, seg, counters);
        acc.add(region, useCase, lead, "all", seg, counters);
        if (levelKey) acc.add(region, useCase, lead, levelKey, seg, counters);
      }
    }

    // ---- D1: next-best-ride suggestion from the live anchor ----
    if (
      curve.surface === "park_hourly" &&
      curve.liveWait !== null &&
      curve.liveWait !== undefined &&
      truthSlots
    ) {
      scoreNextBestRide(
        curve,
        curve.liveWait,
        truthSlots,
        segments,
        region,
        acc,
      );
    }

    // ---- ride-day metrics: best time (D3), slot Spearman, dayPeak ----
    // Only a whole day is a ride-day: intraday origins see a few hours of it.
    if (curve.originKind === "intraday") continue;
    const useCase = curve.surface === "park_hourly" ? "UC2" : "UC3";
    const lead = dayLead(curve.leadDays);
    const srcs = new Set(compared.map((c) => c.src));
    const rideSource = srcs.size === 1 ? [...srcs][0] : "mixed";

    const rideCounters: Record<string, number> = {};
    const best = bestTimeOutcome(compared);
    if (best) {
      rideCounters.rd = 1;
      rideCounters.hitTop2 = best.hitTop2 ? 1 : 0;
      rideCounters.hit30 = best.hit30 ? 1 : 0;
      rideCounters.regret = best.regret;
    }
    const rho = spearman(
      compared.map((c) => c.pred),
      compared.map((c) => c.truth),
    );
    if (rho !== null && compared.length >= MIN_RIDE_DAY_SLOTS) {
      rideCounters.nRho = 1;
      rideCounters.sRho = rho;
    }

    if (curve.surface === "plan_day") {
      const plan = planByOrigin.get(curve.originAt) ?? {
        leadDays: curve.leadDays,
        peaks: [],
        rides: new Set<string>(),
      };
      plan.rides.add(curve.attractionId);
      const p90 = rideDayTruthP90(truthSlots);
      if (curve.dayPeak !== null && p90 !== null) {
        const e = curve.dayPeak - p90;
        rideCounters.nPeak = 1;
        rideCounters.sPeakAe = Math.abs(e);
        rideCounters.sPeakE = e;
        if (curve.expectedError !== null) {
          rideCounters.nStated = 1;
          rideCounters.sStated = curve.expectedError;
          rideCounters.sStatedAe = Math.abs(e);
        }
        plan.peaks.push({ pred: curve.dayPeak, truth: p90 });
      }
      planByOrigin.set(curve.originAt, plan);
    }

    if (Object.keys(rideCounters).length > 0) {
      for (const seg of segments) {
        acc.add(region, useCase, lead, rideSource, seg, rideCounters);
        acc.add(region, useCase, lead, "all", seg, rideCounters);
        if (levelKey) {
          acc.add(region, useCase, lead, levelKey, seg, rideCounters);
        }
      }
    }
  }

  // ---- park-day: dayPeak ordering (UC3) and coverage (D9) ----
  const truthRides = new Set(
    [...truth.entries()].filter(([, s]) => s.size > 0).map(([id]) => id),
  );
  for (const parkDay of input.parkDays) {
    const plan = planByOrigin.get(parkDay.originAt);
    const lead = dayLead(parkDay.leadDays);
    if (plan && plan.peaks.length >= 3) {
      const counters: Record<string, number> = {};
      const rho = spearman(
        plan.peaks.map((p) => p.pred),
        plan.peaks.map((p) => p.truth),
      );
      if (rho !== null) {
        counters.nRank = 1;
        counters.sRank = rho;
      }
      let pairs = 0;
      let ok = 0;
      for (let i = 0; i < plan.peaks.length; i++) {
        for (let j = i + 1; j < plan.peaks.length; j++) {
          const dt = plan.peaks[i].truth - plan.peaks[j].truth;
          if (Math.abs(dt) < MIN_PAIR_GAP_MINUTES) continue;
          const dp = plan.peaks[i].pred - plan.peaks[j].pred;
          pairs++;
          ok += dp === 0 ? 0.5 : Math.sign(dp) === Math.sign(dt) ? 1 : 0;
        }
      }
      if (pairs > 0) {
        counters.pairs = pairs;
        counters.pairsOk = ok;
      }
      if (Object.keys(counters).length > 0) {
        acc.add(region, "UC3", lead, "all", "all", counters);
      }
    }

    if (input.hadWindows) {
      const offered = plan?.rides ?? new Set<string>();
      let truthOffered = 0;
      for (const id of truthRides) if (offered.has(id)) truthOffered++;
      let offeredNotOperating = 0;
      for (const id of offered) if (!truthRides.has(id)) offeredNotOperating++;
      const coverage = {
        truthRides: truthRides.size,
        truthRidesOffered: truthOffered,
        offered: offered.size,
        offeredNotOperating,
        parkDays: truthRides.size > 0 ? 1 : 0,
        parkDaysEmpty: truthRides.size > 0 && offered.size === 0 ? 1 : 0,
      };
      acc.add(region, "D9", lead, parkDay.tier ?? "none", "all", coverage);
      if (parkDay.levelSource) {
        acc.add(
          region,
          "D9",
          lead,
          `level_${parkDay.levelSource}`,
          "all",
          coverage,
        );
      }
    }
  }

  // ---- D6: crowd bucket per park-day ----
  const observations: CrowdObservation[] = [];
  if (input.typicalDayPeak > 0) {
    const headlinerPeaks: number[] = [];
    for (const id of input.headlinerIds) {
      const p90 = rideDayTruthP90(truth.get(id));
      if (p90 !== null) headlinerPeaks.push(p90);
    }
    if (headlinerPeaks.length > 0) {
      const level =
        headlinerPeaks.reduce((a, b) => a + b, 0) / headlinerPeaks.length;
      const ratio = (level / input.typicalDayPeak) * 100;
      const truthOrd = crowdOrdinal(determineCrowdLevel(ratio))!;
      for (const parkDay of input.parkDays) {
        const lead = dayLead(parkDay.leadDays);
        for (const [source, value] of [
          ["predicted", parkDay.predictedCrowdLevel],
          ["plan_day", parkDay.crowdLevel],
        ] as const) {
          const predOrd = crowdOrdinal(value);
          if (predOrd === null) {
            acc.add(region, "D6", lead, source, "all", { unknownPred: 1 });
            continue;
          }
          const busyTrue = truthOrd >= BUSY_ORDINAL;
          const busyPred = predOrd >= BUSY_ORDINAL;
          acc.add(region, "D6", lead, source, "all", {
            n: 1,
            exact: predOrd === truthOrd ? 1 : 0,
            within1: Math.abs(predOrd - truthOrd) <= 1 ? 1 : 0,
            busyTrue: busyTrue ? 1 : 0,
            busyPred: busyPred ? 1 : 0,
            busyBoth: busyTrue && busyPred ? 1 : 0,
          });
          observations.push({
            region,
            lead,
            source,
            predicted: predOrd,
            truth: truthOrd,
            truthRatio: ratio,
          });
        }
      }
    }
  }
  return observations;
}

/**
 * D6 cross-park ordering on the same date: for two parks whose realised
 * buckets differ, did the forecast order them the same way? Counted within
 * each region and over all parks; a predicted tie counts half.
 */
export function scoreCrossParkCrowd(
  observations: CrowdObservation[],
  acc: ScoreAccumulator,
): void {
  const groups = new Map<string, CrowdObservation[]>();
  for (const o of observations) {
    for (const region of [o.region, "ALL"]) {
      const key = [region, o.lead, o.source].join("|");
      const list = groups.get(key) ?? [];
      list.push(o);
      groups.set(key, list);
    }
  }
  for (const [key, list] of groups) {
    const [region, lead, source] = key.split("|");
    let pairs = 0;
    let ok = 0;
    for (let i = 0; i < list.length; i++) {
      for (let j = i + 1; j < list.length; j++) {
        const dt = list[i].truth - list[j].truth;
        if (dt === 0) continue;
        const dp = list[i].predicted - list[j].predicted;
        pairs++;
        ok += dp === 0 ? 0.5 : Math.sign(dp) === Math.sign(dt) ? 1 : 0;
      }
    }
    if (pairs > 0) {
      // ALL is its own group here (pairs across two regions belong to
      // neither), so no automatic copy into ALL.
      acc.add(
        region,
        "D6",
        lead,
        source,
        "all",
        { crossPairs: pairs, crossPairsOk: ok },
        false,
      );
    }
  }
}
