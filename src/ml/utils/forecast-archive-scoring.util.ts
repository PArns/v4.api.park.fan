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
 * | `nBand`, `nBandCov` | park_hourly only: slots with a served band (q95 − q50), of those with truth ≤ pred + band (D8) |
 * | `rd`, `regret` | ride-days scored for best time, Σ regret minutes — the D3 headline |
 * | `rdHit`, `hitTop2`, `hit30`, `rdFlat` | ride-days with a clear best time, hits among them; ride-days whose minimum is shared by > 25 % of slots (no best time to find) |
 * | `nRho`, `sRho` | ride-days with a defined slot Spearman, Σ rho |
 * | `nPeak`, `sPeakAe`, `sPeakE` | ride-days with dayPeak + truth P90, Σ\|err\|, Σerr |
 * | `nStated`, `sStated`, `sStatedAe` | of those, the ones with a served expectedError, Σ stated, Σ realised \|err\| (D8) |
 * | `nPeakBand`, `nPeakBandCov` | plan_day: ride-days with a served band, of those with truth P90 − dayPeak ≤ band — one-sided, like the band (D8); never pooled with the slot band |
 * | `nRank`, `sRank`, `pairs`, `pairsOk` | park-days with a dayPeak rank Spearman, Σ rho; ride pairs, correctly ordered (D4 ordering) |
 * | `truthRides`, `truthRidesOffered`, `offered`, `offeredNotOperating`, `parkDays`, `parkDaysEmpty` | coverage (D9) |
 * | `n`, `exact`, `within1`, `busyTrue`, `busyPred`, `busyBoth`, `unknownPred`, `crossPairs`, `crossPairsOk` | crowd bucket per park-day (D6) |
 * | `d1Sugg`, `d1SuggOk` | next-best-ride suggestions and how many held (D1) |
 * | `d1None`, `d1NoneWorse` | rides with a qualifying plan hour but a gap under 10 min, and how many got 10 min worse anyway — the base rate (D1) |
 * | `d1NoHour`, `d1NoHourWorse` | rides with no qualifying plan hour at all (closing soon, no curve), counted apart so they do not dilute the base rate (D1) |
 *
 * The Chronos-2 shadow (PAR-852) is scored like the surface it shadows — the
 * live variant on the slot leads of UC1/UC2, the grid variant on UC3's day
 * leads — under its own source (`chronos2`, `chronos2_grid`) and never into
 * `all`, `level_*`, D1, D9 or the dayPeak ordering, which judge what is served.
 * {@link scorePairedShadows} adds the paired per-park comparison against the
 * served curve, persistence and H5 that the board's bootstrap reads.
 *
 * UC3 and D9 rows are also written under `level_<tft|catboost|climatology|
 * mixed|none>`, the model behind the day level, so the board can split the
 * horizon by level source — but only for numbers that come from the level:
 * composed / climatology hours, and dayPeak. A `measured` hour is the hourly
 * model's, whatever the day level was.
 */
import { determineCrowdLevel } from "../../common/utils/crowd-level.util";
import { addIsoDays } from "../../common/utils/date.util";
import { DaySlots, weekdayAverageCurve } from "./weekday-average.util";
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
/** A ride-day whose true minimum is shared by more than this share of its
 *  slots has no best time to find: it counts for regret, not for hit rates. */
export const FLAT_MIN_SHARE = 0.25;
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
  /** park_hourly only: per-slot q95 − q50. */
  bands: (number | null)[] | null;
  dayPeak: number | null;
  expectedError: number | null;
  /** plan_day only: the served band around dayPeak. */
  peakBand?: number | null;
  rideQ90: number | null;
  isHeadliner: boolean;
  /** Live STANDBY wait in force at the origin (D1 anchor). */
  liveWait?: number | null;
  /** plan_day intraday: the frontend's next-best-ride "later" hour (D1). */
  laterWait?: number | null;
  laterAt?: number | null;
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
  /** The park's typical-day-peak baseline as it stood at the origin. */
  typicalDayPeak?: number | null;
  /** `crowdLevel` came from the calendar's fallback chain, not a forecast. */
  crowdLevelFallback?: boolean;
}

export interface ParkDayScoringInput {
  region: string;
  curves: ArchivedCurve[];
  parkDays: ArchivedParkDay[];
  /** attraction → slot start (ms) → truth wait. Every ride of the park. */
  truth: Map<string, Map<number, number>>;
  /** Whether the park published an OPERATING window for the date at all. */
  hadWindows: boolean;
  /**
   * The realised day value in the calendar's own statistic (see
   * {@link calendarDayValue}); null when no headliner had a qualifying row.
   * D6 divides it by each park-day's `typicalDayPeak`.
   */
  calendarDayValue: number | null;
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

/** Sources whose number comes from the day level, not the hourly model. */
/**
 * The composer A/B surfaces (PAR-834) and the source each is scored under.
 * Their own use case, so nothing they add reaches a UC3 number the served
 * planner is judged by.
 */
export const SHADOW_USE_CASE = "UC3S";
const SHADOW_SOURCES: Partial<Record<ArchiveSurface, string>> = {
  plan_day_shadow_peak: "shadow_peak_scaled",
  plan_day_shadow_h5: "shadow_h5",
  plan_day_shadow_routed: "shadow_h5_routed",
};

/**
 * The Chronos-2 shadow surfaces (PAR-852) and the source each is scored under.
 * Unlike the composer A/B they keep the served use case and lead (so the board
 * reads them next to the served rows), but like it they never enter `all`.
 */
export const CHRONOS_SOURCES: Partial<Record<ArchiveSurface, string>> = {
  chronos_live: "chronos2",
  chronos_grid: "chronos2_grid",
};

// `composed_h5` is not here: the plain H5 profile uses no day level at all.
const LEVEL_DERIVED = new Set(["composed", "composed_h5_tft", "climatology"]);

export interface Compared {
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
 *
 * Regret is the headline: it is defined on every ride-day. The hit rates are
 * not, and are null on a FLAT ride-day — one whose true minimum is held by more
 * than {@link FLAT_MIN_SHARE} of the slots. A quiet ride at 5 minutes all day
 * would otherwise "hit" whatever the forecast said and inflate the rate.
 */
export function bestTimeOutcome(compared: Compared[]): {
  hitTop2: boolean | null;
  hit30: boolean | null;
  regret: number;
} | null {
  if (compared.length < MIN_RIDE_DAY_SLOTS) return null;
  const byPred = [...compared].sort(
    (a, b) => a.pred - b.pred || a.slot - b.slot,
  );
  const trueMin = Math.min(...compared.map((c) => c.truth));
  const best = byPred[0];
  const regret = best.truth - trueMin;
  const atMin = compared.filter((c) => c.truth === trueMin).length;
  if (atMin / compared.length > FLAT_MIN_SHARE) {
    return { hitTop2: null, hit30: null, regret };
  }
  const top2 = byPred.slice(0, 2);
  return {
    hitTop2: top2.some((c) => c.truth === trueMin),
    hit30: compared.some(
      (c) =>
        c.truth === trueMin && Math.abs(c.slot - best.slot) <= THIRTY_MIN_MS,
    ),
    regret,
  };
}

export function rideDayTruthP90(
  slots: Map<number, number> | undefined,
): number | null {
  if (!slots || slots.size < MIN_RIDE_DAY_SLOTS) return null;
  return percentileCont([...slots.values()], 0.9);
}

/**
 * The realised park-day value in the CALENDAR's statistic, so D6's numerator
 * and denominator are the same function: `calculateTypicalDayPeak` takes, per
 * headliner and park-local date, the P90 of the raw change-log rows that are
 * STANDBY, OPERATING and ≥ 10 min (no opening-window filter), averages it over
 * the headliners, and the baseline is the median of that over 548 days. This
 * is that day value for one date; `inDay` decides the park-local date.
 */
export function calendarDayValue(
  readings: ReadonlyMap<string, readonly TruthReading[]>,
  headlinerIds: ReadonlySet<string>,
  inDay: (timestamp: number) => boolean,
): number | null {
  const p90s: number[] = [];
  for (const id of headlinerIds) {
    const values = (readings.get(id) ?? [])
      .filter(
        (r) =>
          inDay(r.timestamp) &&
          r.status === "OPERATING" &&
          r.waitTime !== null &&
          r.waitTime >= 10,
      )
      .map((r) => r.waitTime as number);
    const p90 = percentileCont(values, 0.9);
    if (p90 !== null) p90s.push(p90);
  }
  if (p90s.length === 0) return null;
  return p90s.reduce((a, b) => a + b, 0) / p90s.length;
}

/**
 * The frontend's next-best-ride "later" value, ported from
 * `park.fan/lib/planner/next-best-ride.ts` (`suggestNextRides`): the maximum
 * forecast over the whole hours of today's `/plan/day` curve whose START lies
 * in [now + walk, now + 120 min] and whose unfolded hour is before the
 * unfolded close hour. The archive has no visitor position, so walk = 0.
 * `hours[].hour` is the plan's own axis (24+ past midnight); `nowMinute` is
 * park-local minutes since midnight.
 */
export function nextRideLater(
  hours: ReadonlyArray<{ hour: number; wait: number }>,
  openHour: number | null,
  rawCloseHour: number | null,
  nowMinute: number,
): { hour: number; wait: number } | null {
  const wrapAt =
    openHour != null && rawCloseHour != null && rawCloseHour < openHour
      ? openHour
      : null;
  const unfold = (hour: number) =>
    wrapAt != null && hour < wrapAt ? hour + 24 : hour;
  const closeHour =
    openHour != null && rawCloseHour != null
      ? rawCloseHour < openHour
        ? rawCloseHour + 24
        : rawCloseHour
      : rawCloseHour;
  const now =
    wrapAt != null && nowMinute < wrapAt * 60 ? nowMinute + 24 * 60 : nowMinute;
  const horizon = now + D1_WINDOW_MINUTES;
  let peak: { hour: number; wait: number } | null = null;
  for (const h of hours) {
    const axis = unfold(h.hour);
    const start = axis * 60;
    if (start < now || start > horizon) continue;
    if (closeHour != null && axis >= closeHour) continue;
    if (!peak || h.wait > peak.wait) peak = { hour: h.hour, wait: h.wait };
  }
  return peak;
}

/**
 * D1, next-best-ride (BENCH-SPEC decision table), on the decision the frontend
 * actually makes: a ride is suggested when its in-force live wait is at least
 * {@link D1_GAP_MINUTES} below the "later" forecast {@link nextRideLater}
 * found. The suggestion held when the ride's truth reached live + 10 within
 * the same 120 minutes.
 *
 * Every judged origin lands in `D1|h0-2|all` (suggestions AND the base rate
 * among non-suggestions, so precision can be read against chance); a
 * suggestion is also counted under the lead bucket and source of its "later"
 * hour.
 */
export function scoreNextBestRide(
  curve: ArchivedCurve,
  truthSlots: Map<number, number>,
  segments: string[],
  region: string,
  acc: ScoreAccumulator,
): void {
  const live = curve.liveWait;
  if (live === null || live === undefined) return;
  const end = curve.originAt + D1_WINDOW_MINUTES * 60_000;
  const future = [...truthSlots.entries()].filter(
    ([slot]) => slot > curve.originAt && slot <= end,
  );
  if (future.length < D1_MIN_TRUTH_SLOTS) return;
  const worse = future.some(([, v]) => v >= live + D1_GAP_MINUTES);

  const suggested =
    curve.laterWait !== null &&
    curve.laterWait !== undefined &&
    curve.laterWait - live >= D1_GAP_MINUTES;
  let trigger: { lead: string; src: string } | null = null;
  if (suggested && curve.laterAt) {
    const idx = Math.round(
      (curve.laterAt - curve.slotStart) / (curve.slotMinutes * 60_000),
    );
    trigger = {
      lead:
        slotLeadBucket(Math.max(0, (curve.laterAt - curve.originAt) / 60_000))
          ?.lead ?? "h0-2",
      src: sourceName(curve.sources[idx]),
    };
  }

  const noHour = curve.laterWait === null || curve.laterWait === undefined;
  const counters: Record<string, number> = suggested
    ? { d1Sugg: 1, d1SuggOk: worse ? 1 : 0 }
    : noHour
      ? { d1NoHour: 1, d1NoHourWorse: worse ? 1 : 0 }
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

  // The routed H5 curve is only archived where it differs from plain H5 (a
  // TFT level applied); elsewhere it IS the plain curve, so the plain row is
  // scored a second time under the routed name and the two always cover the
  // same ride-days.
  const routed = new Set(
    input.curves
      .filter((c) => c.surface === "plan_day_shadow_routed")
      .map((c) => `${c.originAt}|${c.attractionId}`),
  );
  const curves = [
    ...input.curves,
    ...input.curves
      .filter(
        (c) =>
          c.surface === "plan_day_shadow_h5" &&
          !routed.has(`${c.originAt}|${c.attractionId}`),
      )
      .map((c) => ({ ...c, surface: "plan_day_shadow_routed" as const })),
  ];

  for (const curve of curves) {
    const truthSlots = truth.get(curve.attractionId);
    const segments = segmentsOf(curve);

    // An intraday planner row exists for D1 alone: its hours are the 2-hour
    // look-ahead, not a day to score.
    if (curve.surface === "plan_day" && curve.originKind === "intraday") {
      if (truthSlots) {
        scoreNextBestRide(curve, truthSlots, segments, region, acc);
      }
      continue;
    }

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
    const levelKey =
      curve.surface === "plan_day" && curve.levelSource
        ? `level_${curve.levelSource}`
        : null;
    // A composer A/B row (PAR-834): scored under its own use case and one
    // source per composer, and never into "all" — the three are the same
    // slots three times, and pooling them would triple-count the day. A
    // Chronos row (PAR-852) keeps the served use case but, likewise, its own
    // source and never "all".
    const composerShadow = SHADOW_SOURCES[curve.surface] ?? null;
    const shadowSource =
      composerShadow ?? CHRONOS_SOURCES[curve.surface] ?? null;
    if (shadowSource) {
      for (const c of compared) {
        c.src = shadowSource;
      }
    }
    // Scored on slot leads after the origin (UC1/UC2), not on day leads.
    const slotLeads =
      curve.surface === "park_hourly" || curve.surface === "chronos_live";
    // Surfaces whose `bands` hold a per-slot q95 − q50.
    const slotBand = slotLeads || curve.surface === "chronos_grid";

    // ---- slot metrics: MAE, bias, band coverage ----
    for (const c of compared) {
      let useCase: string;
      let lead: string;
      if (slotLeads) {
        const bucket = slotLeadBucket((c.slot - curve.originAt) / 60_000);
        if (!bucket) continue;
        useCase = bucket.useCase;
        lead = bucket.lead;
      } else {
        useCase = composerShadow ? SHADOW_USE_CASE : "UC3";
        lead = dayLead(curve.leadDays);
      }
      const err = c.pred - c.truth;
      const counters: Record<string, number> = {
        n: 1,
        sae: Math.abs(err),
        se: err,
      };
      // One-sided q95 − q50 band, per slot (plan_day has none per hour).
      if (c.band !== null && slotBand) {
        counters.nBand = 1;
        counters.nBandCov = c.truth <= c.pred + c.band ? 1 : 0;
      }
      for (const seg of segments) {
        acc.add(region, useCase, lead, c.src, seg, counters);
        if (shadowSource) continue;
        acc.add(region, useCase, lead, "all", seg, counters);
        if (levelKey && LEVEL_DERIVED.has(c.src)) {
          acc.add(region, useCase, lead, levelKey, seg, counters);
        }
      }
    }

    // ---- ride-day metrics: best time (D3), slot Spearman, dayPeak ----
    // Only a whole day is a ride-day: intraday origins see a few hours of it.
    if (curve.originKind === "intraday") continue;
    const useCase = slotLeads
      ? "UC2"
      : composerShadow
        ? SHADOW_USE_CASE
        : "UC3";
    const lead = dayLead(curve.leadDays);
    const srcs = new Set(compared.map((c) => c.src));
    const rideSource = srcs.size === 1 ? [...srcs][0] : "mixed";

    const shapeCounters: Record<string, number> = {};
    const best = bestTimeOutcome(compared);
    if (best) {
      shapeCounters.rd = 1;
      shapeCounters.regret = best.regret;
      if (best.hitTop2 === null) {
        shapeCounters.rdFlat = 1;
      } else {
        shapeCounters.rdHit = 1;
        shapeCounters.hitTop2 = best.hitTop2 ? 1 : 0;
        shapeCounters.hit30 = best.hit30 ? 1 : 0;
      }
    }
    const rho = spearman(
      compared.map((c) => c.pred),
      compared.map((c) => c.truth),
    );
    if (rho !== null && compared.length >= MIN_RIDE_DAY_SLOTS) {
      shapeCounters.nRho = 1;
      shapeCounters.sRho = rho;
    }

    const peakCounters: Record<string, number> = {};
    // The Chronos grid shadow carries a dayPeak too (its curve's P90), scored
    // under its own source; only the served plan feeds the ordering and D9.
    if (curve.surface === "plan_day" || curve.surface === "chronos_grid") {
      const served = curve.surface === "plan_day";
      const plan = planByOrigin.get(curve.originAt) ?? {
        leadDays: curve.leadDays,
        peaks: [],
        rides: new Set<string>(),
      };
      if (served) plan.rides.add(curve.attractionId);
      const p90 = rideDayTruthP90(truthSlots);
      if (curve.dayPeak !== null && p90 !== null) {
        const e = curve.dayPeak - p90;
        peakCounters.nPeak = 1;
        peakCounters.sPeakAe = Math.abs(e);
        peakCounters.sPeakE = e;
        if (curve.expectedError !== null) {
          peakCounters.nStated = 1;
          peakCounters.sStated = curve.expectedError;
          peakCounters.sStatedAe = Math.abs(e);
        }
        if (curve.peakBand !== null && curve.peakBand !== undefined) {
          peakCounters.nPeakBand = 1;
          // One-sided: the band is an upper half-width, so an over-forecast
          // (dayPeak above the truth) is always inside it.
          peakCounters.nPeakBandCov = -e <= curve.peakBand ? 1 : 0;
        }
        if (served) plan.peaks.push({ pred: curve.dayPeak, truth: p90 });
      }
      if (served) planByOrigin.set(curve.originAt, plan);
    }

    const rideCounters = { ...shapeCounters, ...peakCounters };
    if (Object.keys(rideCounters).length > 0) {
      for (const seg of segments) {
        acc.add(region, useCase, lead, rideSource, seg, rideCounters);
        if (shadowSource) continue;
        acc.add(region, useCase, lead, "all", seg, rideCounters);
        if (levelKey) {
          // dayPeak IS the level; the shape metrics only when the hours were.
          const levelCounters = LEVEL_DERIVED.has(rideSource)
            ? rideCounters
            : peakCounters;
          if (Object.keys(levelCounters).length > 0) {
            acc.add(region, useCase, lead, levelKey, seg, levelCounters);
          }
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
  if (input.calendarDayValue !== null) {
    for (const parkDay of input.parkDays) {
      const typical = parkDay.typicalDayPeak ?? 0;
      if (!(typical > 0)) continue;
      const ratio = (input.calendarDayValue / typical) * 100;
      const truthOrd = crowdOrdinal(determineCrowdLevel(ratio))!;
      const lead = dayLead(parkDay.leadDays);
      // The planner's crowdLevel is a forecast only where the calendar had one;
      // otherwise it is the fallback chain (ML crowd / placeholder moderate),
      // and on d0 it is overridden with the live reading at the origin.
      const planSource = parkDay.crowdLevelFallback
        ? "fallback"
        : parkDay.leadDays === 0
          ? "plan_day_live"
          : "plan_day";
      for (const [source, value] of [
        ["predicted", parkDay.predictedCrowdLevel],
        [planSource, parkDay.crowdLevel],
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
  return observations;
}

/** Additive paired counters of one shadow vs one reference. */
export interface PairedSums {
  n: number;
  saeModel: number;
  saeRef: number;
  seModel: number;
  seRef: number;
}

/**
 * Paired errors of a shadow and a reference on the same truth slots, keyed by
 * use case × lead × model × reference × segment, for ONE park and day — the
 * unit the board's park-cluster bootstrap resamples (PAR-852).
 */
export class PairedAccumulator {
  private readonly rows = new Map<string, PairedSums>();

  add(
    useCase: string,
    lead: string,
    model: string,
    reference: string,
    segment: string,
    pred: number,
    ref: number,
    truth: number,
  ): void {
    const key = [useCase, lead, model, reference, segment].join("|");
    const row = this.rows.get(key) ?? {
      n: 0,
      saeModel: 0,
      saeRef: 0,
      seModel: 0,
      seRef: 0,
    };
    row.n += 1;
    row.saeModel += Math.abs(pred - truth);
    row.saeRef += Math.abs(ref - truth);
    row.seModel += pred - truth;
    row.seRef += ref - truth;
    this.rows.set(key, row);
  }

  entries(): Array<
    {
      useCase: string;
      lead: string;
      model: string;
      reference: string;
      segment: string;
    } & PairedSums
  > {
    return [...this.rows.entries()].map(([key, sums]) => {
      const [useCase, lead, model, reference, segment] = key.split("|");
      return { useCase, lead, model, reference, segment, ...sums };
    });
  }
}

/**
 * The paired comparison of the Chronos-2 shadow (PAR-852) for one park-day:
 * on every truth slot where the shadow AND the reference have a value —
 *
 * - `chronos2` (live, intraday origins) vs `served` (park_hourly of the SAME
 *   origin instant: CatBoost/PCN as served) and vs `persistence` (the live
 *   STANDBY wait in force at the origin, held flat), on UC1/UC2 slot leads;
 * - `chronos2_grid` (06:00 origin) vs `served` (plan_day of the same origin,
 *   its hour value on each quarter-hour, as the D-scores read it) and vs `h5`
 *   (the composer shadow's plain H5 curve, surface 4), on UC3 day leads.
 *
 * Pairing on identical slots is what makes a difference of two MAEs mean
 * anything: each surface on its own coverage would compare different slots.
 */
export function scorePairedShadows(
  curves: ArchivedCurve[],
  truth: Map<string, Map<number, number>>,
  acc: PairedAccumulator,
): void {
  const byKey = new Map<string, ArchivedCurve>();
  for (const c of curves) {
    byKey.set(`${c.originAt}|${c.attractionId}|${c.surface}`, c);
  }
  for (const curve of curves) {
    const model = CHRONOS_SOURCES[curve.surface];
    if (!model) continue;
    const truthSlots = truth.get(curve.attractionId);
    if (!truthSlots) continue;
    const segments = segmentsOf(curve);
    const ref = (surface: ArchiveSurface) =>
      byKey.get(`${curve.originAt}|${curve.attractionId}|${surface}`);
    const live = curve.surface === "chronos_live";
    const refs: Array<{ name: string; curve: ArchivedCurve | undefined }> = live
      ? [{ name: "served", curve: ref("park_hourly") }]
      : [
          { name: "served", curve: ref("plan_day") },
          { name: "h5", curve: ref("plan_day_shadow_h5") },
        ];
    const anchor = live ? curve.liveWait : null;
    for (const [slot, value] of truthSlots) {
      const m = valueAt(curve, slot);
      if (!m) continue;
      let useCase = "UC3";
      let lead = dayLead(curve.leadDays);
      if (live) {
        const bucket = slotLeadBucket((slot - curve.originAt) / 60_000);
        if (!bucket) continue;
        useCase = bucket.useCase;
        lead = bucket.lead;
      }
      for (const r of refs) {
        const rv = r.curve ? valueAt(r.curve, slot) : null;
        if (!rv) continue;
        for (const seg of segments) {
          acc.add(useCase, lead, model, r.name, seg, m.wait, rv.wait, value);
        }
      }
      if (anchor !== null && anchor !== undefined) {
        for (const seg of segments) {
          acc.add(
            useCase,
            lead,
            model,
            "persistence",
            seg,
            m.wait,
            anchor,
            value,
          );
        }
      }
    }
  }
}

/**
 * Model names the generalised paired scorer (PAR-855) writes into
 * `forecast_archive_paired.model`, next to PAR-852's `chronos2` /
 * `chronos2_grid`. The admin model comparison maps them onto its matrix
 * columns (`model-comparison.util.ts`).
 */
export const PAIRED_MODELS = {
  persistence: "persistence",
  shadowH5: "shadow_h5",
  shadowRouted: "shadow_h5_routed",
  shadowPeak: "shadow_peak_scaled",
  chronosGrid: "chronos2_grid",
  weekdayAvg: "weekday_avg",
} as const;

/** Ride-day use cases of the paired table besides the slot ones (UC1/2/3). */
export const PAIRED_BEST_TIME_USE_CASE = "D3";
export const PAIRED_PEAK_USE_CASE = "PEAK";

const PAIRED_SHADOW_NAMES: Partial<Record<ArchiveSurface, string>> = {
  plan_day_shadow_h5: PAIRED_MODELS.shadowH5,
  plan_day_shadow_routed: PAIRED_MODELS.shadowRouted,
  plan_day_shadow_peak: PAIRED_MODELS.shadowPeak,
  chronos_grid: PAIRED_MODELS.chronosGrid,
};

/**
 * The weekday average's inputs for one park and target date: the rollup per
 * ride (date → forward-filled day, see `weekday-average.util.ts`) and the
 * park-local clock of a slot instant ("HH:MM").
 */
export interface WeekdayAverageInput {
  targetDate: string;
  clock: (slotMs: number) => string;
  history: Map<string, Map<string, DaySlots>>;
}

/**
 * The weekday-average curve that stands beside one archived plan_day curve:
 * same origin, same span, in quarter-hours. Built only from rollup dates
 * strictly before the origin's park-local date (targetDate − leadDays).
 */
export function weekdayAverageArchived(
  served: ArchivedCurve,
  weekday: WeekdayAverageInput,
): ArchivedCurve | null {
  const history = weekday.history.get(served.attractionId);
  if (!history || history.size === 0) return null;
  const quarters = Math.round((served.waits.length * served.slotMinutes) / 15);
  if (quarters <= 0) return null;
  const hhmms: string[] = [];
  for (let i = 0; i < quarters; i++) {
    hhmms.push(weekday.clock(served.slotStart + i * TRUTH_SLOT_MS));
  }
  const originDate = addIsoDays(weekday.targetDate, -served.leadDays);
  const waits = weekdayAverageCurve(
    history,
    weekday.targetDate,
    originDate,
    hhmms,
  );
  if (waits.every((w) => w === null)) return null;
  return {
    ...served,
    slotMinutes: 15,
    waits,
    sources: "",
    bands: null,
    dayPeak: null,
    expectedError: null,
    peakBand: null,
  };
}

/**
 * Generalised paired scoring (PAR-855): every model the admin comparison
 * shows, each paired with WHAT IS SERVED for that question on identical slots
 * or ride-days of the same origin instant. Written beside PAR-852's
 * {@link scorePairedShadows} rows, never over them: each (use case, lead,
 * model, reference, segment) key below is produced by exactly one branch, and
 * none of them is a key `scorePairedShadows` writes.
 *
 * | model | reference | use case × lead | origins |
 * |---|---|---|---|
 * | `persistence` (live wait at the origin, held flat) | `served` = park_hourly | UC1/UC2 slot leads | all |
 * | `shadow_h5`, `shadow_h5_routed`, `shadow_peak_scaled`, `chronos2_grid` | `served` = park_hourly | UC1/UC2 slot leads | 06:00 |
 * | `shadow_h5`, `shadow_h5_routed`, `shadow_peak_scaled`, `weekday_avg` | `served` = plan_day | UC3 dN | 06:00 / 07:00 |
 * | the same four + `chronos2_grid` | `served` = plan_day | D3 dN: best-time regret per ride-day, both on the same slots | 06:00 / 07:00 |
 * | `weekday_avg` | `served` = plan_day dayPeak | PEAK dN: \|dayPeak − truth P90\| per ride-day (`all`, `headliner`) | 06:00 / 07:00 |
 *
 * Segment `all` only (and `headliner` for PEAK, which the calendar reads):
 * the matrix shows nothing else, and three segments would triple the rows.
 * The routed H5 curve falls back to plain H5 where it was not archived, as in
 * {@link scoreParkDay}.
 */
export function scorePairedModels(
  curves: ArchivedCurve[],
  truth: Map<string, Map<number, number>>,
  acc: PairedAccumulator,
  weekday: WeekdayAverageInput | null = null,
): void {
  const byKey = new Map<string, ArchivedCurve>();
  for (const c of curves) {
    byKey.set(`${c.originAt}|${c.attractionId}|${c.surface}`, c);
  }
  const list = [...curves];
  for (const c of curves) {
    if (c.surface !== "plan_day_shadow_h5") continue;
    const k = `${c.originAt}|${c.attractionId}|plan_day_shadow_routed`;
    if (!byKey.has(k)) list.push({ ...c, surface: "plan_day_shadow_routed" });
  }
  const sibling = (c: ArchivedCurve, surface: ArchiveSurface) =>
    byKey.get(`${c.originAt}|${c.attractionId}|${surface}`);

  const pairSlots = (
    model: ArchivedCurve,
    ref: ArchivedCurve,
    truthSlots: Map<number, number>,
    name: string,
    slotLeads: boolean,
  ) => {
    for (const [slot, value] of truthSlots) {
      const m = valueAt(model, slot);
      if (!m) continue;
      const r = valueAt(ref, slot);
      if (!r) continue;
      let useCase = "UC3";
      let lead = dayLead(model.leadDays);
      if (slotLeads) {
        const bucket = slotLeadBucket((slot - model.originAt) / 60_000);
        if (!bucket) continue;
        useCase = bucket.useCase;
        lead = bucket.lead;
      }
      acc.add(useCase, lead, name, "served", "all", m.wait, r.wait, value);
    }
  };

  const pairBestTime = (
    model: ArchivedCurve,
    served: ArchivedCurve,
    truthSlots: Map<number, number>,
    name: string,
  ) => {
    const a: Compared[] = [];
    const b: Compared[] = [];
    for (const [slot, value] of truthSlots) {
      const m = valueAt(model, slot);
      const r = valueAt(served, slot);
      if (!m || !r) continue;
      a.push({ slot, pred: m.wait, truth: value, src: "", band: null });
      b.push({ slot, pred: r.wait, truth: value, src: "", band: null });
    }
    const om = bestTimeOutcome(a);
    const os = bestTimeOutcome(b);
    if (!om || !os) return;
    // n = 1 ride-day; Σ|e| of each is its regret (regret ≥ 0, truth 0).
    acc.add(
      PAIRED_BEST_TIME_USE_CASE,
      dayLead(served.leadDays),
      name,
      "served",
      "all",
      om.regret,
      os.regret,
      0,
    );
  };

  for (const c of list) {
    const truthSlots = truth.get(c.attractionId);
    if (!truthSlots || truthSlots.size === 0) continue;

    if (c.surface === "park_hourly") {
      const anchor = c.liveWait;
      if (anchor === null || anchor === undefined) continue;
      for (const [slot, value] of truthSlots) {
        const r = valueAt(c, slot);
        if (!r) continue;
        const bucket = slotLeadBucket((slot - c.originAt) / 60_000);
        if (!bucket) continue;
        acc.add(
          bucket.useCase,
          bucket.lead,
          PAIRED_MODELS.persistence,
          "served",
          "all",
          anchor,
          r.wait,
          value,
        );
      }
      continue;
    }

    if (c.originKind === "intraday") continue;

    if (c.surface === "plan_day") {
      if (!weekday) continue;
      const wkd = weekdayAverageArchived(c, weekday);
      if (!wkd) continue;
      pairSlots(wkd, c, truthSlots, PAIRED_MODELS.weekdayAvg, false);
      pairBestTime(wkd, c, truthSlots, PAIRED_MODELS.weekdayAvg);
      const p90 = rideDayTruthP90(truthSlots);
      const span: number[] = [];
      wkd.waits.forEach((w, i) => {
        if (w === null) return;
        if (valueAt(c, c.slotStart + i * TRUTH_SLOT_MS) === null) return;
        span.push(w);
      });
      const wkdPeak = percentileCont(span, 0.9);
      if (c.dayPeak !== null && p90 !== null && wkdPeak !== null) {
        const segs = c.isHeadliner ? ["all", "headliner"] : ["all"];
        for (const seg of segs) {
          acc.add(
            PAIRED_PEAK_USE_CASE,
            dayLead(c.leadDays),
            PAIRED_MODELS.weekdayAvg,
            "served",
            seg,
            wkdPeak,
            c.dayPeak,
            p90,
          );
        }
      }
      continue;
    }

    const name = PAIRED_SHADOW_NAMES[c.surface];
    if (!name) continue;
    const hourly = sibling(c, "park_hourly");
    if (hourly) pairSlots(c, hourly, truthSlots, name, true);
    const plan = sibling(c, "plan_day");
    if (plan) {
      // chronos2_grid vs plan_day on UC3 is PAR-852's own row.
      if (name !== PAIRED_MODELS.chronosGrid) {
        pairSlots(c, plan, truthSlots, name, false);
      }
      pairBestTime(c, plan, truthSlots, name);
    }
  }
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
