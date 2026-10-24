/**
 * The Chronos-2 shadow's section of the forward-archive board (PAR-852): its
 * own MAE per use case × lead, and the PAIRED difference against each
 * reference with a 95 % park-cluster bootstrap CI — the PAR-828 bench's win
 * criterion, computed on live, leakage-free forecasts.
 *
 * Pure: the scoring service hands in the paired per-park-day rows of
 * `forecast_archive_paired` and the pooled board rows; nothing here reads a
 * database, so the spec pins the arithmetic.
 */

/** BENCH owner rule (2): live confirmation needs ≥ 14 days and ≥ 10 parks per lead. */
export const CHRONOS_MIN_DAYS = 14;
export const CHRONOS_MIN_PARKS = 10;
export const BOOTSTRAP_REPS = 1000;
/** Fixed, so the same rows always give the same CI (the board is cached anyway). */
export const BOOTSTRAP_SEED = 852;

/**
 * The days the live gate counts: the number of scored dates that at least
 * {@link CHRONOS_MIN_PARKS} parks EACH reached (the 10th-best park's count).
 * A union of dates over all parks let one park on 14 days plus nine on one
 * day each pass (PAR-1024). With fewer parks, the weakest park's count.
 */
export function gateDays(datesPerPark: Iterable<Set<string>>): number {
  const counts = [...datesPerPark].map((d) => d.size).sort((x, y) => y - x);
  if (counts.length === 0) return 0;
  return counts[Math.min(CHRONOS_MIN_PARKS, counts.length) - 1];
}

export interface PairedRow {
  targetDate: string;
  parkId: string;
  useCase: string;
  lead: string;
  model: string;
  reference: string;
  segment: string;
  n: number;
  saeModel: number;
  saeRef: number;
  seModel: number;
  seRef: number;
}

export type ChronosVerdict =
  | "insufficient_data"
  | "chronos_better"
  | "reference_better"
  | "no_significant_difference";

/** One shadow-vs-reference comparison on identical slots. */
export interface ChronosComparison {
  /** `served` (what users get on that surface), `persistence` (live wait held flat; live only), `h5` (plain H5 profile; grid only). */
  reference: string;
  /** Paired slots (truth, shadow and reference all present). */
  n: number;
  /** Scored dates reached by each of the 10 best-covered parks ({@link gateDays}), and parks contributing. */
  days: number;
  parks: number;
  /** MAE of each on the paired slots, minutes. */
  modelMae: number | null;
  referenceMae: number | null;
  /** Mean bias (pred − truth) of each on the paired slots. */
  modelBias: number | null;
  referenceBias: number | null;
  /** modelMae − referenceMae, minutes. NEGATIVE = Chronos is better. */
  diff: number | null;
  /** 95 % CI of `diff`, park-cluster bootstrap; null with fewer than 2 parks. */
  ciLow: number | null;
  ciHigh: number | null;
  /** ≥ {@link CHRONOS_MIN_PARKS} parks with ≥ {@link CHRONOS_MIN_DAYS} scored dates each. */
  enoughData: boolean;
  verdict: ChronosVerdict;
}

export interface ChronosBoardRow {
  useCase: string;
  lead: string;
  /** `chronos2` (live, intraday origins) or `chronos2_grid` (06:00 origin, d0–d7). */
  model: string;
  surface: "chronos_live" | "chronos_grid";
  segment: string;
  /** The shadow on its OWN coverage (from the pooled scores) — not comparable
   *  across models; compare through `comparisons`. */
  mae: number | null;
  bias: number | null;
  n: number;
  days: number;
  /** Share of slots with truth ≤ q50 + (q95 − q50). */
  bandCoverage: number | null;
  /** Grid only: |P90 of the curve − truth P90| per ride-day (the served
   *  plan's figure is `dayPeakMae` on the UC3 `all` row). */
  dayPeakMae: number | null;
  comparisons: ChronosComparison[];
}

export interface ChronosBoard {
  /** The API's `CHRONOS_SHADOW_ENABLED` at the time the board was built. */
  captureEnabled: boolean;
  method: {
    diff: string;
    ci: number;
    bootstrap: string;
    reps: number;
    seed: number;
    minDays: number;
    minParks: number;
  };
  rows: ChronosBoardRow[];
}

/** Minimal shape of a pooled board row this needs. */
interface PooledRow {
  useCase: string;
  lead: string;
  source: string;
  segment: string;
  days: number;
  metrics: Record<string, number | null>;
  sums: Record<string, number>;
}

const MODEL_SURFACE: Record<string, "chronos_live" | "chronos_grid"> = {
  chronos2: "chronos_live",
  chronos2_grid: "chronos_grid",
};

/** mulberry32 — small, seedable, good enough for resampling indices. */
export function seededRandom(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function quantile(sorted: number[], p: number): number {
  const pos = (sorted.length - 1) * p;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
}

const round = (v: number, d = 2) => Math.round(v * 10 ** d) / 10 ** d;

/**
 * 95 % CI of the pooled paired difference (ΣA − ΣB) / Σn by resampling PARKS
 * with replacement — days of one park are not independent, so resampling
 * park-days would make the interval too narrow (the bench's finding).
 */
export function parkClusterBootstrap(
  parks: Array<{ n: number; a: number; b: number }>,
  reps = BOOTSTRAP_REPS,
  seed = BOOTSTRAP_SEED,
): { low: number; high: number } | null {
  if (parks.length < 2) return null;
  const rnd = seededRandom(seed);
  const stats: number[] = [];
  for (let r = 0; r < reps; r++) {
    let n = 0;
    let d = 0;
    for (let i = 0; i < parks.length; i++) {
      const p = parks[Math.floor(rnd() * parks.length)];
      n += p.n;
      d += p.a - p.b;
    }
    if (n > 0) stats.push(d / n);
  }
  if (stats.length === 0) return null;
  stats.sort((x, y) => x - y);
  return { low: quantile(stats, 0.025), high: quantile(stats, 0.975) };
}

/** One park × date × stratum (lead) of paired sums, the unit {@link blockBootstrap} weights. */
export interface BootstrapCell {
  park: string;
  /** Target date; ignored by the park-only bootstrap. */
  date: string;
  /** The lead the cell belongs to; strata are weighted equally. */
  stratum: string;
  n: number;
  a: number;
  b: number;
}

/**
 * The statistic both bootstraps resample (PAR-1036): per stratum the pooled
 * paired difference (Σa − Σb) / Σn, then the plain mean over the strata that
 * have data — so a question's leads count equally however many rows each has.
 */
export function stratumMeanDiff(
  cells: Array<{ stratum: string; n: number; a: number; b: number }>,
  weight: (i: number) => number = () => 1,
): number | null {
  const n = new Map<string, number>();
  const d = new Map<string, number>();
  cells.forEach((c, i) => {
    const w = weight(i);
    if (w === 0) return;
    n.set(c.stratum, (n.get(c.stratum) ?? 0) + w * c.n);
    d.set(c.stratum, (d.get(c.stratum) ?? 0) + w * (c.a - c.b));
  });
  let sum = 0;
  let k = 0;
  for (const [s, ns] of n) {
    if (!(ns > 0)) continue;
    sum += d.get(s)! / ns;
    k++;
  }
  return k === 0 ? null : sum / k;
}

/**
 * 95 % CI of {@link stratumMeanDiff} (PAR-1036).
 *
 * - `twoWay: false` — park-cluster: parks are drawn with replacement, all of a
 *   drawn park's dates come along. With one stratum this is
 *   {@link parkClusterBootstrap}, draw for draw.
 * - `twoWay: true` — park × date: parks AND dates are drawn with replacement,
 *   independently, and each park × date cell counts with the product of the
 *   two multiplicities (the pigeonhole bootstrap, Owen 2007). A day that hit
 *   every park at once — a holiday, a heat wave, a feed outage — is then one
 *   draw rather than ten independent ones, which the park-only interval
 *   treats it as. Needs one row per park × date (× stratum).
 *
 * Null with fewer than 2 parks (or, two-way, fewer than 2 dates).
 */
export function blockBootstrap(
  cells: BootstrapCell[],
  opts: { twoWay: boolean },
  reps = BOOTSTRAP_REPS,
  seed = BOOTSTRAP_SEED,
): { low: number; high: number } | null {
  const parkIdx = new Map<string, number>();
  const dateIdx = new Map<string, number>();
  for (const c of cells) {
    if (!(c.n > 0)) continue;
    if (!parkIdx.has(c.park)) parkIdx.set(c.park, parkIdx.size);
    if (!dateIdx.has(c.date)) dateIdx.set(c.date, dateIdx.size);
  }
  const P = parkIdx.size;
  const D = dateIdx.size;
  if (P < 2 || (opts.twoWay && D < 2)) return null;
  // The park-only path pools each park × stratum first: it is the same
  // statistic, and the cost no longer grows with the number of dates.
  const units: Array<{ p: number; d: number } & BootstrapCell> = [];
  if (opts.twoWay) {
    for (const c of cells) {
      if (!(c.n > 0)) continue;
      units.push({ ...c, p: parkIdx.get(c.park)!, d: dateIdx.get(c.date)! });
    }
  } else {
    const pooled = new Map<string, { p: number; d: number } & BootstrapCell>();
    for (const c of cells) {
      if (!(c.n > 0)) continue;
      const k = `${c.park}|${c.stratum}`;
      const u = pooled.get(k) ?? {
        ...c,
        n: 0,
        a: 0,
        b: 0,
        p: parkIdx.get(c.park)!,
        d: 0,
      };
      u.n += c.n;
      u.a += c.a;
      u.b += c.b;
      pooled.set(k, u);
    }
    units.push(...pooled.values());
  }
  const rnd = seededRandom(seed);
  const wp = new Float64Array(P);
  const wd = new Float64Array(D);
  const stats: number[] = [];
  for (let r = 0; r < reps; r++) {
    wp.fill(0);
    for (let i = 0; i < P; i++) wp[Math.floor(rnd() * P)] += 1;
    if (opts.twoWay) {
      wd.fill(0);
      for (let i = 0; i < D; i++) wd[Math.floor(rnd() * D)] += 1;
    }
    const s = stratumMeanDiff(units, (i) =>
      opts.twoWay ? wp[units[i].p] * wd[units[i].d] : wp[units[i].p],
    );
    if (s !== null) stats.push(s);
  }
  if (stats.length === 0) return null;
  stats.sort((x, y) => x - y);
  return { low: quantile(stats, 0.025), high: quantile(stats, 0.975) };
}

export function compare(
  reference: string,
  rows: PairedRow[],
  seed = BOOTSTRAP_SEED,
): ChronosComparison {
  const perPark = new Map<string, { n: number; a: number; b: number }>();
  const datesPerPark = new Map<string, Set<string>>();
  let n = 0;
  let a = 0;
  let b = 0;
  let ea = 0;
  let eb = 0;
  for (const r of rows) {
    if (r.n <= 0) continue;
    const p = perPark.get(r.parkId) ?? { n: 0, a: 0, b: 0 };
    p.n += r.n;
    p.a += r.saeModel;
    p.b += r.saeRef;
    perPark.set(r.parkId, p);
    const d = datesPerPark.get(r.parkId) ?? new Set<string>();
    d.add(r.targetDate);
    datesPerPark.set(r.parkId, d);
    n += r.n;
    a += r.saeModel;
    b += r.saeRef;
    ea += r.seModel;
    eb += r.seRef;
  }
  const ci = parkClusterBootstrap([...perPark.values()], BOOTSTRAP_REPS, seed);
  const days = gateDays(datesPerPark.values());
  const enoughData =
    days >= CHRONOS_MIN_DAYS && perPark.size >= CHRONOS_MIN_PARKS;
  const diff = n > 0 ? (a - b) / n : null;
  let verdict: ChronosVerdict = "insufficient_data";
  if (enoughData && ci) {
    verdict =
      ci.high < 0
        ? "chronos_better"
        : ci.low > 0
          ? "reference_better"
          : "no_significant_difference";
  }
  return {
    reference,
    n,
    days,
    parks: perPark.size,
    modelMae: n > 0 ? round(a / n) : null,
    referenceMae: n > 0 ? round(b / n) : null,
    modelBias: n > 0 ? round(ea / n) : null,
    referenceBias: n > 0 ? round(eb / n) : null,
    diff: diff === null ? null : round(diff),
    ciLow: ci ? round(ci.low) : null,
    ciHigh: ci ? round(ci.high) : null,
    enoughData,
    verdict,
  };
}

/** A stable per-cell seed, so two cells never share one resampling sequence. */
function cellSeed(key: string): number {
  let h = BOOTSTRAP_SEED;
  for (let i = 0; i < key.length; i++)
    h = Math.imul(h ^ key.charCodeAt(i), 16777619);
  return h >>> 0;
}

const REFERENCE_ORDER = ["served", "persistence", "h5"];

function leadOrder(lead: string): number {
  if (lead.startsWith("h")) return Number(lead.slice(1).split("-")[0]);
  if (lead.startsWith("d")) return 1000 + Number(lead.slice(1));
  return 9999;
}

/**
 * The board section: one row per (use case, lead, model, segment) the shadow
 * was scored on, each with its comparisons. Ordered use case → lead → model →
 * segment.
 */
export function buildChronosBoard(
  paired: PairedRow[],
  pooled: PooledRow[],
  captureEnabled: boolean,
): ChronosBoard {
  const cells = new Map<
    string,
    { row: ChronosBoardRow; byRef: Map<string, PairedRow[]> }
  >();
  const cellOf = (
    useCase: string,
    lead: string,
    model: string,
    segment: string,
  ) => {
    const key = [useCase, lead, model, segment].join("|");
    let c = cells.get(key);
    if (!c) {
      c = {
        row: {
          useCase,
          lead,
          model,
          surface: MODEL_SURFACE[model] ?? "chronos_live",
          segment,
          mae: null,
          bias: null,
          n: 0,
          days: 0,
          bandCoverage: null,
          dayPeakMae: null,
          comparisons: [],
        },
        byRef: new Map(),
      };
      cells.set(key, c);
    }
    return c;
  };

  for (const p of pooled) {
    if (!MODEL_SURFACE[p.source]) continue;
    // Slot rows only; the ride-day rows (D3, dayPeak) share the key but carry
    // other counters — their MAE fields are null.
    const c = cellOf(p.useCase, p.lead, p.source, p.segment);
    if (p.metrics.mae !== null && p.metrics.mae !== undefined) {
      c.row.mae = p.metrics.mae;
      c.row.bias = p.metrics.bias ?? null;
      c.row.n = p.sums.n ?? 0;
      c.row.bandCoverage = p.metrics.bandCoverage ?? null;
    }
    if (p.metrics.dayPeakMae !== null && p.metrics.dayPeakMae !== undefined) {
      c.row.dayPeakMae = p.metrics.dayPeakMae;
    }
    c.row.days = Math.max(c.row.days, p.days);
  }
  for (const r of paired) {
    const c = cellOf(r.useCase, r.lead, r.model, r.segment);
    const list = c.byRef.get(r.reference) ?? [];
    list.push(r);
    c.byRef.set(r.reference, list);
  }
  for (const [key, c] of cells) {
    c.row.comparisons = [...c.byRef.entries()]
      .sort(
        (x, y) => REFERENCE_ORDER.indexOf(x[0]) - REFERENCE_ORDER.indexOf(y[0]),
      )
      .map(([ref, rows]) => compare(ref, rows, cellSeed(`${key}|${ref}`)));
  }
  const rows = [...cells.values()]
    .map((c) => c.row)
    .sort(
      (a, b) =>
        a.useCase.localeCompare(b.useCase) ||
        leadOrder(a.lead) - leadOrder(b.lead) ||
        a.model.localeCompare(b.model) ||
        a.segment.localeCompare(b.segment),
    );
  return {
    captureEnabled,
    method: {
      diff: "model MAE − reference MAE on identical slots, minutes; negative = Chronos better",
      ci: 0.95,
      bootstrap: "park-cluster (parks resampled with replacement)",
      reps: BOOTSTRAP_REPS,
      seed: BOOTSTRAP_SEED,
      minDays: CHRONOS_MIN_DAYS,
      minParks: CHRONOS_MIN_PARKS,
    },
    rows,
  };
}
