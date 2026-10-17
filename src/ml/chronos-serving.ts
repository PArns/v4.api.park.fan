import type { Redis } from "ioredis";
import { determineCrowdLevel } from "../common/utils/crowd-level.util";
import type { PredictionDto } from "./dto";
import { roundServedWait } from "./pcn-serving.constants";

/**
 * Serving Chronos-2 on the live intraday surface (PAR-911), behind
 * `SERVE_CHRONOS_INTRADAY` (default OFF).
 *
 * Chronos-2 runs in nf-service as a shadow (PAR-852). The forward archive's
 * capture already asks it for each park's `live` curves at the intraday
 * origins; this module keeps the latest of those curves per ride in Redis so
 * the request path can serve them WITHOUT calling nf-service. The request path
 * reads one MGET; a stale or missing curve leaves the slot as it was.
 *
 * Lead split when the flag is on (lead = slot start − the current slot start,
 * the same base the §7.7 persistence blend uses):
 *
 * | lead | served |
 * | --- | --- |
 * | ≤ 30 min | unchanged — persistence/blend (persistence wins at 15 min, PAR-869) |
 * | 45 … 120 min | Chronos-2 q50 (band q95 − q50, crowd from q80) |
 * | > 120 min | unchanged — PCN + blend (or CatBoost) |
 *
 * See docs/ml/forward-archive.md §10.
 */

/** `SERVE_CHRONOS_INTRADAY=true` — read per call, like `SERVE_PCN_INTRADAY`. */
export function serveChronosIntraday(): boolean {
  return process.env.SERVE_CHRONOS_INTRADAY === "true";
}

/** Leads strictly above this are Chronos's (≤ 30 min stays persistence/blend). */
export const CHRONOS_SERVE_MIN_LEAD_MS = 30 * 60_000;
/** Leads up to and including this are Chronos's (beyond stays PCN). */
export const CHRONOS_SERVE_MAX_LEAD_MS = 120 * 60_000;
/**
 * A curve whose origin is older than this is not served. The cache is
 * refreshed every hour at :05 (+ up to ~15 min of capture) while the flag is
 * on, so a healthy curve is at most ~80 min old; 90 min only trips when a
 * refresh was missed (nf-service down, 503 during TFT training, breaker open)
 * — and the slot then falls back to PCN rather than serving a two-hour-old
 * nowcast.
 */
export const CHRONOS_MAX_CURVE_AGE_MS = 90 * 60_000;
/** Redis lifetime of a cached curve: past the guard, so the guard decides. */
export const CHRONOS_SERVE_TTL_S = 3 * 3600;
/**
 * Park-local hours at which a curve is produced: the intraday origins the
 * live A/B measured (10, 12 … 18) plus, while the flag is on, the odd hours
 * between them as refresh-only calls (no archive rows). Nothing outside the
 * measured 10–18 window is served — after ~19:30 a park falls back to PCN.
 */
export const CHRONOS_SERVE_HOURS: readonly number[] = [
  10, 11, 12, 13, 14, 15, 16, 17, 18,
];

export const chronosServeKey = (attractionId: string): string =>
  `ml:chronos-serve:${attractionId}`;

/** One contiguous run of 15-min slots (a curve per service day). */
export interface ChronosServeSegment {
  /** Epoch ms of the first slot. */
  slotStart: number;
  slotMinutes: number;
  q50: (number | null)[];
  q80: (number | null)[];
  q95: (number | null)[];
}

/** The cached curve of one ride, as written after a `live` call. */
export interface ChronosServeEntry {
  /** Epoch ms of the origin the curve was forecast from. */
  originAt: number;
  revision: string | null;
  segments: ChronosServeSegment[];
}

/** The subset of nf-service's curve the cache needs (structural). */
export interface ChronosCurveLike {
  attractionId: string;
  slotStart: string;
  slotMinutes: number;
  q50: (number | null)[];
  q80: (number | null)[];
  q95: (number | null)[];
}

/** Groups a `live` response's curves into one cache entry per ride. */
export function chronosServeEntries(
  curves: ChronosCurveLike[],
  originAt: Date,
  revision: string | null | undefined,
): Map<string, ChronosServeEntry> {
  const out = new Map<string, ChronosServeEntry>();
  for (const c of curves ?? []) {
    const start = Date.parse(c.slotStart);
    if (!Number.isFinite(start) || !Array.isArray(c.q50)) continue;
    if (c.q50.every((v) => v === null || !Number.isFinite(v))) continue;
    const entry = out.get(c.attractionId) ?? {
      originAt: originAt.getTime(),
      revision: revision ?? null,
      segments: [],
    };
    entry.segments.push({
      slotStart: start,
      slotMinutes: c.slotMinutes || 15,
      q50: c.q50,
      q80: c.q80 ?? [],
      q95: c.q95 ?? [],
    });
    out.set(c.attractionId, entry);
  }
  return out;
}

/** Writes the entries (one key per ride, pipelined). Never throws. */
export async function writeChronosServeCache(
  redis: Redis,
  entries: Map<string, ChronosServeEntry>,
): Promise<number> {
  if (entries.size === 0) return 0;
  try {
    const pipe = redis.pipeline();
    for (const [aid, entry] of entries) {
      pipe.set(
        chronosServeKey(aid),
        JSON.stringify(entry),
        "EX",
        CHRONOS_SERVE_TTL_S,
      );
    }
    await pipe.exec();
    return entries.size;
  } catch {
    return 0;
  }
}

/** The cached curves of these rides; a missing or unreadable key is absent. */
export async function readChronosServeCache(
  redis: Redis,
  attractionIds: string[],
): Promise<Map<string, ChronosServeEntry>> {
  const out = new Map<string, ChronosServeEntry>();
  if (attractionIds.length === 0) return out;
  try {
    const raw = await redis.mget(...attractionIds.map(chronosServeKey));
    attractionIds.forEach((aid, i) => {
      const v = raw?.[i];
      if (!v) return;
      try {
        const parsed = JSON.parse(v) as ChronosServeEntry;
        if (parsed && Array.isArray(parsed.segments)) out.set(aid, parsed);
      } catch {
        // A corrupt key is a missing curve.
      }
    });
  } catch {
    // Redis down → nothing served from Chronos; the slot keeps PCN/CatBoost.
  }
  return out;
}

/** The Chronos quantiles of one slot, or null when the curve has none. */
export function chronosSlot(
  entry: ChronosServeEntry,
  slotMs: number,
): { q50: number; q80: number | null; q95: number | null } | null {
  for (const s of entry.segments) {
    const step = s.slotMinutes * 60_000;
    const off = slotMs - s.slotStart;
    if (off < 0 || off % step !== 0) continue;
    const idx = off / step;
    if (idx >= s.q50.length) continue;
    const q50 = s.q50[idx];
    if (q50 === null || q50 === undefined || !Number.isFinite(q50)) continue;
    const num = (v: number | null | undefined) =>
      v === null || v === undefined || !Number.isFinite(v) ? null : v;
    return { q50, q80: num(s.q80?.[idx]), q95: num(s.q95?.[idx]) };
  }
  return null;
}

/**
 * Overrides the hourly rows in Chronos's lead window, in place, and returns
 * the rows it changed. Pure: the cache read happens in the caller.
 *
 * Per row: q50 → the shown wait (same 5-minute quantization CatBoost and PCN
 * get), q95 − q50 → `uncertaintyMinutes` (the one-sided upper half-width the
 * CatBoost band is), q80 → the crowd signal against the row's carried P50
 * baseline (ratio-vs-P50, the live regime of docs/rules/crowd-levels.md —
 * exactly the quantile split the PCN override uses, clamped so the crowd wait
 * is never below the shown one). A slot outside the window, a ride without a
 * curve, a curve older than {@link CHRONOS_MAX_CURVE_AGE_MS} or a slot the
 * curve has no value for (outside the published window) is left untouched.
 */
export function applyChronosOverride(
  preds: PredictionDto[],
  curves: Map<string, ChronosServeEntry>,
  slotBaseMs: number,
  nowMs: number,
): Set<PredictionDto> {
  const done = new Set<PredictionDto>();
  for (const p of preds) {
    if (p.predictionType !== "hourly") continue;
    const slotMs = Date.parse(p.predictedTime);
    if (!Number.isFinite(slotMs)) continue;
    const lead = slotMs - slotBaseMs;
    if (lead <= CHRONOS_SERVE_MIN_LEAD_MS || lead > CHRONOS_SERVE_MAX_LEAD_MS) {
      continue;
    }
    const entry = curves.get(p.attractionId);
    if (!entry || nowMs - entry.originAt > CHRONOS_MAX_CURVE_AGE_MS) continue;
    const q = chronosSlot(entry, slotMs);
    if (!q) continue;

    const display = Math.max(0, q.q50);
    p.predictedWaitTime = roundServedWait(display);
    if (q.q95 !== null) {
      p.uncertaintyMinutes = Math.max(0, Math.round(q.q95 - display));
    }
    if (p.baseline && p.baseline > 0) {
      const crowdWait = roundServedWait(Math.max(q.q80 ?? display, display));
      p.crowdLevel = determineCrowdLevel(
        (crowdWait / p.baseline) * 100,
      ) as PredictionDto["crowdLevel"];
    }
    p.modelVersion = `${stripServedSuffix(p.modelVersion) ?? "catboost"}+chronos`;
    done.add(p);
  }
  return done;
}

/** The CatBoost version without the served-model suffix (`+pcn`, `+chronos`). */
export function stripServedSuffix(v: string | null | undefined): string | null {
  if (!v) return v ?? null;
  return v.replace(/\+(pcn|chronos)$/, "");
}
