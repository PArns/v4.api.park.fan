/**
 * Pure helpers of the admin day comparison (PAR-855 follow-up): which days are
 * over, which park and ride make a useful default, and which quarter-hours the
 * day view lays out. No I/O; the service feeds them.
 */
import { formatInTimeZone } from "date-fns-tz";
import {
  ArchivedCurve,
  OperatingWindow,
  TRUTH_SLOT_MS,
} from "./forecast-archive-scoring.util";
import {
  ModelComparisonDayOptionsDto,
  ModelComparisonDaySuggestionDto,
} from "../dto/model-comparison.dto";

/**
 * A ride "has truth in most slots" at this share of its window. Parks are
 * ranked by how many rides clear it, and only such a ride is suggested.
 */
export const RICH_TRUTH_COVERAGE = 0.5;
/** Padding around the truth span when the park published no hours. */
export const TRUTH_SPAN_PAD_MS = 3_600_000;

/**
 * The newest date that is over in every one of `timezones` — the last
 * complete park-local day everywhere. A date D is complete when D is strictly
 * before this cutoff's successor, i.e. `D <= lastCompleteDate`.
 */
export function lastCompleteDate(timezones: string[], now: Date): string {
  const localTodays = timezones.map((tz) =>
    formatInTimeZone(now, tz, "yyyy-MM-dd"),
  );
  // ISO dates compare lexically.
  const earliest = localTodays.length
    ? localTodays.reduce((a, b) => (b < a ? b : a))
    : formatInTimeZone(now, "UTC", "yyyy-MM-dd");
  const d = new Date(`${earliest}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}

/** One ride of the chosen date with what the day view could show for it. */
export interface DayOptionRow {
  parkId: string;
  parkName: string;
  timezone: string;
  attractionId: string;
  name: string;
  isHeadliner: boolean;
  /** Distinct archived surfaces at the morning origin (lead 0). */
  models: number;
  /**
   * Share of the operating window (the published hours, else the park-local
   * day) with an OPERATING STANDBY reading ≥ 5 min in force (≤ 3 h old).
   */
  coverage: number;
}

/**
 * Parks ordered by data richness — rides with truth in at least
 * {@link RICH_TRUTH_COVERAGE} of the window, then rides with any truth, then
 * name — and their rides headliners first, then by truth coverage. Rides
 * without any OPERATING truth that day are left out, and a park left without
 * rides with them.
 *
 * `suggested` is the first ride, in that order, that clears
 * {@link RICH_TRUTH_COVERAGE} and has curves from as many models as any ride
 * that clears it; null when no ride clears it.
 */
export function rankDayOptions(
  date: string,
  rows: DayOptionRow[],
): {
  parks: ModelComparisonDayOptionsDto["parks"];
  suggested: ModelComparisonDaySuggestionDto | null;
} {
  const byPark = new Map<string, DayOptionRow[]>();
  for (const r of rows) {
    if (!(r.coverage > 0)) continue;
    const list = byPark.get(r.parkId) ?? [];
    list.push(r);
    byPark.set(r.parkId, list);
  }
  const rideOrder = (a: DayOptionRow, b: DayOptionRow) =>
    Number(b.isHeadliner) - Number(a.isHeadliner) ||
    b.coverage - a.coverage ||
    a.name.localeCompare(b.name, "de");
  const ranked = [...byPark.values()]
    .map((rides) => ({
      rides: [...rides].sort(rideOrder),
      rich: rides.filter((r) => r.coverage >= RICH_TRUTH_COVERAGE).length,
    }))
    .sort(
      (a, b) =>
        b.rich - a.rich ||
        b.rides.length - a.rides.length ||
        a.rides[0].parkName.localeCompare(b.rides[0].parkName, "de"),
    );

  const candidates = ranked.flatMap((p) =>
    p.rides.filter((r) => r.coverage >= RICH_TRUTH_COVERAGE),
  );
  const maxModels = candidates.reduce((m, r) => Math.max(m, r.models), 0);
  const pick = candidates.find((r) => r.models === maxModels) ?? null;

  return {
    parks: ranked.map(({ rides }) => ({
      id: rides[0].parkId,
      name: rides[0].parkName,
      timezone: rides[0].timezone,
      rides: rides.map((r) => ({
        id: r.attractionId,
        name: r.name,
        isHeadliner: r.isHeadliner,
      })),
    })),
    suggested: pick
      ? { date, parkId: pick.parkId, attractionId: pick.attractionId }
      : null,
  };
}

function quarterHours(from: number, to: number): number[] {
  const out: number[] = [];
  for (
    let s = Math.floor(from / TRUTH_SLOT_MS) * TRUTH_SLOT_MS;
    s < to;
    s += TRUTH_SLOT_MS
  ) {
    out.push(s);
  }
  return out;
}

/**
 * The day view's 15-min slot instants, the operating window only:
 *
 * 1. the published window(s), first opening (floored) to last closing;
 * 2. without published hours, the first to the last slot with OPERATING
 *    truth, padded by {@link TRUTH_SPAN_PAD_MS} on both sides;
 * 3. without truth either, the span where the served curve has a value.
 *
 * Never a fixed clock window.
 */
export function dayViewSlots(input: {
  windows: OperatingWindow[];
  truthSlots: number[];
  served: ArchivedCurve | null;
}): number[] {
  const { windows, truthSlots, served } = input;
  if (windows.length > 0) {
    return quarterHours(
      Math.min(...windows.map((w) => w.open)),
      Math.max(...windows.map((w) => w.close)),
    );
  }
  if (truthSlots.length > 0) {
    return quarterHours(
      Math.min(...truthSlots) - TRUTH_SPAN_PAD_MS,
      Math.max(...truthSlots) + TRUTH_SLOT_MS + TRUTH_SPAN_PAD_MS,
    );
  }
  if (served) {
    const step = served.slotMinutes * 60_000;
    let first = -1;
    let last = -1;
    served.waits.forEach((w, i) => {
      if (w === null || w === undefined) return;
      if (first < 0) first = i;
      last = i;
    });
    if (first >= 0) {
      return quarterHours(
        served.slotStart + first * step,
        served.slotStart + (last + 1) * step,
      );
    }
  }
  return [];
}

/** Share of slots with a truth value, 3 decimals; 0 without slots. */
export function truthCoverage(truth: (number | null)[]): number {
  if (truth.length === 0) return 0;
  const n = truth.filter((t) => t !== null).length;
  return Math.round((n / truth.length) * 1000) / 1000;
}
