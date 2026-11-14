/**
 * How accurate a trip region's forecast was: one figure per region for its
 * hub, the measured error of the day's peak wait at the headliners
 * ("Spitzenwartezeit"), averaged over the region's parks. Pure: the service
 * reads the rows and caches the answer. Contract:
 * docs/frontend/trip-regions.md#how-accurate-the-forecast-was-get-v1trip-regionsregionaccuracy.
 *
 * The measurement is the one the planner's "how accurate is this?" already
 * quotes (`totals.expectedError`, `/plan/day`'s `leadTimeMae`):
 * `prediction_lead_snapshots`, the served daily level of each headliner as it
 * was forecast a fixed number of days ahead, against the day's realised P90
 * (docs/ml/reader-facing-accuracy.md). Only the selection differs: the
 * region's parks, the last 30 measured days, one lead distance.
 */
import { addIsoDays } from "../common/utils/date.util";
import type { ParkDayError } from "../ml/services/prediction-lead-snapshot.service";

/**
 * The lead distance the figure is measured at: a forecast made a week
 * ahead, a typical look at the hub's calendar before a trip, and a bucket
 * the archive samples (`LEAD_BUCKETS`). The day-ahead figure would flatter
 * a page people plan weeks ahead with.
 */
export const ACCURACY_LEAD_DAYS = 7;

/** The measured days the figure spans, ending on the region's last measured day. */
export const ACCURACY_WINDOW_DAYS = 30;

/**
 * How far back the read reaches to find the window's last measured day: the
 * scoring job fills a day once it is over everywhere, so the newest day may
 * lag, and a region closed for a while still answers from before.
 */
export const ACCURACY_LOOKBACK_DAYS = 45;

/**
 * A park counts with at least this many scored rows in the window: about
 * three headliners for a week. Fewer is a ride or two on a few days, and a
 * park average of that would weigh as much as Magic Kingdom's month.
 */
export const ACCURACY_MIN_PARK_ROWS = 20;

/**
 * The figure needs measured days spread over at least half the window: a
 * week's figure moves with that week's weather and events.
 */
export const ACCURACY_MIN_SAMPLE_DAYS = 14;

/** The region's figure, as `/accuracy` serves it. */
export interface TripForecastAccuracy {
  /** The window's length in days. */
  days: number;
  /** The window's first and last park-local day; `to` is the last measured one. */
  from: string;
  to: string;
  /** How many days ahead the forecasts were made. */
  leadDays: number;
  /** The mean of the counted parks' mean absolute errors, in minutes, one decimal. */
  maeMinutes: number;
  /** The parks counted ({@link ACCURACY_MIN_PARK_ROWS}). */
  parks: number;
  /** The window's days with a scored row of a counted park. */
  sampleDays: number;
}

/**
 * The region's figure from its parks' daily errors, or null where too little
 * is measured: no counted park, fewer than `minScored` scored rows over the
 * counted parks (the floor the planner's own figure publishes at,
 * `PredictionLeadSnapshotService.MIN_SCORED`), or fewer than
 * {@link ACCURACY_MIN_SAMPLE_DAYS} measured days. A row after its park's
 * `yesterday` (park-local) is not a measured day and is ignored.
 */
export function regionForecastAccuracy(input: {
  parks: ReadonlyArray<{ id: string; yesterday: string }>;
  rows: readonly ParkDayError[];
  minScored: number;
}): TripForecastAccuracy | null {
  const yesterday = new Map(input.parks.map((p) => [p.id, p.yesterday]));
  const measured = input.rows.filter((r) => {
    const last = yesterday.get(r.parkId);
    return last !== undefined && r.targetDate <= last && r.scored > 0;
  });
  if (measured.length === 0) return null;
  const to = measured.reduce(
    (latest, r) => (r.targetDate > latest ? r.targetDate : latest),
    measured[0].targetDate,
  );
  const from = addIsoDays(to, -(ACCURACY_WINDOW_DAYS - 1));

  const byPark = new Map<
    string,
    { scored: number; errorSum: number; dates: Set<string> }
  >();
  for (const r of measured) {
    if (r.targetDate < from || r.targetDate > to) continue;
    const park = byPark.get(r.parkId) ?? {
      scored: 0,
      errorSum: 0,
      dates: new Set<string>(),
    };
    park.scored += r.scored;
    park.errorSum += r.errorSum;
    park.dates.add(r.targetDate);
    byPark.set(r.parkId, park);
  }
  const counted = [...byPark.values()].filter(
    (p) => p.scored >= ACCURACY_MIN_PARK_ROWS,
  );
  const scored = counted.reduce((sum, p) => sum + p.scored, 0);
  const sampleDays = new Set(counted.flatMap((p) => [...p.dates])).size;
  if (
    counted.length === 0 ||
    scored < input.minScored ||
    sampleDays < ACCURACY_MIN_SAMPLE_DAYS
  ) {
    return null;
  }
  const mae =
    counted.reduce((sum, p) => sum + p.errorSum / p.scored, 0) / counted.length;
  return {
    days: ACCURACY_WINDOW_DAYS,
    from,
    to,
    leadDays: ACCURACY_LEAD_DAYS,
    maeMinutes: Math.round(mae * 10) / 10,
    parks: counted.length,
    sampleDays,
  };
}
