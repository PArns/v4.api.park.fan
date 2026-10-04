/**
 * Longest lead at which an `hourly` prediction still serves its `confidence`.
 *
 * `predict.py` builds `confidence` from a distance term (60 % weight) and a
 * spread term. The distance term falls from 95 to its floor of 50 at 22.5 h
 * and stays there, so 60.2 % of 453,076 served values sat at 50 or below
 * (PAR-449). It is not an error estimate: the measured error rises only about
 * 4.9 % over the first 24 h, and beyond 24 h it is not measured at all
 * (PAR-658). Past this lead the API serves `null` instead of the floor.
 */
export const HOURLY_CONFIDENCE_MAX_LEAD_MS = 24 * 60 * 60 * 1000;

/**
 * The `confidence` an `hourly` prediction carries when it leaves the API.
 *
 * The lead is counted from `now`, the moment of delivery, and not from the
 * row's `prediction_time`: a row generated 6 h ago for a slot 25 h out was a
 * 31 h forecast when written and is a 25 h one to the visitor reading it. The
 * stored value is untouched; only the served copy is nulled.
 *
 * Exactly 24 h still serves the value. A missing confidence stays missing.
 */
export function servedHourlyConfidence(
  confidence: number | null | undefined,
  predictedTime: string | Date,
  now: number = Date.now(),
): number | null {
  if (confidence === null || confidence === undefined) return null;
  const lead = new Date(predictedTime).getTime() - now;
  // An unparseable time gives NaN, and NaN > x is false: the value is kept,
  // which is what the code did before this rule existed.
  return lead > HOURLY_CONFIDENCE_MAX_LEAD_MS ? null : confidence;
}

/**
 * `confidenceAdjusted` for a prediction whose live wait deviated from it:
 * half the confidence, and `null` wherever the served confidence is `null`.
 * Only `hourly` rows are nulled past 24 h; `daily` rows are halved as before.
 */
export function servedAdjustedConfidence(
  prediction: {
    confidence: number | null | undefined;
    predictedTime: string | Date;
    predictionType: "hourly" | "daily";
  },
  now: number = Date.now(),
): number | null {
  const served =
    prediction.predictionType === "hourly"
      ? servedHourlyConfidence(
          prediction.confidence,
          prediction.predictedTime,
          now,
        )
      : (prediction.confidence ?? null);
  return served === null ? null : served * 0.5;
}
