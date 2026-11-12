/**
 * How many days ahead the served daily forecast comes from TFT. Past it the
 * calendar's days are CatBoost's long tail, whose error is wider
 * (docs/ml/long-range-forecasting.md). `MLService.getServingDailyPredictions`
 * merges the two at this seam, and the trip-region calendar labels the days
 * beyond it `long_range`, so both read the one number.
 */
export const SERVING_TFT_DAYS = 60;
