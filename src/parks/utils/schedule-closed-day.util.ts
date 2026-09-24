/**
 * The calendar's rule for a day that has NO schedule entry of its own (or only
 * an UNKNOWN one): is it closed because of where it sits in the park's
 * operating history?
 *
 * - Between the first and the last OPERATING date the schedule knows → closed
 *   (a gap inside a published season).
 * - Before the first or after the last OPERATING date → closed ONLY for a
 *   seasonal park. A year-round park that has not published next month's hours
 *   yet keeps its forecast.
 *
 * `CalendarService.buildCalendarDay` and the yearly predictions
 * (`ParkIntegrationService.aggregateDailyPredictions`) both ask this, so the
 * two endpoints call the same day closed. Dates are park-local `YYYY-MM-DD`.
 */
export function isClosedByOperatingRange(
  dateStr: string,
  operatingDateRange: { minDate: string | null; maxDate: string | null },
  isSeasonal: boolean,
): boolean {
  const { minDate, maxDate } = operatingDateRange;
  if (!minDate || !maxDate) return false;
  if (dateStr > minDate && dateStr < maxDate) return true;
  return isSeasonal && (dateStr < minDate || dateStr > maxDate);
}
