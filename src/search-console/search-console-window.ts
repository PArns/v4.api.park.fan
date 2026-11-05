import {
  DEFAULT_WINDOW_DAYS,
  type SiteAnalyticsQuery,
} from "../site-analytics/site-analytics.query";
import { addIsoDays } from "../common/utils/date.util";

/**
 * The query of a rolling window (`days`, or neither `days` nor `from`/`to`)
 * moved to end at the last day with data, the way Search Console's own
 * "last 28 days" does: Google lags by days, so a window that ends today holds
 * fewer days of data than it names. An explicit `from`/`to` and a store
 * without data stay as asked.
 */
export function anchorRollingWindow(
  query: SiteAnalyticsQuery,
  lastDataDay: string | null,
): SiteAnalyticsQuery {
  if (query.from || query.to || !lastDataDay) return query;
  const days = query.days ?? DEFAULT_WINDOW_DAYS;
  return {
    ...query,
    days: null,
    from: addIsoDays(lastDataDay, -(days - 1)),
    to: lastDataDay,
  };
}
