import { ObjectLiteral, SelectQueryBuilder } from "typeorm";
import { formatInParkTimezone } from "./date.util";
import { liveDataCutoff } from "./live-data.util";

/**
 * Shared query shapes for the live-data hypertables (show_live_data,
 * restaurant_live_data). Both domains need "the latest row per entity" and
 * "the latest row per entity that belongs to today in the park's timezone";
 * the only differences are the entity alias and id column.
 */

/**
 * Narrows a live-data query to the latest row per entity:
 * bounded by the live-data cutoff (enables TimescaleDB chunk exclusion),
 * DISTINCT ON the entity id, newest timestamp first.
 */
export function applyLatestPerEntity<T extends ObjectLiteral>(
  qb: SelectQueryBuilder<T>,
  alias: string,
  idColumn: string,
): SelectQueryBuilder<T> {
  return qb
    .andWhere(`${alias}.timestamp >= :cutoff`, { cutoff: liveDataCutoff() })
    .distinctOn([`${alias}.${idColumn}`])
    .orderBy(`${alias}.${idColumn}`, "ASC")
    .addOrderBy(`${alias}.timestamp`, "DESC");
}

/**
 * From rows ordered newest-first, keeps the latest row per entity whose
 * timestamp falls on "today" in the park's timezone.
 *
 * Used when a park is CLOSED to recover the day's schedule: the DB query
 * fetches a >24h lookback window, and this filters it down to the park-local
 * calendar day (a UTC window can't express "today in park time" directly).
 */
export function latestTodayPerEntity<T extends { timestamp: Date }>(
  rows: T[],
  getEntityId: (row: T) => string,
  timezone: string,
): Map<string, T> {
  const todayInParkTz = formatInParkTimezone(new Date(), timezone);
  const result = new Map<string, T>();

  for (const row of rows) {
    if (result.has(getEntityId(row))) continue;
    if (formatInParkTimezone(row.timestamp, timezone) === todayInParkTz) {
      result.set(getEntityId(row), row);
    }
  }

  return result;
}

/**
 * Lookback window for the "today in park time" recovery queries: 24h plus
 * buffer so the window always covers the park-local day regardless of offset.
 */
export const TODAY_LOOKBACK_HOURS = 26;

export function todayLookbackDate(now: Date = new Date()): Date {
  return new Date(now.getTime() - TODAY_LOOKBACK_HOURS * 60 * 60 * 1000);
}
