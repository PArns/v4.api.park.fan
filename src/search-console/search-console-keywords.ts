import { addIsoDays } from "../common/utils/date.util";

/**
 * The keyword report's arithmetic, apart from the database: which week, how visible, and how
 * each keyword moved. The service reads the sums; everything here is pure.
 */

/** A keyword's figures in one week; `position` is null when it was not shown at all. */
export interface KeywordWeek {
  clicks: number;
  impressions: number;
  position: number | null;
}

/** How a keyword moved against the week before. */
export type KeywordTrend = "up" | "down" | "same" | "new" | "lost";

/** One monitored keyword. */
export interface KeywordRow {
  query: string;
  /** The path that drew the most impressions, this week or, for a lost keyword, the week before. */
  page: string | null;
  current: KeywordWeek;
  previous: KeywordWeek;
  /** Places gained (positive) or lost (negative); null unless shown in both weeks. */
  change: number | null;
  trend: KeywordTrend;
}

/** The week's summary over the monitored keywords. */
export interface KeywordSummary {
  keywords: number;
  /** Share of the clicks the keywords would get at position 1, in percent; see `visibility`. */
  visibility: { current: number; previous: number; change: number };
  top3: { current: number; previous: number };
  top10: { current: number; previous: number };
  risers: { keywords: number; places: number };
  fallers: { keywords: number; places: number };
  new: number;
  lost: number;
}

/** A move under one place is noise of the average, not a rise or a fall. */
const MIN_MOVE = 1;

/**
 * The share of searchers who click a result at each position, the common curve from published
 * click-through studies: 28 % at 1, falling to 2 % at 10, 1 % on page two, nothing after.
 */
const CLICK_CURVE = [
  0.28, 0.15, 0.11, 0.08, 0.06, 0.05, 0.04, 0.03, 0.025, 0.02,
];

/** The expected share of clicks at an average position (rounded to the nearest place). */
export function clickShare(position: number | null): number {
  if (position === null) return 0;
  const place = Math.max(1, Math.round(position));
  if (place <= CLICK_CURVE.length) return CLICK_CURVE[place - 1];
  return place <= 20 ? 0.01 : 0;
}

/**
 * The last complete week, Monday to Sunday, that ends on or before `lastDay` (the last day
 * Search Console has), and the week before it. `week` picks an earlier week by any day in it.
 */
export function keywordWeeks(
  lastDay: string,
  week?: string | null,
): {
  current: { from: string; to: string };
  previous: { from: string; to: string };
} {
  const anchor = week && week < lastDay ? week : lastDay;
  const weekday = new Date(`${anchor}T00:00:00Z`).getUTCDay();
  // A week asked for by a day in it ends on its Sunday, unless that Sunday has no data yet.
  let sunday = weekday === 0 ? anchor : addIsoDays(anchor, 7 - weekday);
  if (sunday > lastDay) sunday = addIsoDays(sunday, -7);
  const monday = addIsoDays(sunday, -6);
  return {
    current: { from: monday, to: sunday },
    previous: { from: addIsoDays(monday, -7), to: addIsoDays(sunday, -7) },
  };
}

/** A keyword's change and trend from its two weeks. */
export function keywordMove(
  current: KeywordWeek,
  previous: KeywordWeek,
): { change: number | null; trend: KeywordTrend } {
  if (current.position === null) return { change: null, trend: "lost" };
  if (previous.position === null) return { change: null, trend: "new" };
  const change = Math.round((previous.position - current.position) * 10) / 10;
  if (change >= MIN_MOVE) return { change, trend: "up" };
  if (change <= -MIN_MOVE) return { change, trend: "down" };
  return { change, trend: "same" };
}

/**
 * The summary over the keywords. Visibility weighs each keyword by its impressions over both
 * weeks, so a keyword counts the same in both and the two weeks compare: 100 % would be every
 * keyword at position 1.
 */
export function keywordSummary(rows: KeywordRow[]): KeywordSummary {
  let possible = 0;
  let visibleNow = 0;
  let visibleBefore = 0;
  const summary: KeywordSummary = {
    keywords: rows.length,
    visibility: { current: 0, previous: 0, change: 0 },
    top3: { current: 0, previous: 0 },
    top10: { current: 0, previous: 0 },
    risers: { keywords: 0, places: 0 },
    fallers: { keywords: 0, places: 0 },
    new: 0,
    lost: 0,
  };
  const within = (position: number | null, limit: number) =>
    position !== null && Math.round(position) <= limit;
  for (const row of rows) {
    const weight = row.current.impressions + row.previous.impressions;
    possible += weight * clickShare(1);
    visibleNow += weight * clickShare(row.current.position);
    visibleBefore += weight * clickShare(row.previous.position);
    if (within(row.current.position, 3)) summary.top3.current += 1;
    if (within(row.previous.position, 3)) summary.top3.previous += 1;
    if (within(row.current.position, 10)) summary.top10.current += 1;
    if (within(row.previous.position, 10)) summary.top10.previous += 1;
    if (row.trend === "up") {
      summary.risers.keywords += 1;
      summary.risers.places += row.change ?? 0;
    } else if (row.trend === "down") {
      summary.fallers.keywords += 1;
      summary.fallers.places += -(row.change ?? 0);
    } else if (row.trend === "new") summary.new += 1;
    else if (row.trend === "lost") summary.lost += 1;
  }
  const percent = (value: number) =>
    possible > 0 ? Math.round((value / possible) * 10000) / 100 : 0;
  summary.visibility = {
    current: percent(visibleNow),
    previous: percent(visibleBefore),
    change:
      Math.round((percent(visibleNow) - percent(visibleBefore)) * 100) / 100,
  };
  summary.risers.places = Math.round(summary.risers.places * 10) / 10;
  summary.fallers.places = Math.round(summary.fallers.places * 10) / 10;
  return summary;
}

/** The keywords that moved most, each way, best first. */
export function keywordMovers(
  rows: KeywordRow[],
  limit = 10,
): { risers: KeywordRow[]; fallers: KeywordRow[] } {
  const moved = (trend: KeywordTrend) =>
    rows
      .filter((row) => row.trend === trend)
      .sort((a, b) => Math.abs(b.change ?? 0) - Math.abs(a.change ?? 0))
      .slice(0, limit);
  return { risers: moved("up"), fallers: moved("down") };
}
