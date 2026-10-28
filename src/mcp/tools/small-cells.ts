import { OVERFLOW_VALUE } from "../../site-analytics/site-analytics.constants";

/**
 * Small values in what the site-statistic tools answer. A list of values with
 * few views, visits or events says more than the totals do: the rare path, the
 * unusual browser, the one country, a handful of readers. The admin's reports
 * show every number; the tools, whose answers a model reads and may repeat
 * elsewhere, fold the small ones into one `(other)` row and hide the small
 * values of a series. docs/admin/mcp.md.
 */

/** A row, cell or series value counting fewer than this is small. */
export const MCP_MIN_CELL = 5;

/** The row the small ones are folded into: the label the day's guard uses too. */
export const OTHER_ROW = OVERFLOW_VALUE;

/** One sentence for the descriptions of the tools whose lists fold. */
export const SMALL_CELLS_DESCRIPTION =
  `Values counted fewer than ${MCP_MIN_CELL} times are folded into one "${OTHER_ROW}" row ` +
  "(the counts added up, rates, averages and durations null); totals are complete, but under a " +
  `filter or a language a total from 1 to ${MCP_MIN_CELL - 1} is null and smallValuesHidden says so.`;

/** The sentence for the crosstabs, whose small values are cells as well as rows. */
export const CROSSTAB_SMALL_CELLS =
  `A value on a side whose cells add up to fewer than ${MCP_MIN_CELL} is folded into "${OTHER_ROW}" ` +
  `on that side (a day side keeps every period); a cell below ${MCP_MIN_CELL} is null in counts and ` +
  "counted in other: { cells, count }; the total is complete, but under a filter or a language a total from 1 to " +
  `${MCP_MIN_CELL - 1} is null and smallValuesHidden says so.`;

/** One sentence for the tools that draw a series by something. */
export const SMALL_SERIES_DESCRIPTION =
  `In a series under a filter or a language, or per event or surface, values from 1 to ${MCP_MIN_CELL - 1} ` +
  "are null, and smallValuesHidden says whether any were.";

type Row = Record<string, unknown>;

/** A row whose every field may be null: the folded one leaves what it cannot sum empty. */
export type Foldable<R> = { [K in keyof R]: R[K] | null };

/** How a list is read: what names a row, what counts it, what adds up. */
export interface FoldSpec {
  /** The keys that name a row; the folded row carries `(other)` in each. */
  dims: readonly string[];
  /**
   * The key of the row's count: views, visits or events. A row with
   * `visitors` is counted by them instead.
   */
  count: string;
  /**
   * The measures that add up, summed onto the folded row (null if one of its
   * rows has none). Every other key is left null on it: an average, a rate or
   * a duration of the pieces is not that of the whole, and a new field must not
   * carry one row's value onto the others'.
   */
  sum: readonly string[];
}

/** What decides whether a row is small: its visitors, else its own count. */
function countOf(row: Row, spec: FoldSpec): number {
  const visitors = row.visitors;
  const value = typeof visitors === "number" ? visitors : row[spec.count];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

/** A row that already stands for the values past the day's guard. */
function isOther(row: Row, spec: FoldSpec): boolean {
  return spec.dims.every((dim) => row[dim] === OTHER_ROW);
}

/**
 * The rows with every one that counts fewer than {@link MCP_MIN_CELL} folded
 * into one `(other)` row at the end: its names are `(other)`, its additive
 * measures the sums and the rest null. A row that is already `(other)` (the
 * values past the day's guard) is folded into it, so there is one. Rows that
 * are not small, and a list without small rows, come back as they are.
 */
export function foldSmallRows<R extends object>(
  rows: readonly R[],
  spec: FoldSpec,
): Array<Foldable<R>> {
  const plain = rows as readonly Row[];
  const small = plain.filter(
    (row) => countOf(row, spec) < MCP_MIN_CELL && !isOther(row, spec),
  );
  if (small.length === 0) return [...rows];
  const existing = plain.find((row) => isOther(row, spec));
  const folded = existing ? [...small, existing] : small;
  const kept = plain.filter((row) => !folded.includes(row));

  const keys = [...new Set(folded.flatMap((row) => Object.keys(row)))];
  const other: Row = {};
  for (const key of keys) {
    if (spec.dims.includes(key)) {
      other[key] = OTHER_ROW;
    } else if (spec.sum.includes(key)) {
      const values = folded.map((row) => row[key]);
      other[key] = values.every((value) => typeof value === "number")
        ? (values as number[]).reduce((sum, value) => sum + value, 0)
        : null;
    } else {
      other[key] = null;
    }
  }
  return [...kept, other] as Array<Foldable<R>>;
}

/** A map of value → count, folded like a list: a page's reads per language. */
export function foldSmallCounts(
  counts: Readonly<Record<string, number>>,
): Record<string, number> {
  const rows = foldSmallRows(
    Object.entries(counts).map(([value, count]) => ({ value, count })),
    { dims: ["value"], count: "count", sum: ["count"] },
  );
  return Object.fromEntries(rows.map((row) => [row.value, row.count]));
}

/** One non-zero cell of a crosstab. */
export interface CrosstabCell {
  row: string;
  col: string;
  count: number;
}

/**
 * A crosstab as the tools answer it: the sides folded like lists (a value
 * whose cells add up to fewer than {@link MCP_MIN_CELL} goes under `(other)`,
 * a `day` side keeps every period), then every cell below the limit left
 * null in the matrix, with how many there are and their sum in `other`. The
 * numbers in the matrix and `other` add up to the total.
 */
export function foldCrosstab(
  rows: readonly string[],
  cols: readonly string[],
  cells: readonly CrosstabCell[],
  folds: { row: boolean; col: boolean },
): {
  rows: string[];
  cols: string[];
  counts: Array<Array<number | null>>;
  other: { cells: number; count: number } | null;
} {
  const side = (
    values: readonly string[],
    pick: (cell: CrosstabCell) => string,
    fold: boolean,
  ) => {
    if (!fold) return { kept: [...values], label: (value: string) => value };
    const totals = new Map<string, number>();
    for (const cell of cells) {
      totals.set(pick(cell), (totals.get(pick(cell)) ?? 0) + cell.count);
    }
    const listed = foldSmallRows(
      values.map((value) => ({ value, count: totals.get(value) ?? 0 })),
      { dims: ["value"], count: "count", sum: ["count"] },
    ).map((row) => row.value as string);
    const named = new Set(listed);
    return {
      kept: listed,
      label: (value: string) => (named.has(value) ? value : OTHER_ROW),
    };
  };
  const rowSide = side(rows, (cell) => cell.row, folds.row);
  const colSide = side(cols, (cell) => cell.col, folds.col);

  const merged = new Map<string, CrosstabCell>();
  for (const cell of cells) {
    const row = rowSide.label(cell.row);
    const col = colSide.label(cell.col);
    const key = JSON.stringify([row, col]);
    const entry = merged.get(key) ?? { row, col, count: 0 };
    entry.count += cell.count;
    merged.set(key, entry);
  }

  const rowAt = new Map(rowSide.kept.map((value, i) => [value, i]));
  const colAt = new Map(colSide.kept.map((value, i) => [value, i]));
  const counts: Array<Array<number | null>> = rowSide.kept.map(() =>
    colSide.kept.map(() => 0),
  );
  const other = { cells: 0, count: 0 };
  for (const cell of merged.values()) {
    const i = rowAt.get(cell.row);
    const j = colAt.get(cell.col);
    if (i === undefined || j === undefined) continue;
    if (cell.count < MCP_MIN_CELL) {
      counts[i][j] = null;
      other.cells += 1;
      other.count += cell.count;
    } else {
      counts[i][j] = cell.count;
    }
  }
  return {
    rows: rowSide.kept,
    cols: colSide.kept,
    counts,
    other: other.cells > 0 ? other : null,
  };
}

/** Whether a tool's window is narrowed by a filter or a language. */
export function isNarrowed(input: {
  filters?: readonly unknown[];
  locale?: unknown;
}): boolean {
  return Boolean(input.filters?.length) || Boolean(input.locale);
}

/**
 * A series with its small counts hidden: every count from 1 to
 * {@link MCP_MIN_CELL} − 1 becomes null, and so does every figure derived from
 * one (`derived` names the counts each is divided by, or computed from). A
 * zero stays: no reader is behind it. `hidden` says whether anything changed.
 */
export function hideSmallInSeries<P extends object>(
  series: readonly P[],
  counts: readonly string[],
  derived: Readonly<Record<string, readonly string[]>> = {},
): { series: P[]; hidden: boolean } {
  const small = (value: unknown) =>
    typeof value === "number" && value > 0 && value < MCP_MIN_CELL;
  let hidden = false;
  const masked = (series as readonly Row[]).map((point) => {
    const next: Row = { ...point };
    for (const key of counts) {
      if (small(point[key])) {
        next[key] = null;
        hidden = true;
      }
    }
    for (const [key, bases] of Object.entries(derived)) {
      if (
        next[key] !== null &&
        next[key] !== undefined &&
        bases.some((base) => small(point[base]))
      ) {
        next[key] = null;
        hidden = true;
      }
    }
    return next;
  });
  return { series: masked as P[], hidden };
}

/**
 * A total with its small count hidden when the window is narrowed by a
 * filter or a language: a targeted filter could otherwise read one reader's
 * count off it. `hidden` says whether it was.
 */
export function hideSmallTotal(
  total: number,
  input: { filters?: readonly unknown[]; locale?: unknown },
): { total: number | null; hidden: boolean } {
  if (!isNarrowed(input)) return { total, hidden: false };
  const [point] = hideSmallInSeries([{ total }], ["total"]).series;
  return { total: point.total, hidden: point.total !== total };
}
