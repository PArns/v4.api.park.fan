/**
 * Minimum-cost assignment of rows to columns (the Hungarian algorithm with
 * potentials, O(n²·m)), for a rectangular matrix with at most as many rows as
 * columns: every row gets its own column, some columns stay free.
 *
 * `Infinity` marks a forbidden cell. It is solved as a large finite cost so the
 * potentials stay finite, and a result that had to use one is reported as
 * `feasible: false` with the rows that sit on a forbidden cell, so the caller
 * can tell "cheapest" from "impossible".
 */
export interface AssignmentResult {
  /** Column of each row. */
  rowToCol: number[];
  /** Sum of the chosen cells; `Infinity` when a forbidden cell was used. */
  cost: number;
  feasible: boolean;
  /** Rows the solver could only place on a forbidden cell. */
  forbiddenRows: number[];
}

/** Larger than any real total: a plan's cells are minutes, a few hundred each. */
const FORBIDDEN = 1e9;

export function solveAssignment(cost: number[][]): AssignmentResult {
  const n = cost.length;
  if (n === 0) {
    return { rowToCol: [], cost: 0, feasible: true, forbiddenRows: [] };
  }
  const m = cost[0].length;
  if (n > m) {
    throw new Error(`solveAssignment needs rows ≤ columns, got ${n}×${m}`);
  }

  const a = (i: number, j: number): number => {
    const value = cost[i - 1][j - 1];
    return Number.isFinite(value) ? value : FORBIDDEN;
  };

  // 1-based, as in the textbook form; index 0 is the virtual start column.
  const u = new Float64Array(n + 1);
  const v = new Float64Array(m + 1);
  const p = new Int32Array(m + 1);
  const way = new Int32Array(m + 1);
  const minv = new Float64Array(m + 1);
  const used = new Uint8Array(m + 1);

  for (let i = 1; i <= n; i++) {
    p[0] = i;
    let j0 = 0;
    minv.fill(Infinity);
    used.fill(0);
    do {
      used[j0] = 1;
      const i0 = p[j0];
      let delta = Infinity;
      let j1 = 0;
      for (let j = 1; j <= m; j++) {
        if (used[j]) continue;
        const cur = a(i0, j) - u[i0] - v[j];
        if (cur < minv[j]) {
          minv[j] = cur;
          way[j] = j0;
        }
        if (minv[j] < delta) {
          delta = minv[j];
          j1 = j;
        }
      }
      for (let j = 0; j <= m; j++) {
        if (used[j]) {
          u[p[j]] += delta;
          v[j] -= delta;
        } else {
          minv[j] -= delta;
        }
      }
      j0 = j1;
    } while (p[j0] !== 0);
    do {
      const j1 = way[j0];
      p[j0] = p[j1];
      j0 = j1;
    } while (j0 !== 0);
  }

  const rowToCol = new Array<number>(n).fill(-1);
  for (let j = 1; j <= m; j++) {
    if (p[j] !== 0) rowToCol[p[j] - 1] = j - 1;
  }

  let total = 0;
  const forbiddenRows: number[] = [];
  rowToCol.forEach((col, row) => {
    const value = cost[row][col];
    if (Number.isFinite(value)) total += value;
    else forbiddenRows.push(row);
  });
  return {
    rowToCol,
    cost: forbiddenRows.length > 0 ? Infinity : total,
    feasible: forbiddenRows.length === 0,
    forbiddenRows,
  };
}
