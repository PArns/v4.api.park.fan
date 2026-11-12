import { solveAssignment } from "./hungarian";

/** A small deterministic PRNG, so a failing instance can be reproduced. */
function lcg(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
}

/** The cheapest injective map of rows to columns, by trying every one. */
function bruteForce(cost: number[][]): number {
  const n = cost.length;
  const m = cost[0]?.length ?? 0;
  let best = Infinity;
  const used = new Array<boolean>(m).fill(false);
  const walk = (row: number, sum: number) => {
    if (row === n) {
      best = Math.min(best, sum);
      return;
    }
    for (let col = 0; col < m; col++) {
      if (used[col]) continue;
      used[col] = true;
      walk(row + 1, sum + cost[row][col]);
      used[col] = false;
    }
  };
  walk(0, 0);
  return best;
}

describe("solveAssignment", () => {
  it("solves a textbook square instance", () => {
    const result = solveAssignment([
      [4, 1, 3],
      [2, 0, 5],
      [3, 2, 2],
    ]);
    expect(result.cost).toBe(5);
    expect(result.feasible).toBe(true);
    expect(new Set(result.rowToCol).size).toBe(3);
  });

  it("leaves the dearest columns free when there are more columns than rows", () => {
    const result = solveAssignment([
      [9, 1, 9, 9],
      [9, 9, 9, 2],
    ]);
    expect(result.rowToCol).toEqual([1, 3]);
    expect(result.cost).toBe(3);
  });

  it("refuses more rows than columns", () => {
    expect(() => solveAssignment([[1], [2]])).toThrow();
  });

  it("answers an empty matrix with an empty assignment", () => {
    expect(solveAssignment([])).toEqual({
      rowToCol: [],
      cost: 0,
      feasible: true,
      forbiddenRows: [],
    });
  });

  it("works around forbidden cells and reports when it cannot", () => {
    const around = solveAssignment([
      [Infinity, 5],
      [1, Infinity],
    ]);
    expect(around).toMatchObject({ rowToCol: [1, 0], cost: 6, feasible: true });

    const stuck = solveAssignment([
      [Infinity, 1],
      [Infinity, 2],
    ]);
    expect(stuck.feasible).toBe(false);
    expect(stuck.cost).toBe(Infinity);
    expect(stuck.forbiddenRows).toHaveLength(1);
  });

  it("matches brute force on 400 random rectangular instances", () => {
    const random = lcg(1290);
    for (let k = 0; k < 400; k++) {
      const n = 1 + Math.floor(random() * 5);
      const m = n + Math.floor(random() * 3);
      const cost = Array.from({ length: n }, () =>
        Array.from({ length: m }, () =>
          // A tenth of the cells forbidden, the rest fractional minutes.
          random() < 0.1 ? Infinity : Math.round(random() * 1000) / 10,
        ),
      );
      const expected = bruteForce(cost);
      const result = solveAssignment(cost);
      if (Number.isFinite(expected)) {
        expect(result.feasible).toBe(true);
        expect(result.cost).toBeCloseTo(expected, 6);
        expect(new Set(result.rowToCol).size).toBe(n);
      } else {
        expect(result.feasible).toBe(false);
      }
    }
  });
});
