import { roundServedWait } from "./pcn-serving.constants";

/**
 * roundServedWait must stay in EXACT parity with ml-service predict.py
 * round_to_nearest_5 + the operating min-10 rule (predict.py ~:24-52, :1959-1969)
 * AND with pcn-service score.serve_round — the same value has to appear in the UI,
 * the deviation compare, and the shadow board. The 5er-step convention is what the
 * champion-swap silently broke with a plain Math.round (users saw 23-minute waits).
 */
describe("roundServedWait", () => {
  it("rounds half-up to 5-minute steps (parity with round_to_nearest_5)", () => {
    expect(roundServedWait(12.4)).toBe(10);
    expect(roundServedWait(12.5)).toBe(15);
    expect(roundServedWait(23)).toBe(25);
    expect(roundServedWait(34.7)).toBe(35);
  });

  it("applies the operating min-10 floor to positive waits", () => {
    expect(roundServedWait(2.5)).toBe(10); // rounds to 5, floored to 10
    expect(roundServedWait(7.2)).toBe(10);
    expect(roundServedWait(7.5)).toBe(10);
  });

  it("keeps zero/near-zero waits at 0 (walk-on)", () => {
    expect(roundServedWait(0)).toBe(0);
    expect(roundServedWait(2.4)).toBe(0);
  });

  it("is monotone (rounded crowd q0.8 can never fall below rounded display q0.5)", () => {
    for (let a = 0; a <= 120; a += 0.5) {
      expect(roundServedWait(a + 3)).toBeGreaterThanOrEqual(roundServedWait(a));
    }
  });
});
