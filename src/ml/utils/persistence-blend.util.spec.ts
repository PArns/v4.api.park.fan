import {
  persistenceBlendServe,
  BLEND_GATE_MIN,
  BLEND_FIRST_HOUR_MIN_ANCHOR,
} from "./persistence-blend.util";

describe("persistenceBlendServe (anchor-gated piecewise, mirrors pcn-service score.py)", () => {
  it("blends toward the anchor at short lead, decays to pure PCN by the horizon", () => {
    // pcn=30, anchor=70 (>= gate). full_h=1, horizon=3.
    expect(persistenceBlendServe(30, 70, 0.5)).toBe(70); // ≤1h → pure persistence
    expect(persistenceBlendServe(30, 70, 1.0)).toBe(70); // at full_h → pure persistence
    expect(persistenceBlendServe(30, 70, 2.0)).toBe(50); // α=0.5 → 0.5·70+0.5·30
    expect(persistenceBlendServe(30, 70, 3.0)).toBe(30); // at horizon → pure PCN
    expect(persistenceBlendServe(30, 70, 6.0)).toBe(30); // beyond → pure PCN
  });

  it("first hour serves any anchor of at least 5 min, below the 40-min gate too (PAR-1026)", () => {
    expect(persistenceBlendServe(30, 20, 0.25)).toBe(20); // quiet ride, 15 min ahead
    expect(persistenceBlendServe(30, BLEND_GATE_MIN - 1, 1.0)).toBe(39); // at full_h
    expect(persistenceBlendServe(30, BLEND_FIRST_HOUR_MIN_ANCHOR, 0.5)).toBe(5);
    expect(
      persistenceBlendServe(30, BLEND_FIRST_HOUR_MIN_ANCHOR - 1, 0.5),
    ).toBe(30); // walk-on placeholder → PCN
    expect(persistenceBlendServe(30, 0, 0)).toBe(30);
  });

  it("ramp gates: a below-threshold anchor stays pure PCN past the first hour", () => {
    expect(persistenceBlendServe(30, BLEND_GATE_MIN - 1, 1.5)).toBe(30); // just under the gate
    expect(persistenceBlendServe(30, 20, 2.0)).toBe(30); // quiet anchor
  });

  it("a missing anchor stays pure PCN at every lead", () => {
    expect(persistenceBlendServe(30, undefined, 0.5)).toBe(30);
    expect(persistenceBlendServe(30, null, 0.5)).toBe(30);
    expect(persistenceBlendServe(30, null, 2.0)).toBe(30);
  });

  it("at the gate boundary the ramp does blend", () => {
    expect(persistenceBlendServe(30, BLEND_GATE_MIN, 2.0)).toBe(35); // anchor==gate → α=0.5
  });
});
