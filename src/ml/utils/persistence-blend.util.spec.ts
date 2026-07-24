import {
  persistenceBlendServe,
  BLEND_GATE_MIN,
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

  it("gates: a below-threshold or missing anchor stays pure PCN (quiet-ride protection)", () => {
    expect(persistenceBlendServe(30, BLEND_GATE_MIN - 1, 0.5)).toBe(30); // just under the gate
    expect(persistenceBlendServe(30, 20, 1.0)).toBe(30); // quiet anchor
    expect(persistenceBlendServe(30, undefined, 0.5)).toBe(30); // no anchor
    expect(persistenceBlendServe(30, null, 0.5)).toBe(30);
  });

  it("at the gate boundary the ride does blend", () => {
    expect(persistenceBlendServe(30, BLEND_GATE_MIN, 1.0)).toBe(BLEND_GATE_MIN); // anchor==gate → blends
  });
});
