import { isPeakAfterClosing } from "./analytics.service";

describe("isPeakAfterClosing", () => {
  const TZ = "Asia/Riyadh";
  const TODAY = "2026-11-14";

  it("hides a peak at or after a same-day closing", () => {
    // 19:00 local
    const closing = new Date("2026-11-14T16:00:00Z");
    expect(isPeakAfterClosing("19:00", closing, TZ, TODAY)).toBe(true);
    expect(isPeakAfterClosing("20:00", closing, TZ, TODAY)).toBe(true);
    expect(isPeakAfterClosing("18:00", closing, TZ, TODAY)).toBe(false);
  });

  it.each([
    ["00:00", "2026-11-14T21:00:00Z"],
    ["01:00", "2026-11-14T22:00:00Z"],
    ["02:00", "2026-11-14T23:00:00Z"],
  ])("keeps an evening peak when the park closes at %s", (_, closingIso) => {
    const closing = new Date(closingIso);
    expect(isPeakAfterClosing("20:00", closing, TZ, TODAY)).toBe(false);
    expect(isPeakAfterClosing("23:00", closing, TZ, TODAY)).toBe(false);
  });
});
