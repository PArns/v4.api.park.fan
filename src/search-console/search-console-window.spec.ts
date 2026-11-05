import { anchorRollingWindow } from "./search-console-window";

const LAST = "2026-10-26";

describe("anchorRollingWindow", () => {
  it("ends a rolling window on the last day with data", () => {
    expect(anchorRollingWindow({ days: 28 }, LAST)).toMatchObject({
      days: null,
      from: "2026-09-29",
      to: LAST,
    });
  });

  it("uses the default length when no window is named", () => {
    expect(anchorRollingWindow({}, LAST)).toMatchObject({
      from: "2026-09-27",
      to: LAST,
    });
  });

  it("leaves an explicit window as asked", () => {
    const query = { from: "2026-10-01", to: "2026-10-28" };
    expect(anchorRollingWindow(query, LAST)).toBe(query);
  });

  it("leaves the window alone without any data", () => {
    const query = { days: 28 };
    expect(anchorRollingWindow(query, null)).toBe(query);
  });
});
