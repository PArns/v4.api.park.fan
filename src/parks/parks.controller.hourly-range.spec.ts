import { BadRequestException } from "@nestjs/common";
import { parseHourlyProfileRange } from "./parks.controller";

describe("parseHourlyProfileRange (PAR-978)", () => {
  it("returns undefined without from and to, so the years window stays", () => {
    expect(parseHourlyProfileRange()).toBeUndefined();
  });

  it("passes a real inclusive window through", () => {
    expect(parseHourlyProfileRange("2025-12-01", "2026-10-21")).toEqual({
      from: "2025-12-01",
      to: "2026-10-21",
    });
    expect(parseHourlyProfileRange("2026-10-21", "2026-10-21")).toEqual({
      from: "2026-10-21",
      to: "2026-10-21",
    });
  });

  it.each([
    ["2025-12-01", undefined],
    [undefined, "2026-10-21"],
    ["2026-02-31", "2026-10-21"],
    ["2025-12-1", "2026-10-21"],
    ["2026-10-22", "2026-10-21"],
    ["2020-01-01", "2026-10-21"],
  ])("refuses from=%s to=%s with 400", (from, to) => {
    expect(() => parseHourlyProfileRange(from, to)).toThrow(
      BadRequestException,
    );
  });
});
