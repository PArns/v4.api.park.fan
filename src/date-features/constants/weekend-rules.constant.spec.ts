import { isCountryWeekendDate } from "./weekend-rules.constant";

describe("isCountryWeekendDate", () => {
  // Thursday 15 to Sunday 18 October 2026.
  const days = ["2026-10-15", "2026-10-16", "2026-10-17", "2026-10-18"];

  it("reads Saturday and Sunday where the country has no rule of its own", () => {
    expect(days.map((d) => isCountryWeekendDate(d, "DE"))).toEqual([
      false,
      false,
      true,
      true,
    ]);
  });

  it("reads the country's own weekend", () => {
    expect(days.map((d) => isCountryWeekendDate(d, "SA"))).toEqual([
      false,
      true,
      true,
      false,
    ]);
    expect(days.map((d) => isCountryWeekendDate(d, "sa"))).toEqual([
      false,
      true,
      true,
      false,
    ]);
  });

  it("falls back to the default weekend without a country code", () => {
    expect(days.map((d) => isCountryWeekendDate(d, null))).toEqual([
      false,
      false,
      true,
      true,
    ]);
  });
});
