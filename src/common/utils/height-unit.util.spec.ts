import { publishedHeightUnit } from "./height-unit.util";

describe("publishedHeightUnit", () => {
  it("uses inches for US parks", () => {
    expect(publishedHeightUnit("US")).toBe("in");
    expect(publishedHeightUnit("us")).toBe("in");
  });

  it("uses centimetres everywhere else", () => {
    for (const code of ["DE", "NL", "GB", "JP", "FR"]) {
      expect(publishedHeightUnit(code)).toBe("cm");
    }
  });

  it("falls back to centimetres when the country is unknown", () => {
    expect(publishedHeightUnit(null)).toBe("cm");
    expect(publishedHeightUnit(undefined)).toBe("cm");
  });
});
