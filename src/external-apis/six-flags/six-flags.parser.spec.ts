import {
  parseMinHeightInches,
  inchesToCentimetres,
} from "./six-flags.parser";

/**
 * Six Flags renders ride facts server-side, so the minimum height can be read
 * without a browser. Two traps, both taken from the real markup:
 *
 *  - the value uses a typographic quote (52”), not an ASCII one
 *  - "Min Height Accompanied" contains "Min Height", so a substring search
 *    happily returns the accompanied number for a ride that has both
 *
 * Validated against 14 Cedar Point rides whose heights were read off the pages
 * by hand first; the parser reproduced all 14.
 */
describe("parseMinHeightInches", () => {
  const block = (label: string, value: string) =>
    `<div class="flex gap-x-3 flex-col"><div class="type-16-600 text-navy">${label}</div><p class="text-blue-light"><span class="">${value}</span></p></div>`;

  it("reads the height from the rendered ride facts", () => {
    expect(parseMinHeightInches(block("Min Height", "52”"))).toBe(52);
  });

  it("accepts an ASCII quote too", () => {
    expect(parseMinHeightInches(block("Min Height", '48"'))).toBe(48);
  });

  it("does not mistake the accompanied height for the real one", () => {
    // Woodstock Express: 48" alone, 36" with an adult. The unaccompanied
    // number is the one labelled "Min Height".
    const html =
      block("Min Height", "48”") + block("Min Height Accompanied", "36”");

    expect(parseMinHeightInches(html)).toBe(48);
  });

  it("still finds it when the accompanied block comes first", () => {
    const html =
      block("Min Height Accompanied", "36”") + block("Min Height", "48”");

    expect(parseMinHeightInches(html)).toBe(48);
  });

  it("returns null for a ride page without a height requirement", () => {
    expect(parseMinHeightInches(block("Max People", "4"))).toBeNull();
  });

  it("returns null for an unrelated page", () => {
    expect(parseMinHeightInches("<html><body>404</body></html>")).toBeNull();
  });

  it("rejects implausible values rather than storing nonsense", () => {
    expect(parseMinHeightInches(block("Min Height", "0”"))).toBeNull();
    expect(parseMinHeightInches(block("Min Height", "999”"))).toBeNull();
  });
});

describe("inchesToCentimetres", () => {
  it("converts the heights the US parks actually publish", () => {
    expect(inchesToCentimetres(42)).toBe(107);
    expect(inchesToCentimetres(48)).toBe(122);
    expect(inchesToCentimetres(52)).toBe(132);
    expect(inchesToCentimetres(54)).toBe(137);
  });

  it("survives a round trip at the precision we display", () => {
    // We store centimetres but a US park page says 52" — converting back must
    // land on the published number, or the ride page contradicts the park.
    for (const inches of [36, 40, 42, 44, 46, 48, 52, 54, 60]) {
      const back = Math.round(inchesToCentimetres(inches) / 2.54);
      expect(back).toBe(inches);
    }
  });
});
