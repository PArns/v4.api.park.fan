import { stripInvisibleCharacters } from "./visible-text.util";

/** The characters by code point, so the source shows what is tested. */
const c = (...points: number[]) => String.fromCodePoint(...points);

describe("stripInvisibleCharacters", () => {
  it.each([
    ["a right-to-left override", `Claude${c(0x202e)}`],
    ["bidi isolates", `Claude${c(0x2066, 0x2069)}`],
    ["the Arabic letter mark", `Claude${c(0x061c)}`],
    ["zero-width characters", `Cl${c(0x200b)}au${c(0x200d)}de${c(0xfeff)}`],
    ["a word joiner and invisible operators", `Cl${c(0x2060)}au${c(0x2063)}de`],
    ["line and paragraph separators", `Claude${c(0x2028, 0x2029)}`],
    ["a soft hyphen", `Cla${c(0x00ad)}ude`],
    [
      "Hangul and Mongolian fillers",
      `Cl${c(0x3164)}au${c(0xffa0)}de${c(0x115f, 0x1160, 0x180e)}`,
    ],
    ["tag characters", `Claude${c(0xe0001, 0xe0049, 0xe007f)}`],
    ["controls", `Cl${c(0x00)}au${c(0x1f)}de${c(0x7f, 0x85)}`],
  ])("removes %s", (_label, value) => {
    expect(stripInvisibleCharacters(value)).toBe("Claude");
  });

  it("keeps visible text, accents and other scripts", () => {
    expect(stripInvisibleCharacters("  Phantasialand – Brühl 東京 ")).toBe(
      "Phantasialand – Brühl 東京",
    );
  });

  it("keeps an emoji", () => {
    expect(stripInvisibleCharacters(`Claude ${c(0x1f3a2)}`)).toBe(
      `Claude ${c(0x1f3a2)}`,
    );
  });
});
