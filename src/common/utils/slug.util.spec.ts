import { normalizeForMatching } from "./slug.util";

describe("normalizeForMatching", () => {
  it("returns an empty string for an absent name instead of throwing", () => {
    expect(normalizeForMatching(null)).toBe("");
    expect(normalizeForMatching(undefined)).toBe("");
    expect(normalizeForMatching("")).toBe("");
  });

  it("keeps a trailing country code, which marks the language of a show", () => {
    expect(normalizeForMatching("Taron Coaster (DE)")).toBe(
      "taron coaster (de)",
    );
    expect(normalizeForMatching("V.I.M. Dance Party (DK)")).not.toBe(
      normalizeForMatching("V.I.M. Dance Party (UK)"),
    );
  });

  it("keeps a parenthesis that is not a trailing country code", () => {
    expect(normalizeForMatching("Taron (Coaster) Ride")).toBe(
      "taron (coaster) ride",
    );
  });

  it("strips ® and ™ without leaving transliteration residue", () => {
    expect(normalizeForMatching("Taron® Coaster")).toBe("taron coaster");
    expect(normalizeForMatching("Taron™ Coaster")).toBe("taron coaster");
    expect(normalizeForMatching("Spider-Man® Web Slingers")).toBe(
      "spiderman web slingers",
    );
  });

  it("strips ®/™ and normalizes whitespace together", () => {
    expect(normalizeForMatching("  Taron®   Coaster (DE) ")).toBe(
      "taron coaster (de)",
    );
  });

  it("removes ampersands and apostrophes", () => {
    expect(normalizeForMatching("Walt Disney's Magic Kingdom")).toBe(
      "walt disneys magic kingdom",
    );
  });
});
