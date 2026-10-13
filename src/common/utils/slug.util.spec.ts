import { normalizeForMatching } from "./slug.util";

describe("normalizeForMatching", () => {
  it("strips an uppercase country code suffix", () => {
    expect(normalizeForMatching("Taron Coaster (DE)")).toBe("taron coaster");
    expect(normalizeForMatching("Astérix ( FR )")).toBe("asterix");
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

  it("combines both rules and normalizes whitespace", () => {
    expect(normalizeForMatching("  Taron®   Coaster (DE) ")).toBe(
      "taron coaster",
    );
  });

  it("removes ampersands and apostrophes", () => {
    expect(normalizeForMatching("Walt Disney's Magic Kingdom")).toBe(
      "walt disneys magic kingdom",
    );
  });
});
