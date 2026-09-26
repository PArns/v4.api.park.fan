import {
  chooseNameDuplicateWinner,
  nameDuplicateKey,
  outranksNameDuplicate,
} from "./name-duplicate.util";

/**
 * The rule both the park payload and the attraction sitemap decide by. It has
 * one job beyond picking a row: the answer may not depend on anything that
 * changes between two crawls, because the row it picks owns a public URL.
 *
 * The numbers here are the 2026-09-26 measurement over production: 45 duplicate
 * name groups across 15 parks, 93 rows, and 48 sitemap entries that had no row
 * in the payload before this (PAR-498).
 */
describe("name duplicate winner", () => {
  const winner = (slugs: string[], name: string): string | undefined =>
    chooseNameDuplicateWinner(slugs.map((slug) => ({ slug, name })))?.slug;

  it("prefers the slug without the counter — 42 of the 45 production groups", () => {
    expect(winner(["raven-2", "raven"], "Raven")).toBe("raven");
    expect(winner(["raven", "raven-2"], "Raven")).toBe("raven");
  });

  it("prefers the base slug over every counter, not just over -2", () => {
    expect(
      winner(
        ["playground-4", "playground-2", "playground", "playground-5"],
        "PLAYGROUND",
      ),
    ).toBe("playground");
  });

  it("falls back to the name when every slug carries a counter", () => {
    expect(
      winner(
        ["walibi-express-station-2-2", "walibi-express-station-2"],
        "Walibi Express Station 2",
      ),
    ).toBe("walibi-express-station-2");
  });

  it("falls back to the name when no slug carries a counter", () => {
    expect(winner(["wahoo-racer", "typhoon-twister"], "Typhoon Twister")).toBe(
      "typhoon-twister",
    );
    expect(
      winner(
        ["castaway-bay-sky-climb", "wally-the-walrus"],
        "Wally the Walrus",
      ),
    ).toBe("wally-the-walrus");
  });

  it("matches the name through the transforms generateSlug applies", () => {
    expect(
      winner(
        ["fiesta", "hexenhaus-presented-by-snickers"],
        "Hexenhaus presented by SNICKERS®",
      ),
    ).toBe("hexenhaus-presented-by-snickers");
    expect(winner(["fenix", "feenix"], "Fēnix")).toBe("fenix");
  });

  it("falls back to the slug itself when neither candidate matches the name", () => {
    expect(
      winner(
        ["discovery-bay-mini-waves", "discovery-bay-treehouse"],
        "Discovery Bay",
      ),
    ).toBe("discovery-bay-mini-waves");
  });

  it("survives a name that slugifies to nothing instead of throwing", () => {
    expect(winner(["a", "b"], "—")).toBe("a");
  });

  it("gives the same answer for every input order", () => {
    const slugs = ["nosferatu-3", "nosferatu", "nosferatu-2"];

    for (let shift = 0; shift < slugs.length; shift++) {
      const rotated = [...slugs.slice(shift), ...slugs.slice(0, shift)];

      expect(winner(rotated, "Nosferatu")).toBe("nosferatu");
    }
  });

  it("is a strict order: a row never outranks itself", () => {
    const row = { slug: "raven", name: "Raven" };

    expect(outranksNameDuplicate(row, row)).toBe(false);
    expect(outranksNameDuplicate({ ...row }, row)).toBe(false);
  });

  it("is antisymmetric, so the winner cannot depend on which side asks", () => {
    const base = { slug: "vampire", name: "VAMPIRE" };
    const suffix = { slug: "vampire-2", name: "VAMPIRE" };

    expect(outranksNameDuplicate(base, suffix)).toBe(true);
    expect(outranksNameDuplicate(suffix, base)).toBe(false);
  });

  it("returns nothing for an empty group rather than undefined-as-a-row", () => {
    expect(chooseNameDuplicateWinner([])).toBeUndefined();
  });

  /**
   * The group has to be settled before the winner is. Five active rows carry a
   * name with trailing whitespace (2026-09-26, all at Beto Carrero World), and
   * the payload has always trimmed while the sitemap did not.
   */
  describe("nameDuplicateKey", () => {
    it("trims, so a padded name lands in the same group", () => {
      expect(nameDuplicateKey("Excalibur ")).toBe("Excalibur");
      expect(nameDuplicateKey(" Excalibur")).toBe(
        nameDuplicateKey("Excalibur"),
      );
    });

    it("refuses a blank name, so no surface advertises a row it cannot serve", () => {
      expect(nameDuplicateKey("")).toBeNull();
      expect(nameDuplicateKey("   ")).toBeNull();
      expect(nameDuplicateKey(null)).toBeNull();
      expect(nameDuplicateKey(undefined)).toBeNull();
    });
  });
});
