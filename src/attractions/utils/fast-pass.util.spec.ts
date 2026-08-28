import {
  DEFAULT_FAST_PASS_NAME,
  resolveFastPass,
  SUPPORTED_CURRENCIES,
} from "./fast-pass.util";

describe("resolveFastPass", () => {
  const phantasialand = {
    curatedFastPassName: "QuickPass",
    curatedCurrency: "EUR",
    curatedFastPassTermId: "quick-pass",
  };

  describe("whether there is one at all", () => {
    it("serves the product when the flag is true", () => {
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassPrice: 12 },
          phantasialand,
        ),
      ).toEqual({
        name: "QuickPass",
        price: 12,
        priceFrom: null,
        currency: "EUR",
        termId: "quick-pass",
      });
    });

    it("serves nothing when nobody has checked", () => {
      expect(resolveFastPass({}, phantasialand)).toBeNull();
      expect(resolveFastPass({ hasFastPass: null }, phantasialand)).toBeNull();
    });

    it("serves nothing when the answer is a curated no", () => {
      // `false` and `null` look identical from outside on purpose. We never
      // publish "this ride has no fast pass" — that is our bookkeeping, and
      // the editor is where the difference lives.
      expect(resolveFastPass({ hasFastPass: false }, phantasialand)).toBeNull();
    });

    it("still serves the product when the ride has no price", () => {
      // Disney and Universal price per day. The flag without a number is the
      // honest answer, not a reason to hide the product.
      expect(resolveFastPass({ hasFastPass: true }, phantasialand)).toEqual({
        name: "QuickPass",
        price: null,
        priceFrom: null,
        currency: "EUR",
        termId: "quick-pass",
      });
    });
  });

  describe("name", () => {
    it("takes the park's brand", () => {
      expect(resolveFastPass({ hasFastPass: true }, phantasialand)?.name).toBe(
        "QuickPass",
      );
    });

    it("lets a single ride override it", () => {
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassName: "Lightning Lane Single Pass" },
          { curatedFastPassName: "Lightning Lane Multi Pass" },
        )?.name,
      ).toBe("Lightning Lane Single Pass");
    });

    it("falls back to a neutral name when nobody named it", () => {
      expect(resolveFastPass({ hasFastPass: true }, {})?.name).toBe(
        DEFAULT_FAST_PASS_NAME,
      );
      expect(DEFAULT_FAST_PASS_NAME).toBe("Fast Pass");
    });

    it("ignores names that are only whitespace", () => {
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassName: "   " },
          { curatedFastPassName: "  " },
        )?.name,
      ).toBe(DEFAULT_FAST_PASS_NAME);
    });

    it("accepts a park with no row at all", () => {
      // Projections that never join the park still have to resolve a name.
      expect(resolveFastPass({ hasFastPass: true }, null)?.name).toBe(
        DEFAULT_FAST_PASS_NAME,
      );
    });
  });

  describe("price", () => {
    it("withholds the number when the park has no currency", () => {
      // A bare "12" on a ride page is not a price.
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassPrice: 12 },
          { curatedFastPassName: "Fast Lane" },
        ),
      ).toEqual({
        name: "Fast Lane",
        price: null,
        priceFrom: null,
        currency: null,
        termId: null,
      });
    });

    it("ignores an unknown currency code", () => {
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassPrice: 12 },
          { curatedCurrency: "XYZ" },
        )?.currency,
      ).toBeNull();
    });

    it("normalises the currency to upper case", () => {
      expect(
        resolveFastPass({ hasFastPass: true }, { curatedCurrency: "eur" })
          ?.currency,
      ).toBe("EUR");
      expect(SUPPORTED_CURRENCIES).toContain("EUR");
    });

    it("reads a numeric string, as a raw query hands it over", () => {
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassPrice: "12.50" as unknown as number },
          phantasialand,
        )?.price,
      ).toBe(12.5);
    });

    it("rounds a float to cents", () => {
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassPrice: 12.100000000000001 },
          phantasialand,
        )?.price,
      ).toBe(12.1);
    });

    it("reads a zero as free, not as a missing price", () => {
      // Europa-Park's Virtual Line is a queue-jump product included with
      // admission. "There is one and it costs nothing" and "there is one and
      // nobody has priced it" are different facts, and only one of them is 0.
      expect(
        resolveFastPass({ hasFastPass: true, fastPassPrice: 0 }, phantasialand)
          ?.price,
      ).toBe(0);
    });

    it("serves free even where no currency is known", () => {
      // Nothing costs 0 EUR differently from 0 USD.
      expect(
        resolveFastPass({ hasFastPass: true, fastPassPrice: 0 }, {})?.price,
      ).toBe(0);
    });

    it("treats a negative price as no price at all", () => {
      expect(
        resolveFastPass({ hasFastPass: true, fastPassPrice: -5 }, phantasialand)
          ?.price,
      ).toBeNull();
    });

    it("serves the park's entry price where the ride has none", () => {
      // The normal case: Heide Park sells one Express Ticket for the visit,
      // not one per ride.
      expect(
        resolveFastPass(
          { hasFastPass: true },
          { curatedCurrency: "EUR", curatedFastPassPriceFrom: 25 },
        ),
      ).toMatchObject({ price: null, priceFrom: 25 });
    });

    it("drops the entry price once the ride has its own", () => {
      // "12 € (ab 25 €)" is two answers to one question.
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassPrice: 12 },
          { curatedCurrency: "EUR", curatedFastPassPriceFrom: 25 },
        ),
      ).toMatchObject({ price: 12, priceFrom: null });
    });

    it("withholds the entry price without a currency, like any other price", () => {
      expect(
        resolveFastPass({ hasFastPass: true }, { curatedFastPassPriceFrom: 25 })
          ?.priceFrom,
      ).toBeNull();
    });

    it("carries the park's glossary term so the chip can link", () => {
      expect(
        resolveFastPass({ hasFastPass: true }, phantasialand)?.termId,
      ).toBe("quick-pass");
    });

    it("keeps the park's term even when the ride renames the product", () => {
      // Disney's Single Pass and Multi Pass are two labels over one idea, and
      // the glossary has one entry for it.
      expect(
        resolveFastPass(
          { hasFastPass: true, fastPassName: "Lightning Lane Single Pass" },
          { curatedFastPassTermId: "lightning-lane" },
        )?.termId,
      ).toBe("lightning-lane");
    });
  });
});
