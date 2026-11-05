import { VISITOR_DIMS } from "./site-analytics.constants";
import { parseFilters } from "./site-analytics.query";
import {
  isVisitorDim,
  parseVisitorDimMember,
  visitorBasisOf,
  visitorDimKeys,
  visitorDimMember,
} from "./visitor-dims.util";

const filters = (...raw: string[]) => parseFilters(raw);
const READER = {
  country: "de",
  device: "mobile",
  browser: "safari",
  os: "ios",
};

/** One set of members, for comparing key lists regardless of order. */
const members = (keys: ReturnType<typeof visitorDimKeys>) =>
  keys.map(visitorDimMember).sort();

describe("visitorBasisOf", () => {
  it("counts every reader of the language under no filter", () => {
    expect(visitorBasisOf(filters())).toEqual({ filter: null, by: null });
  });

  it.each(VISITOR_DIMS)("counts under one filter on %s", (dim) => {
    expect(visitorBasisOf(filters(`${dim}:x`))).toEqual({
      filter: { dim, value: "x" },
      by: null,
    });
  });

  it.each([
    ["two dimensions", ["country:de", "device:mobile"]],
    ["two values of one dimension", ["country:de", "country:nl"]],
    ["the same filter twice", ["country:de", "country:de"]],
    ["a path", ["path:/de"]],
    ["a page type", ["page_type:park"]],
    ["the browser language", ["browser_language:de"]],
    ["a locale filter", ["locale:de"]],
    ["the entry page", ["entry:/de"]],
    ["a referrer", ["referrer:google"]],
    ["a visitor dimension and a page", ["country:de", "path:/de"]],
  ])("has none under %s", (_name, raw) => {
    expect(visitorBasisOf(filters(...raw))).toBeNull();
  });

  it("drops a malformed filter before it is counted", () => {
    expect(visitorBasisOf(filters("country", "nothing:x"))).toEqual({
      filter: null,
      by: null,
    });
  });

  describe("for a breakdown", () => {
    it.each(VISITOR_DIMS)("counts per value of %s", (dim) => {
      expect(visitorBasisOf(filters(), dim)).toEqual({ filter: null, by: dim });
    });

    it("counts per value under a filter on the same dimension only", () => {
      expect(visitorBasisOf(filters("country:de"), "country")).toEqual({
        filter: { dim: "country", value: "de" },
        by: "country",
      });
      expect(visitorBasisOf(filters("device:mobile"), "country")).toBeNull();
      expect(
        visitorBasisOf(filters("country:de", "country:nl"), "country"),
      ).toBeNull();
    });

    it.each([
      "path",
      "page_type",
      "exit",
      "referrer",
      "browser_language",
    ] as const)("has none by %s", (dim) => {
      expect(visitorBasisOf(filters(), dim)).toBeNull();
    });
  });
});

describe("isVisitorDim", () => {
  it("is the five dimensions counted on their own", () => {
    expect([...VISITOR_DIMS]).toEqual([
      "country",
      "device",
      "browser",
      "os",
      "channel",
    ]);
    expect(isVisitorDim("os")).toBe(true);
    expect(isVisitorDim("locale")).toBe(false);
  });
});

describe("visitorDimKeys", () => {
  it("counts the reader's four request dimensions in the all-languages scope and each language read", () => {
    const keys = visitorDimKeys(READER, ["de", "en", "de"], null);
    expect(members(keys)).toEqual(
      ["all", "de", "en"]
        .flatMap((scope) => [
          `${scope}|browser|safari`,
          `${scope}|country|de`,
          `${scope}|device|mobile`,
          `${scope}|os|ios`,
        ])
        .sort(),
    );
  });

  it("counts the channel only for a visit that opened here, in its own language", () => {
    const keys = visitorDimKeys(READER, ["de", "en"], {
      locale: "en",
      channel: "search",
    });
    expect(members(keys).filter((key) => key.includes("|channel|"))).toEqual([
      "all|channel|search",
      "en|channel|search",
    ]);
    expect(
      members(visitorDimKeys(READER, ["de"], null)).some((key) =>
        key.includes("|channel|"),
      ),
    ).toBe(false);
  });

  it("counts nothing for a request without a page view", () => {
    expect(visitorDimKeys(READER, [], null)).toEqual([]);
    expect(
      visitorDimKeys(READER, [], { locale: "de", channel: "direct" }),
    ).toEqual([]);
  });

  it("does not count an overflow or an empty value", () => {
    const keys = visitorDimKeys(
      { ...READER, country: "(other)", os: "" },
      ["de"],
      { locale: "de", channel: "(other)" },
    );
    expect(members(keys)).toEqual([
      "all|browser|safari",
      "all|device|mobile",
      "de|browser|safari",
      "de|device|mobile",
    ]);
  });
});

describe("the index members", () => {
  it("round-trips a key, whatever bars its value holds", () => {
    const key = { scope: "all", dim: "browser", value: "a|b" } as const;
    expect(parseVisitorDimMember(visitorDimMember(key))).toEqual(key);
  });

  it.each([
    "",
    "all",
    "all|country",
    "all|country|",
    "|country|de",
    "de|locale|x",
  ])("ignores %p", (member) => {
    expect(parseVisitorDimMember(member)).toBeNull();
  });
});
