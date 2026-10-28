import {
  MAX_HITS_PER_REQUEST,
  SITE_PATH_MAX_LENGTH,
  parseCollectBody,
  propsKeyOf,
  propsOfKey,
  sanitizeProps,
  validateHit,
} from "./site-analytics.hits";

const PARK_ID = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
const base = {
  locale: "de",
  path: "/de/parks/europe/germany/bruehl/phantasialand",
  pageType: "park",
  viewport: "mobile",
};
const pageview = (overrides: Record<string, unknown> = {}) => ({
  t: "pageview",
  ...base,
  ...overrides,
});
const event = (
  name: string,
  props?: unknown,
  overrides: Record<string, unknown> = {},
) => ({
  t: "event",
  ...base,
  name,
  ...(props !== undefined && { props }),
  ...overrides,
});

/**
 * The wire format at the door. There is no event schema, so what is pinned is
 * the pattern of every field: only `{ v: 1, hits: 1..20 }` parses; a hit with
 * a malformed base field or name is dropped alone; a malformed property,
 * referrer, campaign or entity is dropped and the hit kept; properties are
 * kept in key order, at most eight, with an unambiguous canonical key; and
 * `page_read`, the one event the API knows, keeps the most-read counter's key
 * rules.
 */
describe("parseCollectBody", () => {
  it("reads the text body sendBeacon sends and the parsed JSON body alike", () => {
    const body = { v: 1, hits: [pageview()] };
    expect(parseCollectBody(JSON.stringify(body))).toEqual(body.hits);
    expect(parseCollectBody(body)).toEqual(body.hits);
  });

  it("refuses what is not JSON or not the shape", () => {
    for (const body of [
      "not json",
      "",
      undefined,
      null,
      [],
      "[]",
      { hits: [pageview()] },
      { v: 2, hits: [pageview()] },
      { v: "1", hits: [pageview()] },
      { v: 1, hits: [] },
      { v: 1, hits: {} },
      { v: 1, hits: Array(MAX_HITS_PER_REQUEST + 1).fill(pageview()) },
    ]) {
      expect(parseCollectBody(body)).toBeNull();
    }
  });
});

describe("validateHit", () => {
  it("accepts a page view with its referrer and campaign, the campaign lowercased", () => {
    expect(
      validateHit(
        pageview({ referrer: "google.com", campaign: "Summer-2026" }),
      ),
    ).toEqual({
      t: "pageview",
      ...base,
      referrer: "google.com",
      campaign: "summer-2026",
    });
  });

  it("drops a referrer or campaign that is malformed or the site itself, not the hit", () => {
    for (const referrer of [
      "Google.com",
      "https://google.com",
      "-x.com",
      "park.fan",
      "www.park.fan",
      "x".repeat(101),
      7,
    ]) {
      expect(validateHit(pageview({ referrer }))).toMatchObject({
        referrer: null,
      });
    }
    expect(validateHit(pageview({ campaign: "a b" }))).toMatchObject({
      campaign: null,
    });
  });

  it("drops a hit whose base fields do not fit", () => {
    for (const hit of [
      null,
      "pageview",
      pageview({ t: "click" }),
      pageview({ locale: "pt" }),
      pageview({ path: "/pt/parks" }),
      pageview({ path: "/de/Parks" }),
      pageview({ path: "/de/parks/" }),
      pageview({ path: "/de/parks?x=1" }),
      pageview({ path: "/de/park%C3%BC" }),
      pageview({ path: `/de${"/a".repeat(9)}` }),
      pageview({ path: "/en/parks" }),
      pageview({ pageType: "Park" }),
      pageview({ pageType: "1park" }),
      pageview({ pageType: "x".repeat(33) }),
      pageview({ viewport: "watch" }),
    ]) {
      expect(validateHit(hit)).toBeNull();
    }
  });

  it("takes any page type that fits the pattern, the deepest path and ignores unknown fields", () => {
    expect(validateHit(pageview({ pageType: "not_found" }))).not.toBeNull();
    expect(validateHit(pageview({ path: "/de", pageType: "home" }))).not.toBe(
      null,
    );
    const deepest = `/de${`/${"a".repeat(100)}`.repeat(8)}`;
    expect(deepest).toHaveLength(SITE_PATH_MAX_LENGTH);
    expect(validateHit(pageview({ path: deepest }))).not.toBeNull();
    expect(validateHit(pageview({ extra: "ignored" }))).not.toBeNull();
  });

  it("accepts any event name that fits, and drops one that does not", () => {
    expect(validateHit(event("brand_new_event"))).toMatchObject({
      name: "brand_new_event",
      props: {},
      propsKey: "",
      entity: null,
    });
    for (const name of ["x", "Search", "1st", "with-dash", "a".repeat(49), 7]) {
      expect(validateHit(event(name as string))).toBeNull();
    }
  });

  it("keeps properties in key order, numbers and booleans as strings, any script", () => {
    expect(
      validateHit(
        event("search_result_clicked", {
          surface: "hero",
          position: 3,
          exact: false,
          result: "Phantasialand Brühl",
          term: "ジェットコースター",
        }),
      ),
    ).toMatchObject({
      props: {
        exact: "false",
        position: "3",
        result: "Phantasialand Brühl",
        surface: "hero",
        term: "ジェットコースター",
      },
      propsKey:
        "exact=false&position=3&result=Phantasialand Brühl&surface=hero&term=ジェットコースター",
    });
  });

  it("drops a property with a bad key or value alone", () => {
    expect(
      sanitizeProps({
        ok: "fine",
        Bad: "x",
        _overflow: "1",
        "with-dash": "x",
        amp: "a&b",
        eq: "a=b",
        empty: "",
        leading: " space",
        long: "x".repeat(121),
        nan: Number.NaN,
        obj: { a: 1 },
        arr: ["a"],
        nil: null,
      }),
    ).toEqual({ ok: "fine" });
    expect(sanitizeProps("not an object")).toEqual({});
    expect(sanitizeProps(["a"])).toEqual({});
  });

  it("keeps at most eight properties, the first in key order", () => {
    const props = Object.fromEntries(
      "jihgfedcba".split("").map((key) => [key, "v"]),
    );
    expect(Object.keys(sanitizeProps(props))).toEqual("abcdefgh".split(""));
  });

  it("takes a 120-character value, so every translationKey fits", () => {
    const value = "a".repeat(120);
    expect(sanitizeProps({ key: value })).toEqual({ key: value });
  });

  it("round-trips a canonical key", () => {
    const props = { surface: "hero", result: "park" };
    expect(propsKeyOf(props)).toBe("result=park&surface=hero");
    expect(propsOfKey(propsKeyOf(props))).toEqual(props);
    expect(propsOfKey("")).toEqual({});
  });

  it("keeps an entity only as a lowercase UUID", () => {
    expect(
      validateHit(event("tab_changed", { tab: "map" }, { entity: PARK_ID })),
    ).toMatchObject({ entity: PARK_ID });
    expect(
      validateHit(
        event("tab_changed", { tab: "map" }, { entity: PARK_ID.toUpperCase() }),
      ),
    ).toMatchObject({ entity: null });
  });

  it("drops the entity of an event that would say where the reader is", () => {
    for (const name of [
      "nearby_parks_loaded",
      "compass_viewed",
      "compass_ride_opened",
      "compass_anything_new",
    ]) {
      expect(
        validateHit(event(name, { type: "in_park" }, { entity: PARK_ID })),
      ).toMatchObject({ name, props: { type: "in_park" }, entity: null });
    }
    expect(
      validateHit(event("nearby_permission", {}, { entity: PARK_ID })),
    ).toMatchObject({ entity: PARK_ID });
  });

  describe("page_read", () => {
    it("names a post by its translationKey and carries no entity", () => {
      expect(
        validateHit(
          event(
            "page_read",
            { kind: "news", key: "europa-park-2026", extra: "x" },
            { entity: PARK_ID },
          ),
        ),
      ).toMatchObject({
        props: { key: "europa-park-2026", kind: "news" },
        propsKey: "key=europa-park-2026&kind=news",
        entity: null,
      });
    });

    it("names a park or a ride by its entity and nothing else", () => {
      expect(
        validateHit(
          event(
            "page_read",
            { kind: "attraction", key: "ignored" },
            { entity: PARK_ID },
          ),
        ),
      ).toMatchObject({
        props: { kind: "attraction" },
        propsKey: "kind=attraction",
        entity: PARK_ID,
      });
    });

    it("drops a read with another kind, a malformed key or no entity", () => {
      for (const hit of [
        event("page_read", { kind: "glossary", key: "loop" }),
        event("page_read", { kind: "news", key: "Has Spaces" }),
        event("page_read", { kind: "news", key: "-leading" }),
        event("page_read", { kind: "news" }),
        event("page_read", { kind: "park" }),
        event("page_read", { kind: "park" }, { entity: "europa-park" }),
        event("page_read", {}),
      ]) {
        expect(validateHit(hit)).toBeNull();
      }
    });
  });
});
