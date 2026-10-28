import {
  MAX_HITS_PER_REQUEST,
  parseCollectBody,
  validateHit,
} from "./site-analytics.hits";
import {
  MAX_EVENT_PROPS,
  SITE_EVENTS,
  SITE_EVENT_NAMES,
  SITE_EVENT_NAME_MAX_LENGTH,
  SITE_PATH_MAX_LENGTH,
  propValues,
} from "./site-analytics.schema";

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
  props?: Record<string, unknown>,
  overrides: Record<string, unknown> = {},
) => ({ t: "event", ...base, name, ...(props && { props }), ...overrides });

/**
 * The closed schema at the door. What is pinned: only `{ v: 1, hits: 1..20 }`
 * parses, in either content type; a hit outside the schema is dropped alone;
 * a property must be exactly the declared set with an admissible value; an
 * entity or referrer that does not fit is dropped and the hit kept.
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

  it("takes up to twenty hits, malformed ones included, for the hit check", () => {
    const hits = [...Array(MAX_HITS_PER_REQUEST - 1).fill(pageview()), 42];
    expect(parseCollectBody({ v: 1, hits })).toHaveLength(20);
  });
});

describe("validateHit", () => {
  it("accepts a page view and keeps a well-formed referrer", () => {
    expect(validateHit(pageview({ referrer: "google.com" }))).toEqual({
      t: "pageview",
      ...base,
      referrer: "google.com",
    });
  });

  it("drops a referrer that is malformed or the site itself, not the hit", () => {
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
      pageview({ path: "de/parks" }),
      pageview({ path: `/de${"/a".repeat(9)}` }),
      pageview({ path: `/de/${"a".repeat(101)}` }),
      pageview({ path: "/en/parks" }),
      pageview({ pageType: "landing" }),
      pageview({ viewport: "watch" }),
    ]) {
      expect(validateHit(hit)).toBeNull();
    }
  });

  it("accepts the language's own home, the deepest path and ignores unknown fields", () => {
    expect(validateHit(pageview({ path: "/de", pageType: "home" }))).not.toBe(
      null,
    );
    const deepest = `/de${`/${"a".repeat(100)}`.repeat(8)}`;
    expect(deepest).toHaveLength(SITE_PATH_MAX_LENGTH);
    expect(validateHit(pageview({ path: deepest }))).not.toBeNull();
    expect(validateHit(pageview({ extra: "ignored" }))).not.toBeNull();
  });

  it("stores an event's properties in the declared order, whatever order they came in", () => {
    expect(
      validateHit(
        event("search_result_clicked", {
          position: "4-5",
          result: "attraction",
          surface: "hero",
        }),
      ),
    ).toMatchObject({
      name: "search_result_clicked",
      props: ["hero", "attraction", "4-5"],
      entity: null,
    });
  });

  it("drops an event with an unknown name, an unknown, missing or out-of-list property", () => {
    for (const hit of [
      event("search_closed", { surface: "hero" }),
      event("toString"),
      event("__proto__"),
      event("search_opened"),
      event("search_opened", {}),
      event("search_opened", { surface: "footer" }),
      event("search_opened", { surface: "hero", extra: "x" }),
      event("search_opened", { surfaces: "hero" }),
      event("search_opened", { surface: 1 }),
      event("search_opened", ["hero"] as unknown as Record<string, unknown>),
      event("language_switched", { from: "de", to: "pt" }),
      event("glossary_term_viewed", { term: "Loop" }),
      event("glossary_term_viewed", { term: "-loop" }),
      event("glossary_term_viewed", { term: "a".repeat(61) }),
      event("pwa_installed", { source: "x" }),
    ]) {
      expect(validateHit(hit)).toBeNull();
    }
  });

  it("accepts an event without properties with or without an empty object", () => {
    expect(validateHit(event("pwa_installed"))).toMatchObject({ props: [] });
    expect(validateHit(event("pwa_installed", {}))).toMatchObject({
      props: [],
    });
  });

  it("keeps an entity only where the event names one, and only as a lowercase UUID", () => {
    expect(
      validateHit(event("tab_changed", { tab: "map" }, { entity: PARK_ID })),
    ).toMatchObject({ entity: { id: PARK_ID, kind: "park" } });
    expect(
      validateHit(
        event("tab_changed", { tab: "map" }, { entity: PARK_ID.toUpperCase() }),
      ),
    ).toMatchObject({ entity: null });
    expect(
      validateHit(
        event("search_opened", { surface: "hero" }, { entity: PARK_ID }),
      ),
    ).toMatchObject({ entity: null });
    expect(
      validateHit(event("ride_alert_removed", {}, { entity: PARK_ID })),
    ).toMatchObject({ entity: { kind: "attraction" } });
    expect(
      validateHit(
        event("show_follow_add", { source: "panel" }, { entity: PARK_ID }),
      ),
    ).toMatchObject({ entity: { kind: "show" } });
  });

  it("takes a favourite's kind from its type, and keeps no restaurant", () => {
    expect(
      validateHit(
        event("favorite_add", { type: "attraction" }, { entity: PARK_ID }),
      ),
    ).toMatchObject({ entity: { kind: "attraction" } });
    expect(
      validateHit(
        event("favorite_remove", { type: "restaurant" }, { entity: PARK_ID }),
      ),
    ).toMatchObject({ props: ["restaurant"], entity: null });
  });

  it("names a park on nearby_parks_loaded only for the park the reader is in", () => {
    expect(
      validateHit(
        event(
          "nearby_parks_loaded",
          { type: "in_park", source: "gps" },
          { entity: PARK_ID },
        ),
      ),
    ).toMatchObject({ entity: { kind: "park" } });
    expect(
      validateHit(
        event(
          "nearby_parks_loaded",
          { type: "nearby_parks", source: "ip" },
          { entity: PARK_ID },
        ),
      ),
    ).toMatchObject({ entity: null });
  });
});

describe("SITE_EVENTS", () => {
  it("fits its columns: three properties, short names, values within 60 characters", () => {
    for (const name of SITE_EVENT_NAMES) {
      expect(name.length).toBeLessThanOrEqual(SITE_EVENT_NAME_MAX_LENGTH);
      expect(name).toMatch(/^[a-z][a-z_]*$/);
      const { props } = SITE_EVENTS[name];
      expect(props.length).toBeLessThanOrEqual(MAX_EVENT_PROPS);
      for (const prop of props) {
        for (const value of propValues(prop) ?? []) {
          expect(value.length).toBeLessThanOrEqual(60);
          expect(value).not.toBe("");
        }
      }
    }
  });

  it("holds the contract's thirty-six events", () => {
    expect(SITE_EVENT_NAMES).toHaveLength(36);
  });
});
