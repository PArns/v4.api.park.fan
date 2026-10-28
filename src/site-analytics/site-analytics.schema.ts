import {
  PAGE_VIEW_LOCALES,
  type PageViewLocale,
} from "../page-views/page-view.constants";

/**
 * The closed schema of the site statistic: what a hit may say, and every
 * event with its properties. A hit outside it is dropped, one by one, so an
 * unauthenticated write can never become a key-value store. The frontend
 * mirrors these lists as types (`lib/analytics/site-analytics.ts` in park.fan).
 * How to add an event: docs/frontend/site-analytics.md.
 */

/** The site's six languages, the same list the page-view counter uses. */
export const SITE_LOCALES = PAGE_VIEW_LOCALES;
export type SiteLocale = PageViewLocale;

/** What kind of page a hit was sent from (the frontend's `sitePageTypeOf`). */
export const SITE_PAGE_TYPES = [
  "home",
  "park",
  "park_calendar",
  "park_chapter",
  "attraction",
  "geo",
  "guide",
  "guide_index",
  "news",
  "news_index",
  "glossary",
  "glossary_index",
  "search",
  "planner",
  "favorites",
  "other",
] as const;
export type SitePageType = (typeof SITE_PAGE_TYPES)[number];

/** Below 768, 768–1023 and from 1024 CSS pixels of `window.innerWidth`. */
export const SITE_VIEWPORTS = ["mobile", "tablet", "desktop"] as const;
export type SiteViewport = (typeof SITE_VIEWPORTS)[number];

/** Which search field a search event came from. */
export const SEARCH_SURFACES = [
  "hero",
  "header",
  "keyboard",
  "search_page",
  "listing_guides",
  "listing_news",
  "guides_panel",
] as const;
export type SearchSurface = (typeof SEARCH_SURFACES)[number];

export const SEARCH_RESULT_KINDS = [
  "park",
  "attraction",
  "show",
  "restaurant",
  "location",
  "glossary",
  "guide",
  "news",
] as const;

/** A 1-based result position, bucketed. */
export const SEARCH_POSITIONS = ["1", "2", "3", "4-5", "6-10", "11+"] as const;

export const SEARCH_BROWSE_ITEMS = [
  "nearby_park",
  "in_park_ride",
  "popular_park",
  "news",
  "guide",
] as const;

const FAVORITE_TYPES = ["park", "attraction", "show", "restaurant"] as const;

/**
 * `location.pathname`, lowercased, without query, hash or trailing slash: a
 * language and up to eight slug-like segments. Anything else (an encoded
 * character, a query someone pasted) is not a page of the site.
 */
export const SITE_PATH_PATTERN =
  /^\/(en|de|nl|fr|es|it)(\/[a-z0-9][a-z0-9._~-]{0,99}){0,8}$/;

/** A referrer host (`www.` stripped) or a `utm_source` value. */
export const SITE_REFERRER_PATTERN = /^[a-z0-9][a-z0-9.-]{0,99}$/;

/** A glossary term or category id. */
export const SITE_SLUG_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,58}[a-z0-9])?$/;

/** The longest path {@link SITE_PATH_PATTERN} admits: "/de" and 8 × (1 + 100). */
export const SITE_PATH_MAX_LENGTH = 3 + 8 * 101;

/** The most properties one event carries; stored as `p1`…`p3` in this order. */
export const MAX_EVENT_PROPS = 3;

/** One property: a closed list, one of the six languages, or a slug. */
export type SiteEventProp =
  | { name: string; values: readonly string[] }
  | { name: string; kind: "locale" }
  | { name: string; kind: "slug" };

/** What an event's `entity` may name: a park, a ride or a show. */
export const SITE_ENTITY_KINDS = ["park", "attraction", "show"] as const;
export type SiteEntityKind = (typeof SITE_ENTITY_KINDS)[number];

export interface SiteEventSpec {
  props: readonly SiteEventProp[];
  /**
   * The kind of entity the event may carry, decided from its validated
   * properties; `null` drops the entity and keeps the hit. Absent: the event
   * never carries one.
   */
  entity?: (props: readonly string[]) => SiteEntityKind | null;
}

const oneOf = (name: string, values: readonly string[]): SiteEventProp => ({
  name,
  values,
});
const surface = oneOf("surface", SEARCH_SURFACES);
const position = oneOf("position", SEARCH_POSITIONS);
const always = (kind: SiteEntityKind) => (): SiteEntityKind => kind;
/** A restaurant has no page of its own to list, so its id is not kept. */
const favoriteEntity = ([type]: readonly string[]): SiteEntityKind | null =>
  type === "restaurant" ? null : (type as SiteEntityKind);

/**
 * Every event the API counts. The property order is the storage order
 * (`p1`, `p2`, `p3`), so a property is only ever appended, never inserted:
 * an earlier position would shift the meaning of the rows already stored.
 */
export const SITE_EVENTS = {
  search_opened: { props: [surface] },
  search_typed: { props: [surface] },
  search_no_results: { props: [surface] },
  search_result_clicked: {
    props: [surface, oneOf("result", SEARCH_RESULT_KINDS), position],
  },
  search_browse_clicked: {
    props: [surface, oneOf("item", SEARCH_BROWSE_ITEMS), position],
  },
  search_view_all: { props: [surface] },
  favorite_add: {
    props: [oneOf("type", FAVORITE_TYPES)],
    entity: favoriteEntity,
  },
  favorite_remove: {
    props: [oneOf("type", FAVORITE_TYPES)],
    entity: favoriteEntity,
  },
  nearby_permission: { props: [oneOf("result", ["granted", "denied"])] },
  nearby_parks_loaded: {
    props: [
      oneOf("type", ["nearby_parks", "in_park"]),
      oneOf("source", ["gps", "ip"]),
    ],
    entity: ([type]) => (type === "in_park" ? "park" : null),
  },
  language_switched: {
    props: [
      { name: "from", kind: "locale" },
      { name: "to", kind: "locale" },
    ],
  },
  theme_toggled: { props: [oneOf("theme", ["light", "dark", "system"])] },
  tab_changed: {
    props: [
      oneOf("tab", ["attractions", "map", "shows", "restaurants", "weather"]),
    ],
    entity: always("park"),
  },
  location_banner_clicked: { props: [] },
  blog_toast_opened: { props: [] },
  glossary_term_viewed: { props: [{ name: "term", kind: "slug" }] },
  glossary_category_filtered: { props: [{ name: "category", kind: "slug" }] },
  glossary_searched: { props: [] },
  preferred_source_clicked: { props: [] },
  ride_alert_set: {
    props: [oneOf("source", ["card", "central"])],
    entity: always("attraction"),
  },
  ride_alert_removed: { props: [], entity: always("attraction") },
  show_follow_add: {
    props: [oneOf("source", ["card", "panel"])],
    entity: always("show"),
  },
  show_follow_remove: { props: [], entity: always("show") },
  planner_opened: {
    props: [
      oneOf("source", [
        "tab",
        "header",
        "park-header",
        "calendar-day",
        "wizard",
        "plan-list",
        "shared-link",
        "home-hero",
        "kids-page",
      ]),
    ],
  },
  plan_day_started: { props: [], entity: always("park") },
  plan_optimized: { props: [], entity: always("park") },
  planner_closed_empty: { props: [] },
  attraction_filter_used: {
    props: [
      oneOf("filter", [
        "open",
        "off_season",
        "wet",
        "fast_pass",
        "single_rider",
        "covered",
        "height",
        "sort_wait",
      ]),
    ],
    entity: always("park"),
  },
  covered_ride_opened: { props: [] },
  compass_viewed: { props: [] },
  compass_heading_on: { props: [] },
  compass_ride_pinned: { props: [] },
  compass_ride_opened: { props: [oneOf("from", ["bar", "list"])] },
  compass_pill_clicked: { props: [] },
  outbound_link_clicked: {
    props: [oneOf("target", ["park_website", "tickets", "maps", "other"])],
    entity: always("park"),
  },
  pwa_installed: { props: [] },
} as const satisfies Record<string, SiteEventSpec>;

export type SiteEventName = keyof typeof SITE_EVENTS;

/** Every event name, in schema order. */
export const SITE_EVENT_NAMES = Object.keys(SITE_EVENTS) as SiteEventName[];

/** The longest event name, which sizes the column. */
export const SITE_EVENT_NAME_MAX_LENGTH = 32;

export function isSiteEventName(name: unknown): name is SiteEventName {
  return (
    typeof name === "string" &&
    Object.prototype.hasOwnProperty.call(SITE_EVENTS, name)
  );
}

/** The spec of an event, typed loosely enough to iterate. */
export function siteEventSpec(name: SiteEventName): SiteEventSpec {
  return SITE_EVENTS[name];
}

/** The values a property admits, or null for a slug (an open set). */
export function propValues(prop: SiteEventProp): readonly string[] | null {
  if ("values" in prop) return prop.values;
  return prop.kind === "locale" ? SITE_LOCALES : null;
}

/** Whether `value` is admissible for `prop`. */
export function isPropValue(prop: SiteEventProp, value: unknown): boolean {
  if (typeof value !== "string") return false;
  const values = propValues(prop);
  return values ? values.includes(value) : SITE_SLUG_PATTERN.test(value);
}

/** The search events, in the order `/search` reports them. */
export const SEARCH_EVENTS = {
  opened: "search_opened",
  typed: "search_typed",
  resultClicks: "search_result_clicked",
  browseClicks: "search_browse_clicked",
  noResults: "search_no_results",
  viewAll: "search_view_all",
} as const satisfies Record<string, SiteEventName>;
