/**
 * What a counted page view can be about. A closed list on purpose: this is a
 * counter for a handful of rankings, not a general event store — Umami already
 * is one, and a free-form tag would turn an unauthenticated write endpoint into
 * a key-value store. A new ranking adds its kind here.
 *
 * - `article`, `news`: a blog post, keyed by its `translationKey`
 * - `park`, `attraction`: keyed by the entity's UUID, which survives a rename
 *   where a slug does not
 */
export const PAGE_VIEW_KINDS = [
  "article",
  "news",
  "park",
  "attraction",
] as const;
export type PageViewKind = (typeof PAGE_VIEW_KINDS)[number];

/**
 * The frontend's locales. Every ranking is per language: what German readers
 * read is not what Dutch readers read, and a site-wide list would be the
 * German list for everybody, since most traffic is German.
 */
export const PAGE_VIEW_LOCALES = ["en", "de", "nl", "fr", "es", "it"] as const;
export type PageViewLocale = (typeof PAGE_VIEW_LOCALES)[number];

/**
 * A `translationKey` or a UUID: lowercase, digits and inner hyphens. Anything
 * else is refused before it reaches a key or a row.
 */
export const PAGE_KEY_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,118}[a-z0-9])?$/;

/** The longest window a ranking may ask for. */
export const MAX_WINDOW_DAYS = 90;

/** The most entries one ranking request returns. */
export const MAX_TOP_LIMIT = 500;

/**
 * How often one address may count the same page on the same day. Above one,
 * because a household or an office shares an address; low, because the cap is
 * what keeps a script on one machine from moving a ranking.
 */
export const MAX_VIEWS_PER_ADDRESS_PER_DAY = 3;
