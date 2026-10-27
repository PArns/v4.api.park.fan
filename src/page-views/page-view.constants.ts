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
 * A `translationKey`: lowercase, digits and inner hyphens. Anything else is
 * refused before it reaches a key or a row.
 */
export const PAGE_KEY_PATTERN = /^[a-z0-9](?:[a-z0-9-]{0,118}[a-z0-9])?$/;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** The kinds keyed by an entity's UUID rather than by a `translationKey`. */
const UUID_KINDS: readonly PageViewKind[] = ["park", "attraction"];

/**
 * Whether `key` can name a page of `kind`. A park or a ride is a UUID and
 * nothing else: a free-form key there would reach an `IN (…)` on a uuid
 * column and fail the admin's whole overview.
 */
export function isPageKeyFor(kind: PageViewKind, key: unknown): key is string {
  if (typeof key !== "string") return false;
  return UUID_KINDS.includes(kind)
    ? UUID_PATTERN.test(key)
    : PAGE_KEY_PATTERN.test(key);
}

/**
 * The windows a public ranking may ask for: the two the site shows and the
 * admin's longest. A free `days` and `limit` would be a hundred thousand
 * distinct URLs, each a miss at the edge and an aggregate here.
 */
export const TOP_WINDOW_DAYS = [7, 30, 90] as const;

/** The most entries one public ranking returns. */
export const MAX_TOP_LIMIT = 50;

/** The longest window the admin may ask for. */
export const MAX_ADMIN_WINDOW_DAYS = 90;

/**
 * How often one reader may count the same page on the same day. A reader is
 * an address (an IPv6 /64) and a User-Agent, so a household or an office
 * behind one address still counts per browser. Above one for a reader who
 * comes back; low, because a script rotating its User-Agent meets the next
 * cap.
 */
export const MAX_VIEWS_PER_READER_PER_DAY = 3;

/**
 * How many views one address (an IPv6 /64) may count per UTC day across all
 * pages. A carrier gateway shares one address among many readers, so this is
 * generous; it is what keeps one machine from writing a row for every key it
 * can make up.
 */
export const MAX_VIEWS_PER_ADDRESS_PER_DAY = 500;
