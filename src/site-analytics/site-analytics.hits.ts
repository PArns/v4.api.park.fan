import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import {
  PAGE_KEY_PATTERN,
  PAGE_VIEW_KINDS,
  type PageViewKind,
} from "../page-views/page-view.constants";
import {
  MAX_EVENT_PROPS,
  PAGE_READ_EVENT,
  SITE_LOCALES,
  SITE_VIEWPORTS,
  type SiteLocale,
} from "./site-analytics.constants";

/**
 * What a hit may look like. There is no event schema: a new event, property
 * or page type is a call in the frontend and nothing here. Every field is
 * checked against a pattern instead, and the cardinality guards in the
 * collector keep a script from turning that freedom into rows. How to add an
 * event, and the rule that no property may carry free text:
 * docs/frontend/site-analytics.md.
 */

/** The most hits one request may carry; more is a malformed body. */
export const MAX_HITS_PER_REQUEST = 20;

/** The wire format's version; anything else is a malformed body. */
export const COLLECT_BODY_VERSION = 1;

/**
 * `location.pathname`, lowercased, without query, hash or trailing slash: a
 * language and up to eight slug-like segments. Anything else (an encoded
 * character, a query someone pasted) is not a page of the site.
 */
export const SITE_PATH_PATTERN =
  /^\/(en|de|nl|fr|es|it)(\/[a-z0-9][a-z0-9._~-]{0,99}){0,8}$/;

/** The longest path {@link SITE_PATH_PATTERN} admits: "/de" and 8 × (1 + 100). */
export const SITE_PATH_MAX_LENGTH = 3 + 8 * 101;

/** What kind of page a hit came from, as the frontend derives it. */
export const PAGE_TYPE_PATTERN = /^[a-z][a-z0-9_]{0,31}$/;

/** A referrer host (`www.` stripped) or a `utm_source` value. */
export const REFERRER_PATTERN = /^[a-z0-9][a-z0-9.-]{0,99}$/;

/** A `utm_campaign` value, lowercased. */
export const CAMPAIGN_PATTERN = /^[a-z0-9][a-z0-9._+-]{0,99}$/;

/** An event name. */
export const EVENT_NAME_PATTERN = /^[a-z][a-z0-9_]{1,47}$/;

/** A property key. A leading underscore is the API's own (`_overflow`). */
export const PROP_KEY_PATTERN = /^[a-z][a-z0-9_]{0,31}$/;

/**
 * A property's string value: letters and digits in any script, and a little
 * punctuation, up to 120 characters (a post's `translationKey` fits). No `&`
 * and no `=`, which keeps the canonical `k=v&k=v` key unambiguous.
 */
export const PROP_VALUE_PATTERN = /^[\p{L}\p{N}][\p{L}\p{N} ._:/+#-]{0,119}$/u;

interface ValidHitBase {
  locale: SiteLocale;
  path: string;
  pageType: string;
  viewport: (typeof SITE_VIEWPORTS)[number];
}

export interface ValidPageviewHit extends ValidHitBase {
  t: "pageview";
  /** Counted only when this page view opens a visit. */
  referrer: string | null;
  campaign: string | null;
}

export interface ValidEventHit extends ValidHitBase {
  t: "event";
  name: string;
  /** The kept properties, keys in sorted order. */
  props: Record<string, string>;
  /** `k=v` pairs in key order, joined by `&`; `''` without properties. */
  propsKey: string;
  /** A park's, ride's, show's or restaurant's lowercase UUID. */
  entity: string | null;
}

export type ValidHit = ValidPageviewHit | ValidEventHit;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isOneOf = <T extends string>(
  list: readonly T[],
  value: unknown,
): value is T => typeof value === "string" && list.includes(value as T);

/**
 * The hits of a collect body, or null when the body is not JSON or not
 * `{ v: 1, hits: [1..20] }` — the one case the endpoint answers 400. A
 * `text/plain` body (what `sendBeacon` sends, so the browser asks no
 * preflight) arrives as a string; `application/json` arrives parsed.
 */
export function parseCollectBody(body: unknown): unknown[] | null {
  let parsed = body;
  if (typeof body === "string") {
    try {
      parsed = JSON.parse(body);
    } catch {
      return null;
    }
  }
  if (!isRecord(parsed) || parsed.v !== COLLECT_BODY_VERSION) return null;
  const { hits } = parsed;
  if (!Array.isArray(hits)) return null;
  if (hits.length < 1 || hits.length > MAX_HITS_PER_REQUEST) return null;
  return hits;
}

/** A property value as stored, or null for one that is not admissible. */
export function propValue(value: unknown): string | null {
  if (typeof value === "boolean") return String(value);
  if (typeof value === "number")
    return Number.isFinite(value) ? String(value) : null;
  if (typeof value !== "string") return null;
  return PROP_VALUE_PATTERN.test(value) ? value : null;
}

/** The canonical key of a set of properties: `k=v` in key order, `&`-joined. */
export function propsKeyOf(props: Record<string, string>): string {
  return Object.keys(props)
    .sort()
    .map((key) => `${key}=${props[key]}`)
    .join("&");
}

/** The properties a canonical key stands for. */
export function propsOfKey(key: string): Record<string, string> {
  const props: Record<string, string> = {};
  if (!key) return props;
  for (const pair of key.split("&")) {
    const at = pair.indexOf("=");
    props[pair.slice(0, at)] = pair.slice(at + 1);
  }
  return props;
}

/**
 * The kept properties of an event: at most {@link MAX_EVENT_PROPS}, in key
 * order, each with a valid key and value. A property that does not fit is
 * dropped alone; the hit stays.
 */
export function sanitizeProps(raw: unknown): Record<string, string> {
  const props: Record<string, string> = {};
  if (!isRecord(raw)) return props;
  for (const key of Object.keys(raw).sort()) {
    if (Object.keys(props).length >= MAX_EVENT_PROPS) break;
    if (!PROP_KEY_PATTERN.test(key)) continue;
    const value = propValue(raw[key]);
    if (value !== null) props[key] = value;
  }
  return props;
}

/**
 * One hit checked field by field, or null to drop it. A field the wire format
 * does not know is ignored. A malformed referrer, campaign, property or entity
 * is dropped and the hit kept; a malformed base field or name drops the hit.
 */
export function validateHit(raw: unknown): ValidHit | null {
  if (!isRecord(raw)) return null;
  const { t, locale, path, pageType, viewport } = raw;
  if (!isOneOf(SITE_LOCALES, locale)) return null;
  if (typeof path !== "string" || !SITE_PATH_PATTERN.test(path)) return null;
  // The language is the path's first segment; a hit that disagrees with its
  // own path is not one the site sends.
  if (path.split("/")[1] !== locale) return null;
  if (typeof pageType !== "string" || !PAGE_TYPE_PATTERN.test(pageType))
    return null;
  if (!isOneOf(SITE_VIEWPORTS, viewport)) return null;
  const base = { locale, path, pageType, viewport };

  if (t === "pageview") {
    return {
      t,
      ...base,
      referrer: validReferrer(raw.referrer),
      campaign: validCampaign(raw.campaign),
    };
  }
  if (t !== "event") return null;
  const { name } = raw;
  if (typeof name !== "string" || !EVENT_NAME_PATTERN.test(name)) return null;

  let props = sanitizeProps(raw.props);
  let entity = isCanonicalUuid(raw.entity) ? raw.entity : null;
  if (name === PAGE_READ_EVENT) {
    const read = pageRead(props, entity);
    if (!read) return null;
    ({ props, entity } = read);
  }
  return { t, ...base, name, props, propsKey: propsKeyOf(props), entity };
}

/**
 * `page_read` feeds the public rankings, so it keeps the page-view counter's
 * key rules: `kind` is one of the four; a post is named by its
 * `translationKey` and carries no entity; a park or a ride is named by its
 * UUID in `entity` and nothing else. A free-form key under `park` would reach
 * a uuid column and fail the admin's overview (PAR-1089).
 */
function pageRead(
  props: Record<string, string>,
  entity: string | null,
): { props: Record<string, string>; entity: string | null } | null {
  const kind = props.kind as PageViewKind;
  if (!PAGE_VIEW_KINDS.includes(kind)) return null;
  if (kind === "park" || kind === "attraction") {
    return entity ? { props: { kind }, entity } : null;
  }
  const key = props.key;
  if (!key || !PAGE_KEY_PATTERN.test(key)) return null;
  return { props: { key, kind }, entity: null };
}

/** A referrer, or null for none, a malformed one or the site itself. */
function validReferrer(value: unknown): string | null {
  if (typeof value !== "string" || !REFERRER_PATTERN.test(value)) return null;
  if (value === "park.fan" || value.endsWith(".park.fan")) return null;
  return value;
}

/** A campaign, lowercased, or null. */
function validCampaign(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const lower = value.toLowerCase();
  return CAMPAIGN_PATTERN.test(lower) ? lower : null;
}
