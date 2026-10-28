import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import {
  SITE_LOCALES,
  SITE_PAGE_TYPES,
  SITE_PATH_PATTERN,
  SITE_REFERRER_PATTERN,
  SITE_VIEWPORTS,
  isPropValue,
  isSiteEventName,
  siteEventSpec,
  type SiteEntityKind,
  type SiteEventName,
  type SiteLocale,
  type SitePageType,
  type SiteViewport,
} from "./site-analytics.schema";

/** The most hits one request may carry; more is a malformed body. */
export const MAX_HITS_PER_REQUEST = 20;

/** The wire format's version; anything else is a malformed body. */
export const COLLECT_BODY_VERSION = 1;

interface ValidHitBase {
  locale: SiteLocale;
  path: string;
  pageType: SitePageType;
  viewport: SiteViewport;
}

export interface ValidPageviewHit extends ValidHitBase {
  t: "pageview";
  /** Counted only when this page view starts a visit. */
  referrer: string | null;
}

export interface ValidEventHit extends ValidHitBase {
  t: "event";
  name: SiteEventName;
  /** The property values in schema order, one per declared property. */
  props: string[];
  entity: { id: string; kind: SiteEntityKind } | null;
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

/**
 * One hit checked against the closed schema, or null to drop it. A field the
 * schema does not know is ignored; a property is not: an unknown, missing or
 * out-of-list property drops the hit. An `entity` the event may not carry, or
 * that is no lowercase UUID, is dropped and the hit kept, as is a referrer
 * that is malformed or the site itself.
 */
export function validateHit(raw: unknown): ValidHit | null {
  if (!isRecord(raw)) return null;
  const { t, locale, path, pageType, viewport } = raw;
  if (!isOneOf(SITE_LOCALES, locale)) return null;
  if (typeof path !== "string" || !SITE_PATH_PATTERN.test(path)) return null;
  // The language is the path's first segment; a hit that disagrees with its
  // own path is not one the site sends.
  if (path.split("/")[1] !== locale) return null;
  if (!isOneOf(SITE_PAGE_TYPES, pageType)) return null;
  if (!isOneOf(SITE_VIEWPORTS, viewport)) return null;
  const base = { locale, path, pageType, viewport };

  if (t === "pageview") {
    return { t, ...base, referrer: validReferrer(raw.referrer) };
  }
  if (t !== "event" || !isSiteEventName(raw.name)) return null;

  const spec = siteEventSpec(raw.name);
  const given = raw.props ?? {};
  if (!isRecord(given)) return null;
  const names = Object.keys(given);
  if (names.length !== spec.props.length) return null;
  const props: string[] = [];
  for (const prop of spec.props) {
    if (!Object.prototype.hasOwnProperty.call(given, prop.name)) return null;
    const value = given[prop.name];
    if (!isPropValue(prop, value)) return null;
    props.push(value as string);
  }

  const kind = spec.entity?.(props) ?? null;
  const entity =
    kind && isCanonicalUuid(raw.entity) ? { id: raw.entity, kind } : null;
  return { t, ...base, name: raw.name, props, entity };
}

/** A referrer, or null for none, a malformed one or the site itself. */
function validReferrer(value: unknown): string | null {
  if (typeof value !== "string" || !SITE_REFERRER_PATTERN.test(value))
    return null;
  if (value === "park.fan" || value.endsWith(".park.fan")) return null;
  return value;
}
