/**
 * park.fan's own relays for one park, read the way the park page reads them.
 *
 * The browser never asks this API for a calendar day: it asks park.fan's
 * `/api/parks/<geo>/calendar` and `/best-days`, which Vercel keeps until the
 * forecast ring and Cloudflare for an hour more. A copy there can say what the
 * API no longer does, and only a request through the same URL sees it.
 * docs/analytics/display-consistency.md#the-relays.
 */
import { getFrontendUrl } from "../../config/oauth.config";
import { isRevalidationEnabled } from "../../config/revalidation.config";
import { frontendParkPath } from "../../common/utils/frontend-url.util";
import type { GeocodedEntity } from "../../common/utils/url.util";
import { monthBounds, monthOf } from "../../trip-regions/last-year-snapshot";
import type { RelayCopy } from "./display-consistency.rules";

/** A relay answers from a copy in milliseconds and from the API in a second or two. */
export const RELAY_TIMEOUT_MS = 10_000;

/** Named, so park.fan's Cloudflare rules and logs can tell these reads apart. */
export const RELAY_USER_AGENT = "api.park.fan-display-consistency";

/** One relay response: its JSON and what it said about its copy. */
export interface RelayRead {
  body: { meta?: { computedAt?: string }; days: unknown[] };
  copy: RelayCopy;
}

/** The URLs of the park's relays, as the park page asks for them. */
export interface RelayUrls {
  /** The page whose requests these are, sent as `Referer`. */
  page: string;
  /** One per month the dates touch, first to last day, as the calendar grid asks for one. */
  calendar: string[];
  bestDays: string;
}

/** The first and last day of each month `dates` fall in, in order. */
export function monthRanges(
  dates: readonly string[],
): Array<{ first: string; last: string }> {
  return [...new Set(dates.map(monthOf))].sort().map(monthBounds);
}

/**
 * The relays to read for these dates, or null where there are none to read: a
 * park without a geo path has no page, and an API that does not ring the
 * frontend (`REVALIDATE_SECRET` unset: dev, CI) is not the API behind its
 * relays, so they would be compared with another API's figures.
 */
export function relayUrls(
  park: GeocodedEntity,
  dates: readonly string[],
): RelayUrls | null {
  const path = frontendParkPath(park);
  if (!path || !isRevalidationEnabled()) return null;
  const site = getFrontendUrl();
  return {
    page: `${site}${path}`,
    calendar: monthRanges(dates).map(
      ({ first, last }) =>
        `${site}/api${path}/calendar?from=${first}&to=${last}`,
    ),
    bestDays: `${site}/api${path}/best-days`,
  };
}

function copyOf(headers: Headers): RelayCopy {
  const age = Number(headers.get("age"));
  const date = headers.get("date");
  const cloudflare = headers.get("cf-cache-status");
  const vercel = headers.get("x-vercel-cache");
  return {
    ...(headers.has("age") && Number.isFinite(age) && { age }),
    ...(date && { date }),
    ...(cloudflare && { cloudflare }),
    ...(vercel && { vercel }),
  };
}

/**
 * Reads one relay as the park page does. Whatever is not the relay's JSON
 * throws, so the check lists it as a failure and never as a mismatch: no
 * answer in time, a Cloudflare challenge, a status other than 200, a body
 * that is not JSON or has no days.
 *
 * `Referer` is the park page: the relays answer park.fan's own pages only
 * (`isSameOriginRequest` in the frontend).
 */
export async function readRelay(url: string, page: string): Promise<RelayRead> {
  const target = new URL(url);
  const name = `${target.pathname}${target.search}`;
  const response = await fetch(url, {
    signal: AbortSignal.timeout(RELAY_TIMEOUT_MS),
    headers: {
      "User-Agent": RELAY_USER_AGENT,
      Referer: page,
      Accept: "application/json",
    },
  }).catch((error: unknown) => {
    throw new Error(
      `${name}: ${error instanceof Error ? error.message : String(error)}`,
    );
  });
  const mitigated = response.headers.get("cf-mitigated");
  if (mitigated) {
    throw new Error(`${name}: Cloudflare ${mitigated} (${response.status})`);
  }
  if (response.status !== 200) {
    throw new Error(`${name} answered ${response.status}`);
  }
  const type = response.headers.get("content-type") ?? "";
  if (!type.includes("application/json")) {
    throw new Error(`${name} answered ${type || "no content type"}, not JSON`);
  }
  const body = (await response.json().catch(() => {
    throw new Error(`${name} answered JSON that does not parse`);
  })) as Partial<RelayRead["body"]> | null;
  if (!body || !Array.isArray(body.days)) {
    throw new Error(`${name} answered without days`);
  }
  return {
    body: body as RelayRead["body"],
    copy: copyOf(response.headers),
  };
}
