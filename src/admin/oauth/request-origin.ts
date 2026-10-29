import type { GeoipService } from "../../geoip/geoip.service";
import {
  parseUserAgent,
  type BrowserFamily,
} from "../../site-analytics/user-agent.util";

/**
 * Where a request came from, as far as the consent page compares two of them:
 * the country and the browser family, never the address or the User-Agent
 * itself. A link someone else started and sent over arrives with a different
 * country or browser than the admin who opens it; a connection the admin
 * started in this browser arrives with the same.
 */
export interface RequestOrigin {
  /** ISO 3166-1 alpha-2, lowercase (`de`); null when GeoIP cannot place the address. */
  country: string | null;
  /** Null without a User-Agent. */
  browser: BrowserFamily | null;
}

/** The {@link RequestOrigin} of an address and a User-Agent; neither is kept. */
export function requestOrigin(
  geoip: Pick<GeoipService, "lookupCountry">,
  ip: string | null,
  userAgent: string | string[] | null | undefined,
): RequestOrigin {
  const agent = Array.isArray(userAgent) ? userAgent[0] : userAgent;
  return {
    country: ip ? geoip.lookupCountry(ip) : null,
    browser: agent ? parseUserAgent(agent).browser : null,
  };
}
