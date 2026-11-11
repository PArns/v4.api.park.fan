/**
 * The terms of use, on park.fan because the frontend owns the legal pages:
 * free for non-commercial use, forecasts only against an API key, commercial
 * use of the forecasts only under a written agreement, the live data under its
 * sources' terms. The same address as `DATA_TERMS_URL` in the frontend's
 * `lib/agents/licensing.ts`, which its RSL licence and API catalog link to.
 */
export const DATA_TERMS_URL = "https://park.fan/en/impressum#data-use";

/**
 * Every data key starts with this, so a key pasted into the wrong place is
 * recognisable and a secret scanner can match it. Distinct from the admin
 * tokens' `pfa_`/`pfo_`: a data key opens forecasts, never the back office.
 */
export const API_KEY_PREFIX = "pfk_";

/** The request header a data key travels in. `Authorization: Bearer` works too. */
export const API_KEY_HEADER = "x-api-key";

/**
 * What a key was issued for; text in the table, so another kind needs no
 * migration. `internal` is park.fan's own frontend.
 */
export const API_KEY_USAGES = [
  "non-commercial",
  "commercial",
  "internal",
] as const;
export type ApiKeyUsage = (typeof API_KEY_USAGES)[number];

/** Characters of the key kept in clear, enough to tell two keys apart in a list. */
export const API_KEY_VISIBLE_CHARS = API_KEY_PREFIX.length + 6;

/**
 * The hourly allowance a new key gets when the admin names none: room for an
 * app that refreshes a few dozen parks every quarter hour, not for a mirror.
 */
export const DEFAULT_REQUESTS_PER_HOUR = 1000;

/** The response headers that report a key's hourly allowance. */
export const API_KEY_LIMIT_HEADERS = {
  limit: "X-API-Key-Hourly-Limit",
  remaining: "X-API-Key-Hourly-Remaining",
  reset: "X-API-Key-Hourly-Reset",
} as const;

/**
 * Whether the throttle bypass key (`x-auth-key`) still opens the forecast
 * routes. It did before the frontend had a data key of its own; once the
 * frontend sends one (`API_DATA_KEY`), `FORECAST_BYPASS_KEY=off` leaves the
 * bypass key to what its name says, the rate limits.
 */
export function bypassKeyOpensForecasts(): boolean {
  return process.env.FORECAST_BYPASS_KEY !== "off";
}
