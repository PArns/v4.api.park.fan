/**
 * Browser-like HTTP headers for outbound requests to third-party data sources.
 *
 * The default `axios/<version>` User-Agent (and undici's `node`) is an obvious
 * bot signature that some providers — especially Cloudflare-fronted ones such
 * as wartezeiten.app and the ThemeParks Wiki — rate-limit or block more
 * aggressively. Presenting a neutral, current desktop-Chrome fingerprint keeps
 * us looking like a normal client.
 *
 * Centralised here so the Chrome version is bumped in ONE place across every
 * client instead of being copy-pasted (open-meteo used to carry its own copy).
 */
export const BROWSER_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36";

/**
 * Default header set for outbound third-party requests: a browser User-Agent
 * plus the Accept / Accept-Language a real Chrome sends, so we don't read as a
 * bare script if a provider inspects more than just the UA.
 *
 * Spread into each `axios.create({ headers })` (or merged with provider-specific
 * headers like an `apikey`). Per-request headers still override these.
 */
export const BROWSER_HEADERS: Readonly<Record<string, string>> = {
  "User-Agent": BROWSER_USER_AGENT,
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
};
