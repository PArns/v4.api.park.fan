/**
 * Browser and operating-system families and the device class from a
 * User-Agent, for the site statistic's breakdown. Families and a class only,
 * never a version or a device model: the closed lists below are all the
 * statistic keeps, and the User-Agent itself is never stored.
 *
 * In-house rather than a parser library because seven and seven families and
 * three classes are a few dozen ordered tests, and a full parser would be a
 * dependency for a breakdown nobody reads to the minor version.
 */
import type { SiteDevice } from "./site-analytics.constants";

export const BROWSER_FAMILIES = [
  "chrome",
  "safari",
  "firefox",
  "edge",
  "samsung",
  "opera",
  "other",
] as const;
export type BrowserFamily = (typeof BROWSER_FAMILIES)[number];

export const OS_FAMILIES = [
  "ios",
  "android",
  "windows",
  "macos",
  "linux",
  "chromeos",
  "other",
] as const;
export type OsFamily = (typeof OS_FAMILIES)[number];

/**
 * The longest User-Agent the parsers read. A real one is under 300
 * characters; cutting a longer one first keeps every test linear in a bound
 * the caller cannot raise.
 */
export const MAX_USER_AGENT_LENGTH = 512;

/** The part of a User-Agent the parsers read. */
function readable(userAgent: string | null | undefined): string {
  return (userAgent ?? "").slice(0, MAX_USER_AGENT_LENGTH);
}

/** A WebKit browser with a `Safari/` token after the engine's. */
function isWebKitSafari(userAgent: string): boolean {
  const engine = userAgent.indexOf("AppleWebKit/");
  return engine >= 0 && /\bSafari\//.test(userAgent.slice(engine));
}

/**
 * First match wins, so the order is the point: every Chromium browser also
 * says `Chrome/`, and every iOS browser also says `Safari/`. Safari is what is
 * left of WebKit with a `Safari/` token; an iOS in-app view (Instagram,
 * Facebook) has none and stays `other`.
 */
const BROWSER_RULES: ReadonlyArray<
  [BrowserFamily, RegExp | ((ua: string) => boolean)]
> = [
  ["samsung", /SamsungBrowser\//],
  ["edge", /\bEdg(?:e|A|iOS)?\//],
  ["opera", /\bOPR\/|\bOPiOS\/|\bOPT\/|\bOpera\b/],
  ["firefox", /\bFirefox\/|\bFxiOS\//],
  ["chrome", /\bChrome\/|\bCriOS\/|\bChromium\//],
  ["safari", isWebKitSafari],
];

/**
 * iOS before macOS (an iPhone says "like Mac OS X"), Android and ChromeOS
 * before Linux (both say `Linux`). An iPad in desktop mode reports itself as
 * a Mac and is counted as one: nothing in the header tells them apart.
 */
const OS_RULES: ReadonlyArray<[OsFamily, RegExp]> = [
  ["ios", /\b(?:iPhone|iPad|iPod)\b/],
  ["android", /\bAndroid\b/],
  ["chromeos", /\bCrOS\b/],
  ["windows", /\bWindows\b/],
  ["macos", /\bMac OS X\b|\bMacintosh\b/],
  ["linux", /\bLinux\b|\bX11\b/],
];

function firstMatch<T extends string>(
  rules: ReadonlyArray<[T, RegExp | ((ua: string) => boolean)]>,
  userAgent: string,
  fallback: T,
): T {
  for (const [family, rule] of rules) {
    if (typeof rule === "function" ? rule(userAgent) : rule.test(userAgent))
      return family;
  }
  return fallback;
}

/**
 * What marks a tablet. Tested before {@link MOBILE_TOKENS}: an iPad, a
 * Kindle's Silk and many Android tablets also say `Mobile`.
 */
const TABLET_TOKENS = ["iPad", "Tablet", "Kindle", "Silk", "PlayBook"] as const;

/**
 * What marks a phone. `Mobi` is the stem of `Mobile` (Android, iOS, Firefox),
 * `IEMobile` and `Opera Mobi`.
 */
const MOBILE_TOKENS = [
  "Mobi",
  "iPhone",
  "iPod",
  "Windows Phone",
  "Opera Mini",
] as const;

const saysAny = (userAgent: string, tokens: readonly string[]) =>
  tokens.some((token) => userAgent.includes(token));

/**
 * The device class of a User-Agent, from the header alone (no client hint,
 * which a browser sends only after the site asks for it). A tablet says so by
 * name (`iPad`, `Tablet`, `Kindle`, `Silk`, `PlayBook`), or is an Android
 * without `Mobile`, which Chrome and Firefox leave out on tablets; a phone
 * says `Mobi…`, `iPhone`, `iPod`, `Windows Phone` or `Opera Mini`; everything
 * else is a desktop. Plain `includes` tests on at most
 * {@link MAX_USER_AGENT_LENGTH} characters, so no input makes it slow.
 *
 * Known limits: an iPad on iPadOS 13 or later asks for desktop sites and
 * sends a Mac User-Agent, so it counts as a desktop; so does a phone or
 * tablet that asked for the desktop site (`X11`, `Macintosh`). An Android
 * device that says no `Mobile` and is no tablet (a television, a car) counts
 * as a tablet. Bots never get here: the collector drops them first.
 */
export function deviceClassOf(
  userAgent: string | null | undefined,
): SiteDevice {
  const ua = readable(userAgent);
  if (saysAny(ua, TABLET_TOKENS)) return "tablet";
  if (saysAny(ua, MOBILE_TOKENS)) return "mobile";
  return ua.includes("Android") ? "tablet" : "desktop";
}

/**
 * The browser and OS family and the device class of a User-Agent; `other` for
 * a family that is not known, `desktop` for a class that is not.
 */
export function parseUserAgent(userAgent: string | null | undefined): {
  browser: BrowserFamily;
  os: OsFamily;
  device: SiteDevice;
} {
  const ua = readable(userAgent);
  return {
    browser: firstMatch(BROWSER_RULES, ua, "other"),
    os: firstMatch(OS_RULES, ua, "other"),
    device: deviceClassOf(ua),
  };
}
