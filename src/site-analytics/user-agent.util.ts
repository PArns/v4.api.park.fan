/**
 * Browser and operating-system families from a User-Agent, for the site
 * statistic's breakdown. Families only, never a version or a device model:
 * the closed lists below are all the statistic keeps, and the User-Agent
 * itself is never stored.
 *
 * In-house rather than a parser library because seven and seven families are
 * a dozen ordered tests, and a full parser would be a dependency for a
 * breakdown nobody reads to the minor version.
 */

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
 * First match wins, so the order is the point: every Chromium browser also
 * says `Chrome/`, and every iOS browser also says `Safari/`. Safari is what is
 * left of WebKit with a `Safari/` token; an iOS in-app view (Instagram,
 * Facebook) has none and stays `other`.
 */
const BROWSER_RULES: ReadonlyArray<[BrowserFamily, RegExp]> = [
  ["samsung", /SamsungBrowser\//],
  ["edge", /\bEdg(?:e|A|iOS)?\//],
  ["opera", /\bOPR\/|\bOPiOS\/|\bOPT\/|\bOpera\b/],
  ["firefox", /\bFirefox\/|\bFxiOS\//],
  ["chrome", /\bChrome\/|\bCriOS\/|\bChromium\//],
  ["safari", /AppleWebKit\/.*\bSafari\//],
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
  rules: ReadonlyArray<[T, RegExp]>,
  userAgent: string,
  fallback: T,
): T {
  for (const [family, pattern] of rules) {
    if (pattern.test(userAgent)) return family;
  }
  return fallback;
}

/** The browser and OS family of a User-Agent; `other` for what is not known. */
export function parseUserAgent(userAgent: string | null | undefined): {
  browser: BrowserFamily;
  os: OsFamily;
} {
  const ua = userAgent ?? "";
  return {
    browser: firstMatch(BROWSER_RULES, ua, "other"),
    os: firstMatch(OS_RULES, ua, "other"),
  };
}
