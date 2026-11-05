import { UNKNOWN_BROWSER_LANGUAGE } from "./site-analytics.constants";

/** More than the first tag of any real `Accept-Language` needs. */
const MAX_HEADER_LENGTH = 64;

/**
 * The reader's preferred language as the browser states it: the primary
 * subtag of the first `Accept-Language` tag, lowercased (`de-AT;q=1, en` →
 * `de`), and `zz` for anything that is not a two-letter code. The site's own
 * language is the path's; this says what the browser would have liked.
 */
export function browserLanguage(header: string | null | undefined): string {
  const first = (header ?? "").slice(0, MAX_HEADER_LENGTH).split(",")[0];
  const primary = first.split(";")[0].trim().split("-")[0].toLowerCase();
  return /^[a-z]{2}$/.test(primary) ? primary : UNKNOWN_BROWSER_LANGUAGE;
}
