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

/** More than the whole `Accept-Language` of any real browser. */
const MAX_ACCEPTED_HEADER_LENGTH = 256;

/**
 * Every language an `Accept-Language` accepts: the two-letter primary subtag
 * of each tag, lowercased, without the tags weighted `q=0` (refused) and
 * without `*`. Empty without a header. Read for one decision and kept nowhere.
 */
export function acceptedLanguages(
  header: string | null | undefined,
): Set<string> {
  const languages = new Set<string>();
  for (const tag of (header ?? "")
    .slice(0, MAX_ACCEPTED_HEADER_LENGTH)
    .split(",")) {
    const [range, ...params] = tag.split(";");
    const weight = params
      .map((param) => param.trim().toLowerCase())
      .find((param) => param.startsWith("q="));
    if (weight !== undefined && !(Number(weight.slice(2)) > 0)) continue;
    const primary = range.trim().split("-")[0].toLowerCase();
    if (/^[a-z]{2}$/.test(primary)) languages.add(primary);
  }
  return languages;
}
