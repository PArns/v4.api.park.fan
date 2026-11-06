/**
 * Reads Efteling's maintenance calendar (PAR-1245).
 *
 * The page (`/en/park/information/in-maintenance`) is server-rendered: under a
 * header row "Attractions | Closed" sits `<div class="attractions">` with one
 * row per window — a ride link, then the dates as bare text nodes:
 *
 *   "until / November 6, 2026 / inclusive"              started before today
 *   "November 9 / until / December 4, 2026 / inclusive" a window
 *   "December 8, 2026"                                  a single day
 *
 * Regex rather than an HTML library: none is a dependency, the markup is this
 * regular, and the Six Flags height scraper reads its pages the same way. The
 * one thing a library would add, entity decoding, is two lines (`Pira&#241;a`).
 */

export const EFTELING_MAINTENANCE_URL =
  "https://www.efteling.com/en/park/information/in-maintenance";

export interface EftelingMaintenanceRow {
  /** The ride's name as the page prints it. */
  name: string;
  /** First day, `YYYY-MM-DD`; null when the page says only "until …". */
  from: string | null;
  /** Last day, `YYYY-MM-DD`, inclusive. */
  to: string;
}

export interface EftelingMaintenanceParse {
  rows: EftelingMaintenanceRow[];
  /** Rows whose date text matched no known form, as `name: text`. */
  unparsed: string[];
}

const MONTHS = [
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
];

/** "Attractions | Closed" header, then the list. Its absence is a layout change. */
const LIST =
  />\s*Closed\s*<\/div>\s*<\/div>\s*<div class="attractions">([\s\S]*?)<div class="container/;

const ROW =
  /<a href="\/en\/park\/attractions\/[^"]*"[^>]*>([^<]+)<\/a>\s*<\/div>\s*<div[^>]*>([^<]*)<\/div>/g;

const DAY = String.raw`([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?`;
const RANGE = new RegExp(
  String.raw`^(?:${DAY}\s+)?until\s+${DAY}(?:\s+inclusive)?$`,
  "i",
);
const SINGLE = new RegExp(String.raw`^${DAY}$`, "i");

/**
 * The page's rows. Throws when the "Closed" list is missing or holds no row
 * that parses — both mean the page changed, and a sync acting on half a read
 * would delete what it no longer sees.
 */
export function parseEftelingMaintenance(
  html: string,
): EftelingMaintenanceParse {
  const list = LIST.exec(html);
  if (!list) {
    throw new Error(
      'Efteling maintenance page: no "Closed" list found — the layout changed',
    );
  }

  const rows: EftelingMaintenanceRow[] = [];
  const unparsed: string[] = [];
  for (const [, rawName, rawText] of list[1].matchAll(ROW)) {
    const name = decodeEntities(rawName).replace(/\s+/g, " ").trim();
    const text = rawText.replace(/\s+/g, " ").trim();
    const window = parseWindowText(text);
    if (window) rows.push({ name, ...window });
    else unparsed.push(`${name}: ${text}`);
  }

  if (rows.length === 0) {
    throw new Error(
      `Efteling maintenance page: the "Closed" list parsed to zero windows ` +
        `(${unparsed.length} unreadable rows) — the layout changed`,
    );
  }
  return { rows, unparsed };
}

/**
 * One row's date text as a window, or null for an unknown form.
 *
 * A start without a year takes the end's year, or the year before when its
 * month comes after the end's ("December 20 until January 5, 2027").
 */
export function parseWindowText(
  text: string,
): { from: string | null; to: string } | null {
  const single = SINGLE.exec(text);
  if (single) {
    const day = isoDay(single[1], single[2], single[3]);
    return day ? { from: day, to: day } : null;
  }

  const range = RANGE.exec(text);
  if (!range) return null;
  const [, fromMonth, fromDay, fromYear, toMonth, toDay, toYear] = range;
  if (!toYear) return null;
  const to = isoDay(toMonth, toDay, toYear);
  if (!to) return null;
  if (!fromMonth) return { from: null, to };

  const startMonth = MONTHS.indexOf(fromMonth.toLowerCase());
  const endMonth = MONTHS.indexOf(toMonth.toLowerCase());
  const year =
    fromYear ?? String(Number(toYear) - (startMonth > endMonth ? 1 : 0));
  const from = isoDay(fromMonth, fromDay, year);
  return from && from <= to ? { from, to } : null;
}

function isoDay(
  monthName: string,
  day: string,
  year: string | undefined,
): string | null {
  const month = MONTHS.indexOf(monthName.toLowerCase()) + 1;
  if (month === 0 || !year) return null;
  const d = Number(day);
  // Rejects "February 30", which the pattern lets through.
  const probe = new Date(Date.UTC(Number(year), month - 1, d));
  if (probe.getUTCMonth() !== month - 1) return null;
  return `${year}-${String(month).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

function decodeEntities(text: string): string {
  return text
    .replace(/&#(\d+);/g, (_, code: string) =>
      String.fromCodePoint(Number(code)),
    )
    .replace(/&#x([0-9a-f]+);/gi, (_, code: string) =>
      String.fromCodePoint(parseInt(code, 16)),
    )
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}
