/**
 * The checks an admin's ticket product must pass before it is written, and the
 * dry run that plans a sample trip with it. Pure: the admin service hands in
 * the region (with its catalog) and today. Every problem names the path of
 * the field it is about, so the form can show it there. Contract:
 * docs/frontend/trip-regions.md#editing-the-catalog.
 */
import { addIsoDays, isRealIsoDate } from "../common/utils/date.util";
import type { RegionConfig } from "./regions";
import type { TicketSource } from "./entities/ticket-product.entity";
import { withCatalog } from "./ticket-catalog";
import {
  minuteOfClock,
  onSaleAt,
  ticketCatalog,
  TICKET_DAY_KINDS,
  type TicketDayKind,
  type TicketLink,
  type TicketMarket,
  type TicketProduct,
} from "./tickets";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import {
  normalizePlanQuery,
  optimizePlan,
  planResponse,
} from "./trip-plan.assembly";

export interface TicketProblem {
  path: string;
  message: string;
}

/** A product as the admin writes it, with what only the admin sees. */
export interface ValidTicketProduct {
  product: TicketProduct;
  active: boolean;
  /** Absent when the draft gives none: a new product then goes last. */
  sortOrder?: number;
  sources: TicketSource[];
}

/** The site's languages, which a link's `locales` name. */
export const TICKET_LOCALES = ["en", "de", "nl", "fr", "es", "it"] as const;
export const MAX_TICKET_DAYS = 30;
export const MAX_TICKET_WINDOW = 366;
export const MAX_TICKET_URL = 500;
export const MAX_TICKET_LINKS = 10;
export const MAX_TICKET_SOURCES = 30;
const SLUG = /^[a-z0-9]+(-[a-z0-9]+)*$/;

/** The fields a write may carry; anything else at the top is refused by the DTO. */
const LINK_KEYS = new Set([
  "seller",
  "url",
  "official",
  "sponsored",
  "markets",
  "locales",
]);
const SOURCE_KEYS = new Set(["url", "title", "accessed", "note"]);

class Problems {
  readonly list: TicketProblem[] = [];
  add(path: string, message: string): void {
    this.list.push({ path, message });
  }
}

const isInt = (v: unknown): v is number =>
  typeof v === "number" && Number.isInteger(v);
const isObject = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null && !Array.isArray(v);

/**
 * An absolute https url of at most {@link MAX_TICKET_URL} characters, with a
 * host and no user or password: what a link, a blockout calendar or a source
 * may be. `javascript:`, `data:` and plain `http:` are refused.
 */
export function urlProblem(value: unknown): string | null {
  if (typeof value !== "string" || value.length === 0) return "must be a url";
  if (value.length > MAX_TICKET_URL) {
    return `must be at most ${MAX_TICKET_URL} characters`;
  }
  if (/[\s\u0000-\u001f\u007f]/.test(value)) return "must not contain spaces";
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return "must be an absolute url";
  }
  if (url.protocol !== "https:") return "must be https";
  if (url.username || url.password) return "must not carry a user or password";
  if (!url.hostname.includes(".")) return "must name a public host";
  return null;
}

function text(
  p: Problems,
  path: string,
  value: unknown,
  max: number,
  required: boolean,
): string | undefined {
  if (value === undefined || value === null || value === "") {
    if (required) p.add(path, "is required");
    return undefined;
  }
  if (typeof value !== "string") {
    p.add(path, "must be text");
    return undefined;
  }
  const trimmed = value.trim();
  if (required && trimmed.length === 0) p.add(path, "is required");
  if (trimmed.length > max) p.add(path, `must be at most ${max} characters`);
  return trimmed;
}

function list<T>(
  p: Problems,
  path: string,
  value: unknown,
  allowed: readonly T[] | null,
  options: { min?: number } = {},
): T[] {
  if (value === undefined || value === null) {
    if ((options.min ?? 0) > 0) p.add(path, "is required");
    return [];
  }
  if (!Array.isArray(value)) {
    p.add(path, "must be a list");
    return [];
  }
  if (value.length < (options.min ?? 0)) {
    p.add(path, `must hold at least ${options.min}`);
  }
  const seen = new Set<unknown>();
  value.forEach((item, i) => {
    if (typeof item !== "string") p.add(`${path}[${i}]`, "must be text");
    else if (allowed && !allowed.includes(item as T)) {
      p.add(`${path}[${i}]`, `${item} is not one of ${allowed.join(", ")}`);
    }
    if (seen.has(item))
      p.add(`${path}[${i}]`, `${String(item)} is listed twice`);
    seen.add(item);
  });
  return value as T[];
}

function lengths(
  p: Problems,
  path: string,
  value: unknown,
  days: TicketProduct["days"] | undefined,
): { min: number; max: number } | undefined {
  if (value == null) return undefined;
  if (!isObject(value) || !isInt(value.min) || !isInt(value.max)) {
    p.add(path, "must be { min, max } in whole days");
    return undefined;
  }
  if (value.min > value.max) p.add(path, "min must not exceed max");
  if (days && days !== "unlimited") {
    if (value.min < days.min || value.max > days.max) {
      p.add(
        path,
        `must lie within the product's days (${days.min}–${days.max})`,
      );
    }
  }
  return { min: value.min, max: value.max };
}

/**
 * Checks a product as the admin writes it (a create, or a PATCH merged onto
 * the stored product). `slugs` are the region's other products, for a new
 * slug's uniqueness. Returns every problem, and the product when there is none.
 */
export function validateTicketProduct(
  region: RegionConfig,
  draft: Record<string, unknown>,
  options: { creating: boolean; slugs: readonly string[] },
): { problems: TicketProblem[]; valid?: ValidTicketProduct } {
  const p = new Problems();
  const regionParks = region.parks.map((x) => x.slug);
  const groups = region.groups.map((g) => g.key);

  const slug = text(p, "slug", draft.slug, 60, true);
  if (slug && !SLUG.test(slug)) {
    p.add("slug", "must be kebab-case: lower-case letters, digits and hyphens");
  }
  if (slug && options.creating && options.slugs.includes(slug)) {
    p.add("slug", `${slug} exists already in ${region.slug}`);
  }
  const name = text(p, "name", draft.name, 120, true);
  const description = text(p, "description", draft.description, 1000, false);
  const notes = text(p, "notes", draft.notes, 2000, false);
  const resort = text(p, "resort", draft.resort, 60, true);
  if (resort && !groups.includes(resort)) {
    p.add("resort", `must be a group of ${region.slug}: ${groups.join(", ")}`);
  }
  const marketCodes = region.ticketMarkets.codes;
  const markets = list<TicketMarket>(p, "markets", draft.markets, marketCodes, {
    min: 1,
  });
  const parks = list<string>(p, "parks", draft.parks, regionParks, { min: 1 });

  // ── days and windows ────────────────────────────────────────────────────
  let days: TicketProduct["days"] | undefined;
  if (draft.days === "unlimited") days = "unlimited";
  else if (
    isObject(draft.days) &&
    isInt(draft.days.min) &&
    isInt(draft.days.max)
  ) {
    days = { min: draft.days.min, max: draft.days.max };
    if (days.min < 1 || days.max > MAX_TICKET_DAYS || days.min > days.max) {
      p.add("days", `must be 1 ≤ min ≤ max ≤ ${MAX_TICKET_DAYS}`);
    }
  } else {
    p.add("days", 'must be "unlimited" or { min, max } in whole days');
  }
  // null clears an optional field, as a PATCH from the form sends it.
  const pass = draft.pass == null ? undefined : draft.pass;
  if (pass !== undefined && typeof pass !== "boolean") {
    p.add("pass", "must be true or false");
  }
  let windowTable: TicketProduct["windowTable"];
  let windowDays: number | undefined;
  if (days && days !== "unlimited") {
    if (!Array.isArray(draft.windowTable)) {
      p.add("windowTable", "is required for a product with a day count");
    } else {
      const rows = draft.windowTable as unknown[];
      const byDays = new Map<number, number>();
      rows.forEach((row, i) => {
        const path = `windowTable[${i}]`;
        if (!isObject(row) || !isInt(row.days) || !isInt(row.window)) {
          p.add(path, "must be { days, window } in whole days");
          return;
        }
        if (byDays.has(row.days))
          p.add(`${path}.days`, `${row.days} is listed twice`);
        if (row.days < days.min || row.days > days.max) {
          p.add(
            `${path}.days`,
            `must lie within days (${days.min}–${days.max})`,
          );
        }
        if (row.window < row.days) {
          p.add(`${path}.window`, `must be at least its days (${row.days})`);
        }
        if (row.window > MAX_TICKET_WINDOW) {
          p.add(`${path}.window`, `must be at most ${MAX_TICKET_WINDOW}`);
        }
        byDays.set(row.days, row.window);
      });
      let previous = 0;
      for (let d = days.min; d <= days.max; d++) {
        const w = byDays.get(d);
        if (w === undefined) {
          p.add("windowTable", `needs a row for ${d} days`);
          continue;
        }
        if (w < previous) {
          p.add(
            "windowTable",
            `the window for ${d} days is shorter than for ${d - 1}`,
          );
        }
        previous = w;
      }
      windowTable = [...byDays]
        .sort((a, b) => a[0] - b[0])
        .map(([d, window]) => ({ days: d, window }));
    }
    if (draft.windowDays !== undefined && draft.windowDays !== null) {
      p.add("windowDays", "is only for a product without a day count");
    }
    if (pass === true) p.add("pass", "a pass has unlimited days");
  } else if (days === "unlimited") {
    if (Array.isArray(draft.windowTable) && draft.windowTable.length > 0) {
      p.add("windowTable", "is only for a product with a day count");
    }
    if (draft.windowDays !== undefined && draft.windowDays !== null) {
      if (!isInt(draft.windowDays) || draft.windowDays < 1) {
        p.add("windowDays", "must be a whole number of days");
      } else if (draft.windowDays > MAX_TICKET_WINDOW) {
        p.add("windowDays", `must be at most ${MAX_TICKET_WINDOW}`);
      } else windowDays = draft.windowDays;
      if (pass === true) p.add("windowDays", "a pass has no window");
    }
  }
  const maxDays = days && days !== "unlimited" ? days.max : MAX_TICKET_WINDOW;

  // ── hopping and caps ────────────────────────────────────────────────────
  const anchor = draft.anchor;
  if (anchor !== "start" && anchor !== "first_use") {
    p.add("anchor", "must be start or first_use");
  }
  const hopping = draft.hopping;
  if (hopping !== "none" && hopping !== "optional" && hopping !== "included") {
    p.add("hopping", "must be none, optional or included");
  }
  let hopSets: TicketProduct["hopSets"];
  const rawSets = draft.hopSets ?? [];
  if (!Array.isArray(rawSets)) p.add("hopSets", "must be a list");
  else if (hopping === "none" && rawSets.length > 0) {
    p.add("hopSets", "must be empty without hopping");
  } else if (
    (hopping === "optional" || hopping === "included") &&
    rawSets.length === 0
  ) {
    p.add("hopSets", "needs at least one set with hopping");
  } else {
    hopSets = rawSets.flatMap((set, i) => {
      const path = `hopSets[${i}]`;
      if (!isObject(set)) {
        p.add(path, "must be { parks, ticketDays? }");
        return [];
      }
      const setParks = list<string>(p, `${path}.parks`, set.parks, parks, {
        min: 2,
      });
      const ticketDays = lengths(p, `${path}.ticketDays`, set.ticketDays, days);
      return [{ parks: setParks, ...(ticketDays && { ticketDays }) }];
    });
  }
  let caps: TicketProduct["caps"];
  const rawCaps = draft.caps ?? [];
  if (!Array.isArray(rawCaps)) p.add("caps", "must be a list");
  else {
    caps = rawCaps.flatMap((cap, i) => {
      const path = `caps[${i}]`;
      if (!isObject(cap)) {
        p.add(path, "must be { parks, max, ticketDays? }");
        return [];
      }
      const capParks = list<string>(p, `${path}.parks`, cap.parks, parks, {
        min: 1,
      });
      if (!isInt(cap.max) || cap.max < 0 || cap.max > maxDays) {
        p.add(`${path}.max`, `must be a whole number from 0 to ${maxDays}`);
      }
      const ticketDays = lengths(p, `${path}.ticketDays`, cap.ticketDays, days);
      return [
        {
          parks: capParks,
          max: cap.max as number,
          ...(ticketDays && { ticketDays }),
        },
      ];
    });
  }

  // ── allowance, reserved day, sale dates ─────────────────────────────────
  let allowance: TicketProduct["allowance"];
  if (draft.allowance !== undefined && draft.allowance !== null) {
    const a = draft.allowance;
    if (!isObject(a)) p.add("allowance", "must be { parks, visits }");
    else {
      const allowanceParks = list<string>(
        p,
        "allowance.parks",
        a.parks,
        regionParks,
        { min: 1 },
      );
      allowanceParks.forEach((slug, i) => {
        if (parks.includes(slug)) {
          p.add(
            `allowance.parks[${i}]`,
            `${slug} is a ticket-day park already`,
          );
        }
      });
      if (
        a.visits !== "days" &&
        (!isInt(a.visits) || a.visits < 1 || a.visits > MAX_TICKET_WINDOW)
      ) {
        p.add("allowance.visits", 'must be a whole number from 1, or "days"');
      }
      allowance = {
        parks: allowanceParks,
        visits: a.visits as number | "days",
      };
    }
  }
  const reservedDayParks = list<string>(
    p,
    "reservedDayParks",
    draft.reservedDayParks,
    parks,
  );
  let onSale: TicketProduct["onSale"];
  if (draft.onSale !== undefined && draft.onSale !== null) {
    if (!Array.isArray(draft.onSale)) p.add("onSale", "must be a list");
    else {
      onSale = draft.onSale.flatMap((range, i) => {
        const path = `onSale[${i}]`;
        if (!isObject(range)) {
          p.add(path, "must be { from?, to? }");
          return [];
        }
        for (const end of ["from", "to"] as const) {
          if (range[end] != null && !isRealIsoDate(range[end])) {
            p.add(`${path}.${end}`, "must be a real YYYY-MM-DD date");
          }
        }
        if (range.from == null && range.to == null) {
          p.add(path, "needs from or to");
        }
        if (
          isRealIsoDate(range.from) &&
          isRealIsoDate(range.to) &&
          range.from > range.to
        ) {
          p.add(path, "from must not be after to");
        }
        return [
          {
            ...(range.from != null && { from: range.from as string }),
            ...(range.to != null && { to: range.to as string }),
          },
        ];
      });
    }
  }
  if (draft.until !== undefined && draft.until !== null) {
    if (!isRealIsoDate(draft.until)) {
      p.add("until", "must be a real YYYY-MM-DD date");
    }
  }
  if (draft.blockouts != null && typeof draft.blockouts !== "boolean") {
    p.add("blockouts", "must be true or false");
  }
  if (draft.active !== undefined && typeof draft.active !== "boolean") {
    p.add("active", "must be true or false (archive a product to retire it)");
  }
  for (const key of ["blockoutUrl", "reservationUrl"] as const) {
    if (draft[key] !== undefined && draft[key] !== null && draft[key] !== "") {
      const problem = urlProblem(draft[key]);
      if (problem) p.add(key, problem);
    }
  }
  const confidence = draft.confidence;
  if (
    confidence !== "high" &&
    confidence !== "medium" &&
    confidence !== "low"
  ) {
    p.add("confidence", "must be high, medium or low");
  }
  if (
    draft.sortOrder != null &&
    (!isInt(draft.sortOrder) ||
      draft.sortOrder < 0 ||
      draft.sortOrder > 100_000)
  ) {
    p.add("sortOrder", "must be a whole number from 0 to 100000");
  }
  if (
    draft.suggestRank !== undefined &&
    draft.suggestRank !== null &&
    (!isInt(draft.suggestRank) ||
      draft.suggestRank < 0 ||
      draft.suggestRank > 1000)
  ) {
    p.add("suggestRank", "must be null or a whole number from 0 to 1000");
  }

  // ── an evening ticket ───────────────────────────────────────────────────
  let entryFrom: string | undefined;
  if (draft.entryFrom !== undefined && draft.entryFrom !== null) {
    if (
      typeof draft.entryFrom !== "string" ||
      minuteOfClock(draft.entryFrom) === undefined
    ) {
      p.add("entryFrom", "must be a park-local time HH:MM");
    } else entryFrom = draft.entryFrom;
    if (
      draft.suggestRank !== undefined &&
      draft.suggestRank !== null &&
      entryFrom
    ) {
      p.add(
        "suggestRank",
        "must be null for a ticket with entryFrom: plan first plans whole days",
      );
    }
  }
  let validDays: TicketDayKind[] | undefined;
  if (draft.validDays !== undefined && draft.validDays !== null) {
    validDays = list<TicketDayKind>(
      p,
      "validDays",
      draft.validDays,
      TICKET_DAY_KINDS,
      { min: 1 },
    );
  }

  const links = validateLinks(p, draft.links, marketCodes);
  const sources = validateSources(p, draft.sources);

  if (p.list.length > 0) return { problems: p.list };
  const product: TicketProduct = {
    id: slug as string,
    name: name as string,
    resort: resort as string,
    markets,
    parks,
    days: days as TicketProduct["days"],
    ...(windowTable && { windowTable }),
    ...(windowDays !== undefined && { windowDays }),
    anchor: anchor as TicketProduct["anchor"],
    hopping: hopping as TicketProduct["hopping"],
    ...(hopSets && hopSets.length > 0 && { hopSets }),
    ...(caps && caps.length > 0 && { caps }),
    ...(allowance && { allowance }),
    ...(reservedDayParks.length > 0 && { reservedDayParks }),
    ...(onSale && onSale.length > 0 && { onSale }),
    ...(typeof draft.until === "string" && { until: draft.until }),
    ...(pass === true && { pass: true as const }),
    ...(draft.blockouts === true && { blockouts: true as const }),
    ...(typeof draft.blockoutUrl === "string" &&
      draft.blockoutUrl !== "" && { blockoutUrl: draft.blockoutUrl }),
    ...(typeof draft.reservationUrl === "string" &&
      draft.reservationUrl !== "" && { reservationUrl: draft.reservationUrl }),
    ...(entryFrom && { entryFrom }),
    ...(validDays && validDays.length > 0 && { validDays }),
    confidence: confidence as TicketProduct["confidence"],
    shopUrl: (links.find((l) => l.official) as TicketLink).url,
    links,
    ...(description && { description }),
    ...(notes && { notes }),
    ...(isInt(draft.suggestRank) && { suggest: draft.suggestRank }),
    ...(draft.active === false && { archived: true as const }),
  };
  return {
    problems: [],
    valid: {
      product,
      active: draft.active !== false,
      ...(isInt(draft.sortOrder) && { sortOrder: draft.sortOrder }),
      sources,
    },
  };
}

function validateLinks(
  p: Problems,
  value: unknown,
  marketCodes: readonly string[],
): TicketLink[] {
  if (!Array.isArray(value) || value.length === 0) {
    p.add("links", "needs at least the official link");
    return [];
  }
  if (value.length > MAX_TICKET_LINKS) {
    p.add("links", `holds at most ${MAX_TICKET_LINKS}`);
  }
  const out: TicketLink[] = [];
  let official = 0;
  value.forEach((link, i) => {
    const path = `links[${i}]`;
    if (!isObject(link)) {
      p.add(
        path,
        "must be { seller, url, sponsored, official?, markets?, locales? }",
      );
      return;
    }
    for (const key of Object.keys(link)) {
      if (!LINK_KEYS.has(key)) p.add(`${path}.${key}`, "is not a link field");
    }
    const seller = text(p, `${path}.seller`, link.seller, 80, true);
    const problem = urlProblem(link.url);
    if (problem) p.add(`${path}.url`, problem);
    if (typeof link.sponsored !== "boolean") {
      p.add(`${path}.sponsored`, "must be true or false");
    }
    if (link.official != null && link.official !== true) {
      p.add(`${path}.official`, "must be true or left out");
    }
    if (link.official === true) {
      official++;
      if (link.sponsored === true) {
        p.add(`${path}.sponsored`, "the operator's own shop is not sponsored");
      }
    }
    const markets =
      link.markets == null
        ? undefined
        : list<TicketMarket>(p, `${path}.markets`, link.markets, marketCodes);
    const locales =
      link.locales == null
        ? undefined
        : list<string>(p, `${path}.locales`, link.locales, TICKET_LOCALES);
    out.push({
      seller: seller ?? "",
      url: link.url as string,
      ...(link.official === true && { official: true as const }),
      sponsored: link.sponsored === true,
      ...(markets && markets.length > 0 && { markets }),
      ...(locales && locales.length > 0 && { locales }),
    });
  });
  if (official !== 1) {
    p.add("links", "needs exactly one official link, the operator's own shop");
  }
  return out;
}

function validateSources(p: Problems, value: unknown): TicketSource[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) {
    p.add("sources", "must be a list");
    return [];
  }
  if (value.length > MAX_TICKET_SOURCES) {
    p.add("sources", `holds at most ${MAX_TICKET_SOURCES}`);
  }
  return value.flatMap((source, i) => {
    const path = `sources[${i}]`;
    if (!isObject(source)) {
      p.add(path, "must be { url, title?, accessed?, note? }");
      return [];
    }
    for (const key of Object.keys(source)) {
      if (!SOURCE_KEYS.has(key))
        p.add(`${path}.${key}`, "is not a source field");
    }
    const problem = urlProblem(source.url);
    if (problem) p.add(`${path}.url`, problem);
    const title = text(p, `${path}.title`, source.title, 200, false);
    const note = text(p, `${path}.note`, source.note, 500, false);
    if (source.accessed != null && !isRealIsoDate(source.accessed)) {
      p.add(`${path}.accessed`, "must be a real YYYY-MM-DD date");
    }
    return [
      {
        url: source.url as string,
        ...(title && { title }),
        ...(typeof source.accessed === "string" && {
          accessed: source.accessed,
        }),
        ...(note && { note }),
      },
    ];
  });
}

// ── the dry run ─────────────────────────────────────────────────────────────

export interface TicketDryRun {
  owned: "ok" | "failed" | "skipped";
  planFirst: "ok" | "failed" | "skipped";
  note?: string;
  problems: TicketProblem[];
}

/** The sample trip's length: long enough for a 10-day window, short enough to plan in a blink. */
const SAMPLE_DAYS = 10;
/** The sample trip starts this many days after today, as a planner's trip would. */
const SAMPLE_LEAD_DAYS = 7;

/** A calendar with every region park open, waits that differ by park and day. */
export function sampleCalendar(
  region: RegionConfig,
  from: string,
  to: string,
): InternalRegionCalendar {
  const slugs = region.parks.map((p) => p.slug);
  const days = [];
  for (let d = from, i = 0; d <= to; d = addIsoDays(d, 1), i++) {
    days.push({
      date: d,
      isHoliday: false,
      isSchoolVacation: false,
      parks: Object.fromEntries(
        slugs.map((s) => [
          s,
          {
            status: "OPERATING" as const,
            hoursPublished: true,
            crowdLevel: "moderate",
            headlinerWait: 40,
            waitExact: 30 + ((s.length * 7 + i * 11) % 30),
            basis: "forecast" as const,
            dayHours: 12,
          },
        ]),
      ),
    });
  }
  return {
    meta: {
      region: region.slug,
      timezone: region.timezone,
      from,
      to,
      computedAt: "dry-run",
      publishedHoursUntil: {},
    },
    parks: slugs,
    days,
    seasons: [],
  };
}

/**
 * Plans a fixed sample trip with the product, once owned (its shortest
 * length, with its hopper where it has one) and once plan first with it in
 * the catalog, on a synthetic calendar. A plan that throws is a problem; a
 * product sold for no start in the coming year skips the owned run.
 */
export function dryRunTicket(
  region: RegionConfig,
  product: TicketProduct,
  today: string,
): TicketDryRun {
  const catalog = [
    ...ticketCatalog(region).filter((p) => p.id !== product.id),
    { ...product, archived: undefined },
  ];
  const withProduct = withCatalog(region, {
    region: region.slug,
    version: "dry-run",
    products: catalog,
  });
  const known = region.parks.map((p) => p.slug);
  const parks = product.parks.slice(0, SAMPLE_DAYS - 2);
  const problems: TicketProblem[] = [];

  const firstDay = addIsoDays(today, SAMPLE_LEAD_DAYS);
  const lastStart = addIsoDays(today, 365 - SAMPLE_DAYS);
  let from: string | undefined;
  for (let d = firstDay; d <= lastStart; d = addIsoDays(d, 1)) {
    if (onSaleAt(product, d)) {
      from = d;
      break;
    }
  }

  const plan = (start: string, tickets?: string): void => {
    const to = addIsoDays(start, SAMPLE_DAYS - 1);
    const q = normalizePlanQuery(
      withProduct,
      known,
      {
        from: start,
        to,
        parks: parks.join(","),
        split: "fixed",
        ...(tickets && { tickets }),
      },
      today,
    );
    const calendar = sampleCalendar(withProduct, start, to);
    const trip = optimizePlan(withProduct, q, calendar);
    planResponse(withProduct, q, calendar, trip, {
      computedAt: "dry-run",
      today,
      leadMae: new Map(),
    });
  };
  const run = (
    path: "dryRun.owned" | "dryRun.planFirst",
    fn: () => void,
  ): "ok" | "failed" => {
    try {
      fn();
      return "ok";
    } catch (err) {
      problems.push({
        path,
        message: `planning a sample trip failed: ${(err as Error).message}`,
      });
      return "failed";
    }
  };

  const planFirst = run("dryRun.planFirst", () => plan(from ?? firstDay));
  let owned: TicketDryRun["owned"] = "skipped";
  let note: string | undefined;
  if (from) {
    const length = product.days === "unlimited" ? "" : `.${product.days.min}`;
    const hop = product.hopping === "optional" ? ".h" : "";
    owned = run("dryRun.owned", () =>
      plan(from as string, `${product.id}${length}${hop}`),
    );
  } else {
    note = "sold for no start in the coming year: the owned run was skipped";
  }
  return { owned, planFirst, ...(note && { note }), problems };
}
