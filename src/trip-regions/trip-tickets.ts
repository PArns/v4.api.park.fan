/**
 * Tickets in the plan, between the query and the optimizer: `tickets=` read
 * against the catalog (`tickets.ts`), the owned tickets as the optimizer keeps
 * them, plan first's suggestion per resort with its hopper verdict, and the
 * ticket fields of the answer. Pure. Contract:
 * docs/frontend/trip-regions.md#tickets.
 */
import { BadRequestException } from "@nestjs/common";
import { addIsoDays, isRealIsoDate } from "../common/utils/date.util";
import type { RegionConfig } from "./regions";
import {
  allowanceOf,
  capsOf,
  everyDay,
  hopSetsOf,
  onSaleAt,
  parksOf,
  sellsDays,
  ticketCatalog,
  windowOf,
  type TicketProduct,
} from "./tickets";
import {
  ORDER_MATTERS_MINUTES,
  type OptimizedPlan,
  type PlanTicket,
  type PlannedDay,
  type PlannedTicket,
} from "./trip-optimizer";
import { hopPairs, type TripLeg } from "./travel";

/** Tickets per plan. */
export const MAX_TICKETS = 6;

/**
 * A hopper saving of this many minutes per ride recommends it on its own: the
 * threshold under which the plan says "any order will do". A saving summed
 * over a trip would sit inside the forecast's error (about 17 minutes a ride).
 */
export const HOPPER_SAVED_PER_RIDE = ORDER_MATTERS_MINUTES;

/** Trips this long or longer get the UK 14-day tickets as an alternative. */
export const LONG_STAY_DAYS = 10;

/** An owned ticket as the query names it. */
export interface OwnedTicket {
  /** `t1` … `t6`, in the order given. */
  id: string;
  product: string;
  days: number | "unlimited";
  /** `.h?`: the visitor does not know; planned without. */
  hopper: boolean | "unknown";
  start?: string;
}

const ITEM = /^([a-z0-9-]+)(?:\.(\d{1,2}))?(\.h\??)?(?:@(\d{4}-\d{2}-\d{2}))?$/;

const daysBetween = (from: string, to: string): number =>
  Math.round(
    (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) /
      86_400_000,
  );

/** Every date from `from` to `to`, both included. */
function datesFrom(from: string, to: string): string[] {
  const out: string[] = [];
  for (let d = from; d <= to; d = addIsoDays(d, 1)) out.push(d);
  return out;
}

/** The catalog's product, or undefined. */
export function productOf(
  region: { slug: string },
  id: string,
): TicketProduct | undefined {
  return ticketCatalog(region).find((p) => p.id === id);
}

function onSaleText(product: TicketProduct): string {
  return (product.onSale ?? [])
    .map((r) => `${r.from ?? "…"} to ${r.to ?? "…"}`)
    .join(" and ");
}

/**
 * Reads `tickets=`; 400 with the reason otherwise. `trip` is the trip (`/plan`)
 * or the search range (`/when`, where `starts` is false and a date is refused).
 */
export function parseTickets(
  region: RegionConfig,
  raw: string | undefined,
  trip: { from: string; to: string },
  starts: boolean,
): OwnedTicket[] {
  if (!raw) return [];
  const items = raw.split(",");
  if (items.length > MAX_TICKETS) {
    throw new BadRequestException(`tickets takes at most ${MAX_TICKETS}`);
  }
  return items.map((item, i) => {
    const m = ITEM.exec(item);
    if (!m) {
      throw new BadRequestException(
        `tickets: ${item} is not product[.days][.h|.h?][@YYYY-MM-DD]`,
      );
    }
    const [, id, daysText, hop, start] = m;
    const product = productOf(region, id);
    if (!product) {
      throw new BadRequestException(
        `tickets: ${id} is not a ticket of ${region.name}'s catalog`,
      );
    }
    let days: number | "unlimited";
    if (product.days === "unlimited") {
      if (daysText) {
        throw new BadRequestException(
          `tickets: ${id} has unlimited days and takes no length`,
        );
      }
      days = "unlimited";
    } else if (daysText) {
      days = Number(daysText);
      if (!sellsDays(product, days)) {
        throw new BadRequestException(
          `tickets: ${id} is sold for ${product.days.min}${
            product.days.max > product.days.min ? `–${product.days.max}` : ""
          } days`,
        );
      }
    } else if (product.days.min === product.days.max) {
      days = product.days.min;
    } else {
      throw new BadRequestException(
        `tickets: ${id} needs its length, ${id}.<${product.days.min}–${product.days.max}>`,
      );
    }
    if (hop && product.hopping === "none") {
      throw new BadRequestException(`tickets: ${id} has no hopper option`);
    }
    const hopper: OwnedTicket["hopper"] =
      product.hopping === "included"
        ? true
        : hop === ".h"
          ? true
          : hop === ".h?"
            ? "unknown"
            : false;
    if (start !== undefined) {
      validateStart(product, item, start, days, trip, starts);
    } else if (product.onSale) {
      const anyOnSale = datesFrom(trip.from, trip.to).some((d) =>
        onSaleAt(product, d),
      );
      if (!anyOnSale) {
        throw new BadRequestException(
          `tickets: ${id} is sold for starts ${onSaleText(product)}, none in the trip`,
        );
      }
    }
    if (product.until && product.until < trip.from) {
      throw new BadRequestException(
        `tickets: ${id} is valid until ${product.until}`,
      );
    }
    return {
      id: `t${i + 1}`,
      product: id,
      days,
      hopper,
      ...(start !== undefined && { start }),
    };
  });
}

function validateStart(
  product: TicketProduct,
  item: string,
  start: string,
  days: number | "unlimited",
  trip: { from: string; to: string },
  starts: boolean,
): void {
  if (!starts) {
    throw new BadRequestException(
      `tickets: ${item} has a start date, and a fixed start leaves no dates to search; leave out @${start}`,
    );
  }
  if (product.pass) {
    throw new BadRequestException(
      `tickets: ${product.id} is a pass and takes no start date`,
    );
  }
  if (!isRealIsoDate(start)) {
    throw new BadRequestException(`tickets: ${start} is not a date`);
  }
  if (start > trip.to) {
    throw new BadRequestException(
      `tickets: ${item} starts after the trip ends`,
    );
  }
  if (product.reservedDayParks && start < trip.from) {
    throw new BadRequestException(
      `tickets: ${item}: the reserved day must lie in the trip`,
    );
  }
  const window = windowOf(product, days) ?? 1;
  if (addIsoDays(start, window - 1) < trip.from) {
    throw new BadRequestException(
      `tickets: ${item}: its ${window} days end before the trip starts`,
    );
  }
  if (!onSaleAt(product, start)) {
    throw new BadRequestException(
      `tickets: ${product.id} is sold for starts ${onSaleText(product)}`,
    );
  }
}

/**
 * One owned ticket in its full form, the cache key's part: its length always,
 * `.h` whenever it hops (an included hopper reads the same as a written one).
 */
export function canonicalTicket(t: OwnedTicket): string {
  const hop = t.hopper === true ? ".h" : t.hopper === "unknown" ? ".h?" : "";
  return `${t.product}${t.days === "unlimited" ? "" : `.${t.days}`}${hop}${
    t.start ? `@${t.start}` : ""
  }`;
}

/** The owned tickets in their full form, in the order given. */
export function canonicalTickets(tickets: OwnedTicket[]): string {
  return tickets.map(canonicalTicket).join(",");
}

/** Whether an owned ticket hops: its option on (or included) and a hop set at its length. */
export function ownedHops(region: { slug: string }, t: OwnedTicket): boolean {
  const product = productOf(region, t.product);
  return (
    !!product &&
    t.hopper === true &&
    hopSetsOf(product, t.days, true).length > 0
  );
}

/** The hop sets an owned ticket gives a day, for its evenings. */
export function ownedHopSets(
  region: { slug: string },
  t: OwnedTicket,
  hopper = t.hopper === true,
): string[][] {
  const product = productOf(region, t.product);
  return product ? hopSetsOf(product, t.days, hopper) : [];
}

/**
 * The owned tickets as the optimizer keeps them, over a trip's dates. `hopper`
 * overrides one ticket's option (the `.h?` verdict's second plan).
 */
export function planTicketsOf(
  region: RegionConfig,
  owned: OwnedTicket[],
  dates: string[],
  legs: TripLeg[],
  hopperOn?: string,
): PlanTicket[] {
  const first = dates[0];
  const last = dates[dates.length - 1];
  return owned.flatMap((o) => {
    const product = productOf(region, o.product);
    if (!product) return [];
    const parks = parksOf(product, o.days);
    const hopper = o.hopper === true || o.id === hopperOn;
    let window = windowOf(product, o.days) ?? Infinity;
    let start =
      product.reservedDayParks || !Number.isFinite(window)
        ? undefined
        : o.start;
    if (product.until && product.until < last) {
      // A pass or Fun Card that ends inside the trip: its days end there.
      start = first;
      window = Math.max(0, daysBetween(first, product.until) + 1);
    }
    const allowance = allowanceOf(product, o.days);
    return [
      {
        id: o.id,
        parks,
        ...(allowance && { allowance }),
        days: o.days === "unlimited" ? Infinity : o.days,
        window,
        ...(start !== undefined && { start }),
        ...(product.onSale &&
          start === undefined && {
            startOptions: dates.filter((d) => onSaleAt(product, d)),
          }),
        ...(product.reservedDayParks && {
          reservedDayParks: product.reservedDayParks,
          ...(o.start && { reservedDay: o.start }),
        }),
        caps: capsOf(product, o.days)
          .filter((c) => c.max > 0)
          .map((c) => ({ parks: c.parks, max: c.max })),
        hopPairs: hopPairs(
          region,
          legs,
          parks,
          hopSetsOf(product, o.days, hopper),
        ),
      },
    ];
  });
}

// ── plan first: the suggestion ─────────────────────────────────────────────

/** A day's hop or evening hop, as the verdict lists it. */
export interface HopEntry {
  date: string;
  parks: string[];
}

export interface HopperVerdict {
  recommended: boolean;
  savedMinutes: number;
  savedMinutesPerRide: number;
  hopDays: number;
  eveningHops: number;
  fitsMoreParks: number;
  hopDayList: HopEntry[];
  eveningHopList: HopEntry[];
  fitsOnlyWithHopper: string[];
  why: Array<
    | "evening_hops"
    | "hop_days"
    | "fits_more_parks"
    | "saves_minutes"
    | "no_gain"
  >;
}

export interface TicketAlternative {
  product: string;
  days: number;
  markets: string[];
  reason: "long_stay" | "promotion";
}

export interface TicketSuggestion {
  id: string;
  resort: string;
  product: string;
  days: number;
  start: string;
  end: string;
  parks: string[];
  extraDays?: number;
  hopper?: HopperVerdict;
  shopUrl: string;
  alternatives?: TicketAlternative[];
  notCoveredBy?: string[];
}

/** What a plan day carries about its ticket. */
export type DayTicket = Pick<
  PlannedDay,
  "ticket" | "ticketDay" | "ticketAllowance"
>;

/** A plan day's visits as a ticket sees them. */
interface Visit {
  date: string;
  parks: string[];
}

/** The parks of a planned day's visits. */
const parksOfDay = (d: PlannedDay): string[] => d.parks.map((p) => p.slug);

/** The resort whose products a park's suggestion comes from (the first suggested product covering it). */
export function suggestionResortOf(
  region: { slug: string },
  slug: string,
): string | undefined {
  return ticketCatalog(region).find(
    (p) =>
      p.suggest !== undefined &&
      (p.parks.includes(slug) || !!p.allowance?.parks.includes(slug)),
  )?.resort;
}

interface Fit {
  days: number | "unlimited";
  dayUse: number;
  extra: number;
  window: number;
}

/** The shortest length of a product that covers these visits from the first one's date. */
function fitOf(product: TicketProduct, visits: Visit[]): Fit | null {
  if (visits.length === 0) return null;
  const first = visits[0].date;
  const last = visits[visits.length - 1].date;
  if (!onSaleAt(product, first)) return null;
  const span = daysBetween(first, last) + 1;
  const lengths: Array<number | "unlimited"> =
    product.days === "unlimited"
      ? ["unlimited"]
      : Array.from(
          { length: product.days.max - product.days.min + 1 },
          (_, i) => (product.days as { min: number }).min + i,
        );
  for (const n of lengths) {
    const parks = parksOf(product, n);
    const allowance = allowanceOf(product, n);
    let dayUse = 0;
    let allowanceUse = 0;
    let ok = true;
    for (const v of visits) {
      if (v.parks.every((p) => parks.includes(p))) {
        dayUse++;
        if (
          v.parks.length > 1 &&
          !hopSetsOf(product, n, true).some((set) =>
            v.parks.every((p) => set.includes(p)),
          )
        ) {
          ok = false;
        }
      } else if (
        allowance &&
        v.parks.length === 1 &&
        allowance.parks.includes(v.parks[0])
      ) {
        allowanceUse++;
      } else {
        ok = false;
      }
    }
    if (!ok) continue;
    if (n !== "unlimited" && dayUse > n) continue;
    if (allowance && allowanceUse > allowance.visits) continue;
    const capsKept = capsOf(product, n)
      .filter((c) => c.max > 0)
      .every(
        (c) =>
          visits.filter((v) => v.parks.some((p) => c.parks.includes(p)))
            .length <= c.max,
      );
    if (!capsKept) continue;
    const window = windowOf(product, n) ?? Infinity;
    if (span > window) continue;
    return {
      days: n,
      dayUse,
      extra: n === "unlimited" ? 0 : n - dayUse,
      window,
    };
  }
  return null;
}

interface Chosen {
  product: TicketProduct;
  fit: Fit;
  visits: Visit[];
}

/**
 * The tickets for one resort's plan-first visits, in date order: from the
 * earliest visit left, the product that covers the most of the next visits
 * without more than doubling its park days, then the fewest extra days, then
 * the catalog's order; a product with unlimited days only where no counted
 * one covers as many.
 */
function chooseTickets(products: TicketProduct[], visits: Visit[]): Chosen[] {
  const out: Chosen[] = [];
  let rest = visits;
  while (rest.length > 0) {
    let best: (Chosen & { k: number }) | null = null;
    const better = (a: Chosen & { k: number }, b: Chosen & { k: number }) => {
      const sane = (c: Chosen & { k: number }) =>
        c.fit.extra <= Math.max(1, c.fit.dayUse) ? 0 : 1;
      return (
        sane(a) - sane(b) ||
        b.k - a.k ||
        (a.fit.days === "unlimited" ? 1 : 0) -
          (b.fit.days === "unlimited" ? 1 : 0) ||
        a.fit.extra - b.fit.extra ||
        (a.product.suggest ?? 0) - (b.product.suggest ?? 0)
      );
    };
    for (const product of products) {
      for (let k = rest.length; k >= 1; k--) {
        const fit = fitOf(product, rest.slice(0, k));
        if (!fit) continue;
        const candidate = { product, fit, visits: rest.slice(0, k), k };
        if (!best || better(candidate, best) < 0) best = candidate;
        break;
      }
    }
    if (!best) {
      // No product covers this visit alone: it goes without a suggestion.
      rest = rest.slice(1);
      continue;
    }
    out.push({ product: best.product, fit: best.fit, visits: best.visits });
    rest = rest.slice(best.k);
  }
  return out;
}

/** What the suggestion needs from the plan route. */
export interface SuggestionInput {
  region: RegionConfig;
  plan: OptimizedPlan;
  trip: { from: string; to: string };
  owned: OwnedTicket[];
  /** The plan-first resorts planned with the hopper. */
  hopperResorts: string[];
  /** The evenings with these hop sets per date (`trip-evening.ts`). */
  eveningsWith: (
    hopSetsOn: (date: string) => string[][],
  ) => Map<string, { kind: "hop" | "venue"; park?: string }>;
  /**
   * The plan made again with the hopper on (`on`) or off for one resort's plan
   * first, or for one owned ticket; null where the caller cannot (`/when`).
   */
  replan: (change: ReplanChange) => OptimizedPlan | null;
}

export type ReplanChange =
  { resort: string; hopper: boolean } | { ticket: string; hopper: boolean };

/** Plan first's suggestion per resort and the ticket fields of its days. */
export function suggestTickets(input: SuggestionInput): {
  suggestions: TicketSuggestion[];
  days: Map<string, DayTicket>;
} {
  const { region, plan, trip } = input;
  const catalog = ticketCatalog(region);
  const byResort = new Map<string, Visit[]>();
  for (const day of plan.days) {
    if (day.parks.length === 0 || day.ticket) continue;
    const resort = suggestionResortOf(region, day.parks[0].slug);
    if (!resort) continue;
    byResort.set(resort, [
      ...(byResort.get(resort) ?? []),
      { date: day.date, parks: parksOfDay(day) },
    ]);
  }
  const suggestions: TicketSuggestion[] = [];
  const days = new Map<string, DayTicket>();
  const tripDays = daysBetween(trip.from, trip.to) + 1;
  for (const group of region.groups) {
    const visits = byResort.get(group.key);
    if (!visits) continue;
    const products = catalog
      .filter(
        (p) =>
          p.resort === group.key &&
          p.suggest !== undefined &&
          p.markets.includes("us"),
      )
      .sort((a, b) => (a.suggest ?? 0) - (b.suggest ?? 0));
    const notCoveredBy = input.owned
      .filter((o) => productOf(region, o.product)?.resort === group.key)
      .map((o) => o.id);
    chooseTickets(products, visits).forEach((chosen, k) => {
      const id = `suggested:${group.key}${k > 0 ? `:${k + 1}` : ""}`;
      const { product, fit } = chosen;
      const start = chosen.visits[0].date;
      const windowDay = everyDay(product, fit.days);
      let n = 0;
      for (const v of chosen.visits) {
        const allowance = !v.parks.every((p) =>
          parksOf(product, fit.days).includes(p),
        );
        if (allowance) {
          days.set(v.date, { ticket: id, ticketAllowance: true });
        } else {
          n++;
          days.set(v.date, {
            ticket: id,
            ticketDay: windowDay ? daysBetween(start, v.date) + 1 : n,
          });
        }
      }
      const alternatives = alternativesFor(
        catalog,
        group.key,
        chosen.visits,
        // A resort the visitor holds a ticket for needs no other offer.
        k === 0 && notCoveredBy.length === 0 ? tripDays : 0,
      );
      const verdict =
        product.hopping === "optional"
          ? resortVerdict(input, group.key, product, fit, chosen.visits)
          : undefined;
      suggestions.push({
        id,
        resort: group.key,
        product: product.id,
        days: fit.days === "unlimited" ? fit.dayUse : fit.days,
        start,
        end: addIsoDays(start, fit.window - 1),
        parks: [...new Set(chosen.visits.flatMap((v) => v.parks))].sort(),
        ...(fit.extra > 0 && { extraDays: fit.extra }),
        ...(verdict && { hopper: verdict }),
        shopUrl: product.shopUrl,
        ...(alternatives.length > 0 && { alternatives }),
        ...(notCoveredBy.length > 0 && { notCoveredBy }),
      });
    });
  }
  return { suggestions, days };
}

/**
 * The UK 14-day tickets on a long trip (named on a resort's first suggestion
 * only), and a promotion (sold in the US, with sale dates, never suggested
 * itself) that covers the same visits without extra days.
 */
function alternativesFor(
  catalog: TicketProduct[],
  resort: string,
  visits: Visit[],
  tripDays: number,
): TicketAlternative[] {
  const start = visits[0].date;
  const out: TicketAlternative[] = [];
  for (const p of catalog) {
    if (p.resort !== resort || p.pass || p.days === "unlimited") continue;
    if (
      tripDays >= LONG_STAY_DAYS &&
      !p.markets.includes("us") &&
      p.days.min >= 14 &&
      onSaleAt(p, start)
    ) {
      out.push({
        product: p.id,
        days: p.days.min,
        markets: p.markets,
        reason: "long_stay",
      });
      continue;
    }
    if (p.suggest === undefined && p.onSale && p.markets.includes("us")) {
      const fit = fitOf(p, visits);
      if (fit && fit.days !== "unlimited" && fit.extra === 0) {
        out.push({
          product: p.id,
          days: fit.days,
          markets: p.markets,
          reason: "promotion",
        });
      }
    }
  }
  return out;
}

/** Each visit's headliner wait by park, in date order (a hop day's per park). */
function waitsByPark(
  plan: OptimizedPlan,
  parks: Set<string>,
): Map<string, number[]> {
  const out = new Map<string, number[]>();
  for (const day of plan.days) {
    for (const v of day.parks) {
      if (!parks.has(v.slug)) continue;
      const wait = day.kind === "hop" ? v.headlinerWait : day.headlinerWait;
      out.set(v.slug, [...(out.get(v.slug) ?? []), wait ?? NaN]);
    }
  }
  return out;
}

/**
 * The verdict from the plan with the hopper and the plan without it, over
 * the given parks and dates, with the evening hops the hopper opens.
 */
export function hopperVerdict(
  withHopper: OptimizedPlan,
  without: OptimizedPlan,
  parks: Set<string>,
  eveningHopList: HopEntry[],
): HopperVerdict {
  const hopDayList = withHopper.days
    .filter((d) => d.kind === "hop" && d.parks.every((p) => parks.has(p.slug)))
    .map((d) => ({ date: d.date, parks: parksOfDay(d) }));
  const a = waitsByPark(withHopper, parks);
  const b = waitsByPark(without, parks);
  const fitsOnlyWithHopper: string[] = [];
  let saved = 0;
  let common = 0;
  for (const slug of [...parks].sort()) {
    const withList = a.get(slug) ?? [];
    const withoutList = b.get(slug) ?? [];
    for (let i = withoutList.length; i < withList.length; i++) {
      fitsOnlyWithHopper.push(slug);
    }
    const n = Math.min(withList.length, withoutList.length);
    for (let i = 0; i < n; i++) {
      if (Number.isNaN(withList[i]) || Number.isNaN(withoutList[i])) continue;
      saved += withoutList[i] - withList[i];
      common++;
    }
  }
  const savedMinutes = Math.max(0, Math.round(saved));
  const savedMinutesPerRide =
    common > 0 ? Math.max(0, Math.round(saved / common)) : 0;
  // A hop day alone is no reason: it recommends through what it fits or saves.
  const recommended =
    eveningHopList.length > 0 ||
    fitsOnlyWithHopper.length > 0 ||
    savedMinutesPerRide >= HOPPER_SAVED_PER_RIDE;
  const why: HopperVerdict["why"] = [];
  if (eveningHopList.length > 0) why.push("evening_hops");
  if (hopDayList.length > 0) why.push("hop_days");
  if (fitsOnlyWithHopper.length > 0) why.push("fits_more_parks");
  if (savedMinutesPerRide >= HOPPER_SAVED_PER_RIDE) why.push("saves_minutes");
  return {
    recommended,
    savedMinutes,
    savedMinutesPerRide,
    hopDays: hopDayList.length,
    eveningHops: eveningHopList.length,
    fitsMoreParks: fitsOnlyWithHopper.length,
    hopDayList,
    eveningHopList,
    fitsOnlyWithHopper,
    why: recommended ? why : ["no_gain"],
  };
}

/** The evening hops the given hop sets open on the given dates. */
function eveningHopsOn(
  input: SuggestionInput,
  dates: Set<string>,
  sets: string[][],
): HopEntry[] {
  const evenings = input.eveningsWith((date) => (dates.has(date) ? sets : []));
  const out: HopEntry[] = [];
  for (const day of input.plan.days) {
    const evening = evenings.get(day.date);
    if (!dates.has(day.date) || evening?.kind !== "hop" || !evening.park) {
      continue;
    }
    out.push({ date: day.date, parks: [day.parks[0].slug, evening.park] });
  }
  return out;
}

/** Whether the hopper plan can differ: a visit of these parks found no day. */
const hasUnplaced = (plan: OptimizedPlan, parks: Set<string>): boolean =>
  plan.unplaced.some((u) => parks.has(u.slug) && u.reason !== "no_open_day");

/** Plan first: what the suggested product's hopper option adds for its resort. */
function resortVerdict(
  input: SuggestionInput,
  resort: string,
  product: TicketProduct,
  fit: Fit,
  visits: Visit[],
): HopperVerdict {
  const parks = new Set(
    input.region.parks
      .filter((p) => suggestionResortOf(input.region, p.slug) === resort)
      .map((p) => p.slug),
  );
  const dates = new Set(visits.map((v) => v.date));
  const evenings = eveningHopsOn(
    input,
    dates,
    hopSetsOf(product, fit.days, true),
  );
  const on = input.hopperResorts.includes(resort);
  const main = input.plan;
  let other: OptimizedPlan | null = null;
  const hopsHere = main.days.some(
    (d) => d.kind === "hop" && d.parks.every((p) => parks.has(p.slug)),
  );
  if (on ? hopsHere : hasUnplaced(main, parks)) {
    other = input.replan({ resort, hopper: !on });
  }
  const withHopper = on ? main : (other ?? main);
  const without = on ? (other ?? main) : main;
  return hopperVerdict(withHopper, without, parks, evenings);
}

/** An owned `.h?` ticket: what its hopper option would add. */
export function ownedVerdict(
  input: SuggestionInput,
  ticket: OwnedTicket,
): HopperVerdict | undefined {
  const product = productOf(input.region, ticket.product);
  if (!product) return undefined;
  const parks = new Set([
    ...parksOf(product, ticket.days),
    ...(product.allowance?.parks ?? []),
  ]);
  const dates = new Set(
    input.plan.days.filter((d) => d.ticket === ticket.id).map((d) => d.date),
  );
  const evenings = eveningHopsOn(
    input,
    dates,
    hopSetsOf(product, ticket.days, true),
  );
  const main = input.plan;
  const other = hasUnplaced(main, parks)
    ? input.replan({ ticket: ticket.id, hopper: true })
    : null;
  return hopperVerdict(other ?? main, main, parks, evenings);
}

// ── the answer ──────────────────────────────────────────────────────────────

/** `meta.tickets[]`. */
export interface TicketSummary {
  id: string;
  product: string;
  days?: number;
  hopper: boolean | "unknown";
  hopperVerdict?: HopperVerdict;
  start?: string;
  startChosen?: true;
  end?: string;
  usedDays: number;
  /** An every-day product's window, instead of `unusedDays`. */
  windowDays?: number;
  unusedDays?: number;
  unusedReason?:
    | "window_outside_trip"
    | "no_open_day"
    | "rest_days_kept"
    | "parks_fewer_than_days";
  allowanceVisits?: number;
  warnings: Array<
    "blockouts_unchecked" | "park_reservation_needed" | "covers_no_planned_park"
  >;
}

export interface TicketSummaryInput {
  region: RegionConfig;
  owned: OwnedTicket[];
  planned: PlannedTicket[];
  /** The plan's parks. */
  parks: string[];
  /** Trip days a park may go on (free arrival and departure days left out). */
  available: string[];
  /** Owned tickets whose second visits stopped for the trip's rest days. */
  restKept: Set<string>;
  /** The visits each park got, second visits included. */
  visits: Map<string, number>;
  verdicts: Map<string, HopperVerdict>;
}

/** The owned tickets as the answer lists them. */
export function ticketSummaries(input: TicketSummaryInput): TicketSummary[] {
  const { region } = input;
  return input.owned.map((o, i) => {
    const product = productOf(region, o.product) as TicketProduct;
    const planned = input.planned[i] ?? {
      id: o.id,
      usedDays: 0,
      allowanceVisits: 0,
    };
    const covered = [
      ...parksOf(product, o.days),
      ...(product.allowance?.parks ?? []),
    ];
    const warnings: TicketSummary["warnings"] = [];
    if (product.blockouts) warnings.push("blockouts_unchecked");
    if (product.reservationUrl) warnings.push("park_reservation_needed");
    if (!input.parks.some((p) => covered.includes(p))) {
      warnings.push("covers_no_planned_park");
    }
    const days = o.days === "unlimited" ? undefined : o.days;
    const windowDays = everyDay(product, o.days)
      ? windowOf(product, o.days)
      : undefined;
    const unused =
      days !== undefined && windowDays === undefined
        ? Math.max(0, days - planned.usedDays)
        : 0;
    let unusedReason: TicketSummary["unusedReason"];
    if (unused > 0 && days !== undefined) {
      // The visitor's visits of its parks, second visits included.
      const wanted = input.parks
        .filter((p) => parksOf(product, o.days).includes(p))
        .reduce((sum, p) => sum + (input.visits.get(p) ?? 1), 0);
      const inWindow =
        planned.start && planned.end
          ? input.available.filter(
              (d) =>
                d >= (planned.start as string) && d <= (planned.end as string),
            ).length
          : input.available.length;
      if (wanted < days) {
        unusedReason = input.restKept.has(o.id)
          ? "rest_days_kept"
          : "parks_fewer_than_days";
      } else if (inWindow < days) {
        unusedReason = "window_outside_trip";
      } else {
        unusedReason = "no_open_day";
      }
    }
    const verdict = input.verdicts.get(o.id);
    return {
      id: o.id,
      product: o.product,
      ...(days !== undefined && { days }),
      hopper: o.hopper,
      ...(verdict && { hopperVerdict: verdict }),
      ...(product.pass
        ? {}
        : {
            ...(planned.start && { start: planned.start }),
            ...(planned.startChosen && { startChosen: true as const }),
            ...(planned.end && { end: planned.end }),
          }),
      usedDays: planned.usedDays,
      ...(windowDays !== undefined && { windowDays }),
      ...(unused > 0 && { unusedDays: unused, unusedReason }),
      ...(planned.allowanceVisits > 0 && {
        allowanceVisits: planned.allowanceVisits,
      }),
      warnings,
    };
  });
}
