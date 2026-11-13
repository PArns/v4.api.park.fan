/**
 * A region's ticket catalog as one versioned value: the products the admin
 * keeps in `ticket_products` (archived ones included, since `tickets=` may
 * still name them), or the seed in `tickets.ts` where there is no database.
 * Pure: `TicketCatalogService` reads the rows and caches the result, the dev
 * shim takes the seed. Contract: docs/frontend/trip-regions.md#tickets.
 */
import { createHash } from "node:crypto";
import type { RegionConfig } from "./regions";
import {
  linksOf,
  seedTickets,
  TICKET_RULE_KEYS,
  type TicketLink,
  type TicketProduct,
  type TicketRules,
} from "./tickets";
import type { TicketSource } from "./entities/ticket-product.entity";

export interface TicketCatalog {
  region: string;
  /** Changes with every edit: plans and `/when` are cached by it. */
  version: string;
  /** The newest edit; absent for the seed. */
  updatedAt?: string;
  /** In the admin's order, archived products included. */
  products: TicketProduct[];
}

/** A `ticket_products` row as the catalog reads and writes it. */
export interface TicketProductRow {
  region: string;
  slug: string;
  name: string;
  description: string | null;
  active: boolean;
  sortOrder: number;
  suggestRank: number | null;
  resort: string;
  rules: TicketRules;
  links: TicketLink[];
  sources: TicketSource[];
  blockoutUrl: string | null;
  reservationUrl: string | null;
  confidence: string;
  notes: string | null;
  updatedAt?: Date | string;
}

/**
 * The catalog's version: a hash of every product's content in order, so any
 * edit (a rule, a link, the order) changes it and nothing else does.
 */
export function catalogVersion(products: readonly TicketProduct[]): string {
  return createHash("sha256")
    .update(JSON.stringify(products))
    .digest("hex")
    .slice(0, 12);
}

const seedVersions = new Map<string, string>();

/** A region's seed as a catalog: what a reader without a database plans with. */
export function seedCatalog(region: { slug: string }): TicketCatalog {
  const products = seedTickets(region);
  let version = seedVersions.get(region.slug);
  if (!version) {
    version = catalogVersion(products);
    seedVersions.set(region.slug, version);
  }
  return { region: region.slug, version, products };
}

/** The version of the catalog a region carries, the seed's where it carries none. */
export function catalogVersionOf(region: {
  slug: string;
  ticketsVersion?: string;
}): string {
  return region.ticketsVersion ?? seedCatalog(region).version;
}

/** The region with its catalog on it, as every reader of the catalog takes it. */
export function withCatalog(
  region: RegionConfig,
  catalog: TicketCatalog,
): RegionConfig {
  return {
    ...region,
    tickets: catalog.products,
    ticketsVersion: catalog.version,
    ...(catalog.updatedAt && { ticketsUpdatedAt: catalog.updatedAt }),
  };
}

/** A row as the optimizer and the routes read it. */
export function productOfRow(row: TicketProductRow): TicketProduct {
  const official = row.links.find((l) => l.official) ?? row.links[0];
  const rules = Object.fromEntries(
    TICKET_RULE_KEYS.filter((k) => row.rules[k] !== undefined).map((k) => [
      k,
      row.rules[k],
    ]),
  ) as TicketRules;
  return {
    id: row.slug,
    name: row.name,
    resort: row.resort,
    ...rules,
    confidence: row.confidence as TicketProduct["confidence"],
    shopUrl: official?.url ?? "",
    links: row.links,
    ...(row.description && { description: row.description }),
    ...(row.notes && { notes: row.notes }),
    ...(row.blockoutUrl && { blockoutUrl: row.blockoutUrl }),
    ...(row.reservationUrl && { reservationUrl: row.reservationUrl }),
    ...(row.suggestRank != null && { suggest: row.suggestRank }),
    ...(!row.active && { archived: true as const }),
  };
}

/** A product as a row of the table: the seed's write, and the admin's. */
export function rowOfProduct(
  region: string,
  product: TicketProduct,
  extra: {
    sortOrder: number;
    active?: boolean;
    sources?: TicketSource[];
  },
): TicketProductRow {
  const rules = Object.fromEntries(
    TICKET_RULE_KEYS.filter((k) => product[k] !== undefined).map((k) => [
      k,
      product[k],
    ]),
  ) as TicketRules;
  return {
    region,
    slug: product.id,
    name: product.name,
    description: product.description ?? null,
    active: extra.active ?? !product.archived,
    sortOrder: extra.sortOrder,
    suggestRank: product.suggest ?? null,
    resort: product.resort,
    rules,
    links: linksOf(product),
    sources: extra.sources ?? [],
    blockoutUrl: product.blockoutUrl ?? null,
    reservationUrl: product.reservationUrl ?? null,
    confidence: product.confidence,
    notes: product.notes ?? null,
  };
}

/** The seed's rows for a region, in the seed's order (sort orders 10, 20, …). */
export function seedRows(region: { slug: string }): TicketProductRow[] {
  return seedTickets(region).map((product, i) =>
    rowOfProduct(region.slug, product, { sortOrder: (i + 1) * 10 }),
  );
}
