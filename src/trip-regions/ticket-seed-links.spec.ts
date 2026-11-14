import {
  ATTRACTION_TICKETS_SELLER,
  seedLinksFrom,
  seedLinksOf,
  TICKET_CATALOGS,
} from "./tickets";
import { TICKET_LOCALES } from "./ticket-validation";
import { TICKET_SEED_LINKS } from "./ticket-seed-links";

/** Query parameters that mark an affiliate or tracking link. */
const TRACKING =
  /^(utm_|aff|affiliate|partner|ref$|refid|irclickid|clickid|gclid|fbclid|cjevent|awc|subid|tag$|campaign|source$|medium$)/i;

/** Every region's seed products, in one list: their ids are unique across regions. */
const SEED_PRODUCTS = Object.values(TICKET_CATALOGS).flat();

/**
 * The regions whose research came with purchase links. Japan's did not:
 * Attraction Tickets sells nothing there and no seller was shown cheaper, so
 * its products link their `shopUrl` alone.
 */
const LINKED_REGIONS = ["orlando", "southern-california", "paris"];

describe("the catalog's seed links", () => {
  it("has one entry for every product of the regions researched with links, and none for others", () => {
    const ids = SEED_PRODUCTS.map((p) => p.id);
    expect(new Set(ids).size).toBe(ids.length);
    const linked = LINKED_REGIONS.flatMap((slug) =>
      TICKET_CATALOGS[slug].map((p) => p.id),
    ).sort();
    expect(TICKET_SEED_LINKS.map((e) => e.productId).sort()).toEqual(linked);
  });

  it("holds plain https links only: no affiliate or tracking parameters", () => {
    for (const entry of TICKET_SEED_LINKS) {
      for (const link of [
        entry.official,
        entry.cheapest,
        ...(entry.attractionTickets ?? []).map((page) => ({
          seller: ATTRACTION_TICKETS_SELLER,
          url: page.url,
        })),
      ]) {
        if (!link) continue;
        const url = new URL(link.url);
        expect(url.protocol).toBe("https:");
        expect(link.seller.length).toBeGreaterThan(0);
        for (const name of url.searchParams.keys()) {
          expect(name).not.toMatch(TRACKING);
        }
      }
    }
  });

  it("gives every seed product its official link, never a sponsored one", () => {
    for (const product of SEED_PRODUCTS) {
      const links = seedLinksOf(product);
      expect(links.filter((l) => l.official)).toHaveLength(1);
      expect(links.every((l) => l.sponsored === false)).toBe(true);
    }
  });

  it("names only park.fan locales on Attraction Tickets links", () => {
    for (const entry of TICKET_SEED_LINKS) {
      for (const page of entry.attractionTickets ?? []) {
        if (page.locale !== null) {
          expect(TICKET_LOCALES).toContain(page.locale);
        }
      }
    }
  });

  it("orders the cheapest seller, Attraction Tickets, then the official shop, each once", () => {
    const official = { seller: "Walt Disney World", url: "https://a.example/" };
    const cheaper = { seller: "Ticket seller", url: "https://b.example/" };
    const uk = { url: "https://at.example/uk", locale: "en" };
    const all = { url: "https://at.example/all", locale: null };
    expect(
      seedLinksFrom(
        {
          productId: "x",
          cheapest: cheaper,
          attractionTickets: [uk, all],
          official,
        },
        { shopUrl: official.url },
      ),
    ).toEqual([
      { ...cheaper, sponsored: false },
      {
        seller: ATTRACTION_TICKETS_SELLER,
        url: uk.url,
        sponsored: false,
        locales: ["en"],
      },
      { seller: ATTRACTION_TICKETS_SELLER, url: all.url, sponsored: false },
      { ...official, official: true, sponsored: false },
    ]);
    // Attraction Tickets as the cheapest seller: its page is listed once, first.
    const atCheapest = { seller: ATTRACTION_TICKETS_SELLER, url: uk.url };
    expect(
      seedLinksFrom(
        {
          productId: "x",
          cheapest: atCheapest,
          attractionTickets: [uk],
          official,
        },
        { shopUrl: official.url },
      ).map((l) => l.url),
    ).toEqual([uk.url, official.url]);
  });

  it("puts the cheapest seller first, and leaves it out where it is the operator's shop", () => {
    const official = { seller: "Walt Disney World", url: "https://a.example/" };
    const cheaper = { seller: "Ticket seller", url: "https://b.example/" };
    expect(
      seedLinksFrom(
        {
          productId: "x",
          cheapest: cheaper,
          attractionTickets: null,
          official,
        },
        { shopUrl: official.url },
      ),
    ).toEqual([
      { ...cheaper, sponsored: false },
      { ...official, official: true, sponsored: false },
    ]);
    expect(
      seedLinksFrom(
        {
          productId: "x",
          cheapest: official,
          attractionTickets: null,
          official,
        },
        { shopUrl: official.url },
      ),
    ).toEqual([{ ...official, official: true, sponsored: false }]);
  });
});
