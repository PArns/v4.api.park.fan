import { ORLANDO_TICKETS, seedLinksFrom, seedLinksOf } from "./tickets";
import { TICKET_SEED_LINKS } from "./ticket-seed-links";

/** Query parameters that mark an affiliate or tracking link. */
const TRACKING =
  /^(utm_|aff|affiliate|partner|ref$|refid|irclickid|clickid|gclid|fbclid|cjevent|awc|subid|tag$|campaign|source$|medium$)/i;

describe("the catalog's seed links", () => {
  it("has one entry for every seed product and none for others", () => {
    const ids = ORLANDO_TICKETS.map((p) => p.id).sort();
    expect(TICKET_SEED_LINKS.map((e) => e.productId).sort()).toEqual(ids);
  });

  it("holds plain https links only: no affiliate or tracking parameters", () => {
    for (const entry of TICKET_SEED_LINKS) {
      for (const link of [entry.official, entry.cheapest]) {
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
    for (const product of ORLANDO_TICKETS) {
      const links = seedLinksOf(product);
      expect(links.filter((l) => l.official)).toHaveLength(1);
      expect(links.every((l) => l.sponsored === false)).toBe(true);
    }
  });

  it("puts the cheapest seller first, and leaves it out where it is the operator's shop", () => {
    const official = { seller: "Walt Disney World", url: "https://a.example/" };
    const cheaper = { seller: "Ticket seller", url: "https://b.example/" };
    expect(
      seedLinksFrom(
        { productId: "x", cheapest: cheaper, official },
        { shopUrl: official.url },
      ),
    ).toEqual([
      { ...cheaper, sponsored: false },
      { ...official, official: true, sponsored: false },
    ]);
    expect(
      seedLinksFrom(
        { productId: "x", cheapest: official, official },
        { shopUrl: official.url },
      ),
    ).toEqual([{ ...official, official: true, sponsored: false }]);
  });
});
