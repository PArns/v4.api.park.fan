import { ORLANDO } from "./regions";
import { linksOf, ORLANDO_TICKETS, TICKET_RULE_KEYS } from "./tickets";
import type { TicketProduct } from "./tickets";
import {
  dryRunTicket,
  urlProblem,
  validateTicketProduct,
} from "./ticket-validation";

const TODAY = "2026-11-13";
const SLUGS = ORLANDO_TICKETS.map((p) => p.id);

/** A seed product as the admin would write it. */
function draftOf(product: TicketProduct): Record<string, unknown> {
  return {
    slug: product.id,
    name: product.name,
    resort: product.resort,
    ...Object.fromEntries(
      TICKET_RULE_KEYS.filter((k) => product[k] !== undefined).map((k) => [
        k,
        product[k],
      ]),
    ),
    links: linksOf(product),
    ...(product.blockoutUrl && { blockoutUrl: product.blockoutUrl }),
    ...(product.reservationUrl && { reservationUrl: product.reservationUrl }),
    confidence: product.confidence,
    ...(product.notes && { notes: product.notes }),
    suggestRank: product.suggest ?? null,
  };
}

const seed = (id: string) =>
  draftOf(ORLANDO_TICKETS.find((p) => p.id === id) as TicketProduct);

const problemsOf = (draft: Record<string, unknown>, creating = false) =>
  validateTicketProduct(ORLANDO, draft, { creating, slugs: SLUGS }).problems;

const paths = (draft: Record<string, unknown>, creating = false) =>
  problemsOf(draft, creating).map((p) => p.path);

describe("validateTicketProduct", () => {
  it("passes every seed product, and the dry run plans each of them", () => {
    for (const product of ORLANDO_TICKETS) {
      const { problems, valid } = validateTicketProduct(
        ORLANDO,
        draftOf(product),
        { creating: false, slugs: SLUGS },
      );
      expect({ id: product.id, problems }).toEqual({
        id: product.id,
        problems: [],
      });
      const run = dryRunTicket(ORLANDO, valid!.product, TODAY);
      expect({ id: product.id, problems: run.problems }).toEqual({
        id: product.id,
        problems: [],
      });
      expect(run.planFirst).toBe("ok");
    }
  });

  it("names the slug, name, resort, markets and parks by their paths", () => {
    expect(
      paths({ ...seed("wdw-date-based"), slug: "Bad Slug" }, true),
    ).toContain("slug");
    expect(paths({ ...seed("wdw-date-based") }, true)).toContain("slug");
    expect(paths({ ...seed("wdw-date-based"), name: "" })).toContain("name");
    expect(paths({ ...seed("wdw-date-based"), resort: "nowhere" })).toContain(
      "resort",
    );
    expect(paths({ ...seed("wdw-date-based"), markets: ["eu"] })).toContain(
      "markets[0]",
    );
    expect(
      paths({ ...seed("wdw-date-based"), parks: ["epcot", "epcot"] }),
    ).toContain("parks[1]");
    expect(paths({ ...seed("wdw-date-based"), parks: ["nope"] })).toContain(
      "parks[0]",
    );
  });

  it("keeps windows at least their days, never shrinking, one row per length", () => {
    const draft = seed("wdw-date-based");
    const table = (
      draft.windowTable as Array<{ days: number; window: number }>
    ).map((r) => ({ ...r }));
    table[2].window = 2; // 3 days in 2
    expect(paths({ ...draft, windowTable: table })).toEqual(
      expect.arrayContaining(["windowTable[2].window", "windowTable"]),
    );
    expect(
      paths({ ...draft, windowTable: table.filter((r) => r.days !== 5) }),
    ).toContain("windowTable");
    expect(paths({ ...draft, windowDays: 14 })).toContain("windowDays");
    expect(paths({ ...draft, days: { min: 3, max: 2 } })).toContain("days");
    expect(paths({ ...draft, pass: true })).toContain("pass");
    expect(paths({ ...seed("wdw-annual-pass"), windowDays: 10 })).toContain(
      "windowDays",
    );
  });

  it("keeps hop sets and caps inside the product's parks and days", () => {
    const universal = seed("uor-date-based");
    expect(
      paths({
        ...universal,
        hopSets: [{ parks: ["universal-studios-florida", "epcot"] }],
      }),
    ).toContain("hopSets[0].parks[1]");
    expect(
      paths({
        ...universal,
        caps: [{ parks: ["universal-epic-universe"], max: 9 }],
      }),
    ).toContain("caps[0].max");
    expect(
      paths({
        ...universal,
        caps: [
          {
            parks: ["universal-epic-universe"],
            max: 0,
            ticketDays: { min: 1, max: 9 },
          },
        ],
      }),
    ).toContain("caps[0].ticketDays");
    expect(paths({ ...universal, hopping: "none" })).toContain("hopSets");
    expect(paths({ ...seed("seaworld-1-day"), hopping: "optional" })).toContain(
      "hopSets",
    );
  });

  it("keeps an allowance off the ticket-day parks, and sale dates real and in order", () => {
    const magic = seed("wdw-uk-14-day-magic");
    expect(
      paths({ ...magic, allowance: { parks: ["epcot"], visits: 14 } }),
    ).toContain("allowance.parks[0]");
    expect(
      paths({
        ...magic,
        allowance: { parks: ["disneys-typhoon-lagoon-water-park"], visits: 0 },
      }),
    ).toContain("allowance.visits");
    expect(
      paths({ ...magic, onSale: [{ from: "2027-06-01", to: "2027-01-01" }] }),
    ).toContain("onSale[0]");
    expect(paths({ ...magic, onSale: [{ from: "2027-02-30" }] })).toContain(
      "onSale[0].from",
    );
    expect(paths({ ...magic, until: "someday" })).toContain("until");
  });

  it("accepts https links only, exactly one official, never a sponsored one", () => {
    const draft = seed("wdw-date-based");
    const official = (draft.links as Array<Record<string, unknown>>).find(
      (link) => link.official === true,
    )!;
    const affiliate = {
      seller: "Attraction Tickets",
      url: "https://www.attractiontickets.com/en/orlando",
      sponsored: true,
      markets: ["uk"],
      locales: ["en"],
    };
    expect(problemsOf({ ...draft, links: [affiliate, official] })).toEqual([]);
    expect(
      paths({
        ...draft,
        links: [{ ...affiliate, url: "javascript:alert(1)" }, official],
      }),
    ).toContain("links[0].url");
    expect(
      paths({
        ...draft,
        links: [{ ...affiliate, url: "http://example.com/" }, official],
      }),
    ).toContain("links[0].url");
    expect(
      paths({
        ...draft,
        links: [
          { ...affiliate, url: `https://example.com/${"a".repeat(600)}` },
          official,
        ],
      }),
    ).toContain("links[0].url");
    expect(paths({ ...draft, links: [affiliate] })).toContain("links");
    expect(
      paths({
        ...draft,
        links: [official, { ...official, url: "https://b.example/" }],
      }),
    ).toContain("links");
    expect(
      paths({ ...draft, links: [{ ...official, sponsored: true }] }),
    ).toContain("links[0].sponsored");
    expect(
      paths({ ...draft, links: [{ ...affiliate, locales: ["pt"] }, official] }),
    ).toContain("links[0].locales[0]");
    expect(
      paths({ ...draft, links: [{ ...affiliate, tracking: "x" }, official] }),
    ).toContain("links[0].tracking");
    expect(paths({ ...draft, blockoutUrl: "ftp://x.example/" })).toContain(
      "blockoutUrl",
    );
    expect(
      paths({
        ...draft,
        sources: [{ url: "https://a.example/", accessed: "13.11.2026" }],
      }),
    ).toContain("sources[0].accessed");
  });

  it("refuses the urls a link must never be", () => {
    expect(urlProblem("https://disneyworld.disney.go.com/")).toBeNull();
    expect(urlProblem("javascript:alert(1)")).not.toBeNull();
    expect(urlProblem("data:text/html,x")).not.toBeNull();
    expect(urlProblem("https://user:pw@example.com/")).not.toBeNull();
    expect(urlProblem("https://localhost/")).not.toBeNull();
    expect(urlProblem("https://example.com/a b")).not.toBeNull();
  });

  it("lists every problem at once, not the first", () => {
    expect(
      problemsOf({
        ...seed("wdw-date-based"),
        name: "",
        confidence: "sure",
        anchor: "x",
      }).length,
    ).toBeGreaterThanOrEqual(3);
  });
});

describe("dryRunTicket", () => {
  const valid = (id: string) =>
    validateTicketProduct(ORLANDO, seed(id), { creating: false, slugs: SLUGS })
      .valid!.product;

  it("plans owned and plan first for a product on sale", () => {
    expect(dryRunTicket(ORLANDO, valid("wdw-date-based"), TODAY)).toEqual({
      owned: "ok",
      planFirst: "ok",
      problems: [],
    });
  });

  it("skips the owned run for a product sold for no start in the coming year", () => {
    const product = {
      ...valid("wdw-4-park-magic"),
      onSale: [{ from: "2025-01-01", to: "2025-06-01" }],
    };
    const run = dryRunTicket(ORLANDO, product, TODAY);
    expect(run.owned).toBe("skipped");
    expect(run.note).toMatch(/no start in the coming year/);
    expect(run.problems).toEqual([]);
  });

  it("refuses a product whose sample trip cannot be planned", () => {
    const run = dryRunTicket(
      ORLANDO,
      { ...valid("wdw-date-based"), parks: ["not-a-park"] },
      TODAY,
    );
    expect(run.planFirst).toBe("failed");
    expect(run.problems[0]).toMatchObject({ path: "dryRun.planFirst" });
  });
});
