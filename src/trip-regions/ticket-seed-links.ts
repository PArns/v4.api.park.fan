/**
 * The purchase links the ticket catalog is seeded with, one entry per product
 * of `tickets.ts`: the operator's own shop (`official`), where research found
 * them the cheapest authorized seller (`cheapest`) and Attraction Tickets'
 * pages (`attractionTickets`, each for one park.fan locale or for all). The
 * seed orders them cheapest, Attraction Tickets, official. Plain links only:
 * no affiliate or tracking parameters, never sponsored
 * (`ticket-seed-links.spec.ts` checks it).
 *
 * Data, not logic: the shape is the research file's (`{ productId, cheapest:
 * { seller, url } | null, attractionTickets: [{ url, locale }] | null,
 * official: { seller, url } }`; its bundles are not products here),
 * so a new research run replaces this list as it stands. It is read only when
 * a region's `ticket_products` table is empty; once seeded, links are edited
 * in the admin (docs/frontend/trip-regions.md#editing-the-catalog).
 */

export interface SeedLink {
  seller: string;
  url: string;
}

/** An Attraction Tickets page for a product, for one park.fan locale or (null) all. */
export interface SeedAttractionTicketsLink {
  url: string;
  locale: string | null;
}

export interface SeedLinkEntry {
  productId: string;
  cheapest: SeedLink | null;
  attractionTickets: SeedAttractionTicketsLink[] | null;
  official: SeedLink;
}

export const TICKET_SEED_LINKS: SeedLinkEntry[] = [
  {
    productId: "wdw-date-based",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/admission/tickets/",
    },
  },
  {
    productId: "wdw-date-based-plus",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/admission/tickets/",
    },
  },
  {
    productId: "wdw-4-park-magic",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/admission/tickets/four-park-magic-ticket-offer/",
    },
  },
  {
    productId: "wdw-uk-14-day-magic",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Disney Holidays UK",
      url: "https://www.disneyholidays.co.uk/walt-disney-world/uk-ireland-exclusive-tickets/",
    },
  },
  {
    productId: "wdw-uk-7-day-magic",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Disney Holidays UK",
      url: "https://www.disneyholidays.co.uk/walt-disney-world/uk-ireland-exclusive-tickets/",
    },
  },
  {
    productId: "wdw-annual-pass",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/passholder-program/",
    },
  },
  {
    productId: "uor-date-based",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
    },
  },
  {
    productId: "uor-date-based-volcano-bay",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
    },
  },
  {
    productId: "uor-1-day-plus-epic",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
    },
  },
  {
    productId: "uor-2-day-p2p-plus-epic",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/offers/ticketoffer",
    },
  },
  {
    productId: "uor-epic-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
    },
  },
  {
    productId: "uor-volcano-bay-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
    },
  },
  {
    productId: "uor-uk-all-parks",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando UK store",
      url: "https://ukstore.universalorlando.com/park-tickets",
    },
  },
  {
    productId: "uor-uk-three-park-adventure",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando UK store",
      url: "https://ukstore.universalorlando.com/park-tickets",
    },
  },
  {
    productId: "uor-annual-pass",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/annual-passes",
    },
  },
  {
    productId: "uor-annual-pass-3-park",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/annual-passes",
    },
  },
  {
    productId: "seaworld-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-two-park",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-three-park-3-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-four-parks-14-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-uk-3-visit",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-uk-ultimate",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-fun-card",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-fun-card-aquatica",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-annual-pass",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/annual-pass/",
    },
  },
  {
    productId: "seaworld-annual-pass-florida",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/annual-pass/",
    },
  },
  {
    productId: "discovery-cove",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Discovery Cove",
      url: "https://discoverycove.com/orlando/pricing/",
    },
  },
  {
    productId: "discovery-cove-plus-2",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Discovery Cove",
      url: "https://discoverycove.com/orlando/pricing/",
    },
  },
  {
    productId: "discovery-cove-plus-3",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Discovery Cove",
      url: "https://discoverycove.com/orlando/pricing/",
    },
  },
  {
    productId: "busch-gardens-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Busch Gardens Tampa Bay",
      url: "https://buschgardens.com/tampa/tickets/",
    },
  },
  {
    productId: "busch-gardens-fun-card",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Busch Gardens Tampa Bay",
      url: "https://buschgardens.com/tampa/tickets/",
    },
  },
  {
    productId: "legoland-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "LEGOLAND Florida",
      url: "https://www.legoland.com/florida/tickets-passes/tickets/",
    },
  },
  {
    productId: "legoland-2-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "LEGOLAND Florida",
      url: "https://www.legoland.com/florida/tickets-passes/tickets/",
    },
  },
  {
    productId: "legoland-merlin-7-attractions",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "LEGOLAND Florida",
      url: "https://www.legoland.com/florida/tickets-passes/tickets/",
    },
  },
  {
    productId: "legoland-annual-pass",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "LEGOLAND Florida",
      url: "https://www.legoland.com/florida/tickets-passes/",
    },
  },
];
