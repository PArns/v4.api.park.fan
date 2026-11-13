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
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/walt-disney-world-tickets/disney-base-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/walt-disney-world-tickets/disney-base-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/walt-disney-world-themenparks/disney-basis-ticket-1-park-pro-tag",
        locale: "de",
      },
    ],
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/admission/tickets/",
    },
  },
  {
    productId: "wdw-date-based-plus",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/walt-disney-world-tickets/disney-base-ticket-with-water-park",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/walt-disney-world-tickets/disney-base-ticket-with-water-park",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/walt-disney-world-themenparks/disney-basis-ticket-1-park-pro-tag-mit",
        locale: "de",
      },
    ],
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/admission/tickets/",
    },
  },
  {
    productId: "wdw-4-park-magic",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/walt-disney-world-tickets/disney-4-park-magic-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/walt-disney-world-tickets/disney-four-park-magic-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/walt-disney-world-tickets/disney-four-park-magic-ticket",
        locale: "de",
      },
    ],
    official: {
      seller: "Walt Disney World",
      url: "https://disneyworld.disney.go.com/admission/tickets/four-park-magic-ticket-offer/",
    },
  },
  {
    productId: "wdw-uk-14-day-magic",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/walt-disney-world-resort/disney-14-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/walt-disney-world-resort/disney-14-day-ticket",
        locale: "nl",
      },
    ],
    official: {
      seller: "Disney Holidays UK",
      url: "https://www.disneyholidays.co.uk/walt-disney-world/uk-ireland-exclusive-tickets/",
    },
  },
  {
    productId: "wdw-uk-7-day-magic",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/walt-disney-world-resort/disney-7-day-ticket",
        locale: "en",
      },
    ],
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
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/universal-orlando-resort/universal-one-day-park-to-park-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/universal-orlando-resort/universal-one-day-park-to-park-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando/universal-orlando-resort/2-park-1-day-park-to-park-tickets",
        locale: "de",
      },
    ],
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
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/universal-orlando-resort-tickets/universal-orlando-1-day-park-park-plus",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/universal-orlando-resort-tickets/universal-orlando-1-day-park-park-plus",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/universal-orlando-resort/universal-orlando-1-tag-park-park-plus-1-tag",
        locale: "de",
      },
    ],
    official: {
      seller: "Universal Orlando Resort",
      url: "https://www.universalorlando.com/web/en/us/tickets-packages/park-tickets",
    },
  },
  {
    productId: "uor-2-day-p2p-plus-epic",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/universal-orlando-resort-tickets/universal-orlando-2-day-park-park-plus",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/universal-orlando-resort-tickets/universal-orlando-2-day-park-park-plus",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/universal-orlando-2-tages-park-park-plus-1-tages-universal-epic-universe",
        locale: "de",
      },
    ],
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
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/universal-orlando-all-parks-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/universal-orlando-all-parks-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/universal-orlando-4-park-explorer-ticket",
        locale: "de",
      },
    ],
    official: {
      seller: "Universal Orlando UK store",
      url: "https://ukstore.universalorlando.com/park-tickets",
    },
  },
  {
    productId: "uor-uk-three-park-adventure",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/universal-orlando-adventure-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/universal-orlando-adventure-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/universal-orlando-adventure-ticket",
        locale: "de",
      },
    ],
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
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-single-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando/seaworld-orlando/tagestickets",
        locale: "de",
      },
    ],
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-two-park",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/seaworld-orlando-tickets/2-visit-seaworld-and-aquatica-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-two-park-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando/seaworld-orlando/2-park-ticket",
        locale: "de",
      },
    ],
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-three-park-3-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-three-park-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-three-park-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando/seaworld-orlando/3-park-ticket",
        locale: "de",
      },
    ],
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-four-parks-14-day",
    cheapest: {
      seller: "Attraction Tickets",
      url: "https://www.attractiontickets.com/us/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-unlimited-ticket",
    },
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-unlimited-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando/seaworld-orlando/unlimited-seaworld-aquatica-busch-gardens-adventure-island-tickets",
        locale: "de",
      },
    ],
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-uk-3-visit",
    cheapest: {
      seller: "Attraction Tickets",
      url: "https://www.attractiontickets.com/en/orlando/seaworld/three-park-ticket-plus-all-day-dining",
    },
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando/seaworld/three-park-ticket-plus-all-day-dining",
        locale: "en",
      },
    ],
    official: {
      seller: "SeaWorld Orlando",
      url: "https://seaworld.com/orlando/tickets/",
    },
  },
  {
    productId: "seaworld-uk-ultimate",
    cheapest: {
      seller: "Attraction Tickets",
      url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-ultimate-ticket",
    },
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/seaworld-orlando-tickets/seaworld-ultimate-ticket",
        locale: "en",
      },
    ],
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
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/discovery-cove-tickets/day-only-experience",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/discovery-cove-tickets/day-only-experience",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/discovery-cove-tickets/discovery-cove-tageserlebnis",
        locale: "de",
      },
    ],
    official: {
      seller: "Discovery Cove",
      url: "https://discoverycove.com/orlando/pricing/",
    },
  },
  {
    productId: "discovery-cove-plus-2",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/discovery-cove-tickets/discovery-cove-package-swim",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/discovery-cove-tickets/discovery-cove-package-swim",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/discovery-cove-tickets/discovery-cove-package-swim",
        locale: "de",
      },
    ],
    official: {
      seller: "Discovery Cove",
      url: "https://discoverycove.com/orlando/pricing/",
    },
  },
  {
    productId: "discovery-cove-plus-3",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/discovery-cove-tickets/discovery-cove-ultimate-package-swim",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/discovery-cove-tickets/discovery-cove-ultimate-package-swim",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/discovery-cove-orlando/discovery-cove-ultimate-package-swim",
        locale: "de",
      },
    ],
    official: {
      seller: "Discovery Cove",
      url: "https://discoverycove.com/orlando/pricing/",
    },
  },
  {
    productId: "busch-gardens-1-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/au/orlando-attraction-tickets/busch-gardensr-tickets/busch-gardens-tampa-bay-single-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/busch-gardensr-tickets/busch-gardens-tampa-bay-single-day-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/busch-gardensr-tickets/busch-gardens-tagesticket",
        locale: "de",
      },
    ],
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
    cheapest: {
      seller: "Attraction Tickets",
      url: "https://www.attractiontickets.com/us/orlando-attraction-tickets/legoland-florida/legoland-florida-promo-ticket",
    },
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/legoland-florida/legoland-florida-one-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/legoland-florida/legoland-florida-one-day-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando/legoland-florida/tagestickets",
        locale: "de",
      },
    ],
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
    cheapest: {
      seller: "Attraction Tickets",
      url: "https://www.attractiontickets.com/us/orlando-attraction-tickets/merlin-florida-combo-ticket",
    },
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/merlin-florida-combo-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/orlando-attraction-tickets/merlin-florida-combo-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/orlando-attraction-tickets/peppa-pig-legoland-florida-legoland-waterpark",
        locale: "de",
      },
    ],
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
