/**
 * Paris' ticket seed (PAR-1296): Disneyland Paris' and Parc Astérix's
 * products as researched on 2026-11-13 for late 2026 and 2027, and their
 * purchase links, in the shape of `tickets.ts` and `ticket-seed-links.ts`.
 * Read only when the region's `ticket_products` table is empty; once seeded,
 * the admin edits them. No prices.
 *
 * Disney's rules come from its media PDFs, its press site, its pages as the
 * search index quotes them and Attraction Tickets' pages quoting its terms:
 * its shop sends scripts to a waiting room. Dated multi-day tickets run on
 * consecutive days from the start date, at most 4. No ticket includes Extra
 * Magic Time (`regions.ts`). Neither operator sells a ticket for both resorts.
 *
 * Links: no reseller was verified as authorized and clearly cheaper, so the
 * seed lists Attraction Tickets' pages for their locale (it has no French
 * site, so `fr` gets the official link alone), then the operator's shop.
 * Attraction Tickets sells Parc Astérix only with a coach from central Paris.
 */
import type { SeedLinkEntry } from "./ticket-seed-links";
import type { TicketProduct } from "./tickets";

export const PARIS_TICKETS: TicketProduct[] = [
  {
    id: "dlp-1-day",
    name: "Disneyland Paris 1-Day dated ticket (1 Park, or 2 Parks with .h)",
    resort: "disneyland-paris",
    markets: ["eu", "intl"],
    parks: ["disneyland-park", "disney-adventure-world"],
    days: {
      min: 1,
      max: 1,
    },
    windowTable: [
      {
        days: 1,
        window: 1,
      },
    ],
    anchor: "start",
    hopping: "optional",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-adventure-world"],
      },
    ],
    confidence: "high",
    shopUrl: "https://tickets.disneylandparis.com/en-gb/tickets",
    description:
      "One day at Disneyland Park or Disney Adventure World, or at both with the 2-park version, on the date chosen at purchase.",
    notes:
      "Dated: valid on the selected date only, and the park reservation comes with the ticket. 1 Park: one park of the guest's choice for the whole day (no switching); 2 Parks (.h): both parks the same day. Dated 1- to 4-day tickets are refundable up to 3 days before the visit date (Disney FAQ, search index; the sellers say 72 hours). The price varies by date.",
    suggest: 10,
  },
  {
    id: "dlp-multi-day",
    name: "Disneyland Paris 2-, 3- or 4-Day dated ticket (2 Parks)",
    resort: "disneyland-paris",
    markets: ["eu", "intl"],
    parks: ["disneyland-park", "disney-adventure-world"],
    days: {
      min: 2,
      max: 4,
    },
    windowTable: [
      {
        days: 2,
        window: 2,
      },
      {
        days: 3,
        window: 3,
      },
      {
        days: 4,
        window: 4,
      },
    ],
    anchor: "start",
    hopping: "included",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-adventure-world"],
      },
    ],
    confidence: "high",
    shopUrl: "https://tickets.disneylandparis.com/en-gb/tickets",
    description:
      "Two, three or four consecutive days with both parks every day, starting on the date chosen at purchase.",
    notes:
      "The days are consecutive from the start date (Disney's group fact sheet: 'Les deux jours de visite sont obligatoirement consécutifs'; Attraction Tickets: 'N consecutive days from your start date'). Both parks every day, hopping at any time. 4 days is the longest ticket sold; a longer stay needs a second ticket. Park reservations are included. Disney Hotel + ticket packages cover the stay's days instead.",
    suggest: 10,
  },
  {
    id: "dlp-1-day-liberte",
    name: "Disneyland Paris 1-Day undated ticket (Billet Liberté, 1 or 2 Parks)",
    resort: "disneyland-paris",
    markets: ["eu", "intl"],
    parks: ["disneyland-park", "disney-adventure-world"],
    days: {
      min: 1,
      max: 1,
    },
    windowTable: [
      {
        days: 1,
        window: 1,
      },
    ],
    anchor: "first_use",
    hopping: "optional",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-adventure-world"],
      },
    ],
    blockouts: true,
    blockoutUrl:
      "https://www.disneylandparis.com/ars-availability-calendar/fr-fr?selectedPage=liberte",
    reservationUrl: "https://www.disneylandparis.com/en-usd/register-tickets",
    confidence: "medium",
    shopUrl: "https://tickets.disneylandparis.com/en-gb/tickets",
    description:
      "An undated 1-day ticket for one park or both, valid for a year; the visit date must be registered in advance.",
    notes:
      "Valid one year from the day after purchase. The visit date must be registered in Disney's reservation system, subject to availability; 31 October and 31 December are excluded, and 14 July for tickets bought up to 30 September 2026 (Disney FAQ via search index; Attraction Tickets terms list all three). Not refundable after the 14-day withdrawal period (Attraction Tickets: non-refundable). Dearer than most dated tickets; the planner should never suggest it.",
  },
  {
    id: "dlp-pass-bronze-one",
    name: "Disneyland Pass Bronze One (one park per visit)",
    resort: "disneyland-paris",
    markets: ["eu"],
    parks: ["disneyland-park", "disney-adventure-world"],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl:
      "https://www.disneylandparis.com/ars-availability-calendar/en-usd?selectedPage=dlpass",
    reservationUrl:
      "https://www.disneylandparis.com/ars-availability-calendar/en-usd?selectedPage=dlpass",
    confidence: "medium",
    shopUrl: "https://www.disneylandparis.com/en-int/offers/disneyland-pass",
    description:
      "Disneyland Paris annual pass valid on 170 days a year at one park per visit date.",
    notes:
      "Since 12 February 2026 the range is Bronze One, Bronze Max, Silver and Gold. Bronze One: 170 access days on its own validity calendar, one park per visit date (no switching). Every pass needs a reservation for each visit date, at most 3 held at once (Conditions Générales d'Abonnement, version of 1 June 2026, art. 3.5); restriction days by pass type (art. 3.6); no access to special evenings or events (art. 3.4).",
  },
  {
    id: "dlp-pass",
    name: "Disneyland Pass Bronze Max, Silver or Gold (both parks)",
    resort: "disneyland-paris",
    markets: ["eu"],
    parks: ["disneyland-park", "disney-adventure-world"],
    days: "unlimited",
    anchor: "first_use",
    hopping: "included",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-adventure-world"],
      },
    ],
    pass: true,
    blockouts: true,
    blockoutUrl:
      "https://www.disneylandparis.com/ars-availability-calendar/en-usd?selectedPage=dlpass",
    reservationUrl:
      "https://www.disneylandparis.com/ars-availability-calendar/en-usd?selectedPage=dlpass",
    confidence: "medium",
    shopUrl: "https://www.disneylandparis.com/en-int/offers/disneyland-pass",
    description:
      "Disneyland Paris annual passes with both parks per visit date: Bronze Max (210 days), Silver (300 days), Gold (every day, with Extra Magic Time).",
    notes:
      "Blockout rule only: Bronze Max is valid on 210 days and Silver on 300 days of their published validity calendars; Gold has no restriction days. All tiers need a reservation per visit date (3 held at once) and give no access to special evenings or events (CGA of 1 June 2026, art. 3.4-3.6). Gold holders get Extra Magic Time (Disney FAQ, search index). Tier day counts from Disney's own calendar PDFs and pass page as indexed, and a UK fan site (Nov 2025, updated Jul 2026).",
  },
  {
    id: "asterix-1-day",
    name: "Parc Astérix dated 1-day ticket (Billet Futé or Liberté Flex)",
    resort: "parc-asterix",
    markets: ["eu", "intl"],
    parks: ["parc-asterix"],
    days: {
      min: 1,
      max: 1,
    },
    windowTable: [
      {
        days: 1,
        window: 1,
      },
    ],
    anchor: "start",
    hopping: "none",
    confidence: "high",
    shopUrl: "https://www.parcasterix.fr/offres/billets",
    description: "One day at Parc Astérix on the date chosen at purchase.",
    notes:
      "Billet Futé: booked at least 7 days ahead, price varies by date, not refundable or changeable. Billet Liberté Flex: date changeable up to 1 hour before opening. 2026 tickets are valid 4 April 2026-3 January 2027 by the opening calendar, Liberté Flex 2027 27 March 2027-2 January 2028. Neither covers the Nocturnes Peur sur le Parc (19:00-01:00), which need their own ticket. Billet Tribu (5-15 people) and Journée Premium (with Filotomatix fast pass) are price variants of the same day ticket.",
    suggest: 10,
  },
  {
    id: "asterix-noel",
    name: "Parc Astérix Billet Noël 2026 (undated, Noël Gaulois only)",
    resort: "parc-asterix",
    markets: ["eu"],
    parks: ["parc-asterix"],
    days: {
      min: 1,
      max: 1,
    },
    windowTable: [
      {
        days: 1,
        window: 1,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    onSale: [
      {
        from: "2026-12-19",
        to: "2027-01-03",
      },
    ],
    confidence: "high",
    shopUrl: "https://www.parcasterix.fr/offres/billets",
    description:
      "One undated day at Parc Astérix during Noël Gaulois, 19 December 2026 to 3 January 2027.",
    notes:
      "Single price for adults and children, valid any one day 19 December 2026-3 January 2027; not refundable or exchangeable. A 2027 edition is not on sale yet.",
    suggest: 20,
  },
  {
    id: "asterix-2-day",
    name: "Parc Astérix 2-day ticket (2 consecutive days, ticket office only)",
    resort: "parc-asterix",
    markets: ["eu"],
    parks: ["parc-asterix"],
    days: {
      min: 2,
      max: 2,
    },
    windowTable: [
      {
        days: 2,
        window: 2,
      },
    ],
    anchor: "start",
    hopping: "none",
    confidence: "medium",
    shopUrl: "https://www.parcasterix.fr/tarifs-au-guichet",
    description:
      "Two consecutive days at Parc Astérix, sold at the park's ticket office.",
    notes:
      "'Billet 2 jours consécutifs' is listed only in the official ticket-office price list for the 2026 season ('Tarifs au 04/04/2026'); it is not sold online, so the start is the day of purchase at the gate. Whether it returns for 2027 is not stated.",
  },
  {
    id: "asterix-pass-saison",
    name: "Parc Astérix Pass Saison (Découverte, Gaulois, Premium)",
    resort: "parc-asterix",
    markets: ["eu"],
    parks: ["parc-asterix"],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    until: "2027-01-03",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://www.parcasterix.fr/offres/pass-saison",
    confidence: "medium",
    shopUrl: "https://www.parcasterix.fr/offres/pass-saison",
    description:
      "Parc Astérix season pass: one visit a day for the season, outside the pass's restriction days.",
    notes:
      "2026 passes: Découverte, Gaulois, Premium, valid for the 2026 season (4 April 2026-3 January 2027), one entry a day, restriction days by pass type, no access at private hires, special events or 9 November-18 December 2026 (CGU Pass Saison). The 2027 passes are announced ('Retrouvez bientôt nos offres Pass Saison 2027') but not on sale; until reflects the 2026 pass. Only for repeat visitors; the planner should not suggest it.",
  },
];

/** The purchase links of {@link PARIS_TICKETS}, one entry per product. */
export const PARIS_SEED_LINKS: SeedLinkEntry[] = [
  {
    productId: "dlp-1-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/disneyland-paris-tickets/1-day-1-park-disneyland-paris-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/disneyland-paris-tickets/1-day-1-park-disneyland-paris-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/paris/disneyland-paris/1-tag-1-park-ticket",
        locale: "de",
      },
    ],
    official: {
      seller: "Disneyland Paris",
      url: "https://tickets.disneylandparis.com/en-gb/tickets",
    },
  },
  {
    productId: "dlp-multi-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/paris-attraction-tickets/disneyland-paris-tickets",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/paris-attraction-tickets/disneyland-paris-tickets",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/paris/disneyland-paris",
        locale: "de",
      },
    ],
    official: {
      seller: "Disneyland Paris",
      url: "https://tickets.disneylandparis.com/en-gb/tickets",
    },
  },
  {
    productId: "dlp-1-day-liberte",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/paris-attraction-tickets/disneylandr-paris-tickets/disneylandr-paris-1-day-open-dated-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/paris-attraction-tickets/disneylandr-paris-tickets/disneylandr-paris-1-day-open-dated-ticket",
        locale: "nl",
      },
    ],
    official: {
      seller: "Disneyland Paris",
      url: "https://tickets.disneylandparis.com/en-gb/tickets",
    },
  },
  {
    productId: "dlp-pass-bronze-one",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Disneyland Paris",
      url: "https://www.disneylandparis.com/en-int/offers/disneyland-pass",
    },
  },
  {
    productId: "dlp-pass",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Disneyland Paris",
      url: "https://www.disneylandparis.com/en-int/offers/disneyland-pass",
    },
  },
  {
    productId: "asterix-1-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/paris-attraction-tickets/asterix-parkr-ticket-transport/asterix-parkr-ticket-transport",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/node/280257",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/paris-attraction-tickets/asterix-parkr-ticket-transport/asterix-parkr-ticket-transport",
        locale: "de",
      },
    ],
    official: {
      seller: "Parc Astérix",
      url: "https://www.parcasterix.fr/offres/billets",
    },
  },
  {
    productId: "asterix-noel",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Parc Astérix",
      url: "https://www.parcasterix.fr/offres/billets",
    },
  },
  {
    productId: "asterix-2-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Parc Astérix",
      url: "https://www.parcasterix.fr/tarifs-au-guichet",
    },
  },
  {
    productId: "asterix-pass-saison",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Parc Astérix",
      url: "https://www.parcasterix.fr/offres/pass-saison",
    },
  },
];
