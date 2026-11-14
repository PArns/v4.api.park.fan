/**
 * Southern California's ticket seed (PAR-1303): the products as researched
 * on 2026-11-13 for late 2026 and 2027, and their purchase links, in the shape
 * of `tickets.ts` and `ticket-seed-links.ts`. Read only when the region's
 * `ticket_products` table is empty; once seeded, the admin edits them. No
 * prices. Where a rule could not be settled, an entry keeps the side that
 * cannot plan a day the ticket does not allow and says so in `notes`.
 *
 * Disneyland's multi-day tickets are good for 13 days from the first use, the
 * first day counted (Disney Parks Blog; planDisney); its 1-day tickets are
 * priced by date in tiers, one product here. Every Disneyland day needs a park
 * reservation, and since 2026-06-09 the Park Hopper works at any time of day.
 *
 * Links: no cheapest authorized seller could be confirmed for any product
 * (the resellers' and Universal's stores refuse automated reads, and no
 * price was seen on both sides), so the seed lists Attraction Tickets' pages
 * for their locale, then the operator's own shop.
 */
import type { SeedLinkEntry } from "./ticket-seed-links";
import type { TicketProduct } from "./tickets";

export const SOUTHERN_CALIFORNIA_TICKETS: TicketProduct[] = [
  {
    id: "dlr-1-day",
    name: "Disneyland Resort 1-Day ticket (1 Park Per Day, Park Hopper with .h)",
    resort: "disneyland-resort",
    markets: ["us", "uk", "intl"],
    parks: ["disneyland-park", "disney-california-adventure-park"],
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
        parks: ["disneyland-park", "disney-california-adventure-park"],
      },
    ],
    confidence: "high",
    shopUrl: "https://disneyland.disney.go.com/admission/tickets/",
    notes:
      "Date-based: the price follows the date's tier (Tier 0 to Tier 6). Entry also needs a theme park reservation for that park and date. Park Hopper (.h): since 2026-06-09 the guest may cross to the other park at any time after entering the first, subject to capacity (the 11:00 rule ended). Attraction Tickets sells an open-dated 1-day ticket that also needs a reservation.",
    suggest: 10,
  },
  {
    id: "dlr-multi-day",
    name: "Disneyland Resort 2- to 5-Day ticket (1 Park Per Day, Park Hopper with .h)",
    resort: "disneyland-resort",
    markets: ["us", "uk", "intl"],
    parks: ["disneyland-park", "disney-california-adventure-park"],
    days: {
      min: 2,
      max: 5,
    },
    windowTable: [
      {
        days: 2,
        window: 13,
      },
      {
        days: 3,
        window: 13,
      },
      {
        days: 4,
        window: 13,
      },
      {
        days: 5,
        window: 13,
      },
    ],
    anchor: "first_use",
    hopping: "optional",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-california-adventure-park"],
      },
    ],
    confidence: "high",
    shopUrl: "https://disneyland.disney.go.com/admission/tickets/",
    notes:
      "Usable on any N days within 13 days of the first use, the first day counted as day 1 (Disney: 'within 13 days after first use'; planDisney: first day 17 June, last day 29 June), or by the ticket's stated expiry if earlier. No date tiers. A theme park reservation is needed for every day. Promotional and resident tickets differ and are left out.",
    suggest: 20,
  },
  {
    id: "citypass-socal-disneyland",
    name: "Southern California CityPASS: Disneyland Resort 2-Day + 2 attractions",
    resort: "disneyland-resort",
    markets: ["us", "uk", "intl"],
    parks: [
      "disneyland-park",
      "disney-california-adventure-park",
      "legoland-california",
      "seaworld-san-diego",
    ],
    days: {
      min: 4,
      max: 4,
    },
    windowTable: [
      {
        days: 4,
        window: 14,
      },
    ],
    anchor: "first_use",
    hopping: "optional",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-california-adventure-park"],
      },
    ],
    caps: [
      {
        parks: ["disneyland-park", "disney-california-adventure-park"],
        max: 2,
      },
      {
        parks: ["legoland-california"],
        max: 1,
      },
      {
        parks: ["seaworld-san-diego"],
        max: 1,
      },
    ],
    confidence: "medium",
    shopUrl: "https://www.citypass.com/southern-california",
    notes:
      "A Disneyland Resort 2-Day ticket (1 Park Per Day or Park Hopper version) plus one visit to each of two attractions chosen from LEGOLAND California, SeaWorld San Diego, San Diego Zoo and the Safari Park. 14 consecutive days from the first visit or reservation, that day included. Reservations needed at Disneyland and LEGOLAND.",
  },
  {
    id: "citypass-socal-universal",
    name: "Southern California CityPASS: Universal Studios Hollywood + 2 attractions",
    resort: "universal-studios-hollywood",
    markets: ["us", "uk", "intl"],
    parks: [
      "universal-studios-hollywood",
      "legoland-california",
      "seaworld-san-diego",
    ],
    days: {
      min: 4,
      max: 4,
    },
    windowTable: [
      {
        days: 4,
        window: 14,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    caps: [
      {
        parks: ["universal-studios-hollywood"],
        max: 2,
      },
      {
        parks: ["legoland-california"],
        max: 1,
      },
      {
        parks: ["seaworld-san-diego"],
        max: 1,
      },
    ],
    confidence: "medium",
    shopUrl: "https://www.citypass.com/southern-california",
    notes:
      "Universal Studios Hollywood '1 Day General Admission + 2nd Day Free' (the second visit within 7 days of the first, the first day included; second-visit blackout 26-31 December) plus two attractions as above. 14 consecutive days from the first visit. The 7-day rule for Universal's second day is stricter than the 14-day window and is not expressed by the caps.",
  },
  {
    id: "citypass-socal-disneyland-universal",
    name: "Southern California CityPASS: Disneyland Resort 2-Day + Universal Studios Hollywood + 2 attractions",
    resort: "disneyland-resort",
    markets: ["us", "uk", "intl"],
    parks: [
      "disneyland-park",
      "disney-california-adventure-park",
      "universal-studios-hollywood",
      "legoland-california",
      "seaworld-san-diego",
    ],
    days: {
      min: 6,
      max: 6,
    },
    windowTable: [
      {
        days: 6,
        window: 14,
      },
    ],
    anchor: "first_use",
    hopping: "optional",
    hopSets: [
      {
        parks: ["disneyland-park", "disney-california-adventure-park"],
      },
    ],
    caps: [
      {
        parks: ["disneyland-park", "disney-california-adventure-park"],
        max: 2,
      },
      {
        parks: ["universal-studios-hollywood"],
        max: 2,
      },
      {
        parks: ["legoland-california"],
        max: 1,
      },
      {
        parks: ["seaworld-san-diego"],
        max: 1,
      },
    ],
    confidence: "medium",
    shopUrl: "https://www.citypass.com/southern-california",
    notes:
      "Both the Disneyland 2-Day ticket and Universal's 1 Day + 2nd Day Free (second Universal visit within 7 days of the first, blackout 26-31 December), plus two attractions as above; 14 consecutive days from the first visit.",
  },
  {
    id: "citypass-san-diego",
    name: "San Diego CityPASS (SeaWorld San Diego and/or LEGOLAND California + 3 attractions)",
    resort: "day-trips",
    markets: ["us", "uk", "intl"],
    parks: ["seaworld-san-diego", "legoland-california"],
    days: {
      min: 1,
      max: 2,
    },
    windowTable: [
      {
        days: 1,
        window: 9,
      },
      {
        days: 2,
        window: 9,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    caps: [
      {
        parks: ["seaworld-san-diego"],
        max: 1,
      },
      {
        parks: ["legoland-california"],
        max: 1,
      },
    ],
    confidence: "high",
    shopUrl: "https://www.citypass.com/san-diego",
    notes:
      "Sold with SeaWorld San Diego, LEGOLAND California or both, plus three San Diego attractions that are not parks here. 9 consecutive days starting with the first visit; usable within a year of purchase. LEGOLAND needs a reservation.",
  },
  {
    id: "ush-1-day",
    name: "Universal Studios Hollywood 1-Day General Admission",
    resort: "universal-studios-hollywood",
    markets: ["us", "uk", "intl"],
    parks: ["universal-studios-hollywood"],
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
    shopUrl:
      "https://store.universalstudioshollywood.com/en/us/store/c/uh_ice_default_pb_tickets",
    notes: "Valid on the date selected.",
    suggest: 10,
  },
  {
    id: "ush-2-day",
    name: "Universal Studios Hollywood 2-Day General Admission (and 'Buy a Day, Get a 2nd Day Free')",
    resort: "universal-studios-hollywood",
    markets: ["us", "uk", "intl"],
    parks: ["universal-studios-hollywood"],
    days: {
      min: 2,
      max: 2,
    },
    windowTable: [
      {
        days: 2,
        window: 7,
      },
    ],
    anchor: "start",
    hopping: "none",
    confidence: "medium",
    shopUrl:
      "https://store.universalstudioshollywood.com/en/us/store/c/uh_ice_default_pb_tickets",
    notes:
      "First visit on the selected date, the second within 7 days of it, the first day included (Monday to the following Sunday). The 2026 'Buy a Day, Get a 2nd Day Free' offer had blockout dates and ended with visits by 2026-12-16; a 2027 edition is not confirmed. Universal's own terms are quoted from search-index copies of its pages.",
    suggest: 20,
  },
  {
    id: "ush-express",
    name: "Universal Studios Hollywood Universal Express (1 day)",
    resort: "universal-studios-hollywood",
    markets: ["us", "uk", "intl"],
    parks: ["universal-studios-hollywood"],
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
    confidence: "medium",
    shopUrl:
      "https://store.universalstudioshollywood.com/en/us/store/c/uh_ice_default_pb_tickets",
    notes:
      "Admission on the selected date plus one Express entry per ride, attraction and seated show (Fast & Furious: Hollywood Drift excluded per Universal's package description). Limited daily quantity.",
  },
  {
    id: "ush-vip-experience",
    name: "Universal Studios Hollywood VIP Experience",
    resort: "universal-studios-hollywood",
    markets: ["us", "uk", "intl"],
    parks: ["universal-studios-hollywood"],
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
    confidence: "medium",
    shopUrl:
      "https://www.universalstudioshollywood.com/web/en/us/vip-experience",
    notes:
      "A guided day on the selected date with admission, unlimited Express access, lunch and valet parking.",
  },
  {
    id: "knotts-1-day",
    name: "Knott's Berry Farm Park Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["knotts-berry-farm"],
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
    shopUrl: "https://www.sixflags.com/knotts/daily-tickets",
    notes:
      "Dated: the visit date is picked at purchase. Knott's Scary Farm nights need their own ticket.",
    suggest: 10,
  },
  {
    id: "sfmm-1-day",
    name: "Six Flags Magic Mountain Park Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["six-flags-magic-mountain"],
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
    shopUrl: "https://www.sixflags.com/magicmountain/daily-tickets",
    notes: "Dated: the visit date is picked at purchase.",
    suggest: 10,
  },
  {
    id: "sfmm-any-day",
    name: "Six Flags Magic Mountain Good Any Day Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["six-flags-magic-mountain"],
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
        to: "2027-01-03",
      },
    ],
    confidence: "high",
    shopUrl: "https://www.sixflags.com/magicmountain/daily-tickets",
    notes:
      "One visit on any operating day through 2027-01-03 (the version on sale in November 2026).",
    suggest: 20,
  },
  {
    id: "seaworld-sd-1-day",
    name: "SeaWorld San Diego Single-Day Ticket",
    resort: "day-trips",
    markets: ["us", "uk", "intl"],
    parks: ["seaworld-san-diego"],
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
    shopUrl: "https://seaworld.com/san-diego/tickets/",
    notes:
      "Valid only on the date printed. Attraction Tickets sells an open-dated one-day ticket for the calendar year chosen.",
    suggest: 10,
  },
  {
    id: "seaworld-sd-any-day",
    name: "SeaWorld San Diego Any Day Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["seaworld-san-diego"],
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
    confidence: "high",
    shopUrl: "https://seaworld.com/san-diego/tickets/",
    notes:
      "One visit on any day within 6 months of purchase (a Black Friday version expires 2027-03-31).",
    suggest: 20,
  },
  {
    id: "seaworld-sd-2-day",
    name: "SeaWorld San Diego Two-Day Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["seaworld-san-diego"],
    days: {
      min: 2,
      max: 2,
    },
    windowTable: [
      {
        days: 2,
        window: 14,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    confidence: "high",
    shopUrl: "https://seaworld.com/san-diego/tickets/",
    notes:
      "Two visits within 6 months of purchase, the second within 14 days of the first; counted here with the first day.",
    suggest: 30,
  },
  {
    id: "seaworld-sd-2-day-intl",
    name: "SeaWorld California Two-Day Ticket (Attraction Tickets)",
    resort: "day-trips",
    markets: ["uk", "intl"],
    parks: ["seaworld-san-diego"],
    days: {
      min: 2,
      max: 2,
    },
    windowTable: [
      {
        days: 2,
        window: 7,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    confidence: "medium",
    shopUrl: "https://seaworld.com/san-diego/tickets/",
    notes:
      "Attraction Tickets' version: open-dated within the calendar year chosen; the second visit within 7 days of the first; expires in full 7 days after first use.",
  },
  {
    id: "seaworld-sesame-two-park",
    name: "SeaWorld San Diego + Sesame Place San Diego Two-Park Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["seaworld-san-diego", "sesame-place-san-diego"],
    days: {
      min: 2,
      max: 2,
    },
    windowTable: [
      {
        days: 2,
        window: 180,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    caps: [
      {
        parks: ["seaworld-san-diego"],
        max: 1,
      },
      {
        parks: ["sesame-place-san-diego"],
        max: 1,
      },
    ],
    confidence: "medium",
    shopUrl: "https://sesameplace.com/san-diego/tickets/",
    notes:
      "One visit to each park; expires 6 months from purchase; no gap between the two visits is stated (the 180-day window stands for that). No blockout dates.",
  },
  {
    id: "seaworld-sd-fun-card-2027",
    name: "SeaWorld San Diego 2027 Fun Card",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["seaworld-san-diego"],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    until: "2027-12-31",
    pass: true,
    blockouts: true,
    blockoutUrl: "https://seaworld.com/san-diego/tickets/",
    confidence: "high",
    shopUrl: "https://seaworld.com/san-diego/tickets/",
    notes:
      "Unlimited visits to 2027-12-31 except the blockout dates 2027-05-30, 07-03, 07-04, 07-10, 07-17, 07-24, 07-31 and 09-05.",
  },
  {
    id: "sesame-sd-1-day",
    name: "Sesame Place San Diego Single-Day Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["sesame-place-san-diego"],
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
    shopUrl: "https://sesameplace.com/san-diego/tickets/",
    notes:
      "Valid only on the date selected. Outside summer and holiday weeks the park opens on weekends only.",
    suggest: 10,
  },
  {
    id: "sesame-sd-any-day",
    name: "Sesame Place San Diego Any-Day Ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["sesame-place-san-diego"],
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
    confidence: "high",
    shopUrl: "https://sesameplace.com/san-diego/tickets/",
    notes:
      "One visit within 6 months of purchase (a seasonal version expires 2026-12-31).",
    suggest: 20,
  },
  {
    id: "legoland-ca-1-day",
    name: "LEGOLAND California 1-Day ticket (with SEA LIFE Aquarium)",
    resort: "day-trips",
    markets: ["us", "uk", "intl"],
    parks: ["legoland-california"],
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
    shopUrl: "https://www.legoland.com/california/tickets-passes/tickets/",
    notes:
      "Admission to the theme park and SEA LIFE Aquarium; no reservation needed. The Water Park needs the 3-park Resort Hopper.",
    suggest: 10,
  },
  {
    id: "legoland-ca-multi-day",
    name: "LEGOLAND California multi-day 2-park ticket",
    resort: "day-trips",
    markets: ["us", "intl"],
    parks: ["legoland-california"],
    days: {
      min: 2,
      max: 2,
    },
    windowTable: [
      {
        days: 2,
        window: 3,
      },
    ],
    anchor: "start",
    hopping: "none",
    confidence: "low",
    shopUrl: "https://www.legoland.com/california/tickets-passes/tickets/",
    notes:
      "LEGOLAND sells multi-day tickets but its terms say only 'within the designated validity period stated at the time of purchase'. Sellers state 3, 5 or 90 days for the second day; the plan keeps the shortest (both days within 3 days).",
    suggest: 20,
  },
  {
    id: "gocity-la-all-inclusive",
    name: "Go City Los Angeles All-Inclusive Pass",
    resort: "day-trips",
    markets: ["us", "uk", "intl"],
    parks: ["knotts-berry-farm"],
    days: {
      min: 1,
      max: 1,
    },
    windowTable: [
      {
        days: 1,
        window: 14,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    caps: [
      {
        parks: ["knotts-berry-farm"],
        max: 1,
      },
    ],
    confidence: "low",
    shopUrl: "https://gocity.com/en/los-angeles",
    notes:
      "A 2- to 7-day pass for 35+ attractions; of the parks here it covers Knott's Berry Farm once. Pass days are used within 14 days of the first redemption; activation within a year of purchase. Terms from an authorized reseller's page.",
  },
  {
    id: "gocity-la-all-inclusive-plus",
    name: "Go City Los Angeles All-Inclusive Pass Plus",
    resort: "day-trips",
    markets: ["us", "uk", "intl"],
    parks: [
      "knotts-berry-farm",
      "universal-studios-hollywood",
      "legoland-california",
    ],
    days: {
      min: 1,
      max: 3,
    },
    windowTable: [
      {
        days: 1,
        window: 14,
      },
      {
        days: 2,
        window: 14,
      },
      {
        days: 3,
        window: 14,
      },
    ],
    anchor: "first_use",
    hopping: "none",
    caps: [
      {
        parks: ["knotts-berry-farm"],
        max: 1,
      },
      {
        parks: ["universal-studios-hollywood"],
        max: 1,
      },
      {
        parks: ["legoland-california"],
        max: 1,
      },
    ],
    confidence: "low",
    shopUrl: "https://gocity.com/en/los-angeles",
    notes:
      "The All-Inclusive Pass plus one Universal Studios Hollywood day (same-day reservation needed) and LEGOLAND California (not on the same day as the San Diego Zoo); every visit within the pass's days and 14 days of first redemption.",
  },
];

/** The purchase links of {@link SOUTHERN_CALIFORNIA_TICKETS}, one entry per product. */
export const SOUTHERN_CALIFORNIA_SEED_LINKS: SeedLinkEntry[] = [
  {
    productId: "dlr-1-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/los-angeles-attraction-tickets/disneyland-california/disneyland-california-1-day-ticket-one-park",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/los-angeles-attraction-tickets/disneyland-california/disneyland-california-1-day-ticket-one-park",
        locale: "nl",
      },
    ],
    official: {
      seller: "Disneyland Resort",
      url: "https://disneyland.disney.go.com/admission/tickets/",
    },
  },
  {
    productId: "dlr-multi-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/anaheim-buena-park-attraction-tickets/disneyland-california-tickets",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/anaheim-buena-park-attraction-tickets/disneyland-california-tickets",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/los-angeles/disneyland-resort-california",
        locale: "de",
      },
    ],
    official: {
      seller: "Disneyland Resort",
      url: "https://disneyland.disney.go.com/admission/tickets/",
    },
  },
  {
    productId: "citypass-socal-disneyland",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "CityPASS",
      url: "https://www.citypass.com/southern-california",
    },
  },
  {
    productId: "citypass-socal-universal",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "CityPASS",
      url: "https://www.citypass.com/southern-california",
    },
  },
  {
    productId: "citypass-socal-disneyland-universal",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "CityPASS",
      url: "https://www.citypass.com/southern-california",
    },
  },
  {
    productId: "citypass-san-diego",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/san-diego-attraction-tickets/san-diego-citypass",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/san-diego-attraction-tickets/san-diego-citypassr",
        locale: "nl",
      },
    ],
    official: {
      seller: "CityPASS",
      url: "https://www.citypass.com/san-diego",
    },
  },
  {
    productId: "ush-1-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/universal-studios-hollywood/universal-studios-hollywood-one-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/universal-studios-hollywood/universal-studios-hollywood-one-day-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/kalifornien/universal-studios-hollywood/tagesticket-mit-early-park-admission",
        locale: "de",
      },
    ],
    official: {
      seller: "Universal Studios Hollywood",
      url: "https://store.universalstudioshollywood.com/en/us/store/c/uh_ice_default_pb_tickets",
    },
  },
  {
    productId: "ush-2-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/universal-studios-hollywood-tickets/universal-studios-hollywood-buy-day-get-2nd-day-free-special-off",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/universal-studios-hollywood-tickets/universal-studios-hollywood-buy-day-get-2nd-day-free-special-off",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/universal-studios-hollywood/buy-a-day-get-a-2nd-day-free",
        locale: "de",
      },
    ],
    official: {
      seller: "Universal Studios Hollywood",
      url: "https://store.universalstudioshollywood.com/en/us/store/c/uh_ice_default_pb_tickets",
    },
  },
  {
    productId: "ush-express",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/los-angeles-attraction-tickets/universal-studios-hollywood-tickets/universal-studios-hollywood-front-line-pass",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/los-angeles-attraction-tickets/universal-studios-hollywood-tickets/universal-studios-hollywood-front-line-pass",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/kalifornien/universal-studios-hollywood/universal-express-tickets",
        locale: "de",
      },
    ],
    official: {
      seller: "Universal Studios Hollywood",
      url: "https://store.universalstudioshollywood.com/en/us/store/c/uh_ice_default_pb_tickets",
    },
  },
  {
    productId: "ush-vip-experience",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/los-angeles-attraction-tickets/universal-studios-hollywood-tickets/universal-studios-hollywood-vip-experience",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/los-angeles-attraction-tickets/universal-studios-hollywood-tickets/universal-studios-hollywood-vip-experience",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/kalifornien/universal-studios-hollywood/vip-experience-tickets",
        locale: "de",
      },
    ],
    official: {
      seller: "Universal Studios Hollywood",
      url: "https://www.universalstudioshollywood.com/web/en/us/vip-experience",
    },
  },
  {
    productId: "knotts-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Knott's Berry Farm",
      url: "https://www.sixflags.com/knotts/daily-tickets",
    },
  },
  {
    productId: "sfmm-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Six Flags Magic Mountain",
      url: "https://www.sixflags.com/magicmountain/daily-tickets",
    },
  },
  {
    productId: "sfmm-any-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Six Flags Magic Mountain",
      url: "https://www.sixflags.com/magicmountain/daily-tickets",
    },
  },
  {
    productId: "seaworld-sd-1-day",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/san-diego-attraction-tickets/seaworld-california/seaworld-san-diego-one-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/san-diego-attraction-tickets/seaworld-california/seaworld-san-diego-one-day-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/san-diego-attraction-tickets/seaworldr-california-2022/seaworldr-california-1-tagesticket",
        locale: "de",
      },
    ],
    official: {
      seller: "SeaWorld San Diego",
      url: "https://seaworld.com/san-diego/tickets/",
    },
  },
  {
    productId: "seaworld-sd-any-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld San Diego",
      url: "https://seaworld.com/san-diego/tickets/",
    },
  },
  {
    productId: "seaworld-sd-2-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld San Diego",
      url: "https://seaworld.com/san-diego/tickets/",
    },
  },
  {
    productId: "seaworld-sd-2-day-intl",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/san-diego-attraction-tickets/seaworld-california/seaworld-san-diego-two-day-ticket",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/san-diego-attraction-tickets/seaworld-california/seaworld-san-diego-two-day-ticket",
        locale: "nl",
      },
      {
        url: "https://www.attractiontickets.com/de/san-diego-attraction-tickets/seaworldr-california-2022/seaworldr-california-2-tagesticket",
        locale: "de",
      },
    ],
    official: {
      seller: "SeaWorld San Diego",
      url: "https://seaworld.com/san-diego/tickets/",
    },
  },
  {
    productId: "seaworld-sesame-two-park",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Sesame Place San Diego",
      url: "https://sesameplace.com/san-diego/tickets/",
    },
  },
  {
    productId: "seaworld-sd-fun-card-2027",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "SeaWorld San Diego",
      url: "https://seaworld.com/san-diego/tickets/",
    },
  },
  {
    productId: "sesame-sd-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Sesame Place San Diego",
      url: "https://sesameplace.com/san-diego/tickets/",
    },
  },
  {
    productId: "sesame-sd-any-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "Sesame Place San Diego",
      url: "https://sesameplace.com/san-diego/tickets/",
    },
  },
  {
    productId: "legoland-ca-1-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "LEGOLAND California Resort",
      url: "https://www.legoland.com/california/tickets-passes/tickets/",
    },
  },
  {
    productId: "legoland-ca-multi-day",
    cheapest: null,
    attractionTickets: null,
    official: {
      seller: "LEGOLAND California Resort",
      url: "https://www.legoland.com/california/tickets-passes/tickets/",
    },
  },
  {
    productId: "gocity-la-all-inclusive",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/los-angeles-attraction-tickets/go-los-angeles-all-inclusive-pass",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/los-angeles-attraction-tickets/go-los-angeles-all-inclusive-pass",
        locale: "nl",
      },
    ],
    official: {
      seller: "Go City",
      url: "https://gocity.com/en/los-angeles",
    },
  },
  {
    productId: "gocity-la-all-inclusive-plus",
    cheapest: null,
    attractionTickets: [
      {
        url: "https://www.attractiontickets.com/en/los-angeles-attraction-tickets/go-city-los-angeles-all-inclusive-pass-plus",
        locale: "en",
      },
      {
        url: "https://www.attractiontickets.com/en-nl/los-angeles-attraction-tickets/go-city-los-angeles-all-inclusive-pass-plus",
        locale: "nl",
      },
    ],
    official: {
      seller: "Go City",
      url: "https://gocity.com/en/los-angeles",
    },
  },
];
