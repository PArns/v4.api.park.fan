/**
 * Japan's ticket seed (PAR-1304): the products researched for late 2026 and
 * 2027 from the operators' pages (Tokyo Disney Resort's FAQ, Universal Studios
 * Japan's releases, Fuji-Q's and LEGOLAND Japan's ticket pages), each with its
 * official link only (no seller was shown to be cheaper; Attraction Tickets
 * sells nothing in Japan). No prices. Tokyo Disney Resort sells no multi-day
 * ticket and no full-day hopper: every day is a 1-Day Passport for one park,
 * and its After 3 and After 5 Passports are evening tickets (`entryFrom`,
 * `validDays`). The paid ride add-ons (Universal Express Pass, Fuji-Q's Zekkyo
 * Priority Ticket) admit nobody and are left out. Contract:
 * docs/frontend/trip-regions.md#tickets.
 */
import type { TicketProduct } from "./tickets";

export const JAPAN_TICKETS: TicketProduct[] = [
  {
    id: "tdr-1-day-passport",
    name: "1-Day Passport",
    resort: "tokyo-disney-resort",
    markets: ["jp", "intl"],
    parks: ["tokyo-disneyland", "tokyo-disneysea"],
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
    shopUrl: "https://www.tokyodisneyresort.jp/en/ticket/index.html",
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: "https://www.tokyodisneyresort.jp/en/ticket/index.html",
        official: true,
        sponsored: false,
      },
    ],
    description:
      "One day at Tokyo Disneyland or Tokyo DisneySea from opening; date and park chosen at purchase.",
    notes:
      "Official FAQ: multi-day passports 'are not sold'; buy one per day. The park is fixed at purchase. Date-variable price. Each date goes on sale at 14:00 JST two months ahead; sales stop without notice when the day's limit is reached, so popular days sell out (official ticket page, snippet). Up to 10 per online order; no refund for personal reasons. The online date can be changed while the new date is not sold out; the park and ticket type cannot (secondary: Disney Park Nerds 2026-08-30, HontoJapan 2026-07-27). Gate ticket booths are closed (secondary). Disney Hotel guests can buy passports for the dates of their stay even when online sales show sold out (secondary). Disney Premier Access is a paid per-attraction add-on bought in the app after entry; not a ticket.",
    suggest: 10,
  },
  {
    id: "tdr-after-3-passport",
    name: "After 3 Passport (Weekends & Holidays)",
    resort: "tokyo-disney-resort",
    markets: ["jp"],
    parks: ["tokyo-disneyland", "tokyo-disneysea"],
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
    entryFrom: "15:00",
    validDays: ["weekend", "holiday"],
    confidence: "high",
    shopUrl: "https://www.tokyodisneyresort.jp/en/ticket/index.html",
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: "https://www.tokyodisneyresort.jp/en/ticket/index.html",
        official: true,
        sponsored: false,
      },
    ],
    description:
      "Entry to the selected park from 15:00 on Saturdays, Sundays and national holidays.",
    notes:
      "Official FAQ: 'After 3 Passport (Weekends & Holidays)'. Replaced the Early Evening Passport on 2026-09-15 (OLC release 2026-04-21, snippet). Fixed date and park; no second park. In summer 2026 (07-01..09-14) the 'After 3 Summer Passport' was valid every day.",
  },
  {
    id: "tdr-after-5-passport",
    name: "After 5 Passport (Weekdays)",
    resort: "tokyo-disney-resort",
    markets: ["jp"],
    parks: ["tokyo-disneyland", "tokyo-disneysea"],
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
    entryFrom: "17:00",
    validDays: ["weekday"],
    confidence: "high",
    shopUrl: "https://www.tokyodisneyresort.jp/en/ticket/index.html",
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: "https://www.tokyodisneyresort.jp/en/ticket/index.html",
        official: true,
        sponsored: false,
      },
    ],
    description:
      "Entry to the selected park from 17:00 on weekdays that are not holidays.",
    notes:
      "Official FAQ: 'After 5 Passport (Weekdays)', from 5:00 p.m. on weekdays, excluding holidays. Replaced the Weeknight Passport on 2026-09-15. Fixed date and park; no second park.",
  },
  {
    id: "tdr-1-day-park-hopper",
    name: "1-Day Park Hopper Passport (Limited Period)",
    resort: "tokyo-disney-resort",
    markets: ["jp"],
    parks: ["tokyo-disneyland", "tokyo-disneysea"],
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
    hopping: "included",
    hopSets: [
      {
        parks: ["tokyo-disneyland", "tokyo-disneysea"],
      },
    ],
    onSale: [
      {
        from: "2026-01-13",
        to: "2026-03-31",
      },
      {
        from: "2026-07-01",
        to: "2026-09-14",
      },
    ],
    confidence: "medium",
    shopUrl: "https://www.tokyodisneyresort.jp/en/ticket/park_hopper",
    links: [
      {
        seller: "Tokyo Disney Resort",
        url: "https://www.tokyodisneyresort.jp/en/ticket/park_hopper",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Start park chosen at purchase; from 11:00 both parks. Sold in limited numbers for admission 2026-01-13..03-31 (KKday listing quoting TDR) and 2026-07-01..09-14 (Disney Park Nerds). No period announced for 2027: archived, kept so an owned ticket can name it.",
    archived: true,
  },
  {
    id: "usj-1-day-studio-pass",
    name: "1-Day Studio Pass",
    resort: "universal-studios-japan",
    markets: ["jp"],
    parks: ["universal-studios-japan"],
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
    shopUrl: "https://www.usj.co.jp/web/en/us/tickets",
    links: [
      {
        seller: "Universal Studios Japan",
        url: "https://www.usj.co.jp/web/en/us/tickets",
        official: true,
        sponsored: false,
      },
    ],
    description: "Park admission for the selected date.",
    notes:
      "Since visits on 2026-09-01 every Studio Pass is bought for a date (USJ notice 2026-07-31, snippet; Travelism 2026-08-19). Tickets from the official store can move to another date from purchase until the 90th day counted from the day after the original date; a price difference applies; type and age category cannot change (secondary, quoting USJ). Date-variable price. QR codes bought from ticket-agency resale or auction sites are voided (USJ ticket page, snippet). Super Nintendo World needs an Area Timed Entry ticket on most days (see usj-express-pass).",
    suggest: 10,
  },
  {
    id: "usj-1-5-day-studio-pass",
    name: "1.5-Day Studio Pass",
    resort: "universal-studios-japan",
    markets: ["jp"],
    parks: ["universal-studios-japan"],
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
    shopUrl: "https://www.usj.co.jp/web/en/us/tickets",
    links: [
      {
        seller: "Universal Studios Japan",
        url: "https://www.usj.co.jp/web/en/us/tickets",
        official: true,
        sponsored: false,
      },
    ],
    description:
      "Day 1 from 15:00, day 2 from opening, on two consecutive days.",
    notes:
      "Its first day admits from 15:00, which the plan does not model: an owned one is planned as two whole days, and it is never suggested. USJ ticket lineup (Japanese, snippet): day 1 from 15:00, day 2 from park opening. 'Valid only for 2 consecutive days' (several resellers incl. JTB's product page). Since visits on 2026-09-01 every Studio Pass is bought for a date (USJ notice 2026-07-31, snippet; Travelism 2026-08-19). Tickets from the official store can move to another date from purchase until the 90th day counted from the day after the original date; a price difference applies; type and age category cannot change (secondary, quoting USJ). Date-variable price. QR codes bought from ticket-agency resale or auction sites are voided (USJ ticket page, snippet).",
  },
  {
    id: "usj-2-day-studio-pass",
    name: "2-Day Studio Pass",
    resort: "universal-studios-japan",
    markets: ["jp"],
    parks: ["universal-studios-japan"],
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
    shopUrl: "https://www.usj.co.jp/web/en/us/tickets",
    links: [
      {
        seller: "Universal Studios Japan",
        url: "https://www.usj.co.jp/web/en/us/tickets",
        official: true,
        sponsored: false,
      },
    ],
    description: "Two full days.",
    notes:
      "Resellers say two consecutive days; the official lineup snippet says only '2 days'. The plan keeps them consecutive, the side that cannot plan a day the ticket does not allow. Since visits on 2026-09-01 every Studio Pass is bought for a date (USJ notice 2026-07-31, snippet; Travelism 2026-08-19). Tickets from the official store can move to another date from purchase until the 90th day counted from the day after the original date; a price difference applies; type and age category cannot change (secondary, quoting USJ). Date-variable price. QR codes bought from ticket-agency resale or auction sites are voided (USJ ticket page, snippet).",
    suggest: 30,
  },
  {
    id: "usj-twilight-pass",
    name: "Twilight Pass",
    resort: "universal-studios-japan",
    markets: ["jp"],
    parks: ["universal-studios-japan"],
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
    entryFrom: "15:00",
    confidence: "medium",
    shopUrl: "https://www.usj.co.jp/web/en/us/tickets",
    links: [
      {
        seller: "Universal Studios Japan",
        url: "https://www.usj.co.jp/web/en/us/tickets",
        official: true,
        sponsored: false,
      },
    ],
    description: "Entry from 15:00 on the selected date.",
    notes:
      "USJ ticket lineup (Japanese, snippet). Not offered on every date (unverified). Since visits on 2026-09-01 every Studio Pass is bought for a date (USJ notice 2026-07-31, snippet; Travelism 2026-08-19). Tickets from the official store can move to another date from purchase until the 90th day counted from the day after the original date; a price difference applies; type and age category cannot change (secondary, quoting USJ). Date-variable price. QR codes bought from ticket-agency resale or auction sites are voided (USJ ticket page, snippet).",
  },
  {
    id: "fujiq-one-day-pass",
    name: "One Day Pass (unlimited rides)",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    shopUrl: "https://www.fujiq.jp/en/ticket/",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/ticket/",
        official: true,
        sponsored: false,
      },
    ],
    description:
      "Unlimited rides for one day, apart from a few attractions; price by date.",
    notes:
      "Fuji-Q ticket page and monthly price lists (read 2026-11-13). Online or at the gate. Entry to Fuji-Q is free (free admission ticket); rides need a pass or per-ride tickets. FUJIYAMA Walk/Slider and the haunted house Senritsu Meikyu need a separate timed ticket. Date change within 60 days of purchase, by 23:00 on the planned day; no refunds or partial changes (Fuji-Q date-change page).",
    suggest: 10,
  },
  {
    id: "fujiq-one-day-pass-priority-admission",
    name: "One-day pass with priority admission",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    onSale: [
      {
        to: "2026-11-30",
      },
      {
        from: "2027-02-06",
      },
    ],
    confidence: "high",
    shopUrl: "https://www.fujiq.jp/en/ticket/",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/ticket/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Online only. 'Not sold from December 2026 to 2027-02-05' (ticket page). Entry to Fuji-Q is free (free admission ticket); rides need a pass or per-ride tickets. FUJIYAMA Walk/Slider and the haunted house Senritsu Meikyu need a separate timed ticket. Date change within 60 days of purchase, by 23:00 on the planned day; no refunds or partial changes (Fuji-Q date-change page).",
    suggest: 20,
  },
  {
    id: "fujiq-premium-pass",
    name: "Fuji-Q Premium Pass",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    shopUrl: "https://www.fujiq.jp/en/event/fujiq_premiumpass.html",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/event/fujiq_premiumpass.html",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Priority admission + a wristband for unlimited priority boarding on 12 thrill rides + one timed ticket each for Senritsu Meikyu, FUJIYAMA Walk and Slider. Sold through CLUB Fuji-Q; no age categories; not for infants or seniors.",
  },
  {
    id: "fujiq-one-day-pass-virtual-queue",
    name: "One Day Pass with virtual queue 'Bunshin-kun'",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    shopUrl: "https://membersclub.fujiq.jp/ticket/set/?productCode=FP-VQ",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://membersclub.fujiq.jp/ticket/set/?productCode=FP-VQ",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "One Day Pass plus a virtual queue in the official 'Q-chan' app for selected rides. Limited; sold until the day before; adults, juniors and children only; no changes after purchase.",
  },
  {
    id: "fujiq-afternoon-pass",
    name: "Afternoon Pass",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    entryFrom: "13:00",
    confidence: "high",
    shopUrl: "https://www.fujiq.jp/en/ticket/",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/ticket/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Unlimited rides from 13:00. Online or at the gate. Entry to Fuji-Q is free (free admission ticket); rides need a pass or per-ride tickets. FUJIYAMA Walk/Slider and the haunted house Senritsu Meikyu need a separate timed ticket. Date change within 60 days of purchase, by 23:00 on the planned day; no refunds or partial changes (Fuji-Q date-change page).",
  },
  {
    id: "fujiq-sunset-night-pass",
    name: "Sunset Night Pass",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    entryFrom: "16:00",
    confidence: "high",
    shopUrl: "https://www.fujiq.jp/en/ticket/",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/ticket/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Unlimited rides from 16:00 on days open after 19:00; sold only on the day at the entrance windows and information. The plan does not check the days it is sold on.",
  },
  {
    id: "fujiq-thomas-land-pass",
    name: "Thomas Land Pass",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
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
    shopUrl: "https://www.fujiq.jp/en/ticket/",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/ticket/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "All 12 Thomas Land rides only; other rides need their own tickets. For families with small children.",
  },
  {
    id: "fujiq-annual-pass",
    name: "Annual Passport",
    resort: "fuji-q-highland",
    markets: ["jp"],
    parks: ["fuji-q-highland"],
    days: "unlimited",
    anchor: "start",
    hopping: "none",
    pass: true,
    confidence: "high",
    shopUrl: "https://www.fujiq.jp/en/ticket/yearpass.html",
    links: [
      {
        seller: "Fuji-Q Highland",
        url: "https://www.fujiq.jp/en/ticket/yearpass.html",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Unlimited rides for 365 days from the purchase date; online only; personal.",
  },
  {
    id: "legoland-japan-1-day",
    name: "LEGOLAND Japan 1DAY Passport",
    resort: "legoland-japan",
    markets: ["jp"],
    parks: ["legoland-japan"],
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
    shopUrl: "https://www.legoland.jp/en/tickets/1daypass/",
    links: [
      {
        seller: "LEGOLAND Japan Resort",
        url: "https://www.legoland.jp/en/tickets/1daypass/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Official ticket page and 1DAY price calendar (Oct 2026-Apr 2027). Price by date band (six bands); buying online before the day is cheaper than on the day, and a ticket issued at the gate on the day carries a per-ticket fee. Date change for a fee; no cancellation or refund. Child rate 3-18.",
    suggest: 10,
  },
  {
    id: "legoland-japan-combo-1-day",
    name: "LEGOLAND Japan + SEA LIFE Nagoya Combo 1DAY Passport",
    resort: "legoland-japan",
    markets: ["jp"],
    parks: ["legoland-japan"],
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
    shopUrl: "https://www.legoland.jp/en/tickets/1daypass/",
    links: [
      {
        seller: "LEGOLAND Japan Resort",
        url: "https://www.legoland.jp/en/tickets/1daypass/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "Both on the same day only (official). SEA LIFE Nagoya is not a park in our data. Price by date band (six bands); buying online before the day is cheaper than on the day, and a ticket issued at the gate on the day carries a per-ticket fee. Date change for a fee; no cancellation or refund. Child rate 3-18.",
    suggest: 20,
  },
  {
    id: "legoland-japan-hotel-combo-2-day",
    name: "Combo 2 Day Pass (LEGOLAND Japan Hotel guests only)",
    resort: "legoland-japan",
    markets: ["jp"],
    parks: ["legoland-japan"],
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
    confidence: "high",
    shopUrl: "https://www.legoland.jp/en/hotel/plans/",
    links: [
      {
        seller: "LEGOLAND Japan Resort",
        url: "https://www.legoland.jp/en/hotel/plans/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "The only 2-day LEGOLAND Japan ticket found. Included in hotel plans; LEGOLAND and SEA LIFE on the arrival day and the following day, with re-entry; collected at hotel reception from the morning of arrival (official hotel FAQ).",
  },
  {
    id: "legoland-japan-annual-pass",
    name: "LEGOLAND Japan Annual Pass (Weekday, Standard, Premium)",
    resort: "legoland-japan",
    markets: ["jp"],
    parks: ["legoland-japan"],
    days: "unlimited",
    anchor: "first_use",
    hopping: "none",
    pass: true,
    blockouts: true,
    blockoutUrl:
      "https://www.legoland.jp/operation/information-for-annual-pass-holders/annual-pass-calendar/",
    confidence: "medium",
    shopUrl: "https://www.legoland.jp/en/tickets/annual-pass/",
    links: [
      {
        seller: "LEGOLAND Japan Resort",
        url: "https://www.legoland.jp/en/tickets/annual-pass/",
        official: true,
        sponsored: false,
      },
    ],
    notes:
      "12 months. Weekday: LEGOLAND on 258 admission days; Standard and Premium add SEA LIFE; admission days per pass on the pass calendar. Whether the 12 months start at purchase or first use was not read.",
  },
];
