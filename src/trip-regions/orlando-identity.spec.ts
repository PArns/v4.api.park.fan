import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { addIsoDays } from "../common/utils/date.util";
import type { ClimateNormals } from "../external-apis/weather/climate-normals";
import { ORLANDO } from "./regions";
import {
  normalizePlanQuery,
  optimizePlan,
  planResponse,
} from "./trip-plan.assembly";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import type { TripParkDayInternal } from "./trip-calendar.util";
import { regionBestDays } from "./trip-best-days";
import { findTripDates } from "./trip-when";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";

/**
 * Orlando's answers do not move when the planner learns other regions: the
 * plan, `/when` and `/best-days` of a rich synthetic Orlando year (every park,
 * early slots, extended evenings, party nights, closures, heat, the hurricane
 * season, school breaks, last year's days, owned tickets) are compared whole
 * with the answers the code gave before the multi-region refactor (PAR-1302),
 * kept in `__fixtures__/orlando-identity.json`.
 *
 * `WRITE_ORLANDO_IDENTITY=1` rewrites the file: only for a change that is
 * meant to move Orlando's answers, and the diff then says how.
 */

const FIXTURE = join(__dirname, "__fixtures__", "orlando-identity.json");
const TODAY = "2027-01-04";
const FROM = "2027-01-04";
const DAYS = 330;

/** A deterministic number in [0, mod) per key. */
function hash(key: string, mod: number): number {
  let h = 7;
  for (const ch of key) h = (h * 31 + ch.charCodeAt(0)) % 104729;
  return h % mod;
}

const pad = (n: number) => String(n).padStart(2, "0");
const at = (date: string, hour: number, minute = 0) =>
  `${date}T${pad(Math.floor(hour) % 24)}:${pad(minute)}:00-05:00`;

/** The synthetic year: every Orlando park, the features each route reads. */
function orlandoYear(): InternalRegionCalendar {
  const days: InternalRegionCalendar["days"] = [];
  for (let i = 0; i < DAYS; i++) {
    const date = addIsoDays(FROM, i);
    const month = Number(date.slice(5, 7));
    const weekday = new Date(`${date}T12:00:00Z`).getUTCDay();
    const parks: Record<string, TripParkDayInternal> = {};
    for (const config of ORLANDO.parks) {
      const slug = config.slug;
      const h = hash(`${slug}|${date}`, 1000);
      const season = 14 * Math.sin((i + slug.length) / 11);
      const weekly = weekday === 0 || weekday === 6 ? 9 : 0;
      const wait = 38 + season + weekly + (h % 23) + (h % 7) / 10;
      const closed =
        (slug === "disneys-typhoon-lagoon-water-park" && date < "2027-01-27") ||
        (slug === "universal-volcano-bay" && date < "2027-03-24") ||
        (config.type === "water_park" && hash(`${slug}${date}c`, 9) === 0);
      const unknown = !closed && i > 240 && config.group !== "day-trips";
      const lastYear = !closed && !unknown && i > 180;
      const longRange = !closed && !unknown && !lastYear && i > 60;
      const hours = config.type === "theme_park" ? 12 + (h % 3) : 8;
      const party =
        slug === "magic-kingdom-park" &&
        ((month >= 8 && month <= 10 && h % 3 === 0) ||
          (month >= 11 && h % 2 === 0));
      const dayHours = party ? 9 : hours;
      const extended =
        !party &&
        DISNEY_THEMES.includes(slug) &&
        (weekday === 1 || weekday === 3) &&
        h % 2 === 0;
      const early =
        config.type === "theme_park" && config.group !== "day-trips"
          ? config.group === "universal-orlando"
            ? "early_park_admission"
            : "early_entry"
          : undefined;
      const tempMax =
        month >= 6 && month <= 9 ? 31 + (h % 6) : month >= 4 ? 28 : 22;
      const weather =
        i < 16
          ? {
              tempMax: tempMax + (h % 2 ? 2 : 0),
              precipitationMm: h % 5,
              icon: h % 4 === 0 ? 61 : 1,
              basis: "forecast" as const,
            }
          : {
              tempMax,
              precipitationMm: (h % 9) / 2,
              basis: "climate_normal" as const,
            };
      if (closed) {
        parks[slug] = {
          status: "CLOSED",
          hoursPublished: true,
          crowdLevel: "closed",
          basis: "none",
          ...(h % 2 === 0 && { closedBy: `closure-${slug}` }),
        };
        continue;
      }
      const opens = 9;
      const closes = opens + dayHours;
      const cell: TripParkDayInternal = {
        status: unknown ? "UNKNOWN" : "OPERATING",
        hoursPublished: !unknown,
        crowdLevel: ["very_low", "low", "moderate", "high", "very_high"][h % 5],
        basis: lastYear ? "last_year" : longRange ? "long_range" : "forecast",
        ...(config.type === "theme_park" &&
          !(h % 37 === 0) && {
            headlinerWait: Math.round(wait / 5) * 5,
            waitExact: wait,
          }),
        ...(!unknown && {
          opensAt: at(date, opens),
          closesAt: at(date, closes),
          dayHours,
        }),
        usualHours: hours,
        ...(early &&
          !unknown && {
            early: {
              kind: early,
              opensAt: at(date, opens - 1),
              closesAt: at(date, opens),
            },
          }),
        ...(party &&
          !unknown && {
            evening: {
              kind: "ticketed_event" as const,
              opensAt: at(date, 19),
              closesAt: at(date, 23, 59),
            },
            closesEarly: true as const,
            dayGuestCloseAt: at(date, closes),
          }),
        ...(extended &&
          !unknown && {
            evening: {
              kind: "extended_evening" as const,
              opensAt: at(date, closes),
              closesAt: at(date, closes + 2),
            },
          }),
        weather,
        seasons: party ? ["season-party"] : [],
      };
      parks[slug] = cell;
    }
    const school =
      (date >= "2027-03-13" && date <= "2027-03-21") ||
      (date >= "2027-06-05" && date <= "2027-08-09") ||
      (date >= "2027-11-22" && date <= "2027-11-26");
    days.push({
      date,
      isHoliday: hash(date, 29) === 0,
      isSchoolVacation: school,
      ...(i > 300 && { schoolVacationUnknown: true }),
      ...(school && { holidays: ["Spring break"] }),
      parks,
    });
  }
  return {
    meta: {
      region: "orlando",
      timezone: "America/New_York",
      from: FROM,
      to: addIsoDays(FROM, DAYS - 1),
      computedAt: "identity",
      forecastUntil: addIsoDays(FROM, 180),
      publishedHoursUntil: {},
    },
    parks: ORLANDO.parks.map((p) => p.slug),
    days,
    seasons: [
      {
        id: "season-party",
        parkSlug: "magic-kingdom-park",
        kind: "halloween",
        name: "Party",
        startDate: "2027-08-01",
        endDate: "2027-12-31",
        dates: null,
        status: "confirmed",
        separateTicket: true,
        opensAt: "19:00",
        closesAt: "24:00",
      } as unknown as InternalRegionCalendar["seasons"][number],
    ],
  };
}

const DISNEY_THEMES = [
  "magic-kingdom-park",
  "epcot",
  "disneys-hollywood-studios",
  "disneys-animal-kingdom-theme-park",
];

/** A slice of the year: what a route reads for one window. */
function slice(
  year: InternalRegionCalendar,
  from: string,
  to: string,
): InternalRegionCalendar {
  return {
    ...year,
    meta: { ...year.meta, from, to },
    days: year.days.filter((d) => d.date >= from && d.date <= to),
  };
}

/** Climate normals with a summer the heat term reads. */
function climate(): ClimateNormals {
  const out: ClimateNormals = {};
  for (let i = 0; i < 366; i++) {
    const md = addIsoDays("2028-01-01", i).slice(5, 10);
    const month = Number(md.slice(0, 2));
    out[md] = {
      temperatureMax: month >= 6 && month <= 9 ? 32 : month >= 4 ? 27 : 22,
      temperatureMin: 15,
      precipitationSum: month >= 6 && month <= 9 ? 6 : 2,
    } as ClimateNormals[string];
  }
  return out;
}

const PLAN_QUERIES: Array<
  Omit<TripPlanQueryDto, "from" | "to"> & {
    from: string;
    to: string;
  }
> = [
  { from: "2027-01-10", to: "2027-01-20" },
  {
    from: "2027-02-01",
    to: "2027-02-14",
    parks:
      "magic-kingdom-park:2,epcot,universal-epic-universe,universal-studios-florida,universal-islands-of-adventure,seaworld-orlando",
    hotel: "disney_deluxe",
    area: "disney",
    fireworks: "want",
  },
  {
    from: "2027-03-01",
    to: "2027-03-08",
    parks:
      "magic-kingdom-park,epcot,disneys-hollywood-studios,disneys-animal-kingdom-theme-park,universal-studios-florida,universal-islands-of-adventure,universal-epic-universe,busch-gardens-tampa,legoland-florida",
    ticket: "hopper",
    hop: "1",
    area: "universal",
    hotel: "universal_premier",
    rest: "none",
  },
  {
    from: "2027-02-01",
    to: "2027-02-14",
    tickets: "wdw-uk-14-day-magic@2027-02-01,uor-date-based.3.h?",
    area: "international_drive",
  },
  {
    from: "2027-04-05",
    to: "2027-04-15",
    parks:
      "epcot:2,universal-volcano-bay,disneys-typhoon-lagoon-water-park,discovery-cove-orlando,aquatica-orlando",
    rest: "max:3",
    lock: "epcot@2027-04-07,rest@2027-04-09",
    split: "fixed",
    arrival: "park",
    departure: "park",
    blocks: "0",
  },
  {
    from: "2027-08-20",
    to: "2027-09-02",
    tickets: "wdw-date-based.4.h,uor-date-based.3",
    hopper: "universal-orlando",
    hop: "1",
    evenings: "1",
  },
  {
    from: "2027-10-20",
    to: "2027-11-05",
    parks: "magic-kingdom-park,epcot,universal-epic-universe,seaworld-orlando",
    fireworks: "want",
    tickets: "seaworld-fun-card,wdw-annual-pass",
  },
];

function answers() {
  const year = orlandoYear();
  const known = ORLANDO.parks.map((p) => p.slug);
  const plans = PLAN_QUERIES.map((query) => {
    const q = normalizePlanQuery(ORLANDO, known, query, TODAY);
    const calendar = slice(year, q.from, q.to);
    const trip = optimizePlan(ORLANDO, q, calendar);
    return planResponse(ORLANDO, q, calendar, trip, {
      computedAt: "identity",
      today: TODAY,
      leadMae: new Map([
        [10, 12],
        [30, 15],
      ]),
    });
  });
  const when = [
    { days: 10, parks: undefined, within: undefined },
    {
      days: 7,
      parks:
        "magic-kingdom-park,epcot,universal-epic-universe,universal-islands-of-adventure",
      within: undefined,
    },
  ].map(({ days, parks }) => {
    const from = "2027-01-10";
    const to = "2027-11-20";
    return findTripDates({
      region: ORLANDO,
      calendar: slice(year, from, to),
      climate: climate(),
      climatePark: "magic-kingdom-park",
      query: {
        from,
        to,
        days,
        limit: 5,
        gap: 7,
        suggest: true,
        plan: normalizePlanQuery(
          ORLANDO,
          known,
          {
            from,
            to: addIsoDays(from, days - 1),
            ...(parks && { parks }),
          },
          TODAY,
          { from, to, starts: false },
        ),
      },
    });
  });
  const bestDays = [
    regionBestDays(ORLANDO, slice(year, "2027-01-10", "2027-02-20"), 5),
    regionBestDays(ORLANDO, slice(year, "2027-08-01", "2027-10-30"), 5),
  ];
  return JSON.parse(JSON.stringify({ plans, when, bestDays })) as Answers;
}

interface Answers {
  plans: unknown[];
  when: Array<{
    months: Array<Record<string, unknown>>;
    windows: Array<{ plan: Record<string, unknown> }>;
  }>;
  bestDays: unknown[];
}

/**
 * `parksLeftOut` (PAR-1309) is a new view of what Orlando already sent: the
 * chosen parks with no visit, each of them in `unplaced` with its reason.
 * Checked so, then left out of the comparison.
 */
function withoutParksLeftOut(answer: Record<string, unknown>): void {
  const leftOut = (answer.parksLeftOut ?? []) as Array<{
    park: string;
    reason: string;
  }>;
  const unplaced = (answer.unplaced ?? []) as Array<
    { slug: string; reason: string } | string
  >;
  for (const p of leftOut) {
    expect(
      unplaced.some((u) =>
        typeof u === "string"
          ? u === p.park
          : u.slug === p.park && u.reason === p.reason,
      ),
    ).toBe(true);
  }
  delete answer.parksLeftOut;
}

/**
 * The fields the multi-region contract added to Orlando's answers, each a new
 * name for a value Orlando already sent: `/when`'s months carry the region's
 * own school breaks as `localSchoolBreakDays` beside `usSchoolBreakDays`.
 * Checked equal, then left out of the comparison.
 */
function withoutAliases(answers: Answers): Answers {
  for (const plan of answers.plans) {
    withoutParksLeftOut(plan as Record<string, unknown>);
  }
  for (const when of answers.when) {
    for (const window of when.windows) withoutParksLeftOut(window.plan);
  }
  for (const when of answers.when) {
    for (const month of when.months) {
      expect(month.localSchoolBreakDays).toEqual(month.usSchoolBreakDays);
      delete month.localSchoolBreakDays;
    }
  }
  return answers;
}

describe("Orlando's answers", () => {
  it("are the answers the code gave before the multi-region refactor", () => {
    const now = withoutAliases(answers());
    if (process.env.WRITE_ORLANDO_IDENTITY === "1") {
      writeFileSync(FIXTURE, `${JSON.stringify(now)}\n`);
    }
    const before = JSON.parse(readFileSync(FIXTURE, "utf8")) as unknown;
    expect(now).toEqual(before);
  });
});
