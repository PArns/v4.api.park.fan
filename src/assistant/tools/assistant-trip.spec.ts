import { DataSource } from "typeorm";
import { fromZonedTime } from "date-fns-tz";
import type { Redis } from "ioredis";
import { runAssistantTool } from "./assistant-tools";
import { localizePageUrls } from "./assistant-actions";
import {
  dateRuns,
  hubPath,
  regionParkFor,
  type RegionParkName,
} from "./assistant-trip";
import { ParksService } from "../../parks/parks.service";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import type { CalendarService } from "../../parks/services/calendar.service";
import {
  TripRegionService,
  type InternalRegionCalendar,
} from "../../trip-regions/trip-region.service";
import { TripPlanService } from "../../trip-regions/trip-plan.service";
import type { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
import { ORLANDO } from "../../trip-regions/regions";
import { isoInZone } from "../../trip-regions/trip-calendar.util";
import type { TripParkDayInternal } from "../../trip-regions/trip-calendar.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";

const TZ = "America/New_York";
const today = getCurrentDateInTimezone(TZ);
const day = (n: number) => addIsoDays(today, n);
const at = (date: string, clock: string) =>
  isoInZone(fromZonedTime(`${date}T${clock}:00`, TZ), TZ);

const NAMES: Record<string, string> = {
  "magic-kingdom-park": "Magic Kingdom Park",
  epcot: "EPCOT",
  "disneys-hollywood-studios": "Disney's Hollywood Studios",
  "disneys-animal-kingdom-theme-park": "Disney's Animal Kingdom Theme Park",
  "universal-studios-florida": "Universal Studios Florida",
  "universal-islands-of-adventure": "Islands of Adventure",
  "universal-epic-universe": "Epic Universe",
  "universal-volcano-bay": "Volcano Bay",
  "seaworld-orlando": "SeaWorld Orlando",
};
const idOf = (slug: string) =>
  `00000000-0000-4000-8000-${String(
    ORLANDO.parks.findIndex((p) => p.slug === slug),
  ).padStart(12, "0")}`;
const regionParks = ORLANDO.parks.map((config) => ({
  config,
  park: {
    id: idOf(config.slug),
    slug: config.slug,
    name: NAMES[config.slug] ?? config.slug,
    continentSlug: config.continent,
    countrySlug: config.country,
    citySlug: config.city,
    timezone: TZ,
    retiredAt: null,
    curatedAbbreviations: config.slug === "magic-kingdom-park" ? "MK" : null,
  },
}));

/** A park day as the region calendar holds it, open 09:00 until `close`. */
function open(
  date: string,
  wait: number,
  basis: TripParkDayInternal["basis"] = "forecast",
  close = "22:00",
): TripParkDayInternal {
  return {
    status: "OPERATING",
    hoursPublished: true,
    opensAt: at(date, "09:00"),
    closesAt: at(date, close),
    crowdLevel: "low",
    headlinerWait: wait,
    basis,
    dayHours: Number(close.slice(0, 2)) - 9,
  };
}

const PARTY = "s-party";
const FOOD = "s-food";

/** Five trip days: MK with Early Entry and a party night, EPCOT, Epic past the forecast. */
function calendar(): InternalRegionCalendar {
  const dates = [10, 11, 12, 13, 14].map(day);
  return {
    meta: {
      region: "orlando",
      timezone: TZ,
      from: dates[0],
      to: dates[4],
      computedAt: "",
      publishedHoursUntil: {},
    },
    parks: ["magic-kingdom-park", "epcot", "universal-epic-universe"],
    days: dates.map((date, i) => {
      const mk = open(date, [50, 50, 30, 60, 60][i], "forecast", "22:00");
      mk.early = {
        kind: "early_entry",
        opensAt: at(date, "08:30"),
        closesAt: at(date, "09:00"),
      };
      if (i === 2) {
        Object.assign(mk, {
          closesAt: at(date, "18:00"),
          dayHours: 9,
          evening: {
            kind: "ticketed_event",
            opensAt: at(date, "19:00"),
            closesAt: at(addIsoDays(date, 1), "00:00"),
          },
          seasons: [PARTY],
        });
      }
      const epcot = open(date, [35, 35, 40, 45, 45][i]);
      epcot.seasons = [FOOD];
      const epic = open(
        date,
        [70, 70, 75, 55, 55][i],
        i >= 3 ? "last_year" : "forecast",
      );
      return {
        date,
        isHoliday: false,
        isSchoolVacation: false,
        parks: {
          "magic-kingdom-park": mk,
          epcot,
          "universal-epic-universe": epic,
        },
      };
    }),
    seasons: [
      {
        id: PARTY,
        parkSlug: "magic-kingdom-park",
        kind: "christmas",
        name: "Mickey's Very Merry Christmas Party",
        startDate: day(0),
        endDate: day(40),
        dates: [day(12), day(14), day(16), day(60)],
        status: "confirmed",
        separateTicket: true,
        opensAt: "19:00",
        closesAt: "24:00",
      },
      {
        id: FOOD,
        parkSlug: "epcot",
        kind: "special_event",
        name: "EPCOT International Food & Wine Festival",
        startDate: day(0),
        endDate: day(30),
        dates: null,
        status: "expected",
        separateTicket: false,
        opensAt: null,
        closesAt: null,
      },
    ],
  };
}

const PHL = {
  id: "11111111-1111-4111-8111-111111111111",
  slug: "phantasialand",
  name: "Phantasialand",
  timezone: "Europe/Berlin",
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
};

/** Every day from `from` to `to` open at each of `slugs`, with waits that differ by park and day. */
function wideCalendar(
  from: string,
  to: string,
  slugs: readonly string[],
): InternalRegionCalendar {
  const dates: string[] = [];
  for (let d = from; d <= to; d = addIsoDays(d, 1)) dates.push(d);
  return {
    meta: {
      region: "orlando",
      timezone: TZ,
      from,
      to,
      computedAt: "",
      publishedHoursUntil: {},
    },
    parks: [...slugs],
    days: dates.map((date, i) => ({
      date,
      isHoliday: false,
      isSchoolVacation: false,
      parks: Object.fromEntries(
        slugs.map((slug, p) => [
          slug,
          {
            ...open(date, 30 + ((p * 7 + i * 13) % 40)),
            seasons: [FOOD],
          },
        ]),
      ),
    })),
    seasons: calendar().seasons,
  };
}

function setup(
  build: (from: string, to: string) => InternalRegionCalendar = () =>
    calendar(),
) {
  const store = new Map<string, string>();
  const redis = {
    get: jest.fn(async (k: string) => store.get(k) ?? null),
    set: jest.fn(async (k: string, v: string) => {
      store.set(k, v);
      return "OK";
    }),
  } as unknown as Redis;
  const parks = {
    findById: jest.fn(async (id: string) => {
      if (id === PHL.id) return PHL;
      const found = regionParks.find((p) => p.park.id === id);
      return found?.park ?? null;
    }),
  };
  const regions = new TripRegionService(
    parks as unknown as ParksService,
    {} as CalendarService,
    {} as ParkSeasonService,
    redis,
  );
  jest.spyOn(regions, "resolveParks").mockResolvedValue(regionParks as never);
  const internalCalendar = jest
    .spyOn(regions, "internalCalendar")
    .mockImplementation(async (_region, from, to) => build(from, to));
  const plans = new TripPlanService(
    regions,
    {
      getLeadTimeMae: jest.fn(async () => null),
    } as unknown as PredictionLeadSnapshotService,
    redis,
  );
  const seasons = {
    forParkInRange: jest.fn(async () => [
      {
        id: "s-hw",
        parkId: PHL.id,
        kind: "halloween",
        name: null,
        startDate: day(1),
        endDate: day(20),
        dates: null,
        status: "announced",
        separateTicket: false,
        opensAt: null,
        closesAt: null,
      },
    ]),
  };
  const providers = new Map<unknown, unknown>([
    [TripRegionService, regions],
    [TripPlanService, plans],
    [ParksService, parks],
    [ParkSeasonService, seasons],
    [
      DataSource,
      {
        getRepository: () => ({
          find: async ({ where }: { where: { slug: string } }) =>
            where.slug === PHL.slug ? [PHL] : [],
        }),
      },
    ],
  ]);
  const call = async (name: string, input: unknown, locale = "de") => {
    const onError = jest.fn();
    const result = await runAssistantTool(
      name,
      input,
      { get: <T>(token: unknown) => providers.get(token) as T, locale },
      onError,
    );
    expect(onError).not.toHaveBeenCalled();
    return {
      ...result,
      json: result.isError
        ? null
        : (JSON.parse(result.content) as Record<string, any>),
    };
  };
  return { call, internalCalendar, seasons };
}

describe("regionParkFor", () => {
  const parks: RegionParkName[] = regionParks.map(({ config, park }) => ({
    slug: config.slug,
    id: park.id,
    name: park.name,
    abbreviations: park.curatedAbbreviations ? ["MK"] : [],
  }));

  it("finds a park by slug, name, abbreviation or the one name that contains the words", () => {
    expect(regionParkFor("epcot", parks)).toMatchObject({ slug: "epcot" });
    expect(regionParkFor("mk", parks)).toMatchObject({
      slug: "magic-kingdom-park",
    });
    expect(regionParkFor("Epic", parks)).toMatchObject({
      slug: "universal-epic-universe",
    });
    expect(regionParkFor("Hollywood Studios", parks)).toMatchObject({
      slug: "disneys-hollywood-studios",
    });
  });

  it("hands back the candidates for a word several parks share, and nothing for a park elsewhere", () => {
    expect(regionParkFor("Universal", parks)).toMatchObject({
      ambiguous: true,
      parks: expect.arrayContaining([
        {
          slug: "universal-studios-florida",
          name: "Universal Studios Florida",
        },
      ]),
    });
    expect(regionParkFor("Phantasialand", parks)).toBeNull();
  });
});

describe("dateRuns", () => {
  it("writes consecutive days as one range", () => {
    expect(
      dateRuns(["2026-12-03", "2026-12-01", "2026-12-02", "2026-12-05"]),
    ).toEqual(["2026-12-01..2026-12-03", "2026-12-05"]);
  });
});

describe("the hub's address", () => {
  it("takes a hubUrl with or without a locale and keeps only the plan's parameters", () => {
    expect(
      hubPath(
        "https://park.fan/de/orlando?from=2027-03-03&to=2027-03-10&parks=epcot:1,magic-kingdom-park:2&hop=0#planner",
      ),
    ).toBe(
      "/orlando?from=2027-03-03&to=2027-03-10&parks=epcot:1,magic-kingdom-park:2&hop=0#planner",
    );
    expect(hubPath("https://park.fan/orlando")).toBe("/orlando");
  });

  it("refuses another host, another page, an unknown parameter or value and another anchor", () => {
    for (const url of [
      "https://evil.example/orlando?from=2027-03-03",
      "https://park.fan/de/orlando/2027/3?from=2027-03-03",
      "https://park.fan/orlando?from=2027-03-03&next=https://evil.example",
      "https://park.fan/orlando?parks=<script>",
      "https://park.fan/orlando?from=2027-03-03#top",
    ]) {
      expect([url, hubPath(url)]).toEqual([url, null]);
    }
  });

  it("puts the chat's language in front of the hub, not of a page that only starts like it", () => {
    expect(
      localizePageUrls(
        {
          hubUrl: "https://park.fan/orlando?from=2027-03-03#planner",
          other: "https://park.fan/orlandox",
        },
        "nl",
      ),
    ).toEqual({
      hubUrl: "https://park.fan/nl/orlando?from=2027-03-03#planner",
      other: "https://park.fan/orlandox",
    });
  });
});

describe("plan_vacation", () => {
  const trip = {
    from: day(10),
    to: day(14),
    parks: [
      { park: "Magic Kingdom" },
      { park: "epcot" },
      { park: "Epic", visits: 1 },
    ],
  };

  it("retells the plan day by day in park time, with the hub and the planner's list", async () => {
    const { call } = setup();
    const { json } = await call("plan_vacation", trip);
    expect(json?.timesIn).toMatch(/^park time, America\/New_York/);
    expect(json?.days.map((d: { kind: string }) => d.kind)).toEqual([
      "arrival",
      "park",
      "park",
      "park",
      "departure",
    ]);
    const parkDays = json?.days.filter(
      (d: { kind: string }) => d.kind === "park",
    );
    for (const d of parkDays) {
      expect(d.weekday).toMatch(/day$/);
      expect(d.parks[0].hours).toMatch(/^09:00–(22|18):00$/);
      expect(d.why.length).toBeGreaterThan(0);
    }
    const epic = parkDays.find(
      (d: { parks: Array<{ park: string }> }) =>
        d.parks[0].park === "Epic Universe",
    );
    expect(epic.parks[0].basis).toBe("last_year");
    expect(epic.watch).toContain("estimate from last year, not a forecast");
    const epcot = parkDays.find(
      (d: { parks: Array<{ park: string }> }) => d.parks[0].park === "EPCOT",
    );
    expect(epcot.parks[0].events).toEqual([
      "EPCOT International Food & Wine Festival (expected)",
    ]);
    expect(json?.hubUrl).toBe(
      `https://park.fan/de/orlando?from=${day(10)}&to=${day(14)}&parks=epcot:1,magic-kingdom-park:1,universal-epic-universe:1&hotel=offsite&blocks=1&fireworks=any&split=auto&rest=auto&hop=0&arrival=free&departure=free#planner`,
    );
    expect(json?.reserve).toHaveLength(3);
    expect(json?.reserve).toContainEqual({
      park: idOf("universal-epic-universe"),
      date: epic.date,
    });
    expect(json?.totals.means).toMatch(/per headliner ride, not per day/);
  });

  it("names a hotel's perk and the travel from its area", async () => {
    const { call } = setup();
    const { json } = await call("plan_vacation", {
      ...trip,
      area: "disney",
    });
    const mk = json?.days.find(
      (d: { parks?: Array<{ park: string }> }) =>
        d.parks?.[0].park === "Magic Kingdom Park",
    );
    expect(mk.parks[0].perks).toEqual(["Early Entry"]);
    expect(json?.travelFrom).toBe("the hotel area Walt Disney World");
    expect(mk.travel).toMatchObject({ mode: "drive", plusMinutes: 15 });
    expect(mk.travel.via).toMatch(/Transportation and Ticket Center/);
    // The perk is named once, in perks, not again among the reasons.
    expect(mk.why.join(" ")).not.toMatch(/Early Entry/);
  });

  it("keeps the longest trip with every visit under the result's size limit", async () => {
    const slugs = [
      "magic-kingdom-park",
      "epcot",
      "disneys-hollywood-studios",
      "disneys-animal-kingdom-theme-park",
      "universal-studios-florida",
      "universal-islands-of-adventure",
      "universal-epic-universe",
    ];
    const { call } = setup((from, to) => wideCalendar(from, to, slugs));
    const { json, content } = await call("plan_vacation", {
      from: day(10),
      to: day(30),
      area: "universal",
      hotel: "universal_premier",
      parks: slugs.map((park, i) => ({ park, visits: i < 2 ? 3 : 2 })),
    });
    expect(json?.days).toHaveLength(21);
    expect(json?.reserve).toHaveLength(16);
    expect(content.length).toBeLessThan(9_000);
  });

  it("refuses a trip over three weeks, a park outside Orlando and a plan the route refuses", async () => {
    const { call } = setup();
    const long = await call("plan_vacation", {
      from: day(10),
      to: day(40),
    });
    expect(long.isError).toBe(true);
    expect(long.content).toMatch(/at most 21 days/);
    const elsewhere = await call("plan_vacation", {
      ...trip,
      parks: [{ park: "phantasialand" }],
    });
    expect(elsewhere.isError).toBe(true);
    expect(elsewhere.content).toMatch(/Orlando parks only/);
    const crowded = await call("plan_vacation", {
      ...trip,
      parks: [
        { park: "epcot", visits: 3 },
        { park: "Magic Kingdom", visits: 3 },
      ],
    });
    expect(crowded.isError).toBe(true);
    expect(crowded.content).toMatch(/More park visits than trip days/);
  });
});

describe("get_park_events", () => {
  it("lists Orlando's seasons with their status and the slots as date ranges", async () => {
    const { call } = setup();
    const { json } = await call("get_park_events", {
      from: day(10),
      to: day(20),
    });
    expect(json?.scope).toBe("Orlando");
    expect(json?.seasons).toContainEqual({
      park: "Magic Kingdom Park",
      name: "Mickey's Very Merry Christmas Party",
      kind: "christmas",
      status: "confirmed",
      runs: `${day(0)}..${day(40)}`,
      onlyOn: [day(12), day(14), day(16)],
      separateTicket: true,
      hours: "19:00–24:00",
    });
    expect(json?.statusMeans.expected).toMatch(/last year's pattern/);
    expect(json?.slots).toContainEqual({
      park: "Magic Kingdom Park",
      slot: "Early Entry (Disney resort hotel guests)",
      minutesBeforeOpening: 30,
      days: [`${day(10)}..${day(14)}`],
    });
    expect(json?.slots).toContainEqual(
      expect.objectContaining({
        park: "Magic Kingdom Park",
        hours: "19:00–24:00",
        dayGuestsLeave: "18:00",
        days: [day(12)],
      }),
    );
  });

  it("gives a park elsewhere its seasons only, a nameless one by its kind", async () => {
    const { call, seasons, internalCalendar } = setup();
    const { json } = await call("get_park_events", { park: "phantasialand" });
    expect(seasons.forParkInRange).toHaveBeenCalled();
    expect(internalCalendar).not.toHaveBeenCalled();
    expect(json?.seasons).toEqual([
      expect.objectContaining({
        park: "Phantasialand",
        name: "Halloween season",
        status: "announced",
      }),
    ]);
    expect(json?.slots).toMatch(/Orlando parks only/);
  });

  it("refuses a window over 92 days", async () => {
    const { call } = setup();
    const result = await call("get_park_events", {
      from: day(1),
      to: day(120),
    });
    expect(result.isError).toBe(true);
  });
});

describe("get_travel_time", () => {
  it("drives to Magic Kingdom's car park and names the transfer and the source", async () => {
    const { call } = setup();
    const { json } = await call("get_travel_time", {
      from: "Epic Universe",
      to: "Magic Kingdom",
    });
    expect(json).toMatchObject({
      from: "Epic Universe",
      to: "Magic Kingdom Park",
      mode: "drive",
      plusMinutes: 15,
      basis: expect.stringMatching(/no traffic/),
      source: expect.stringMatching(/OpenStreetMap/),
    });
    expect(json?.doorToDoorMinutes).toBe(json?.minutes + 15);
  });

  it("walks from a Universal hotel through CityWalk", async () => {
    const { call } = setup();
    const { json } = await call("get_travel_time", {
      area: "universal",
      to: "Islands of Adventure",
    });
    expect(json?.alternatives).toContainEqual({
      mode: "walk",
      minutes: 15,
      via: "through CityWalk",
    });
  });

  it("says it has nothing for a park outside Orlando", async () => {
    const { call } = setup();
    const result = await call("get_travel_time", {
      from: "epcot",
      to: "phantasialand",
    });
    expect(result.isError).toBe(true);
    expect(result.content).toMatch(/Orlando/);
  });
});

describe("the trip in the planner and the hub", () => {
  it("reserves a plan's days across parks once each, in order", async () => {
    const { call } = setup();
    const result = await call("plan_trip", {
      days: [
        { park: idOf("epcot"), date: day(12) },
        { park: idOf("magic-kingdom-park"), date: day(11) },
        { park: idOf("epcot"), date: day(12) },
      ],
    });
    expect(result.action).toEqual({
      type: "planner_reserve_trip",
      days: [
        {
          park: expect.objectContaining({ slug: "magic-kingdom-park" }),
          date: day(11),
        },
        { park: expect.objectContaining({ slug: "epcot" }), date: day(12) },
      ],
    });
    expect((await call("plan_trip", { park: "epcot", days: [] })).isError).toBe(
      true,
    );
  });

  it("opens the hub, empty or with a plan", async () => {
    const { call } = setup();
    expect((await call("open_page", { page: "orlando" })).action).toEqual({
      type: "navigate",
      path: "/orlando",
    });
    expect(
      (
        await call("open_page", {
          url: `https://park.fan/de/orlando?from=${day(10)}&to=${day(14)}#planner`,
        })
      ).action,
    ).toEqual({
      type: "navigate",
      path: `/orlando?from=${day(10)}&to=${day(14)}#planner`,
    });
    expect(
      (await call("open_page", { url: "https://park.fan/orlando?x=1" }))
        .isError,
    ).toBe(true);
  });
});
