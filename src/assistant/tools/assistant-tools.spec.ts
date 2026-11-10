import { DataSource } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { getParkTool } from "../../mcp/tools/get-park.tool";
import { getAttractionTool } from "../../mcp/tools/get-attraction.tool";
import { BestDaysService } from "../../parks/services/best-days.service";
import { AssistantPostsClient } from "./assistant-posts.client";

describe("get_post", () => {
  const providers = (post: jest.Mock) =>
    new Map<unknown, unknown>([[AssistantPostsClient, { post }]]);

  it("passes the question on for the matching sections", async () => {
    const post = jest.fn(async () => ({ text: "…", sections: ["Tickets"] }));
    await run(
      "get_post",
      { slug: "europa-park-guide", question: "fast pass worth it" },
      providers(post),
      "de",
    );
    expect(post).toHaveBeenCalledWith(
      "de",
      "europa-park-guide",
      "fast pass worth it",
    );
  });

  it("still takes a slug alone", async () => {
    const post = jest.fn(async () => ({ text: "…" }));
    await run("get_post", { slug: "europa-park-guide" }, providers(post));
    expect(post).toHaveBeenCalledWith("en", "europa-park-guide", undefined);
  });
});

describe("get_best_days", () => {
  it("ranks only rated open days, an unknown one never the quietest", async () => {
    const day = (date: string, crowdLevel: string) => ({
      date,
      crowdLevel,
      isHoliday: false,
      isSchoolVacation: false,
    });
    const providers = new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({
            id: "11111111-1111-4111-8111-111111111111",
            name: "Phantasialand",
          })),
        },
      ],
      [
        BestDaysService,
        {
          getBestDays: jest.fn(async () => ({
            meta: { windowFrom: "2026-11-05", windowTo: "2026-11-10" },
            days: [
              day("2026-11-05", "unknown"),
              day("2026-11-06", "high"),
              day("2026-11-07", "closed"),
              day("2026-11-08", "low"),
            ],
          })),
        },
      ],
    ]);
    const result = await runAssistantTool(
      "get_best_days",
      { park: "11111111-1111-4111-8111-111111111111" },
      { get: <T>(token: unknown) => providers.get(token) as T, locale: "en" },
      jest.fn(),
    );
    const json = JSON.parse(result.content);
    expect(json.openDays).toBe(3);
    expect(json.unratedOpenDays).toBe(1);
    expect(
      json.tenQuietestOpenDays.map((d: { date: string }) => d.date),
    ).toEqual(["2026-11-08", "2026-11-06"]);
  });
});
import {
  foldText,
  lastYearDays,
  rideBadges,
  runAssistantTool,
  showsCard,
  showsOnDay,
  reopeningOf,
  reopenSearchFrom,
  storedToolInput,
  CHAT_END_COOLDOWN_MINUTES,
} from "./assistant-tools";
import { PlanDayService } from "../../parks/services/plan-day.service";
import { CalendarService } from "../../parks/services/calendar.service";
import { FavoritesService } from "../../favorites/favorites.service";
import { AssistantRateLimitService } from "../assistant-rate-limit.service";
import { ShowsService } from "../../shows/shows.service";
import { formatInTimeZone } from "date-fns-tz";
import type { Show } from "../../shows/entities/show.entity";
import type { ShowLiveData } from "../../shows/entities/show-live-data.entity";

describe("the same day last year", () => {
  it("keeps the date and finds the nearest day of the same weekday", () => {
    // Saturday 2026-11-07: a year back the 7th is a Friday, the Saturday is the 8th.
    expect(lastYearDays("2026-11-07")).toEqual({
      sameDate: "2025-11-07",
      sameWeekday: "2025-11-08",
    });
    // Halloween 2026 is a Saturday; 2025-10-31 a Friday, so Saturday 2025-11-01.
    expect(lastYearDays("2026-10-31").sameWeekday).toBe("2025-11-01");
    // Across a leap day: Monday 2029-03-05 → Monday 2028-03-06 (the 5th is a Sunday).
    expect(lastYearDays("2029-03-05")).toEqual({
      sameDate: "2028-03-05",
      sameWeekday: "2028-03-06",
    });
    expect(lastYearDays("2028-02-29").sameDate).toBe("2027-02-28");
    for (const date of ["2026-01-01", "2026-06-15", "2027-12-31"]) {
      const { sameDate, sameWeekday } = lastYearDays(date);
      const dow = (iso: string) => new Date(`${iso}T00:00:00Z`).getUTCDay();
      expect(dow(sameWeekday)).toBe(dow(date));
      const off = Math.abs(Date.parse(sameWeekday) - Date.parse(sameDate));
      expect(off).toBeLessThanOrEqual(3 * 86_400_000);
    }
  });

  it("answers with the calendar's day, and null for a day it has nothing on", async () => {
    const calendar = {
      buildCalendarResponse: jest.fn(async () => ({
        days: [
          {
            date: "2025-11-08",
            status: "OPERATING",
            crowdLevel: "high",
            isHoliday: false,
            isBridgeDay: false,
            isSchoolVacation: true,
            headlinerForecast: { avgWait: 55, rides: [], actual: true },
          },
        ],
      })),
    };
    const providers = new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({
            ...park,
            timezone: "Europe/Berlin",
          })),
        },
      ],
      [CalendarService, calendar],
    ]);
    const { json } = await run(
      "get_park_history",
      { park: PARK_ID, date: "2026-11-07" },
      providers,
    );
    expect(json.sameDateLastYear).toBeNull();
    expect(json.sameWeekdayLastYear).toMatchObject({
      date: "2025-11-08",
      weekday: "Saturday",
      crowdLevel: "high",
      headlinerPeakWaitMinutes: 55,
      schoolVacation: true,
    });
  });

  it("reads a past day's level unknown where nothing was measured, not the stored forecast", async () => {
    const providers = new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({
            ...park,
            timezone: "Europe/Berlin",
          })),
        },
      ],
      [
        CalendarService,
        {
          buildCalendarResponse: jest.fn(async () => ({
            days: [
              {
                date: "2025-11-07",
                status: "OPERATING",
                crowdLevel: "low",
                isHoliday: false,
                isBridgeDay: false,
                isSchoolVacation: false,
                headlinerForecast: { avgWait: 30, rides: [], actual: false },
              },
            ],
          })),
        },
      ],
    ]);
    const { json } = await run(
      "get_park_history",
      { park: PARK_ID, date: "2026-11-07" },
      providers,
    );
    expect(json.sameDateLastYear).toMatchObject({
      crowdLevel: "unknown",
      headlinerPeakWaitMinutes: null,
    });
  });

  it("reads one past day itself with on, and refuses today or a day ahead", async () => {
    const daysAgo = (days: number) =>
      new Date(Date.now() - days * 86_400_000).toISOString().slice(0, 10);
    const ON = daysAgo(4);
    const providers = new Map<unknown, unknown>([
      [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
      [
        CalendarService,
        {
          buildCalendarResponse: jest.fn(async () => ({
            days: [
              {
                date: ON,
                status: "OPERATING",
                crowdLevel: "moderate",
                headlinerForecast: { avgWait: 40, rides: [], actual: true },
              },
            ],
          })),
        },
      ],
    ]);
    const { json } = await run(
      "get_park_history",
      { park: PARK_ID, on: ON },
      providers,
    );
    expect(json.day).toMatchObject({
      date: ON,
      crowdLevel: "moderate",
      headlinerPeakWaitMinutes: 40,
    });
    for (const on of [daysAgo(-3), daysAgo(400)]) {
      const result = await runAssistantTool(
        "get_park_history",
        { park: PARK_ID, on },
        context(providers),
        jest.fn(),
      );
      expect(result.isError).toBe(true);
    }
  });
});

describe("get_shows", () => {
  it("lists the shows with a time that day, earliest first, status only from a reading", () => {
    const show = (id: string, name: string, extra = {}) =>
      ({ id, name, isSeasonal: false, seasonMonths: null, ...extra }) as Show;
    const result = showsOnDay(
      [
        show("a", "Night Parade", { landName: "Main Street" }),
        show("b", "Stunt Show"),
        show("c", "Meet & Greet"),
      ],
      new Map([
        [
          "b",
          {
            status: "OPERATING",
            showtimes: [
              {
                type: "Performance Time",
                startTime: "2026-11-05T10:00:00Z",
                endTime: "2026-11-05T10:25:00Z",
              },
            ],
          } as unknown as ShowLiveData,
        ],
      ]),
      new Map([
        ["a", ["2026-11-05T18:00:00.000Z"]],
        ["b", ["2026-11-05T10:00:00.000Z", "2026-11-05T14:00:00.000Z"]],
      ]),
      "Europe/Berlin",
    );
    expect(result).toEqual({
      shows: [
        {
          id: "b",
          name: "Stunt Show",
          status: "OPERATING",
          showtimes: ["2026-11-05T10:00:00.000Z", "2026-11-05T14:00:00.000Z"],
          durationMinutes: 25,
        },
        {
          id: "a",
          name: "Night Parade",
          land: "Main Street",
          status: null,
          showtimes: ["2026-11-05T18:00:00.000Z"],
        },
      ],
      withShowtimes: 2,
      withoutShowtimes: 1,
      // Named, so a show without a time today is "no time published", not "none".
      withoutShowtimesNames: ["Meet & Greet"],
    });
  });
});
import {
  calendarCard,
  localizePageUrls,
  parkInstant,
  tilesCard,
  type ForecastSummary,
} from "./assistant-actions";
import { getFrontendUrl } from "../../config/oauth.config";

describe("page urls in the chat's language", () => {
  it("prefixes the locale to park and ride pages only, once", () => {
    const site = getFrontendUrl();
    const result = localizePageUrls(
      {
        pageUrl: `${site}/parks/europe/germany/bruehl/phantasialand/taron`,
        rides: [{ url: `${site}/parks/europe/germany/bruehl/phantasialand` }],
        post: `${site}/fr/news/x`,
        other: "https://evil.example/parks/x",
      },
      "fr",
    );
    expect(result).toEqual({
      pageUrl: `${site}/fr/parks/europe/germany/bruehl/phantasialand/taron`,
      rides: [{ url: `${site}/fr/parks/europe/germany/bruehl/phantasialand` }],
      post: `${site}/fr/news/x`,
      other: "https://evil.example/parks/x",
    });
    expect(localizePageUrls(`${site}/parks/x`, "pt")).toBe(`${site}/parks/x`);
  });

  it("turns the API paths find_parks_nearby hands out into page urls", () => {
    const site = getFrontendUrl();
    expect(
      localizePageUrls(
        { url: "/v1/parks/europe/germany/bruehl/phantasialand/taron" },
        "de",
      ),
    ).toEqual({
      url: `${site}/de/parks/europe/germany/bruehl/phantasialand/taron`,
    });
  });
});

const PARK_ID = "11111111-1111-4111-8111-111111111111";
const park = {
  id: PARK_ID,
  name: "Phantasialand",
  slug: "phantasialand",
  timezone: "Europe/Berlin",
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
};

function context(providers: Map<unknown, unknown>, locale = "en") {
  return { get: <T>(token: unknown) => providers.get(token) as T, locale };
}

async function run(
  name: string,
  input: unknown,
  providers: Map<unknown, unknown>,
  locale = "en",
) {
  const onError = jest.fn();
  const result = await runAssistantTool(
    name,
    input,
    context(providers, locale),
    onError,
  );
  expect(onError).not.toHaveBeenCalled();
  return { ...result, json: JSON.parse(result.content) as Record<string, any> };
}

const profiles = (types: Record<string, string[]>) => ({
  findByAttractions: jest.fn(async (ids: string[]) => {
    return new Map(
      ids
        .filter((id) => types[id])
        .map((id) => [
          id,
          { attractionId: id, types: types[id], elements: [] },
        ]),
    );
  }),
});

describe("find_restaurants", () => {
  const restaurants = [
    {
      id: "r1",
      name: "Eiscafé Rutmors",
      lastUpdated: "x",
      status: "OPERATING",
    },
    {
      id: "r2",
      name: "Wok",
      cuisineType: "Asian",
      requiresReservation: null,
      status: "OPERATING",
    },
    {
      id: "r3",
      name: "Ice Cream Stand",
      requiresReservation: true,
      status: "OPERATING",
    },
  ];
  const providers = new Map<unknown, unknown>([
    [
      ParksService,
      {
        findById: jest.fn(async () => ({ ...park })),
        loadParkRelations: jest.fn(async (p: object) => ({
          ...p,
          restaurants: [
            { id: "r1", landName: "Rookburgh" },
            { id: "r2", landName: null },
          ],
        })),
      },
    ],
    [
      ParkIntegrationService,
      {
        buildIntegratedResponse: jest.fn(async () => ({
          status: "OPERATING",
          restaurants,
        })),
      },
    ],
  ]);

  it("lists the park's food, filtered by name, with only the facts park.fan knows", async () => {
    const { json } = await run(
      "find_restaurants",
      { park: PARK_ID, query: "EIS" },
      providers,
    );
    expect(json).toMatchObject({ park: "Phantasialand", total: 3, matched: 1 });
    expect(json.restaurants).toEqual([
      { name: "Eiscafé Rutmors", status: "OPERATING", land: "Rookburgh" },
    ]);
    const all = await run("find_restaurants", { park: PARK_ID }, providers);
    // No reading of its own: the payload's status is a guess from the park's, not passed on.
    expect(all.json.restaurants[1]).toEqual({
      name: "Wok",
      status: null,
      cuisine: "Asian",
    });
    expect(all.json.restaurants[2]).toEqual({
      name: "Ice Cream Stand",
      status: null,
      requiresReservation: true,
    });
  });

  it("links the park page in the chat's language", async () => {
    const { json } = await run(
      "find_restaurants",
      { park: PARK_ID },
      providers,
      "nl",
    );
    expect(json.pageUrl).toBe(
      `${getFrontendUrl()}/nl/parks/europe/germany/bruehl/phantasialand`,
    );
  });

  it("draws them as a card with nulls for what nobody said", async () => {
    const { action } = await run(
      "show_card",
      { kind: "restaurants", park: PARK_ID, query: "ice" },
      providers,
    );
    expect(action).toEqual({
      type: "card",
      card: {
        kind: "restaurants",
        park: "Phantasialand",
        url: "/parks/europe/germany/bruehl/phantasialand",
        restaurants: [
          {
            name: "Ice Cream Stand",
            status: null,
            land: null,
            cuisine: null,
            requiresReservation: true,
          },
        ],
      },
    });
  });

  it("folds case and accents for the query", () => {
    expect(foldText("Crème Glacée")).toBe("creme glacee");
  });
});

describe("compare_rides across a country", () => {
  const where: unknown[] = [];
  const builder = {
    innerJoinAndSelect: () => builder,
    where: () => builder,
    andWhere: (_sql: string, params?: unknown) => {
      if (params) where.push(params);
      return builder;
    },
    getMany: jest.fn(async () => [
      {
        id: "a",
        parkId: "p1",
        name: "Taron",
        slug: "taron",
        park: { name: "Phantasialand", country: "Germany" },
      },
      {
        id: "b",
        parkId: "p2",
        name: "Blue Fire",
        slug: "blue-fire",
        park: { name: "Europa-Park", country: "Germany" },
      },
      {
        id: "c",
        parkId: "p2",
        name: "Carousel",
        slug: "carousel",
        park: { name: "Europa-Park", country: "Germany" },
      },
      {
        id: "d",
        parkId: "p2",
        name: "Mystery",
        slug: "mystery",
        park: { name: "Europa-Park", country: "Germany" },
      },
    ]),
  };
  const providers = new Map<unknown, unknown>([
    [
      DataSource,
      { getRepository: () => ({ createQueryBuilder: () => builder }) },
    ],
    [
      RideProfileService,
      {
        findByAttractions: jest.fn(
          async () =>
            new Map([
              [
                "a",
                {
                  attractionId: "a",
                  types: ["launch-coaster"],
                  elements: [],
                  stats: { topSpeedKmh: 117 },
                },
              ],
              [
                "b",
                {
                  attractionId: "b",
                  types: ["launch-coaster"],
                  elements: [],
                  stats: { topSpeedKmh: 100 },
                },
              ],
              [
                "c",
                {
                  attractionId: "c",
                  types: ["carousel"],
                  elements: [],
                  stats: { topSpeedKmh: 10 },
                },
              ],
            ]),
        ),
      },
    ],
  ]);

  it("ranks the country's coasters in one query, counting the rides of unknown type", async () => {
    const { json } = await run(
      "compare_rides",
      { country: "deutschland", by: "topSpeed", type: "coaster" },
      providers,
    );
    expect(where).toContainEqual({ code: "DE", country: "deutschland" });
    expect(builder.getMany).toHaveBeenCalledTimes(1);
    expect(json).toMatchObject({
      country: "Germany",
      parks: 2,
      type: "coaster",
      ranked: 2,
      typeUnknown: 1,
    });
    expect(
      json.rides.map(
        (r: { name: string; park: string }) => `${r.name} (${r.park})`,
      ),
    ).toEqual(["Taron (Phantasialand)", "Blue Fire (Europa-Park)"]);
  });

  it("takes a park or a country, not both and not neither", async () => {
    for (const input of [
      { by: "topSpeed" },
      { park: PARK_ID, country: "DE", by: "topSpeed" },
    ]) {
      const result = await runAssistantTool(
        "compare_rides",
        input,
        context(providers),
        jest.fn(),
      );
      expect(result).toEqual({
        content: "Give either park or country",
        isError: true,
      });
    }
  });
});

describe("get_park_live › funFacts", () => {
  afterEach(() => jest.restoreAllMocks());

  const withFacts = (funFacts: unknown) =>
    jest
      .spyOn(getParkTool, "handler")
      .mockResolvedValue({ name: "Efteling", funFacts });

  it("hands the model the chat language's list", async () => {
    withFacts({ en: ["English fact"], nl: ["Nederlands feitje"] });
    const { json } = await run(
      "get_park_live",
      { park: PARK_ID },
      new Map(),
      "nl",
    );
    expect(json.funFacts).toEqual(["Nederlands feitje"]);
  });

  it("falls back to English when that language has none", async () => {
    withFacts({ en: ["English fact"] });
    const { json } = await run(
      "get_park_live",
      { park: PARK_ID },
      new Map(),
      "de",
    );
    expect(json.funFacts).toEqual(["English fact"]);
  });

  it("leaves the key out when the park has none", async () => {
    withFacts(null);
    const { json } = await run(
      "get_park_live",
      { park: PARK_ID },
      new Map(),
      "de",
    );
    expect("funFacts" in json).toBe(false);
  });
});

describe("a closed park's crowd", () => {
  afterEach(() => jest.restoreAllMocks());

  const crowd = {
    level: "very_low",
    avgWaitMinutes: 0,
    avgWaitTodayMinutes: 24,
    peakWaitTodayMinutes: 55,
    peakHourLocal: 14,
    occupancyPercent: 3,
    comparedToTypicalPercent: -90,
  };
  const summary = (status: string) =>
    jest.spyOn(getParkTool, "handler").mockResolvedValue({
      name: "Six Flags Fiesta Texas",
      status,
      crowd,
      scheduleToday: [],
      nextSchedule: { date: "2026-11-07", opens: "2026-11-07T12:00:00-06:00" },
    });

  // Once read out as "very low for the moment" for a park nobody could enter.
  it("keeps only the day so far once the park has closed", async () => {
    summary("CLOSED");
    const { json } = await run("get_park_live", { park: PARK_ID }, new Map());
    expect(json.crowd).toEqual({
      avgWaitTodayMinutes: 24,
      peakWaitTodayMinutes: 55,
      peakHourLocal: 14,
      level: null,
      levelWhy: expect.stringContaining("closed"),
    });
    expect(json.nextSchedule).toBeDefined();
  });

  it("keeps the whole reading while it runs", async () => {
    summary("OPERATING");
    const { json } = await run("get_park_live", { park: PARK_ID }, new Map());
    expect(json.crowd).toEqual(crowd);
  });

  it("draws the park card of a closed park without a level", async () => {
    summary("CLOSED");
    const result = await run(
      "show_card",
      { kind: "park", park: PARK_ID },
      new Map(),
    );
    expect(result.action).toMatchObject({
      type: "card",
      card: { kind: "park", status: "CLOSED", crowdLevel: null },
    });
  });
});

describe("show_card kind rides with a type", () => {
  afterEach(() => jest.restoreAllMocks());

  it("lists only the coasters, from every waiting ride", async () => {
    const handler = jest.spyOn(getParkTool, "handler").mockResolvedValue({
      name: "Phantasialand",
      pageUrl: "https://park.fan/parks/europe/germany/bruehl/phantasialand",
      rides: {
        longestWaits: [
          { id: "a", name: "Taron", waitMinutes: 45 },
          { id: "b", name: "River Quest", waitMinutes: 30 },
          { id: "c", name: "Raik", waitMinutes: 5 },
        ],
        down: [
          { id: "d", name: "Black Mamba" },
          { id: "e", name: "Talocan" },
        ],
      },
    });
    const providers = new Map<unknown, unknown>([
      [
        RideProfileService,
        profiles({
          a: ["launch-coaster"],
          b: ["rapids"],
          c: ["boomerang"],
          d: ["inverted-coaster"],
          e: ["top-spin"],
        }),
      ],
      [
        DataSource,
        { getRepository: () => ({ find: jest.fn(async () => []) }) },
      ],
    ]);
    const { action, json } = await run(
      "show_card",
      { kind: "rides", park: PARK_ID, type: "coaster", order: "shortest" },
      providers,
    );
    expect(
      (handler.mock.calls[0][0] as { longestWaits: number }).longestWaits,
    ).toBeGreaterThan(30);
    expect(action).toEqual({
      type: "card",
      card: expect.objectContaining({
        kind: "rides",
        rides: [
          { name: "Raik", status: "OPERATING", waitMinutes: 5, url: null },
          { name: "Taron", status: "OPERATING", waitMinutes: 45, url: null },
          // Down rides follow, under the same type filter.
          { name: "Black Mamba", status: "DOWN", waitMinutes: null, url: null },
        ],
      }),
    });
    expect(json.shown).toBe("3 coasters of Phantasialand");
    // The model reads the rows, with their types, so it can name its pick after the card.
    expect(json.rows).toEqual([
      {
        name: "Raik",
        status: "OPERATING",
        waitMinutes: 5,
        types: ["boomerang"],
      },
      {
        name: "Taron",
        status: "OPERATING",
        waitMinutes: 45,
        types: ["launch-coaster"],
      },
      {
        name: "Black Mamba",
        status: "DOWN",
        waitMinutes: null,
        types: ["inverted-coaster"],
      },
    ]);
    expect(json.typeUnknown).toBe(0);

    // A dark ride with no wait to speak of is found though it never makes the longest-wait list.
    const dark = await run(
      "show_card",
      { kind: "rides", park: PARK_ID, type: "dark_ride" },
      new Map(providers).set(
        RideProfileService,
        profiles({ a: ["launch-coaster"], c: ["dark-ride", "animatronics"] }),
      ),
    );
    expect(dark.json.shown).toBe("1 dark rides of Phantasialand");
    expect(dark.json.rows).toEqual([
      {
        name: "Raik",
        status: "OPERATING",
        waitMinutes: 5,
        types: ["dark-ride", "animatronics"],
      },
    ]);
    // River Quest, Black Mamba and Talocan have no profile: left out, and counted.
    expect(dark.json.typeUnknown).toBe(3);

    // A full "shortest" list leaves no room for rides that are down.
    const full = await run(
      "show_card",
      {
        kind: "rides",
        park: PARK_ID,
        type: "coaster",
        order: "shortest",
        limit: 2,
      },
      providers,
    );
    expect(
      (full.action as { card: { rides: unknown[] } }).card.rides,
    ).toHaveLength(2);
  });
});

describe("rides the visitor marked as ridden", () => {
  afterEach(() => jest.restoreAllMocks());

  it("are marked in the rows of a rides card, the others not", async () => {
    jest.spyOn(getParkTool, "handler").mockResolvedValue({
      name: "Phantasialand",
      status: "OPERATING",
      rides: {
        longestWaits: [
          { id: "a", name: "Taron", waitMinutes: 45 },
          { id: "c", name: "Raik", waitMinutes: 5 },
        ],
        down: [],
      },
    });
    const providers = new Map<unknown, unknown>([
      [RideProfileService, profiles({})],
      [
        DataSource,
        { getRepository: () => ({ find: jest.fn(async () => []) }) },
      ],
    ]);
    const result = await runAssistantTool(
      "show_card",
      { kind: "rides", park: PARK_ID },
      { ...context(providers), ridden: new Set(["a"]) },
      jest.fn(),
    );
    expect(JSON.parse(result.content).rows).toEqual([
      { name: "Taron", status: "OPERATING", waitMinutes: 45, ridden: true },
      { name: "Raik", status: "OPERATING", waitMinutes: 5 },
    ]);
  });
});

describe("show_card kind rides for the rain", () => {
  afterEach(() => jest.restoreAllMocks());

  it("lists the rides that keep you dry, the unknown ones counted apart", async () => {
    jest.spyOn(getParkTool, "handler").mockResolvedValue({
      name: "Phantasialand",
      status: "OPERATING",
      rides: {
        longestWaits: [
          { id: "a", name: "Taron", waitMinutes: 45 },
          { id: "b", name: "Winja's Fear", waitMinutes: 30 },
          { id: "c", name: "Maus au Chocolat", waitMinutes: 20 },
        ],
        down: [{ id: "d", name: "Talocan" }],
      },
    });
    const dryness: Record<string, string | null> = {
      a: "covered_queue",
      b: "indoor",
      c: "indoor",
      d: null,
    };
    const find = jest.fn(async ({ where }: { where: { id: unknown } }) => {
      const ids = (where.id as { value: string[] }).value;
      return ids.map((id) => ({ id, indoorOutdoor: dryness[id] }));
    });
    const providers = new Map<unknown, unknown>([
      [RideProfileService, profiles({})],
      [DataSource, { getRepository: () => ({ find }) }],
    ]);
    const covered = await run(
      "show_card",
      { kind: "rides", park: PARK_ID, shelter: "covered" },
      providers,
    );
    expect(covered.json.rows.map((row: { name: string }) => row.name)).toEqual([
      "Taron",
      "Winja's Fear",
      "Maus au Chocolat",
    ]);
    // The model can tell a roofed queue from a ride that is dry the whole way.
    expect(covered.json.rows[0].indoorOutdoor).toBe("covered_queue");
    expect(covered.json.shelterUnknown).toBe(1);
    expect(covered.json).not.toHaveProperty("shelterIncomplete");

    const indoor = await run(
      "show_card",
      { kind: "rides", park: PARK_ID, shelter: "indoor" },
      providers,
    );
    expect(indoor.json.shown).toBe("2 indoor rides of Phantasialand");
  });
});

describe("compare_parks", () => {
  afterEach(() => jest.restoreAllMocks());

  const dayAhead = (days: number) =>
    new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
  const DATE = dayAhead(3);
  const parkRow = (id: string, name: string) => ({
    ...park,
    id,
    name,
    slug: name.toLowerCase().replace(/ /g, "-"),
  });
  const PHL = parkRow("11111111-1111-4111-8111-111111111111", "Phantasialand");
  const EP = parkRow("22222222-2222-4222-8222-222222222222", "Europa-Park");
  const MP = parkRow("33333333-3333-4333-8333-333333333333", "Movie Park");
  const HP = parkRow("44444444-4444-4444-8444-444444444444", "Hansa-Park");
  const LL = parkRow("55555555-5555-4555-8555-555555555555", "Legoland");
  const days: Record<string, unknown> = {
    [PHL.id]: {
      status: "OPERATING",
      crowdLevel: "high",
      headlinerForecast: { avgWait: 60 },
    },
    [EP.id]: {
      status: "OPERATING",
      crowdLevel: "low",
      headlinerForecast: { avgWait: 40 },
    },
    [MP.id]: {
      status: "OPERATING",
      crowdLevel: "low",
      headlinerForecast: { avgWait: 25 },
    },
    [HP.id]: { status: "CLOSED", crowdLevel: "closed" },
    [LL.id]: { status: "OPERATING", crowdLevel: "unknown" },
  };
  const all = [PHL, EP, MP, HP, LL];
  const providers = (rows = all) =>
    new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async (id: string) =>
            rows.find((row) => row.id === id),
          ),
          getFirstOperatingDayFrom: jest.fn(async () => ({
            date: dayAhead(20),
          })),
        },
      ],
      [
        DataSource,
        {
          getRepository: () => ({
            createQueryBuilder: () => {
              const query = {
                where: () => query,
                andWhere: () => query,
                getMany: async () => rows,
              };
              return query;
            },
          }),
        },
      ],
      [
        CalendarService,
        {
          buildCalendarResponse: jest.fn(async (p: { id: string }) => ({
            meta: {
              scheduleCoverage: { from: "2025-12-26", to: dayAhead(100) },
              hasOperatingSchedule: p.id !== LL.id,
            },
            days: [{ date: DATE, ...(days[p.id] as object) }],
          })),
        },
      ],
    ]);

  it("ranks a country's parks quietest first, closed and unrated apart", async () => {
    const { json } = await run(
      "compare_parks",
      { country: "Germany", date: DATE },
      providers(),
    );
    // Two "low" parks: the one with the shorter expected peak first.
    expect(json.ranked.map((row: { name: string }) => row.name)).toEqual([
      "Movie Park",
      "Europa-Park",
      "Phantasialand",
    ]);
    expect(json.ranked[0]).toMatchObject({
      id: MP.id,
      crowdLevel: "low",
      expectedPeakWaitMinutes: 25,
    });
    // A park that publishes no hours is listed with the reason, never ranked or called closed.
    expect(json.hoursNotPublished).toEqual([
      expect.objectContaining({
        name: "Legoland",
        why: expect.stringContaining("publishes no opening hours"),
      }),
    ]);
    // A closed park says when it opens next.
    expect(json.closed).toEqual([
      { name: "Hansa-Park", nextOpenDay: dayAhead(20) },
    ]);
  });

  it("keeps an open park without a level out of the ranking, with the reason", async () => {
    const saved = days[MP.id];
    days[MP.id] = { status: "OPERATING", crowdLevel: "unknown" };
    try {
      const { json } = await run(
        "compare_parks",
        { parks: [MP.id, EP.id], date: DATE },
        providers(),
      );
      expect(json.ranked.map((row: { name: string }) => row.name)).toEqual([
        "Europa-Park",
      ]);
      expect(json.unrated).toEqual([
        expect.objectContaining({
          name: "Movie Park",
          why: expect.any(String),
        }),
      ]);
    } finally {
      days[MP.id] = saved;
    }
  });

  it("compares the parks it is given and refuses a day past the forecast", async () => {
    const { json } = await run(
      "compare_parks",
      { parks: [PHL.id, EP.id], date: DATE },
      providers(),
    );
    expect(json.ranked.map((row: { name: string }) => row.name)).toEqual([
      "Europa-Park",
      "Phantasialand",
    ]);
    const far = await run(
      "compare_parks",
      { parks: [PHL.id, EP.id], date: dayAhead(400) },
      providers(),
    );
    expect(far.json.ranked).toEqual([]);
    expect(far.json.notKnown).toEqual([
      { name: "Phantasialand", why: expect.stringContaining("looks ahead") },
      { name: "Europa-Park", why: expect.stringContaining("looks ahead") },
    ]);
  });

  it("asks for a city when a country has too many parks", async () => {
    const many = Array.from({ length: 31 }, (_, i) =>
      parkRow(
        `66666666-6666-4666-8666-${String(i).padStart(12, "0")}`,
        `P${i}`,
      ),
    );
    const result = await runAssistantTool(
      "compare_parks",
      { country: "US" },
      context(providers(many)),
      jest.fn(),
    );
    expect(result).toMatchObject({ isError: true });
    expect(result.content).toContain("name a city");
  });
});

describe("compare_dates", () => {
  afterEach(() => jest.restoreAllMocks());

  const dayAhead = (days: number) =>
    new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
  const OPEN_QUIET = dayAhead(3);
  const OPEN_BUSY = dayAhead(4);
  const CLOSED = dayAhead(5);
  const LATER = dayAhead(200);
  const published = dayAhead(100);
  const calendarDay = (date: string, extra: object) => ({
    date,
    isHoliday: false,
    isBridgeDay: false,
    isSchoolVacation: false,
    ...extra,
  });
  const providers = () => {
    const build = jest.fn(async () => ({
      meta: {
        scheduleCoverage: { from: "2025-12-26", to: published },
        hasOperatingSchedule: true,
      },
      days: [
        calendarDay(OPEN_QUIET, { status: "OPERATING", crowdLevel: "low" }),
        calendarDay(OPEN_BUSY, { status: "OPERATING", crowdLevel: "high" }),
        calendarDay(CLOSED, { status: "CLOSED", crowdLevel: "closed" }),
        calendarDay(LATER, { status: "CLOSED", crowdLevel: "closed" }),
      ],
    }));
    return {
      build,
      providers: new Map<unknown, unknown>([
        [
          ParksService,
          {
            findById: jest.fn(async () => ({ ...park })),
            getFirstOperatingDayFrom: jest.fn(async () => ({
              date: dayAhead(7),
            })),
          },
        ],
        [CalendarService, { buildCalendarResponse: build }],
      ]),
    };
  };

  it("puts the days side by side, a closed one with the next day the park opens", async () => {
    const { providers: p } = providers();
    const { json } = await run(
      "compare_dates",
      { park: PARK_ID, dates: [CLOSED, OPEN_BUSY, OPEN_QUIET] },
      p,
    );
    expect(json.days.map((d: { date: string }) => d.date)).toEqual([
      OPEN_QUIET,
      OPEN_BUSY,
      CLOSED,
    ]);
    expect(json.days[2]).toMatchObject({
      status: "CLOSED",
      hoursPublished: true,
      nextOpenDay: dayAhead(7),
    });
    expect(json.days[2].crowdLevel).toBeUndefined();
    expect(json.quietestOpenDay).toBe(OPEN_QUIET);
  });

  it("calls a day past the published hours unknown and says why, never closed", async () => {
    const { providers: p } = providers();
    const { json } = await run(
      "compare_dates",
      { park: PARK_ID, dates: [LATER] },
      p,
    );
    expect(json.days[0]).toMatchObject({
      status: "UNKNOWN",
      hoursPublished: false,
      estimatedStatus: "CLOSED",
    });
    expect(json.days[0].why).toContain(published);
  });

  it("reads each month once, not the months between", async () => {
    const { providers: p, build } = providers();
    await run(
      "compare_dates",
      { park: PARK_ID, dates: [OPEN_QUIET, LATER] },
      p,
    );
    const months = new Set([OPEN_QUIET, LATER].map((date) => date.slice(0, 7)));
    expect(build).toHaveBeenCalledTimes(months.size);
  });

  it("takes a stretch of days, and refuses a past day or one past the forecast", async () => {
    const { providers: p } = providers();
    const { json } = await run(
      "compare_dates",
      { park: PARK_ID, from: OPEN_QUIET, to: CLOSED },
      p,
    );
    expect(json.days).toHaveLength(3);
    for (const input of [
      { dates: [dayAhead(-3)] },
      { dates: [dayAhead(400)] },
      { from: OPEN_QUIET, to: dayAhead(60) },
      { dates: [OPEN_QUIET], from: OPEN_QUIET, to: CLOSED },
    ]) {
      const result = await runAssistantTool(
        "compare_dates",
        { park: PARK_ID, ...input },
        context(p),
        jest.fn(),
      );
      expect(result.isError).toBe(true);
    }
  });
});

describe("ride card badges", () => {
  it("sends each badge only when true or known", () => {
    expect(
      rideBadges({
        name: "Taron",
        facts: {
          hasSingleRider: true,
          hasVirtualLine: false,
          fastPass: { name: "QuickPass", price: 12 },
          isCurrentlyInSeason: null,
        },
        worksPeriod: { from: "2026-11-01", runningToday: true },
        ropeDrop: { worth: true, strength: "high", savingsMinutes: 40 },
      }),
    ).toEqual({
      ropeDrop: { strength: "high", savings: 40 },
      singleRider: true,
      paidExpress: true,
      worksPeriod: true,
    });
    expect(
      rideBadges({
        name: "Silver Star",
        facts: {
          fastPass: { name: "Virtual Line", price: 0 },
          isCurrentlyInSeason: false,
        },
        worksPeriod: { from: "2027-01-10", runningToday: false },
        ropeDrop: { worth: false, strength: null, savingsMinutes: 10 },
      }),
    ).toEqual({ outOfSeason: true });
  });
});

describe("shows card", () => {
  it("orders by the next start, max ten, times with the park's offset", () => {
    const now = Date.parse("2026-11-05T12:00:00Z");
    const card = showsCard(
      {
        name: "Phantasialand",
        timezone: "Europe/Berlin",
        pageUrl: `${getFrontendUrl()}/parks/europe/germany/bruehl/phantasialand`,
      },
      "2026-11-05",
      [
        {
          name: "Morning",
          status: null,
          showtimes: ["2026-11-05T09:00:00.000Z"],
        },
        {
          name: "Twice",
          status: "OPERATING",
          showtimes: ["2026-11-05T10:00:00.000Z", "2026-11-05T15:00:00.000Z"],
        },
        { name: "Soon", status: null, showtimes: ["2026-11-05T13:00:00.000Z"] },
      ],
      now,
    );
    expect(card.shows.map((show) => show.name)).toEqual([
      "Soon",
      "Twice",
      "Morning",
    ]);
    expect(card.shows[1].times).toEqual([
      "2026-11-05T11:00:00+01:00",
      "2026-11-05T16:00:00+01:00",
    ]);
    expect(card.url).toBe("/parks/europe/germany/bruehl/phantasialand");
  });
});

describe("display tools", () => {
  it("tell the model its text is shown already, so it does not write it twice", async () => {
    const { json } = await run(
      "open_page",
      { url: "https://park.fan/de/news/taron-neu" },
      new Map(),
    );
    expect(json.opened).toBe(true);
    expect(json.alreadyShown).toMatch(/do not repeat/);
  });
});

describe("open_page beyond parks, rides and posts", () => {
  const EP_ID = "22222222-2222-4222-8222-222222222222";
  const europaPark = {
    ...park,
    id: EP_ID,
    name: "Europa-Park",
    slug: "europa-park",
    citySlug: "rust",
  };
  const providers = () =>
    new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async (id: string) =>
            id === EP_ID ? europaPark : { ...park },
          ),
        },
      ],
    ]);
  const open = async (input: Record<string, unknown>) =>
    runAssistantTool("open_page", input, context(providers()), jest.fn());

  it("opens a park's wait-time calendar, which the widget respells per language", async () => {
    expect((await open({ park: PARK_ID, section: "calendar" })).action).toEqual(
      {
        type: "navigate",
        path: "/parks/europe/germany/bruehl/phantasialand/wait-time-calendar",
      },
    );
  });

  it("puts parks side by side by their full path, which no other park shares", async () => {
    expect((await open({ compare: [PARK_ID, EP_ID] })).action).toEqual({
      type: "navigate",
      path: "/compare?parks=europe/germany/bruehl/phantasialand,europe/germany/rust/europa-park",
    });
  });

  it("opens the site's own pages", async () => {
    expect((await open({ page: "alerts" })).action).toEqual({
      type: "navigate",
      path: "/alerts",
    });
    expect((await open({ page: "best_time" })).action).toMatchObject({
      path: "/best-time-to-visit",
    });
  });

  it("says why a park has no with-kids page instead of opening a 404", async () => {
    const rides = Array.from({ length: 30 }, (_, i) => ({
      id: `r${i}`,
      name: `Ride ${i}`,
      minimumHeight: i < 8 ? 100 : null,
    }));
    const result = await runAssistantTool(
      "open_page",
      { park: PARK_ID, section: "kids" },
      context(
        new Map<unknown, unknown>([
          ...providers(),
          [DataSource, { getRepository: () => ({ find: async () => rides }) }],
        ]),
      ),
      jest.fn(),
    );
    expect(result.isError).toBe(true);
    expect(result.content).toContain("only 8 of its 30 rides");
  });

  it("refuses two targets, or a section without its park", async () => {
    for (const input of [
      { park: PARK_ID, page: "alerts" },
      { section: "calendar", page: "favorites" },
    ]) {
      const result = await open(input);
      expect(result.isError).toBe(true);
      expect(result.action).toBeUndefined();
    }
  });
});

describe("calendar and tiles cards", () => {
  const forecast: ForecastSummary = {
    park: {
      name: "Phantasialand",
      pageUrl: `${getFrontendUrl()}/parks/europe/germany/bruehl/phantasialand`,
      timezone: "Europe/Berlin",
    },
    scheduleCoverage: { from: "2026-03-01", to: "2026-11-06" },
    days: [
      {
        date: "2026-11-05",
        status: "OPERATING",
        // Any instant comes out with the park's own offset.
        opens: "2026-11-05T08:00:00.000Z",
        closes: "2026-11-05T18:00+01:00",
        crowdLevel: "high",
        expectedPeakWaitMinutes: 52.4,
        schoolVacation: true,
        weather: {
          basis: "forecast",
          condition: "Rain",
          weatherCode: 61,
          tempMin: 4,
          tempMax: 9,
        },
      },
      {
        date: "2026-11-06",
        status: "CLOSED",
        opens: null,
        closes: null,
        crowdLevel: "closed",
      },
      {
        // Past the published schedule: the calendar's status is a guess.
        date: "2026-11-07",
        status: "OPERATING",
        opens: "2026-11-07T09:00+01:00",
        closes: "2026-11-07T18:00+01:00",
        crowdLevel: "unknown",
        weather: {
          basis: "climate_normal",
          condition: null,
          tempMin: 3,
          tempMax: 10,
        },
      },
    ],
  };

  it("shows the calendar's days, closed without a level, unpublished as unknown", () => {
    const card = calendarCard(forecast, "2026-11-05", 7);
    expect(card.url).toBe("/parks/europe/germany/bruehl/phantasialand");
    expect(card.days).toEqual([
      {
        date: "2026-11-05",
        status: "OPERATING",
        crowdLevel: "high",
        opens: "2026-11-05T09:00:00+01:00",
        closes: "2026-11-05T18:00:00+01:00",
        holiday: true,
      },
      {
        date: "2026-11-06",
        status: "CLOSED",
        crowdLevel: null,
        opens: null,
        closes: null,
        holiday: false,
      },
      {
        date: "2026-11-07",
        status: "UNKNOWN",
        crowdLevel: "unknown",
        opens: null,
        closes: null,
        holiday: false,
      },
    ]);
  });

  it("tiles a forecast day with its expected peak and no live numbers", () => {
    expect(tilesCard(forecast, "2026-11-05", null)).toEqual({
      kind: "tiles",
      park: "Phantasialand",
      url: "/parks/europe/germany/bruehl/phantasialand",
      date: "2026-11-05",
      live: false,
      crowdLevel: "high",
      avgWaitMinutes: null,
      peakWaitMinutes: 52,
      openRides: null,
      totalRides: null,
      opens: "2026-11-05T09:00:00+01:00",
      closes: "2026-11-05T18:00:00+01:00",
      weather: {
        condition: "Rain",
        weatherCode: 61,
        tempMin: 4,
        tempMax: 9,
        climateNormal: false,
      },
    });
    expect(tilesCard(forecast, "2026-11-07", null).weather?.climateNormal).toBe(
      true,
    );
  });

  it("tiles today live, with the live level rather than the forecast's", () => {
    const card = tilesCard(forecast, "2026-11-05", {
      crowd: {
        level: "moderate",
        avgWaitMinutes: 21.6,
        peakWaitTodayMinutes: 60,
      },
      scheduleToday: [
        { opens: "2026-11-05T08:00:00.000Z", closes: "2026-11-05T17:00:00Z" },
      ],
      rides: { total: 30, byStatus: { OPERATING: 24, DOWN: 1 } },
    });
    expect(card).toMatchObject({
      live: true,
      crowdLevel: "moderate",
      avgWaitMinutes: 22,
      peakWaitMinutes: 60,
      openRides: 24,
      totalRides: 30,
      opens: "2026-11-05T09:00:00+01:00",
      closes: "2026-11-05T18:00:00+01:00",
    });
  });

  it("writes a UTC+0 park's times with +00:00, never Z", () => {
    expect(parkInstant("2026-11-05T09:00:00.000Z", "Europe/London")).toBe(
      "2026-11-05T09:00:00+00:00",
    );
    expect(parkInstant(null, "Europe/London")).toBeNull();
  });

  it("refuses a day before the park's today or past the forecast", async () => {
    const providers = new Map<unknown, unknown>([
      [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
    ]);
    for (const input of [
      { kind: "calendar", park: PARK_ID, from: "2020-01-01" },
      { kind: "tiles", park: PARK_ID, date: "2099-01-01" },
    ]) {
      const result = await runAssistantTool(
        "show_card",
        input,
        context(providers),
        jest.fn(),
      );
      expect(result.isError).toBe(true);
      expect(result.action).toBeUndefined();
    }
  });
});

describe("planner tools", () => {
  const dayAhead = (days: number) =>
    new Date(Date.now() + days * 86_400_000).toISOString().slice(0, 10);
  const DATE = dayAhead(3);
  const SHOW_INSTANT = `${DATE}T13:30:00.000Z`;
  // The park's clock for that instant, whichever side of a DST switch the test runs on.
  const SHOW_START = formatInTimeZone(
    new Date(SHOW_INSTANT),
    "Europe/Berlin",
    "HH:mm",
  );
  const planner = {
    days: [
      {
        park: { slug: "phantasialand", name: "Phantasialand" },
        date: DATE,
        entries: [
          { id: "r1", kind: "ride" as const, name: "Taron", rideSlug: "taron" },
          { id: "b1", kind: "block" as const, name: "Lunch", start: "12:00" },
        ],
      },
    ],
  };
  const providers = new Map<unknown, unknown>([
    [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
    [
      ShowsService,
      {
        findByParkId: jest.fn(async () => [
          { id: "s1", slug: "stunt-show", name: "Stunt Show" },
        ]),
        getShowtimeInstantsOnDate: jest.fn(
          async () => new Map([["s1", [SHOW_INSTANT]]]),
        ),
      },
    ],
  ]);
  const call = (
    name: string,
    input: Record<string, unknown>,
    plan: typeof planner | { days: [] } | null = planner,
  ) =>
    runAssistantTool(
      name,
      { park: PARK_ID, date: DATE, ...input },
      { ...context(providers), planner: plan },
      jest.fn(),
    );
  const plannerPark = {
    slug: "phantasialand",
    name: "Phantasialand",
    geo: { continent: "europe", country: "germany", city: "bruehl" },
    timezone: "Europe/Berlin",
  };

  it.each([
    [
      "plan_remove",
      { entryIds: ["r1"] },
      { type: "planner_remove", entryIds: ["r1"] },
    ],
    [
      "plan_move",
      { entryId: "b1", toIndex: 0 },
      { type: "planner_move", entryId: "b1", toIndex: 0 },
    ],
    [
      "plan_update_block",
      { entryId: "b1", start: "12:30", durationMinutes: 45 },
      {
        type: "planner_update_block",
        entryId: "b1",
        start: "12:30",
        durationMinutes: 45,
      },
    ],
    [
      "plan_add_show",
      { showSlug: "stunt-show", start: SHOW_START },
      { type: "planner_add_show", showSlug: "stunt-show", start: SHOW_START },
    ],
    [
      "plan_set_party",
      { prefs: { riderHeightCm: 120, avoidWet: true } },
      {
        type: "planner_set_prefs",
        prefs: { riderHeightCm: 120, avoidWet: true },
      },
    ],
    [
      "plan_mark_done",
      { entryIds: ["r1"] },
      { type: "planner_mark_done", entryIds: ["r1"], done: true },
    ],
    ["plan_clear_day", { confirmed: true }, { type: "planner_clear_day" }],
    ["plan_undo", {}, { type: "planner_undo" }],
  ])("%s asks the planner for its change", async (name, input, action) => {
    const result = await call(name, input);
    expect(result.isError).toBeFalsy();
    expect(result.action).toEqual({ park: plannerPark, date: DATE, ...action });
  });

  describe("plan_add_rides and maintenance", () => {
    const TARON = "66666666-6666-4666-8666-666666666666";
    const RAIK = "77777777-7777-4777-8777-777777777777";
    const rideRow = (id: string, slug: string, name: string, periods = []) => ({
      id,
      slug,
      name,
      park: { slug: "phantasialand", timezone: "Europe/Berlin" },
      curatedOutOfServiceFrom: null,
      curatedOutOfServiceTo: null,
      curatedOutOfServicePeriods: periods,
      minimumHeight: slug === "taron" ? 130 : null,
    });
    const shut = { from: dayAhead(1), to: dayAhead(30), toUncertain: false };
    const add = (
      rideIds: string[],
      plan: typeof planner = planner,
      input: Record<string, unknown> = {},
    ) =>
      runAssistantTool(
        "plan_add_rides",
        { park: PARK_ID, date: DATE, rideIds, ...input },
        {
          ...context(
            new Map<unknown, unknown>([
              ...providers,
              [
                DataSource,
                {
                  getRepository: () => ({
                    find: jest.fn(async () => [
                      rideRow(TARON, "taron", "Taron"),
                      rideRow(RAIK, "raik", "Raik", [shut] as never),
                    ]),
                  }),
                },
              ],
            ]),
          ),
          planner: plan,
        },
        jest.fn(),
      );

    it("leaves a ride in maintenance out and says until when", async () => {
      const result = await add([TARON, RAIK]);
      expect(result.isError).toBeFalsy();
      expect(result.action).toEqual({
        type: "planner_add_rides",
        park: plannerPark,
        date: DATE,
        rides: [{ slug: "taron", name: "Taron" }],
      });
      expect(JSON.parse(result.content)).toMatchObject({
        added: ["Taron"],
        notAdded: [{ name: "Raik", reason: "maintenance", window: shut }],
      });
    });

    it("sends no planner change when every ride is in maintenance", async () => {
      const result = await add([RAIK]);
      expect(result.isError).toBeFalsy();
      expect(result.action).toBeUndefined();
      expect(JSON.parse(result.content)).toMatchObject({
        added: [],
        notAdded: [{ name: "Raik" }],
      });
    });

    // A 1 m child and Taron's 130 cm: picked for the party unasked, it once went into the plan.
    const withChild = {
      days: [{ ...planner.days[0], prefs: { riderHeightCm: 100 } }],
    };

    it("leaves out a ride the party's shortest rider is too short for", async () => {
      const result = await add([TARON], withChild);
      expect(result.isError).toBeFalsy();
      expect(result.action).toBeUndefined();
      expect(JSON.parse(result.content)).toMatchObject({
        added: [],
        notAdded: [
          {
            name: "Taron",
            reason: "too_short",
            minimumHeightCm: 130,
            riderHeightCm: 100,
          },
        ],
      });
    });

    it("adds it anyway once the user asked for it despite the height", async () => {
      const result = await add([TARON], withChild, { despiteHeight: true });
      expect(result.action).toMatchObject({
        rides: [{ slug: "taron", name: "Taron" }],
      });
    });
  });

  it("plan_trip reserves each day once, in order", async () => {
    const later = dayAhead(5);
    const result = await call("plan_trip", { dates: [later, DATE, later] });
    expect(result.action).toEqual({
      type: "planner_reserve_days",
      park: plannerPark,
      dates: [DATE, later],
    });
  });

  it("plan_share needs a plan", async () => {
    expect((await call("plan_share", {})).action).toEqual({
      type: "planner_share",
    });
    expect((await call("plan_share", {}, { days: [] })).isError).toBe(true);
  });

  it.each([
    ["an entry id the plan does not hold", "plan_remove", { entryIds: ["x9"] }],
    ["a day that is not planned", "plan_undo", { date: dayAhead(9) }],
    ["a ride as a block", "plan_update_block", { entryId: "r1", label: "x" }],
    ["a block with nothing to change", "plan_update_block", { entryId: "b1" }],
    ["a place past the end", "plan_move", { entryId: "r1", toIndex: 2 }],
    [
      "a time the show does not start",
      "plan_add_show",
      { showSlug: "stunt-show", start: "15:00" },
    ],
    [
      "an unknown show",
      "plan_add_show",
      { showSlug: "parade", start: SHOW_START },
    ],
    ["clearing without a yes", "plan_clear_day", {}],
    ["empty prefs", "plan_set_party", { prefs: {} }],
  ])("refuses %s", async (_case, name, input) => {
    const result = await call(name, input);
    expect(result.isError).toBe(true);
    expect(result.action).toBeUndefined();
  });

  it("refuses a change without a planner snapshot", async () => {
    expect(
      (await call("plan_remove", { entryIds: ["r1"] }, null)).isError,
    ).toBe(true);
  });
});

describe("alert tools", () => {
  const RIDE_ID = "22222222-2222-4222-8222-222222222222";
  const ride = (openWithPark = false, parkSlug = "phantasialand") => ({
    id: RIDE_ID,
    slug: "taron",
    name: "Taron",
    openWithPark,
    park: { ...park, slug: parkSlug },
  });
  const today = formatInTimeZone(new Date(), "Europe/Berlin", "yyyy-MM-dd");
  const instant = `${today}T23:59:00.000Z`;
  const start = formatInTimeZone(new Date(instant), "Europe/Berlin", "HH:mm");
  const providers = (found: unknown) =>
    new Map<unknown, unknown>([
      [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
      [
        DataSource,
        { getRepository: () => ({ findOne: jest.fn(async () => found) }) },
      ],
      [
        ShowsService,
        {
          findByParkId: jest.fn(async () => [
            { id: "s1", slug: "stunt-show", name: "Stunt Show" },
          ]),
          getShowtimeInstantsOnDate: jest.fn(
            async () => new Map([["s1", [instant]]]),
          ),
        },
      ],
    ]);
  const alerts = {
    push: "ready" as const,
    rides: [
      {
        park: "phantasialand",
        rideSlug: "taron",
        name: "Taron",
        kind: "reopen" as const,
      },
    ],
    shows: [],
  };
  const call = (
    name: string,
    input: Record<string, unknown>,
    found: unknown = ride(),
    held: typeof alerts | null = alerts,
  ) =>
    runAssistantTool(
      name,
      { park: PARK_ID, ...input },
      { ...context(providers(found)), alerts: held },
      jest.fn(),
    );
  const plannerPark = {
    slug: "phantasialand",
    name: "Phantasialand",
    geo: { continent: "europe", country: "germany", city: "bruehl" },
    timezone: "Europe/Berlin",
  };

  it("sets a wait alert and a reopen alert on a ride of the park", async () => {
    expect(
      (
        await call("set_ride_alert", {
          rideId: RIDE_ID,
          kind: "wait_below",
          thresholdMinutes: 20,
        })
      ).action,
    ).toEqual({
      type: "alert_ride_set",
      park: plannerPark,
      rideId: RIDE_ID,
      rideSlug: "taron",
      rideName: "Taron",
      kind: "wait_below",
      thresholdMinutes: 20,
    });
    expect(
      (
        await call("set_ride_alert", {
          rideId: RIDE_ID,
          kind: "reopen",
          thresholdMinutes: 20,
        })
      ).action,
    ).not.toHaveProperty("thresholdMinutes");
  });

  it("sets a show reminder for one of today's performances", async () => {
    const result = await call("set_show_reminder", {
      showSlug: "stunt-show",
      start,
    });
    expect(result.action).toEqual({
      type: "alert_show_set",
      park: plannerPark,
      showId: "s1",
      showSlug: "stunt-show",
      showName: "Stunt Show",
      start: parkInstant(instant, "Europe/Berlin"),
    });
  });

  it("removes an alert the browser holds", async () => {
    expect((await call("remove_alert", { rideSlug: "taron" })).action).toEqual({
      type: "alert_remove",
      park: plannerPark,
      rideSlug: "taron",
    });
  });

  it.each([
    [
      "a ride of another park",
      "set_ride_alert",
      { rideId: RIDE_ID, kind: "reopen" },
      ride(false, "efteling"),
    ],
    [
      "a wait alert without a threshold",
      "set_ride_alert",
      { rideId: RIDE_ID, kind: "wait_below" },
      ride(),
    ],
    [
      "a threshold over the push limit",
      "set_ride_alert",
      { rideId: RIDE_ID, kind: "wait_below", thresholdMinutes: 241 },
      ride(),
    ],
    [
      "a reopen alert on a ride open with the park",
      "set_ride_alert",
      { rideId: RIDE_ID, kind: "reopen" },
      ride(true),
    ],
    [
      "a reminder at a time the show does not start",
      "set_show_reminder",
      { showSlug: "stunt-show", start: "03:07" },
      ride(),
    ],
    [
      "removing an alert the browser does not hold",
      "remove_alert",
      { showSlug: "stunt-show" },
      ride(),
    ],
    ["removing without saying which", "remove_alert", {}, ride()],
  ])("refuses %s", async (_case, name, input, found) => {
    const result = await call(name, input, found);
    expect(result.isError).toBe(true);
    expect(result.action).toBeUndefined();
  });

  it("offers nothing where the browser cannot receive push", async () => {
    const result = await call(
      "set_ride_alert",
      { rideId: RIDE_ID, kind: "reopen" },
      ride(),
      { ...alerts, push: "unavailable" as never },
    );
    expect(result.isError).toBe(true);
  });
});

describe("reopeningOf", () => {
  it("names a breakdown's start and how such outages ended before", () => {
    expect(
      reopeningOf(
        {
          status: "DOWN",
          outage: {
            startedAt: "2026-11-05T10:00:00.000Z",
            signal: "down",
            estimate: {
              recoveryWithin30: 0.4,
              recoveryWithin60: 0.6,
              recoveryWindow: { from: "2026-11-05T10:40:00.000Z" },
            },
          },
          downtime: { kind: "figures", medianMinutes: 35 },
        },
        11,
      ),
    ).toEqual({
      reason: "breakdown",
      downSince: "2026-11-05T10:00:00.000Z",
      reportedByPark: true,
      backWithin30MinutesPercent: 0.4,
      backWithin60MinutesPercent: 0.6,
      likelyBack: { from: "2026-11-05T10:40:00.000Z" },
      typicalOutageMinutes: 35,
    });
  });

  it("names a works period's last closed day, and says when that is our guess", () => {
    expect(
      reopeningOf(
        {
          status: "REFURBISHMENT",
          worksPeriod: {
            to: "2027-03-13",
            toUncertain: true,
            runningToday: true,
          },
        },
        11,
      ),
    ).toEqual({
      reason: "works",
      closedUntil: "2027-03-13",
      estimatedByParkFan: true,
    });
  });

  it("names the next season month out of season", () => {
    expect(
      reopeningOf(
        {
          status: "CLOSED",
          facts: {
            seasonMonths: [4, 5, 6, 7, 8, 9, 10],
            isCurrentlyInSeason: false,
          },
        },
        11,
      ),
    ).toMatchObject({ reason: "season", nextSeasonMonth: 4 });
  });

  it("says unknown without data, and nothing while the ride runs", () => {
    expect(reopeningOf({ status: "CLOSED" }, 11)).toEqual({
      reason: "unknown",
      status: "CLOSED",
    });
    expect(reopeningOf({ status: "OPERATING" }, 11)).toBeNull();
  });
});

describe("favourites", () => {
  const ID = "33333333-3333-4333-8333-333333333333";
  const favorites = {
    parks: [PARK_ID],
    attractions: [ID],
    shows: [],
    restaurants: [],
  };

  it("get_favorites reads the ids the browser sent through /v1/favorites' service", async () => {
    const getFavorites = jest.fn(async () => ({
      parks: [
        {
          id: PARK_ID,
          name: "Phantasialand",
          status: "OPERATING",
          analytics: { crowdLevel: "high", avgWaitTime: 32 },
        },
      ],
      attractions: [
        {
          id: ID,
          name: "Taron",
          park: { name: "Phantasialand" },
          status: "OPERATING",
          queues: [{ queueType: "STANDBY", waitTime: 45 }],
          crowdLevel: "high",
        },
      ],
      shows: [],
      restaurants: [],
    }));
    const result = await runAssistantTool(
      "get_favorites",
      {},
      {
        ...context(new Map([[FavoritesService, { getFavorites }]])),
        favorites,
      },
      jest.fn(),
    );
    expect(getFavorites).toHaveBeenCalledWith([PARK_ID], [ID], [], []);
    expect(JSON.parse(result.content)).toEqual({
      parks: [
        {
          id: PARK_ID,
          name: "Phantasialand",
          status: "OPERATING",
          crowdLevel: "high",
          avgWaitMinutes: 32,
        },
      ],
      rides: [
        {
          id: ID,
          name: "Taron",
          park: "Phantasialand",
          status: "OPERATING",
          waitMinutes: 45,
          crowdLevel: "high",
        },
      ],
      shows: [],
      restaurants: [],
    });
  });

  it("set_favorite names what it found, and refuses an id nobody has", async () => {
    const providers = (row: unknown) =>
      new Map<unknown, unknown>([
        [
          DataSource,
          { getRepository: () => ({ findOne: jest.fn(async () => row) }) },
        ],
      ]);
    const set = await runAssistantTool(
      "set_favorite",
      { entity: "attraction", id: ID, on: false },
      context(providers({ id: ID, name: "Taron" })),
      jest.fn(),
    );
    expect(set.action).toEqual({
      type: "favorite_set",
      entity: "attraction",
      id: ID,
      name: "Taron",
      on: false,
    });
    const missing = await runAssistantTool(
      "set_favorite",
      { entity: "show", id: ID },
      context(providers(null)),
      jest.fn(),
    );
    expect(missing.isError).toBe(true);
    expect(missing.action).toBeUndefined();
  });
});

describe("reopenSearchFrom", () => {
  it("starts the day after a works period's last closed day", () => {
    expect(
      reopenSearchFrom(
        { reason: "works", closedUntil: "2027-03-13" },
        "2026-11-05",
      ),
    ).toBe("2027-03-14");
  });

  it("starts on the first of the next season month, next year once it has come round", () => {
    const season = (next: number) => ({
      reason: "season" as const,
      seasonMonths: [4, 10],
      nextSeasonMonth: next,
    });
    expect(reopenSearchFrom(season(4), "2026-11-05")).toBe("2027-04-01");
    expect(reopenSearchFrom(season(12), "2026-11-05")).toBe("2026-12-01");
  });

  it("has nothing to search from without an end", () => {
    expect(
      reopenSearchFrom({ reason: "works", closedUntil: null }, "2026-11-05"),
    ).toBeNull();
    expect(
      reopenSearchFrom({ reason: "unknown", status: "CLOSED" }, "2026-11-05"),
    ).toBeNull();
  });
});

describe("get_ride › today / get_park_day", () => {
  // The fixture is 7 November 2026, and `planDate` refuses a day more than two days back, so the
  // clock is the fixture's: a fixed date in the input stops being valid two days after it is written.
  beforeEach(() => {
    jest.spyOn(Date, "now").mockReturnValue(Date.parse("2026-11-07T08:00:00Z"));
  });
  afterEach(() => jest.restoreAllMocks());

  const RIDE = "55555555-5555-4555-8555-555555555555";
  // Phantasialand at 09:00, Taron an hour later; a show past midnight.
  const plan = {
    timezone: "Europe/Berlin",
    tier: "measured",
    context: {
      date: "2026-11-07",
      status: "OPERATING",
      openHour: 9,
      closeHour: 23,
    },
    rides: [
      {
        attractionId: RIDE,
        attractionName: "Taron",
        opensAt: "10:00",
        opensAtConfidence: "high",
        runsOnWeekdays: [1, 2, 3, 4, 5, 6, 7],
        dayPeak: 45,
        hours: [
          { hour: 10, wait: 20 },
          { hour: 14, wait: 45 },
          { hour: 21, wait: 10 },
        ],
      },
    ],
    shows: [
      {
        showName: "Night show",
        source: "scheduled",
        times: ["22:00", "00:30"],
      },
    ],
  };
  const providers = () =>
    new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({ ...park })),
          getFirstOperatingDayFrom: jest.fn(async () => null),
        },
      ],
      [PlanDayService, { buildPlanDay: jest.fn(async () => plan) }],
    ]);

  it("says when a ride starts that opens after the park", async () => {
    jest.spyOn(getAttractionTool, "handler").mockResolvedValue({
      id: RIDE,
      name: "Taron",
      status: "CLOSED",
      park: { id: PARK_ID, name: "Phantasialand" },
      facts: {},
    });
    const { json } = await run("get_ride", { rideId: RIDE }, providers());
    // 10:00 in Brühl is 09:00 UTC, the test's visitor clock.
    expect(json.today).toEqual({
      opensAt: "2026-11-07 09:00",
      opensAtConfidence: "high",
      runsOnWeekdays: [1, 2, 3, 4, 5, 6, 7],
    });
  });

  it("names the rides a maintenance window shuts that day", async () => {
    const window = { from: "2026-11-01", to: "2026-11-20", toUncertain: false };
    const ridesInMaintenance = jest.fn(async () => [
      { id: "88888888-8888-4888-8888-888888888888", name: "Raik", window },
    ]);
    const { json } = await run(
      "get_park_day",
      { park: PARK_ID, date: "2026-11-07" },
      new Map<unknown, unknown>([
        ...providers(),
        [
          PlanDayService,
          { buildPlanDay: jest.fn(async () => plan), ridesInMaintenance },
        ],
      ]),
    );
    expect(json.ridesInMaintenance).toEqual([
      { id: "88888888-8888-4888-8888-888888888888", name: "Raik", window },
    ]);
    expect(json.rides.map((ride: { name: string }) => ride.name)).toEqual([
      "Taron",
    ]);
  });

  it("marks the rides the party's shortest rider may not take, planned ones by name", async () => {
    const withHeights = {
      ...plan,
      rides: [
        { ...plan.rides[0], attractionSlug: "taron", minimumHeight: 130 },
        {
          ...plan.rides[0],
          attractionId: "99999999-9999-4999-8999-999999999999",
          attractionSlug: "wakobato",
          attractionName: "Wakobato",
          minimumHeight: 95,
        },
      ],
    };
    const result = await runAssistantTool(
      "get_park_day",
      { park: PARK_ID, date: "2026-11-07" },
      {
        ...context(
          new Map<unknown, unknown>([
            ...providers(),
            [
              PlanDayService,
              { buildPlanDay: jest.fn(async () => withHeights) },
            ],
          ]),
        ),
        planner: {
          days: [
            {
              park: { slug: "phantasialand", name: "Phantasialand" },
              date: "2026-11-07",
              prefs: { riderHeightCm: 100 },
              entries: [
                { id: "r1", kind: "ride", name: "Taron", rideSlug: "taron" },
              ],
            },
          ],
        },
      },
      jest.fn(),
    );
    const json = JSON.parse(result.content);
    expect(json.partyRiderHeightCm).toBe(100);
    expect(json.rides).toMatchObject([
      { name: "Taron", minimumHeightCm: 130, tooShortForParty: true },
      { name: "Wakobato", minimumHeightCm: 95 },
    ]);
    expect(json.rides[1]).not.toHaveProperty("tooShortForParty");
    expect(json.plannedTooShort).toEqual([
      { name: "Taron", minimumHeightCm: 130 },
    ]);
  });

  it("keeps the day when the maintenance lookup fails", async () => {
    const { json } = await run(
      "get_park_day",
      { park: PARK_ID, date: "2026-11-07" },
      providers(),
    );
    expect(json.ridesInMaintenance).toBeUndefined();
    expect(json.rides).toHaveLength(1);
  });

  it("lays out a planned day with each ride's opening and shortest hour", async () => {
    const { json } = await run(
      "get_park_day",
      { park: PARK_ID, date: "2026-11-07" },
      providers(),
    );
    expect(json).toMatchObject({
      park: "Phantasialand",
      opens: "2026-11-07 08:00",
      closes: "2026-11-07 22:00",
      rides: [
        {
          name: "Taron",
          opensAt: "2026-11-07 09:00",
          predictedPeakWaitMinutes: 45,
          shortestAt: { at: "2026-11-07 20:00", waitMinutes: 10 },
        },
      ],
      // 00:30 belongs to the night after.
      shows: [{ times: ["2026-11-07 21:00", "2026-11-07 23:30"] }],
    });
  });
});

describe("get_ride › reopening", () => {
  afterEach(() => {
    jest.useRealTimers();
    jest.restoreAllMocks();
  });

  const RIDE = "44444444-4444-4444-8444-444444444444";
  const run = async (ride: Record<string, unknown>, opening: unknown) => {
    jest.spyOn(getAttractionTool, "handler").mockResolvedValue({
      id: RIDE,
      name: "Chiapas",
      park: { id: PARK_ID, name: "Phantasialand" },
      facts: {},
      ...ride,
    });
    const getFirstOperatingDayFrom = jest.fn(async () => opening);
    const result = await runAssistantTool(
      "get_ride",
      { rideId: RIDE },
      context(
        new Map<unknown, unknown>([
          [
            ParksService,
            {
              findById: jest.fn(async () => ({ ...park })),
              getFirstOperatingDayFrom,
            },
          ],
        ]),
      ),
      jest.fn(),
    );
    return {
      reopening: JSON.parse(result.content).reopening,
      getFirstOperatingDayFrom,
    };
  };

  it("finds the season month in the park's clock at a month's turn", async () => {
    // 23:30 UTC on 31 March is already April in Brühl.
    jest.useFakeTimers({ now: new Date("2027-03-31T23:30:00Z") });
    const { reopening, getFirstOperatingDayFrom } = await run(
      {
        status: "CLOSED",
        facts: { seasonMonths: [4, 10], isCurrentlyInSeason: false },
      },
      { date: "2027-10-02" },
    );
    expect(reopening).toMatchObject({
      reason: "season",
      nextSeasonMonth: 10,
      reopensOn: "2027-10-02",
    });
    expect(getFirstOperatingDayFrom).toHaveBeenCalledWith(
      PARK_ID,
      "2027-10-01",
    );
  });

  it("names the park's first open day after a works period", async () => {
    const { reopening, getFirstOperatingDayFrom } = await run(
      {
        status: "REFURBISHMENT",
        worksPeriod: {
          to: "2099-03-13",
          toUncertain: false,
          runningToday: true,
        },
      },
      { date: "2099-03-20" },
    );
    expect(reopening).toEqual({
      reason: "works",
      closedUntil: "2099-03-13",
      reopensOn: "2099-03-20",
    });
    expect(getFirstOperatingDayFrom).toHaveBeenCalledWith(
      PARK_ID,
      "2099-03-14",
    );
  });

  it("says null when the calendar has no opening yet, and looks nothing up without an end", async () => {
    expect(
      (
        await run(
          {
            status: "REFURBISHMENT",
            worksPeriod: {
              to: "2099-03-13",
              toUncertain: false,
              runningToday: true,
            },
          },
          null,
        )
      ).reopening.reopensOn,
    ).toBeNull();
    const open = await run(
      {
        status: "REFURBISHMENT",
        worksPeriod: { to: null, toUncertain: false, runningToday: true },
      },
      null,
    );
    expect(open.reopening).toEqual({ reason: "works", closedUntil: null });
    expect(open.getFirstOperatingDayFrom).not.toHaveBeenCalled();
  });
});

describe("end_chat", () => {
  const CHAT = "55555555-5555-4555-8555-555555555555";
  const setup = (previousMessage: string | null) => {
    const update = jest.fn(async () => ({ affected: 1 }));
    const endChats = jest.fn(async () => undefined);
    const ctx = {
      ...context(
        new Map<unknown, unknown>([
          [DataSource, { getRepository: () => ({ update }) }],
          [AssistantRateLimitService, { endChats }],
        ]),
      ),
      chat: {
        conversationId: CHAT,
        pagePath: "/de",
        session: "session-1",
        sessionHash: "h",
        message: "…",
        previousMessage,
      },
      answer: { round: 0, done: new Set<string>() },
    };
    const call = (input: unknown) =>
      runAssistantTool("end_chat", input, ctx, jest.fn());
    return { call, update, endChats };
  };

  it("closes the chat, bans the session for the cooldown and tells the widget", async () => {
    const { call, update, endChats } = setup("an earlier insult");
    const result = await call({ reason: "slurs", note: "after a warning" });
    expect(result.action).toEqual({
      type: "chat_end",
      cooldownMinutes: CHAT_END_COOLDOWN_MINUTES,
    });
    expect(update).toHaveBeenCalledWith(CHAT, {
      endedAt: expect.any(Date),
      endedReason: "slurs: after a warning",
    });
    expect(endChats).toHaveBeenCalledWith(
      "session-1",
      CHAT_END_COOLDOWN_MINUTES * 60,
    );
  });

  it("refuses on a chat's first question, where no warning can have come before", async () => {
    const { call, update } = setup(null);
    const result = await call({ reason: "insults" });
    expect(result.isError).toBe(true);
    expect(update).not.toHaveBeenCalled();
  });

  it("takes no reason milder than abuse", async () => {
    const { call } = setup("x");
    expect((await call({ reason: "criticism" })).isError).toBe(true);
  });
});

describe("storedToolInput", () => {
  it("keeps which party details were set, never their values", () => {
    expect(
      storedToolInput({
        park: "phantasialand",
        prefs: { riderHeightCm: 112, avoidWet: true },
      }),
    ).toEqual({ park: "phantasialand", prefs: ["riderHeightCm", "avoidWet"] });
    expect(
      storedToolInput({
        park: "phantasialand",
        by: "speed",
        riderHeightCm: 112,
      }),
    ).toEqual({ park: "phantasialand", by: "speed", riderHeightCm: "set" });
    expect(storedToolInput({ rideId: "x" })).toEqual({ rideId: "x" });
  });
});
