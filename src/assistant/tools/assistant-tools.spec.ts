import { DataSource } from "typeorm";
import { PlanOptimizeService } from "../../planner/plan-optimize.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { FacilitiesService } from "../../facilities/facilities.service";
import { LocationService } from "../../location/location.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { getParkTool } from "../../mcp/tools/get-park.tool";
import { getAttractionTool } from "../../mcp/tools/get-attraction.tool";
import { findAttractionsTool } from "../../mcp/tools/find-attractions.tool";
import { BestDaysService } from "../../parks/services/best-days.service";
import { AssistantPostsClient } from "./assistant-posts.client";
import { AssistantKnowledgeService } from "../assistant-knowledge.service";

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

describe("read_knowledge", () => {
  const knowledge = {
    read: jest.fn(async (topic: string) =>
      topic === "forecasts"
        ? { topic, text: "Three crowd readings exist. Fancast …" }
        : null,
    ),
    topicNames: jest.fn(async () => ["forecasts"]),
  };
  const providers = new Map<unknown, unknown>([
    [AssistantKnowledgeService, knowledge],
  ]);

  it("returns a stored topic's text", async () => {
    const { json } = await run(
      "read_knowledge",
      { topic: "forecasts" },
      providers,
    );
    expect(json).toEqual({
      topic: "forecasts",
      text: "Three crowd readings exist. Fancast …",
    });
  });

  it("refuses a topic the base does not hold, naming the ones it does", async () => {
    const result = await runAssistantTool(
      "read_knowledge",
      { topic: "secrets" },
      context(providers),
      jest.fn(),
    );
    expect(result.isError).toBe(true);
    expect(result.content).toContain("forecasts");
  });

  it("refuses a name that is no topic name before asking the base", async () => {
    const result = await runAssistantTool(
      "read_knowledge",
      { topic: "Forecasts!" },
      context(providers),
      jest.fn(),
    );
    expect(result.errorKind).toBe("invalid_input");
  });
});

describe("posts and glossary cards", () => {
  const POST = {
    title: "Im Phantasialand lohnt der Quick Pass nur an vollen Tagen",
    description: "Den Quick Pass brauchst du nur an vollen Tagen.",
    date: "2026-10-14",
    updated: true,
    kind: "article",
    slug: "phantasialand-tipps",
    url: "https://park.fan/de/blog/phantasialand-tipps",
    readingTimeMinutes: 14,
    category: "Parkguides",
    image: "/media/blog/phantasialand-tipps/cover.webp?v=3",
    imagePosition: "50% 40%",
  };
  const NEWS = {
    ...POST,
    title: "Brühl prüft Bürgerbegehren gegen Phantasialand-Erweiterung",
    updated: false,
    kind: "news",
    slug: "phantasialand-erweiterung-unterschriften-abgegeben",
    url: "https://park.fan/de/news/phantasialand-erweiterung-unterschriften-abgegeben",
    image: "https://evil.example/cover.jpg",
  };
  const client = (posts: unknown[], terms: unknown[] = []) => ({
    search: jest.fn(async () => ({ total: posts.length, posts })),
    post: jest.fn(async () => ({ text: "Nur an vollen Tagen.", sections: [] })),
    glossary: jest.fn(async () => ({ results: terms })),
  });
  const providers = (posts: ReturnType<typeof client>) =>
    new Map<unknown, unknown>([[AssistantPostsClient, posts]]);

  it("shows posts as a card and reads the first one for the question", async () => {
    const posts = client([POST, NEWS]);
    const result = await run(
      "show_card",
      {
        kind: "posts",
        query: "Quick Pass Phantasialand",
        postKind: "article",
        question: "Lohnt sich der Quick Pass?",
      },
      providers(posts),
      "de",
    );
    expect(posts.search).toHaveBeenCalledWith(
      "de",
      "Quick Pass Phantasialand",
      "article",
      undefined,
    );
    expect(posts.post).toHaveBeenCalledWith(
      "de",
      "phantasialand-tipps",
      "Lohnt sich der Quick Pass?",
    );
    expect(result.action).toEqual({
      type: "card",
      card: {
        kind: "posts",
        posts: [
          {
            title: POST.title,
            url: "/de/blog/phantasialand-tipps",
            date: "2026-10-14",
            updated: true,
            news: false,
            readingTimeMinutes: 14,
            excerpt: POST.description,
            category: "Parkguides",
            image: POST.image,
            imagePosition: "50% 40%",
          },
          expect.objectContaining({
            news: true,
            // A cover from another host is no cover.
            image: null,
            imagePosition: null,
          }),
        ],
      },
    });
    expect(result.json.read).toEqual({
      text: "Nur an vollen Tagen.",
      sections: [],
    });
    expect(result.json.posts[0]).toEqual({
      title: POST.title,
      description: POST.description,
      date: "2026-10-14",
      updated: true,
      kind: "article",
      slug: "phantasialand-tipps",
      url: POST.url,
    });
  });

  it("shows the posts it is given by slug, without a search", async () => {
    const posts = client([NEWS]);
    await run(
      "show_card",
      { kind: "posts", slugs: [NEWS.slug] },
      providers(posts),
      "de",
    );
    expect(posts.search).toHaveBeenCalledWith("de", "", "any", [NEWS.slug]);
    expect(posts.post).not.toHaveBeenCalled();
  });

  it("says why no card comes when no post matches", async () => {
    const result = await run(
      "show_card",
      { kind: "posts", query: "Halloween", postKind: "news" },
      providers(client([])),
      "de",
    );
    expect(result.action).toBeUndefined();
    expect(result.json.why).toBe(
      'no park.fan news matches "Halloween" in this language',
    );
  });

  it("reads the best hit when search_posts gets a question", async () => {
    const posts = client([POST]);
    const result = await run(
      "search_posts",
      { query: "Quick Pass", question: "Lohnt sich der Quick Pass?" },
      providers(posts),
      "de",
    );
    expect(result.json.read.text).toBe("Nur an vollen Tagen.");
    expect(result.json.posts[0]).not.toHaveProperty("image");
  });

  it("still answers when the post cannot be read", async () => {
    const posts = client([POST]);
    posts.post.mockRejectedValueOnce(new Error("assistant-post answered 500"));
    const result = await run(
      "search_posts",
      { query: "Quick Pass", question: "Lohnt er sich?" },
      providers(posts),
      "de",
    );
    expect(result.json.read).toEqual({
      error: "could not read the post right now",
    });
    expect(result.json.posts).toHaveLength(1);
  });

  it("shows the best glossary term with its canonical page", async () => {
    const posts = client(
      [],
      [
        {
          name: "Blockbremse",
          slug: "blockbremse",
          shortDefinition: "Eine Bremse, die einen Zug anhält.",
          category: "coasters",
          url: "https://park.fan/de/glossar/blockbremse",
        },
        {
          name: "Trimmbremse",
          slug: "trimmbremse",
          shortDefinition: "Eine Bremse, die nur abbremst.",
          category: "coasters",
        },
      ],
    );
    const result = await run(
      "show_card",
      { kind: "glossary", query: "Blockbremse" },
      providers(posts),
      "de",
    );
    expect(result.action).toEqual({
      type: "card",
      card: {
        kind: "glossary",
        terms: [
          {
            name: "Blockbremse",
            definition: "Eine Bremse, die einen Zug anhält.",
            category: "coasters",
            url: "/de/glossar/blockbremse",
          },
        ],
      },
    });
    const looked = await run(
      "look_up_glossary",
      { term: "Bremse" },
      providers(posts),
      "de",
    );
    // A frontend without the url field: the old path, which redirects.
    expect(looked.json.terms[1].url).toBe(
      "https://park.fan/de/glossary/trimmbremse",
    );
  });
});

describe("show_card kind ride", () => {
  afterEach(() => jest.restoreAllMocks());

  it("draws no card for a name two rides share, and hands back both with their ids", async () => {
    jest.spyOn(findAttractionsTool, "handler").mockResolvedValue({
      attractions: [
        {
          id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
          name: "Wilde Maus",
          park: { name: "Wiener Prater" },
        },
        {
          id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          name: "Wilde Maus",
          park: { name: "Erlebnispark Tripsdrill" },
        },
      ],
    });
    const attraction = jest.spyOn(getAttractionTool, "handler");
    const result = await run(
      "show_card",
      { kind: "ride", ride: "Wilde Maus" },
      new Map<unknown, unknown>(),
    );
    expect(result.action).toBeUndefined();
    expect(attraction).not.toHaveBeenCalled();
    expect(result.json.ambiguous).toBe(true);
    expect(result.json.rides.map((ride: { id: string }) => ride.id)).toEqual([
      "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    ]);
  });

  it("takes a ride by its name and hands the model everything get_ride would", async () => {
    const RIDE_ID = "33333333-3333-4333-8333-333333333333";
    jest.spyOn(findAttractionsTool, "handler").mockResolvedValue({
      attractions: [
        { id: RIDE_ID, name: "Taron", park: { name: "Phantasialand" } },
      ],
    });
    jest.spyOn(getAttractionTool, "handler").mockResolvedValue({
      id: RIDE_ID,
      name: "Taron",
      park: null,
      status: "OPERATING",
      queues: [{ type: "STANDBY", waitMinutes: 25 }],
      facts: { hasSingleRider: true, minHeightCm: null },
      bestVisitTimesToday: [{ from: "17:00", to: "17:30" }],
      predictionAccuracy: { mae: 4 },
    });
    const result = await run(
      "show_card",
      { kind: "ride", ride: "Taron" },
      new Map<unknown, unknown>(),
    );
    expect(result.action).toMatchObject({
      type: "card",
      card: { kind: "ride", name: "Taron", waitMinutes: 25, singleRider: true },
    });
    expect(result.json.ride.bestVisitTimesToday).toEqual([
      { from: "17:00", to: "17:30" },
    ]);
    expect(result.json.ride.unknownFacts).toEqual(["minHeightCm"]);
    expect(result.json.ride).not.toHaveProperty("predictionAccuracy");
  });
});

describe("get_best_days", () => {
  it("ranks only rated open days, an unknown one never the quietest", async () => {
    const day = (date: string, crowdLevel: string, status = "OPERATING") => ({
      date,
      status: crowdLevel === "closed" ? "CLOSED" : status,
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
            timezone: "Europe/Berlin",
          })),
          getOperatingDateRange: jest.fn(async () => ({
            minDate: "2026-01-01",
            maxDate: "2026-11-09",
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
              // Past the published hours: never ranked, counted apart.
              day("2026-11-10", "very_low"),
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
    expect(json.closedDays).toBe(1);
    expect(json.unratedOpenDays).toBe(1);
    expect(json.hoursNotPublishedDays).toBe(1);
    expect(json.hoursNotPublishedWhy).toContain("2026-11-09");
    expect(
      json.tenQuietestOpenDays.map((d: { date: string }) => d.date),
    ).toEqual(["2026-11-08", "2026-11-06"]);
  });

  it("refuses a range past its 90 days and points at get_days", async () => {
    const providers = new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({
            id: "11111111-1111-4111-8111-111111111111",
            name: "Phantasialand",
            timezone: "Europe/Berlin",
          })),
        },
      ],
    ]);
    const result = await runAssistantTool(
      "get_best_days",
      {
        park: "11111111-1111-4111-8111-111111111111",
        from: "2099-05-01",
        to: "2099-05-31",
      },
      { get: <T>(token: unknown) => providers.get(token) as T, locale: "en" },
      jest.fn(),
    );
    expect(result.isError).toBe(true);
    expect(result.content).toContain("get_days");
  });
});
import {
  foldText,
  rideBadges,
  runAssistantTool,
  showsCard,
  showsOnDay,
  reopeningOf,
  parkShutAt,
  performancesOf,
  reopenSearchFrom,
  storedToolInput,
  CHAT_END_COOLDOWN_MINUTES,
} from "./assistant-tools";
import { PlanDayService } from "../../parks/services/plan-day.service";
import { WeatherService } from "../../parks/weather.service";
import { WeatherWarningsService } from "../../parks/weather-warnings.service";
import { CalendarService } from "../../parks/services/calendar.service";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import { HolidaysService } from "../../holidays/holidays.service";
import { FavoritesService } from "../../favorites/favorites.service";
import { AssistantRateLimitService } from "../assistant-rate-limit.service";
import { ShowsService } from "../../shows/shows.service";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { addIsoDays, eachIsoDate } from "../../common/utils/date.util";
import type { Show } from "../../shows/entities/show.entity";
import type { ShowLiveData } from "../../shows/entities/show-live-data.entity";

describe("the same day last year", () => {
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
    // A park without a country reads 52 weeks back.
    expect(json.comparableDayLastYear).toMatchObject({
      date: "2025-11-08",
      weekday: "Saturday",
      crowdLevel: "high",
      headlinerPeakWaitMinutes: 55,
      schoolVacation: true,
    });
  });

  it("compares a holiday with the same holiday last year (PAR-1377)", async () => {
    const easterMonday = (date: string) => ({
      date,
      name: "Easter Monday",
      localName: "Ostermontag",
      country: "DE",
      region: null,
      holidayType: "public",
      isNationwide: true,
    });
    const buildCalendarResponse = jest.fn(
      async (_park: unknown, fromDate: Date) => {
        const date = formatInTimeZone(fromDate, "Europe/Berlin", "yyyy-MM-dd");
        return {
          days: [
            {
              date,
              status: "OPERATING",
              crowdLevel: date === "2026-04-06" ? "very_high" : "low",
              isHoliday: date === "2026-04-06",
              isBridgeDay: false,
              isSchoolVacation: false,
              headlinerForecast: { avgWait: 30, rides: [], actual: true },
            },
          ],
        };
      },
    );
    const providers = new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({
            ...park,
            countryCode: "DE",
            regionCode: "NRW",
            timezone: "Europe/Berlin",
          })),
        },
      ],
      [CalendarService, { buildCalendarResponse }],
      [
        HolidaysService,
        {
          getHolidays: jest.fn(async () => [
            easterMonday("2026-04-06"),
            easterMonday("2027-03-29"),
          ]),
          getSchoolHolidayCoverage: jest.fn(async () => null),
        },
      ],
      [ParkSeasonService, { forParkInRange: jest.fn(async () => []) }],
    ]);
    const { json } = await run(
      "get_park_history",
      { park: PARK_ID, date: "2027-03-29" },
      providers,
    );
    // 52 weeks back is Monday 30 March 2026, an ordinary Monday.
    expect(json.comparableDayLastYear).toMatchObject({
      date: "2026-04-06",
      weekday: "Monday",
      crowdLevel: "very_high",
    });
    expect(json.sameDateLastYear).toMatchObject({ date: "2026-03-29" });
    // Each day is read on its own, once.
    expect(buildCalendarResponse).toHaveBeenCalledTimes(2);
  });

  it("passes over a day the park was closed a year back (PAR-1386)", async () => {
    // NRW's Christmas breaks; Phantasialand opened its winter on 26 December 2025.
    const breakDays = (from: string, to: string) =>
      eachIsoDate(from, to).map((date) => ({
        date,
        name: "Christmas Holidays",
        localName: "Weihnachtsferien",
        country: "DE",
        region: "DE-NW",
        holidayType: "school",
        isNationwide: false,
      }));
    const buildCalendarResponse = jest.fn(
      async (_park: unknown, fromDate: Date) => {
        const date = formatInTimeZone(fromDate, "Europe/Berlin", "yyyy-MM-dd");
        const open = date >= "2025-12-26";
        return {
          days: [
            {
              date,
              status: open ? "OPERATING" : "CLOSED",
              crowdLevel: open ? "high" : "closed",
              isHoliday: false,
              isBridgeDay: false,
              isSchoolVacation: date >= "2025-12-22",
              ...(open && {
                headlinerForecast: { avgWait: 55, rides: [], actual: true },
              }),
            },
          ],
        };
      },
    );
    const providers = new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({
            ...park,
            countryCode: "DE",
            regionCode: "NRW",
            timezone: "Europe/Berlin",
          })),
        },
      ],
      [CalendarService, { buildCalendarResponse }],
      [
        HolidaysService,
        {
          getHolidays: jest.fn(async () => [
            ...breakDays("2025-12-22", "2026-01-06"),
            ...breakDays("2026-12-23", "2027-01-06"),
          ]),
          getSchoolHolidayCoverage: jest.fn(async () => null),
        },
      ],
      [ParkSeasonService, { forParkInRange: jest.fn(async () => []) }],
    ]);
    const { json } = await run(
      "get_park_history",
      { park: PARK_ID, date: "2026-12-23" },
      providers,
    );
    // 52 weeks back, Wednesday 24 December 2025, was a break day too, and closed.
    expect(json.comparableDayLastYear).toMatchObject({
      date: "2025-12-31",
      weekday: "Wednesday",
      crowdLevel: "high",
      headlinerPeakWaitMinutes: 55,
    });
    // The two days picked, then the same date.
    expect(buildCalendarResponse).toHaveBeenCalledTimes(3);
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
  extra: Record<string, unknown> = {},
) {
  const onError = jest.fn();
  const result = await runAssistantTool(
    name,
    input,
    { ...context(providers, locale), ...extra },
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
      {
        id: "r1",
        name: "Eiscafé Rutmors",
        status: "OPERATING",
        land: "Rookburgh",
      },
    ]);
    const all = await run("find_restaurants", { park: PARK_ID }, providers);
    // No reading of its own: the payload's status is a guess from the park's, not passed on.
    expect(all.json.restaurants[1]).toMatchObject({
      name: "Wok",
      status: null,
      cuisine: "Asian",
    });
    expect(all.json.restaurants[2]).toEqual({
      id: "r3",
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

describe("places near a ride", () => {
  // Phantasialand's own positions: Cocorico sits under Talocan, Zambesi across the park.
  const entities = {
    attractions: [
      {
        id: "talocan",
        name: "Talocan",
        latitude: "50.7994214",
        longitude: "6.8811600",
      },
      {
        id: "taron",
        name: "Taron",
        latitude: "50.7996591",
        longitude: "6.8829768",
      },
    ],
    shows: [],
    restaurants: [
      {
        id: "zambesi",
        name: "Restaurant Zambesi",
        latitude: "50.7982550",
        longitude: "6.8812602",
      },
      {
        id: "cocorico",
        name: "Restaurante Cocorico",
        latitude: "50.7992805",
        longitude: "6.8812974",
      },
      {
        id: "cantina",
        name: "La Cantina Tacana",
        latitude: "50.7991622",
        longitude: "6.8821973",
      },
      { id: "nowhere", name: "Food Truck", latitude: null, longitude: null },
    ],
  };
  const providers = new Map<unknown, unknown>([
    [
      ParksService,
      {
        findById: jest.fn(async () => ({ ...park })),
        loadParkRelations: jest.fn(async (p: object) => ({
          ...p,
          ...entities,
        })),
      },
    ],
    [
      ParkIntegrationService,
      {
        buildIntegratedResponse: jest.fn(async () => ({
          status: "OPERATING",
          restaurants: entities.restaurants.map(({ id, name }) => ({
            id,
            name,
          })),
        })),
      },
    ],
  ]);

  it("lists the restaurants nearest a ride first, with the walk", async () => {
    const { json } = await run(
      "find_restaurants",
      { park: PARK_ID, near: "talocan" },
      providers,
    );
    expect(json.near).toEqual({ id: "talocan", name: "Talocan", kind: "ride" });
    expect(json.restaurants.map((r: { id: string }) => r.id)).toEqual([
      "cocorico",
      "cantina",
      "zambesi",
      "nowhere",
    ]);
    expect(json.restaurants[0]).toMatchObject({ metres: 20, walkMinutes: 1 });
    expect(json.restaurants[3].metres).toBeUndefined();
  });

  it("keeps the walk out of the card but hands it to the model", async () => {
    const { action, json } = await run(
      "show_card",
      { kind: "restaurants", park: PARK_ID, near: "Talocan", query: "rest" },
      providers,
    );
    const card = (action as { card: { restaurants: object[] } }).card;
    expect(card.restaurants).toEqual([
      {
        name: "Restaurante Cocorico",
        status: null,
        land: null,
        cuisine: null,
        requiresReservation: null,
      },
      {
        name: "Restaurant Zambesi",
        status: null,
        land: null,
        cuisine: null,
        requiresReservation: null,
      },
    ]);
    expect(json.near).toMatchObject({ name: "Talocan" });
    expect(json.restaurants[1]).toMatchObject({ id: "zambesi", metres: 130 });
  });

  it("measures from one place to several and names the ones it cannot place", async () => {
    const { json } = await run(
      "get_walking_distance",
      {
        park: PARK_ID,
        from: "Taron",
        to: ["Cocorico", "Food Truck", "Space Mountain"],
      },
      providers,
    );
    expect(json.from).toEqual({ id: "taron", name: "Taron", kind: "ride" });
    expect(json.distances[0]).toEqual({
      id: "cocorico",
      name: "Restaurante Cocorico",
      kind: "restaurant",
      metres: 130,
      walkMinutes: 3,
    });
    expect(json.distances[1]).toMatchObject({
      name: "Food Truck",
      why: expect.any(String),
    });
    expect(json.distances[2]).toMatchObject({ asked: "Space Mountain" });
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
        errorKind: "refused",
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

  it("keeps the whole reading but the occupancy while it runs", async () => {
    summary("OPERATING");
    const { json } = await run("get_park_live", { park: PARK_ID }, new Map());
    const { occupancyPercent: _occupancy, ...shown } = crowd;
    expect(json.crowd).toEqual(shown);
  });

  it("hands the model the whole live summary with the park card", async () => {
    summary("OPERATING");
    const result = await run(
      "show_card",
      { kind: "park", park: PARK_ID },
      new Map(),
    );
    // One call instead of the card and get_park_live side by side.
    const { occupancyPercent: _occupancy, ...shown } = crowd;
    expect(result.json.live).toMatchObject({
      name: "Six Flags Fiesta Texas",
      crowd: shown,
    });
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
        id: "c",
        name: "Raik",
        status: "OPERATING",
        waitMinutes: 5,
        types: ["boomerang"],
      },
      {
        id: "a",
        name: "Taron",
        status: "OPERATING",
        waitMinutes: 45,
        types: ["launch-coaster"],
      },
      {
        id: "d",
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
        id: "c",
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
      {
        id: "a",
        name: "Taron",
        status: "OPERATING",
        waitMinutes: 45,
        ridden: true,
      },
      { id: "c", name: "Raik", status: "OPERATING", waitMinutes: 5 },
    ]);
  });
});

describe("show_card kind rides for a child", () => {
  afterEach(() => jest.restoreAllMocks());

  const providers = (rows: object[]) =>
    new Map<unknown, unknown>([
      [RideProfileService, profiles({})],
      [
        DataSource,
        { getRepository: () => ({ find: jest.fn(async () => rows) }) },
      ],
    ]);

  it("keeps the rides a child may ride, the others named with their limits", async () => {
    jest.spyOn(getParkTool, "handler").mockResolvedValue({
      name: "Phantasialand",
      status: "OPERATING",
      rides: {
        longestWaits: [
          { id: "a", name: "Taron", waitMinutes: 45 },
          { id: "b", name: "Wellenflug", waitMinutes: 10 },
          { id: "c", name: "Raik", waitMinutes: 5 },
        ],
        down: [],
      },
    });
    const { json } = await run(
      "show_card",
      { kind: "rides", park: PARK_ID, riderHeightCm: 110 },
      providers([
        { id: "a", name: "Taron", minimumHeight: 130 },
        { id: "b", name: "Wellenflug", minimumHeight: 100 },
        { id: "c", name: "Raik", minimumHeight: null },
      ]),
    );
    expect(json.rows).toEqual([
      expect.objectContaining({ name: "Wellenflug", minimumHeightCm: 100 }),
    ]);
    expect(json.notAllowedRides).toEqual([
      { id: "a", name: "Taron", minimumHeightCm: 130 },
    ]);
    expect(json.heightUnknownRides).toEqual([{ id: "c", name: "Raik" }]);
  });

  it("says why the card of a closed park is empty", async () => {
    jest.spyOn(getParkTool, "handler").mockResolvedValue({
      name: "Phantasialand",
      status: "CLOSED",
      rides: { longestWaits: [], down: [] },
      nextSchedule: { date: "2026-11-14" },
    });
    const { json } = await run(
      "show_card",
      { kind: "rides", park: PARK_ID, shelter: "covered" },
      providers([]),
    );
    expect(json).toMatchObject({
      parkStatus: "CLOSED",
      why: expect.stringContaining("not operating"),
      nextOpening: { date: "2026-11-14" },
    });
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

  it("ranks a park whose day guests leave early behind a full day with a similar wait", async () => {
    // Magic Kingdom on a party night: 40 min but out at 18:00 against a usual
    // 23:00; Animal Kingdom 45 min over a full day (as on 2026-11-24).
    const MK = parkRow("66666666-6666-4666-8666-666666666666", "Magic Kingdom");
    const AK = parkRow(
      "77777777-7777-4777-8777-777777777777",
      "Animal Kingdom",
    );
    const NY = "America/New_York";
    const local = (date: string, time: string) =>
      fromZonedTime(`${date}T${time}:00`, NY).toISOString();
    const window = (from: Date, to: Date) => {
      const out: string[] = [];
      for (
        let d = from.toISOString().slice(0, 10);
        d <= to.toISOString().slice(0, 10);
        d = addIsoDays(d, 1)
      ) {
        out.push(d);
      }
      return out;
    };
    const calendarFor = jest.fn(
      async (p: { id: string }, from: Date, to: Date) => ({
        meta: {
          scheduleCoverage: { from: "2025-12-26", to: dayAhead(100) },
          hasOperatingSchedule: true,
        },
        days: window(from, to).map((date) => ({
          date,
          status: "OPERATING",
          crowdLevel: p.id === MK.id ? "low" : "moderate",
          headlinerForecast: { avgWait: p.id === MK.id ? 40 : 45 },
          hours: {
            openingTime: local(date, "09:00"),
            closingTime:
              p.id === MK.id && date === DATE
                ? local(date, "18:00")
                : local(date, p.id === MK.id ? "23:00" : "20:00"),
            isInferred: false,
          },
        })),
      }),
    );
    const parks = [MK, AK].map((row) => ({ ...row, timezone: NY }));
    const { json } = await run(
      "compare_parks",
      { parks: [MK.id, AK.id], date: DATE },
      new Map<unknown, unknown>([
        [
          ParksService,
          {
            findById: jest.fn(async (id: string) =>
              parks.find((row) => row.id === id),
            ),
            getSchedule: jest.fn(async () => []),
          },
        ],
        [CalendarService, { buildCalendarResponse: calendarFor }],
      ]),
    );
    expect(json.ranked.map((row: { name: string }) => row.name)).toEqual([
      "Animal Kingdom",
      "Magic Kingdom",
    ]);
    expect(json.ranked[1]).toMatchObject({
      closes: `${DATE} 18:00`,
      closesEarlyForDayGuests: true,
      usualClose: "23:00",
    });
    expect(json.ranked[0]).not.toHaveProperty("closesEarlyForDayGuests");
  });

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

  it("names a park whose published schedule gives the day no status, never drops it", async () => {
    const saved = days[EP.id];
    days[EP.id] = { status: "UNKNOWN", crowdLevel: "unknown" };
    try {
      const { json } = await run(
        "compare_parks",
        { country: "Germany", date: DATE },
        providers(),
      );
      expect(
        json.ranked.map((row: { name: string }) => row.name),
      ).not.toContain("Europa-Park");
      expect(json.notKnown).toContainEqual({
        name: "Europa-Park",
        why: "the park's published schedule names no opening status for that day",
      });
    } finally {
      days[EP.id] = saved;
    }
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

describe("get_days", () => {
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
      "get_days",
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
    expect(json.quietestOpenDays).toEqual([OPEN_QUIET, OPEN_BUSY]);
  });

  it("calls a day past the published hours unknown and says why, never closed", async () => {
    const { providers: p } = providers();
    const { json } = await run(
      "get_days",
      { park: PARK_ID, dates: [LATER] },
      p,
    );
    expect(json.days[0]).toMatchObject({
      status: "UNKNOWN",
      hoursPublished: false,
      estimatedStatus: "CLOSED",
    });
    expect(json.unknownDaysWhy).toContain(published);
  });

  it("reads each month once, not the months between", async () => {
    const { providers: p, build } = providers();
    await run("get_days", { park: PARK_ID, dates: [OPEN_QUIET, LATER] }, p);
    const months = new Set([OPEN_QUIET, LATER].map((date) => date.slice(0, 7)));
    expect(build).toHaveBeenCalledTimes(months.size);
  });

  it("takes a stretch of days, and refuses a past day or one past the forecast", async () => {
    const { providers: p } = providers();
    const { json } = await run(
      "get_days",
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
        "get_days",
        { park: PARK_ID, ...input },
        context(p),
        jest.fn(),
      );
      expect(result.isError).toBe(true);
    }
  });
});

describe("times in park time", () => {
  it("gives a park's times in its own clock and names the zone, wherever the visitor is", async () => {
    const day = new Date(Date.now() + 3 * 86_400_000)
      .toISOString()
      .slice(0, 10);
    const result = await runAssistantTool(
      "get_park_day",
      { park: PARK_ID, date: day },
      {
        ...context(
          new Map<unknown, unknown>([
            [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
            [
              PlanDayService,
              {
                buildPlanDay: jest.fn(async () => ({
                  tier: "measured",
                  context: {
                    date: day,
                    status: "OPERATING",
                    openHour: 9,
                    closeHour: 18,
                  },
                  rides: [],
                  shows: [],
                })),
              },
            ],
          ]),
        ),
        // A visitor in New York planning a day in Brühl hears Brühl's hours.
        timeZone: "America/New_York",
      },
      jest.fn(),
    );
    const json = JSON.parse(result.content);
    expect(json.timesIn).toMatch(
      /^park time, Europe\/Berlin; now there \d{4}-\d{2}-\d{2} \d{2}:\d{2}$/,
    );
    expect(json.opens).toBe(`${day} 09:00`);
  });
});

describe("a ride by its name", () => {
  afterEach(() => jest.restoreAllMocks());

  const providers = () =>
    new Map<unknown, unknown>([
      [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
    ]);

  it("takes the ride whose name is the reference, with no find_rides round first", async () => {
    jest.spyOn(findAttractionsTool, "handler").mockResolvedValue({
      attractions: [
        { id: "r-mamba", name: "Black Mamba", park: { name: "Phantasialand" } },
        { id: "r-taron", name: "Taron", park: { name: "Phantasialand" } },
      ],
    });
    const getRide = jest
      .spyOn(getAttractionTool, "handler")
      .mockResolvedValue({ id: "r-taron", name: "Taron", facts: {} });
    await run("get_ride", { ride: "taron", park: PARK_ID }, providers());
    expect(getRide.mock.calls[0][0]).toMatchObject({ attractionId: "r-taron" });
  });

  it("hands several equal rides back to pick from", async () => {
    jest.spyOn(findAttractionsTool, "handler").mockResolvedValue({
      attractions: [
        { id: "a", name: "Pirates", park: { name: "Disneyland Park" } },
        { id: "b", name: "Pirates", park: { name: "Magic Kingdom" } },
      ],
    });
    const { json } = await run("get_ride", { ride: "Pirates" }, providers());
    expect(json).toMatchObject({
      ambiguous: true,
      rides: [{ id: "a" }, { id: "b" }],
    });
  });
});

describe("get_hours", () => {
  // The fixtures plan 14 November 2026; on that day itself the tool drops the hours already gone,
  // so the clock stands days before it, whatever day the suite runs.
  beforeEach(() => {
    jest.useFakeTimers({ now: new Date("2026-11-10T12:00:00Z") });
  });
  afterEach(() => {
    jest.useRealTimers();
  });
  const plan = (status: string, extra: object = {}) => ({
    tier: "composed",
    context: { date: "2026-11-14", status, openHour: 10, closeHour: 18 },
    rides: [
      {
        attractionId: "a",
        attractionName: "Taron",
        isHeadliner: true,
        hours: [
          { hour: 9, wait: 5 },
          { hour: 10, wait: 20 },
          { hour: 11, wait: 40 },
          { hour: 17, wait: 10 },
          { hour: 18, wait: 5 },
        ],
      },
      {
        attractionId: "b",
        attractionName: "Black Mamba",
        isHeadliner: true,
        hours: [
          { hour: 10, wait: 10 },
          { hour: 11, wait: 30 },
          { hour: 17, wait: 20 },
        ],
      },
      {
        attractionId: "c",
        attractionName: "Kiddie",
        hours: [{ hour: 10, wait: 90 }],
      },
    ],
    shows: [],
    ...extra,
  });
  const providers = (built: object) =>
    new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({ ...park })),
          getOperatingDateRange: jest.fn(async () => ({
            minDate: "2026-01-01",
            maxDate: "2027-01-10",
          })),
          getFirstOperatingDayFrom: jest.fn(async () => ({
            date: "2026-11-20",
          })),
        },
      ],
      [PlanDayService, { buildPlanDay: jest.fn(async () => built) }],
    ]);

  it("lays the day out hour by hour from the headliners, opening to closing", async () => {
    const { json } = await run(
      "get_hours",
      { park: PARK_ID, date: "2026-11-14" },
      providers(plan("OPERATING")),
    );
    // 09:00 is before opening and 18:00 the closing hour: neither is listed.
    expect(
      json.hours.map((h: { waitMinutes: number }) => h.waitMinutes),
    ).toEqual([15, 35, 15]);
    expect(json.counted).toBe("headliners");
    expect(json.bestHours).toHaveLength(3);
    expect(json.busiestHour).toBe(json.hours[1].at);
    expect(json.tierWhy).toContain("spread");
  });

  it("gives a closed day no hours and the next day the park opens", async () => {
    const { json } = await run(
      "get_hours",
      { park: PARK_ID, date: "2026-11-14" },
      providers(
        plan("CLOSED", {
          context: {
            date: "2026-11-14",
            status: "CLOSED",
            openHour: null,
            closeHour: null,
          },
          rides: [],
        }),
      ),
    );
    expect(json).toMatchObject({
      status: "CLOSED",
      hours: [],
      nextOpenDay: "2026-11-20",
    });
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

  it("opens a glossary term under its segment in the reader's language", async () => {
    expect(
      (await open({ url: "https://park.fan/de/glossar/blockbremse" })).action,
    ).toEqual({ type: "navigate", path: "/de/glossar/blockbremse" });
    expect(
      (await open({ url: "https://park.fan/de/glossarx/blockbremse" })).action,
    ).toBeUndefined();
  });

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
    expect((await open({ page: "how_it_works" })).action).toMatchObject({
      path: "/how-park-fan-works",
    });
    expect((await open({ page: "fancast" })).action).toMatchObject({
      path: "/fancast",
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

  it("moves an entry to a time, given a time or a place but not both", async () => {
    const moved = await call("plan_move", { entryId: "r1", start: "14:00" });
    expect(moved.action).toEqual({
      type: "planner_move",
      park: plannerPark,
      date: DATE,
      entryId: "r1",
      start: "14:00",
    });
    expect((await call("plan_move", { entryId: "r1" })).isError).toBe(true);
    expect(
      (await call("plan_move", { entryId: "r1", toIndex: 0, start: "14:00" }))
        .isError,
    ).toBe(true);
  });

  it("files reserved days with the party's smallest rider", async () => {
    const result = await runAssistantTool(
      "plan_trip",
      { park: PARK_ID, dates: [DATE], riderHeightCm: 105 },
      { ...context(providers), planner },
      jest.fn(),
    );
    expect(result.action).toEqual({
      type: "planner_reserve_days",
      park: plannerPark,
      dates: [DATE],
      prefs: { riderHeightCm: 105 },
    });
  });

  it("prices a planned day: each ride's wait at its start, the totals and what sorting saves", async () => {
    const day = {
      park: { slug: "phantasialand", name: "Phantasialand" },
      date: DATE,
      prefs: { riderHeightCm: 110 },
      entries: [
        {
          id: "r1",
          kind: "ride" as const,
          name: "Taron",
          rideSlug: "taron",
          startMinute: 600,
        },
        {
          id: "r2",
          kind: "ride" as const,
          name: "Raik",
          rideSlug: "raik",
          startMinute: 660,
        },
        {
          id: "s1",
          kind: "show" as const,
          name: "Stunt Show",
          showSlug: "stunt-show",
          startMinute: 900,
          durationMinutes: 30,
        },
      ],
    };
    const optimize = jest.fn(async () => ({
      status: "optimized",
      refusal: null,
      before: {
        totalWaitMinutes: 75,
        idleMinutes: 10,
        walkMinutes: 20,
        endMinute: 1000,
        overflow: 0,
        clashes: 0,
      },
      after: {
        totalWaitMinutes: 55,
        idleMinutes: 5,
        walkMinutes: 12,
        endMinute: 980,
        overflow: 0,
        clashes: 0,
      },
    }));
    const priced = new Map<unknown, unknown>([
      ...providers,
      [
        ParksService,
        {
          findById: jest.fn(async () => ({ ...park })),
          findByGeographicPath: jest.fn(async () => ({ ...park, id: PARK_ID })),
        },
      ],
      [
        PlanDayService,
        {
          buildPlanDay: jest.fn(async () => ({
            context: { openHour: 9, closeHour: 18 },
            rides: [
              {
                attractionSlug: "taron",
                attractionName: "Taron",
                minimumHeight: 130,
                hours: [{ hour: 10, wait: 45 }],
                slots: [{ minute: 600, wait: 40 }],
              },
              {
                attractionSlug: "raik",
                attractionName: "Raik",
                hours: [{ hour: 11, wait: 10 }],
              },
            ],
            shows: [
              {
                showSlug: "stunt-show",
                showName: "Stunt Show",
                times: ["14:00", "16:30"],
              },
            ],
          })),
          ridesInWorks: jest.fn(async () => []),
        },
      ],
      [
        REDIS_CLIENT,
        { get: jest.fn(async () => null), set: jest.fn(async () => "OK") },
      ],
      [PlanOptimizeService, { optimize }],
    ]);
    const result = await runAssistantTool(
      "review_plan_day",
      { park: PARK_ID, date: DATE },
      { ...context(priced), planner: { days: [day] } },
      jest.fn(),
    );
    expect(result.isError).toBeFalsy();
    const json = JSON.parse(result.content);
    expect(json.entries).toEqual([
      {
        id: "r1",
        kind: "ride",
        name: "Taron",
        at: "10:00",
        waitMinutes: 40,
        tooShortForParty: { minimumHeightCm: 130 },
      },
      { id: "r2", kind: "ride", name: "Raik", at: "11:00", waitMinutes: 10 },
      {
        id: "s1",
        kind: "show",
        name: "Stunt Show",
        at: "15:00",
        notAShowTime: { showTimes: ["14:00", "16:30"] },
      },
    ]);
    expect(json).toMatchObject({
      totalWaitMinutes: 75,
      endsAt: "16:40",
      closes: "18:00",
      sortingSaves:
        "plan_optimize would save about 20 min of queueing and 8 min of walking",
    });
    expect(optimize).toHaveBeenCalledWith(
      {
        continent: "europe",
        country: "germany",
        city: "bruehl",
        parkSlug: "phantasialand",
      },
      expect.objectContaining({
        mode: "day",
        preview: true,
        prefs: { riderHeightCm: 110 },
      }),
    );
  });

  describe("reply chips and the interview", () => {
    const answer = () => ({ round: 0, done: new Set<string>() });
    const ask = (
      name: string,
      input: Record<string, unknown>,
      extra: Record<string, unknown> = {},
    ) =>
      runAssistantTool(
        name,
        input,
        { ...context(providers), planner, answer: answer(), ...extra },
        jest.fn(),
      );

    it("sends two to four clean replies, once per answer", async () => {
      const result = await ask("suggest_replies", {
        replies: [
          "Next summer",
          "**I know our dates**",
          "next summer",
          "see https://x.y",
        ],
      });
      expect(result.action).toEqual({
        type: "replies",
        replies: ["Next summer", "I know our dates"],
      });
      expect((await ask("suggest_replies", { replies: ["Yes"] })).isError).toBe(
        true,
      );
      // One chip too long for the widget drops alone; the others still show.
      expect(
        (
          await ask("suggest_replies", {
            replies: [
              "26 de octubre al 4 de noviembre",
              "Mantengamos el 24 de octubre al 2 de noviembre",
            ],
          })
        ).action,
      ).toEqual({
        type: "replies",
        replies: ["26 de octubre al 4 de noviembre"],
      });
      const state = answer();
      const twice = async () =>
        runAssistantTool(
          "suggest_replies",
          { replies: ["Yes", "No"] },
          { ...context(providers), answer: state },
          jest.fn(),
        );
      expect((await twice()).isError).toBeFalsy();
      expect((await twice()).content).toMatch(/already ran/);
    });

    it("starts an interview on its first step and names what to ask", async () => {
      const result = await ask("interview", {
        op: "start",
        kind: "trip",
        region: "orlando",
      });
      expect(result.action).toEqual({
        type: "interview_state",
        interview: {
          kind: "trip",
          region: "orlando",
          step: "region",
          answered: [],
          skipped: [],
          mustDoRides: [],
          source: "chat",
        },
      });
      expect(JSON.parse(result.content).next).toMatchObject({
        step: "region",
        of: "1 of 3",
      });
    });

    const running = {
      kind: "trip" as const,
      region: "orlando",
      step: "parks",
      answered: ["region", "dates"],
      skipped: [],
      mustDoRides: [],
      source: "planner" as const,
    };

    it("goes from the core to the plan, taking what was said early too", async () => {
      const result = await ask(
        "interview",
        { op: "answered", also: ["tickets"] },
        { interview: running },
      );
      expect(result.action).toEqual({
        type: "interview_state",
        interview: {
          ...running,
          step: "refine",
          answered: ["region", "dates", "parks", "tickets"],
        },
      });
      expect(JSON.parse(result.content).next).toMatchObject({
        step: "refine",
        of: "the plan stands",
      });
    });

    it("takes up a refinement the user picked, keeps the party with the trip", async () => {
      const planned = {
        ...running,
        step: "refine",
        answered: ["region", "dates", "parks"],
      };
      const picked = await ask(
        "interview",
        { op: "ask", step: "party" },
        { interview: planned },
      );
      expect(picked.action).toMatchObject({
        interview: { step: "party" },
      });
      expect(
        (
          await ask(
            "interview",
            { op: "ask", step: "dates" },
            { interview: planned },
          )
        ).isError,
      ).toBe(true);
      const answered = await ask(
        "interview",
        { op: "answered", party: { size: 4, heightsCm: [105, 132] } },
        { interview: { ...planned, step: "party" } },
      );
      expect(answered.action).toEqual({
        type: "interview_state",
        interview: {
          ...planned,
          step: "refine",
          answered: ["region", "dates", "parks", "party"],
        },
        party: { size: 4, heightsCm: [105, 132] },
        region: "orlando",
      });
    });

    it("ends when they have enough or want to stop, and needs a running interview", async () => {
      const done = await ask(
        "interview",
        { op: "end", outcome: "done" },
        { interview: running },
      );
      expect(done.action).toEqual({
        type: "interview_state",
        interview: null,
        outcome: "done",
      });
      const left = await ask(
        "interview",
        { op: "end" },
        { interview: running },
      );
      expect(left.action).toMatchObject({ outcome: "left" });
      expect((await ask("interview", { op: "answered" })).isError).toBe(true);
    });
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
    afterEach(() => jest.restoreAllMocks());
    const add = (
      rides: string[],
      plan: typeof planner = planner,
      input: Record<string, unknown> = {},
    ) =>
      runAssistantTool(
        "plan_add_rides",
        { park: PARK_ID, date: DATE, rides, ...input },
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

    it("takes the party's height from the call when the plan has none yet", async () => {
      const result = await add([TARON], planner, { riderHeightCm: 100 });
      expect(result.action).toBeUndefined();
      expect(JSON.parse(result.content)).toMatchObject({
        notAdded: [{ name: "Taron", reason: "too_short" }],
      });
    });

    it("adds it anyway once the user asked for it despite the height", async () => {
      const result = await add([TARON], withChild, { despiteHeight: true });
      expect(result.action).toMatchObject({
        rides: [{ slug: "taron", name: "Taron" }],
      });
    });

    // "Ja, alle Tipps einplanen" cost five find_rides rounds: the earlier answer's ids were gone.
    it("takes a ride by its name at the park, so no lookup goes first", async () => {
      const search = jest
        .spyOn(findAttractionsTool, "handler")
        .mockResolvedValue({ attractions: [{ id: TARON, name: "Taron" }] });
      const result = await add(["taron", RAIK.toUpperCase()]);
      expect(search.mock.calls[0][0]).toMatchObject({ query: "taron" });
      expect(result.action).toMatchObject({
        rides: [{ slug: "taron", name: "Taron" }],
      });
      expect(JSON.parse(result.content)).toMatchObject({
        added: ["Taron"],
        notAdded: [{ name: "Raik", reason: "maintenance" }],
      });
    });

    it("names a ride it cannot place and still adds the rest", async () => {
      jest
        .spyOn(findAttractionsTool, "handler")
        .mockResolvedValue({ attractions: [] });
      const result = await add(["Nirgendwo-Bahn", TARON]);
      expect(result.action).toMatchObject({
        rides: [{ slug: "taron", name: "Taron" }],
      });
      expect(JSON.parse(result.content)).toMatchObject({
        added: ["Taron"],
        notAdded: [{ name: "Nirgendwo-Bahn", reason: "not_found" }],
      });
    });

    it("hands two rides of one name back with their ids instead of a guess", async () => {
      jest.spyOn(findAttractionsTool, "handler").mockResolvedValue({
        attractions: [
          { id: TARON, name: "Winja", park: { name: "Phantasialand" } },
          { id: RAIK, name: "Winja", park: { name: "Phantasialand" } },
        ],
      });
      const result = await add(["Winja"]);
      expect(result.action).toBeUndefined();
      expect(JSON.parse(result.content)).toMatchObject({
        added: [],
        notAdded: [
          {
            name: "Winja",
            reason: "ambiguous",
            candidates: [{ id: TARON }, { id: RAIK }],
          },
        ],
      });
    });

    it("adds a ride named twice once", async () => {
      jest
        .spyOn(findAttractionsTool, "handler")
        .mockResolvedValue({ attractions: [{ id: TARON, name: "Taron" }] });
      const result = await add(["Taron", TARON]);
      expect(result.action).toMatchObject({
        rides: [{ slug: "taron", name: "Taron" }],
      });
      expect((result.action as { rides: unknown[] }).rides).toHaveLength(1);
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

describe("performancesOf (PAR-1355)", () => {
  const times = ["13:00", "14:00", "15:00", "16:00", "17:00"];

  it("gives a CLOSED ride on a schedule its next start instead of a reopening", () => {
    expect(
      performancesOf(
        { status: "CLOSED", performanceTimesToday: times },
        true,
        "13:37",
      ),
    ).toEqual({ times, next: "14:00" });
  });

  it("says none is left once the last has begun", () => {
    expect(
      performancesOf(
        { effectiveStatus: "OPERATING", performanceTimesToday: times },
        null,
        "17:30",
      ),
    ).toEqual({ times, next: null });
  });

  it("leaves a breakdown, works, a shut park and a ride without times alone", () => {
    const ride = { status: "CLOSED", performanceTimesToday: times };
    expect(
      performancesOf({ ...ride, status: "DOWN" }, true, "12:00"),
    ).toBeNull();
    expect(
      performancesOf({ ...ride, status: "REFURBISHMENT" }, true, "12:00"),
    ).toBeNull();
    expect(
      performancesOf(
        { ...ride, worksPeriod: { runningToday: true } },
        true,
        "12:00",
      ),
    ).toBeNull();
    expect(performancesOf(ride, false, "12:00")).toBeNull();
    expect(
      performancesOf(
        { status: "CLOSED", performanceTimesToday: null },
        true,
        "12:00",
      ),
    ).toBeNull();
  });
});

describe("places around the user and the way there", () => {
  // Phantasialand: Taron, the toilet beside it, and a visitor standing at Talocan.
  const entities = {
    latitude: "50.7997",
    longitude: "6.8812",
    attractions: [
      {
        id: "talocan",
        slug: "talocan",
        name: "Talocan",
        latitude: "50.7994214",
        longitude: "6.8811600",
      },
      {
        id: "taron",
        slug: "taron",
        name: "Taron",
        latitude: "50.7996591",
        longitude: "6.8829768",
      },
    ],
    shows: [],
    restaurants: [
      {
        id: "cocorico",
        name: "Restaurante Cocorico",
        latitude: "50.7992805",
        longitude: "6.8812974",
      },
    ],
  };
  const toilet = {
    id: "0f6f2b4e-1c1a-4c3e-9a51-6b1c2d3e4f50",
    type: "toilets",
    latitude: 50.7995,
    longitude: 6.8828,
    name: null,
    wheelchair: "yes",
    changingTable: true,
    fee: null,
  };
  const providers = (available = true) =>
    new Map<unknown, unknown>([
      [
        ParksService,
        {
          findById: jest.fn(async () => ({ ...park })),
          loadParkRelations: jest.fn(async (p: object) => ({
            ...p,
            ...entities,
          })),
        },
      ],
      [
        ParkIntegrationService,
        {
          buildIntegratedResponse: jest.fn(async () => ({
            status: "OPERATING",
            attractions: [
              {
                id: "talocan",
                name: "Talocan",
                status: "OPERATING",
                queues: [{ queueType: "STANDBY", waitTime: 10 }],
              },
              {
                id: "taron",
                name: "Taron",
                status: "OPERATING",
                queues: [{ queueType: "STANDBY", waitTime: 35 }],
              },
            ],
            restaurants: [{ id: "cocorico", name: "Restaurante Cocorico" }],
          })),
        },
      ],
      [
        FacilitiesService,
        {
          publicFor: jest.fn(async () => ({
            available,
            facilities: available ? [toilet] : [],
          })),
          parkIdOf: jest.fn(async (id: string) =>
            id === toilet.id ? PARK_ID : null,
          ),
        },
      ],
      [
        LocationService,
        {
          findNearby: jest.fn(async () => ({
            type: "in_park",
            data: { park: { id: PARK_ID } },
          })),
        },
      ],
    ]);
  const atTalocan = { lat: 50.7994, lng: 6.8812 };

  it("asks for the position when none came with the message", async () => {
    const { json } = await run(
      "find_nearby",
      { park: PARK_ID, fromMe: true },
      providers(),
    );
    expect(json).toMatchObject({ needsPosition: true });
  });

  it("lists what is around the user without handing over the position", async () => {
    const { json } = await run(
      "find_nearby",
      { park: PARK_ID, fromMe: true, kinds: ["rides", "toilets"] },
      providers(),
      "de",
      { location: atTalocan },
    );
    expect(json.from).toEqual({ fromTheUser: true });
    expect(json.rides.map((r: { id: string }) => r.id)).toEqual([
      "talocan",
      "taron",
    ]);
    expect(json.rides[0]).toMatchObject({ waitMinutes: 10, metres: 0 });
    expect(json.toilets[0]).toMatchObject({
      wheelchair: "yes",
      changingTable: true,
    });
    expect(json.credit.say).toContain("OpenStreetMap");
    expect(JSON.stringify(json)).not.toContain("50.7994,");
  });

  it("says toilets are not mapped yet where the park has none approved", async () => {
    const { json } = await run(
      "find_nearby",
      { park: PARK_ID, near: "Taron", kinds: ["toilets"] },
      providers(false),
    );
    expect(json.toilets).toMatchObject({ notKnown: true });
    expect(json.credit).toBeUndefined();
  });

  it("tells a user far from the park that they are not there", async () => {
    const { json } = await run(
      "find_nearby",
      { park: PARK_ID, fromMe: true },
      providers(),
      "de",
      { location: { lat: 52.52, lng: 13.405 } },
    );
    expect(json).toMatchObject({ notAtPark: true });
  });

  it("measures from the user to named rides", async () => {
    const { json } = await run(
      "get_walking_distance",
      { park: PARK_ID, fromMe: true, to: ["Taron"] },
      providers(),
      "de",
      { location: atTalocan },
    );
    expect(json.distances[0]).toMatchObject({ name: "Taron", metres: 130 });
  });

  it("draws the way to a ride and to a toilet, crediting OpenStreetMap for the toilet", async () => {
    const ride = await run(
      "show_card",
      { kind: "navigate", park: PARK_ID, to: "taron" },
      providers(),
    );
    expect(ride.action).toMatchObject({
      type: "card",
      card: {
        kind: "navigate",
        name: "Taron",
        place: "ride",
        latitude: 50.7996591,
        mapsUrl: expect.stringContaining("travelmode=walking"),
      },
    });
    expect(
      (ride.action as { card: { credit?: unknown } }).card.credit,
    ).toBeUndefined();
    const loo = await run(
      "show_card",
      { kind: "navigate", park: PARK_ID, to: toilet.id },
      providers(),
    );
    expect(loo.action).toMatchObject({
      card: {
        place: "toilets",
        credit: { text: "© OpenStreetMap contributors" },
      },
    });
  });

  it("finds the park of a toilet from its id when park is left out (the production failure)", async () => {
    const loo = await run(
      "show_card",
      { kind: "navigate", to: toilet.id, fromMe: true },
      providers(),
      "de",
      { location: atTalocan },
    );
    expect(loo.action).toMatchObject({
      card: { place: "toilets", park: expect.any(String) },
    });
  });

  it("takes the park the visitor stands in for a ride, show or restaurant without park", async () => {
    for (const [to, place] of [
      ["taron", "ride"],
      ["Restaurante Cocorico", "restaurant"],
    ] as const) {
      const out = await run(
        "show_card",
        { kind: "navigate", to, fromMe: true },
        providers(),
        "de",
        { location: atTalocan },
      );
      expect(out.action).toMatchObject({ card: { place } });
    }
  });

  it("names what is missing when neither park nor position says where", async () => {
    const onError = jest.fn();
    const out = await runAssistantTool(
      "show_card",
      { kind: "navigate", to: "taron" },
      context(providers()),
      onError,
    );
    expect(out.content).toContain("kind navigate needs park");
  });
});

describe("parkShutAt", () => {
  const day = { status: "OPERATING", openHour: 11, closeHour: 20 };

  it("tells the night and the morning from the open hours", () => {
    expect(parkShutAt(day, 22)).toBe("closed_for_the_night");
    expect(parkShutAt(day, 8)).toBe("not_open_yet");
    expect(parkShutAt(day, 14)).toBeNull();
  });

  it("keeps a day that runs past midnight open in its small hours", () => {
    const late = { status: "OPERATING", openHour: 10, closeHour: 1 };
    expect(parkShutAt(late, 0)).toBeNull();
    expect(parkShutAt(late, 23)).toBeNull();
    expect(parkShutAt(late, 7)).toBe("not_open_yet");
  });

  it("calls a closed day closed and says nothing without hours", () => {
    expect(
      parkShutAt({ status: "CLOSED", openHour: null, closeHour: null }, 12),
    ).toBe("closed_today");
    expect(
      parkShutAt({ status: "UNKNOWN", openHour: null, closeHour: null }, 12),
    ).toBeNull();
  });
});

describe("reopeningOf", () => {
  it("names the cold before a breakdown, with the forecast hour it reaches the ride's temperature", () => {
    expect(
      reopeningOf(
        {
          status: "DOWN",
          outage: { startedAt: "2026-11-23T09:00:00.000Z", signal: "down" },
          weatherClosure: {
            cause: "cold",
            probability: 0.91,
            temperatureC: 1.2,
            thresholdC: 4.5,
            outlook: "opens_when_warmer",
            expectedOpenAt: "2026-11-23T12:00:00.000Z",
          },
        },
        11,
      ),
    ).toEqual({
      reason: "weather",
      cause: "cold",
      probabilityPercent: 91,
      temperatureNowC: 1.2,
      usuallyRunsFromC: 4.5,
      likelyOpensAround: "2026-11-23T12:00:00.000Z",
    });
  });

  it("names snow without promising a time", () => {
    expect(
      reopeningOf(
        {
          status: "CLOSED",
          weatherClosure: {
            cause: "snow",
            probability: 0.93,
            temperatureC: -1,
            thresholdC: null,
            outlook: null,
            expectedOpenAt: null,
          },
        },
        1,
      ),
    ).toEqual({
      reason: "weather",
      cause: "snow",
      probabilityPercent: 93,
      temperatureNowC: -1,
    });
  });

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

  it("calls the park's maintenance status maintenance, with no window known", () => {
    expect(reopeningOf({ status: "REFURBISHMENT" }, 11)).toMatchObject({
      reason: "maintenance",
      windowKnown: false,
    });
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

describe("get_park_weather", () => {
  beforeEach(() => {
    jest.spyOn(Date, "now").mockReturnValue(Date.parse("2026-11-07T08:00:00Z"));
  });
  afterEach(() => jest.restoreAllMocks());

  const providers = (warnings: () => Promise<unknown[]>) =>
    new Map<unknown, unknown>([
      [ParksService, { findById: jest.fn(async () => ({ ...park })) }],
      [
        WeatherService,
        {
          getNowcast: jest.fn(async () => ({
            currentTemperatureC: 6,
            currentApparentTemperatureC: 2,
            currentHumidity: 91,
            currentlyRaining: true,
            currentRainIntensity: "moderate",
            rainStartsAt: null,
            rainEndsAt: "2026-11-07T10:15:00+01:00",
            thunderstormStartsAt: null,
            thunderstormEndsAt: null,
            hailStartsAt: null,
            hailEndsAt: null,
            stormStartsAt: "2026-11-07T13:00:00+01:00",
            stormEndsAt: null,
            currentWindSpeedKmh: 28,
            currentWindGustsKmh: 52,
            peakWindGustsKmh: 74,
            temperatureMinC: 4,
            temperatureMaxC: 9,
          })),
          getHourlyForecast: jest.fn(async () => [
            {
              time: "2026-11-07T06:00:00.000Z",
              temperature: 5,
              precipitation: 0.4,
              weatherCode: 61,
              windSpeed: 20,
              precipitationProbability: 70,
              windGusts: 45,
            },
            {
              time: "2026-11-07T09:00:00.000Z",
              temperature: 6,
              precipitation: 1.2,
              weatherCode: 63,
              windSpeed: 30,
              precipitationProbability: 90,
              windGusts: 74,
            },
          ]),
        },
      ],
      [WeatherWarningsService, { getActiveWarnings: jest.fn(warnings) }],
    ]);

  it("gives wind, gusts, the warnings in force and each hour's chance of rain", async () => {
    const { json } = await run(
      "get_park_weather",
      { park: PARK_ID },
      providers(async () => [
        {
          event: "STURMBÖEN",
          eventEn: "storm gusts",
          severity: "Moderate",
          onset: "2026-11-07T12:00:00.000Z",
          expires: "2026-11-07T20:00:00.000Z",
          headline: "Amtliche WARNUNG vor STURMBÖEN",
          headlineEn: "Official WARNING of STORM GUSTS",
          instruction: null,
          instructionEn: null,
        },
      ]),
    );
    expect(json.now).toMatchObject({
      humidityPercent: 91,
      rainIntensity: "moderate",
      // Park time, as every instant Parky reads.
      stormStartsAt: "2026-11-07 13:00",
      windKmh: 28,
      windGustsKmh: 52,
      peakWindGustsKmh: 74,
    });
    expect(json.warnings).toEqual([
      {
        event: "storm gusts",
        severity: "Moderate",
        from: "2026-11-07 13:00",
        until: "2026-11-07 21:00",
        headline: "Official WARNING of STORM GUSTS",
        instruction: null,
      },
    ]);
    // The hour that has passed is left out, the next one kept.
    expect(json.nextHours).toEqual([
      {
        time: "2026-11-07 10:00",
        temperatureC: 6,
        precipitationMm: 1.2,
        precipitationChancePercent: 90,
        weatherCode: 63,
        windKmh: 30,
        windGustsKmh: 74,
      },
    ]);
  });

  it("answers with the weather when the warnings cannot be read", async () => {
    const { json } = await run(
      "get_park_weather",
      { park: PARK_ID },
      providers(async () => {
        throw new Error("warnings down");
      }),
    );
    expect(json.warnings).toEqual([]);
    expect(json.now.temperatureC).toBe(6);
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
    // Park time: 10:00 in Brühl, whatever the visitor's clock (UTC here) says.
    expect(json.today).toEqual({
      opensAt: "2026-11-07 10:00",
      opensAtConfidence: "high",
      runsOnWeekdays: [1, 2, 3, 4, 5, 6, 7],
    });
  });

  it("gives a ride on set times its next start, no reopening and no best visit times (PAR-1358)", async () => {
    jest.spyOn(getAttractionTool, "handler").mockResolvedValue({
      id: RIDE,
      name: "Die 3 Mausketiere",
      status: "CLOSED",
      park: { id: PARK_ID, name: "Phantasialand" },
      performanceTimesToday: ["23:59"],
      bestVisitTimesToday: [{ time: "2026-11-07T11:45:00.000Z" }],
      facts: {},
    });
    const { json } = await run("get_ride", { rideId: RIDE }, providers());
    expect(json.today.performances).toEqual({
      times: ["23:59"],
      next: "23:59",
    });
    expect(json.reopening).toBeUndefined();
    expect(json.bestVisitTimesToday).toBeUndefined();
  });

  it("gives the day's weather with its units, never the legacy rainChance", async () => {
    const wet = {
      ...plan,
      context: {
        ...plan.context,
        weather: {
          basis: "forecast",
          condition: "Drizzle, dense intensity",
          icon: 55,
          tempMin: -1.5,
          tempMax: 2.6,
          rainChance: 12.6,
          precipitationMm: 12.6,
          precipitationProbability: 85,
          precipitationHours: 11,
          windMax: 24,
          windGustsMax: 48,
        },
      },
    };
    const { json } = await run(
      "get_park_day",
      { park: PARK_ID, date: "2026-11-07" },
      new Map<unknown, unknown>([
        ...providers(),
        [PlanDayService, { buildPlanDay: jest.fn(async () => wet) }],
      ]),
    );
    expect(json.weather).toEqual({
      basis: "forecast",
      condition: "Drizzle, dense intensity",
      weatherCode: 55,
      tempMin: -1.5,
      tempMax: 2.6,
      precipitationMm: 12.6,
      precipitationChancePercent: 85,
      precipitationHours: 11,
      windMaxKmh: 24,
      windGustsMaxKmh: 48,
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
      {
        id: "55555555-5555-4555-8555-555555555555",
        name: "Taron",
        minimumHeightCm: 130,
      },
    ]);
  });

  it("labels a day past the published hours as park.fan's estimate", async () => {
    const { json } = await run(
      "get_park_day",
      { park: PARK_ID, date: "2026-11-07" },
      new Map<unknown, unknown>([
        ...providers(),
        [
          ParksService,
          {
            findById: jest.fn(async () => ({ ...park })),
            getOperatingDateRange: jest.fn(async () => ({
              minDate: "2026-03-01",
              maxDate: "2026-11-01",
            })),
          },
        ],
      ]),
    );
    expect(json.hoursPublished).toBe(false);
    expect(json.statusWhy).toContain("2026-11-01");
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
      opens: "2026-11-07 09:00",
      closes: "2026-11-07 23:00",
      rides: [
        {
          name: "Taron",
          opensAt: "2026-11-07 10:00",
          predictedPeakWaitMinutes: 45,
          shortestAt: { at: "2026-11-07 21:00", waitMinutes: 10 },
        },
      ],
      // 00:30 belongs to the night after.
      shows: [{ times: ["2026-11-07 22:00", "2026-11-08 00:30"] }],
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
