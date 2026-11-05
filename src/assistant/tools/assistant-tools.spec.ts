import { DataSource } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { RideProfileService } from "../../attractions/services/ride-profile.service";
import { getParkTool } from "../../mcp/tools/get-park.tool";
import { BestDaysService } from "../../parks/services/best-days.service";

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
import { foldText, runAssistantTool } from "./assistant-tools";
import {
  calendarCard,
  localizePageUrls,
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
      },
    });
    const providers = new Map<unknown, unknown>([
      [
        RideProfileService,
        profiles({ a: ["launch-coaster"], b: ["rapids"], c: ["boomerang"] }),
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
    expect(action).toMatchObject({
      type: "card",
      card: { kind: "rides", rides: [{ name: "Raik" }, { name: "Taron" }] },
    });
    expect(json.shown).toBe("2 coasters of Phantasialand");
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

describe("calendar and tiles cards", () => {
  const forecast: ForecastSummary = {
    park: {
      name: "Phantasialand",
      pageUrl: `${getFrontendUrl()}/parks/europe/germany/bruehl/phantasialand`,
    },
    scheduleCoverage: { from: "2026-03-01", to: "2026-11-06" },
    days: [
      {
        date: "2026-11-05",
        status: "OPERATING",
        opens: "2026-11-05T09:00+01:00",
        closes: "2026-11-05T18:00+01:00",
        crowdLevel: "high",
        expectedPeakWaitMinutes: 52.4,
        schoolVacation: true,
        weather: {
          basis: "forecast",
          condition: "Rain",
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
        opens: "2026-11-05T09:00+01:00",
        closes: "2026-11-05T18:00+01:00",
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
      opens: "2026-11-05T09:00+01:00",
      closes: "2026-11-05T18:00+01:00",
      weather: {
        condition: "Rain",
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
        { opens: "2026-11-05T09:00+01:00", closes: "2026-11-05T18:00+01:00" },
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
    });
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
