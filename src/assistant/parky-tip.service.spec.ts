import type { ModuleRef } from "@nestjs/core";
import type { Redis } from "ioredis";
import {
  addIsoDays,
  eachIsoDate,
  formatInParkTimezone,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import { monthBounds, monthOf } from "../trip-regions/last-year-snapshot";
import type { Park } from "../parks/entities/park.entity";
import type { AssistantSettingsService } from "./assistant-settings.service";
import {
  ParkyTipService,
  parkyVerdict,
  validParkyTip,
} from "./parky-tip.service";
import {
  PARKY_TIP_PROMPT_VERSION,
  parkyTipInput,
  parkyTipSystemPrompt,
} from "./parky-tip-prompt";
import type { CalendarDay } from "../parks/dto/integrated-calendar.dto";

const create = jest.fn();
jest.mock("@anthropic-ai/sdk", () => {
  class Anthropic {
    messages = { create };
  }
  return { __esModule: true, default: Anthropic };
});

describe("parkyVerdict", () => {
  it.each([
    ["very_low", "good"],
    ["low", "good"],
    ["moderate", "meh"],
    ["high", "bad"],
    ["very_high", "bad"],
    ["extreme", "bad"],
    ["unknown", null],
    ["closed", null],
  ] as const)("%s is %s", (level, verdict) => {
    expect(parkyVerdict(level)).toBe(verdict);
  });
});

describe("validParkyTip", () => {
  it("keeps one plain sentence and drops wrapping quotes", () => {
    expect(
      validParkyTip(' "Samstag wird voll, nimm lieber den Sonntag!" '),
    ).toBe("Samstag wird voll, nimm lieber den Sonntag!");
  });

  it.each([
    ["empty", "  "],
    ["two lines", "Ein Satz.\nNoch einer."],
    ["a link", "Schau auf https://park.fan vorbei."],
    ["markdown", "Das wird **voll**."],
    ["too long", "a".repeat(161)],
  ])("refuses %s", (_name, text) => {
    expect(validParkyTip(text)).toBeNull();
  });

  it("counts an emoji as one character", () => {
    expect(validParkyTip(`${"a".repeat(159)}🎢`)).not.toBeNull();
  });
});

describe("parkyTipInput", () => {
  const day = (date: string, extra: Partial<CalendarDay> = {}) =>
    ({
      date,
      status: "OPERATING",
      crowdLevel: "high",
      ...extra,
    }) as CalendarDay;

  it("tells today's level so far beside its forecast, on today's line only", () => {
    const input = parkyTipInput(
      "Phantasialand",
      "Europe/Berlin",
      "2026-11-17",
      "2026-11-17",
      [
        day("2026-11-17", { todayCrowdLevel: "very_low" }),
        day("2026-11-18", { todayCrowdLevel: "low" }),
      ],
    );
    expect(input).toContain(
      "CHOSEN DAY Tuesday 2026-11-17; crowd level high; so far today: very low",
    );
    expect(input).toMatch(/^Wednesday 2026-11-18; crowd level high$/m);
    expect(input.match(/so far today/g)).toHaveLength(1);
  });

  it("leaves today's level so far out of the tip for another day", () => {
    const input = parkyTipInput(
      "Phantasialand",
      "Europe/Berlin",
      "2026-11-17",
      "2026-11-19",
      [day("2026-11-17", { todayCrowdLevel: "very_low" }), day("2026-11-19")],
    );
    expect(input).toMatch(/^Tuesday 2026-11-17; crowd level high$/m);
    expect(input).not.toContain("so far");
  });

  it("says nothing so far on a closed today", () => {
    const input = parkyTipInput(
      "Phantasialand",
      "Europe/Berlin",
      "2026-11-17",
      "2026-11-18",
      [
        day("2026-11-17", {
          status: "CLOSED",
          crowdLevel: "closed",
          todayCrowdLevel: "low",
        }),
      ],
    );
    expect(input).not.toContain("so far");
  });

  it("gives the day's precipitation in millimetres, never as a chance of rain", () => {
    const input = parkyTipInput(
      "Phantasialand",
      "Europe/Berlin",
      "2026-11-17",
      "2026-11-20",
      [
        day("2026-11-20", {
          weather: {
            basis: "forecast",
            condition: "Rain showers, moderate intensity",
            icon: 81,
            tempMin: 7,
            tempMax: 9.8,
            rainChance: 47.5,
            precipitationMm: 47.5,
          },
        } as Partial<CalendarDay>),
      ],
    );
    expect(input).toContain(
      "weather Rain showers, moderate intensity, 7-10 °C, 47.5 mm precipitation over the day",
    );
    expect(input).not.toContain("%");
  });

  it("adds the chance of rain, its hours and the gusts where the forecast has them", () => {
    const input = parkyTipInput(
      "Phantasialand",
      "Europe/Berlin",
      "2026-11-17",
      "2026-11-20",
      [
        day("2026-11-20", {
          weather: {
            basis: "forecast",
            condition: "Rain showers, moderate intensity",
            icon: 81,
            tempMin: 7,
            tempMax: 9.8,
            rainChance: 47.5,
            precipitationMm: 47.5,
            precipitationProbability: 96,
            precipitationHours: 14,
            windGustsMax: 61.2,
          },
        } as Partial<CalendarDay>),
      ],
    );
    expect(input).toContain(
      "47.5 mm precipitation over the day, 96 % chance of rain, 14 hours with precipitation, gusts up to 61 km/h",
    );
  });

  it("says nothing about rain when no precipitation was measured", () => {
    const input = parkyTipInput(
      "Phantasialand",
      "Europe/Berlin",
      "2026-11-17",
      "2026-11-20",
      [
        day("2026-11-20", {
          weather: {
            basis: "forecast",
            condition: "Overcast",
            icon: 3,
            tempMin: 2,
            tempMax: 6,
            rainChance: 0,
          },
        } as Partial<CalendarDay>),
      ],
    );
    expect(input).toMatch(/weather Overcast, 2-6 °C$/m);
  });

  it("tells the model millimetres are an amount and only the chance is a percentage", () => {
    expect(parkyTipSystemPrompt("de")).toMatch(
      /never turn millimetres into a percentage; the chance may be said as given/,
    );
  });

  it("asks for both levels when today so far and its forecast differ", () => {
    expect(parkyTipSystemPrompt("de")).toMatch(
      /so far.*When the chosen day is today and the two differ, say both honestly/,
    );
  });
});

describe("ParkyTipService", () => {
  const park = {
    id: "park-1",
    name: "Phantasialand",
    timezone: "Europe/Berlin",
  } as Park;
  const today = getCurrentDateInTimezone(park.timezone);
  const date = addIsoDays(today, 5);

  let store: Map<string, string>;
  let redis: Redis;
  let settings: jest.Mocked<
    Pick<
      AssistantSettingsService,
      "publicConfig" | "apiKey" | "budgetSpent" | "addTokens"
    >
  >;
  let buildCalendarResponse: jest.Mock;
  let service: ParkyTipService;

  beforeEach(() => {
    create.mockReset().mockResolvedValue({
      stop_reason: "end_turn",
      usage: { input_tokens: 300, output_tokens: 40 },
      content: [{ type: "text", text: "Ruhiger Tag, perfekt für Taron! 🎢" }],
    });
    store = new Map();
    redis = {
      get: jest.fn((key: string) => Promise.resolve(store.get(key) ?? null)),
      set: jest.fn((key: string, value: string) => {
        store.set(key, value);
        return Promise.resolve("OK");
      }),
      mget: jest.fn((...keys: string[]) =>
        Promise.resolve(keys.map((key) => store.get(key) ?? null)),
      ),
    } as unknown as Redis;
    settings = {
      publicConfig: jest
        .fn()
        .mockResolvedValue({ enabled: true, rolloutPercent: 100 }),
      apiKey: jest.fn().mockResolvedValue("sk-ant-test"),
      budgetSpent: jest.fn().mockResolvedValue(false),
      addTokens: jest.fn().mockResolvedValue(undefined),
    };
    buildCalendarResponse = jest.fn().mockResolvedValue({
      days: [
        { date, status: "OPERATING", crowdLevel: "low", isHoliday: false },
        {
          date: addIsoDays(date, 1),
          status: "OPERATING",
          crowdLevel: "high",
          isHoliday: false,
        },
      ],
    });
    const moduleRef = {
      get: () => ({ buildCalendarResponse }),
    } as unknown as ModuleRef;
    service = new ParkyTipService(
      moduleRef,
      settings as unknown as AssistantSettingsService,
      redis,
    );
  });

  it("writes the tip once, counts its tokens and serves the cache after", async () => {
    const first = await service.tip(park, date, "de");
    expect(first).toEqual({
      date,
      verdict: "good",
      crowdLevel: "low",
      text: "Ruhiger Tag, perfekt für Taron! 🎢",
      promptVersion: PARKY_TIP_PROMPT_VERSION,
    });
    expect(settings.addTokens).toHaveBeenCalledWith(340);

    const second = await service.tip(park, date, "de");
    expect(second).toEqual(first);
    expect(create).toHaveBeenCalledTimes(1);
    // The calendar is read on every request: the day's level picks the cache entry.
    expect(buildCalendarResponse).toHaveBeenCalledTimes(2);
  });

  it("writes a new sentence once the forecast moves the day's level", async () => {
    await service.tip(park, date, "de");
    buildCalendarResponse.mockResolvedValue({
      days: [{ date, status: "OPERATING", crowdLevel: "very_low" }],
    });
    create.mockResolvedValue({
      stop_reason: "end_turn",
      usage: { input_tokens: 300, output_tokens: 40 },
      content: [{ type: "text", text: "Fast leer, Taron ohne Anstehen!" }],
    });

    const moved = await service.tip(park, date, "de");

    expect(create).toHaveBeenCalledTimes(2);
    expect(moved).toMatchObject({
      verdict: "good",
      crowdLevel: "very_low",
      text: "Fast leer, Taron ohne Anstehen!",
    });
  });

  it("counts the day from the park's today, which keys the cache", async () => {
    await service.tip(park, date, "de");

    const [request] = create.mock.calls[0] as [
      { system: string; messages: { content: string }[] },
    ];
    expect(request.messages[0].content).toMatch(
      new RegExp(`^TODAY at the park: [A-Z][a-z]+day ${today}$`, "m"),
    );
    expect(request.system).toContain("tomorrow");
    expect([...store.keys()]).toEqual([
      expect.stringMatching(
        new RegExp(
          `^parky-tip:v${PARKY_TIP_PROMPT_VERSION}:park-1:${today}:${date}:[\\w-]{16}:de$`,
        ),
      ),
    ]);
  });

  it("writes from the today its caller read", async () => {
    const tomorrow = addIsoDays(today, 1);
    await service.tip(park, date, "de", tomorrow);

    const [request] = create.mock.calls[0] as [
      { messages: { content: string }[] },
    ];
    expect(request.messages[0].content).toMatch(
      new RegExp(`^TODAY at the park: [A-Z][a-z]+day ${tomorrow}$`, "m"),
    );
    expect([...store.keys()][0]).toContain(`:${tomorrow}:${date}:`);
  });

  it("writes a new sentence once a neighbouring day it reads moves", async () => {
    await service.tip(park, date, "de");
    // The day after went from busy to quiet: a sentence steering away from it is wrong now.
    buildCalendarResponse.mockResolvedValue({
      days: [
        { date, status: "OPERATING", crowdLevel: "low" },
        {
          date: addIsoDays(date, 1),
          status: "OPERATING",
          crowdLevel: "very_low",
        },
      ],
    });

    await service.tip(park, date, "de");

    expect(create).toHaveBeenCalledTimes(2);
    expect(store.size).toBe(2);
  });

  it("writes a new sentence once a neighbour closes or opens", async () => {
    await service.tip(park, date, "de");
    buildCalendarResponse.mockResolvedValue({
      days: [
        { date, status: "OPERATING", crowdLevel: "low" },
        { date: addIsoDays(date, 1), status: "CLOSED", crowdLevel: "closed" },
      ],
    });

    await service.tip(park, date, "de");

    expect(create).toHaveBeenCalledTimes(2);
  });

  it("keeps the sentence when only the weather moves", async () => {
    const weather = (condition: string) => ({
      basis: "forecast",
      condition,
      tempMin: 3,
      tempMax: 9,
      rainChance: 20,
    });
    buildCalendarResponse.mockResolvedValue({
      days: [
        {
          date,
          status: "OPERATING",
          crowdLevel: "low",
          weather: weather("cloudy"),
        },
      ],
    });
    await service.tip(park, date, "de");
    buildCalendarResponse.mockResolvedValue({
      days: [
        {
          date,
          status: "OPERATING",
          crowdLevel: "low",
          weather: weather("rain"),
        },
      ],
    });

    await service.tip(park, date, "de");

    expect(create).toHaveBeenCalledTimes(1);
  });

  it("writes today's sentence again once the level so far moves", async () => {
    const day = (soFar: string) => ({
      days: [
        {
          date: today,
          status: "OPERATING",
          crowdLevel: "high",
          todayCrowdLevel: soFar,
        },
      ],
    });
    buildCalendarResponse.mockResolvedValue(day("very_low"));
    const first = await service.tip(park, today, "de");
    const [request] = create.mock.calls[0] as [
      { messages: { content: string }[] },
    ];
    expect(request.messages[0].content).toContain("so far today: very low");
    expect(first?.todayCrowdLevel).toBe("very_low");

    buildCalendarResponse.mockResolvedValue(day("high"));
    const moved = await service.tip(park, today, "de");

    expect(create).toHaveBeenCalledTimes(2);
    expect(moved?.todayCrowdLevel).toBe("high");
    // Kept with the sentence, so a cache hit names the level it was written from too.
    expect((await service.tip(park, today, "de"))?.todayCrowdLevel).toBe(
      "high",
    );
  });

  it("names no level so far on another day's tip, nor on today's before it has one", async () => {
    const tomorrow = addIsoDays(today, 1);
    buildCalendarResponse.mockResolvedValue({
      days: [
        {
          date: today,
          status: "OPERATING",
          crowdLevel: "high",
          todayCrowdLevel: "low",
        },
        { date: tomorrow, status: "OPERATING", crowdLevel: "low" },
      ],
    });
    const other = await service.tip(park, tomorrow, "de");
    expect(other).not.toBeNull();
    expect(other).not.toHaveProperty("todayCrowdLevel");

    buildCalendarResponse.mockResolvedValue({
      days: [{ date: today, status: "OPERATING", crowdLevel: "high" }],
    });
    const early = await service.tip(park, today, "de");
    expect(early).not.toBeNull();
    expect(early).not.toHaveProperty("todayCrowdLevel");
    expect(JSON.parse([...store.values()].pop()!)).not.toHaveProperty(
      "todayCrowdLevel",
    );
  });

  it("keeps the tip of a day around today when only today's level so far moves", async () => {
    const tomorrow = addIsoDays(today, 1);
    const days = (soFar: string) => ({
      days: [
        {
          date: today,
          status: "OPERATING",
          crowdLevel: "high",
          todayCrowdLevel: soFar,
        },
        { date: tomorrow, status: "OPERATING", crowdLevel: "low" },
      ],
    });
    buildCalendarResponse.mockResolvedValue(days("very_low"));
    await service.tip(park, tomorrow, "de");
    const [request] = create.mock.calls[0] as [
      { messages: { content: string }[] },
    ];
    expect(request.messages[0].content).not.toContain("so far");

    buildCalendarResponse.mockResolvedValue(days("high"));
    await service.tip(park, tomorrow, "de");

    expect(create).toHaveBeenCalledTimes(1);
  });

  it("reads the whole months around the days it sees and keeps only those days", async () => {
    // The last day of a month: its window ends in the next one, so both months, whole.
    const late = monthBounds(monthOf(addIsoDays(today, 40))).last;
    const { first } = monthBounds(monthOf(late));
    const { last } = monthBounds(monthOf(addIsoDays(late, 1)));
    buildCalendarResponse.mockResolvedValue({
      days: eachIsoDate(first, last).map((d) => ({
        date: d,
        status: "OPERATING",
        crowdLevel: "low",
      })),
    });

    await service.tip(park, late, "de");

    const [, fromDate, toDate, includeHourly] = buildCalendarResponse.mock
      .calls[0] as [Park, Date, Date, string];
    expect(formatInParkTimezone(fromDate, park.timezone)).toBe(first);
    expect(formatInParkTimezone(toDate, park.timezone)).toBe(last);
    expect(includeHourly).toBe("none");
    const [request] = create.mock.calls[0] as [
      { messages: { content: string }[] },
    ];
    const lines = request.messages[0].content
      .split("\n")
      .filter((line) => /\d{4}-\d{2}-\d{2}; /.test(line));
    expect(lines.map((line) => /(\d{4}-\d{2}-\d{2});/.exec(line)?.[1])).toEqual(
      [-3, -2, -1, 0, 1, 2, 3].map((n) => addIsoDays(late, n)),
    );
  });

  it("reads from the month of today on when the window would start before it", async () => {
    await service.tip(park, today, "de");

    const [, fromDate] = buildCalendarResponse.mock.calls[0] as [Park, Date];
    expect(formatInParkTimezone(fromDate, park.timezone)).toBe(
      `${today.slice(0, 7)}-01`,
    );
  });

  it("hands the chat the sentence it keeps and the facts it read, from one calendar read", async () => {
    const shown = await service.tip(park, date, "de");
    buildCalendarResponse.mockClear();

    const day = await service.dayTip(park, date, "de");

    expect(day).toEqual({
      text: shown?.text,
      facts: expect.stringContaining("CHOSEN DAY"),
    });
    expect(day?.facts).toContain("TODAY at the park:");
    expect(create).toHaveBeenCalledTimes(1);
    expect(buildCalendarResponse).toHaveBeenCalledTimes(1);
  });

  it("never writes a tip for the chat: a miss hands it the facts without a sentence", async () => {
    expect(await service.dayTip(park, date, "fr")).toEqual({
      text: null,
      facts: expect.stringContaining("CHOSEN DAY"),
    });
    expect(create).not.toHaveBeenCalled();
    expect(settings.apiKey).not.toHaveBeenCalled();
    expect(settings.budgetSpent).not.toHaveBeenCalled();
    expect(settings.addTokens).not.toHaveBeenCalled();
    expect(store.size).toBe(0);
  });

  it("hands the chat no sentence for a refused tip, while switched off or outside the horizon", async () => {
    create.mockResolvedValue({
      stop_reason: "end_turn",
      usage: { input_tokens: 300, output_tokens: 40 },
      content: [{ type: "text", text: "Mehr auf **park.fan**" }],
    });
    expect(await service.tip(park, date, "de")).toBeNull();
    expect((await service.dayTip(park, date, "de"))?.text).toBeNull();

    create.mockResolvedValue({
      stop_reason: "end_turn",
      usage: { input_tokens: 300, output_tokens: 40 },
      content: [{ type: "text", text: "Ruhiger Tag!" }],
    });
    await service.tip(park, date, "fr");
    settings.publicConfig.mockResolvedValue({
      enabled: false,
      rolloutPercent: 0,
    });
    expect((await service.dayTip(park, date, "fr"))?.text).toBeNull();
    expect(await service.dayTip(park, addIsoDays(today, -1), "de")).toBeNull();
    expect(create).toHaveBeenCalledTimes(2);
  });

  it("makes one model call for concurrent requests", async () => {
    await Promise.all([
      service.tip(park, date, "de"),
      service.tip(park, date, "de"),
    ]);
    expect(create).toHaveBeenCalledTimes(1);
  });

  it("keeps a refused sentence as nothing, without asking again", async () => {
    create.mockResolvedValue({
      stop_reason: "end_turn",
      usage: { input_tokens: 300, output_tokens: 40 },
      content: [
        { type: "text", text: "Mehr auf [park.fan](https://park.fan)" },
      ],
    });
    expect(await service.tip(park, date, "de")).toBeNull();
    expect(await service.tip(park, date, "de")).toBeNull();
    expect(create).toHaveBeenCalledTimes(1);
  });

  it("says nothing on a closed or unrated day, past the horizon or while switched off", async () => {
    buildCalendarResponse.mockResolvedValue({
      days: [{ date, status: "CLOSED", crowdLevel: "closed" }],
    });
    expect(await service.tip(park, date, "de")).toBeNull();
    buildCalendarResponse.mockResolvedValue({
      days: [{ date, status: "OPERATING", crowdLevel: "unknown" }],
    });
    expect(await service.tip(park, date, "de")).toBeNull();
    expect(store.size).toBe(0);
    expect(await service.tip(park, addIsoDays(today, -1), "de")).toBeNull();
    expect(await service.tip(park, addIsoDays(today, 91), "de")).toBeNull();
    settings.publicConfig.mockResolvedValue({
      enabled: false,
      rolloutPercent: 0,
    });
    expect(await service.tip(park, addIsoDays(today, 2), "de")).toBeNull();
    expect(create).not.toHaveBeenCalled();
  });

  it("does not cache a spent budget", async () => {
    settings.budgetSpent.mockResolvedValue(true);
    expect(await service.tip(park, date, "de")).toBeNull();
    settings.budgetSpent.mockResolvedValue(false);
    expect(await service.tip(park, date, "de")).not.toBeNull();
  });
});
