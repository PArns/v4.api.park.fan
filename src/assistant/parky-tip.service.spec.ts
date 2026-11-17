import type { ModuleRef } from "@nestjs/core";
import type { Redis } from "ioredis";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import type { Park } from "../parks/entities/park.entity";
import type { AssistantSettingsService } from "./assistant-settings.service";
import {
  ParkyTipService,
  parkyVerdict,
  validParkyTip,
} from "./parky-tip.service";
import { PARKY_TIP_PROMPT_VERSION } from "./parky-tip-prompt";

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
      text: "Ruhiger Tag, perfekt für Taron! 🎢",
      promptVersion: PARKY_TIP_PROMPT_VERSION,
    });
    expect(settings.addTokens).toHaveBeenCalledWith(340);

    const second = await service.tip(park, date, "de");
    expect(second).toEqual(first);
    expect(create).toHaveBeenCalledTimes(1);
    expect(buildCalendarResponse).toHaveBeenCalledTimes(1);
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
      `parky-tip:v${PARKY_TIP_PROMPT_VERSION}:park-1:${today}:${date}:de`,
    ]);
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

  it("says nothing on a closed day, past the horizon or while switched off", async () => {
    buildCalendarResponse.mockResolvedValue({
      days: [{ date, status: "CLOSED", crowdLevel: "closed" }],
    });
    expect(await service.tip(park, date, "de")).toBeNull();
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
