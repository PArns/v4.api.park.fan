import type { Request, Response } from "express";
import type { ModuleRef } from "@nestjs/core";
import { ApiKeyService } from "../api-keys/api-key.service";
import { AssistantController } from "./assistant.controller";
import { AssistantRateLimitService } from "./assistant-rate-limit.service";
import type { AssistantChatService } from "./assistant-chat.service";
import type { AssistantChallengeService } from "./assistant-challenge.service";
import type { AssistantRecordsService } from "./assistant-records.service";
import type { AssistantSettingsService } from "./assistant-settings.service";

/** Just enough Redis for the limiter: counters with a lifetime. */
function fakeRedis() {
  const values = new Map<string, number>();
  const expires = new Map<string, number>();
  return {
    incr: (key: string) => {
      values.set(key, (values.get(key) ?? 0) + 1);
      return Promise.resolve(values.get(key));
    },
    expire: (key: string, seconds: number) => {
      if (!expires.has(key)) expires.set(key, seconds);
      return Promise.resolve(1);
    },
    set: (key: string, _value: string, _ex: string, seconds: number) => {
      values.set(key, 1);
      expires.set(key, seconds);
      return Promise.resolve("OK");
    },
    del: (key: string) => {
      values.delete(key);
      expires.delete(key);
      return Promise.resolve(1);
    },
    ttl: (key: string) =>
      Promise.resolve(values.has(key) ? (expires.get(key) ?? -1) : -2),
  };
}

function fakeResponse() {
  const sent: {
    status?: number;
    headers: Record<string, string>;
    body?: unknown;
  } = { headers: {} };
  // A stream is opened, written and ended only; what it carries is the chat service's test.
  const response = {
    writableEnded: false,
    flushHeaders: () => undefined,
    on: () => response,
    write: () => true,
    end: () => {
      response.writableEnded = true;
    },
    status: (code: number) => {
      sent.status = code;
      return response;
    },
    setHeader: (name: string, value: string) => {
      sent.headers[name] = value;
      return response;
    },
    json: (body: unknown) => {
      sent.body = body;
      return response;
    },
  };
  return { response: response as unknown as Response, sent };
}

describe("AssistantController.ask, junk and pauses", () => {
  function setup() {
    const rateLimit = new AssistantRateLimitService(fakeRedis() as never);
    // The short window is tested in its own right; here every question passes it.
    jest
      .spyOn(rateLimit, "check")
      .mockResolvedValue({ allowed: true, retryAfterSeconds: 0 });
    const chat = {
      prepare: jest.fn().mockResolvedValue("off"),
    } as unknown as AssistantChatService;
    const challenge = {
      session: () => "session-1",
    } as unknown as AssistantChallengeService;
    const controller = new AssistantController(
      {} as AssistantSettingsService,
      chat,
      {} as AssistantRecordsService,
      rateLimit,
      challenge,
      {} as ModuleRef,
    );
    const ask = async (message: string) => {
      const { response, sent } = fakeResponse();
      await controller.ask(
        { locale: "de", pagePath: "/de", message, sessionToken: "t" },
        { headers: {}, socket: {} } as unknown as Request,
        response,
      );
      return sent;
    };
    return { ask, chat };
  }

  it("answers junk without the model, and pauses the third in a row", async () => {
    const { ask, chat } = setup();
    expect((await ask("???")).status).toBe(422);
    expect((await ask("sdfghjk")).status).toBe(422);
    const third = await ask("aaaaaa");
    expect(third.status).toBe(429);
    expect(third.headers["Retry-After"]).toBe("600");
    // Paused: even a real question waits now.
    expect((await ask("Wartezeit Taron?")).status).toBe(429);
    expect(chat.prepare).not.toHaveBeenCalled();
  });

  it("counts junk only in a row: a real question in between starts over", async () => {
    const { ask, chat } = setup();
    await ask("???");
    await ask("!!!");
    await ask("Ist Phantasialand offen?");
    expect(chat.prepare).toHaveBeenCalledTimes(1);
    expect((await ask("???")).status).toBe(422);
    expect((await ask("???")).status).toBe(422);
  });
});

describe("AssistantController.ask, after Parky ended a chat", () => {
  function setup(prepared: unknown = "off") {
    const rateLimit = new AssistantRateLimitService(fakeRedis() as never);
    jest
      .spyOn(rateLimit, "check")
      .mockResolvedValue({ allowed: true, retryAfterSeconds: 0 });
    const chat = {
      prepare: jest.fn().mockResolvedValue(prepared),
    } as unknown as AssistantChatService;
    const controller = new AssistantController(
      {} as AssistantSettingsService,
      chat,
      {} as AssistantRecordsService,
      rateLimit,
      { session: () => "session-1" } as unknown as AssistantChallengeService,
      {} as ModuleRef,
    );
    const ask = async () => {
      const { response, sent } = fakeResponse();
      await controller.ask(
        { locale: "de", pagePath: "/de", message: "Hallo", sessionToken: "t" },
        { headers: {}, socket: {} } as unknown as Request,
        response,
      );
      return sent;
    };
    return { ask, chat, rateLimit };
  }

  it("refuses the session with 403 chat_ended for the cooldown, without asking the model", async () => {
    const { ask, chat, rateLimit } = setup();
    await rateLimit.endChats("session-1", 15 * 60);
    const refused = await ask();
    expect(refused.status).toBe(403);
    expect(refused.body).toEqual({
      statusCode: 403,
      message: "chat_ended",
      code: "chat_ended",
      retryAfterSeconds: 900,
    });
    expect(refused.headers["Retry-After"]).toBe("900");
    expect(chat.prepare).not.toHaveBeenCalled();
  });

  it("asks again once the cooldown is over, and keeps the ended chat itself closed", async () => {
    const { ask, chat } = setup("ended");
    const refused = await ask();
    expect(chat.prepare).toHaveBeenCalledTimes(1);
    expect(refused.status).toBe(403);
    expect(refused.body).toEqual({
      statusCode: 403,
      message: "chat_ended",
      code: "chat_ended",
    });
    expect(refused.headers["Retry-After"]).toBeUndefined();
  });
});

describe("AssistantController.ask, the short window and Parky's offers", () => {
  const CHAT = "5d0f6c1e-3b2a-4c8d-9e7f-1a2b3c4d5e6f";

  function setup(chips: readonly string[]) {
    const rateLimit = new AssistantRateLimitService(fakeRedis() as never);
    const chat = {
      prepare: jest.fn((input: { message: string }) =>
        Promise.resolve({ answersOffer: chips.includes(input.message) }),
      ),
      answer: jest.fn().mockResolvedValue(undefined),
    } as unknown as AssistantChatService;
    const controller = new AssistantController(
      {} as AssistantSettingsService,
      chat,
      {} as AssistantRecordsService,
      rateLimit,
      { session: () => "session-1" } as unknown as AssistantChallengeService,
      {} as ModuleRef,
    );
    const ask = async (message: string, conversationId?: string) => {
      const { response, sent } = fakeResponse();
      await controller.ask(
        {
          locale: "de",
          pagePath: "/de",
          message,
          sessionToken: "t",
          ...(conversationId && { conversationId }),
        },
        { headers: {}, socket: {} } as unknown as Request,
        response,
      );
      return sent.status;
    };
    return { ask };
  }

  it("lets an interview go on chip after chip, and still holds typed rapid fire", async () => {
    const chips = [
      "Morgen",
      "Ich bin noch unsicher",
      "Europa-Park bitte",
      "Taron zuerst",
    ];
    const { ask } = setup(chips);
    // The interview's first question opens the chat and counts; every tap after it is a reply
    // Parky offered, which the window lets through however fast it comes.
    expect(await ask("Plan meinen Parktag mit mir, Frage für Frage.")).toBe(
      200,
    );
    for (const chip of chips) expect(await ask(chip, CHAT)).toBe(200);
    // Typed questions fill the rest of the window, and the one after waits.
    expect(await ask("Wie lang ist Blue Fire?", CHAT)).toBe(200);
    expect(await ask("Und Silver Star?", CHAT)).toBe(200);
    expect(await ask("Und Voltron?", CHAT)).toBe(429);
  });

  it("still counts every reply in the ten-minute window", async () => {
    const { ask } = setup(["Ja"]);
    for (let i = 0; i < 20; i++) expect(await ask("Ja", CHAT)).toBe(200);
    expect(await ask("Ja", CHAT)).toBe(429);
  });
});

describe("AssistantRateLimitService, feedback", () => {
  it("takes five an hour per session and twenty a day per address", async () => {
    const limiter = new AssistantRateLimitService(fakeRedis() as never);
    const ip = "203.0.113.7";
    for (let i = 0; i < 5; i++) {
      expect((await limiter.feedback("session-1", ip)).allowed).toBe(true);
    }
    expect((await limiter.feedback("session-1", ip)).allowed).toBe(false);
    // Fresh sessions from the same address, as a troll would farm them, share the address's day.
    for (let session = 2; session <= 4; session++) {
      for (let i = 0; i < 5; i++) {
        expect((await limiter.feedback(`session-${session}`, ip)).allowed).toBe(
          true,
        );
      }
    }
    expect((await limiter.feedback("session-5", ip)).allowed).toBe(false);
    expect((await limiter.feedback("session-6", "198.51.100.1")).allowed).toBe(
      true,
    );
  });
});

describe("AssistantRateLimitService, the short window", () => {
  it("takes three questions in thirty seconds and refuses the fourth", async () => {
    const limiter = new AssistantRateLimitService(fakeRedis() as never);
    const ask = () => limiter.check({ session: "session-1" }, "chatShort");
    for (let i = 0; i < 3; i++) expect((await ask()).allowed).toBe(true);
    expect(await ask()).toEqual({ allowed: false, retryAfterSeconds: 30 });
  });
});

describe("AssistantController.session, the app's way in with a data key", () => {
  const APP_KEY = `pfk_${"a".repeat(43)}`;
  const PLAIN_KEY = `pfk_${"b".repeat(43)}`;

  function setup() {
    const rateLimit = new AssistantRateLimitService(fakeRedis() as never);
    const keys: Record<string, unknown> = {
      [APP_KEY]: {
        id: "key-app",
        usage: "internal",
        requestsPerHour: null,
        allowedParkIds: null,
        permissions: ["live.waits"],
        expiresAt: null,
      },
      [PLAIN_KEY]: {
        id: "key-plain",
        usage: "standard",
        requestsPerHour: 1000,
        allowedParkIds: null,
        permissions: ["live.waits", "parks.details"],
        expiresAt: null,
      },
    };
    const store = {
      lookup: jest.fn((presented: string) =>
        Promise.resolve(
          keys[presented]
            ? { status: "active", key: keys[presented] }
            : { status: "unknown" },
        ),
      ),
      countRequest: jest.fn().mockResolvedValue(null),
    };
    const moduleRef = {
      get: (token: unknown) => (token === ApiKeyService ? store : undefined),
    } as unknown as ModuleRef;
    const challenge = {
      open: jest.fn().mockResolvedValue({ token: "browser-token" }),
      openForKey: jest.fn().mockReturnValue({ token: "app-token" }),
    };
    const controller = new AssistantController(
      {} as AssistantSettingsService,
      {} as AssistantChatService,
      {} as AssistantRecordsService,
      rateLimit,
      challenge as unknown as AssistantChallengeService,
      moduleRef,
    );
    const open = (body: unknown, headers: Record<string, string> = {}) =>
      controller.session(
        body,
        {
          headers,
          socket: { remoteAddress: "203.0.113.9" },
        } as unknown as Request,
        fakeResponse().response,
      );
    return { open, challenge, rateLimit, store };
  }

  it("issues a session without Turnstile to a key carrying assistant.chat", async () => {
    const { open, challenge } = setup();
    await expect(open({}, { "x-api-key": APP_KEY })).resolves.toEqual({
      sessionToken: "app-token",
      expiresInSeconds: 7200,
    });
    expect(challenge.open).not.toHaveBeenCalled();
  });

  it("refuses a key without the permission, and a request with neither token nor key", async () => {
    const { open, challenge } = setup();
    await expect(open({}, { "x-api-key": PLAIN_KEY })).rejects.toMatchObject({
      status: 403,
    });
    await expect(open({})).rejects.toMatchObject({ status: 401 });
    await expect(
      open({}, { "x-api-key": `pfk_${"z".repeat(43)}` }),
    ).rejects.toMatchObject({ status: 401 });
    expect(challenge.openForKey).not.toHaveBeenCalled();
  });

  it("keeps the browser's Turnstile path as it was, key or not", async () => {
    const { open, challenge } = setup();
    await expect(
      open({ turnstileToken: "solved" }, { "x-api-key": PLAIN_KEY }),
    ).resolves.toMatchObject({ sessionToken: "browser-token" });
    expect(challenge.open).toHaveBeenCalledWith("solved", expect.anything());
    expect(challenge.openForKey).not.toHaveBeenCalled();
  });

  it("still counts sessions per address, and caps them per key", async () => {
    const { open } = setup();
    for (let i = 0; i < 10; i++) {
      await open({}, { "x-api-key": APP_KEY });
    }
    await expect(open({}, { "x-api-key": APP_KEY })).rejects.toMatchObject({
      status: 429,
    });
    const limiter = new AssistantRateLimitService(fakeRedis() as never);
    for (let i = 0; i < 2000; i++) {
      await limiter.check({ key: "key-app" }, "keySession");
    }
    expect(
      (await limiter.check({ key: "key-app" }, "keySession")).allowed,
    ).toBe(false);
  });
});
