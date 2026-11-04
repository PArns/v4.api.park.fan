import type { Request, Response } from "express";
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
  const response = {
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
