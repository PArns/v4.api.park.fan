import { HttpException, UnauthorizedException } from "@nestjs/common";
import {
  ForecastAccessGuard,
  readPresentedApiKey,
} from "./forecast-access.guard";
import {
  keepResponsePrivate,
  mergeVary,
  privateCacheControl,
} from "./private-cache";

const VALID = `pfk_${"c".repeat(43)}`;

function context(
  headers: Record<string, unknown>,
  params: Record<string, string> = {},
  query: Record<string, string> = {},
) {
  const request: Record<string, unknown> = { headers, params, query };
  const sent = new Map<string, unknown>();
  const response = {
    headersSent: false,
    setHeader(name: string, value: unknown) {
      sent.set(name.toLowerCase(), value);
      return response;
    },
  };
  return {
    request,
    sent,
    response,
    ctx: {
      getHandler: () => undefined,
      switchToHttp: () => ({
        getRequest: () => request,
        getResponse: () => response,
      }),
    } as never,
  };
}

const LIMITED = `pfk_${"e".repeat(43)}`;

function guard(anyPark = false) {
  const apiKeys = {
    resolve: jest.fn(async (value: string) =>
      value === VALID
        ? {
            id: "k1",
            usage: "non-commercial",
            requestsPerHour: 2,
            allowedParkIds: null,
          }
        : value === LIMITED
          ? {
              id: "k2",
              usage: "commercial",
              requestsPerHour: null,
              allowedParkIds: ["mk-id"],
            }
          : null,
    ),
    countRequest: jest.fn(
      async (): Promise<{
        limit: number;
        remaining: number;
        resetSeconds: number;
        exceeded: boolean;
      } | null> => ({
        limit: 2,
        remaining: 1,
        resetSeconds: 600,
        exceeded: false,
      }),
    ),
  };
  const parks = {
    findByGeographicPath: jest.fn(async (...path: string[]) =>
      path[3] === "magic-kingdom-park" ? { id: "mk-id" } : { id: "other-id" },
    ),
  };
  const moduleRef = {
    get: (token: { name?: string }) =>
      token?.name === "ParksService" ? parks : apiKeys,
  };
  const reflector = { get: () => anyPark };
  return {
    guard: new ForecastAccessGuard(moduleRef as never, reflector as never),
    apiKeys,
    parks,
  };
}

describe("readPresentedApiKey", () => {
  it("reads X-API-Key, and a Bearer value only when it is a data key", () => {
    expect(readPresentedApiKey({ "x-api-key": ` ${VALID} ` })).toBe(VALID);
    expect(readPresentedApiKey({ authorization: `Bearer ${VALID}` })).toBe(
      VALID,
    );
    expect(readPresentedApiKey({ authorization: "Bearer pfa_admin" })).toBe(
      null,
    );
    expect(readPresentedApiKey({})).toBe(null);
  });
});

describe("ForecastAccessGuard", () => {
  const previous = process.env.THROTTLE_BYPASS_KEYS;
  beforeEach(() => {
    process.env.THROTTLE_BYPASS_KEYS = "frontend-secret";
  });
  afterEach(() => {
    process.env.THROTTLE_BYPASS_KEYS = previous;
  });

  it("lets park.fan's own frontend through without a data key, privately", async () => {
    const { guard: g, apiKeys } = guard();
    const { ctx, sent } = context({ "x-auth-key": "frontend-secret" });
    await expect(g.canActivate(ctx)).resolves.toBe(true);
    expect(apiKeys.resolve).not.toHaveBeenCalled();
    expect(sent.get("cache-control")).toBe("private, no-store");
  });

  it("lets an active data key through and attaches it", async () => {
    const { guard: g } = guard();
    const { ctx, request } = context({ "x-api-key": VALID });
    await expect(g.canActivate(ctx)).resolves.toBe(true);
    expect(request.apiKey).toEqual({
      id: "k1",
      usage: "non-commercial",
      requestsPerHour: 2,
      allowedParkIds: null,
    });
  });

  it("refuses a request without a key, naming the terms", async () => {
    const { guard: g } = guard();
    const { ctx, sent } = context({});
    const error = await g.canActivate(ctx).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UnauthorizedException);
    expect((error as UnauthorizedException).getResponse()).toMatchObject({
      error: "api-key-required",
      terms: "https://park.fan/en/impressum#data-use",
    });
    expect(sent.get("www-authenticate")).toBe('ApiKey header="X-API-Key"');
    // A guard runs before every interceptor, so nothing else would set it.
    expect(sent.get("cache-control")).toBe("private, no-store");
  });

  it("reports the hourly allowance and refuses a key over it with 429", async () => {
    const { guard: g, apiKeys } = guard();
    const ok = context({ "x-api-key": VALID });
    await g.canActivate(ok.ctx);
    expect(ok.sent.get("x-api-key-hourly-limit")).toBe("2");
    expect(ok.sent.get("x-api-key-hourly-remaining")).toBe("1");

    apiKeys.countRequest.mockResolvedValueOnce({
      limit: 2,
      remaining: 0,
      resetSeconds: 600,
      exceeded: true,
    });
    const over = context({ "x-api-key": VALID });
    const error = (await g
      .canActivate(over.ctx)
      .catch((e: unknown) => e)) as HttpException;
    expect(error.getStatus()).toBe(429);
    expect(error.getResponse()).toMatchObject({
      error: "api-key-rate-limited",
      retryAfterSeconds: 600,
    });
    expect(over.sent.get("cache-control")).toBe("private, no-store");
  });

  it("stops admitting the bypass key once FORECAST_BYPASS_KEY=off", async () => {
    process.env.FORECAST_BYPASS_KEY = "off";
    try {
      const { guard: g } = guard();
      const bypass = context({ "x-auth-key": "frontend-secret" });
      const error = await g.canActivate(bypass.ctx).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(UnauthorizedException);

      const keyed = context({
        "x-auth-key": "frontend-secret",
        "x-api-key": VALID,
      });
      await expect(g.canActivate(keyed.ctx)).resolves.toBe(true);
    } finally {
      delete process.env.FORECAST_BYPASS_KEY;
    }
  });

  describe("a key limited to some parks", () => {
    const route = (park: string) => ({
      continent: "north-america",
      country: "united-states",
      city: "orlando",
      parkSlug: park,
    });

    it("reads its own parks", async () => {
      const { guard: g } = guard();
      const { ctx } = context(
        { "x-api-key": LIMITED },
        route("magic-kingdom-park"),
      );
      await expect(g.canActivate(ctx)).resolves.toBe(true);
    });

    it("is refused for another park, before the hourly count", async () => {
      const { guard: g, apiKeys } = guard();
      const { ctx } = context({ "x-api-key": LIMITED }, route("epcot"));
      const error = (await g
        .canActivate(ctx)
        .catch((e: unknown) => e)) as HttpException;
      expect(error.getStatus()).toBe(403);
      expect(error.getResponse()).toMatchObject({
        error: "api-key-park-not-allowed",
      });
      expect(apiKeys.countRequest).not.toHaveBeenCalled();
    });

    it("is refused on a route that names no park", async () => {
      const { guard: g } = guard();
      const { ctx } = context({ "x-api-key": LIMITED });
      const error = (await g
        .canActivate(ctx)
        .catch((e: unknown) => e)) as HttpException;
      expect(error.getResponse()).toMatchObject({
        error: "api-key-park-required",
      });
    });

    it("may call /verify, which checks ?park= when it is given", async () => {
      const { guard: g } = guard(true);
      await expect(
        g.canActivate(context({ "x-api-key": LIMITED }).ctx),
      ).resolves.toBe(true);
      const other = context(
        { "x-api-key": LIMITED },
        {},
        { park: "north-america/united-states/orlando/epcot" },
      );
      const error = (await g
        .canActivate(other.ctx)
        .catch((e: unknown) => e)) as HttpException;
      expect(error.getStatus()).toBe(403);
    });
  });

  it("does not count park.fan's own frontend", async () => {
    const { guard: g, apiKeys } = guard();
    await g.canActivate(context({ "x-auth-key": "frontend-secret" }).ctx);
    expect(apiKeys.countRequest).not.toHaveBeenCalled();
  });

  describe("with FORECAST_SHARED_CACHE=vary", () => {
    beforeEach(() => {
      process.env.FORECAST_SHARED_CACHE = "vary";
    });
    afterEach(() => {
      delete process.env.FORECAST_SHARED_CACHE;
    });

    it("varies on the key headers and lets the route's public policy stand", async () => {
      const { guard: g } = guard();
      const { ctx, sent, response } = context({ "x-api-key": VALID });
      await g.canActivate(ctx);
      expect(sent.get("vary")).toBe("X-API-Key, x-auth-key");
      response.setHeader("Cache-Control", "public, max-age=300, s-maxage=300");
      response.setHeader("Vary", "Accept-Encoding");
      expect(sent.get("cache-control")).toBe(
        "public, max-age=300, s-maxage=300",
      );
      expect(sent.get("vary")).toBe("Accept-Encoding, X-API-Key, x-auth-key");
    });

    it("keeps a key sent as a Bearer token private", async () => {
      const { guard: g } = guard();
      const { ctx, sent, response } = context({
        authorization: `Bearer ${VALID}`,
      });
      await g.canActivate(ctx);
      response.setHeader("Cache-Control", "public, max-age=300, s-maxage=300");
      expect(sent.get("cache-control")).toBe("private, max-age=300");
    });

    it("leaves a refusal private", async () => {
      const { guard: g } = guard();
      const { ctx, sent } = context({});
      await g.canActivate(ctx).catch(() => undefined);
      expect(sent.get("cache-control")).toBe("private, no-store");
    });
  });

  it("refuses an unknown or revoked key as invalid", async () => {
    const { guard: g } = guard();
    const { ctx } = context({ "x-api-key": `pfk_${"d".repeat(43)}` });
    const error = await g.canActivate(ctx).catch((e: unknown) => e);
    expect((error as UnauthorizedException).getResponse()).toMatchObject({
      error: "api-key-invalid",
    });
  });
});

describe("privateCacheControl", () => {
  it("keeps the browser max-age and drops everything shared", () => {
    expect(
      privateCacheControl(
        "public, max-age=300, s-maxage=300, stale-while-revalidate=60",
      ),
    ).toBe("private, max-age=300");
  });

  it("stores nothing when the handler set no max-age", () => {
    expect(privateCacheControl(undefined)).toBe("private, no-store");
    expect(privateCacheControl("public, s-maxage=600")).toBe(
      "private, no-store",
    );
  });
});

describe("keepResponsePrivate", () => {
  it("rewrites a public Cache-Control set after it and drops CDN-Cache-Control", () => {
    const headers = new Map<string, unknown>();
    const response = {
      headersSent: false,
      setHeader(name: string, value: unknown) {
        headers.set(name.toLowerCase(), value);
        return response;
      },
    };
    keepResponsePrivate(response as never);
    expect(headers.get("cache-control")).toBe("private, no-store");

    response.setHeader(
      "Cache-Control",
      "public, max-age=300, s-maxage=300, stale-while-revalidate=600",
    );
    response.setHeader("CDN-Cache-Control", "max-age=3600");
    expect(headers.get("cache-control")).toBe("private, max-age=300");
    expect(headers.has("cdn-cache-control")).toBe(false);
  });
});

describe("mergeVary", () => {
  it("adds each name once, keeping what was there", () => {
    expect(mergeVary("Accept-Encoding", ["X-API-Key"])).toBe(
      "Accept-Encoding, X-API-Key",
    );
    expect(mergeVary("x-api-key", ["X-API-Key"])).toBe("x-api-key");
    expect(mergeVary(undefined, ["A", "B"])).toBe("A, B");
  });
});
