import { UnauthorizedException } from "@nestjs/common";
import {
  ForecastAccessGuard,
  readPresentedApiKey,
} from "./forecast-access.guard";
import { keepResponsePrivate, privateCacheControl } from "./private-cache";

const VALID = `pfk_${"c".repeat(43)}`;

function context(headers: Record<string, unknown>) {
  const request: Record<string, unknown> = { headers };
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
    ctx: {
      switchToHttp: () => ({
        getRequest: () => request,
        getResponse: () => response,
      }),
    } as never,
  };
}

function guard() {
  const apiKeys = {
    resolve: jest.fn(async (value: string) =>
      value === VALID ? { id: "k1", usage: "non-commercial" } : null,
    ),
  };
  return {
    guard: new ForecastAccessGuard({ get: () => apiKeys } as never),
    apiKeys,
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
    expect(request.apiKey).toEqual({ id: "k1", usage: "non-commercial" });
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
