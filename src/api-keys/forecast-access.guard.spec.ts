import { UnauthorizedException } from "@nestjs/common";
import {
  ForecastAccessGuard,
  readPresentedApiKey,
} from "./forecast-access.guard";
import { privateCacheControl } from "./private-cache.interceptor";

const VALID = `pfk_${"c".repeat(43)}`;

function context(headers: Record<string, unknown>) {
  const request: Record<string, unknown> = { headers };
  const response = { setHeader: jest.fn() };
  return {
    request,
    response,
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
  return { guard: new ForecastAccessGuard(apiKeys as never), apiKeys };
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

  it("lets park.fan's own frontend through without a data key", async () => {
    const { guard: g, apiKeys } = guard();
    const { ctx } = context({ "x-auth-key": "frontend-secret" });
    await expect(g.canActivate(ctx)).resolves.toBe(true);
    expect(apiKeys.resolve).not.toHaveBeenCalled();
  });

  it("lets an active data key through and attaches it", async () => {
    const { guard: g } = guard();
    const { ctx, request } = context({ "x-api-key": VALID });
    await expect(g.canActivate(ctx)).resolves.toBe(true);
    expect(request.apiKey).toEqual({ id: "k1", usage: "non-commercial" });
  });

  it("refuses a request without a key, naming the terms", async () => {
    const { guard: g } = guard();
    const { ctx, response } = context({});
    const error = await g.canActivate(ctx).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UnauthorizedException);
    expect((error as UnauthorizedException).getResponse()).toMatchObject({
      error: "api-key-required",
      terms: "https://park.fan/en/impressum#data-use",
    });
    expect(response.setHeader).toHaveBeenCalledWith(
      "WWW-Authenticate",
      'ApiKey header="X-API-Key"',
    );
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
