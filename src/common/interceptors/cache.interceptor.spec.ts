import { CallHandler, ExecutionContext } from "@nestjs/common";
import { of } from "rxjs";
import { firstValueFrom } from "rxjs";
import { HttpCacheInterceptor } from "./cache.interceptor";

/**
 * Build a mock ExecutionContext + a `headers` capture map for the
 * fake express response. Returns helpers so each test can inspect
 * exactly which headers were set.
 */
interface MockResponse {
  setHeader: jest.Mock;
  status: jest.Mock;
}

function buildMockContext(reqHeaders: Record<string, string> = {}) {
  const headers: Record<string, string> = {};
  let status: number | null = null;
  const response: MockResponse = {
    setHeader: jest.fn((name: string, value: string) => {
      headers[name] = value;
    }),
    status: jest.fn((code: number) => {
      status = code;
      return response;
    }),
  };
  const request = { headers: reqHeaders };
  const ctx = {
    switchToHttp: () => ({
      getResponse: () => response,
      getRequest: () => request,
    }),
  } as unknown as ExecutionContext;
  return { ctx, headers, response, getStatus: () => status };
}

const handlerOf = (body: unknown): CallHandler => ({
  handle: () => of(body),
});

describe("HttpCacheInterceptor", () => {
  describe("single TTL (browser == CDN)", () => {
    it("emits Cache-Control with matching max-age and s-maxage", async () => {
      const interceptor = new HttpCacheInterceptor(900);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(
        interceptor.intercept(ctx, handlerOf({ foo: "bar" })),
      );

      expect(headers["Cache-Control"]).toBe(
        "public, max-age=900, s-maxage=900, stale-while-revalidate=1800",
      );
      // No browser/CDN split → no separate CDN-Cache-Control.
      expect(headers["CDN-Cache-Control"]).toBeUndefined();
    });

    it("keeps stale-while-revalidate short for sub-2-minute TTLs", async () => {
      const interceptor = new HttpCacheInterceptor(60);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(interceptor.intercept(ctx, handlerOf({})));

      expect(headers["Cache-Control"]).toBe(
        "public, max-age=60, s-maxage=60, stale-while-revalidate=60",
      );
    });
  });

  describe("browser/CDN split", () => {
    it("emits a separate CDN-Cache-Control so Cloudflare can cache longer than browsers", async () => {
      const interceptor = new HttpCacheInterceptor(60, 900);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(interceptor.intercept(ctx, handlerOf({})));

      // Browser-facing header: 60s. We keep s-maxage in here too so
      // non-Cloudflare CDNs still see the long TTL.
      expect(headers["Cache-Control"]).toBe(
        "public, max-age=60, s-maxage=900, stale-while-revalidate=60",
      );
      // CDN-only header: Cloudflare reads this in preference.
      expect(headers["CDN-Cache-Control"]).toBe(
        "public, max-age=900, stale-while-revalidate=60",
      );
    });

    it("does not emit CDN-Cache-Control when the explicit sMaxAge equals maxAge", async () => {
      // Edge case: caller passed sMaxAge but it matches maxAge → no
      // information gain, so we suppress the extra header.
      const interceptor = new HttpCacheInterceptor(120, 120);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(interceptor.intercept(ctx, handlerOf({})));

      expect(headers["CDN-Cache-Control"]).toBeUndefined();
    });
  });

  describe("ETag", () => {
    it("emits a strong ETag derived from the response body", async () => {
      const interceptor = new HttpCacheInterceptor(60);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(
        interceptor.intercept(ctx, handlerOf({ a: 1, b: 2 })),
      );

      expect(headers["ETag"]).toMatch(/^"[a-f0-9]{32}"$/);
    });

    it("returns 304 when the client's If-None-Match matches the body's ETag", async () => {
      const body = { foo: "bar" };

      // First call: capture the ETag the interceptor would produce.
      const probe = new HttpCacheInterceptor(60);
      const probeCtx = buildMockContext();
      await firstValueFrom(probe.intercept(probeCtx.ctx, handlerOf(body)));
      const etag = probeCtx.headers["ETag"];

      // Second call: client sends that ETag back → expect 304.
      const interceptor = new HttpCacheInterceptor(60);
      const { ctx, response, getStatus } = buildMockContext({
        "if-none-match": etag,
      });

      await firstValueFrom(interceptor.intercept(ctx, handlerOf(body)));

      expect(getStatus()).toBe(304);
      // Cache headers are NOT set on a 304 — the existing browser/CDN
      // entry stays in place.
      expect(response.setHeader).not.toHaveBeenCalledWith(
        "Cache-Control",
        expect.anything(),
      );
    });
  });

  describe("removed legacy headers", () => {
    it("does not emit Last-Modified (was previously set to Date.now() on every request, defeating its purpose)", async () => {
      const interceptor = new HttpCacheInterceptor(60);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(interceptor.intercept(ctx, handlerOf({})));

      expect(headers["Last-Modified"]).toBeUndefined();
    });

    it("does not emit Vary: Accept (caused unnecessary cache fragmentation on CDNs)", async () => {
      const interceptor = new HttpCacheInterceptor(60);
      const { ctx, headers } = buildMockContext();

      await firstValueFrom(interceptor.intercept(ctx, handlerOf({})));

      expect(headers["Vary"]).toBeUndefined();
    });
  });
});
