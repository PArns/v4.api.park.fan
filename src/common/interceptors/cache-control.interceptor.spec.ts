import { CallHandler, ExecutionContext } from "@nestjs/common";
import { of, firstValueFrom } from "rxjs";
import { CacheControlInterceptor } from "./cache-control.interceptor";

interface MockResponse {
  setHeader: jest.Mock;
  getHeader: jest.Mock;
  status: jest.Mock;
}

function buildMockContext(
  url: string,
  method = "GET",
  reqHeaders: Record<string, string> = {},
  presetHeaders: Record<string, string> = {},
) {
  const headers: Record<string, string> = { ...presetHeaders };
  let status: number | null = null;
  const response: MockResponse = {
    setHeader: jest.fn((name: string, value: string) => {
      headers[name] = value;
    }),
    getHeader: jest.fn((name: string) => headers[name]),
    status: jest.fn((code: number) => {
      status = code;
      return response;
    }),
  };
  const request = { url, method, headers: reqHeaders };
  const ctx = {
    switchToHttp: () => ({
      getResponse: () => response,
      getRequest: () => request,
    }),
  } as unknown as ExecutionContext;
  return { ctx, headers, response, getStatus: () => status };
}

const handlerOf = (body: unknown): CallHandler => ({ handle: () => of(body) });

describe("CacheControlInterceptor", () => {
  const interceptor = new CacheControlInterceptor();

  describe("no-store for operator-only surfaces", () => {
    it.each([
      "/v1/admin/queue-status",
      "/v1/admin/system-health",
      "/v1/ml/dashboard",
      "/v1/ml/monitoring/alerts",
      "/v1/ml/health",
      "/v1/ml/accuracy/system",
    ])("marks %s as private, no-store", async (url) => {
      const { ctx, headers } = buildMockContext(url);
      await firstValueFrom(interceptor.intercept(ctx, handlerOf({ ok: true })));
      expect(headers["Cache-Control"]).toBe(
        "private, no-store, no-cache, must-revalidate",
      );
    });
  });

  describe("user-facing caching is unaffected", () => {
    it("still caches park predictions", async () => {
      const { ctx, headers } = buildMockContext(
        "/v1/parks/europe/de/rust/europa-park/predictions/yearly",
      );
      await firstValueFrom(interceptor.intercept(ctx, handlerOf({ d: 1 })));
      expect(headers["Cache-Control"]).toContain("max-age");
      expect(headers["Cache-Control"]).not.toContain("no-store");
    });

    it("does not snag slugs that merely contain 'ml' or 'admin'", async () => {
      const { ctx, headers } = buildMockContext(
        "/v1/parks/asia/jp/tokyo/some-admington-ml-park",
      );
      await firstValueFrom(interceptor.intercept(ctx, handlerOf({ d: 1 })));
      expect(headers["Cache-Control"]).not.toContain("no-store");
    });
  });

  describe("ETag (delegated to Express's native weak ETag)", () => {
    it("does NOT set its own ETag — Express owns ETag/304", async () => {
      const { ctx, headers } = buildMockContext("/v1/parks");
      await firstValueFrom(interceptor.intercept(ctx, handlerOf({ a: 1 })));
      expect(headers["ETag"]).toBeUndefined();
    });
  });

  describe("respects a locally-set Cache-Control", () => {
    it("does not overwrite an existing Cache-Control header", async () => {
      const { ctx, headers } = buildMockContext(
        "/v1/parks",
        "GET",
        {},
        {
          "Cache-Control": "public, max-age=120, s-maxage=120",
        },
      );
      await firstValueFrom(interceptor.intercept(ctx, handlerOf({ a: 1 })));
      expect(headers["Cache-Control"]).toBe(
        "public, max-age=120, s-maxage=120",
      );
    });
  });

  describe("Last-Modified", () => {
    it("emits Last-Modified only when the body carries a timestamp", async () => {
      const withTs = buildMockContext("/v1/parks");
      await firstValueFrom(
        interceptor.intercept(
          withTs.ctx,
          handlerOf({ updatedAt: "2026-01-01T00:00:00.000Z" }),
        ),
      );
      expect(withTs.headers["Last-Modified"]).toBeDefined();

      const without = buildMockContext("/v1/parks");
      await firstValueFrom(
        interceptor.intercept(without.ctx, handlerOf({ a: 1 })),
      );
      expect(without.headers["Last-Modified"]).toBeUndefined();
    });
  });
});
