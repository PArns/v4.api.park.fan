import { CallHandler, ExecutionContext } from "@nestjs/common";
import { of, firstValueFrom } from "rxjs";
import { NoCdnCacheInterceptor } from "./no-cdn-cache.interceptor";

interface MockResponse {
  headersSent: boolean;
  setHeader: jest.Mock;
}

function buildMockContext({
  headersSent = false,
}: { headersSent?: boolean } = {}) {
  const headers: Record<string, string> = {};
  const response: MockResponse = {
    headersSent,
    setHeader: jest.fn((name: string, value: string) => {
      // Node throws exactly this once the response is on the wire.
      if (headersSent) {
        throw new Error("Cannot set headers after they are sent to the client");
      }
      headers[name] = value;
    }),
  };
  const ctx = {
    switchToHttp: () => ({ getResponse: () => response }),
  } as unknown as ExecutionContext;
  return { ctx, headers, response };
}

const handlerOf = (body: unknown): CallHandler => ({ handle: () => of(body) });

describe("NoCdnCacheInterceptor", () => {
  const interceptor = new NoCdnCacheInterceptor();

  it("keeps the response out of every shared cache", async () => {
    const { ctx, headers } = buildMockContext();

    await firstValueFrom(interceptor.intercept(ctx, handlerOf({ a: 1 })));

    expect(headers["Cache-Control"]).toBe(
      "private, no-store, no-cache, must-revalidate",
    );
    expect(headers["Pragma"]).toBe("no-cache");
  });

  it("sets no header once the response has been sent", async () => {
    // Same failure mode as the sibling cache interceptors: a client that gave
    // up mid-flight leaves the headers flushed, and setting one then throws
    // ERR_HTTP_HEADERS_SENT — logging an abandoned request as a 500.
    const { ctx, response } = buildMockContext({ headersSent: true });

    await expect(
      firstValueFrom(interceptor.intercept(ctx, handlerOf({ a: 1 }))),
    ).resolves.toEqual({ a: 1 });

    expect(response.setHeader).not.toHaveBeenCalled();
  });
});
