import { Logger } from "@nestjs/common";
import type { ArgumentsHost } from "@nestjs/common";
import { HttpExceptionFilter } from "../../src/common/filters/http-exception.filter";

/** What a caller receives: the body that was written, and the headers set. */
export interface FilteredResponse {
  body: Record<string, unknown>;
  headers: Record<string, string>;
}

/**
 * Run a call that is expected to throw through the real `HttpExceptionFilter`
 * and return what a client would actually receive.
 *
 * `HttpExceptionFilter` is registered globally and writes every error response
 * on this API, so a controller's or guard's `throw` is a draft rather than the
 * answer. That distinction is not academic: `retryAfterSeconds` sat in the
 * thrown body of both rate limiters while the filter deleted it on the way out,
 * and the specs that asserted it on the thrown exception stayed green for as
 * long as the bug existed (PAR-146). Assertions about what a caller learns
 * belong on this side of the filter.
 *
 * The call is made exactly once — a limiter counts per call, and a second one
 * would make this helper part of what the test measures.
 *
 * It lives under `test/` rather than beside the filter because it calls `jest`.
 * `tsconfig.build.json` excludes the `test` directory and files ending in
 * `spec.ts`, and nothing else — so a helper under `src/` with neither shape
 * compiles into `dist/` and ships a `jest.fn()` in the production image.
 */
export async function throughFilter(
  call: () => Promise<unknown>,
  { method = "POST", url = "/" }: { method?: string; url?: string } = {},
): Promise<FilteredResponse> {
  const didNotThrow = Symbol("did not throw");
  const thrown = await call().then(
    () => didNotThrow,
    (error: unknown) => error,
  );
  // Before the filter, not after: a regression that stops throwing would
  // otherwise send `undefined` down the 500 path, which writes a real entry to
  // the error log on disk before the assertion gets its turn.
  if (thrown === didNotThrow) {
    throw new Error(`expected ${method} ${url} to throw, and it resolved`);
  }

  const json = jest.fn();
  const headers: Record<string, string> = {};
  const host = {
    switchToHttp: () => ({
      getResponse: () => ({
        status: () => ({ json }),
        headersSent: false,
        header: (name: string, value: string) => {
          headers[name] = value;
        },
      }),
      getRequest: () => ({ method, url }),
    }),
  } as unknown as ArgumentsHost;

  const warn = jest.spyOn(Logger.prototype, "warn").mockImplementation();
  const error = jest.spyOn(Logger.prototype, "error").mockImplementation();
  try {
    new HttpExceptionFilter().catch(thrown, host);
  } finally {
    warn.mockRestore();
    error.mockRestore();
  }

  return { body: json.mock.calls[0][0] as Record<string, unknown>, headers };
}
