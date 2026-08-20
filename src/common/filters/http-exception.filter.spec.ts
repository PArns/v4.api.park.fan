import { BadRequestException, Logger } from "@nestjs/common";
import type { ArgumentsHost } from "@nestjs/common";
import { HttpExceptionFilter } from "./http-exception.filter";

/**
 * The error path is the one that logs the secret.
 *
 * `LoggingInterceptor` writes its line from a `tap()`, which is a next-only
 * observer: a request that throws never reaches it, and this filter logs
 * instead. So the redaction added to the interceptor covered only requests that
 * succeeded — while the requests that carry a valid `?pass=` and fail are
 * something this codebase produces on purpose (`cache/reset` without
 * `?confirm=true`, and every endpoint the pass authenticates and is then
 * refused on).
 */
function hostFor(url: string, method = "POST") {
  const json = jest.fn();
  const status = jest.fn(() => ({ json }));
  const host = {
    switchToHttp: () => ({
      getResponse: () => ({ status }),
      getRequest: () => ({ method, url }),
    }),
  } as unknown as ArgumentsHost;
  return { host, status, json };
}

describe("HttpExceptionFilter", () => {
  let warn: jest.SpyInstance;
  let error: jest.SpyInstance;

  beforeEach(() => {
    warn = jest.spyOn(Logger.prototype, "warn").mockImplementation();
    error = jest.spyOn(Logger.prototype, "error").mockImplementation();
  });

  afterEach(() => jest.restoreAllMocks());

  it("keeps the shared admin pass out of the 4xx log line", () => {
    const { host } = hostFor("/v1/admin/cache/reset?pass=S3CR3T");
    new HttpExceptionFilter().catch(
      new BadRequestException("Confirmation required"),
      host,
    );

    expect(warn).toHaveBeenCalledTimes(1);
    const line = warn.mock.calls[0][0] as string;
    expect(line).not.toContain("S3CR3T");
    expect(line).toContain("pass=***");
  });

  it("keeps it out of the 5xx log line too", () => {
    const { host } = hostFor("/v1/admin/sync?pass=S3CR3T&force=true");
    new HttpExceptionFilter().catch(new Error("boom"), host);

    expect(error).toHaveBeenCalledTimes(1);
    const line = error.mock.calls[0][0] as string;
    expect(line).not.toContain("S3CR3T");
    expect(line).toContain("force=true");
  });

  it("does not echo it back in the response body either", () => {
    const { host, json } = hostFor("/v1/admin/x?pass=S3CR3T");
    new HttpExceptionFilter().catch(new BadRequestException("no"), host);

    expect(json).toHaveBeenCalledTimes(1);
    expect(json.mock.calls[0][0].path).toBe("/v1/admin/x?pass=***");
  });

  it("leaves a URL with nothing to redact exactly as requested", () => {
    // The log has to stay worth reading.
    const { host, json } = hostFor("/v1/parks?limit=20", "GET");
    new HttpExceptionFilter().catch(new BadRequestException("nope"), host);

    expect(warn.mock.calls[0][0]).toContain("/v1/parks?limit=20");
    expect(json.mock.calls[0][0].path).toBe("/v1/parks?limit=20");
  });
});
