import { BadRequestException, Logger } from "@nestjs/common";
import type { ArgumentsHost } from "@nestjs/common";
import { HttpExceptionFilter } from "./http-exception.filter";
import { logToFile } from "../utils/file-logger.util";

jest.mock("../utils/file-logger.util", () => ({ logToFile: jest.fn() }));

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

/**
 * A 500 in the admin used to be a message and nothing else.
 *
 * The park editor answered every request with "Cannot read properties of
 * undefined (reading 'databaseName')" — a TypeORM metadata error that had to
 * be reproduced against production data before anybody could see the stack,
 * because the only copy of it was in a container log behind a deploy console.
 * The reference is the thread back: the same six characters sit in the
 * response body and in the log entry.
 */
describe("HttpExceptionFilter — server errors", () => {
  beforeEach(() => {
    jest.spyOn(Logger.prototype, "error").mockImplementation();
    jest.spyOn(Logger.prototype, "warn").mockImplementation();
    (logToFile as jest.Mock).mockClear();
  });

  afterEach(() => jest.restoreAllMocks());

  it("gives a 500 a reference and writes the same one to the error log", () => {
    const { host, json } = hostFor("/v1/admin/content/parks/abc", "GET");

    new HttpExceptionFilter().catch(
      new TypeError("Cannot read properties of undefined (reading 'x')"),
      host,
    );

    const body = json.mock.calls[0][0] as { reference?: string };
    expect(body.reference).toMatch(/^[0-9a-f]{6}$/);

    expect(logToFile).toHaveBeenCalledTimes(1);
    const [file, entry] = (logToFile as jest.Mock).mock.calls[0] as [
      string,
      Record<string, unknown>,
    ];
    expect(file).toBe("api-errors");
    expect(entry.reference).toBe(body.reference);
    expect(entry.status).toBe(500);
    expect(entry.method).toBe("GET");
    expect(entry.name).toBe("TypeError");
    expect(entry.stack).toContain("TypeError");
  });

  it("records the statement a failed query died on, but never its parameters", () => {
    const { host } = hostFor("/v1/admin/content/parks", "GET");
    const failure = Object.assign(new Error("relation does not exist"), {
      name: "QueryFailedError",
      query: "SELECT * FROM parks WHERE email = $1",
      parameters: ["someone@example.com"],
    });

    new HttpExceptionFilter().catch(failure, host);

    const entry = (logToFile as jest.Mock).mock.calls[0][1] as Record<
      string,
      unknown
    >;
    expect(entry.query).toBe("SELECT * FROM parks WHERE email = $1");
    expect(entry.parameterCount).toBe(1);
    expect(JSON.stringify(entry)).not.toContain("someone@example.com");
  });

  it("leaves a 4xx without a reference — nothing was logged to point at", () => {
    const { host, json } = hostFor("/v1/admin/content/parks", "POST");

    new HttpExceptionFilter().catch(new BadRequestException("nope"), host);

    expect(json.mock.calls[0][0]).not.toHaveProperty("reference");
    expect(logToFile).not.toHaveBeenCalled();
  });
});
