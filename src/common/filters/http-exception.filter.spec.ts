import {
  BadRequestException,
  HttpException,
  HttpStatus,
  Logger,
} from "@nestjs/common";
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
function hostFor(
  url: string,
  method = "POST",
  { headersSent = false }: { headersSent?: boolean } = {},
) {
  const json = jest.fn();
  const status = jest.fn(() => ({ json }));
  const header = jest.fn();
  const host = {
    switchToHttp: () => ({
      getResponse: () => ({ status, headersSent, header }),
      getRequest: () => ({ method, url }),
    }),
  } as unknown as ArgumentsHost;
  return { host, status, json, header };
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
 * What a handler attaches to an error body has to arrive.
 *
 * The filter rebuilt every response from `message` and `error`, so a field a
 * throw site added was gone by the time the response was written. Four sites do
 * that today, and the expensive one is the rate limiter: `retryAfterSeconds`
 * has never reached a single client on any 429 this API raises by hand, on
 * `/v1/trips` or on the two push-follow routes. The caller learned it had to
 * wait and never how long.
 */
describe("HttpExceptionFilter — fields a handler attaches", () => {
  beforeEach(() => {
    jest.spyOn(Logger.prototype, "warn").mockImplementation();
    jest.spyOn(Logger.prototype, "error").mockImplementation();
  });

  afterEach(() => jest.restoreAllMocks());

  /** The 429 both limiters throw, shape for shape. */
  const tooManyRequests = (retryAfterSeconds: unknown) =>
    new HttpException(
      {
        statusCode: HttpStatus.TOO_MANY_REQUESTS,
        message: "Too many trip writes from this address",
        retryAfterSeconds,
      },
      HttpStatus.TOO_MANY_REQUESTS,
    );

  it("carries retryAfterSeconds to the client, in the body and in the header", () => {
    const { host, json, header } = hostFor("/v1/trips");

    new HttpExceptionFilter().catch(tooManyRequests(1800), host);

    expect(json.mock.calls[0][0]).toMatchObject({
      statusCode: 429,
      message: "Too many trip writes from this address",
      retryAfterSeconds: 1800,
    });
    expect(header).toHaveBeenCalledWith("Retry-After", "1800");
  });

  it("rounds a fractional wait up, never down to zero", () => {
    // `Retry-After` has no sub-second form. Rounded down, the 0.4 s a limiter
    // is still holding would read as "come back now" to the caller it just
    // turned away — and the body keeps the exact figure either way.
    const { host, json, header } = hostFor("/v1/trips");

    new HttpExceptionFilter().catch(tooManyRequests(0.4), host);

    expect(header).toHaveBeenCalledWith("Retry-After", "1");
    expect(json.mock.calls[0][0].retryAfterSeconds).toBe(0.4);
  });

  it.each([
    ["no figure at all", undefined],
    ["a negative one", -5],
    ["a non-finite one", Number.POSITIVE_INFINITY],
    ["a string", "1800"],
    // Past 2^53 `String()` writes `1e+21`, which is not `delta-seconds` and is
    // not something a client parses. Neither limiter can reach it — both
    // figures are a Redis TTL — and the guard belongs to the header rather
    // than to today's two callers.
    ["one too large to write as seconds", 1e21],
  ])("sets no Retry-After for %s", (_label, value) => {
    // A header the caller can trust or none: a guessed wait is worse than an
    // absent one, because a client that sees the header stops thinking.
    const { host, header } = hostFor("/v1/trips");

    new HttpExceptionFilter().catch(tooManyRequests(value), host);

    expect(header).not.toHaveBeenCalled();
  });

  it("sets no Retry-After on a status that is not a 429", () => {
    // The header belongs to the limiter. A 400 that happens to carry a field of
    // that name is not one, and answering it with a wait would send a caller
    // away from a request that will fail exactly the same way later.
    const { host, json, header } = hostFor("/v1/trips");

    new HttpExceptionFilter().catch(
      new HttpException(
        { message: "Not a trip payload", retryAfterSeconds: 30 },
        HttpStatus.BAD_REQUEST,
      ),
      host,
    );

    expect(header).not.toHaveBeenCalled();
    expect(json.mock.calls[0][0].retryAfterSeconds).toBe(30);
  });

  it("carries a field that is not about waiting, on a status that is not a 429", () => {
    // The rule is about the body, not about 429s. `admin/cache/reset` refuses
    // an unconfirmed FLUSHALL with a `warning` that says what the confirmation
    // would have done — a sentence that only means anything to the person
    // reading the refusal.
    const { host, json } = hostFor("/v1/admin/cache/reset");

    new HttpExceptionFilter().catch(
      new HttpException(
        {
          statusCode: HttpStatus.BAD_REQUEST,
          message: "FLUSHALL requires explicit confirmation.",
          warning: "This will delete ALL Redis cache data.",
        },
        HttpStatus.BAD_REQUEST,
      ),
      host,
    );

    expect(json.mock.calls[0][0]).toMatchObject({
      statusCode: 400,
      warning: "This will delete ALL Redis cache data.",
    });
  });

  // The other half of this change is not here, and deliberately so: making the
  // filter generic moved the decision about what a client may learn to the
  // throw site. `AdminAuthController` used to attach Cloudflare's own Turnstile
  // error code to a 403 on a public route, relying on the filter to delete it.
  // It is logged instead now, and pinned in `admin-auth.login-turnstile.spec.ts`
  // — a case written here would assert against a body this file made up.

  it("lets no thrown field redefine the envelope", () => {
    // `path` is redacted here and `stack` is withheld in production, so both
    // are the filter's to write. A body that could set them would undo the two
    // things this filter exists to guarantee — which is why the extras are
    // spread first and every field below overwrites them.
    const { host, json } = hostFor("/v1/admin/x?pass=S3CR3T");

    new HttpExceptionFilter().catch(
      new HttpException(
        {
          message: "no",
          path: "/somewhere/else",
          timestamp: "1999-01-01T00:00:00.000Z",
          statusCode: 200,
          reference: "abcdef",
          stack: "Error: at the caller's choosing",
        },
        HttpStatus.BAD_REQUEST,
      ),
      host,
    );

    const body = json.mock.calls[0][0];
    expect(body.path).toBe("/v1/admin/x?pass=***");
    expect(body.statusCode).toBe(400);
    expect(body.timestamp).not.toBe("1999-01-01T00:00:00.000Z");
    expect(body).not.toHaveProperty("reference");
    // Outside production the filter writes the real stack over it; in
    // production it writes none and the reserved-key rule drops this one. The
    // thrown string may not appear either way.
    expect(body.stack ?? "").not.toContain("at the caller's choosing");
  });

  it("drops an extra that cannot be serialized, and keeps the ones beside it", () => {
    // This filter is the last thing that can still answer the request. A
    // circular reference in a thrown body would throw inside `response.json()`,
    // where nothing catches it — the caller would get no answer at all instead
    // of an answer missing one field.
    //
    // Field by field rather than all or nothing: an unserializable `detail`
    // must not take the `retryAfterSeconds` next to it down, which is the whole
    // field this pass-through exists for.
    const circular: Record<string, unknown> = { name: "loop" };
    circular.self = circular;
    const { host, json, header } = hostFor("/v1/trips");

    expect(() =>
      new HttpExceptionFilter().catch(
        new HttpException(
          {
            message: "boom",
            detail: circular,
            retryAfterSeconds: 30,
          },
          HttpStatus.TOO_MANY_REQUESTS,
        ),
        host,
      ),
    ).not.toThrow();

    const body = json.mock.calls[0][0];
    expect(body).not.toHaveProperty("detail");
    expect(body.retryAfterSeconds).toBe(30);
    expect(body.message).toBe("boom");
    expect(header).toHaveBeenCalledWith("Retry-After", "30");
    expect(() => JSON.stringify(body)).not.toThrow();
  });

  it("leaves a plain string body exactly as it was", () => {
    // Most of this codebase throws `new HttpException("Trip not found", 404)`.
    // There is nothing to carry there, and the response may not grow a field.
    const { host, json, header } = hostFor("/v1/trips/abc");

    new HttpExceptionFilter().catch(
      new HttpException("Trip not found", HttpStatus.NOT_FOUND),
      host,
    );

    const body = json.mock.calls[0][0] as Record<string, unknown>;
    expect(body).toMatchObject({
      statusCode: 404,
      path: "/v1/trips/abc",
      message: "Trip not found",
    });
    // The envelope and nothing else. `stack` is left out of the comparison
    // because it depends on `NODE_ENV`, and that branch predates this change.
    expect(
      Object.keys(body)
        .filter((key) => key !== "stack")
        .sort(),
    ).toEqual(["message", "path", "statusCode", "timestamp"]);
    expect(header).not.toHaveBeenCalled();
  });

  it("does not set Retry-After on a response already on the wire", () => {
    // Same reason the body is not written twice: the headers are gone.
    const { host, header } = hostFor("/v1/trips", "POST", {
      headersSent: true,
    });

    new HttpExceptionFilter().catch(tooManyRequests(1800), host);

    expect(header).not.toHaveBeenCalled();
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

  it("does not write a second response over one already on the wire", () => {
    // The filter is where ERR_HTTP_HEADERS_SENT lands after an interceptor
    // tried to set a header on an abandoned request. Answering it with
    // `response.status().json()` throws the very same error a second time —
    // this time inside the filter, where nothing catches it.
    const { host, status, json } = hostFor("/v1/parks/x", "GET", {
      headersSent: true,
    });

    expect(() =>
      new HttpExceptionFilter().catch(
        new Error("Cannot set headers after they are sent to the client"),
        host,
      ),
    ).not.toThrow();

    expect(status).not.toHaveBeenCalled();
    expect(json).not.toHaveBeenCalled();
  });

  it("still records the failure so an abandoned request is not silent", () => {
    // Skipping the write must not skip the bookkeeping: the request still
    // failed, and the error log is the only place that says so.
    const { host } = hostFor("/v1/parks/x", "GET", { headersSent: true });

    new HttpExceptionFilter().catch(new Error("boom"), host);

    expect(logToFile).toHaveBeenCalledTimes(1);
    const [file] = (logToFile as jest.Mock).mock.calls[0] as [string];
    expect(file).toBe("api-errors");
  });
});
