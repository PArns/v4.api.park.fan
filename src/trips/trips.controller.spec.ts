import { Test } from "@nestjs/testing";
import { Logger } from "@nestjs/common";
import type { ArgumentsHost } from "@nestjs/common";
import { Request } from "express";
import { TripsController } from "./trips.controller";
import { TripsService } from "./trips.service";
import { TripWriteRateLimitService } from "./trip-write-rate-limit.service";
import { HttpExceptionFilter } from "../common/filters/http-exception.filter";

/**
 * What a caller actually receives when a controller call throws.
 *
 * The globally registered `HttpExceptionFilter` writes every error response on
 * this API, so a controller's throw is a draft and not the answer. Running the
 * real filter is the difference between asserting what this route intends and
 * asserting what leaves the process.
 */
async function throughFilter(call: () => Promise<unknown>) {
  // Called exactly once: the limiter counts per call, and a second one would
  // make this helper's own bookkeeping part of what the test measures.
  const marker = Symbol("did not throw");
  const thrown = await call().then(
    () => marker,
    (error: unknown) => error,
  );
  if (thrown === marker) throw new Error("expected the call to throw");

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
      getRequest: () => ({ method: "POST", url: "/v1/trips" }),
    }),
  } as unknown as ArgumentsHost;

  const warn = jest.spyOn(Logger.prototype, "warn").mockImplementation();
  try {
    new HttpExceptionFilter().catch(thrown, host);
  } finally {
    warn.mockRestore();
  }

  return { body: json.mock.calls[0][0] as Record<string, unknown>, headers };
}

/**
 * The delete half of the credential-is-the-id trade.
 *
 * Everything worth pinning here is a thing a caller could otherwise learn or
 * spend that they should not: which ids are shaped like real ones (so a
 * malformed id answers exactly as a missing one, and never reaches the
 * database), how many attempts they get (the update bucket, counted before any
 * work, so a miss costs the same as a hit), and whether a 404 means "gone" or
 * "never was" (it means neither — see `TripsService.find`).
 */
describe("TripsController · DELETE", () => {
  const ID = "n7Qk2Fd3Xb9pLmZa";

  let controller: TripsController;
  let remove: jest.Mock;
  let check: jest.Mock;

  const request = (ip = "203.0.113.7") =>
    ({ headers: { "x-forwarded-for": ip }, ip }) as unknown as Request;

  beforeEach(async () => {
    remove = jest.fn().mockResolvedValue(true);
    check = jest
      .fn()
      .mockResolvedValue({ allowed: true, retryAfterSeconds: 0 });

    const moduleRef = await Test.createTestingModule({
      controllers: [TripsController],
      providers: [
        {
          provide: TripsService,
          useValue: {
            create: jest.fn(),
            find: jest.fn(),
            update: jest.fn(),
            remove,
          },
        },
        { provide: TripWriteRateLimitService, useValue: { check } },
      ],
    }).compile();

    controller = moduleRef.get(TripsController);
  });

  it("deletes a trip and answers with no content", async () => {
    await expect(controller.remove(ID, request())).resolves.toBeUndefined();
    expect(remove).toHaveBeenCalledWith(ID);
  });

  it("answers 404 for an id with nothing behind it", async () => {
    remove.mockResolvedValue(false);
    await expect(controller.remove(ID, request())).rejects.toMatchObject({
      status: 404,
    });
  });

  it("answers 404 for an id that cannot be a trip id, without a lookup", async () => {
    // Not 400: the caller learns the same thing either way, and the id is the
    // whole of the authorisation, so this route says as little as it can about
    // which ids are shaped like real ones. Same answer as GET and PUT give.
    for (const bad of ["", "has spaces", "a/b", "x".repeat(33), "über"]) {
      await expect(controller.remove(bad, request())).rejects.toMatchObject({
        status: 404,
      });
    }
    expect(remove).not.toHaveBeenCalled();
  });

  it("counts against the update bucket, not a bucket of its own", async () => {
    // A third bucket would hand a script a fresh allowance for guessing ids.
    await controller.remove(ID, request());
    expect(check).toHaveBeenCalledWith("203.0.113.7", "update");
  });

  it("answers 429 over the limit, and does not touch the trip", async () => {
    check.mockResolvedValue({ allowed: false, retryAfterSeconds: 1800 });
    await expect(controller.remove(ID, request())).rejects.toMatchObject({
      status: 429,
    });
    expect(remove).not.toHaveBeenCalled();
  });

  it("tells the caller how long to wait — through the filter, not just in the throw", async () => {
    // The figure is pinned where it is read rather than where it is written.
    // It sat in the exception body all along; what it never survived was
    // `HttpExceptionFilter`, which rebuilt the response from `message` and
    // `error` alone and dropped everything else (PAR-146). An assertion on the
    // thrown object would have stayed green through all of that, which is why
    // the real filter runs here.
    check.mockResolvedValue({ allowed: false, retryAfterSeconds: 1800 });

    const { body, headers } = await throughFilter(() =>
      controller.remove(ID, request()),
    );

    expect(body).toMatchObject({ statusCode: 429, retryAfterSeconds: 1800 });
    expect(headers["Retry-After"]).toBe("1800");
  });

  it("says the same thing on all three write verbs", async () => {
    // One limiter, one answer. The bug was reported against DELETE because
    // that is the route PAR-136 added, and it was never about the route.
    check.mockResolvedValue({ allowed: false, retryAfterSeconds: 90 });
    const payload = { payload: { version: 1, parkId: "x", items: [] } };

    for (const call of [
      () => controller.create(payload as never, request()),
      () => controller.update(ID, payload as never, request()),
      () => controller.remove(ID, request()),
    ]) {
      const { body, headers } = await throughFilter(call);
      expect(body).toMatchObject({ statusCode: 429, retryAfterSeconds: 90 });
      expect(headers["Retry-After"]).toBe("90");
    }
  });

  it("spends an attempt even on an id that is not there", async () => {
    // Counted before the work, exactly as the PUT does it: a limiter that only
    // counted hits would let somebody enumerate ids for free, and a miss is the
    // only answer an enumeration ever gets.
    remove.mockResolvedValue(false);
    await expect(controller.remove(ID, request())).rejects.toMatchObject({
      status: 404,
    });
    expect(check).toHaveBeenCalledTimes(1);
  });
});
