import { Test } from "@nestjs/testing";
import { Request } from "express";
import { TripsController } from "./trips.controller";
import { TripsService } from "./trips.service";
import { TripWriteRateLimitService } from "./trip-write-rate-limit.service";

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
    // Deliberately NOT asserting `retryAfterSeconds` on the wire, though this
    // route puts it in the exception body exactly as POST and PUT do:
    // `HttpExceptionFilter` rebuilds every error response from `message` and
    // `error` alone, so the figure never leaves the process and no
    // `Retry-After` header is set either. Pinning it here would promise a
    // client something it cannot read — see PAR-146.
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
