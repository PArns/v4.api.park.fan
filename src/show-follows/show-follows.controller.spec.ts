import { Test } from "@nestjs/testing";
import { HttpException } from "@nestjs/common";
import { ShowFollowsController } from "./show-follows.controller";
import {
  ShowFollowsService,
  MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION,
} from "./show-follows.service";
import { PushService } from "../push/push.service";
import { PushFollowWriteRateLimitService } from "../push/push-follow-write-rate-limit.service";
import { PushFollowAccessGuard } from "../push/push-follow-access.guard";
import { Show } from "../shows/entities/show.entity";
import { Park } from "../parks/entities/park.entity";
import { ShowFollow } from "./entities/show-follow.entity";

describe("ShowFollowsController", () => {
  const ENDPOINT = "https://fcm.googleapis.com/fcm/send/e1";

  const park = () =>
    ({
      id: "park-1",
      name: "Europa-Park",
      slug: "europa-park",
      timezone: "Europe/Berlin",
      citySlug: "rust",
      countrySlug: "germany",
      continentSlug: "europe",
    }) as Park;

  const show = () =>
    ({
      id: "show-1",
      name: "Feuerwerk",
      slug: "feuerwerk",
      park: park(),
    }) as Show;

  let controller: ShowFollowsController;
  let showFollows: {
    findShowForFollow: jest.Mock;
    countForSubscription: jest.Mock;
    find: jest.Mock;
    upsert: jest.Mock;
    remove: jest.Mock;
    listForSubscription: jest.Mock;
  };
  let pushService: { findByEndpoint: jest.Mock };
  let rateLimit: { check: jest.Mock };

  beforeEach(async () => {
    showFollows = {
      findShowForFollow: jest
        .fn()
        .mockImplementation(async () => ({ show: show(), park: park() })),
      countForSubscription: jest.fn().mockResolvedValue(0),
      find: jest.fn().mockResolvedValue(null),
      upsert: jest.fn().mockImplementation(
        async (
          subscriptionId: string,
          showId: string,
          startTime: Date | null,
        ) =>
          ({
            id: "follow-1",
            subscriptionId,
            showId,
            startTime: startTime ?? null,
            createdAt: new Date("2026-09-01T00:00:00.000Z"),
            updatedAt: new Date("2026-09-01T00:00:00.000Z"),
          }) as ShowFollow,
      ),
      remove: jest.fn().mockResolvedValue(undefined),
      listForSubscription: jest.fn().mockResolvedValue([]),
    };
    pushService = {
      findByEndpoint: jest
        .fn()
        .mockImplementation(async () => ({ id: "sub-1", endpoint: ENDPOINT })),
    };
    rateLimit = {
      check: jest
        .fn()
        .mockResolvedValue({ allowed: true, retryAfterSeconds: 0 }),
    };

    const moduleRef = await Test.createTestingModule({
      controllers: [ShowFollowsController],
      providers: [
        { provide: ShowFollowsService, useValue: showFollows },
        { provide: PushService, useValue: pushService },
        { provide: PushFollowWriteRateLimitService, useValue: rateLimit },
        // A real instance, not a mock — same reasoning as
        // `ride-alerts.controller.spec.ts`.
        PushFollowAccessGuard,
      ],
    }).compile();

    controller = moduleRef.get(ShowFollowsController);
  });

  const req = () => ({ headers: {}, socket: {} }) as never;

  it("refuses to list when the endpoint has no subscription", async () => {
    pushService.findByEndpoint.mockResolvedValueOnce(null);
    await expect(controller.list(ENDPOINT)).rejects.toMatchObject({
      status: 404,
    });
  });

  it("follows a known show", async () => {
    const result = await controller.create(
      { endpoint: ENDPOINT, showId: "show-1" },
      req(),
    );
    expect(showFollows.upsert).toHaveBeenCalledWith("sub-1", "show-1", null);
    expect(result.path).toBe("/parks/europe/germany/rust/europa-park#shows");
  });

  it("refuses a subscription-less endpoint before touching the show", async () => {
    pushService.findByEndpoint.mockResolvedValueOnce(null);
    await expect(
      controller.create({ endpoint: ENDPOINT, showId: "show-1" }, req()),
    ).rejects.toMatchObject({ status: 404 });
    expect(showFollows.findShowForFollow).not.toHaveBeenCalled();
  });

  it("refuses a show that does not exist", async () => {
    showFollows.findShowForFollow.mockResolvedValueOnce(null);
    await expect(
      controller.create({ endpoint: ENDPOINT, showId: "gone" }, req()),
    ).rejects.toMatchObject({ status: 404 });
    expect(showFollows.upsert).not.toHaveBeenCalled();
  });

  it("refuses a new follow once the per-subscription cap is reached", async () => {
    showFollows.countForSubscription.mockResolvedValueOnce(
      MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION,
    );
    await expect(
      controller.create({ endpoint: ENDPOINT, showId: "show-1" }, req()),
    ).rejects.toMatchObject({ status: 400 });
    expect(showFollows.upsert).not.toHaveBeenCalled();
  });

  it("does not enforce the cap when the show is already followed", async () => {
    showFollows.find.mockResolvedValueOnce({ id: "follow-1" } as ShowFollow);
    showFollows.countForSubscription.mockResolvedValueOnce(
      MAX_SHOW_FOLLOWS_PER_SUBSCRIPTION,
    );
    await controller.create({ endpoint: ENDPOINT, showId: "show-1" }, req());
    expect(showFollows.upsert).toHaveBeenCalledWith("sub-1", "show-1", null);
  });

  it("passes a chosen performance through to the row", async () => {
    const startTime = "2026-09-08T23:10:00.000Z";
    const result = await controller.create(
      { endpoint: ENDPOINT, showId: "show-1", startTime },
      req(),
    );
    expect(showFollows.upsert).toHaveBeenCalledWith(
      "sub-1",
      "show-1",
      new Date(startTime),
    );
    expect(result.startTime).toBe(startTime);
  });

  it("reports the park's zone, so a clock time can be read as the park posts it", async () => {
    const result = await controller.create(
      { endpoint: ENDPOINT, showId: "show-1" },
      req(),
    );
    expect(result.timezone).toBe("Europe/Berlin");
    expect(result.startTime).toBeNull();
  });

  it("unfollows idempotently", async () => {
    await controller.remove({ endpoint: ENDPOINT, showId: "show-1" }, req());
    expect(showFollows.remove).toHaveBeenCalledWith("sub-1", "show-1");
  });

  it("answers 429 once the write limiter says no", async () => {
    rateLimit.check.mockResolvedValueOnce({
      allowed: false,
      retryAfterSeconds: 42,
    });
    await expect(
      controller.create({ endpoint: ENDPOINT, showId: "show-1" }, req()),
    ).rejects.toBeInstanceOf(HttpException);
    expect(showFollows.upsert).not.toHaveBeenCalled();
  });
});
