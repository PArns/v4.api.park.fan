import { Test } from "@nestjs/testing";
import { HttpException } from "@nestjs/common";
import { RideAlertsController } from "./ride-alerts.controller";
import {
  RideAlertsService,
  MAX_RIDE_ALERTS_PER_SUBSCRIPTION,
} from "./ride-alerts.service";
import { PushService } from "../push/push.service";
import { PushFollowWriteRateLimitService } from "../push/push-follow-write-rate-limit.service";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { RideAlert } from "./entities/ride-alert.entity";

/**
 * One rule runs through this controller, same as `push` and `trips`: never
 * accept a write that can never do anything — no subscription to notify, no
 * such ride, or a park whose wait times this deploy can never read.
 */
describe("RideAlertsController", () => {
  const ENDPOINT = "https://fcm.googleapis.com/fcm/send/e1";

  const park = (overrides: Partial<Park> = {}) =>
    ({
      id: "park-1",
      name: "Phantasialand",
      slug: "phantasialand",
      citySlug: "bruehl",
      countrySlug: "germany",
      continentSlug: "europe",
      ...overrides,
    }) as Park;

  const attraction = (overrides: Partial<Attraction> = {}) =>
    ({
      id: "ride-1",
      name: "Taron",
      slug: "taron",
      park: park(),
      retiredAt: null,
      isSeasonal: false,
      seasonMonths: null,
      seasonOutSince: null,
      ...overrides,
    }) as unknown as Attraction;

  let controller: RideAlertsController;
  let rideAlerts: {
    findAttractionForAlert: jest.Mock;
    countForSubscription: jest.Mock;
    find: jest.Mock;
    upsert: jest.Mock;
    remove: jest.Mock;
    listForSubscription: jest.Mock;
  };
  let pushService: { findByEndpoint: jest.Mock };
  let rateLimit: { check: jest.Mock };

  beforeEach(async () => {
    rideAlerts = {
      findAttractionForAlert: jest.fn().mockImplementation(async () => ({
        attraction: attraction(),
        park: park(),
      })),
      countForSubscription: jest.fn().mockResolvedValue(0),
      find: jest.fn().mockResolvedValue(null),
      upsert: jest.fn().mockImplementation(
        async (
          subscriptionId: string,
          attractionId: string,
          thresholdMinutes: number,
        ) =>
          ({
            id: "alert-1",
            subscriptionId,
            attractionId,
            thresholdMinutes,
            armed: true,
            lastTriggeredAt: null,
            createdAt: new Date("2026-09-01T00:00:00.000Z"),
            updatedAt: new Date("2026-09-01T00:00:00.000Z"),
          }) as RideAlert,
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
      controllers: [RideAlertsController],
      providers: [
        { provide: RideAlertsService, useValue: rideAlerts },
        { provide: PushService, useValue: pushService },
        { provide: PushFollowWriteRateLimitService, useValue: rateLimit },
      ],
    }).compile();

    controller = moduleRef.get(RideAlertsController);
  });

  const req = () => ({ headers: {}, socket: {} }) as never;

  it("refuses to list when the endpoint has no subscription", async () => {
    pushService.findByEndpoint.mockResolvedValueOnce(null);
    await expect(controller.list(ENDPOINT)).rejects.toMatchObject({
      status: 404,
    });
  });

  it("creates an alert for a known attraction with readable wait times", async () => {
    const result = await controller.create(
      { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
      req(),
    );
    expect(rideAlerts.upsert).toHaveBeenCalledWith("sub-1", "ride-1", 20);
    expect(result.thresholdMinutes).toBe(20);
    expect(result.armed).toBe(true);
    expect(result.outOfSeason).toBe(false);
    expect(result.retired).toBe(false);
    expect(result.path).toBe(
      "/parks/europe/germany/bruehl/phantasialand/taron",
    );
  });

  it("surfaces a ride confirmed out of season rather than hiding a dormant alert", async () => {
    // Fixed at 2026-08-21 — August, so a December-only ride is confirmed out.
    jest.useFakeTimers().setSystemTime(new Date("2026-08-21T12:00:00.000Z"));
    try {
      rideAlerts.findAttractionForAlert.mockResolvedValueOnce({
        attraction: attraction({ isSeasonal: true, seasonMonths: [12] }),
        park: park(),
      });
      const result = await controller.create(
        { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
        req(),
      );
      expect(result.outOfSeason).toBe(true);
    } finally {
      jest.useRealTimers();
    }
  });

  it("surfaces a ride retired after the alert was created", async () => {
    rideAlerts.findAttractionForAlert.mockResolvedValueOnce({
      attraction: attraction({ retiredAt: new Date("2026-02-15") }),
      park: park(),
    });
    const result = await controller.create(
      { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
      req(),
    );
    expect(result.retired).toBe(true);
  });

  it("refuses a subscription-less endpoint before touching the attraction", async () => {
    pushService.findByEndpoint.mockResolvedValueOnce(null);
    await expect(
      controller.create(
        { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
        req(),
      ),
    ).rejects.toMatchObject({ status: 404 });
    expect(rideAlerts.findAttractionForAlert).not.toHaveBeenCalled();
  });

  it("refuses an attraction that does not exist or is retired", async () => {
    rideAlerts.findAttractionForAlert.mockResolvedValueOnce(null);
    await expect(
      controller.create(
        { endpoint: ENDPOINT, attractionId: "gone", thresholdMinutes: 20 },
        req(),
      ),
    ).rejects.toMatchObject({ status: 404 });
    expect(rideAlerts.upsert).not.toHaveBeenCalled();
  });

  it("refuses a park whose wait times can never be read", async () => {
    // Hansa-Park, per src/parks/data/live-wait-time-sources.ts.
    rideAlerts.findAttractionForAlert.mockResolvedValueOnce({
      attraction: attraction({
        park: park({ citySlug: "sierksdorf", slug: undefined }),
      }),
      park: park({ citySlug: "sierksdorf", slug: "hansa-park" }),
    });
    await expect(
      controller.create(
        { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
        req(),
      ),
    ).rejects.toMatchObject({ status: 400 });
    expect(rideAlerts.upsert).not.toHaveBeenCalled();
  });

  it("refuses a new alert once the per-subscription cap is reached", async () => {
    rideAlerts.countForSubscription.mockResolvedValueOnce(
      MAX_RIDE_ALERTS_PER_SUBSCRIPTION,
    );
    await expect(
      controller.create(
        { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
        req(),
      ),
    ).rejects.toMatchObject({ status: 400 });
    expect(rideAlerts.upsert).not.toHaveBeenCalled();
  });

  it("does not enforce the cap when updating an alert already held", async () => {
    rideAlerts.find.mockResolvedValueOnce({ id: "alert-1" } as RideAlert);
    rideAlerts.countForSubscription.mockResolvedValueOnce(
      MAX_RIDE_ALERTS_PER_SUBSCRIPTION,
    );
    await controller.create(
      { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 15 },
      req(),
    );
    expect(rideAlerts.upsert).toHaveBeenCalledWith("sub-1", "ride-1", 15);
  });

  it("removes an alert idempotently", async () => {
    await controller.remove(
      { endpoint: ENDPOINT, attractionId: "ride-1" },
      req(),
    );
    expect(rideAlerts.remove).toHaveBeenCalledWith("sub-1", "ride-1");
  });

  it("answers 429 once the write limiter says no", async () => {
    rateLimit.check.mockResolvedValueOnce({
      allowed: false,
      retryAfterSeconds: 42,
    });
    await expect(
      controller.create(
        { endpoint: ENDPOINT, attractionId: "ride-1", thresholdMinutes: 20 },
        req(),
      ),
    ).rejects.toBeInstanceOf(HttpException);
    expect(rideAlerts.upsert).not.toHaveBeenCalled();
  });
});
