import { Test } from "@nestjs/testing";
import { HttpException } from "@nestjs/common";
import { PushController } from "./push.controller";
import { PushService } from "./push.service";
import { TripsService } from "../trips/trips.service";
import { PushSubscribeDto } from "./dto/push.dto";

/**
 * One rule runs through this controller: never accept a subscription this
 * deploy cannot send to. A stored subscription against a server with no VAPID
 * keys, against a trip nobody created, or against a URL no push service issued
 * produces no notification and no error — the visitor is left looking at a
 * switch that is on and does nothing, which is the worst state this feature has.
 */
describe("PushController", () => {
  const VALID = {
    endpoint: "https://fcm.googleapis.com/fcm/send/eX9-abc",
    p256dh: "BN4Gv",
    auth: "k9Qb",
    tripId: "n7Qk2Fd3Xb9pLmZa",
    locale: "de-AT",
    timezone: "Europe/Berlin",
    topics: ["next-up"],
  } as PushSubscribeDto;

  let controller: PushController;
  let subscribe: jest.Mock;
  let trip: unknown;

  const withVapid = <T>(run: () => T): T => {
    const before = {
      pub: process.env.VAPID_PUBLIC_KEY,
      priv: process.env.VAPID_PRIVATE_KEY,
      sub: process.env.VAPID_SUBJECT,
    };
    process.env.VAPID_PUBLIC_KEY = "test-public";
    process.env.VAPID_PRIVATE_KEY = "test-private";
    process.env.VAPID_SUBJECT = "mailto:hello@park.fan";
    try {
      return run();
    } finally {
      restore("VAPID_PUBLIC_KEY", before.pub);
      restore("VAPID_PRIVATE_KEY", before.priv);
      restore("VAPID_SUBJECT", before.sub);
    }
  };

  const restore = (key: string, value: string | undefined) => {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  };

  beforeEach(async () => {
    subscribe = jest.fn().mockImplementation(async () => ({ id: "sub-1" }));
    trip = { id: VALID.tripId };

    const moduleRef = await Test.createTestingModule({
      controllers: [PushController],
      providers: [
        {
          provide: PushService,
          useValue: { subscribe, unsubscribe: jest.fn() },
        },
        {
          provide: TripsService,
          useValue: { find: jest.fn().mockImplementation(async () => trip) },
        },
      ],
    }).compile();

    controller = moduleRef.get(PushController);
    // Nothing configured unless a test says so.
    restore("VAPID_PUBLIC_KEY", undefined);
    restore("VAPID_PRIVATE_KEY", undefined);
    restore("VAPID_SUBJECT", undefined);
  });

  it("says push is unavailable, and offers no key, when nothing is configured", () => {
    const status = controller.status();
    expect(status.available).toBe(false);
    expect(status.publicKey).toBeUndefined();
    // The topic list is still published: a browser that cannot subscribe still
    // needs to know what it would be subscribing to.
    expect(status.topics).toEqual(["next-up"]);
  });

  it("hands over the public key once configured", () => {
    withVapid(() => {
      const status = controller.status();
      expect(status.available).toBe(true);
      expect(status.publicKey).toBe("test-public");
    });
  });

  it("answers 503 rather than storing a subscription it can never send to", async () => {
    await expect(controller.subscribe({ ...VALID })).rejects.toMatchObject({
      status: 503,
    });
    expect(subscribe).not.toHaveBeenCalled();
  });

  it("stores a valid subscription", async () => {
    await withVapid(async () => {
      await controller.subscribe({ ...VALID });
      expect(subscribe).toHaveBeenCalledWith(
        expect.objectContaining({
          endpoint: VALID.endpoint,
          tripId: VALID.tripId,
          locale: "de-AT",
          timezone: "Europe/Berlin",
          topics: ["next-up"],
        }),
      );
    });
  });

  it("refuses an endpoint no push service issued", async () => {
    await withVapid(async () => {
      for (const endpoint of [
        "https://evil.test/collect",
        "https://web.push.apple.com.evil.test/x",
        "http://fcm.googleapis.com/fcm/send/x",
        "https://ml-service:8000/predict",
      ]) {
        await expect(
          controller.subscribe({ ...VALID, endpoint }),
        ).rejects.toBeInstanceOf(HttpException);
      }
      expect(subscribe).not.toHaveBeenCalled();
    });
  });

  it("refuses a subscription against a trip nobody created", async () => {
    await withVapid(async () => {
      trip = null;
      await expect(controller.subscribe({ ...VALID })).rejects.toMatchObject({
        status: 404,
      });
      expect(subscribe).not.toHaveBeenCalled();
    });
  });

  it("refuses a body whose topics are all unknown", async () => {
    await withVapid(async () => {
      await expect(
        controller.subscribe({ ...VALID, topics: ["ride-down"] }),
      ).rejects.toMatchObject({ status: 400 });
      expect(subscribe).not.toHaveBeenCalled();
    });
  });

  it("falls back to English for an unreadable locale rather than refusing", async () => {
    await withVapid(async () => {
      await controller.subscribe({ ...VALID, locale: "not a tag" });
      expect(subscribe).toHaveBeenCalledWith(
        expect.objectContaining({ locale: "en" }),
      );
    });
  });

  it("drops a timezone that is not a zone rather than storing it", async () => {
    await withVapid(async () => {
      await controller.subscribe({ ...VALID, timezone: "'; DROP TABLE" });
      expect(subscribe).toHaveBeenCalledWith(
        expect.objectContaining({ timezone: null }),
      );
    });
  });
});
