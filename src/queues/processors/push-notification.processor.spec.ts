import { Test } from "@nestjs/testing";
import { PushNotificationProcessor } from "./push-notification.processor";
import { PushService } from "../../push/push.service";
import { TripsService } from "../../trips/trips.service";
import { ShowFollowsService } from "../../show-follows/show-follows.service";
import { ShowsService } from "../../shows/shows.service";
import { REDIS_CLIENT } from "../../common/redis/redis.module";

/**
 * The two halves of `handleDue` are unrelated (a trip topic vs. a followed
 * show, which has no topic at all) and share only the send/dedupe plumbing —
 * so the property worth pinning is that neither can silently swallow or
 * duplicate the other's work when both fire in the same tick.
 */
describe("PushNotificationProcessor", () => {
  const withVapid = async <T>(run: () => Promise<T> | T): Promise<T> => {
    const before = {
      pub: process.env.VAPID_PUBLIC_KEY,
      priv: process.env.VAPID_PRIVATE_KEY,
      sub: process.env.VAPID_SUBJECT,
    };
    process.env.VAPID_PUBLIC_KEY = "test-public";
    process.env.VAPID_PRIVATE_KEY = "test-private";
    process.env.VAPID_SUBJECT = "mailto:hello@park.fan";
    try {
      return await run();
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

  const tripSubscription = {
    id: "sub-trip",
    endpoint: "https://fcm.googleapis.com/fcm/send/trip",
    p256dh: "k",
    auth: "a",
    tripId: "trip-1",
    topics: ["next-up"],
    locale: "de",
    timezone: "Europe/Berlin",
    failureCount: 0,
    lastNotifiedAt: null,
  };

  const showSubscription = {
    id: "sub-show",
    endpoint: "https://fcm.googleapis.com/fcm/send/show",
    p256dh: "k",
    auth: "a",
    tripId: null,
    topics: [],
    locale: "de",
    timezone: null,
    failureCount: 0,
    lastNotifiedAt: null,
  };

  // 2026-10-17, 18:00 UTC.
  const NOW = Date.parse("2026-10-17T18:00:00.000Z");

  let processor: PushNotificationProcessor;
  let pushService: {
    allSubscriptions: jest.Mock;
    findByIds: jest.Mock;
    send: jest.Mock;
  };
  let tripsService: { find: jest.Mock };
  let showFollowsService: { allFollows: jest.Mock };
  let showsService: { findBatchCurrentStatusByShows: jest.Mock };
  let redisStore: Map<string, string>;
  let redis: { exists: jest.Mock; set: jest.Mock };

  beforeEach(async () => {
    redisStore = new Map();
    redis = {
      exists: jest.fn(async (key: string) => (redisStore.has(key) ? 1 : 0)),
      set: jest.fn(async (key: string) => {
        redisStore.set(key, "1");
        return "OK";
      }),
    };

    pushService = {
      allSubscriptions: jest.fn().mockResolvedValue([]),
      findByIds: jest.fn().mockResolvedValue(new Map()),
      send: jest.fn().mockResolvedValue(true),
    };
    tripsService = { find: jest.fn().mockResolvedValue(null) };
    showFollowsService = { allFollows: jest.fn().mockResolvedValue([]) };
    showsService = {
      findBatchCurrentStatusByShows: jest.fn().mockResolvedValue(new Map()),
    };

    const moduleRef = await Test.createTestingModule({
      providers: [
        PushNotificationProcessor,
        { provide: PushService, useValue: pushService },
        { provide: TripsService, useValue: tripsService },
        { provide: ShowFollowsService, useValue: showFollowsService },
        { provide: ShowsService, useValue: showsService },
        { provide: REDIS_CLIENT, useValue: redis },
      ],
    }).compile();

    processor = moduleRef.get(PushNotificationProcessor);
    jest.spyOn(Date, "now").mockReturnValue(NOW);
    restore("VAPID_PUBLIC_KEY", undefined);
    restore("VAPID_PRIVATE_KEY", undefined);
    restore("VAPID_SUBJECT", undefined);
  });

  afterEach(() => {
    jest.spyOn(Date, "now").mockRestore();
  });

  it("does nothing when push is not configured", async () => {
    await processor.handleDue({} as never);
    expect(pushService.allSubscriptions).not.toHaveBeenCalled();
    expect(showFollowsService.allFollows).not.toHaveBeenCalled();
  });

  it("sends a due trip notification to a subscribed topic", async () => {
    await withVapid(async () => {
      pushService.allSubscriptions.mockResolvedValueOnce([tripSubscription]);
      // NOW is 20:00 in Berlin (18:00 UTC, CEST) — 20:15 is a 15-minute lead,
      // inside dueNotifications' 10-20 minute window.
      tripsService.find.mockResolvedValueOnce({
        payload: {
          version: 2,
          parks: {
            phantasialand: {
              slug: "phantasialand",
              name: "Phantasialand",
              timezone: "Europe/Berlin",
              days: {
                "2026-10-17": {
                  entries: [
                    {
                      id: "taron-1",
                      attractionName: "Taron",
                      startMinute: 20 * 60 + 15,
                    },
                  ],
                },
              },
            },
          },
        },
      });

      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(1);
      expect(pushService.send).toHaveBeenCalledWith(
        tripSubscription,
        expect.objectContaining({ title: expect.stringContaining("Taron") }),
      );
    });
  });

  it("does not resend a trip notification already marked sent", async () => {
    await withVapid(async () => {
      pushService.allSubscriptions.mockResolvedValue([tripSubscription]);
      // 20:00 Berlin + 15 min lead, same as the test above.
      const payload = {
        version: 2,
        parks: {
          p: {
            slug: "p",
            name: "P",
            timezone: "Europe/Berlin",
            days: {
              "2026-10-17": {
                entries: [
                  {
                    id: "e1",
                    attractionName: "Ride",
                    startMinute: 20 * 60 + 15,
                  },
                ],
              },
            },
          },
        },
      };
      tripsService.find.mockResolvedValue({ payload });

      await processor.handleDue({} as never);
      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(1);
    });
  });

  it("follows have no topic to check — a ShowFollow row alone is consent", async () => {
    await withVapid(async () => {
      showFollowsService.allFollows.mockResolvedValueOnce([
        { id: "f1", subscriptionId: "sub-show", showId: "show-1" },
      ]);
      showsService.findBatchCurrentStatusByShows.mockResolvedValueOnce(
        new Map([
          [
            "show-1",
            {
              status: "OPERATING",
              showtimes: [
                { startTime: new Date(NOW + 30 * 60_000).toISOString() },
              ],
              show: {
                name: "Feuerwerk",
                park: {
                  name: "Europa-Park",
                  slug: "europa-park",
                  timezone: "Europe/Berlin",
                  continentSlug: "europe",
                  countrySlug: "germany",
                  citySlug: "rust",
                },
              },
            },
          ],
        ]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-show", showSubscription]]),
      );

      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(1);
      expect(pushService.send).toHaveBeenCalledWith(
        showSubscription,
        expect.objectContaining({
          title: expect.stringContaining("Feuerwerk"),
        }),
      );
    });
  });

  it("skips a show that is not OPERATING", async () => {
    await withVapid(async () => {
      showFollowsService.allFollows.mockResolvedValueOnce([
        { id: "f1", subscriptionId: "sub-show", showId: "show-1" },
      ]);
      showsService.findBatchCurrentStatusByShows.mockResolvedValueOnce(
        new Map([
          [
            "show-1",
            {
              status: "CLOSED",
              showtimes: [
                { startTime: new Date(NOW + 30 * 60_000).toISOString() },
              ],
              show: { name: "Feuerwerk", park: { name: "Europa-Park" } },
            },
          ],
        ]),
      );

      await processor.handleDue({} as never);
      expect(pushService.send).not.toHaveBeenCalled();
    });
  });

  it("sends both a trip and a show-follow notification in the same tick", async () => {
    await withVapid(async () => {
      pushService.allSubscriptions.mockResolvedValueOnce([tripSubscription]);
      tripsService.find.mockResolvedValueOnce({
        payload: {
          version: 2,
          parks: {
            p: {
              slug: "p",
              name: "P",
              timezone: "Europe/Berlin",
              days: {
                "2026-10-17": {
                  entries: [
                    {
                      id: "e1",
                      attractionName: "Ride",
                      startMinute: 20 * 60 + 15,
                    },
                  ],
                },
              },
            },
          },
        },
      });

      showFollowsService.allFollows.mockResolvedValueOnce([
        { id: "f1", subscriptionId: "sub-show", showId: "show-1" },
      ]);
      showsService.findBatchCurrentStatusByShows.mockResolvedValueOnce(
        new Map([
          [
            "show-1",
            {
              status: "OPERATING",
              showtimes: [
                { startTime: new Date(NOW + 30 * 60_000).toISOString() },
              ],
              show: {
                name: "Feuerwerk",
                park: { name: "Europa-Park", timezone: "Europe/Berlin" },
              },
            },
          ],
        ]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-show", showSubscription]]),
      );

      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(2);
    });
  });
});
