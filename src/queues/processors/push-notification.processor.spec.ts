import { Test } from "@nestjs/testing";
import { Logger } from "@nestjs/common";
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
    subscriptionsWithTrip: jest.Mock;
    findByIds: jest.Mock;
    send: jest.Mock;
  };
  let tripsService: { find: jest.Mock };
  let showFollowsService: { allFollows: jest.Mock };
  let showsService: {
    findBatchCurrentStatusByShows: jest.Mock;
    getShowtimesOnDate: jest.Mock;
  };
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
      subscriptionsWithTrip: jest.fn().mockResolvedValue([]),
      findByIds: jest.fn().mockResolvedValue(new Map()),
      send: jest.fn().mockResolvedValue(true),
    };
    tripsService = { find: jest.fn().mockResolvedValue(null) };
    showFollowsService = { allFollows: jest.fn().mockResolvedValue([]) };
    showsService = {
      findBatchCurrentStatusByShows: jest.fn().mockResolvedValue(new Map()),
      // The show-follow branch verifies a showtime against
      // `getShowtimesOnDate` rather than trusting the (possibly stale/
      // projected) `showtimes` on the status row — see
      // `followedShowsDueToday`. Defaults to "nothing verified"; tests that
      // expect a show-follow send configure this explicitly.
      getShowtimesOnDate: jest.fn().mockResolvedValue(new Map()),
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
    expect(pushService.subscriptionsWithTrip).not.toHaveBeenCalled();
    expect(showFollowsService.allFollows).not.toHaveBeenCalled();
  });

  it("sends a due trip notification to a subscribed topic", async () => {
    await withVapid(async () => {
      pushService.subscriptionsWithTrip.mockResolvedValueOnce([tripSubscription]);
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
      pushService.subscriptionsWithTrip.mockResolvedValue([tripSubscription]);
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

  it("reports what it already sent, not zero, when a later trip in the same tick throws", async () => {
    await withVapid(async () => {
      const logSpy = jest
        .spyOn(Logger.prototype, "log")
        .mockImplementation(() => undefined);
      try {
        const secondSubscription = {
          ...tripSubscription,
          id: "sub-trip-2",
          endpoint: "https://fcm.googleapis.com/fcm/send/trip-2",
          tripId: "trip-2",
        };
        // Two distinct trips, each with its own subscriber, so
        // `subscriptionsByTrip` groups them into two separate iterations —
        // insertion order is iteration order for a `Map`, so trip-1 (which
        // sends) runs before trip-2 (which throws).
        pushService.subscriptionsWithTrip.mockResolvedValueOnce([
          tripSubscription,
          secondSubscription,
        ]);
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
                    { id: "e1", attractionName: "Ride", startMinute: 20 * 60 + 15 },
                  ],
                },
              },
            },
          },
        };
        tripsService.find
          .mockResolvedValueOnce({ payload }) // trip-1: sends
          .mockRejectedValueOnce(new Error("db down")); // trip-2: throws

        await processor.handleDue({} as never);

        // The one send that happened before the throw actually happened...
        expect(pushService.send).toHaveBeenCalledTimes(1);
        // ...and the summary log says so — not the flat 0 a bug in
        // `handleTripNotifications`'s own error handling used to report by
        // discarding every `sent++` from before the throw.
        expect(logSpy).toHaveBeenCalledWith(
          expect.stringContaining("Sent 1 push notification"),
        );
      } finally {
        logSpy.mockRestore();
      }
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
      // NOW is 20:00 Berlin (CEST, UTC+2); the fixture's showtime is 30 min
      // later, i.e. 20:30 local — the "verified for today" time
      // `getShowtimesOnDate` would answer for a genuinely-reported showtime.
      showsService.getShowtimesOnDate.mockResolvedValueOnce(
        new Map([["show-1", ["20:30"]]]),
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

  it("sends to every follower of a popular show, spanning more than one send batch", async () => {
    await withVapid(async () => {
      // 25 followers on one show — more than the 20-per-batch cap, so this
      // only passes if the batching loop walks every batch rather than
      // stopping after the first.
      const followerCount = 25;
      const follows = Array.from({ length: followerCount }, (_, i) => ({
        id: `f${i}`,
        subscriptionId: `sub-${i}`,
        showId: "show-1",
      }));
      const subscriptionsById = new Map(
        follows.map((f) => [
          f.subscriptionId,
          {
            ...showSubscription,
            id: f.subscriptionId,
            endpoint: `${showSubscription.endpoint}-${f.subscriptionId}`,
          },
        ]),
      );

      showFollowsService.allFollows.mockResolvedValueOnce(follows);
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
      showsService.getShowtimesOnDate.mockResolvedValueOnce(
        new Map([["show-1", ["20:30"]]]),
      );
      pushService.findByIds.mockResolvedValueOnce(subscriptionsById);

      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(followerCount);
    });
  });

  it("does not notify about a showtime findBatchCurrentStatusByShows carries but getShowtimesOnDate never verified", async () => {
    // The exact gap `followedShowsDueToday` closes: a projected/stale
    // showtime with nothing to back it up must not fire.
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
                park: { name: "Europa-Park", timezone: "Europe/Berlin" },
              },
            },
          ],
        ]),
      );
      // Default mock: getShowtimesOnDate verifies nothing for today.

      await processor.handleDue({} as never);
      expect(pushService.send).not.toHaveBeenCalled();
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
      pushService.subscriptionsWithTrip.mockResolvedValueOnce([tripSubscription]);
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
      showsService.getShowtimesOnDate.mockResolvedValueOnce(
        new Map([["show-1", ["20:30"]]]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-show", showSubscription]]),
      );

      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(2);
    });
  });

  it("keeps sending trip notifications when the show-follow half throws", async () => {
    await withVapid(async () => {
      pushService.subscriptionsWithTrip.mockResolvedValueOnce([tripSubscription]);
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

      showFollowsService.allFollows.mockRejectedValueOnce(new Error("db down"));

      await processor.handleDue({} as never);
      expect(pushService.send).toHaveBeenCalledTimes(1);
      expect(pushService.send).toHaveBeenCalledWith(
        tripSubscription,
        expect.objectContaining({ title: expect.stringContaining("Ride") }),
      );
    });
  });

  it("notifies about a showtime just after local midnight, whose lead window opens the calendar day before", async () => {
    await withVapid(async () => {
      // A fresh `Date.now` for this one test: 23:30 Berlin (CEST) on the
      // 17th — 30 minutes before a showtime at 00:00 on the 18th, which is
      // inside the 25-35 minute lead window `dueShowNotifications` uses. At
      // this exact instant `todayStr` (read off `startedMs`) is still the
      // 17th, so the old code asked `getShowtimesOnDate` for the 17th and
      // could never find a showtime dated the 18th.
      const justBeforeMidnight = Date.parse("2026-10-17T21:30:00.000Z"); // 23:30 CEST
      jest.spyOn(Date, "now").mockReturnValue(justBeforeMidnight);

      showFollowsService.allFollows.mockResolvedValueOnce([
        { id: "f1", subscriptionId: "sub-show", showId: "show-1" },
      ]);
      showsService.findBatchCurrentStatusByShows.mockResolvedValueOnce(
        new Map([
          [
            "show-1",
            {
              status: "OPERATING",
              showtimes: [],
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
      // Nothing verified for "today" (the 17th) — the showtime belongs to
      // the 18th, which is exactly the date the fix also has to query.
      showsService.getShowtimesOnDate.mockImplementation(
        async (_parkId: string, _tz: string, dateStr: string) =>
          dateStr === "2026-10-18"
            ? new Map([["show-1", ["00:00"]]])
            : new Map(),
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

  it("does not notify about a showtime that falls in a spring-forward gap", async () => {
    await withVapid(async () => {
      // 2026-03-29 is the day Berlin's clocks jump 02:00 -> 03:00 (at 01:00
      // UTC) — 02:30 local that day never happens. `fromZonedTime` still
      // resolves it (empirically, to 2026-03-29T00:30:00Z), so "now" is set
      // 30 minutes before THAT instant: inside `dueShowNotifications`'
      // 25-35 minute lead window, which is what makes this test meaningful —
      // without the round-trip guard this exact "now" sends, guarded it
      // must not, because the source time never happened.
      const at = Date.parse("2026-03-29T00:00:00.000Z"); // 01:00 CET, before the jump
      jest.spyOn(Date, "now").mockReturnValue(at);

      showFollowsService.allFollows.mockResolvedValueOnce([
        { id: "f1", subscriptionId: "sub-show", showId: "show-1" },
      ]);
      showsService.findBatchCurrentStatusByShows.mockResolvedValueOnce(
        new Map([
          [
            "show-1",
            {
              status: "OPERATING",
              showtimes: [],
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
      showsService.getShowtimesOnDate.mockImplementation(
        async (_parkId: string, _tz: string, dateStr: string) =>
          dateStr === "2026-03-29"
            ? new Map([["show-1", ["02:30"]]])
            : new Map(),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-show", showSubscription]]),
      );

      await processor.handleDue({} as never);
      expect(pushService.send).not.toHaveBeenCalled();
    });
  });

  it("keeps sending show-follow notifications when the trip half throws", async () => {
    await withVapid(async () => {
      pushService.subscriptionsWithTrip.mockResolvedValueOnce([tripSubscription]);
      tripsService.find.mockRejectedValueOnce(new Error("db down"));

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
      showsService.getShowtimesOnDate.mockResolvedValueOnce(
        new Map([["show-1", ["20:30"]]]),
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
});
