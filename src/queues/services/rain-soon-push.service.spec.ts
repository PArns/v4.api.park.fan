import { RainSoonPushService } from "./rain-soon-push.service";
import {
  LiveStatus,
  QueueType,
} from "../../external-apis/themeparks/themeparks.types";

/**
 * The producer behind the `rain-soon` topic, run against a mock nowcast.
 *
 * What is worth pinning is the lifecycle across ticks, because a rain warning
 * repeats for as long as the rain is ahead: one push per front, silence while it
 * rains, and a second push for a second front. The redis mock keeps real state
 * so the tests drive the same sequence the job sees.
 */
describe("RainSoonPushService", () => {
  // 2026-10-17, 12:00 UTC = 14:00 in Berlin (CEST).
  const NOW = Date.parse("2026-10-17T12:00:00.000Z");
  const at = (min: number) => new Date(NOW + min * 60_000).toISOString();

  const park = {
    id: "park-1",
    name: "Phantasialand",
    slug: "phantasialand",
    citySlug: "bruehl",
    countrySlug: "germany",
    continentSlug: "europe",
    timezone: "Europe/Berlin",
  };

  const subscription = (over: Record<string, unknown> = {}) => ({
    id: "sub-1",
    endpoint: "https://fcm.googleapis.com/fcm/send/1",
    p256dh: "k",
    auth: "a",
    tripId: "trip-1",
    topics: ["rain-soon"],
    locale: "de",
    timezone: "Europe/Berlin",
    ...over,
  });

  const plan = {
    version: 2,
    parks: {
      phantasialand: {
        slug: "phantasialand",
        name: "Phantasialand",
        timezone: "Europe/Berlin",
        geo: { continent: "europe", country: "germany", city: "bruehl" },
        days: {
          "2026-10-17": { entries: [{ id: "a", startMinute: 600 }] },
        },
      },
    },
  };

  const attraction = (
    id: string,
    indoorOutdoor: string | null,
    over: Record<string, unknown> = {},
  ) => ({ id, name: id, indoorOutdoor, isSeasonal: false, ...over });

  const reading = (id: string, waitTime: number, over = {}) => ({
    attractionId: id,
    queueType: QueueType.STANDBY,
    status: LiveStatus.OPERATING,
    waitTime,
    isHeartbeat: false,
    ...over,
  });

  let store: Map<string, string>;
  let redis: { get: jest.Mock; set: jest.Mock; del: jest.Mock };
  let pushService: { subscriptionsWithTopic: jest.Mock; send: jest.Mock };
  let tripsService: { find: jest.Mock };
  let rideAlertsService: { parkIdsBySubscription: jest.Mock };
  let parksService: { findByGeographicPath: jest.Mock; findById: jest.Mock };
  let weatherService: { getNowcast: jest.Mock };
  let queueDataService: { findCurrentStatusByAttractionIds: jest.Mock };
  let attractionRepository: { find: jest.Mock };
  let service: RainSoonPushService;

  const nowcast = (over = {}) => ({
    currentlyRaining: false,
    rainStartsAt: null,
    rainEndsAt: null,
    ...over,
  });

  beforeEach(() => {
    store = new Map();
    redis = {
      get: jest.fn(async (k: string) => store.get(k) ?? null),
      set: jest.fn(async (k: string, v: string, ...args: unknown[]) => {
        if (args.includes("NX") && store.has(k)) return null;
        store.set(k, v);
        return "OK";
      }),
      del: jest.fn(async (k: string) => (store.delete(k) ? 1 : 0)),
    };
    pushService = {
      subscriptionsWithTopic: jest.fn().mockResolvedValue([subscription()]),
      send: jest.fn().mockResolvedValue(true),
    };
    tripsService = { find: jest.fn().mockResolvedValue({ payload: plan }) };
    rideAlertsService = {
      parkIdsBySubscription: jest.fn().mockResolvedValue(new Map()),
    };
    parksService = {
      findByGeographicPath: jest.fn().mockResolvedValue(park),
      findById: jest.fn().mockResolvedValue(park),
    };
    weatherService = { getNowcast: jest.fn().mockResolvedValue(nowcast()) };
    attractionRepository = {
      find: jest
        .fn()
        .mockResolvedValue([
          attraction("taron", "outdoor"),
          attraction("maus", "indoor"),
          attraction("fly", "covered_queue"),
          attraction("closed", "indoor"),
        ]),
    };
    queueDataService = {
      findCurrentStatusByAttractionIds: jest.fn().mockResolvedValue(
        new Map([
          ["maus", [reading("maus", 15)]],
          ["fly", [reading("fly", 20)]],
          ["closed", [reading("closed", 5, { status: LiveStatus.CLOSED })]],
        ]),
      ),
    };
    service = new RainSoonPushService(
      pushService as never,
      tripsService as never,
      rideAlertsService as never,
      parksService as never,
      weatherService as never,
      queueDataService as never,
      attractionRepository as never,
      redis as never,
    );
  });

  it("sends nothing when no rain is forecast", async () => {
    expect(await service.run(NOW)).toBe(0);
    expect(pushService.send).not.toHaveBeenCalled();
  });

  it("does nothing without subscribers for the topic", async () => {
    pushService.subscriptionsWithTopic.mockResolvedValue([]);
    expect(await service.run(NOW)).toBe(0);
    expect(weatherService.getNowcast).not.toHaveBeenCalled();
  });

  it("warns 20 minutes ahead and names the open covered rides, shortest queue first", async () => {
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    expect(await service.run(NOW)).toBe(1);

    const [sub, message] = pushService.send.mock.calls[0];
    expect(sub.id).toBe("sub-1");
    expect(message.title).toBe("Regen ab etwa 14:20 Uhr");
    expect(message.body).toBe(
      "Phantasialand. Überdacht und offen: maus (15 Min.), fly (20 Min.)",
    );
    expect(message.url).toBe("/parks/europe/germany/bruehl/phantasialand");
    // The closed covered ride and the outdoor one are not named.
    expect(message.body).not.toContain("closed");
    expect(message.body).not.toContain("taron");
  });

  it("sends the warning alone when the park does not know enough about its rides", async () => {
    attractionRepository.find.mockResolvedValue([
      attraction("maus", "indoor"),
      attraction("a", null),
      attraction("b", null),
      attraction("c", null),
    ]);
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    await service.run(NOW);

    expect(pushService.send.mock.calls[0][1].body).toBe("Phantasialand");
    expect(
      queueDataService.findCurrentStatusByAttractionIds,
    ).not.toHaveBeenCalled();
  });

  it("does not trust a heartbeat or a stale row as 'open'", async () => {
    queueDataService.findCurrentStatusByAttractionIds.mockResolvedValue(
      new Map([
        ["maus", [reading("maus", 15, { isHeartbeat: true })]],
        ["fly", []],
      ]),
    );
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    await service.run(NOW);

    expect(pushService.send.mock.calls[0][1].body).toBe("Phantasialand");
  });

  it("sends one push per front: the next ticks stay silent", async () => {
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    expect(await service.run(NOW)).toBe(1);
    expect(await service.run(NOW + 5 * 60_000)).toBe(0);
    expect(await service.run(NOW + 10 * 60_000)).toBe(0);
    expect(pushService.send).toHaveBeenCalledTimes(1);
  });

  it("sends nothing when the rain is already falling", async () => {
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ currentlyRaining: true, rainEndsAt: at(40) }),
    );

    expect(await service.run(NOW)).toBe(0);
    expect(pushService.send).not.toHaveBeenCalled();
    expect(store.get("push:rain-soon:park-1")).toBe("raining");
  });

  it("warns again for a second front once the first has ended", async () => {
    weatherService.getNowcast.mockResolvedValueOnce(
      nowcast({ rainStartsAt: at(20) }),
    );
    await service.run(NOW);

    weatherService.getNowcast.mockResolvedValueOnce(
      nowcast({ currentlyRaining: true, rainEndsAt: at(30) }),
    );
    await service.run(NOW + 30 * 60_000);

    weatherService.getNowcast.mockResolvedValueOnce(nowcast());
    await service.run(NOW + 90 * 60_000);
    expect(store.has("push:rain-soon:park-1")).toBe(false);

    const later = NOW + 180 * 60_000;
    weatherService.getNowcast.mockResolvedValueOnce(
      nowcast({ rainStartsAt: new Date(later + 15 * 60_000).toISOString() }),
    );
    expect(await service.run(later)).toBe(1);
    expect(pushService.send).toHaveBeenCalledTimes(2);
  });

  it("holds the push back in the subscriber's quiet hours", async () => {
    pushService.subscriptionsWithTopic.mockResolvedValue([
      subscription({ id: "night", timezone: "America/Chicago" }),
      subscription({ id: "day" }),
    ]);
    // 08:00 UTC is 03:00 in Chicago (CDT) and 10:00 in Berlin.
    const chicagoNight = Date.parse("2026-10-17T08:00:00.000Z");
    weatherService.getNowcast.mockResolvedValue(
      nowcast({
        rainStartsAt: new Date(chicagoNight + 20 * 60_000).toISOString(),
      }),
    );

    expect(await service.run(chicagoNight)).toBe(1);
    expect(pushService.send.mock.calls.map(([s]) => s.id)).toEqual(["day"]);
  });

  it("leaves the front unclaimed while every recipient is in quiet hours", async () => {
    pushService.subscriptionsWithTopic.mockResolvedValue([
      subscription({ id: "night", timezone: "America/Chicago" }),
    ]);
    const chicagoNight = Date.parse("2026-10-17T08:00:00.000Z");
    weatherService.getNowcast.mockResolvedValue(
      nowcast({
        rainStartsAt: new Date(chicagoNight + 20 * 60_000).toISOString(),
      }),
    );

    expect(await service.run(chicagoNight)).toBe(0);
    expect(store.has("push:rain-soon:park-1")).toBe(false);
  });

  it("still sends the warning when the covered-ride lookup fails", async () => {
    attractionRepository.find.mockRejectedValue(new Error("db down"));
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    expect(await service.run(NOW)).toBe(1);
    expect(pushService.send.mock.calls[0][1].body).toBe("Phantasialand");
  });

  it("treats an unknown stored state as none", async () => {
    store.set("push:rain-soon:park-1", "garbage");
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );
    // NX cannot claim over the junk value, so nothing is sent, but no throw.
    expect(await service.run(NOW)).toBe(0);
  });

  it("reaches a subscriber who has only a ride alert in the park", async () => {
    pushService.subscriptionsWithTopic.mockResolvedValue([
      subscription({ id: "alerts-only", tripId: null }),
    ]);
    rideAlertsService.parkIdsBySubscription.mockResolvedValue(
      new Map([["alerts-only", new Set(["park-1"])]]),
    );
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    expect(await service.run(NOW)).toBe(1);
    expect(tripsService.find).not.toHaveBeenCalled();
  });

  it("does not warn a subscriber whose plan is for another day", async () => {
    tripsService.find.mockResolvedValue({
      payload: {
        ...plan,
        parks: {
          phantasialand: {
            ...plan.parks.phantasialand,
            days: {
              "2026-10-20": { entries: [{ id: "a", startMinute: 600 }] },
            },
          },
        },
      },
    });
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    expect(await service.run(NOW)).toBe(0);
    expect(weatherService.getNowcast).not.toHaveBeenCalled();
  });

  it("sends once to two browsers on one park, and reads the forecast once", async () => {
    pushService.subscriptionsWithTopic.mockResolvedValue([
      subscription({ id: "one" }),
      subscription({ id: "two", locale: "en" }),
    ]);
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );

    expect(await service.run(NOW)).toBe(2);
    expect(weatherService.getNowcast).toHaveBeenCalledTimes(1);
    const titles = pushService.send.mock.calls.map(([, m]) => m.title);
    expect(titles).toEqual([
      "Regen ab etwa 14:20 Uhr",
      "Rain from about 14:20",
    ]);
  });

  it("sends nothing, rather than a repeat, when the state cannot be written", async () => {
    weatherService.getNowcast.mockResolvedValue(
      nowcast({ rainStartsAt: at(20) }),
    );
    redis.set.mockRejectedValue(new Error("redis down"));

    expect(await service.run(NOW)).toBe(0);
    expect(pushService.send).not.toHaveBeenCalled();
  });

  it("lets one park's failure leave the others alone", async () => {
    const other = {
      ...park,
      id: "park-2",
      name: "Europa-Park",
      slug: "europa-park",
    };
    pushService.subscriptionsWithTopic.mockResolvedValue([
      subscription({ id: "a", tripId: null }),
      subscription({ id: "b", tripId: null }),
    ]);
    rideAlertsService.parkIdsBySubscription.mockResolvedValue(
      new Map([
        ["a", new Set(["park-1"])],
        ["b", new Set(["park-2"])],
      ]),
    );
    parksService.findById.mockImplementation(async (id: string) =>
      id === "park-1" ? park : other,
    );
    weatherService.getNowcast
      .mockRejectedValueOnce(new Error("open-meteo down"))
      .mockResolvedValueOnce(nowcast({ rainStartsAt: at(20) }));

    expect(await service.run(NOW)).toBe(1);
    expect(pushService.send.mock.calls[0][1].body).toContain("Europa-Park");
  });
});
