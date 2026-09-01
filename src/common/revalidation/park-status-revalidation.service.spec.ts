import { Redis } from "ioredis";
import { ParkStatusRevalidationService } from "./park-status-revalidation.service";
import { RevalidationService } from "./revalidation.service";
import {
  diffParkStatuses,
  parkCacheTag,
  type ParkOperatingStatus,
  type TaggablePark,
} from "./park-status-transitions";

/**
 * The frontend caches a park's structure fetch for a day, and two blocks in it are dated to that
 * day: a show's showtimes, and the shows'/restaurants' status (the park response reports them as
 * CLOSED for as long as the park is). So the entry is written at whatever hour it happened to be
 * filled — overnight, in practice — and then stands. On 2026-09-01 every park on the site served
 * yesterday's showtimes under "no performances today".
 *
 * This side already recomputes park status every five minutes, so the transition is observed here
 * and posted as one webhook carrying only the parks that flipped.
 */

const PHANTASIALAND: TaggablePark = {
  id: "p1",
  slug: "phantasialand",
  citySlug: "bruehl",
  countrySlug: "germany",
  continentSlug: "europe",
};

const DISNEYLAND_PARIS: TaggablePark = {
  id: "p2",
  slug: "disneyland-park",
  citySlug: "paris",
  countrySlug: "france",
  continentSlug: "europe",
};

const DISNEYLAND_ANAHEIM: TaggablePark = {
  id: "p3",
  slug: "disneyland-park",
  citySlug: "anaheim",
  countrySlug: "united-states",
  continentSlug: "north-america",
};

const statuses = (
  entries: Record<string, ParkOperatingStatus>,
): Map<string, ParkOperatingStatus> => new Map(Object.entries(entries));

describe("parkCacheTag", () => {
  it("is the geo path, byte-identical to the frontend's twin", () => {
    expect(parkCacheTag(PHANTASIALAND)).toBe(
      "park:europe/germany/bruehl/phantasialand",
    );
  });

  it("separates two parks that share a slug — they do not open at the same time", () => {
    expect(parkCacheTag(DISNEYLAND_PARIS)).not.toBe(
      parkCacheTag(DISNEYLAND_ANAHEIM),
    );
  });

  it("returns null for a park whose geocoding never completed (it has no frontend URL either)", () => {
    expect(parkCacheTag({ ...PHANTASIALAND, citySlug: null })).toBeNull();
  });
});

describe("diffParkStatuses", () => {
  it("reports nothing on the first run and records what it saw", () => {
    const { tags, nextSnapshot } = diffParkStatuses(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING", p2: "CLOSED" }),
      {},
    );

    expect(tags).toEqual([]);
    expect(nextSnapshot).toEqual({ p1: "OPERATING", p2: "CLOSED" });
  });

  it("reports a park that opened", () => {
    const { tags } = diffParkStatuses(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING", p2: "CLOSED" }),
      { p1: "CLOSED", p2: "CLOSED" },
    );

    expect(tags).toEqual(["park:europe/germany/bruehl/phantasialand"]);
  });

  it("reports a park that closed — its shows read as running until something says otherwise", () => {
    const { tags } = diffParkStatuses(
      [PHANTASIALAND],
      statuses({ p1: "CLOSED" }),
      { p1: "OPERATING" },
    );

    expect(tags).toEqual(["park:europe/germany/bruehl/phantasialand"]);
  });

  it("stays quiet for a park that was already open", () => {
    const { tags } = diffParkStatuses(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
      { p1: "OPERATING" },
    );

    expect(tags).toEqual([]);
  });

  it("keeps the previous status for a park this cycle had no reading for", () => {
    const { tags, nextSnapshot } = diffParkStatuses(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING" }),
      { p1: "OPERATING", p2: "OPERATING" },
    );

    // p2 must not read as a transition on the next cycle just because it was skipped on this one.
    expect(tags).toEqual([]);
    expect(nextSnapshot).toEqual({ p1: "OPERATING", p2: "OPERATING" });
  });

  it("drops a park that no longer exists instead of carrying it forever", () => {
    const { nextSnapshot } = diffParkStatuses(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
      { p1: "CLOSED", gone: "OPERATING" },
    );

    expect(nextSnapshot).toEqual({ p1: "OPERATING" });
  });
});

describe("ParkStatusRevalidationService", () => {
  let store: Map<string, string>;
  let redis: Redis;
  let revalidation: { revalidateTags: jest.Mock };
  let service: ParkStatusRevalidationService;

  const snapshot = () =>
    JSON.parse(store.get("revalidate:park-status") ?? "null") as unknown;
  const sentTags = () =>
    revalidation.revalidateTags.mock.calls.map(
      (call: unknown[]) => call[0] as string[],
    );

  beforeEach(() => {
    store = new Map();
    redis = {
      get: jest.fn((key: string) => Promise.resolve(store.get(key) ?? null)),
      set: jest.fn((key: string, value: string) => {
        store.set(key, value);
        return Promise.resolve("OK");
      }),
    } as unknown as Redis;
    revalidation = { revalidateTags: jest.fn().mockResolvedValue(true) };
    service = new ParkStatusRevalidationService(
      redis,
      revalidation as unknown as RevalidationService,
    );
  });

  it("seeds on the first run without posting all 213 parks", async () => {
    const sent = await service.revalidateChangedParks(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING", p2: "OPERATING" }),
    );

    expect(sent).toBe(0);
    expect(revalidation.revalidateTags).not.toHaveBeenCalled();
    expect(snapshot()).toEqual({ p1: "OPERATING", p2: "OPERATING" });
  });

  it("posts only the park that flipped, and asks for an immediate expiry", async () => {
    await service.revalidateChangedParks(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "CLOSED", p2: "CLOSED" }),
    );

    const sent = await service.revalidateChangedParks(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING", p2: "CLOSED" }),
    );

    expect(sent).toBe(1);
    expect(revalidation.revalidateTags).toHaveBeenCalledWith(
      ["park:europe/germany/bruehl/phantasialand"],
      { immediate: true },
    );
  });

  it("treats a corrupt snapshot as a first run rather than taking the warmup down", async () => {
    store.set("revalidate:park-status", "{not json");

    const sent = await service.revalidateChangedParks(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
    );

    expect(sent).toBe(0);
    expect(snapshot()).toEqual({ p1: "OPERATING" });
  });

  // The edge copy of a park response cannot be purged from this service, so the frontend's
  // re-fetch can land on one cached minutes before the gates opened — and pin it for a day. The
  // second round is what bounds that to the CDN window.
  it("repeats the transition once the edge window has passed", async () => {
    const openedAt = Date.parse("2026-09-01T09:00:00Z");
    jest.spyOn(Date, "now").mockReturnValue(openedAt);

    await service.revalidateChangedParks(
      [PHANTASIALAND],
      statuses({ p1: "CLOSED" }),
    );
    await service.revalidateChangedParks(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
    );
    expect(sentTags()).toEqual([["park:europe/germany/bruehl/phantasialand"]]);

    // Five minutes later: still inside the window, and nothing else changed.
    jest.spyOn(Date, "now").mockReturnValue(openedAt + 5 * 60 * 1000);
    await service.revalidateChangedParks(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
    );
    expect(sentTags()).toHaveLength(1);

    // Past it: the same tag goes again, without a second status change.
    jest.spyOn(Date, "now").mockReturnValue(openedAt + 17 * 60 * 1000);
    const sent = await service.revalidateChangedParks(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
    );

    expect(sent).toBe(1);
    expect(sentTags()[1]).toEqual(["park:europe/germany/bruehl/phantasialand"]);

    // And it does not repeat forever.
    jest.spyOn(Date, "now").mockReturnValue(openedAt + 40 * 60 * 1000);
    await service.revalidateChangedParks(
      [PHANTASIALAND],
      statuses({ p1: "OPERATING" }),
    );
    expect(sentTags()).toHaveLength(2);

    jest.spyOn(Date, "now").mockRestore();
  });

  it("sends a due repeat and a fresh transition in one POST", async () => {
    const openedAt = Date.parse("2026-09-01T09:00:00Z");
    jest.spyOn(Date, "now").mockReturnValue(openedAt);

    await service.revalidateChangedParks(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "CLOSED", p2: "CLOSED" }),
    );
    await service.revalidateChangedParks(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING", p2: "CLOSED" }),
    );

    jest.spyOn(Date, "now").mockReturnValue(openedAt + 17 * 60 * 1000);
    await service.revalidateChangedParks(
      [PHANTASIALAND, DISNEYLAND_PARIS],
      statuses({ p1: "OPERATING", p2: "OPERATING" }),
    );

    expect(revalidation.revalidateTags).toHaveBeenCalledTimes(2);
    expect(sentTags()[1].sort()).toEqual([
      "park:europe/france/paris/disneyland-park",
      "park:europe/germany/bruehl/phantasialand",
    ]);

    jest.spyOn(Date, "now").mockRestore();
  });

  it("swallows a Redis failure — it runs inside the wait-times batch", async () => {
    (redis.get as jest.Mock).mockRejectedValue(new Error("redis down"));

    await expect(
      service.revalidateChangedParks(
        [PHANTASIALAND],
        statuses({ p1: "OPERATING" }),
      ),
    ).resolves.toBe(0);
  });
});
