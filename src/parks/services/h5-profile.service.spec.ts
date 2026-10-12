import { H5ProfileService, H5_NEGATIVE_CACHE_MS } from "./h5-profile.service";
import { Park } from "../entities/park.entity";

/**
 * The read side's two guards: one build per key while it runs, and a failed
 * build replayed for a short while instead of re-running two statements on
 * every request against a database that is the reason they failed.
 */
describe("H5ProfileService", () => {
  const park = { id: "p1", slug: "phl", timezone: "Europe/Berlin" } as Park;

  const build = (fails: () => boolean) => {
    const transaction = jest.fn().mockImplementation(async () => {
      if (fails())
        throw new Error("canceling statement due to statement timeout");
      return [];
    });
    const redis = {
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue("OK"),
    };
    const service = new H5ProfileService(
      { transaction } as never,
      redis as never,
    );
    return { service, transaction, redis };
  };

  afterEach(() => jest.useRealTimers());

  it("replays a failure for the negative-cache window, then tries again", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-09T10:00:00Z"));
    let failing = true;
    const { service, transaction } = build(() => failing);

    await expect(service.getProfiles(park, "2026-10-09")).rejects.toThrow(
      "statement timeout",
    );
    // Two statements per build.
    expect(transaction).toHaveBeenCalledTimes(2);

    await expect(service.getProfiles(park, "2026-10-09")).rejects.toThrow(
      "statement timeout",
    );
    expect(transaction).toHaveBeenCalledTimes(2);

    failing = false;
    jest.setSystemTime(Date.now() + H5_NEGATIVE_CACHE_MS + 1);
    await expect(service.getProfiles(park, "2026-10-09")).resolves.toEqual(
      new Map(),
    );
    expect(transaction).toHaveBeenCalledTimes(4);
  });

  it("builds once for requests that arrive together, and caches the result", async () => {
    const { service, transaction, redis } = build(() => false);

    await Promise.all([
      service.getProfiles(park, "2026-10-09"),
      service.getProfiles(park, "2026-10-09"),
    ]);

    expect(transaction).toHaveBeenCalledTimes(2);
    expect(redis.set).toHaveBeenCalledWith(
      "plan-day:h5:v2:p1:2026-10-09",
      "{}",
      "EX",
      6 * 3600,
    );
  });

  it("parses the compact slot string the history read returns", () => {
    expect(H5ProfileService.parseSlots("09:15=35,10:00=40,bad")).toEqual([
      { minute: 555, wait: 35 },
      { minute: 600, wait: 40 },
    ]);
    expect(H5ProfileService.parseSlots(null)).toEqual([]);
  });
});
