import { SiteEntityNames } from "./site-entity-names.service";

/**
 * What is pinned about the id list `page_read` is checked against: loaded
 * once and kept for ten minutes, the last copy while the database is away,
 * nothing before the first.
 */
describe("SiteEntityNames.knownIds", () => {
  const PARK = "3f1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
  const RIDE = "5a1b2c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
  let parks: jest.Mock;
  let attractions: jest.Mock;
  let names: SiteEntityNames;

  beforeEach(() => {
    parks = jest.fn().mockResolvedValue([{ id: PARK }]);
    attractions = jest.fn().mockResolvedValue([{ id: RIDE }]);
    names = new SiteEntityNames(
      { query: parks } as never,
      { query: attractions } as never,
      {} as never,
      {} as never,
    );
  });

  it("loads every park and ride id once, and again after ten minutes", async () => {
    const t0 = Date.now();
    const ids = await names.knownIds(t0);
    expect(ids?.parks.has(PARK)).toBe(true);
    expect(ids?.attractions.has(RIDE)).toBe(true);
    await names.knownIds(t0 + 60_000);
    expect(parks).toHaveBeenCalledTimes(1);
    await names.knownIds(Date.now() + 11 * 60_000);
    expect(parks).toHaveBeenCalledTimes(2);
  });

  it("keeps the last copy while the database is away, and has none before the first", async () => {
    parks.mockRejectedValueOnce(new Error("down"));
    await expect(names.knownIds()).resolves.toBeNull();
    expect(parks).toHaveBeenCalledTimes(1);
    await names.knownIds();
    expect(parks).toHaveBeenCalledTimes(1); // a failure backs off for the TTL
    const ids = await names.knownIds(Date.now() + 11 * 60_000);
    parks.mockRejectedValueOnce(new Error("down again"));
    await expect(names.knownIds(Date.now() + 11 * 60_000)).resolves.toBe(ids);
  });
});
