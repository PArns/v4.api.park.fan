import { SixFlagsHeightsProcessor } from "./six-flags-heights.processor";

/**
 * ThemeParks.wiki has no minimumHeight for the Six Flags and former Cedar
 * Fair parks — the field is absent from their entity documents — so ~1400
 * rides across 26 parks carry NULL and nothing upstream will ever fill them.
 *
 * This job reads the numbers off the parks' own ride pages. It only ever
 * fills gaps: any height we already hold, whether from the wiki or the
 * curated seed, wins.
 */
describe("SixFlagsHeightsProcessor", () => {
  const repo = { find: jest.fn(), update: jest.fn() };
  const attractionsService = { getRepository: () => repo };
  const client = { fetchMinHeightInches: jest.fn() };

  const processor = () =>
    new SixFlagsHeightsProcessor(attractionsService as never, client as never);

  const attraction = (over: Record<string, unknown> = {}) => ({
    id: "a1",
    slug: "steel-vengeance",
    minimumHeight: null,
    park: { slug: "cedar-point", name: "Cedar Point" },
    ...over,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    client.fetchMinHeightInches.mockResolvedValue(null);
  });

  it("stores the height in centimetres and records that the park publishes inches", async () => {
    // Centimetres stay the canonical value, but a US park page says 52" —
    // without the unit a ride page would show "132 cm" to an audience that
    // has never seen the number expressed that way.
    repo.find.mockResolvedValue([attraction()]);
    client.fetchMinHeightInches.mockResolvedValue(52);

    await processor().handleSyncHeights({} as never);

    expect(repo.update).toHaveBeenCalledWith("a1", {
      minimumHeight: 132,
      minimumHeightUnit: "in",
    });
  });

  it("asks the park's own site slug, not ours", async () => {
    repo.find.mockResolvedValue([attraction()]);

    await processor().handleSyncHeights({} as never);

    expect(client.fetchMinHeightInches).toHaveBeenCalledWith(
      "cedarpoint",
      "steel-vengeance",
    );
  });

  it("skips parks that are not Six Flags properties", async () => {
    repo.find.mockResolvedValue([
      attraction({ park: { slug: "phantasialand", name: "Phantasialand" } }),
    ]);

    await processor().handleSyncHeights({} as never);

    expect(client.fetchMinHeightInches).not.toHaveBeenCalled();
    expect(repo.update).not.toHaveBeenCalled();
  });

  it("writes nothing when the ride has no height requirement", async () => {
    repo.find.mockResolvedValue([attraction()]);
    client.fetchMinHeightInches.mockResolvedValue(null);

    await processor().handleSyncHeights({} as never);

    expect(repo.update).not.toHaveBeenCalled();
  });

  it("only asks for attractions we have no height for", async () => {
    repo.find.mockResolvedValue([]);

    await processor().handleSyncHeights({} as never);

    expect(repo.find).toHaveBeenCalledWith(
      expect.objectContaining({ where: { minimumHeight: expect.anything() } }),
    );
  });

  it("fetches one ride at a time", async () => {
    // Sequential by design: only DB and Redis work is batched in this project,
    // never the upstream requests.
    let inFlight = 0;
    let peak = 0;
    client.fetchMinHeightInches.mockImplementation(async () => {
      peak = Math.max(peak, ++inFlight);
      await new Promise((r) => setTimeout(r, 1));
      inFlight--;
      return 48;
    });
    repo.find.mockResolvedValue([
      attraction({ id: "a1", slug: "one" }),
      attraction({ id: "a2", slug: "two" }),
      attraction({ id: "a3", slug: "three" }),
    ]);

    await processor().handleSyncHeights({} as never);

    expect(peak).toBe(1);
    expect(repo.update).toHaveBeenCalledTimes(3);
  });

  it("keeps going when one ride fails", async () => {
    client.fetchMinHeightInches
      .mockRejectedValueOnce(new Error("timeout"))
      .mockResolvedValueOnce(48);
    repo.find.mockResolvedValue([
      attraction({ id: "a1", slug: "one" }),
      attraction({ id: "a2", slug: "two" }),
    ]);

    await processor().handleSyncHeights({} as never);

    expect(repo.update).toHaveBeenCalledTimes(1);
    expect(repo.update).toHaveBeenCalledWith("a2", {
      minimumHeight: 122,
      minimumHeightUnit: "in",
    });
  });
});
