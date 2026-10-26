import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * What the wiki path records when the name match claims a row that answers to
 * Queue-Times (PAR-1081).
 *
 * `externalId` is unique globally and source-scoped, so a ride both sources
 * report has one row and one id — whichever source created it. The name match
 * has always handed the wiki's entity to such a row, and the row has always
 * kept its `qt-ride-` id, which is right: the Queue-Times path finds it
 * through that id. What was missing is the other half. The wiki id went
 * nowhere at all, and `WaitTimesProcessor` keys its lookup on
 * `themeparks-wiki:<id>` — filled from the mappings plus an `externalId`
 * fallback that assumes the id is a wiki id. For a `qt-ride-` row the fallback
 * registers `themeparks-wiki:qt-ride-11272`, which no `/live` entry carries,
 * so every wiki reading for the ride was dropped.
 *
 * Measured against production on 2026-10-26: 35 listed rows in 2 parks with no
 * wiki mapping; 30 of them — all of Energylandia, all created in the
 * 2025-12-24 seed before the park had a `wikiEntityId` — with no mapping at
 * all, hence nothing but `system-reconciliation` CLOSED for seven days while
 * the wiki published their waits. All 32 of Energylandia's cross-source rows
 * carried a fresh `last_listed_at`, which is what proves the match itself was
 * never the problem.
 *
 * Each case carries the counter-check that makes it discriminating: the two
 * neighbouring shapes — a re-issue, and a row reached through a shared
 * Queue-Times id — must NOT take the incoming wiki id, and for different
 * reasons.
 */
describe("ChildrenMetadataProcessor — cross-source identity", () => {
  const attractionRepo = {
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const childEntityRepo = {
    find: jest.fn().mockResolvedValue([]),
    update: jest.fn(),
    findOne: jest.fn().mockResolvedValue(null),
  };
  const manager = { query: jest.fn() };
  const mappingRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    manager,
  };
  const themeParksMapper = { mapAttraction: jest.fn() };
  const retirementService = { retire: jest.fn(), unretire: jest.fn() };

  let processor: ChildrenMetadataProcessor;
  let logged: string[];

  const parkId = "park-energylandia";

  /** The wiki entity the feed lists for `Formuła Autodrom`. */
  const wikiId = "492102f3-2d51-4473-bf57-96560336abf5";
  /** The id the row has answered to since the seed. */
  const qtId = "qt-ride-11272";

  /**
   * `Formuła Autodrom` as the table holds it: created by the Queue-Times path
   * while the park had no `wikiEntityId`, so its identity is Queue-Times'.
   * The name is byte-identical to the feed's — the diacritics are not the
   * issue, and a fixture that spelled it `Formula` would test the wrong thing.
   */
  const qtRow = {
    id: "row-formula-autodrom",
    externalId: qtId,
    slug: "formula-autodrom",
    name: "Formuła Autodrom",
    queueTimesEntityId: "11272",
    retiredReason: null,
    retiredKind: null,
    latitude: "49.9338890",
    longitude: "19.2016670",
    attractionType: "RIDE",
  };

  const feedRow = (over: Record<string, unknown> = {}) => ({
    externalId: wikiId,
    name: qtRow.name,
    parkId,
    latitude: 49.933889,
    longitude: 19.201667,
    attractionType: "RIDE",
    ...over,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    logged = [];
    attractionRepo.find.mockResolvedValue([{ ...qtRow }]);
    mappingRepository.find.mockResolvedValue([]);
    mappingRepository.findOne.mockResolvedValue(null);
    manager.query.mockResolvedValue([]);
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      retirementService as any,
      { getRepository: () => childEntityRepo } as any,
      { getRepository: () => childEntityRepo } as any,
      {} as any,
      {} as any,
      themeParksMapper as any,
      {} as any,
      mappingRepository as any,
      {} as any,
      {} as any,
      {} as any,
    );
    jest
      .spyOn((processor as any).logger, "log")
      .mockImplementation((message: unknown) => logged.push(String(message)));
    jest.spyOn((processor as any).logger, "warn").mockImplementation(() => {});
  });

  /**
   * `listedExternalIds` carries what this run's `/children` held. The incoming
   * wiki id is always in it — it is why the child is being synced at all.
   */
  const sync = (
    feed: Record<string, unknown>,
    listed: string[] = [wikiId],
    listedRowIds = new Set<string>(),
  ) => {
    themeParksMapper.mapAttraction.mockReturnValue(feed);
    return (processor as any)
      .syncAttraction({}, parkId, {
        claimed: new Set<string>(),
        listedExternalIds: new Set(listed),
        listedRowIds,
      })
      .then(() => listedRowIds);
  };

  it("maps the wiki id onto the row it adopted across sources", async () => {
    await sync(feedRow());

    expect(mappingRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        internalEntityId: qtRow.id,
        internalEntityType: "attraction",
        externalSource: "themeparks-wiki",
        externalEntityId: wikiId,
      }),
    );
  });

  it("leaves `externalId` on the Queue-Times id", async () => {
    // The row is reachable from Queue-Times only through this id, and the
    // column is unique globally — moving it would strand that source to heal
    // this one. The wiki id belongs in the mapping instead, which is the whole
    // shape of the fix.
    await sync(feedRow());

    const written = attractionRepo.update.mock.calls.map(
      ([, changes]) => changes,
    );
    expect(written.some((c) => "externalId" in c)).toBe(false);
  });

  it("writes no column at all when the feed moved no value", async () => {
    await sync(feedRow());

    expect(attractionRepo.update).not.toHaveBeenCalled();
    expect(attractionRepo.save).not.toHaveBeenCalled();
  });

  it("still stamps the row as listed", async () => {
    // The stamp was never the broken half: all 32 production rows had it. A
    // regression here would re-date every absence in the park.
    const listedRowIds = await sync(feedRow());

    expect([...listedRowIds]).toEqual([qtRow.id]);
  });

  it("says so once, in the run that writes the mapping", async () => {
    await sync(feedRow());

    expect(logged.filter((l) => l.includes("mapped wiki"))).toHaveLength(1);
    expect(logged.join("\n")).toContain(wikiId);
  });

  it("says nothing on the next run, when the mapping is already right", async () => {
    // `createMapping` is a no-op once the mapping names this row, and it runs
    // for every listed child of every run — a line per adopted row per night
    // would report an identity write that did not happen.
    mappingRepository.findOne.mockResolvedValue({
      id: "mapping-1",
      internalEntityId: qtRow.id,
      internalEntityType: "attraction",
      externalSource: "themeparks-wiki",
      externalEntityId: wikiId,
    });

    await sync(feedRow());

    expect(mappingRepository.save).not.toHaveBeenCalled();
    expect(mappingRepository.update).not.toHaveBeenCalled();
    expect(logged.filter((l) => l.includes("mapped wiki"))).toHaveLength(0);
  });

  describe("the two shapes that must not take the incoming id", () => {
    /**
     * A row carrying its own wiki id, still listed, reached through the
     * Queue-Times id it shares with the incoming entity. It may belong to a
     * different live wiki entity, so claiming this id would route that
     * entity's readings here — the same orphaning the re-issue branch refuses
     * by not rewriting a listed id.
     */
    const ownWikiRow = {
      ...qtRow,
      id: "row-own-wiki",
      externalId: "7a1c0b44-2d6e-4f31-8b55-c0de1f2a3b4c",
    };

    it("does not map a listed row's shared Queue-Times id onto the new entity", async () => {
      attractionRepo.find.mockResolvedValue([{ ...ownWikiRow }]);

      await sync(
        feedRow({ queueTimesEntityId: ownWikiRow.queueTimesEntityId }),
        [wikiId, ownWikiRow.externalId],
      );

      expect(mappingRepository.save).not.toHaveBeenCalled();
      const written = attractionRepo.update.mock.calls.map(
        ([, changes]) => changes,
      );
      expect(written.some((c) => "externalId" in c)).toBe(false);
    });

    it("still moves a re-issued row onto the new id, rather than mapping it", async () => {
      // The counter-check to the case above: the same row, with its own id no
      // longer listed. That is a re-issue, and it keeps its history by taking
      // the id — the branch this change must not have swallowed.
      attractionRepo.find.mockResolvedValue([{ ...ownWikiRow }]);

      await sync(feedRow(), [wikiId]);

      expect(attractionRepo.update).toHaveBeenCalledWith(ownWikiRow.id, {
        externalId: wikiId,
      });
      expect(mappingRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          internalEntityId: ownWikiRow.id,
          externalEntityId: wikiId,
        }),
      );
      expect(logged.filter((l) => l.includes("mapped wiki"))).toHaveLength(0);
    });
  });
});
