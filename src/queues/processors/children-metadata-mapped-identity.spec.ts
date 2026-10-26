import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * A listed wiki id with no row of its own, read through
 * `external_entity_mapping` before the insert (PAR-949).
 *
 * A merge re-points the loser's mappings onto the winner, but
 * `attractions.externalId` is one column, so when the feed keeps listing BOTH
 * ids the winner answers to the loser's only through the mapping. Until this
 * read existed the next run inserted the loser again as a silent `-3` row.
 *
 * Fixture: Parque Warner's `Expedientes Warren`, two wiki ids, both listed.
 * The same fixture without the mapping inserts — that is the counter-check.
 */
describe("ChildrenMetadataProcessor — a listed id that only a mapping knows", () => {
  const attractionRepo = {
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const childEntityRepo = { find: jest.fn(), update: jest.fn() };
  const mappingRepository = {
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    manager: { query: jest.fn().mockResolvedValue([]) },
  };
  const themeParksMapper = { mapAttraction: jest.fn() };

  const parkId = "park-warner";
  const winnerId = "11111111-1111-4111-8111-111111111111";
  const loserId = "22222222-2222-4222-8222-222222222222";
  const winner = {
    id: "row-winner",
    externalId: winnerId,
    slug: "expedientes-warren",
    name: "Expedientes Warren",
    queueTimesEntityId: "8701",
    retiredReason: null,
    retiredKind: null,
    latitude: "40.2300000",
    longitude: "-3.5900000",
    attractionType: "ATTRACTION",
  };

  let processor: ChildrenMetadataProcessor;

  beforeEach(() => {
    jest.clearAllMocks();
    attractionRepo.find.mockResolvedValue([{ ...winner }]);
    attractionRepo.save.mockImplementation(async (row) => ({
      ...row,
      id: "row-inserted",
    }));
    themeParksMapper.mapAttraction.mockReturnValue({
      externalId: loserId,
      name: winner.name,
      parkId,
      attractionType: "ATTRACTION",
    });
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      { unretire: jest.fn() } as any,
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
  });

  const sync = async () => {
    const listedRowIds = new Set<string>();
    await (processor as any).syncAttraction({}, parkId, {
      claimed: new Set<string>(),
      // Both ids are listed: the case the re-issue rule cannot reach.
      listedExternalIds: new Set([winnerId, loserId]),
      listedRowIds,
    });
    return listedRowIds;
  };

  const mappingTo = (internalEntityId: string) => ({
    id: "mapping-loser",
    internalEntityId,
    internalEntityType: "attraction",
    externalSource: "themeparks-wiki",
    externalEntityId: loserId,
  });

  it("stamps the merge winner instead of inserting the loser again", async () => {
    mappingRepository.findOne.mockResolvedValue(mappingTo(winner.id));

    const listed = await sync();

    expect(attractionRepo.save).not.toHaveBeenCalled();
    expect([...listed]).toEqual([winner.id]);
    // The winner keeps its own id; the second one lives in the mapping.
    expect(attractionRepo.update).not.toHaveBeenCalled();
    expect(mappingRepository.save).not.toHaveBeenCalled();
  });

  it("inserts as before when the mapping names a row of another park", async () => {
    mappingRepository.findOne.mockResolvedValue(mappingTo("row-other-park"));

    const listed = await sync();

    expect(attractionRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({
        externalId: loserId,
        slug: "expedientes-warren-2",
      }),
    );
    expect([...listed]).toEqual(["row-inserted"]);
  });

  it("inserts as before when no mapping exists", async () => {
    mappingRepository.findOne.mockResolvedValue(null);

    const listed = await sync();

    expect(attractionRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ externalId: loserId }),
    );
    expect([...listed]).toEqual(["row-inserted"]);
    expect(mappingRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        internalEntityId: "row-inserted",
        externalEntityId: loserId,
      }),
    );
  });
});
