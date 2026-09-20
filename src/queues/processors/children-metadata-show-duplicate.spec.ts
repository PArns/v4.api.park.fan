import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * Queue-Times reports anything you queue for as a ride, so meet & greets and
 * theatre shows arrive here as ATTRACTIONs. ThemeParks.wiki reports the same
 * entity as a SHOW, and `syncShow` puts it in `shows`. Creating the ride row
 * beside it puts one entity in the catalogue twice — and the ride half is the
 * one that starves, because `ConflictResolverService.mergeEntities` keys on
 * the name without the `entityType` and folds the Queue-Times ATTRACTION into
 * the wiki SHOW. Its `qt-ride-…` id never reaches the wait-times lookup, so
 * `reconcileMissingAttractions` writes it `system-reconciliation` CLOSED while
 * the show beside it is fed normally.
 *
 * PAR-161 retired 21 such rows across 6 parks. Every one of them had a `shows`
 * row that predated it — the newest by eight months — so the guard below is
 * what stops the next sync from putting them back.
 */
describe("ChildrenMetadataProcessor — Queue-Times rides that are already shows", () => {
  const attractionRepo = {
    findOne: jest.fn(),
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const showRepo = { find: jest.fn() };
  const mappingRepository = { findOne: jest.fn(), save: jest.fn() };

  let processor: ChildrenMetadataProcessor;

  const parkId = "park-usj";

  /** What Queue-Times sends for the show USJ already carries in `shows`. */
  const incomingShowAsRide = {
    externalId: "qt-ride-12083",
    name: "Sesame Street 4-D Movie Magic™",
    entityType: "ATTRACTION",
  };

  beforeEach(() => {
    jest.clearAllMocks();
    attractionRepo.find.mockResolvedValue([]);
    attractionRepo.save.mockImplementation((row: any) => ({
      id: "new-row",
      ...row,
    }));
    showRepo.find.mockResolvedValue([]);
    mappingRepository.findOne.mockResolvedValue(null);

    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      { retire: jest.fn() } as any,
      { getRepository: () => showRepo } as any,
      { getRepository: () => ({}) } as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      mappingRepository as any,
      {} as any,
      {} as any,
      {} as any,
    );
  });

  it("does not create the ride row when this park already carries a show of that name", async () => {
    showRepo.find.mockResolvedValue([
      { name: "Sesame Street 4-D Movie Magic™" },
    ]);

    await (processor as any).syncQtAttraction(incomingShowAsRide, parkId);

    expect(attractionRepo.save).not.toHaveBeenCalled();
    expect(mappingRepository.save).not.toHaveBeenCalled();
  });

  it("matches across the spellings the two sources disagree on", async () => {
    // The wiki writes this one without the trademark sign, Queue-Times with
    // it. That single character is the whole reason Shrek was fed while
    // Sesame Street starved: the resolver's own `normalizeForMatching` leaves
    // a literal "(tm)" behind, dropping the similarity to 0.882.
    showRepo.find.mockResolvedValue([{ name: "Shrek's 4-D Adventure" }]);

    await (processor as any).syncQtAttraction(
      {
        externalId: "qt-ride-12084",
        name: "Shrek’s 4-D Adventure™",
        entityType: "ATTRACTION",
      },
      parkId,
    );

    expect(attractionRepo.save).not.toHaveBeenCalled();
  });

  it("still creates a ride that is not a show here", async () => {
    showRepo.find.mockResolvedValue([{ name: "Festival of the Lion King" }]);

    await (processor as any).syncQtAttraction(
      {
        externalId: "qt-ride-12979",
        name: "Hollywood Dream - The Ride",
        entityType: "ATTRACTION",
      },
      parkId,
    );

    expect(attractionRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ externalId: "qt-ride-12979" }),
    );
  });

  it("keeps updating a ride that already has a row, show of that name or not", async () => {
    // The guard covers creation only. A row that is already fed must keep its
    // Queue-Times mapping — dropping it is what left 30 rides unresolvable in
    // PAR-311.
    showRepo.find.mockResolvedValue([
      { name: "Sesame Street 4-D Movie Magic™" },
    ]);
    attractionRepo.find.mockResolvedValue([
      {
        id: "row-sesame",
        externalId: "qt-ride-12083",
        slug: "sesame-street-4-d-movie-magic",
        name: "Sesame Street 4-D Movie Magic™",
        queueTimesEntityId: "12083",
        parkId,
      },
    ]);

    await (processor as any).syncQtAttraction(incomingShowAsRide, parkId);

    expect(attractionRepo.save).not.toHaveBeenCalled();
    expect(mappingRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        internalEntityId: "row-sesame",
        externalSource: "queue-times",
        externalEntityId: "qt-ride-12083",
      }),
    );
  });

  it("reads this park's shows once per sync, not once per ride", async () => {
    showRepo.find.mockResolvedValue([{ name: "Festival of the Lion King" }]);
    const ctx = { claimed: new Set<string>() };

    for (const name of ["Ride A", "Ride B", "Ride C"]) {
      await (processor as any).syncQtAttraction(
        { externalId: `qt-ride-${name}`, name, entityType: "ATTRACTION" },
        parkId,
        ctx,
      );
    }

    expect(showRepo.find).toHaveBeenCalledTimes(1);
  });

  it("does not let a show whose name normalizes to nothing block every ride", async () => {
    // `normalizeName` keeps only [a-z0-9] after transliteration, so a name
    // made of punctuation leaves an empty key — and an empty key would sit in
    // the set matching nothing useful. Dropping empties on the show side is
    // what keeps one such row from being compared against at all.
    showRepo.find.mockResolvedValue([{ name: "。。。" }, { name: null }]);

    await (processor as any).syncQtAttraction(
      {
        externalId: "qt-ride-999",
        name: "Hollywood Dream - The Ride",
        entityType: "ATTRACTION",
      },
      parkId,
    );

    expect(attractionRepo.save).toHaveBeenCalledWith(
      expect.objectContaining({ externalId: "qt-ride-999" }),
    );
  });
});
