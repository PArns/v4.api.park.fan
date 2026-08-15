import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * These two private methods are the live source of the duplicate attraction
 * rows. Both looked existing rows up by `externalId` only — which is
 * source-scoped — so a ride reported by both ThemeParks.wiki and Queue-Times
 * became two rows, the second taking a "-2" slug. 147 such pairs existed in
 * production, still growing as of 2026-07-14.
 *
 * The lookup was also not park-scoped: findOne({ where: { externalId } })
 * searches every park in the database.
 */
describe("ChildrenMetadataProcessor — cross-source duplicate prevention", () => {
  const attractionRepo = {
    findOne: jest.fn(),
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const mappingRepository = { findOne: jest.fn(), save: jest.fn() };
  const themeParksMapper = { mapAttraction: jest.fn() };

  let processor: ChildrenMetadataProcessor;

  const parkId = "park-blackpool";

  /** The row Queue-Times created first. */
  const queueTimesRow = {
    id: "row-alice",
    externalId: "qt-ride-12979",
    slug: "alice-in-wonderland",
    name: "Alice in Wonderland",
    queueTimesEntityId: "12979",
    parkId,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    attractionRepo.save.mockImplementation((row: any) => ({
      id: "new-row",
      ...row,
    }));
    mappingRepository.findOne.mockResolvedValue(null);

    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      { getRepository: () => ({}) } as any,
      { getRepository: () => ({}) } as any,
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

  describe("wiki sync", () => {
    it("updates the existing Queue-Times row instead of creating a '-2' duplicate", async () => {
      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: "59f971c2-f3fa-4ca4-9b92-6c3a097d5b61",
        name: "Alice in Wonderland",
        parkId,
        latitude: 53.7906,
        longitude: -3.0553,
      });
      attractionRepo.find.mockResolvedValue([queueTimesRow]);

      await (processor as any).syncAttraction({}, parkId);

      expect(attractionRepo.save).not.toHaveBeenCalled();
      expect(attractionRepo.update).toHaveBeenCalledWith(
        "row-alice",
        expect.objectContaining({ latitude: 53.7906 }),
      );
    });

    it("scopes the lookup to the park", async () => {
      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: "wiki-uuid",
        name: "Valhalla",
        parkId,
      });
      attractionRepo.find.mockResolvedValue([]);

      await (processor as any).syncAttraction({}, parkId);

      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({ where: { parkId } }),
      );
    });

    it("does not fold two identically named rides onto the same row in one pass", async () => {
      // Wet'n'Wild really has five rows called "Restroom". With a name
      // fallback and no memory of what it already matched, every incoming
      // restroom would update the same row and the rest would vanish.
      const existingRestroom = {
        id: "row-restroom",
        externalId: "qt-ride-1",
        slug: "restroom",
        name: "Restroom",
        queueTimesEntityId: "1",
        parkId,
      };
      attractionRepo.find.mockResolvedValue([existingRestroom]);
      const ctx = { claimed: new Set<string>() };

      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: "wiki-restroom-1",
        name: "Restroom",
        parkId,
      });
      await (processor as any).syncAttraction({}, parkId, ctx);

      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: "wiki-restroom-2",
        name: "Restroom",
        parkId,
      });
      await (processor as any).syncAttraction({}, parkId, ctx);

      expect(attractionRepo.update).toHaveBeenCalledTimes(1);
      expect(attractionRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({ name: "Restroom", slug: "restroom-2" }),
      );
    });

    it("still creates a genuinely new ride", async () => {
      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: "wiki-uuid",
        name: "Valhalla",
        parkId,
      });
      attractionRepo.find.mockResolvedValue([queueTimesRow]);

      await (processor as any).syncAttraction({}, parkId);

      expect(attractionRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({ name: "Valhalla", slug: "valhalla" }),
      );
    });
  });

  describe("queue-times sync", () => {
    const wikiRow = {
      id: "row-wedgie",
      externalId: "wiki-wedgie-uuid",
      slug: "mega-wedgie",
      name: "Mega Wedgie",
      queueTimesEntityId: null,
      parkId,
    };

    it("adopts the existing wiki row and backfills its queue-times ID", async () => {
      attractionRepo.find.mockResolvedValue([wikiRow]);

      await (processor as any).syncQtAttraction(
        { externalId: "qt-ride-555", name: "Mega Wedgie" },
        parkId,
      );

      expect(attractionRepo.save).not.toHaveBeenCalled();
      expect(attractionRepo.update).toHaveBeenCalledWith(
        "row-wedgie",
        expect.objectContaining({ queueTimesEntityId: "555" }),
      );
    });

    it("still creates a ride the wiki does not know", async () => {
      attractionRepo.find.mockResolvedValue([wikiRow]);

      await (processor as any).syncQtAttraction(
        { externalId: "qt-ride-999", name: "Big Kahuna" },
        parkId,
      );

      expect(attractionRepo.save).toHaveBeenCalledWith(
        expect.objectContaining({ name: "Big Kahuna", slug: "big-kahuna" }),
      );
    });
  });
});
