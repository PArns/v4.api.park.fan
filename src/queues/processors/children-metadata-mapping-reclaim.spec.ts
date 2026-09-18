import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * `external_entity_mapping` has no foreign key on `internal_entity_id`, so a
 * merge that deletes the row a mapping names leaves the mapping behind. The
 * unique index is on `(external_source, external_entity_id)` alone, which
 * means the dead row keeps holding the upstream's id — and `createMapping`
 * used to read that index as "a mapping exists", create nothing and say
 * nothing.
 *
 * What that costs is not tidiness. Without its Queue-Times mapping a ride
 * cannot be resolved by `WaitTimesProcessor` at all (its `externalId`
 * fallback covers `themeparks-wiki` only), so `reconcileMissingAttractions`
 * writes it a permanent `system-reconciliation` CLOSED series. Measured
 * against production on 2026-09-18: 201 stranded rows, 38 of them sitting on
 * the id of a live attraction, 30 of those one park's rides — every one of
 * the 30 open in the Queue-Times feed at that moment and CLOSED in our API.
 *
 * Each case is a pair: the state that produces the outcome, and the same
 * fixture with the deciding fact taken back.
 */
describe("ChildrenMetadataProcessor — stranded mappings", () => {
  const mappingRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    manager: { query: jest.fn() },
  };

  let processor: ChildrenMetadataProcessor;

  const liveRide = "row-abyssus";
  const deadRide = "row-abyssus-ghost";
  const qtId = "qt-ride-11281";

  const createMapping = (internalEntityId: string) =>
    (processor as any).createMapping(
      internalEntityId,
      "attraction",
      "queue-times",
      qtId,
    );

  beforeEach(() => {
    jest.clearAllMocks();
    mappingRepository.find.mockResolvedValue([]);
    mappingRepository.findOne.mockResolvedValue(null);
    mappingRepository.manager.query.mockResolvedValue([]);
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => ({}) } as any,
      {} as any,
      { getRepository: () => ({}) } as any,
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

  it("writes a mapping when the upstream id is unclaimed", async () => {
    await createMapping(liveRide);

    expect(mappingRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        internalEntityId: liveRide,
        externalSource: "queue-times",
        externalEntityId: qtId,
      }),
    );
    expect(mappingRepository.update).not.toHaveBeenCalled();
  });

  it("takes the upstream id back when the row that holds it names a deleted ride", async () => {
    mappingRepository.findOne.mockResolvedValue({
      id: "mapping-1",
      internalEntityId: deadRide,
      internalEntityType: "attraction",
      externalSource: "queue-times",
      externalEntityId: qtId,
    });
    // `SELECT 1 FROM attractions WHERE id::text = $1` finds nothing.
    mappingRepository.manager.query.mockResolvedValue([]);

    await createMapping(liveRide);

    expect(mappingRepository.manager.query).toHaveBeenCalledWith(
      expect.stringContaining("FROM attractions"),
      [deadRide],
    );
    expect(mappingRepository.update).toHaveBeenCalledWith("mapping-1", {
      internalEntityId: liveRide,
      internalEntityType: "attraction",
      matchConfidence: 1.0,
      matchMethod: "exact",
    });
    expect(mappingRepository.save).not.toHaveBeenCalled();
  });

  it("leaves the row alone when the ride it names still exists", async () => {
    // The same fixture, with the deciding fact taken back: two live entities
    // arguing over one upstream id is a genuine conflict, and
    // `ParkMetadataProcessor` owns it. This path must not decide it.
    mappingRepository.findOne.mockResolvedValue({
      id: "mapping-1",
      internalEntityId: deadRide,
      internalEntityType: "attraction",
      externalSource: "queue-times",
      externalEntityId: qtId,
    });
    mappingRepository.manager.query.mockResolvedValue([{ "?column?": 1 }]);

    await createMapping(liveRide);

    expect(mappingRepository.update).not.toHaveBeenCalled();
    expect(mappingRepository.save).not.toHaveBeenCalled();
  });

  it("asks nothing when the row already names the ride we are mapping", async () => {
    mappingRepository.findOne.mockResolvedValue({
      id: "mapping-1",
      internalEntityId: liveRide,
      internalEntityType: "attraction",
      externalSource: "queue-times",
      externalEntityId: qtId,
    });

    await createMapping(liveRide);

    expect(mappingRepository.manager.query).not.toHaveBeenCalled();
    expect(mappingRepository.update).not.toHaveBeenCalled();
    expect(mappingRepository.save).not.toHaveBeenCalled();
  });

  it("compares the stranded id as text, because the column is not a uuid", async () => {
    // `internal_entity_id` is `character varying`. Casting it to uuid would
    // raise 22P02 on any row whose value is not one, inside the sync.
    mappingRepository.findOne.mockResolvedValue({
      id: "mapping-1",
      internalEntityId: "not-a-uuid",
      internalEntityType: "attraction",
      externalSource: "queue-times",
      externalEntityId: qtId,
    });

    await createMapping(liveRide);

    const [sql] = mappingRepository.manager.query.mock.calls[0];
    expect(sql).toContain("id::text = $1");
    expect(sql).not.toContain("::uuid");
  });
});

/**
 * The second half of the same failure: a ride that loses its mapping after it
 * was created never used to get one back, because `syncQtAttraction` only
 * wrote mappings on the branch that creates a row.
 */
describe("ChildrenMetadataProcessor — syncQtAttraction remaps an existing ride", () => {
  const attractionRepo = {
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const mappingRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    manager: { query: jest.fn() },
  };

  let processor: ChildrenMetadataProcessor;

  const feedEntity = {
    externalId: "qt-ride-11281",
    name: "Abyssus",
    latitude: null,
    longitude: null,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    mappingRepository.findOne.mockResolvedValue(null);
    mappingRepository.manager.query.mockResolvedValue([]);
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      {} as any,
      { getRepository: () => ({}) } as any,
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

  it("maps a ride that already exists and carries no mapping", async () => {
    attractionRepo.find.mockResolvedValue([
      {
        id: "row-abyssus",
        externalId: "qt-ride-11281",
        slug: "abyssus",
        name: "Abyssus",
        queueTimesEntityId: "11281",
      },
    ]);

    await (processor as any).syncQtAttraction(feedEntity, "park-energylandia");

    expect(attractionRepo.save).not.toHaveBeenCalled();
    expect(mappingRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        internalEntityId: "row-abyssus",
        externalSource: "queue-times",
        externalEntityId: "qt-ride-11281",
      }),
    );
  });

  it("still maps a ride it creates", async () => {
    attractionRepo.find.mockResolvedValue([]);
    attractionRepo.save.mockResolvedValue({
      id: "row-new",
      externalId: "qt-ride-11281",
    });

    await (processor as any).syncQtAttraction(feedEntity, "park-energylandia");

    expect(mappingRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({ internalEntityId: "row-new" }),
    );
  });
});

describe("ChildrenMetadataProcessor — an entity type outside the four", () => {
  const mappingRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
    manager: { query: jest.fn() },
  };

  let processor: ChildrenMetadataProcessor;

  beforeEach(() => {
    jest.clearAllMocks();
    mappingRepository.manager.query.mockResolvedValue([]);
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => ({}) } as any,
      {} as any,
      { getRepository: () => ({}) } as any,
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

  // `internal_entity_type` is a `character varying`, so a value outside the
  // four types reaches this. A bare property lookup would answer `constructor`
  // with a function and interpolate it into the SQL.
  it.each(["banana", "constructor", "toString", "__proto__"])(
    "leaves a %s row alone instead of interpolating it into SQL",
    async (entityType) => {
      mappingRepository.findOne.mockResolvedValue({
        id: "mapping-1",
        internalEntityId: "row-gone",
        internalEntityType: entityType,
        externalSource: "queue-times",
        externalEntityId: "qt-ride-1",
      });

      await (processor as any).createMapping(
        "row-live",
        "attraction",
        "queue-times",
        "qt-ride-1",
      );

      expect(mappingRepository.manager.query).not.toHaveBeenCalled();
      expect(mappingRepository.update).not.toHaveBeenCalled();
      expect(mappingRepository.save).not.toHaveBeenCalled();
    },
  );
});
