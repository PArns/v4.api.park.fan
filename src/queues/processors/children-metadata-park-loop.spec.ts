import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * One child that cannot be written used to cost its park the whole feed.
 * The three sync loops ran bare inside the park-wide try/catch, so the first
 * throw ended the park: the children after it, the reclassification and
 * absence steps, the mapping job and the cache eviction all went with it.
 *
 * Measured in production on 2026-10-07 (PAR-714): Knott's Soak City had
 * written eight of its nine rows last on 2026-07-26 — 71 days — and Cedar
 * Point Shores fifteen of seventeen on 2026-07-27. Both parks have the same
 * signature: feed index 0 is written every night, and index 1 is a child
 * whose row lives in the resort's OTHER park (Gremmie Lagoon in Knott's Berry
 * Farm, Muffleheads Beach Bar in Cedar Point). `attractions.externalId` is
 * globally unique, not park-scoped, so that insert can never succeed, and the
 * loop died on it every single run.
 *
 * Each case carries its counter-check, so a green "the rest was written"
 * cannot come from a run where nothing threw at all.
 */
describe("ChildrenMetadataProcessor — one unwritable child does not cost the park", () => {
  const attractionRepo = {
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const retirementService = { retire: jest.fn(), unretire: jest.fn() };
  const childEntityRepo = {
    find: jest.fn().mockResolvedValue([]),
    update: jest.fn(),
    findOne: jest.fn().mockResolvedValue(null),
  };
  const mappingRepository = { find: jest.fn(), findOne: jest.fn() };
  const themeParksMapper = { mapAttraction: jest.fn() };

  let processor: ChildrenMetadataProcessor;
  let entityMappingsQueue: { add: jest.Mock };
  let redis: { del: jest.Mock };

  const parkId = "park-knotts-soak-city";
  const parkName = "Knott’s Soak City";

  /**
   * Knott's Soak City as the feed served it on 2026-10-07, in order and
   * trimmed to the first four attractions. Index 1 is the child that cannot
   * be inserted; index 0, 2 and 3 all have rows of their own.
   */
  const feedChildren = [
    { id: "7a5d8e3d", entityType: "ATTRACTION", name: "Shore Break" },
    { id: "06eccc72", entityType: "ATTRACTION", name: "Gremmie Lagoon" },
    { id: "cac2b7a2", entityType: "ATTRACTION", name: "Old Man Falls" },
    { id: "90beb334", entityType: "ATTRACTION", name: "Laguna Storm Watch" },
    { id: "1acbd120", entityType: "RESTAURANT", name: "Portside Pizza" },
    { id: "035cbd49", entityType: "RESTAURANT", name: "Longboard’s Grill" },
  ];

  /** The id at feed index 1, the one whose row belongs to the sibling park. */
  const unwritableId = "06eccc72";

  /**
   * What Postgres raises when the insert hits the global unique index. The
   * message is asserted on in one case below so the log line stays useful.
   */
  const uniqueViolation = () =>
    Object.assign(
      new Error(
        "duplicate key value violates unique constraint " +
          '"UQ_a94e9dc2762dfca8a463d173657"',
      ),
      { code: "23505" },
    );

  const syncedAttractionNames = (spy: jest.SpyInstance): string[] =>
    spy.mock.calls.map((call) => call[0].name);

  beforeEach(() => {
    jest.clearAllMocks();
    mappingRepository.find.mockResolvedValue([]);
    mappingRepository.findOne.mockResolvedValue(null);
    childEntityRepo.find.mockResolvedValue([]);
    childEntityRepo.findOne.mockResolvedValue(null);
    attractionRepo.find.mockResolvedValue([]);
    entityMappingsQueue = { add: jest.fn() };
    redis = { del: jest.fn() };

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

    (processor as any).parksService = {
      findAll: jest
        .fn()
        .mockResolvedValue([{ id: parkId, name: parkName, wikiEntityId: "w" }]),
    };
    (processor as any).themeParksClient = {
      getEntityChildren: jest
        .fn()
        .mockResolvedValue({ children: feedChildren }),
    };
    (processor as any).entityMappingsQueue = entityMappingsQueue;
    (processor as any).redis = redis;
  });

  /** Throws for exactly the child at feed index 1, succeeds for the rest. */
  const syncAttractionThatThrowsOnce = (error: Error = uniqueViolation()) =>
    jest
      .spyOn(processor as any, "syncAttraction")
      .mockImplementation(async (entity: any) => {
        if (entity.id === unwritableId) throw error;
      });

  describe("the attraction loop", () => {
    it("writes every other listed attraction when one of them throws", async () => {
      const spy = syncAttractionThatThrowsOnce();
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);

      await processor.handleFetchChildren({} as any);

      // All four are attempted, and the three writable ones get through.
      expect(syncedAttractionNames(spy)).toEqual([
        "Shore Break",
        "Gremmie Lagoon",
        "Old Man Falls",
        "Laguna Storm Watch",
      ]);
      // The two that used to be unreachable: everything after feed index 1.
      expect(syncedAttractionNames(spy).slice(2)).toEqual([
        "Old Man Falls",
        "Laguna Storm Watch",
      ]);
    });

    it("stopped at the throwing child before this change", async () => {
      // The counter-check for the case above, and the reason the fix exists.
      // Without a guard per child, the loop propagates out of the three sync
      // loops into the park-wide handler. Reproduced here by running the same
      // feed through a bare loop over the same spy.
      const spy = syncAttractionThatThrowsOnce();

      const bare = async () => {
        for (const child of feedChildren.filter(
          (c) => c.entityType === "ATTRACTION",
        )) {
          await (processor as any).syncAttraction(child, parkId, {
            claimed: new Set(),
            listedExternalIds: new Set(),
          });
        }
      };

      await expect(bare()).rejects.toThrow("unique constraint");
      // Index 0 was written, index 1 threw, index 2 and 3 were never reached.
      expect(syncedAttractionNames(spy)).toEqual([
        "Shore Break",
        "Gremmie Lagoon",
      ]);
    });

    it("names the park, the child and the error so the skip is not silent", async () => {
      const errors: string[] = [];
      jest
        .spyOn((processor as any).logger, "error")
        .mockImplementation((msg: any) => {
          errors.push(String(msg));
        });
      const warns: string[] = [];
      jest
        .spyOn((processor as any).logger, "warn")
        .mockImplementation((msg: any) => {
          warns.push(String(msg));
        });
      syncAttractionThatThrowsOnce();
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);

      await processor.handleFetchChildren({} as any);

      const skip = errors.find((e) => e.includes("skipped attraction"));
      expect(skip).toBeDefined();
      expect(skip).toContain(parkName);
      expect(skip).toContain("Gremmie Lagoon");
      expect(skip).toContain(unwritableId);
      expect(skip).toContain("UQ_a94e9dc2762dfca8a463d173657");

      // And the park-level tally, so a run's summary carries the number.
      expect(warns.some((w) => w.includes("1 of 6 listed children"))).toBe(
        true,
      );
      expect(warns.some((w) => w.includes("Skipped Children: 1"))).toBe(true);
    });

    it("logs no skip at all when every child is writable", async () => {
      // The counter-check: the assertions above have to be able to fail.
      const errors: string[] = [];
      jest
        .spyOn((processor as any).logger, "error")
        .mockImplementation((msg: any) => {
          errors.push(String(msg));
        });
      const spy = jest
        .spyOn(processor as any, "syncAttraction")
        .mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);

      await processor.handleFetchChildren({} as any);

      expect(syncedAttractionNames(spy)).toHaveLength(4);
      expect(errors.filter((e) => e.includes("skipped"))).toEqual([]);
    });
  });

  describe("the steps after the loop", () => {
    it("still reclassifies, retires the absent and evicts the cache", async () => {
      // These four all sit after the sync loops and each has its own
      // try/catch. Before the fix the throw jumped past all of them, so a
      // park with one bad child also stopped retiring and stopped
      // invalidating — which is why Soak City's rows went stale rather than
      // merely unwritten.
      syncAttractionThatThrowsOnce();
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);
      const reclassified = jest
        .spyOn(processor as any, "retireReclassifiedAttractions")
        .mockResolvedValue(undefined);
      const childEntities = jest
        .spyOn(processor as any, "retireReclassifiedChildEntities")
        .mockResolvedValue(undefined);
      const absent = jest
        .spyOn(processor as any, "retireAbsentAttractions")
        .mockResolvedValue(undefined);

      await processor.handleFetchChildren({} as any);

      expect(reclassified).toHaveBeenCalledTimes(1);
      expect(childEntities).toHaveBeenCalledTimes(1);
      expect(absent).toHaveBeenCalledTimes(1);
      expect(entityMappingsQueue.add).toHaveBeenCalledTimes(1);
      expect(redis.del).toHaveBeenCalledTimes(1);
    });

    it("hands the absence step the full listed set, so a skipped child is not treated as gone", async () => {
      // The one real hazard the fix opens: the absence step now runs where
      // the throw used to prevent it. It must not read the skipped child as
      // absent. It cannot, because every id the response carried is in
      // `listedExternalIds` whatever happened to its row — asserted here
      // rather than argued, since the row's own clock would otherwise start.
      syncAttractionThatThrowsOnce();
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);
      const absent = jest
        .spyOn(processor as any, "retireAbsentAttractions")
        .mockResolvedValue(undefined);

      await processor.handleFetchChildren({} as any);

      const [, , listedCount, listedExternalIds] = absent.mock.calls[0];
      expect(listedCount).toBe(4);
      expect([...(listedExternalIds as Set<string>)].sort()).toEqual(
        feedChildren.map((c) => c.id).sort(),
      );
      expect((listedExternalIds as Set<string>).has(unwritableId)).toBe(true);
    });
  });

  describe("shows and restaurants", () => {
    it("keeps going when a restaurant throws, and still writes the next one", async () => {
      jest
        .spyOn(processor as any, "syncAttraction")
        .mockResolvedValue(undefined);
      const spy = jest
        .spyOn(processor as any, "syncRestaurant")
        .mockImplementation(async (entity: any) => {
          if (entity.id === "1acbd120") throw new Error("restaurant write");
        });

      await processor.handleFetchChildren({} as any);

      expect(spy.mock.calls.map((c) => (c[0] as any).name)).toEqual([
        "Portside Pizza",
        "Longboard’s Grill",
      ]);
    });

    it("keeps going when a show throws", async () => {
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockResolvedValue({
          children: [
            { id: "s1", entityType: "SHOW", name: "Snoopy on Ice" },
            { id: "s2", entityType: "SHOW", name: "Charlie Brown Stage" },
          ],
        }),
      };
      const spy = jest
        .spyOn(processor as any, "syncShow")
        .mockImplementation(async (entity: any) => {
          if (entity.id === "s1") throw new Error("show write");
        });

      await processor.handleFetchChildren({} as any);

      expect(spy.mock.calls.map((c) => (c[0] as any).name)).toEqual([
        "Snoopy on Ice",
        "Charlie Brown Stage",
      ]);
    });
  });

  describe("what the guard does not swallow", () => {
    it("still loses the park when the feed call itself fails", async () => {
      // The guard is per child, not per park. A feed that cannot be fetched
      // leaves nothing to iterate, and the park-wide handler keeps that case.
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockRejectedValue(new Error("429")),
      };
      const spy = jest
        .spyOn(processor as any, "syncAttraction")
        .mockResolvedValue(undefined);
      const absent = jest
        .spyOn(processor as any, "retireAbsentAttractions")
        .mockResolvedValue(undefined);

      await expect(
        processor.handleFetchChildren({} as any),
      ).resolves.toBeUndefined();

      expect(spy).not.toHaveBeenCalled();
      expect(absent).not.toHaveBeenCalled();
    });
  });
});
