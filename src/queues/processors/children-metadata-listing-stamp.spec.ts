import {
  COORDINATE_SCALE,
  ChildrenMetadataProcessor,
  coordinateChanged,
  lastListingOf,
} from "./children-metadata.processor";

/**
 * Two halves of one change (PAR-739).
 *
 * The sync used to write `name`, `latitude` and `longitude` on every listed
 * row of every run, and `absent_since` read the resulting `updatedAt` as "the
 * feed listed this row". Both of those were measured on 2026-10-13: 721,469
 * such statements since 2026-06-03 and 6,706 in the last run alone, while of
 * 434 rows checked against the live feed across 12 parks **none** carried a
 * value the feed had moved. The signal was therefore paid for twice over and
 * still wrong — a curation sweep moves `updatedAt` too, which dated nine rows'
 * absence from the sweep rather than from their last listing.
 *
 * So the write is now conditional and the listing is recorded on its own
 * column, in one statement per park. Each case below has the counter-check
 * that makes it discriminating: a row the feed DID move is written, and a
 * comparison that only looks right is the one this file exists to prevent.
 */
describe("ChildrenMetadataProcessor — the listing stamp and the value comparison", () => {
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
  const manager = { query: jest.fn() };
  const mappingRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    manager,
  };
  const themeParksMapper = { mapAttraction: jest.fn() };

  let processor: ChildrenMetadataProcessor;

  const parkId = "park-europa-park";

  /**
   * `Blue Fire Megacoaster` as the feed and the table both hold it. The
   * coordinates are the stored ones: `numeric(10,7)`, and TypeORM hands a
   * numeric back as a string, which is the whole reason the comparison is not
   * a `!==`.
   */
  const storedRow = {
    id: "row-blue-fire",
    externalId: "d5a7f0a2-1b8e-4a1e-9e1e-0a3f6a7c9d11",
    slug: "blue-fire-megacoaster",
    name: "Blue Fire Megacoaster",
    queueTimesEntityId: null,
    retiredReason: null,
    latitude: "48.2661670",
    longitude: "7.7222220",
    attractionType: "RIDE",
  };

  const feedRow = (over: Record<string, unknown> = {}) => ({
    externalId: storedRow.externalId,
    name: storedRow.name,
    parkId,
    latitude: 48.266167,
    longitude: 7.722222,
    attractionType: "RIDE",
    ...over,
  });

  beforeEach(() => {
    jest.clearAllMocks();
    attractionRepo.find.mockResolvedValue([{ ...storedRow }]);
    attractionRepo.save.mockResolvedValue({
      id: "row-fresh",
      externalId: "new-id",
    });
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
  });

  const sync = (
    feed: Record<string, unknown>,
    listedRowIds = new Set<string>(),
  ) => {
    themeParksMapper.mapAttraction.mockReturnValue(feed);
    return (processor as any)
      .syncAttraction({}, parkId, {
        claimed: new Set<string>(),
        listedExternalIds: new Set([storedRow.externalId]),
        listedRowIds,
      })
      .then(() => listedRowIds);
  };

  describe("what the sync writes to a row it matched", () => {
    it("writes nothing when the feed carries the stored values", async () => {
      await sync(feedRow());

      expect(attractionRepo.update).not.toHaveBeenCalled();
      expect(attractionRepo.save).not.toHaveBeenCalled();
    });

    it("writes the one field the feed moved, and only that one", async () => {
      await sync(feedRow({ name: "Blue Fire Megacoaster XL" }));

      expect(attractionRepo.update).toHaveBeenCalledTimes(1);
      expect(attractionRepo.update).toHaveBeenCalledWith(storedRow.id, {
        name: "Blue Fire Megacoaster XL",
      });
    });

    it("writes a coordinate the park actually moved", async () => {
      await sync(feedRow({ latitude: 48.27 }));

      expect(attractionRepo.update).toHaveBeenCalledWith(storedRow.id, {
        latitude: 48.27,
      });
    });

    it("does not write a coordinate that differs past the stored scale", async () => {
      // The trap this comparison exists for. The wiki publishes full float
      // precision, the column keeps seven decimals, and a row written at
      // every run is exactly what a `stored !== incoming` on those two values
      // produces — 6,706 of them per run, none of which changes a thing.
      await sync(
        feedRow({ latitude: 48.26616704321, longitude: 7.72222199999 }),
      );

      expect(attractionRepo.update).not.toHaveBeenCalled();
    });

    it("leaves a stored coordinate alone where the feed carries none", async () => {
      // `mapAttraction` omits the field for an entity without a location. The
      // update has always let such a column stand rather than nulling a
      // coordinate we once had, and a value comparison must not turn that
      // into a write of `undefined`.
      await sync(feedRow({ latitude: undefined, longitude: undefined }));

      expect(attractionRepo.update).not.toHaveBeenCalled();
    });

    it("fills a coordinate the row never had", async () => {
      attractionRepo.find.mockResolvedValue([
        { ...storedRow, latitude: null, longitude: null },
      ]);

      await sync(feedRow());

      expect(attractionRepo.update).toHaveBeenCalledWith(storedRow.id, {
        latitude: 48.266167,
        longitude: 7.722222,
      });
    });

    it("writes attractionType only when the feed carries one that differs", async () => {
      // `/children` does not carry the field at all, which is why the
      // production statement never mentioned it. Where the detail sync does
      // provide one, a change still has to land.
      await sync(feedRow({ attractionType: undefined }));
      expect(attractionRepo.update).not.toHaveBeenCalled();

      await sync(feedRow({ attractionType: "WALKTHROUGH" }));
      expect(attractionRepo.update).toHaveBeenCalledWith(storedRow.id, {
        attractionType: "WALKTHROUGH",
      });
    });
  });

  describe("which rows count as listed", () => {
    it("collects the row it matched, written or not", async () => {
      const listed = await sync(feedRow());

      // The row got no write at all, and it is still the row the feed listed
      // today. That is the point of the stamp: before this change the only
      // record of the listing was the write that is now skipped.
      expect([...listed]).toEqual([storedRow.id]);
    });

    it("collects a row it inserted", async () => {
      attractionRepo.find.mockResolvedValue([]);

      const listed = await sync(feedRow({ externalId: "new-id" }));

      expect([...listed]).toEqual(["row-fresh"]);
    });

    it("collects a row reached under a re-issued id", async () => {
      // The feed dropped the row's id and reports the same attraction under a
      // new one. It is in the feed, so it is listed — the same reading the
      // absence step takes of `claimed`.
      themeParksMapper.mapAttraction.mockReturnValue(
        feedRow({ externalId: "reissued-id" }),
      );
      const listedRowIds = new Set<string>();

      await (processor as any).syncAttraction({}, parkId, {
        claimed: new Set<string>(),
        listedExternalIds: new Set(["reissued-id"]),
        listedRowIds,
      });

      expect([...listedRowIds]).toEqual([storedRow.id]);
    });
  });

  describe("the stamp itself", () => {
    it("is one statement for the whole park", async () => {
      await (processor as any).stampListedRows(
        new Set(["row-a", "row-b", "row-c"]),
      );

      expect(manager.query).toHaveBeenCalledTimes(1);
      const [sql, params] = manager.query.mock.calls[0];
      expect(sql).toContain("SET last_listed_at = now()");
      expect(sql).toContain("id = ANY($1::uuid[])");
      expect(params[0]).toEqual(["row-a", "row-b", "row-c"]);
    });

    it("is raw SQL, so it does not move `updatedAt`", async () => {
      // Going through the repository would move the `@UpdateDateColumn` and
      // put the signal back on the column this one replaces.
      await (processor as any).stampListedRows(new Set(["row-a"]));

      expect(attractionRepo.update).not.toHaveBeenCalled();
    });

    it("issues nothing for a park whose feed listed nothing", async () => {
      await (processor as any).stampListedRows(new Set<string>());

      expect(manager.query).not.toHaveBeenCalled();
    });
  });

  describe("the comparison and the fallback, on their own", () => {
    it("compares at the scale the column stores", () => {
      expect(COORDINATE_SCALE).toBe(7);
      expect(coordinateChanged("48.2661670", 48.26616704321)).toBe(false);
      expect(coordinateChanged("48.2661670", 48.266168)).toBe(true);
      // Rounding happens on both sides, so a value that rounds INTO the
      // stored one is not a change either.
      expect(coordinateChanged("48.2661670", 48.26616699999)).toBe(false);
    });

    it("treats zero as a coordinate and absence as no information", () => {
      // 0° is a real latitude on the equator, so a falsy check here would
      // rewrite every row that has one on every run.
      expect(coordinateChanged("0.0000000", 0)).toBe(false);
      expect(coordinateChanged(null, 48.266167)).toBe(true);
      expect(coordinateChanged("48.2661670", undefined)).toBe(false);
      expect(coordinateChanged(null, undefined)).toBe(false);
    });

    it("reads the stamp where there is one and `updatedAt` where there is not", () => {
      const listed = new Date("2026-10-01T03:30:00Z");
      const written = new Date("2026-10-09T12:29:46Z");

      expect(lastListingOf({ lastListedAt: listed, updatedAt: written })).toBe(
        listed,
      );
      expect(lastListingOf({ lastListedAt: null, updatedAt: written })).toBe(
        written,
      );
      expect(lastListingOf({ updatedAt: written })).toBe(written);
    });
  });
});
