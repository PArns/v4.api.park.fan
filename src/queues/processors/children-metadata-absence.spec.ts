import { IsNull } from "typeorm";
import {
  ABSENT_UPSTREAM_REASON,
  RECLASSIFIED_UPSTREAM_REASONS,
  isOnParkPage,
  retiredKindOf,
} from "../../attractions/services/attraction-retirement.service";
import { SYNTHETIC_SOURCES } from "../../common/utils/outage-rows.sql";
import {
  ABSENT_UPSTREAM_RETIRE_DAYS,
  ABSENT_UPSTREAM_SEED_FRESH_WRITE_HOURS,
  ABSENT_UPSTREAM_SEED_LOOKBACK_DAYS,
  ChildrenMetadataProcessor,
} from "./children-metadata.processor";

/**
 * An entity can leave a park's `/children` without being reclassified: the
 * wiki re-issues seasonal mazes under a new id every season, and once did the
 * same to all of Walibi Belgium's rides. The old row then stayed active
 * forever. On 2026-10-02 that was 515 rows in 83 parks, and 43 of them were
 * served as CLOSED in place of their live twin (PAR-621).
 *
 * Each retiring case has a twin with the one deciding fact taken back, so a
 * green "not retired" cannot come from a branch that never ran.
 */
describe("ChildrenMetadataProcessor — attractions absent upstream", () => {
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

  const parkId = "park-sfot";
  const parkName = "Six Flags Over Texas";
  const now = new Date("2026-10-02T03:30:00Z");

  const daysBefore = (days: number) =>
    new Date(now.getTime() - days * 86_400_000);

  /**
   * `Ghost Town NEW!`, last listed on 2026-06-03 and carrying the clock to
   * prove it. `absentSince` is what the gate reads, so it is set here and the
   * seeding path below gets its own fixtures — a row in the steady state is
   * seeded, and these tests are about the gate rather than about the seed.
   */
  const deadRow = {
    id: "row-ghost-town",
    name: "Ghost Town NEW!",
    externalId: "0b7d7c55-1a2b-4c3d-8e9f-001122334455",
    updatedAt: daysBefore(121),
    absentSince: daysBefore(121),
  };
  const liveExternalId = "f2d2b7fd-4618-4a73-98fd-efffd99840de";

  beforeEach(() => {
    jest.clearAllMocks();
    // A fresh copy per test: the processor writes the seeded or cleared clock
    // back onto the row objects it was handed.
    attractionRepo.find.mockResolvedValue([{ ...deadRow }]);
    mappingRepository.find.mockResolvedValue([]);
    mappingRepository.findOne.mockResolvedValue(null);
    manager.query.mockResolvedValue([]);
    childEntityRepo.find.mockResolvedValue([]);
    childEntityRepo.findOne.mockResolvedValue(null);
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

  const retireAbsent = (
    listedExternalIds: string[] = [liveExternalId],
    claimed: string[] = [],
    listedAttractionCount = listedExternalIds.length,
  ) =>
    (processor as any).retireAbsentAttractions(
      parkId,
      parkName,
      listedAttractionCount,
      new Set(listedExternalIds),
      new Set(claimed),
      now,
    );

  describe("the park synced and the row did not", () => {
    it("retires a row absent for the whole window, as not closed", async () => {
      await retireAbsent();

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      const [requests] = retirementService.retire.mock.calls[0];
      expect(requests).toEqual([
        {
          attractionId: "row-ghost-town",
          retiredAt: now.toISOString(),
          reason: ABSENT_UPSTREAM_REASON,
        },
      ]);
    });

    it("asks for this park's active rows without a Queue-Times id, and brings the clock along", async () => {
      // The query no longer carries the window. It used to filter on
      // `updatedAt`, which is why a curation write bought a row 60 more days
      // (PAR-656); the window is now applied to `absentSince` in the two
      // cases below, and this run has to see rows of every age in order to
      // stop the clock on the ones the feed listed again.
      await retireAbsent();

      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: {
            parkId,
            retiredAt: IsNull(),
            queueTimesEntityId: IsNull(),
          },
          select: expect.arrayContaining(["updatedAt", "absentSince"]),
        }),
      );
    });

    it("retires a row whose clock passed the window and keeps one a day short of it", async () => {
      expect(ABSENT_UPSTREAM_RETIRE_DAYS).toBe(60);

      attractionRepo.find.mockResolvedValue([
        { ...deadRow, absentSince: daysBefore(61) },
      ]);
      await retireAbsent();
      expect(retirementService.retire).toHaveBeenCalledTimes(1);

      jest.clearAllMocks();
      mappingRepository.find.mockResolvedValue([]);
      manager.query.mockResolvedValue([]);
      attractionRepo.find.mockResolvedValue([
        { ...deadRow, absentSince: daysBefore(59) },
      ]);
      await retireAbsent();
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("no longer lets a curation write push the retirement out by a window", async () => {
      // The six rows measured on 2026-10-04: absent upstream for months,
      // `updatedAt` set to the day a curation charge wrote them. Under the old
      // gate none of them was a candidate at all.
      attractionRepo.find.mockResolvedValue([
        { ...deadRow, updatedAt: daysBefore(4), absentSince: daysBefore(164) },
      ]);

      await retireAbsent();

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      const [requests] = retirementService.retire.mock.calls[0];
      expect(requests[0].reason).toBe(ABSENT_UPSTREAM_REASON);
    });

    it("keeps the row when its id is still listed", async () => {
      await retireAbsent([liveExternalId, deadRow.externalId]);

      expect(attractionRepo.find).toHaveBeenCalledTimes(1);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("keeps the row when this run claimed it under another id", async () => {
      await retireAbsent([liveExternalId], [deadRow.id]);

      expect(attractionRepo.find).toHaveBeenCalledTimes(1);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("keeps a Queue-Times row, which the wiki never listed", async () => {
      attractionRepo.find.mockResolvedValue([
        { ...deadRow, externalId: "qt-ride-1234" },
      ]);

      await retireAbsent();

      expect(retirementService.retire).not.toHaveBeenCalled();
    });
  });

  describe("the park's own sync is not fresh", () => {
    it("retires nothing when the response listed no attraction at all", async () => {
      // A park whose `/children` comes back without rides is our feed going
      // quiet, not every ride going away.
      await retireAbsent([], [], 0);

      expect(attractionRepo.find).not.toHaveBeenCalled();
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("is not reached when the park's `/children` call fails", async () => {
      const spy = jest
        .spyOn(processor as any, "retireAbsentAttractions")
        .mockResolvedValue(undefined);
      (processor as any).parksService = {
        findAll: jest
          .fn()
          .mockResolvedValue([
            { id: parkId, name: parkName, wikiEntityId: "wiki-sfot" },
          ]),
      };
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockRejectedValue(new Error("503")),
      };
      (processor as any).entityMappingsQueue = { add: jest.fn() };
      (processor as any).redis = { del: jest.fn() };

      await processor.handleFetchChildren({} as any);

      expect(spy).not.toHaveBeenCalled();
    });

    it("is handed every listed id and the rows this run claimed", async () => {
      // The pair to the case above: same park, the call succeeds.
      const spy = jest
        .spyOn(processor as any, "retireAbsentAttractions")
        .mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncAttraction")
        .mockImplementation(async (_e: unknown, _p: unknown, ctx: any) => {
          ctx.claimed.add("row-claimed");
        });
      jest.spyOn(processor as any, "syncShow").mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "retireReclassifiedAttractions")
        .mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "retireReclassifiedChildEntities")
        .mockResolvedValue(undefined);
      (processor as any).parksService = {
        findAll: jest
          .fn()
          .mockResolvedValue([
            { id: parkId, name: parkName, wikiEntityId: "wiki-sfot" },
          ]),
      };
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockResolvedValue({
          children: [
            { id: liveExternalId, entityType: "ATTRACTION", name: "SAW" },
            { id: "show-1", entityType: "SHOW", name: "Fright Lights" },
            { id: "hotel-1", entityType: "HOTEL", name: "Hotel" },
          ],
        }),
      };
      (processor as any).entityMappingsQueue = { add: jest.fn() };
      (processor as any).redis = { del: jest.fn() };

      await processor.handleFetchChildren({} as any);

      expect(spy).toHaveBeenCalledTimes(1);
      const [p, name, count, listed, claimed] = spy.mock.calls[0];
      expect([p, name, count]).toEqual([parkId, parkName, 1]);
      expect([...(listed as Set<string>)].sort()).toEqual(
        ["hotel-1", liveExternalId, "show-1"].sort(),
      );
      expect([...(claimed as Set<string>)]).toEqual(["row-claimed"]);
    });
  });

  describe("the absence clock", () => {
    /** The same row before anything had written `absent_since`. */
    const unseeded = {
      ...deadRow,
      updatedAt: daysBefore(4),
      absentSince: null,
    };

    /** `[seedReadingQuery, seedUpdate, recentReadingQuery]`, in call order. */
    const seedCalls = () => manager.query.mock.calls;

    it("seeds from the last real reading when it is older than the row's own write", async () => {
      // The case the column exists for: curated four days ago, silent since
      // April. The clock has to say April, or the retirement waits 56 days for
      // a fact that was already true.
      attractionRepo.find.mockResolvedValue([{ ...unseeded }]);
      manager.query.mockImplementation(async (sql: string) =>
        sql.includes("GROUP BY")
          ? [{ attractionId: deadRow.id, last_read: daysBefore(164) }]
          : [],
      );

      await retireAbsent();

      const [sql, params] = seedCalls()[0];
      expect(sql).toContain("GROUP BY");
      expect(sql).toContain("is_heartbeat IS NOT TRUE");
      expect(params[0]).toEqual([deadRow.id]);
      expect(params[1]).toEqual(daysBefore(ABSENT_UPSTREAM_SEED_LOOKBACK_DAYS));
      expect(params[2]).toEqual([...SYNTHETIC_SOURCES]);

      const [updateSql, updateParams] = seedCalls()[1];
      expect(updateSql).toContain("SET absent_since = v.absent_since");
      expect(updateSql).toContain("a.absent_since IS NULL");
      expect(updateParams[1]).toEqual([daysBefore(164).toISOString()]);

      // And the same run acts on it, rather than waiting for the next.
      expect(retirementService.retire).toHaveBeenCalledTimes(1);
    });

    it("starts the clock today for a row the sync wrote inside the fresh-write window", async () => {
      // The regression this window exists to prevent. Measured on 2026-10-04:
      // 421 of the 2.393 active rows in scope had no real reading in 60 days
      // AND had been written inside two days, so the feed was listing them.
      // Back-dating one of those to its last reading retires it on the first
      // run an id goes missing, instead of 60 days later.
      attractionRepo.find.mockResolvedValue([
        { ...unseeded, updatedAt: daysBefore(1) },
      ]);
      manager.query.mockImplementation(async (sql: string) =>
        sql.includes("GROUP BY")
          ? [{ attractionId: deadRow.id, last_read: daysBefore(164) }]
          : [],
      );

      await retireAbsent();

      // No lookback at all: nothing was back-datable, so nothing was asked.
      expect(
        manager.query.mock.calls.filter(([sql]: [string]) =>
          sql.includes("GROUP BY"),
        ),
      ).toHaveLength(0);
      const [, updateParams] = manager.query.mock.calls[0];
      expect(updateParams[1]).toEqual([now.toISOString()]);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("back-dates a row written outside the fresh-write window", async () => {
      // The pair to the case above, with the one deciding fact moved past the
      // window. Four days is what the six measured rows carried.
      expect(ABSENT_UPSTREAM_SEED_FRESH_WRITE_HOURS).toBe(72);
      attractionRepo.find.mockResolvedValue([
        { ...unseeded, updatedAt: daysBefore(4) },
      ]);
      manager.query.mockImplementation(async (sql: string) =>
        sql.includes("GROUP BY")
          ? [{ attractionId: deadRow.id, last_read: daysBefore(164) }]
          : [],
      );

      await retireAbsent();

      expect(seedCalls()[0][0]).toContain("GROUP BY");
      expect(seedCalls()[1][1][1]).toEqual([daysBefore(164).toISOString()]);
      expect(retirementService.retire).toHaveBeenCalledTimes(1);
    });

    it("falls back to the row's own write when no real reading is left", async () => {
      // `NEXUS AI` on 2026-10-04: created 2026-08-20, 1025 rows in
      // `queue_data`, every one of them our own bookkeeping. The seed can then
      // only be too late, which delays a retirement and never causes one.
      attractionRepo.find.mockResolvedValue([{ ...unseeded }]);

      await retireAbsent();

      const [, updateParams] = seedCalls()[1];
      expect(updateParams[1]).toEqual([daysBefore(4).toISOString()]);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("keeps the earlier value when the row's own write is the older one", async () => {
      attractionRepo.find.mockResolvedValue([
        { ...unseeded, updatedAt: daysBefore(200) },
      ]);
      manager.query.mockImplementation(async (sql: string) =>
        sql.includes("GROUP BY")
          ? [{ attractionId: deadRow.id, last_read: daysBefore(164) }]
          : [],
      );

      await retireAbsent();

      const [, updateParams] = seedCalls()[1];
      expect(updateParams[1]).toEqual([daysBefore(200).toISOString()]);
    });

    it("asks for no reading at all when every row already has a clock", async () => {
      await retireAbsent();

      // Only the seven-day reading gate, no seed lookback: the 400-day window
      // is paid for once per row and never again.
      expect(manager.query).toHaveBeenCalledTimes(1);
      expect(manager.query.mock.calls[0][1][1]).toEqual(daysBefore(7));
    });

    it("stops the clock on a row the feed listed again", async () => {
      await retireAbsent([liveExternalId, deadRow.externalId]);

      const [sql, params] = manager.query.mock.calls[0];
      expect(sql).toContain("SET absent_since = NULL");
      expect(params[0]).toEqual([deadRow.id]);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("stops the clock on a row this run claimed under another id", async () => {
      await retireAbsent([liveExternalId], [deadRow.id]);

      const [sql] = manager.query.mock.calls[0];
      expect(sql).toContain("SET absent_since = NULL");
    });

    it("writes nothing when the listed rows never carried a clock", async () => {
      attractionRepo.find.mockResolvedValue([
        { ...deadRow, absentSince: null },
      ]);

      await retireAbsent([liveExternalId, deadRow.externalId]);

      expect(manager.query).not.toHaveBeenCalled();
    });

    it("leaves a Queue-Times row out of both directions", async () => {
      // Its id is one the wiki never issued, so the feed not listing it says
      // nothing. Neither the clock nor the retirement applies.
      attractionRepo.find.mockResolvedValue([
        { ...deadRow, externalId: "qt-ride-1234" },
      ]);

      await retireAbsent();

      expect(manager.query).not.toHaveBeenCalled();
      expect(retirementService.retire).not.toHaveBeenCalled();
    });
  });

  describe("a row another source still fills", () => {
    it("keeps a row that received a reading of its own within the week", async () => {
      manager.query.mockResolvedValue([{ attractionId: deadRow.id }]);

      await retireAbsent();

      expect(manager.query).toHaveBeenCalledTimes(1);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("does not count our own bookkeeping as a reading", async () => {
      await retireAbsent();

      const [sql, params] = manager.query.mock.calls[0];
      expect(sql).toContain("is_heartbeat IS NOT TRUE");
      // `queue_data."attractionId"` is uuid in the database even though the
      // entity declares text; a text[] parameter fails with "uuid = text".
      expect(sql).toContain("ANY($1::uuid[])");
      expect(params[0]).toEqual([deadRow.id]);
      expect(params[1]).toEqual(new Date(now.getTime() - 7 * 86_400_000));
      expect(params[2]).toEqual([...SYNTHETIC_SOURCES]);
      expect(retirementService.retire).toHaveBeenCalledTimes(1);
    });

    it("keeps a row with a mapping from another source", async () => {
      mappingRepository.find.mockResolvedValue([
        { internalEntityId: deadRow.id },
      ]);

      await retireAbsent();

      expect(retirementService.retire).not.toHaveBeenCalled();
    });
  });

  describe("the id comes back", () => {
    it("lifts the retirement in syncAttraction", async () => {
      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: deadRow.externalId,
        name: deadRow.name,
        parkId,
      });
      attractionRepo.find.mockResolvedValue([
        {
          ...deadRow,
          slug: "ghost-town-new",
          queueTimesEntityId: null,
          retiredReason: ABSENT_UPSTREAM_REASON,
        },
      ]);

      await (processor as any).syncAttraction({}, parkId);

      expect(retirementService.unretire).toHaveBeenCalledWith(deadRow.id);
    });
  });

  /**
   * A maze missing from `/children` between November and September is out of
   * season, not gone. The first run of the absence step retired 186 rows in
   * 41 parks on 2026-10-03, seasonal mazes among them (PAR-682).
   */
  describe("a seasonal row between two seasons", () => {
    const seasonal = (over: Record<string, unknown> = {}) => ({
      ...deadRow,
      isSeasonal: true,
      curatedIsSeasonal: null,
      ...over,
    });

    it("keeps a row the detector calls seasonal", async () => {
      attractionRepo.find.mockResolvedValue([seasonal()]);

      await retireAbsent();

      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("keeps a row an editor marked seasonal against the detector", async () => {
      attractionRepo.find.mockResolvedValue([
        seasonal({ isSeasonal: false, curatedIsSeasonal: true }),
      ]);

      await retireAbsent();

      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("retires a row an editor marked not seasonal against the detector", async () => {
      attractionRepo.find.mockResolvedValue([
        seasonal({ curatedIsSeasonal: false }),
      ]);

      await retireAbsent();

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
    });

    it("retires a seasonal row whose name a row of this run already carries", async () => {
      // The `-2` twin grown before the sync learned to hand a re-issued id to
      // the old row. Left active, the dead row takes the name group back.
      attractionRepo.find
        .mockResolvedValueOnce([seasonal()])
        .mockResolvedValueOnce([{ id: "row-twin", name: "Ghost Town NEW!" }]);

      await retireAbsent([liveExternalId], ["row-twin"]);

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      expect(retirementService.retire.mock.calls[0][0][0].attractionId).toBe(
        deadRow.id,
      );
    });

    it("keeps it when the row of this run carries another name", async () => {
      attractionRepo.find
        .mockResolvedValueOnce([seasonal()])
        .mockResolvedValueOnce([{ id: "row-other", name: "Fright Lights" }]);

      await retireAbsent([liveExternalId], ["row-other"]);

      expect(retirementService.retire).not.toHaveBeenCalled();
    });
  });

  /**
   * The wiki hands seasonal attractions a new entity id every season. The old
   * row has to take the new id, or the sync grows a `-2` twin with no history
   * beside it — 16 such pairs on 2026-10-03 (PAR-682).
   */
  describe("the wiki re-issues the entity under a new id", () => {
    const newId = "9f8e7d6c-5b4a-4321-8fed-cba987654321";

    beforeEach(() => {
      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: newId,
        name: deadRow.name,
        parkId,
      });
      // The insert path, taken when no row qualifies: a fresh `-2` twin.
      attractionRepo.save.mockResolvedValue({
        id: "row-twin",
        externalId: newId,
      });
    });

    const sync = (listed: string[] | undefined) =>
      (processor as any).syncAttraction({}, parkId, {
        claimed: new Set<string>(),
        listedExternalIds: listed ? new Set(listed) : undefined,
      });

    const oldRow = (over: Record<string, unknown> = {}) => ({
      ...deadRow,
      slug: "ghost-town-new",
      queueTimesEntityId: null,
      retiredReason: ABSENT_UPSTREAM_REASON,
      ...over,
    });

    it("moves the old row onto the new id and lifts its retirement", async () => {
      attractionRepo.find.mockResolvedValue([oldRow()]);

      await sync([newId]);

      expect(attractionRepo.save).not.toHaveBeenCalled();
      expect(attractionRepo.update).toHaveBeenCalledWith(
        deadRow.id,
        expect.objectContaining({ externalId: newId }),
      );
      expect(mappingRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({
          internalEntityId: deadRow.id,
          externalSource: "themeparks-wiki",
          externalEntityId: newId,
        }),
      );
      expect(retirementService.unretire).toHaveBeenCalledWith(deadRow.id);
    });

    it("also takes an active row whose id left the list", async () => {
      attractionRepo.find.mockResolvedValue([oldRow({ retiredReason: null })]);

      await sync([newId]);

      expect(attractionRepo.save).not.toHaveBeenCalled();
      expect(attractionRepo.update).toHaveBeenCalledWith(
        deadRow.id,
        expect.objectContaining({ externalId: newId }),
      );
    });

    it("does not take a row whose id is still listed — that is a rename next door", async () => {
      attractionRepo.find.mockResolvedValue([oldRow({ retiredReason: null })]);

      await sync([newId, deadRow.externalId]);

      expect(attractionRepo.update).not.toHaveBeenCalled();
      expect(attractionRepo.save).toHaveBeenCalledTimes(1);
    });

    it("does not take a ride a human retired as closed", async () => {
      attractionRepo.find.mockResolvedValue([
        oldRow({ retiredReason: "Closed permanently. Source: https://…" }),
      ]);

      await sync([newId]);

      expect(attractionRepo.update).not.toHaveBeenCalled();
      expect(attractionRepo.save).toHaveBeenCalledTimes(1);
    });

    it("does not take anything without the listed ids", async () => {
      attractionRepo.find.mockResolvedValue([oldRow({ retiredReason: null })]);

      await sync(undefined);

      expect(attractionRepo.update).not.toHaveBeenCalled();
      expect(attractionRepo.save).toHaveBeenCalledTimes(1);
    });
  });

  describe("what a visitor is told", () => {
    const retired = {
      retiredAt: now,
      retiredReason: ABSENT_UPSTREAM_REASON,
    };

    it("is not a permanent closure", () => {
      expect(retiredKindOf(retired)).toBe("reclassified");
      expect(isOnParkPage(retired, now)).toBe(false);
      expect(RECLASSIFIED_UPSTREAM_REASONS).toContain(ABSENT_UPSTREAM_REASON);
    });

    it("carries a source and no internal references", () => {
      expect(ABSENT_UPSTREAM_REASON).toContain("https://");
      expect(ABSENT_UPSTREAM_REASON).not.toMatch(/PAR-\d+/);
    });

    /** The copy is also the marker; see the same pin in the reclassification spec. */
    it("is pinned, because the copy is also the marker", () => {
      expect(ABSENT_UPSTREAM_REASON).toBe(
        "ThemeParks.wiki no longer lists this entity among its park's " +
          "attractions, so it is no longer tracked as a ride. The date is when " +
          "this was noticed, not when it left the list. " +
          "Source: https://api.themeparks.wiki/",
      );
    });
  });
});
