import { IsNull, LessThan } from "typeorm";
import {
  ABSENT_UPSTREAM_REASON,
  RECLASSIFIED_UPSTREAM_REASONS,
  isOnParkPage,
  retiredKindOf,
} from "../../attractions/services/attraction-retirement.service";
import { SYNTHETIC_SOURCES } from "../../common/utils/outage-rows.sql";
import {
  ABSENT_UPSTREAM_RETIRE_DAYS,
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
    manager,
  };
  const themeParksMapper = { mapAttraction: jest.fn() };

  let processor: ChildrenMetadataProcessor;

  const parkId = "park-sfot";
  const parkName = "Six Flags Over Texas";
  const now = new Date("2026-10-02T03:30:00Z");

  /** `Ghost Town NEW!`, last listed on 2026-06-03. */
  const deadRow = {
    id: "row-ghost-town",
    name: "Ghost Town NEW!",
    externalId: "0b7d7c55-1a2b-4c3d-8e9f-001122334455",
  };
  const liveExternalId = "f2d2b7fd-4618-4a73-98fd-efffd99840de";

  beforeEach(() => {
    jest.clearAllMocks();
    attractionRepo.find.mockResolvedValue([deadRow]);
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

    it("asks only for this park's rows untouched for 60 days, without a Queue-Times id", async () => {
      // The window is enforced by the query, so the query is what is pinned:
      // a row the sync wrote 59 days ago never reaches the filter below it.
      await retireAbsent();

      const cutoff = new Date(
        now.getTime() - ABSENT_UPSTREAM_RETIRE_DAYS * 86_400_000,
      );
      expect(ABSENT_UPSTREAM_RETIRE_DAYS).toBe(60);
      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: {
            parkId,
            retiredAt: IsNull(),
            queueTimesEntityId: IsNull(),
            updatedAt: LessThan(cutoff),
          },
        }),
      );
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
