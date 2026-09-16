import { In, IsNull, Not } from "typeorm";
import {
  RECLASSIFIED_UPSTREAM_REASON,
  RECLASSIFIED_UPSTREAM_REASONS,
} from "../../attractions/services/attraction-retirement.service";
import { ChildrenMetadataProcessor } from "./children-metadata.processor";

/**
 * ThemeParks.wiki changes an entity's `entityType` without changing its id.
 * The sync fans the children out by type and writes each into its own table,
 * and `externalId` is unique per table — so the row in the table the entity
 * left behind is never touched again. 34 such rows existed in production on
 * 2026-09-15: 17 at Universal Studios Singapore (reclassified 2026-04-25),
 * ten at Tokyo Disneyland and five at Tokyo DisneySea (2026-04-23), plus two
 * that must NOT be retired because Queue-Times still reports them as rides.
 *
 * Each case below is a pair: the state that produces the outcome, and the
 * same fixture with the deciding fact taken back. A test that only asserts an
 * absence is green whenever the branch is not reached at all.
 */
describe("ChildrenMetadataProcessor — upstream entityType changes", () => {
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

  const parkId = "park-uss";
  const parkName = "Universal Studios Singapore";
  /** `Sesame Street`, an ATTRACTION until 2026-04-25 and a SHOW since. */
  const showExternalId = "8d1ea3fc-0a1b-4c2d-9e3f-4a5b6c7d8e9f";

  const staleRow = {
    id: "row-sesame-street",
    name: "Sesame Street",
    parkId: "park-uss",
  };

  beforeEach(() => {
    jest.clearAllMocks();
    // No candidate is claimed by another source unless a test says so.
    mappingRepository.find.mockResolvedValue([]);
    mappingRepository.findOne.mockResolvedValue(null);
    childEntityRepo.find.mockResolvedValue([]);
    childEntityRepo.findOne.mockResolvedValue(null);
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      retirementService as any,
      // `find`/`update` are stubbed because the sibling direction
      // (`retireReclassifiedChildEntities`) runs in the same pass. Without
      // them it throws a TypeError into its own catch and the wiring cases
      // below would pass while that branch never ran.
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

  const otherShowExternalId = "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9";

  const retireReclassified = (externalIds: string[]) =>
    (processor as any).retireReclassifiedAttractions(
      parkId,
      parkName,
      externalIds,
    );

  describe("ATTRACTION → SHOW", () => {
    it("retires the abandoned attraction row", async () => {
      attractionRepo.find.mockResolvedValue([staleRow]);

      await retireReclassified([showExternalId]);

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      const [requests] = retirementService.retire.mock.calls[0];
      expect(requests).toHaveLength(1);
      expect(requests[0].attractionId).toBe("row-sesame-street");
      expect(requests[0].reason).toBe(RECLASSIFIED_UPSTREAM_REASON);
      expect(Date.parse(requests[0].retiredAt)).not.toBeNaN();
    });

    it("leaves the row alone when the entity still arrives as an ATTRACTION", async () => {
      // The counter-check for the case above. The park does have shows, so the
      // branch runs with a real id list — only this row's id is not in it, and
      // the query comes back without it.
      attractionRepo.find.mockResolvedValue([]);

      await retireReclassified([otherShowExternalId]);

      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            externalId: In([otherShowExternalId]),
          }),
        }),
      );
      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("does not query at all when the park publishes no shows or restaurants", async () => {
      attractionRepo.find.mockResolvedValue([staleRow]);

      await retireReclassified([]);

      expect(attractionRepo.find).not.toHaveBeenCalled();
      expect(retirementService.retire).not.toHaveBeenCalled();
    });
  });

  describe("a row with a second source", () => {
    it("asks only for rows that have no Queue-Times id and are not retired yet", async () => {
      attractionRepo.find.mockResolvedValue([]);

      await retireReclassified([showExternalId]);

      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: {
            externalId: In([showExternalId]),
            retiredAt: IsNull(),
            queueTimesEntityId: IsNull(),
          },
        }),
      );
    });

    it("retires nothing when the query comes back empty", async () => {
      // Disneyland Paris' `Mickey's PhilharMagic`: a SHOW to the wiki, a ride
      // with a wait time to Queue-Times, and still receiving real OPERATING
      // readings. The filter above is what keeps it, so the pair proves the
      // branch was entered and then found nothing rather than never running.
      attractionRepo.find.mockResolvedValue([]);

      await retireReclassified([showExternalId]);

      expect(attractionRepo.find).toHaveBeenCalledTimes(1);
      expect(retirementService.retire).not.toHaveBeenCalled();
    });
  });

  describe("restaurants count too", () => {
    it("retires an attraction row whose entity is now a RESTAURANT", async () => {
      const restaurantExternalId = "f1e2d3c4-b5a6-4978-8a9b-0c1d2e3f4a5b";
      attractionRepo.find.mockResolvedValue([
        { id: "row-mels-drive-in", name: "Mel's Drive-In", parkId },
      ]);

      await retireReclassified([restaurantExternalId]);

      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            externalId: In([restaurantExternalId]),
          }),
        }),
      );
      expect(retirementService.retire).toHaveBeenCalledTimes(1);
    });
  });

  describe("the ids it is given", () => {
    it("retires every reclassified row of the park in one call", async () => {
      const ids = ["show-a", "show-b", "restaurant-c"];
      attractionRepo.find.mockResolvedValue([
        { id: "row-a", name: "A", parkId },
        { id: "row-b", name: "B", parkId },
      ]);

      await retireReclassified(ids);

      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({ externalId: In(ids) }),
        }),
      );
      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      expect(retirementService.retire.mock.calls[0][0]).toHaveLength(2);
    });
  });

  /**
   * The check above is worthless if the sync never calls it, so this drives
   * the real handler once and reads the ids it was handed.
   */
  describe("wired into the children sync", () => {
    const childrenOf = (park: string) => ({
      children: [
        { id: "att-1", entityType: "ATTRACTION", name: "Battlestar Galactica" },
        { id: "show-1", entityType: "SHOW", name: "Sesame Street" },
        { id: "rest-1", entityType: "RESTAURANT", name: "Mel's Drive-In" },
        { id: `ignored-${park}`, entityType: "HOTEL", name: "Hard Rock Hotel" },
      ],
    });

    it("skips an id that also arrived as an ATTRACTION in the same response", async () => {
      // `/children` listing one entity under two types is the upstream fault
      // `dedupePollEntities` handles for live data. Without the exclusion the
      // row would flip between retired and not on every single run.
      const spy = jest
        .spyOn(processor as any, "retireReclassifiedAttractions")
        .mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncAttraction")
        .mockResolvedValue(undefined);
      jest.spyOn(processor as any, "syncShow").mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);

      (processor as any).parksService = {
        findAll: jest
          .fn()
          .mockResolvedValue([
            { id: parkId, name: parkName, wikiEntityId: "wiki-uss" },
          ]),
      };
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockResolvedValue({
          children: [
            { id: "both", entityType: "ATTRACTION", name: "Sesame Street" },
            { id: "both", entityType: "SHOW", name: "Sesame Street" },
            { id: "show-only", entityType: "SHOW", name: "Egyptian Legends" },
          ],
        }),
      };
      (processor as any).entityMappingsQueue = { add: jest.fn() };
      (processor as any).redis = { del: jest.fn() };

      await processor.handleFetchChildren({} as any);

      expect(spy).toHaveBeenCalledWith(parkId, parkName, ["show-only"]);
    });

    it("hands over the show and restaurant ids, and not the attraction ones", async () => {
      const spy = jest
        .spyOn(processor as any, "retireReclassifiedAttractions")
        .mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncAttraction")
        .mockResolvedValue(undefined);
      const showSpy = jest
        .spyOn(processor as any, "syncShow")
        .mockResolvedValue(undefined);
      const restaurantSpy = jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);

      (processor as any).parksService = {
        findAll: jest
          .fn()
          .mockResolvedValue([
            { id: parkId, name: parkName, wikiEntityId: "wiki-uss" },
          ]),
      };
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockResolvedValue(childrenOf("uss")),
      };
      (processor as any).entityMappingsQueue = { add: jest.fn() };
      (processor as any).redis = { del: jest.fn() };

      await processor.handleFetchChildren({} as any);

      expect(spy).toHaveBeenCalledTimes(1);
      expect(spy).toHaveBeenCalledWith(parkId, parkName, ["show-1", "rest-1"]);

      // The replacement row has to exist before the old one is retired — a
      // reader who only sees the assertion above could move the call up.
      expect(showSpy.mock.invocationCallOrder[0]).toBeLessThan(
        spy.mock.invocationCallOrder[0],
      );
      expect(restaurantSpy.mock.invocationCallOrder[0]).toBeLessThan(
        spy.mock.invocationCallOrder[0],
      );
    });

    it("keeps syncing the park when retiring throws", async () => {
      jest
        .spyOn(processor as any, "retireReclassifiedAttractions")
        .mockRejectedValue(new Error("deadlock detected"));
      jest
        .spyOn(processor as any, "syncAttraction")
        .mockResolvedValue(undefined);
      jest.spyOn(processor as any, "syncShow").mockResolvedValue(undefined);
      jest
        .spyOn(processor as any, "syncRestaurant")
        .mockResolvedValue(undefined);

      const queueAdd = jest.fn();
      const redisDel = jest.fn();
      (processor as any).parksService = {
        findAll: jest
          .fn()
          .mockResolvedValue([
            { id: parkId, name: parkName, wikiEntityId: "wiki-uss" },
          ]),
      };
      (processor as any).themeParksClient = {
        getEntityChildren: jest.fn().mockResolvedValue(childrenOf("uss")),
      };
      (processor as any).entityMappingsQueue = { add: queueAdd };
      (processor as any).redis = { del: redisDel };

      await processor.handleFetchChildren({} as any);

      // Both of these sit after the retirement call and are what an unguarded
      // throw would have cost this park.
      expect(queueAdd).toHaveBeenCalledTimes(1);
      expect(redisDel).toHaveBeenCalledTimes(1);
    });
  });

  /**
   * The way back. Without it a single malformed `/children` response would
   * strand a park's rides for good, since nothing else clears `retired_at`.
   */
  describe("the entity becomes an ATTRACTION again", () => {
    const syncBack = (row: Record<string, unknown>) => {
      themeParksMapper.mapAttraction.mockReturnValue({
        externalId: showExternalId,
        name: "Sesame Street",
        parkId,
      });
      attractionRepo.find.mockResolvedValue([
        { slug: "sesame-street", queueTimesEntityId: null, ...row },
      ]);
      return (processor as any).syncAttraction({}, parkId);
    };

    it("lifts a retirement this sync wrote", async () => {
      await syncBack({
        id: "row-sesame-street",
        externalId: showExternalId,
        name: "Sesame Street",
        retiredReason: RECLASSIFIED_UPSTREAM_REASON,
      });

      // Through the service, not a column write: lifting a retirement has the
      // same cache and sitemap consequences as setting one, and only
      // `unretire` carries the eviction and the revalidation with it.
      expect(retirementService.unretire).toHaveBeenCalledWith(
        "row-sesame-street",
      );
      const [, patch] = attractionRepo.update.mock.calls[0];
      expect(patch).not.toHaveProperty("retiredAt");

      // The decision reads `retiredReason` off the candidate row, so it has to
      // be selected. Without this the check is silently dead in production
      // while every mocked test above stays green.
      expect(attractionRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          select: expect.arrayContaining(["retiredReason"]),
        }),
      );
    });

    it("leaves a retirement a human entered alone", async () => {
      // Same row, same upstream answer, and the only difference is who wrote
      // the reason. Retiring by hand has to outlive the nightly sync.
      await syncBack({
        id: "row-sesame-street",
        externalId: showExternalId,
        name: "Sesame Street",
        retiredReason:
          "Demolished in January 2026. Source: https://example.org",
      });

      expect(retirementService.unretire).not.toHaveBeenCalled();
      expect(attractionRepo.update).toHaveBeenCalledTimes(1);
    });
  });

  /**
   * `queue_times_entity_id` is written by the entity mapping job for a
   * Queue-Times match only. A `wartezeiten-app` match leaves nothing but the
   * `external_entity_mapping` row, and `WaitTimesProcessor` resolves live data
   * through that mapping all the same — 39 attractions carried exactly that
   * combination in production on 2026-09-15.
   */
  describe("a row claimed by a source that is not the wiki", () => {
    it("is left alone even though it carries no Queue-Times id", async () => {
      attractionRepo.find.mockResolvedValue([staleRow]);
      mappingRepository.find.mockResolvedValue([
        { internalEntityId: "row-sesame-street" },
      ]);

      await retireReclassified([showExternalId]);

      expect(retirementService.retire).not.toHaveBeenCalled();
    });

    it("is retired when no such mapping exists", async () => {
      // The pair: same row, same query, and the only difference is whether a
      // foreign mapping came back.
      attractionRepo.find.mockResolvedValue([staleRow]);
      mappingRepository.find.mockResolvedValue([]);

      await retireReclassified([showExternalId]);

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
    });

    it("asks about every source except the wiki's own", async () => {
      attractionRepo.find.mockResolvedValue([staleRow]);

      await retireReclassified([showExternalId]);

      expect(mappingRepository.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: {
            internalEntityId: In(["row-sesame-street"]),
            internalEntityType: "attraction",
            externalSource: Not("themeparks-wiki"),
          },
        }),
      );
    });
  });

  /**
   * The lookup is global while `syncAttraction` is park-scoped, so a row whose
   * park moved upstream is retired automatically and cannot come back on its
   * own. The warning is the only thing that says so.
   */
  describe("a row that belongs to another park", () => {
    it("is retired, and named with its own park", async () => {
      const warn = jest
        .spyOn((processor as any).logger, "warn")
        .mockImplementation(() => undefined);
      attractionRepo.find.mockResolvedValue([
        { id: "row-moved", name: "Moved Ride", parkId: "park-other" },
      ]);

      await retireReclassified([showExternalId]);

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn.mock.calls[0][0]).toContain("park-other");
    });

    it("says nothing when every row belongs to the park being synced", async () => {
      const warn = jest
        .spyOn((processor as any).logger, "warn")
        .mockImplementation(() => undefined);
      attractionRepo.find.mockResolvedValue([staleRow]);

      await retireReclassified([showExternalId]);

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      expect(warn).not.toHaveBeenCalled();
    });
  });

  /**
   * The reason ends up on the public attraction detail endpoint
   * (`AttractionResponseDto.fromEntity` serves `retiredReason`), so it has to
   * read as a sentence to a visitor rather than as a note to a developer.
   */
  describe("the reason a visitor reads", () => {
    it("carries a source and no internal references", () => {
      expect(RECLASSIFIED_UPSTREAM_REASON).toContain("https://");
      expect(RECLASSIFIED_UPSTREAM_REASON).not.toMatch(/PAR-\d+/);
      expect(RECLASSIFIED_UPSTREAM_REASON).not.toContain("docs/");
      expect(RECLASSIFIED_UPSTREAM_REASON).not.toContain(".md");
    });

    /**
     * The copy IS the marker, so rewording it strands every row already
     * retired under the old text — the un-retire check stops recognising them
     * and the retire filter skips them because `retired_at` is set. This pin
     * makes that impossible to do by accident: changing the sentence fails
     * here, and the fix is to move the old value into
     * `RECLASSIFIED_UPSTREAM_REASONS` in the same commit.
     */
    it("is pinned, because the copy is also the marker", () => {
      expect(RECLASSIFIED_UPSTREAM_REASON).toBe(
        "ThemeParks.wiki lists this entity as a show or a restaurant rather " +
          "than an attraction, so it is no longer tracked as a ride. The date " +
          "is when this was noticed, not when the reclassification happened. " +
          "Source: https://api.themeparks.wiki/",
      );
      expect(RECLASSIFIED_UPSTREAM_REASONS).toContain(
        RECLASSIFIED_UPSTREAM_REASON,
      );
    });
  });

  /**
   * The protection runs one way, and that is the intended behaviour rather
   * than a gap: the wiki is the source for what an entity *is*.
   */
  describe("a human un-retirement does not survive", () => {
    it("retires the row again while the wiki still calls it a show", async () => {
      // `POST /admin/unretire-attraction/:id` clears both columns, so the row
      // looks exactly like one that was never retired.
      attractionRepo.find.mockResolvedValue([staleRow]);

      await retireReclassified([showExternalId]);

      expect(retirementService.retire).toHaveBeenCalledTimes(1);
      expect(retirementService.retire.mock.calls[0][0][0].reason).toBe(
        RECLASSIFIED_UPSTREAM_REASON,
      );
    });
  });
});
