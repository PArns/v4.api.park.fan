import { In, IsNull } from "typeorm";
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
  const retirementService = { retire: jest.fn() };

  let processor: ChildrenMetadataProcessor;

  const parkId = "park-uss";
  const parkName = "Universal Studios Singapore";
  /** `Sesame Street`, an ATTRACTION until 2026-04-25 and a SHOW since. */
  const showExternalId = "8d1ea3fc-0a1b-4c2d-9e3f-4a5b6c7d8e9f";

  const staleRow = { id: "row-sesame-street", name: "Sesame Street" };

  beforeEach(() => {
    jest.clearAllMocks();
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      retirementService as any,
      { getRepository: () => ({}) } as any,
      { getRepository: () => ({}) } as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
    );
  });

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
      expect(requests[0].reason).toContain("ThemeParks.wiki");
      expect(Date.parse(requests[0].retiredAt)).not.toBeNaN();
    });

    it("leaves the row alone when the entity still arrives as an ATTRACTION", async () => {
      // The counter-check for the case above: same repository, same row, and
      // the only difference is that no show or restaurant carries this id.
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
            parkId,
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
        { id: "row-mels-drive-in", name: "Mel's Drive-In" },
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
        { id: "row-a", name: "A" },
        { id: "row-b", name: "B" },
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

    it("hands over the show and restaurant ids, and not the attraction ones", async () => {
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
        getEntityChildren: jest.fn().mockResolvedValue(childrenOf("uss")),
      };
      (processor as any).entityMappingsQueue = { add: jest.fn() };
      (processor as any).redis = { del: jest.fn() };

      await processor.handleFetchChildren({} as any);

      expect(spy).toHaveBeenCalledTimes(1);
      expect(spy).toHaveBeenCalledWith(parkId, parkName, ["show-1", "rest-1"]);
    });
  });
});
