import { In, IsNull } from "typeorm";
import {
  RECLASSIFIED_AS_ATTRACTION_REASON,
  RECLASSIFIED_AS_ATTRACTION_REASONS,
  ChildrenMetadataProcessor,
  isReclassifiedAsAttractionReason,
} from "./children-metadata.processor";

/**
 * The `SHOW`/`RESTAURANT` -> `ATTRACTION` half of the same upstream drift its
 * sibling spec covers in the other direction.
 *
 * Until `shows` and `restaurants` grew a `retired_at`, this direction could
 * not be built at all: the sync created the `attractions` row and left the old
 * one standing, so the park payload carried both. Nothing in production has
 * moved this way yet — all 34 collisions measured on 2026-09-15 run from
 * `ATTRACTION` to `SHOW` — which is exactly why every case here is a pair: the
 * state that produces the outcome, and the same fixture with the deciding fact
 * taken back. A test that only asserts an absence is green whenever the branch
 * was never reached.
 */
describe("ChildrenMetadataProcessor — entities reclassified as attractions", () => {
  const showRepo = { find: jest.fn(), update: jest.fn(), findOne: jest.fn() };
  const restaurantRepo = {
    find: jest.fn(),
    update: jest.fn(),
    findOne: jest.fn(),
  };
  const mappingRepository = { find: jest.fn(), findOne: jest.fn() };
  const redis = { del: jest.fn(), keys: jest.fn(), unlink: jest.fn() };
  const revalidationService = { revalidateTags: jest.fn() };
  const themeParksMapper = { mapShow: jest.fn(), mapRestaurant: jest.fn() };

  let processor: ChildrenMetadataProcessor;

  const parkName = "Universal Studios Singapore";
  /** An entity the wiki listed as a SHOW and now lists as an ATTRACTION. */
  const rideExternalId = "3f2a91c4-7b18-4d5e-8a6f-0c1d2e3f4a5b";
  const otherExternalId = "9e8d7c6b-5a49-4382-b1c0-d2e3f4a5b6c7";

  const staleShow = { id: "row-show", name: "Waterworld", parkId: "park-uss" };
  const staleRestaurant = {
    id: "row-restaurant",
    name: "Mel's Drive-In",
    parkId: "park-uss",
  };

  beforeEach(() => {
    jest.clearAllMocks();
    // Nothing is claimed by a second source unless a case says so. This is
    // also the production state: `external_entity_mapping` held zero rows for
    // 'show' or 'restaurant' on 2026-09-16.
    mappingRepository.find.mockResolvedValue([]);
    showRepo.find.mockResolvedValue([]);
    restaurantRepo.find.mockResolvedValue([]);
    redis.keys.mockResolvedValue([]);
    revalidationService.revalidateTags.mockResolvedValue(undefined);

    processor = new ChildrenMetadataProcessor(
      { getRepository: () => ({}) } as any,
      {} as any,
      { getRepository: () => showRepo } as any,
      { getRepository: () => restaurantRepo } as any,
      {} as any,
      {} as any,
      themeParksMapper as any,
      {} as any,
      mappingRepository as any,
      {} as any,
      redis as any,
      revalidationService as any,
    );
  });

  const retireReclassified = (externalIds: string[]) =>
    (processor as any).retireReclassifiedChildEntities(parkName, externalIds);

  describe("SHOW → ATTRACTION", () => {
    it("retires the abandoned show row", async () => {
      showRepo.find.mockResolvedValue([staleShow]);

      await retireReclassified([rideExternalId]);

      expect(showRepo.update).toHaveBeenCalledTimes(1);
      const [criteria, patch] = showRepo.update.mock.calls[0];
      expect(criteria).toEqual({ id: In(["row-show"]) });
      expect(patch.retiredReason).toBe(RECLASSIFIED_AS_ATTRACTION_REASON);
      expect(patch.retiredAt).toBeInstanceOf(Date);
      expect(Number.isNaN(patch.retiredAt.getTime())).toBe(false);
    });

    it("leaves the row alone when the entity still arrives as a SHOW", async () => {
      // The counter-check for the case above. The park does have attractions,
      // so the branch runs with a real id list — this row's id is simply not
      // in it, and the query comes back empty.
      showRepo.find.mockResolvedValue([]);

      await retireReclassified([otherExternalId]);

      expect(showRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({
            externalId: In([otherExternalId]),
          }),
        }),
      );
      expect(showRepo.update).not.toHaveBeenCalled();
    });

    it("asks only for rows that are not retired yet", async () => {
      // Without this the sync would rewrite `retired_at` on every nightly run
      // and evict the park's caches each time.
      showRepo.find.mockResolvedValue([staleShow]);

      await retireReclassified([rideExternalId]);

      expect(showRepo.find).toHaveBeenCalledWith(
        expect.objectContaining({
          where: expect.objectContaining({ retiredAt: IsNull() }),
        }),
      );
    });

    it("does not query at all when the park publishes no attractions", async () => {
      showRepo.find.mockResolvedValue([staleShow]);

      await retireReclassified([]);

      expect(showRepo.find).not.toHaveBeenCalled();
      expect(showRepo.update).not.toHaveBeenCalled();
    });
  });

  describe("RESTAURANT → ATTRACTION", () => {
    it("retires the abandoned restaurant row", async () => {
      restaurantRepo.find.mockResolvedValue([staleRestaurant]);

      await retireReclassified([rideExternalId]);

      expect(restaurantRepo.update).toHaveBeenCalledTimes(1);
      const [criteria, patch] = restaurantRepo.update.mock.calls[0];
      expect(criteria).toEqual({ id: In(["row-restaurant"]) });
      expect(patch.retiredReason).toBe(RECLASSIFIED_AS_ATTRACTION_REASON);
    });

    it("retires both tables in one pass when both hold a row", async () => {
      showRepo.find.mockResolvedValue([staleShow]);
      restaurantRepo.find.mockResolvedValue([staleRestaurant]);

      await retireReclassified([rideExternalId]);

      expect(showRepo.update).toHaveBeenCalledTimes(1);
      expect(restaurantRepo.update).toHaveBeenCalledTimes(1);
    });
  });

  describe("the second-source guard", () => {
    it("leaves a show a foreign source has claimed", async () => {
      showRepo.find.mockResolvedValue([staleShow]);
      mappingRepository.find.mockImplementation((options: any) =>
        Promise.resolve(
          options.where.internalEntityType === "show"
            ? [{ internalEntityId: "row-show" }]
            : [],
        ),
      );

      await retireReclassified([rideExternalId]);

      expect(showRepo.update).not.toHaveBeenCalled();
    });

    it("retires the same row once that claim is gone", async () => {
      // The counter-check: same fixture, only the mapping taken back. Without
      // it the case above would pass even if the branch never ran.
      showRepo.find.mockResolvedValue([staleShow]);
      mappingRepository.find.mockResolvedValue([]);

      await retireReclassified([rideExternalId]);

      expect(showRepo.update).toHaveBeenCalledTimes(1);
    });

    it("asks about the row's own entity type, not the attraction one", async () => {
      // A show's id in the `attraction` partition of the mapping table belongs
      // to a different row. Querying the wrong type would read another
      // entity's claim — and, since nothing writes 'show' mappings today,
      // would be the difference between an inert guard and a wrong one.
      showRepo.find.mockResolvedValue([staleShow]);
      restaurantRepo.find.mockResolvedValue([staleRestaurant]);

      await retireReclassified([rideExternalId]);

      const types = mappingRepository.find.mock.calls.map(
        ([options]: any[]) => options.where.internalEntityType,
      );
      expect(types).toEqual(expect.arrayContaining(["show", "restaurant"]));
      expect(types).not.toContain("attraction");
    });
  });

  describe("cache eviction", () => {
    it("evicts and revalidates once a row was retired", async () => {
      // A retirement is a plain column write: without this the park payload
      // keeps serving the row for up to 24h plus the CDN window.
      showRepo.find.mockResolvedValue([staleShow]);

      await retireReclassified([rideExternalId]);

      expect(redis.del).toHaveBeenCalled();
      expect(revalidationService.revalidateTags).toHaveBeenCalledWith([
        "geo",
        "parks",
        "attractions",
      ]);
    });

    it("does not evict when nothing was retired", async () => {
      showRepo.find.mockResolvedValue([]);
      restaurantRepo.find.mockResolvedValue([]);

      await retireReclassified([rideExternalId]);

      expect(redis.del).not.toHaveBeenCalled();
      expect(revalidationService.revalidateTags).not.toHaveBeenCalled();
    });
  });

  describe("the way back", () => {
    it("lifts a retirement this sync wrote", async () => {
      themeParksMapper.mapShow.mockReturnValue({
        externalId: rideExternalId,
        name: "Waterworld",
      });
      showRepo.findOne.mockResolvedValue({
        ...staleShow,
        retiredAt: new Date(),
        retiredReason: RECLASSIFIED_AS_ATTRACTION_REASON,
      });

      await (processor as any).syncShow(
        { id: rideExternalId, entityType: "SHOW" },
        "park-uss",
      );

      const clearing = showRepo.update.mock.calls.find(
        ([, patch]: any[]) => patch.retiredAt === null,
      );
      expect(clearing).toBeDefined();
      expect(clearing[1]).toEqual({ retiredAt: null, retiredReason: null });
    });

    it("leaves a retirement a human entered", async () => {
      // Same fixture, only the reason changed. The sync wins arguments about
      // what an entity IS; it does not overrule a curator about whether a row
      // should exist.
      themeParksMapper.mapShow.mockReturnValue({
        externalId: rideExternalId,
        name: "Waterworld",
      });
      showRepo.findOne.mockResolvedValue({
        ...staleShow,
        retiredAt: new Date(),
        retiredReason: "Demolished in 2025, see https://example.com/news",
      });

      await (processor as any).syncShow(
        { id: rideExternalId, entityType: "SHOW" },
        "park-uss",
      );

      const clearing = showRepo.update.mock.calls.find(
        ([, patch]: any[]) => patch.retiredAt === null,
      );
      expect(clearing).toBeUndefined();
    });
  });

  describe("the marker string", () => {
    it("reads as a user-facing sentence with its source", () => {
      // It is served on the public detail endpoint, so it carries no issue
      // numbers, file paths or notes to the next developer.
      expect(RECLASSIFIED_AS_ATTRACTION_REASON).toContain("https://");
      expect(RECLASSIFIED_AS_ATTRACTION_REASON).not.toMatch(/PAR-\d+/);
      expect(RECLASSIFIED_AS_ATTRACTION_REASON).not.toContain("docs/");
      expect(RECLASSIFIED_AS_ATTRACTION_REASON).not.toContain(".md");
    });

    it("is listed among the wordings the un-retire check accepts", () => {
      // Changing the constant without adding the old value here strands every
      // row already retired under it.
      expect(RECLASSIFIED_AS_ATTRACTION_REASONS).toContain(
        RECLASSIFIED_AS_ATTRACTION_REASON,
      );
      expect(
        isReclassifiedAsAttractionReason(RECLASSIFIED_AS_ATTRACTION_REASON),
      ).toBe(true);
    });

    it("does not match a retirement written by the other direction", () => {
      // The two markers live in the same sync and must not be confused: a row
      // retired as "now a show" is an attraction, and lifting it here would
      // resurrect the wrong row.
      expect(
        isReclassifiedAsAttractionReason(
          "ThemeParks.wiki lists this entity as a show or a restaurant rather " +
            "than an attraction, so it is no longer tracked as a ride.",
        ),
      ).toBe(false);
      expect(isReclassifiedAsAttractionReason(null)).toBe(false);
      expect(isReclassifiedAsAttractionReason(undefined)).toBe(false);
    });
  });
});
