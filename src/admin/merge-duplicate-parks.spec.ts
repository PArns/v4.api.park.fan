import { AdminController } from "./admin.controller";
import { DuplicatePair } from "../parks/services/park-validator.service";
import { Park } from "../parks/entities/park.entity";

/**
 * `POST /v1/admin/merge-duplicate-parks` with `autoDetect: true` used to merge
 * every pair the detector returned, in one call, inside a transaction with no
 * undo — so the only way to ask what it would do was to let it do it.
 *
 * These cases pin the two things that now stand in the way, and both are about
 * the endpoint rather than the detector: nothing is written without
 * `dryRun: false`, and a pair marked for review is never merged even then. The
 * `sharedPoint` pair is the one the issue asks about by name, because it is the
 * first branch that admits a pair whose names score under 0.85.
 *
 * The controller is built by hand rather than through a testing module: its
 * constructor takes fourteen queues and a Redis client this path never touches,
 * and naming the three services it does use keeps the case readable and stops a
 * new constructor argument from silently shifting the wiring.
 */
describe("AdminController.mergeDuplicateParks", () => {
  const park = (p: Partial<Park>): Park => ({ ...p }) as Park;

  /** The Wet'n'Wild rows, detected by `sharedPoint`: disjoint sources, so no
   * shared id, so never `safe`. */
  const wetnwildWiki = park({
    id: "ww-wiki",
    name: "Wet'n'Wild",
    city: "Oxenford",
    wikiEntityId: "ww-wiki-id",
    queueTimesEntityId: null,
    wartezeitenEntityId: null,
  });
  const wetnwildQueueTimes = park({
    id: "ww-qt",
    name: "Wet 'n' Wild Gold Coast",
    city: "Oxenford",
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-146",
    wartezeitenEntityId: null,
  });

  /** Universal Studios Hollywood, one Queue-Times id on both rows. Safe. */
  const ushLosAngeles = park({
    id: "ush-la",
    name: "Universal Studios Hollywood",
    city: "Los Angeles",
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-66",
    wartezeitenEntityId: null,
  });
  const ushBullCreek = park({
    id: "ush-bc",
    name: "Universal Studios Hollywood",
    city: "Bull Creek",
    wikiEntityId: "ush-wiki-id",
    queueTimesEntityId: "qt-park-66",
    wartezeitenEntityId: null,
  });

  const pairOf = (
    p1: Park,
    p2: Park,
    safe: boolean,
    reason: string,
  ): DuplicatePair => ({
    park1: { id: p1.id, name: p1.name, city: p1.city },
    park2: { id: p2.id, name: p2.name, city: p2.city },
    score: safe ? 1 : 0.6923,
    reason,
    sharedEntityIds: {
      wiki: false,
      queueTimes: safe,
      wartezeiten: false,
    },
    safe,
    reviewReason: safe ? null : "no upstream source holds one id for both rows",
  });

  const sharedPointPair = pairOf(
    wetnwildWiki,
    wetnwildQueueTimes,
    false,
    "same coordinates, one park per source",
  );
  const sharedIdPair = pairOf(
    ushLosAngeles,
    ushBullCreek,
    true,
    "shared queue-times ID",
  );

  const allParks = [
    wetnwildWiki,
    wetnwildQueueTimes,
    ushLosAngeles,
    ushBullCreek,
  ];

  let repairDuplicates: jest.Mock;
  let mergeParks: jest.Mock;
  let findDuplicates: jest.Mock;
  let controller: AdminController;

  const build = (duplicates: DuplicatePair[]) => {
    findDuplicates = jest.fn().mockResolvedValue(duplicates);
    repairDuplicates = jest.fn().mockResolvedValue({
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: duplicates.filter((d) => d.safe).length,
      errors: [],
    });
    mergeParks = jest.fn();

    const parkRepository = {
      find: jest.fn().mockResolvedValue(allParks),
      findOne: jest
        .fn()
        .mockImplementation(({ where }: { where: { id: string } }) =>
          Promise.resolve(allParks.find((p) => p.id === where.id) ?? null),
        ),
    };

    controller = Object.create(AdminController.prototype) as AdminController;
    Object.assign(controller, {
      parkValidatorService: {
        findDuplicates,
        getParkRepository: () => parkRepository,
      },
      parkRepairService: { repairDuplicates },
      parkMergeService: { mergeParks },
    });
    return controller;
  };

  it("writes nothing when autoDetect is sent without dryRun:false", async () => {
    const result = await build([sharedIdPair]).mergeDuplicateParks({
      autoDetect: true,
    });

    expect(repairDuplicates).not.toHaveBeenCalled();
    expect(result.dryRun).toBe(true);
    expect(result.merged).toBe(0);
    expect(result.planned.map((p) => p.loserId)).toEqual([ushLosAngeles.id]);
  });

  it("does not merge a sharedPoint pair, even with dryRun:false", async () => {
    const result = await build([sharedPointPair]).mergeDuplicateParks({
      autoDetect: true,
      dryRun: false,
    });

    // The gate is what refuses it, so the merge is never asked for at all —
    // asserting only on `merged` would pass on a merge that failed for some
    // other reason.
    expect(repairDuplicates).toHaveBeenCalledTimes(1);
    expect(repairDuplicates).toHaveBeenCalledWith([]);
    expect(result.planned).toEqual([]);
    expect(result.skipped.map((p) => p.loserId)).toEqual([
      wetnwildQueueTimes.id,
    ]);
    expect(result.merged).toBe(0);
  });

  it("merges the safe pair beside it and leaves the other one reported", async () => {
    // Both in one call, because the failure worth guarding against is a gate
    // that refuses everything: a run that merges nothing proves nothing about
    // a run that merges the wrong thing.
    const result = await build([
      sharedPointPair,
      sharedIdPair,
    ]).mergeDuplicateParks({ autoDetect: true, dryRun: false });

    expect(repairDuplicates).toHaveBeenCalledWith([
      { winnerId: ushBullCreek.id, loserId: ushLosAngeles.id },
    ]);
    expect(result.merged).toBe(1);
    expect(result.results[0].loserName).toBe(ushLosAngeles.name);
    expect(result.skipped.map((p) => p.loserId)).toEqual([
      wetnwildQueueTimes.id,
    ]);
  });

  it("keeps merging a manual pair by default, and previews it on dryRun:true", async () => {
    // The admin's merge button sends exactly this body. A default dry run here
    // would be a button that stops working and says it succeeded.
    const merging = build([]);
    mergeParks.mockResolvedValue({
      success: true,
      winnerId: ushBullCreek.id,
      winnerName: ushBullCreek.name,
      loserId: ushLosAngeles.id,
      loserName: ushLosAngeles.name,
      migratedAttractions: 0,
      migratedShows: 0,
      migratedRestaurants: 0,
      migratedScheduleEntries: 0,
      migratedMappings: 0,
      errors: [],
    });

    const merged = await merging.mergeDuplicateParks({
      park1Id: ushLosAngeles.id,
      park2Id: ushBullCreek.id,
    });

    expect(mergeParks).toHaveBeenCalledWith(ushBullCreek.id, ushLosAngeles.id);
    expect(merged.merged).toBe(1);
    expect(merged.dryRun).toBe(false);

    // Same controller and the same mock, so the call count below is the one
    // from the merge above and nothing else.
    const preview = await merging.mergeDuplicateParks({
      park1Id: ushLosAngeles.id,
      park2Id: ushBullCreek.id,
      dryRun: true,
    });

    expect(mergeParks).toHaveBeenCalledTimes(1);
    expect(preview.dryRun).toBe(true);
    expect(preview.merged).toBe(0);
    expect(preview.planned[0]).toMatchObject({
      winnerId: ushBullCreek.id,
      loserId: ushLosAngeles.id,
    });
  });

  it("counts the two sets apart on the read route", async () => {
    const result = await build([
      sharedPointPair,
      sharedIdPair,
    ]).listDuplicateParks();

    expect(result.total).toBe(2);
    expect(result.safe).toBe(1);
    expect(result.needsReview).toBe(1);
    expect(result.pairs.find((p) => !p.safe)?.reviewReason).toContain(
      "no upstream source",
    );
  });
});
