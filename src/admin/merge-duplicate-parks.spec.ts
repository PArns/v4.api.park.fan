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
    // The default stands for a run in which every pair handed over merged, so
    // it answers with one `merged: true` verdict per pair — the shape the
    // service really returns. A case that needs a failure overrides it.
    repairDuplicates = jest
      .fn()
      .mockImplementation(
        (pairs: Array<{ winnerId: string; loserId: string }>) =>
          Promise.resolve({
            fixedQtMismatches: 0,
            fixedWzMismatches: 0,
            addedQtIds: 0,
            addedWzIds: 0,
            mergedDuplicates: pairs.length,
            errors: [],
            pairs: pairs.map((pair) => ({
              ...pair,
              merged: true,
              error: null,
            })),
          }),
      );
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

  it("reads a form-encoded dryRun, where every value is a string", async () => {
    // This body takes no DTO, so nothing coerces it, and Nest parses
    // `application/x-www-form-urlencoded` by default — `curl -d dryRun=true`
    // arrives as "true". Under a strict comparison that is not `true`, and the
    // one request that asked for a dry run would have deleted a park.
    const controller = build([]);
    mergeParks.mockResolvedValue({ success: true });

    const preview = await controller.mergeDuplicateParks({
      park1Id: ushLosAngeles.id,
      park2Id: ushBullCreek.id,
      dryRun: "true" as unknown as boolean,
    });

    expect(mergeParks).not.toHaveBeenCalled();
    expect(preview.dryRun).toBe(true);
  });

  it("reads a form-encoded autoDetect and a form-encoded dryRun:false", async () => {
    // The other direction of the same fix, and the one that writes. Before the
    // parse, `autoDetect: "false"` was truthy and merged everything; a
    // `readBodyFlag` narrowed back to booleans would drop this call into the
    // usage message instead of the branch.
    const result = await build([sharedIdPair]).mergeDuplicateParks({
      autoDetect: "true" as unknown as boolean,
      dryRun: "false" as unknown as boolean,
    });

    expect(result.dryRun).toBe(false);
    expect(repairDuplicates).toHaveBeenCalledWith([
      { winnerId: ushBullCreek.id, loserId: ushLosAngeles.id },
    ]);
    expect(result.merged).toBe(1);
  });

  it("does not take a string autoDetect as a merge instruction", async () => {
    const result = await build([sharedIdPair]).mergeDuplicateParks({
      autoDetect: "false" as unknown as boolean,
    });

    expect(repairDuplicates).not.toHaveBeenCalled();
    expect(result.message).toContain("Either autoDetect=true");
  });

  it("refuses a flag it cannot read rather than picking a side", async () => {
    const controller = build([sharedIdPair]);

    await expect(
      controller.mergeDuplicateParks({
        autoDetect: true,
        dryRun: "yes" as unknown as boolean,
      }),
    ).rejects.toThrow("dryRun must be a boolean");

    await expect(
      controller.mergeDuplicateParks({
        autoDetect: 1 as unknown as boolean,
      }),
    ).rejects.toThrow("autoDetect must be a boolean");

    expect(repairDuplicates).not.toHaveBeenCalled();
  });

  it("reads an explicit null as the absent key it stands for", async () => {
    // The documented exception to the 400 above, pinned so that narrowing the
    // condition to `raw !== undefined` cannot turn a JSON null into a refusal
    // without anything going red.
    const result = await build([sharedIdPair]).mergeDuplicateParks({
      autoDetect: true,
      dryRun: null as unknown as boolean,
    });

    expect(result.dryRun).toBe(true);
    expect(repairDuplicates).not.toHaveBeenCalled();
  });

  it("refuses a manual pair of one park in the dry run too", async () => {
    // `mergeParks` rejects one id on both sides. A preview that answers
    // "would be merged" over the same request promises what the write refuses.
    const controller = build([]);

    const result = await controller.mergeDuplicateParks({
      park1Id: ushLosAngeles.id,
      park2Id: ushLosAngeles.id,
      dryRun: true,
    });

    expect(result.planned).toEqual([]);
    expect(result.errors[0].error).toContain("into itself");
    expect(mergeParks).not.toHaveBeenCalled();
  });

  it("reports an empty catalogue as a dry run rather than as a merge", async () => {
    const result = await build([]).mergeDuplicateParks({ autoDetect: true });

    expect(result.message).toBe("No duplicates found");
    expect(result.dryRun).toBe(true);
    expect(result.merged).toBe(0);
    expect(repairDuplicates).not.toHaveBeenCalled();
  });

  it("says so when a skipped pair lost one of its rows to the merge beside it", async () => {
    // Three rows for one park: A–B is safe and merges, B–C still needs review,
    // and B is gone by the time anybody reads the list.
    const third = park({
      id: "ush-third",
      name: "Universal Studios Hollywood Backlot",
      city: "Los Angeles",
      wikiEntityId: null,
      queueTimesEntityId: null,
      wartezeitenEntityId: "universalstudioshollywood",
    });
    const overlapping = pairOf(ushLosAngeles, third, false, "same city");
    const controller = build([sharedIdPair, overlapping]);
    (
      controller as unknown as {
        parkValidatorService: { getParkRepository: () => { find: jest.Mock } };
      }
    ).parkValidatorService
      .getParkRepository()
      .find.mockResolvedValue([...allParks, third]);

    const result = await controller.mergeDuplicateParks({
      autoDetect: true,
      dryRun: false,
    });

    expect(result.results.map((r) => r.loserId)).toEqual([ushLosAngeles.id]);
    expect(result.skipped[0].reviewReason).toContain(
      "a merge in this run was attempted on one of these rows",
    );
  });

  it("keeps the pair that merged when the row it deleted fails the next pair", async () => {
    // Three rows for one park, all safe: the detector pairs A–B and A–C, and
    // `determineMergeWinner` makes A the loser of both, because the other two
    // carry a wiki id and A does not. The first merge deletes A, the second
    // fails on it, and that failure is reported as `{ parkId: A }` — the very
    // id the successful pair also holds as its loser. Searching the error list
    // by id therefore disowns the merge that really happened.
    const ushUniversalCity = park({
      id: "ush-uc",
      name: "Universal Studios Hollywood",
      city: "Universal City",
      wikiEntityId: "ush-uc-wiki-id",
      queueTimesEntityId: "qt-park-66",
      wartezeitenEntityId: null,
    });
    const secondSharedIdPair = pairOf(
      ushLosAngeles,
      ushUniversalCity,
      true,
      "shared queue-times ID",
    );
    const controller = build([sharedIdPair, secondSharedIdPair]);
    (
      controller as unknown as {
        parkValidatorService: { getParkRepository: () => { find: jest.Mock } };
      }
    ).parkValidatorService
      .getParkRepository()
      .find.mockResolvedValue([...allParks, ushUniversalCity]);

    repairDuplicates.mockResolvedValue({
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 1,
      errors: [
        {
          parkId: ushLosAngeles.id,
          error: `Park ${ushLosAngeles.id} not found`,
        },
      ],
      pairs: [
        {
          winnerId: ushBullCreek.id,
          loserId: ushLosAngeles.id,
          merged: true,
          error: null,
        },
        {
          winnerId: ushUniversalCity.id,
          loserId: ushLosAngeles.id,
          merged: false,
          error: `Park ${ushLosAngeles.id} not found`,
        },
      ],
    });

    const result = await controller.mergeDuplicateParks({
      autoDetect: true,
      dryRun: false,
    });

    // Both pairs were really attempted, and the second really failed — without
    // that the case would be green over a run in which nothing collided.
    expect(repairDuplicates).toHaveBeenCalledWith([
      { winnerId: ushBullCreek.id, loserId: ushLosAngeles.id },
      { winnerId: ushUniversalCity.id, loserId: ushLosAngeles.id },
    ]);
    expect(result.errors).toEqual([
      {
        parkId: ushLosAngeles.id,
        error: `Park ${ushLosAngeles.id} not found`,
      },
    ]);

    // The winner is what tells the two pairs apart here; both share a loser.
    expect(result.results.map((r) => r.winnerId)).toEqual([ushBullCreek.id]);
    expect(result.merged).toBe(1);
  });

  it("reports nothing rather than the wrong park when the verdicts do not line up", async () => {
    // The position carries the names, so a verdict list that does not match the
    // plan would print a real deletion under another park's name. This endpoint
    // deletes parks; saying nothing is the safer half of that choice.
    const controller = build([sharedIdPair]);

    repairDuplicates.mockResolvedValue({
      fixedQtMismatches: 0,
      fixedWzMismatches: 0,
      addedQtIds: 0,
      addedWzIds: 0,
      mergedDuplicates: 1,
      errors: [],
      pairs: [
        {
          winnerId: "some-other-winner",
          loserId: "some-other-loser",
          merged: true,
          error: null,
        },
      ],
    });

    const result = await controller.mergeDuplicateParks({
      autoDetect: true,
      dryRun: false,
    });

    expect(result.results).toEqual([]);
    expect(result.merged).toBe(0);
    expect(result.errors[0].error).toContain("did not line up");
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
