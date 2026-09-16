import { ParkRepairService } from "./park-repair.service";

/**
 * `repairDuplicates` is the producer of the one promise its caller depends on:
 * `pairs` holds exactly one verdict per pair handed in, at that pair's own
 * index, whether the merge worked or not.
 *
 * `admin.controller.ts` reads that list by position, because the `errors` list
 * beside it is keyed by park and cannot tell two pairs apart when they share a
 * loser — three rows for one park give the detector (1,2), (1,3) and (2,3), the
 * first merge deletes the shared row, and the second fails on it carrying the
 * id the successful pair also holds (PAR-268).
 *
 * These cases sit on the producer, because the consumer's own case can only
 * describe the promise through a hand-written mock: a missing push in either
 * branch here would shift every later index and go unnoticed there.
 *
 * The service is built by hand rather than through a testing module — the merge
 * is the only collaborator this method touches, and naming it keeps a new
 * constructor argument from silently shifting the wiring.
 */
describe("ParkRepairService.repairDuplicates", () => {
  const build = (mergeParks: jest.Mock) => {
    const service = Object.create(
      ParkRepairService.prototype,
    ) as ParkRepairService;
    Object.assign(service, {
      logger: { log: jest.fn(), error: jest.fn(), warn: jest.fn() },
      parkMergeService: { mergeParks },
    });
    return service;
  };

  const pairs = [
    { winnerId: "win-1", loserId: "lose-shared" },
    { winnerId: "win-2", loserId: "lose-shared" },
    { winnerId: "win-3", loserId: "lose-3" },
  ];

  it("answers with one verdict per pair, in the order the pairs came in", async () => {
    // The trio PAR-268 is about, plus a third outcome: the first pair merges,
    // the second throws on the row the first one deleted, the third is refused
    // by the merge service rather than throwing. All three loser ids are of no
    // use in telling the first two apart — `lose-shared` is both of them.
    const mergeParks = jest
      .fn()
      .mockResolvedValueOnce({
        success: true,
        winnerName: "Winner One",
        loserName: "Loser",
        errors: [],
      })
      .mockRejectedValueOnce(new Error("Park lose-shared not found"))
      .mockResolvedValueOnce({ success: false, errors: ["still has rides"] });

    const result = await build(mergeParks).repairDuplicates(pairs);

    expect(result.pairs).toEqual([
      {
        winnerId: "win-1",
        loserId: "lose-shared",
        merged: true,
        error: null,
      },
      {
        winnerId: "win-2",
        loserId: "lose-shared",
        merged: false,
        error: "Park lose-shared not found",
      },
      {
        winnerId: "win-3",
        loserId: "lose-3",
        merged: false,
        error: "Merge failed: still has rides",
      },
    ]);

    // The two lists answer different questions and both stay as they were:
    // `mergedDuplicates` counts, `errors` says which row broke.
    expect(result.mergedDuplicates).toBe(1);
    expect(result.errors.map((e) => e.parkId)).toEqual([
      "lose-shared",
      "lose-3",
    ]);
  });

  it("returns an empty verdict list for an empty run rather than nothing", async () => {
    // The controller indexes into this list, so "no pairs" has to be a list of
    // length 0 and not an absent field.
    const mergeParks = jest.fn();

    const result = await build(mergeParks).repairDuplicates([]);

    expect(result.pairs).toEqual([]);
    expect(result.mergedDuplicates).toBe(0);
    expect(mergeParks).not.toHaveBeenCalled();
  });
});
