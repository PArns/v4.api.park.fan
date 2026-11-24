import { BadRequestException, Injectable } from "@nestjs/common";
import { In } from "typeorm";
import {
  ParkValidatorService,
  type DuplicatePairVerdict,
} from "../parks/services/park-validator.service";
import { ParkRepairService } from "../parks/services/park-repair.service";
import { ParkMergeService } from "../parks/services/park-merge.service";
import { determineMergeWinner } from "../parks/utils/park-merge.util";
import { readBodyFlag } from "./read-body-flag";

/**
 * One pair of `POST merge-duplicate-parks`, on either side of the gate.
 *
 * In `planned` it is a merge that ran or would run; in `skipped` it is a pair
 * the detector found and `autoDetect` refuses to act on, with `reviewReason`
 * saying what a human has to settle. The winner is resolved either way, so the
 * operator can send that one pair back as a manual merge without working out
 * which row survives.
 *
 * Both lists belong to `autoDetect`, with one exception: a manual pair asked
 * for as a dry run returns itself in `planned`, because that is the whole
 * answer. A manual pair that really merges reports in `results` and leaves
 * both lists empty — it was never a plan, it was an instruction.
 *
 * `score` is the pair's name similarity, and it is `null` for a manual pair:
 * two ids typed into a form were never scored by anything.
 */
export interface ParkMergePlanEntry {
  winnerId: string;
  winnerName: string;
  loserId: string;
  loserName: string;
  score: number | null;
  reason: string;
  reviewReason: string | null;
}

/** A pair that merged, and what moved from the removed park to the kept one. */
export interface ParkMergeOutcome {
  winnerId: string;
  winnerName: string;
  loserId: string;
  loserName: string;
  migratedAttractions: number;
  migratedShows: number;
  migratedRestaurants: number;
  migratedScheduleEntries: number;
  migratedMappings: number;
}

export interface ParkMergeError {
  parkId: string;
  error: string;
}

/** The answer of `POST merge-duplicate-parks`, whichever path it took. */
export interface ParkMergeResponse {
  message: string;
  dryRun: boolean;
  merged: number;
  planned: ParkMergePlanEntry[];
  skipped: ParkMergePlanEntry[];
  results: ParkMergeOutcome[];
  errors: ParkMergeError[];
}

/** The body of `POST merge-duplicate-parks`; the flags may arrive as strings. */
export interface ParkMergeRequest {
  park1Id?: string;
  park2Id?: string;
  autoDetect?: boolean;
  dryRun?: boolean;
}

/** A duplicate listing with the pairs safe to merge counted apart. */
export interface DuplicateListing<T extends { safe: boolean }> {
  total: number;
  safe: number;
  needsReview: number;
  pairs: T[];
}

/**
 * `safe` and `needsReview` counted apart, so the two sets are readable without
 * going pair by pair: the safe ones are what an automatic merge would take,
 * the others are the work.
 */
export function listBySafety<T extends { safe: boolean }>(
  pairs: T[],
): DuplicateListing<T> {
  const safe = pairs.filter((p) => p.safe).length;
  return { total: pairs.length, safe, needsReview: pairs.length - safe, pairs };
}

/** A response in which nothing merged. */
function nothingMerged(
  message: string,
  dryRun: boolean,
  lists: Partial<
    Pick<ParkMergeResponse, "planned" | "skipped" | "errors">
  > = {},
): ParkMergeResponse {
  return {
    message,
    dryRun,
    merged: 0,
    planned: [],
    skipped: [],
    results: [],
    errors: [],
    ...lists,
  };
}

/** A response after real merges. */
function mergedResponse(
  results: ParkMergeOutcome[],
  errors: ParkMergeError[],
  planned: ParkMergePlanEntry[] = [],
  skipped: ParkMergePlanEntry[] = [],
): ParkMergeResponse {
  return {
    message: `Merged ${results.length} duplicate park(s)`,
    dryRun: false,
    merged: results.length,
    planned,
    skipped,
    results,
    errors,
  };
}

/**
 * Finds and merges duplicate parks for the admin's duplicates screen and the
 * MCP `merge_parks` tool, which both reach it through
 * `AdminController.mergeDuplicateParks`.
 *
 * **`autoDetect` picks its own victims, so it defaults to a dry run** and
 * merges only pairs `findDuplicates` marks `safe` — `dryRun: false` buys the
 * write, never the pairs a human still has to look at. Before PAR-247 it had
 * neither: every pair the detector returned was merged inside one call, in a
 * transaction with no undo, and the only way to ask what it would do was to
 * let it do it.
 *
 * **The manual pair keeps its default**, which is a real merge. Two ids typed
 * into a form are the human judgement `autoDetect` lacks, and the admin's
 * merge button sends exactly that body with no `dryRun` — a default flipped
 * here would be a button that silently stops working. It does honour an
 * explicit `dryRun: true`, because the alternative is the trap the attraction
 * endpoint already sprang once: a request that says "Probelauf", deletes the
 * row, and prints the outcome as a preview.
 */
@Injectable()
export class ParkDuplicateMergeService {
  constructor(
    private readonly parkValidatorService: ParkValidatorService,
    private readonly parkRepairService: ParkRepairService,
    private readonly parkMergeService: ParkMergeService,
  ) {}

  /**
   * The park duplicates, and which row a merge would keep.
   *
   * A read, which the park side did not have: detection lived inside the merge
   * behind `autoDetect: true`, and that flag merged everything it found in the
   * same call. It stays a read of its own, because asking it should not need
   * the `owner` role the merge does.
   *
   * `winnerId` is resolved by `determineMergeWinner`, the same function the
   * merge uses, because the order two ids are typed in carries no weight and
   * an operator who believes otherwise deletes the wrong park.
   */
  async list(): Promise<DuplicateListing<DuplicatePairVerdict>> {
    return listBySafety(
      await this.parkValidatorService.findDuplicatesWithWinners(),
    );
  }

  async merge(body: ParkMergeRequest = {}): Promise<ParkMergeResponse> {
    // Both flags decide whether rows get deleted, so an uninterpretable value
    // is refused rather than read as one side of the question.
    const autoDetect = readBodyFlag(body.autoDetect);
    const dryRun = readBodyFlag(body.dryRun);
    for (const [name, raw, parsed] of [
      ["autoDetect", body.autoDetect, autoDetect],
      ["dryRun", body.dryRun, dryRun],
    ] as const) {
      if (raw !== undefined && raw !== null && parsed === undefined) {
        throw new BadRequestException(
          `${name} must be a boolean (true or false)`,
        );
      }
    }

    if (autoDetect) {
      // Nothing is written unless the caller asks for it in as many words.
      return this.mergeDetected(dryRun !== false);
    }
    if (body.park1Id && body.park2Id) {
      // Only an explicit `true` previews here — see the class docblock for
      // why this default is the other way round from `autoDetect`'s.
      return this.mergePair(body.park1Id, body.park2Id, dryRun === true);
    }
    return nothingMerged(
      "Either autoDetect=true or both park1Id and park2Id must be provided",
      false,
    );
  }

  private async mergeDetected(dryRun: boolean): Promise<ParkMergeResponse> {
    const duplicates = await this.parkValidatorService.findDuplicates();
    if (duplicates.length === 0) {
      return nothingMerged("No duplicates found", dryRun);
    }

    const errors: ParkMergeError[] = [];
    const planned: ParkMergePlanEntry[] = [];
    const skipped: ParkMergePlanEntry[] = [];
    const mergePairs: Array<{ winnerId: string; loserId: string }> = [];

    // Every park of every pair in one query, not two lookups per pair.
    const allIds = Array.from(
      new Set(
        duplicates.flatMap((d) => [d.park1.id, d.park2.id]).filter(Boolean),
      ),
    );
    const parks = allIds.length
      ? await this.parkValidatorService
          .getParkRepository()
          .find({ where: { id: In(allIds) } })
      : [];
    const parkById = new Map(parks.map((p) => [p.id, p]));

    for (const duplicate of duplicates) {
      const park1 = parkById.get(duplicate.park1.id);
      const park2 = parkById.get(duplicate.park2.id);
      if (!park1 || !park2) {
        errors.push({
          parkId: duplicate.park1.id || duplicate.park2.id,
          error: "Park not found",
        });
        continue;
      }

      const verdict = determineMergeWinner(park1, park2);
      const winner = verdict.winnerId === park1.id ? park1 : park2;
      const loser = verdict.winnerId === park1.id ? park2 : park1;
      const entry: ParkMergePlanEntry = {
        winnerId: winner.id,
        winnerName: winner.name,
        loserId: loser.id,
        loserName: loser.name,
        score: duplicate.score,
        reason: duplicate.reason,
        reviewReason: duplicate.reviewReason,
      };

      // A pair a human still has to look at is reported and left alone —
      // `dryRun: false` is permission to write, not permission to decide.
      if (!duplicate.safe) {
        skipped.push(entry);
        continue;
      }

      planned.push(entry);
      mergePairs.push(verdict);
    }

    if (dryRun) {
      return nothingMerged(
        `Dry run: ${planned.length} pair(s) would be merged, ${skipped.length} need review`,
        true,
        { planned, skipped, errors },
      );
    }

    const repairResult =
      await this.parkRepairService.repairDuplicates(mergePairs);

    // The names come from `planned`, which was built from the park rows
    // themselves — the entry and its merge pair are pushed together, so the
    // two lists line up, and `repairDuplicates` returns one verdict per pair
    // in that same order.
    //
    // The verdict is read BY POSITION and never searched for by id.
    // `repairResult.errors` is keyed by park, and one run can hold the same
    // row as the loser of two pairs: three rows for one park give the
    // detector (1,2), (1,3) and (2,3), the first merge deletes the shared
    // row, the second fails on it, and the failure carries exactly the
    // `loserId` the successful pair also has. An id search therefore reads
    // the second pair's failure onto the first, dropping a park that really
    // was deleted out of `results` — the one list that says what this call
    // did — and undercounting `merged`.
    //
    // The position carries the names, so the ids are checked rather than
    // trusted: this endpoint deletes parks, and a verdict read against the
    // wrong entry would report a real deletion under another park's name,
    // which is worse than reporting nothing.
    //
    // The walk goes over `planned` and not over the verdicts, so that a list
    // which is too SHORT is a reported error rather than a quiet tail of
    // pairs nobody accounts for. Surplus verdicts are reported after the
    // loop, so neither direction can go missing.
    const results: ParkMergeOutcome[] = [];
    planned.forEach((pair, index) => {
      const verdict = repairResult.pairs[index];
      if (
        !verdict ||
        pair.winnerId !== verdict.winnerId ||
        pair.loserId !== verdict.loserId
      ) {
        errors.push({
          parkId: pair.loserId,
          error: `No merge verdict lined up with the planned pair ${pair.winnerId} / ${pair.loserId} at position ${index}; whether it merged is unknown`,
        });
        return;
      }
      if (!verdict.merged) return;

      // `repairDuplicates` does not return the migration counts; the merge
      // service only logs them.
      results.push({
        winnerId: pair.winnerId,
        winnerName: pair.winnerName,
        loserId: pair.loserId,
        loserName: pair.loserName,
        migratedAttractions: 0,
        migratedShows: 0,
        migratedRestaurants: 0,
        migratedScheduleEntries: 0,
        migratedMappings: 0,
      });
    });

    for (const surplus of repairResult.pairs.slice(planned.length)) {
      errors.push({
        parkId: surplus.loserId,
        error: `Merge verdict for ${surplus.winnerId} / ${surplus.loserId} has no planned pair; it is left out of the results`,
      });
    }

    // A skipped pair can share a row with a pair that just merged: three
    // rows for one park give A–B safe and B–C for review, and B is gone by
    // the time the operator reads the list. The entry still names it, so the
    // "send it back as a manual pair" this list is for would answer "Park
    // not found".
    //
    // Read off `planned` rather than `results`, i.e. every row a merge was
    // ATTEMPTED on, which is what the warning is about: a failed attempt is
    // the case where the row is gone and this run is the reason. In the same
    // trio, the second pair fails precisely BECAUSE the first one deleted the
    // row they share, and `results` holds only the first. The wider set costs
    // a warning on a row that is still there, and the warning says re-run
    // detection, which is true either way.
    const touchedByAMerge = new Set(planned.map((p) => p.loserId));
    for (const entry of skipped) {
      if (
        !touchedByAMerge.has(entry.winnerId) &&
        !touchedByAMerge.has(entry.loserId)
      ) {
        continue;
      }
      entry.reviewReason = `a merge in this run was attempted on one of these rows, which may no longer exist — re-run detection before acting${
        entry.reviewReason ? ` (${entry.reviewReason})` : ""
      }`;
    }

    errors.push(...repairResult.errors);
    return mergedResponse(results, errors, planned, skipped);
  }

  private async mergePair(
    park1Id: string,
    park2Id: string,
    dryRun: boolean,
  ): Promise<ParkMergeResponse> {
    const parkRepo = this.parkValidatorService.getParkRepository();
    const park1 = await parkRepo.findOne({ where: { id: park1Id } });
    const park2 = await parkRepo.findOne({ where: { id: park2Id } });
    if (!park1 || !park2) {
      return nothingMerged("One or both parks not found", dryRun, {
        errors: [{ parkId: park1Id || park2Id, error: "Park not found" }],
      });
    }

    const { winnerId, loserId } = determineMergeWinner(park1, park2);
    const winner = winnerId === park1.id ? park1 : park2;
    const loser = winnerId === park1.id ? park2 : park1;

    if (dryRun) {
      // A preview that promises what the write would refuse is worse than no
      // preview: `mergeParks` rejects one id on both sides, so the dry run
      // has to reject it too, in the same words.
      if (winnerId === loserId) {
        const refusal = `Refusing to merge park ${winnerId} into itself`;
        return nothingMerged(refusal, true, {
          errors: [{ parkId: winnerId, error: refusal }],
        });
      }
      return nothingMerged(
        `Dry run: "${loser.name}" would be merged into "${winner.name}"`,
        true,
        {
          planned: [
            {
              winnerId,
              winnerName: winner.name,
              loserId,
              loserName: loser.name,
              score: null,
              reason: "manual pair",
              reviewReason: null,
            },
          ],
        },
      );
    }

    try {
      const merge = await this.parkMergeService.mergeParks(winnerId, loserId);
      if (!merge.success) {
        return mergedResponse(
          [],
          [{ parkId: loserId, error: merge.errors.join(", ") }],
        );
      }
      return mergedResponse(
        [
          {
            winnerId: merge.winnerId,
            winnerName: merge.winnerName,
            loserId: merge.loserId,
            loserName: merge.loserName,
            migratedAttractions: merge.migratedAttractions,
            migratedShows: merge.migratedShows,
            migratedRestaurants: merge.migratedRestaurants,
            migratedScheduleEntries: merge.migratedScheduleEntries,
            migratedMappings: merge.migratedMappings,
          },
        ],
        [],
      );
    } catch (error) {
      return mergedResponse(
        [],
        [
          {
            parkId: loserId,
            error: error instanceof Error ? error.message : String(error),
          },
        ],
      );
    }
  }
}
