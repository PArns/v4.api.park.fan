import type {
  DuplicatePair,
  MismatchedQtId,
  MismatchedWzId,
  MissingQtId,
  MissingWzId,
  ValidationReport,
} from "../parks/services/park-validator.service";
import type { RepairResult } from "../parks/services/park-repair.service";

/** The answer of `POST validate-and-repair-parks`. */
export interface ParkValidationResponse {
  validation: {
    mismatchedQtIds: number;
    mismatchedWzIds: number;
    missingQtIds: number;
    missingWzIds: number;
    duplicates: number;
    summary: ValidationReport["summary"];
  };
  /** Present only after `autoFix: true`; `errors` is a count here. */
  repair?: Omit<RepairResult, "errors"> & { errors: number };
  report: {
    mismatchedQtIds: Array<
      Pick<MismatchedQtId, "parkId" | "parkName" | "currentQtId" | "reason">
    >;
    mismatchedWzIds: Array<
      Pick<MismatchedWzId, "parkId" | "parkName" | "currentWzId" | "reason">
    >;
    missingQtIds: Array<
      Pick<MissingQtId, "parkId" | "parkName" | "suggestedQtId">
    >;
    missingWzIds: Array<
      Pick<MissingWzId, "parkId" | "parkName" | "suggestedWzId">
    >;
    duplicates: Array<
      Pick<DuplicatePair, "park1" | "park2" | "score" | "reason">
    >;
  };
}

/** The validator's report as the admin route answers it. */
export function toParkValidationResponse(
  report: ValidationReport,
  repair: RepairResult | null,
): ParkValidationResponse {
  return {
    validation: {
      mismatchedQtIds: report.mismatchedQtIds.length,
      mismatchedWzIds: report.mismatchedWzIds.length,
      missingQtIds: report.missingQtIds.length,
      missingWzIds: report.missingWzIds.length,
      duplicates: report.duplicates.length,
      summary: report.summary,
    },
    repair: repair ? { ...repair, errors: repair.errors.length } : undefined,
    report: {
      mismatchedQtIds: report.mismatchedQtIds.map((m) => ({
        parkId: m.parkId,
        parkName: m.parkName,
        currentQtId: m.currentQtId,
        reason: m.reason,
      })),
      mismatchedWzIds: report.mismatchedWzIds.map((m) => ({
        parkId: m.parkId,
        parkName: m.parkName,
        currentWzId: m.currentWzId,
        reason: m.reason,
      })),
      missingQtIds: report.missingQtIds.map((m) => ({
        parkId: m.parkId,
        parkName: m.parkName,
        suggestedQtId: m.suggestedQtId,
      })),
      missingWzIds: report.missingWzIds.map((m) => ({
        parkId: m.parkId,
        parkName: m.parkName,
        suggestedWzId: m.suggestedWzId,
      })),
      duplicates: report.duplicates.map((d) => ({
        park1: d.park1,
        park2: d.park2,
        score: d.score,
        reason: d.reason,
      })),
    },
  };
}
