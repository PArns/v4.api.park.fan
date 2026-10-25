import { Column, Entity, PrimaryColumn } from "typeorm";
import type { MlDecisionReportDto } from "../dto/ml-overview.dto";

/**
 * A decision of the admin ML overview, judged once on its `evaluateOn`
 * (PAR-1059). Written by the daily job after the 12:00 UTC scoring with
 * INSERT … ON CONFLICT DO NOTHING and never updated: the verdict must not
 * drift, and the paired rows it rests on age out after 120 days.
 *
 * Moving a decision's `evaluateOn` in ml-decisions.json produces a new row
 * for the new date; the old one stays. Plain table, no FK, a few rows a year.
 */
@Entity("ml_decision_reports")
export class MlDecisionReport {
  @PrimaryColumn({ name: "decision_id", type: "varchar", length: 64 })
  decisionId: string;

  @PrimaryColumn({ name: "evaluate_on", type: "date" })
  evaluateOn: string;

  /** The report's verdict (MlDecisionState), also inside `report`. */
  @Column({ type: "varchar", length: 16 })
  verdict: string;

  /** True when a mandatory check failed. */
  @Column({ type: "boolean" })
  warning: boolean;

  @Column({ type: "jsonb" })
  report: MlDecisionReportDto;

  @Column({ name: "created_at", type: "timestamptz" })
  createdAt: Date;
}
