import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * Column widths of the score key. Every label the scorer emits must fit —
 * `forecast-archive-scoring.util.spec.ts` asserts it, because one over-long
 * label fails its 500-row INSERT and rolls back the whole scoring date.
 */
export const SCORE_KEY_WIDTHS = {
  region: 8,
  useCase: 8,
  lead: 8,
  source: 32,
  segment: 12,
} as const;

/**
 * Daily scores of the forward archive (PAR-831), one row per
 * (target date × region × use case × lead × source × segment).
 *
 * `sums` holds ADDITIVE counters only (n, Σ|err|, Σerr, hits, …), never a mean,
 * so the board can pool any window of days by adding rows and dividing once —
 * the same "sum the minutes, then divide" rule the crowd levels follow. The
 * counter names are documented in `forecast-archive-scoring.util.ts` and
 * docs/ml/forward-archive.md.
 *
 * `region` includes an explicit `ALL` row per key rather than leaving the board
 * to add the regions up: the cross-park pairwise counters (D6) are counted
 * over pairs, and pairs across two regions belong to neither.
 */
@Entity("forecast_archive_scores")
@Index("idx_fas_target", ["targetDate"])
export class ForecastArchiveScore {
  @PrimaryColumn({ name: "target_date", type: "date" })
  targetDate: string;

  /** EU / NA / ASIA / OTHER / ALL. */
  @PrimaryColumn({ type: "varchar", length: SCORE_KEY_WIDTHS.region })
  region: string;

  /** UC1 / UC2 / UC3 / D6 / D9. */
  @PrimaryColumn({
    name: "use_case",
    type: "varchar",
    length: SCORE_KEY_WIDTHS.useCase,
  })
  useCase: string;

  /** h0-1 … h24-48 for 15-min slot leads, d0 … d7 for day leads. */
  @PrimaryColumn({ type: "varchar", length: SCORE_KEY_WIDTHS.lead })
  lead: string;

  /** Slot source (pcn_blend, catboost, measured, composed, …), `mixed`, or `all`. */
  @PrimaryColumn({ type: "varchar", length: SCORE_KEY_WIDTHS.source })
  source: string;

  /** all / busy (ex-ante) / headliner. */
  @PrimaryColumn({ type: "varchar", length: SCORE_KEY_WIDTHS.segment })
  segment: string;

  @Column({ type: "jsonb" })
  sums: Record<string, number>;

  @Column({ name: "scored_at", type: "timestamptz" })
  scoredAt: Date;
}
