import { Column, Entity, Index, PrimaryColumn } from "typeorm";

/**
 * Paired per-park-day errors of a SHADOW model against a reference on the
 * same slots (PAR-852): the input of the board's park-cluster bootstrap.
 *
 * `forecast_archive_scores` pools every park of a region into one row, so it
 * can give a mean but never a confidence interval: days of one park are not
 * independent, and the bench (PAR-828) draws its 95 % CI by resampling PARKS.
 * This table keeps exactly what that resampling needs — per park and day, the
 * paired slot count and both error sums — and nothing per slot.
 *
 * One row per (target date, park, use case, lead, model, reference, segment).
 * `n` slots carried a truth value AND a value from both the model and the
 * reference; `sae_model` / `sae_ref` are Σ|pred − truth| of each over those
 * slots, `se_*` Σ(pred − truth). The paired difference of a cell is
 * (Σ sae_model − Σ sae_ref) / Σ n — "sum the minutes, then divide".
 *
 * Plain table, no FK, app-side retention (forward-archive.md §2): ~8 k rows a
 * day, kept {@link PAIRED_RETENTION_DAYS} days.
 */
export const PAIRED_RETENTION_DAYS = 120;

/**
 * Which scoring definition a row was written under. 2 (PAR-1024): truth
 * without heartbeat rows, slot waits of both arms served-rounded. 3
 * (PAR-1035/1037): the matrix rows are paired on the rows where EVERY
 * candidate of the question has a value (reference `served_joint`), the
 * day-peak rows are split by the model behind the served level
 * (`served_tft` / `served_catboost`), and D7 rows exist. Readers pool only
 * the current definition, so a live gate is never met on rows scored the
 * old way; rows from before the column read as 1.
 */
export const PAIRED_DEFINITION = 3;

@Entity("forecast_archive_paired")
@Index("idx_fap_target", ["targetDate"])
export class ForecastArchivePaired {
  @PrimaryColumn({ name: "target_date", type: "date" })
  targetDate: string;

  @PrimaryColumn({ name: "park_id", type: "uuid" })
  parkId: string;

  /** UC1 / UC2 (live, slot leads) or UC3 (grid, day leads). */
  @PrimaryColumn({ name: "use_case", type: "varchar", length: 8 })
  useCase: string;

  /** h0-1 / h1-2 / h2-6 or d0 … d7. */
  @PrimaryColumn({ type: "varchar", length: 8 })
  lead: string;

  /** The shadow: `chronos2` (live) or `chronos2_grid` (grid). */
  @PrimaryColumn({ type: "varchar", length: 24 })
  model: string;

  /** `served` (park_hourly / plan_day), `persistence` (live anchor), `h5`. */
  @PrimaryColumn({ type: "varchar", length: 16 })
  reference: string;

  /** all / busy / headliner. */
  @PrimaryColumn({ type: "varchar", length: 12 })
  segment: string;

  /** EU / NA / ASIA / OTHER — the board filters on it. */
  @Column({ type: "varchar", length: 8 })
  region: string;

  @Column({ type: "integer" })
  n: number;

  @Column({ name: "sae_model", type: "double precision" })
  saeModel: number;

  @Column({ name: "sae_ref", type: "double precision" })
  saeRef: number;

  @Column({ name: "se_model", type: "double precision" })
  seModel: number;

  @Column({ name: "se_ref", type: "double precision" })
  seRef: number;

  /** {@link PAIRED_DEFINITION} at write time. */
  @Column({ type: "smallint", default: 1 })
  definition: number;

  @Column({ name: "scored_at", type: "timestamptz" })
  scoredAt: Date;
}
