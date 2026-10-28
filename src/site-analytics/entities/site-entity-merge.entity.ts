import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * Which park, ride, show or restaurant a merged-away one became. The
 * statistic's rows keep the id they were counted under and are mapped through
 * this table when read, because rewriting a compressed hypertable is DML on
 * compressed data. Always the final survivor: a merge into a row that was
 * itself merged away later is rewritten to the new survivor
 * (`recordEntityMerge` in `src/parks/utils/merge-dependencies.ts`).
 */
@Entity("site_entity_merges")
export class SiteEntityMerge {
  @PrimaryColumn({ name: "loser_id", type: "uuid" })
  loserId: string;

  @Column({ name: "winner_id", type: "uuid" })
  winnerId: string;

  @Column({ name: "merged_at", type: "timestamptz", default: () => "now()" })
  mergedAt: Date;
}
